"""迟到处理 / 补数任务专用接口：文件导入导出、批量回执、覆盖核对。

这些路径比通用资源路由更具体，必须先于 ``/{module}/{entry_id}`` 注册，
否则 ``/api/late_handler/imports`` 会被当成“id 为 imports 的记录”。
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field

from app.services import service_for
from app.services import late_bypass

router = APIRouter(prefix="/api", tags=["迟到补数"])


class IngestPayload(BaseModel):
    content: str = Field(description="文件内容（CSV 文本或 JSON）")
    filename: str = ""
    watermark: float | None = None
    segment_gap: float | None = None
    paused_strategy: str = Field(default="extend", pattern="^(extend|split)$")


class ContentPayload(BaseModel):
    content: str
    filename: str = ""
    paused_strategy: str = Field(default="extend", pattern="^(extend|split)$")


def _late_service():
    service = service_for("late_handler")
    if service is None:  # pragma: no cover - 注册表固定包含该服务
        raise HTTPException(status_code=500, detail="迟到处理服务未注册")
    return service


def _backfill_service():
    service = service_for("backfill_job")
    if service is None:  # pragma: no cover
        raise HTTPException(status_code=500, detail="补数任务服务未注册")
    return service


def _csv_response(filename: str, header: list[str], rows: list[dict[str, Any]]) -> StreamingResponse:
    import csv
    import io

    output = io.StringIO()
    output.write("﻿")
    writer = csv.DictWriter(output, fieldnames=header, lineterminator="\n", extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---- 迟到处理 ----

@router.get("/late_handler/imports")
def list_late_imports() -> dict[str, Any]:
    """导入台账：同源导出文件的接收记录，可核对是否重复导入。"""
    service = _late_service()
    ledger = service.list_import_ledger()
    return {"items": ledger, "total": len(ledger)}


@router.get("/late_handler/exports")
def list_late_exports() -> dict[str, Any]:
    """导出台账：每次导出的校验和、区段数、作业数。"""
    service = _late_service()
    ledger = service.list_export_ledger()
    return {"items": ledger, "total": len(ledger)}


@router.get("/late_handler/{policy_name}/processing")
def late_processing_detail(policy_name: str) -> dict[str, Any]:
    """迟到处理页面：实际采用的迟到比例与阈值、批次回执、区段与作业覆盖。"""
    detail = _late_service().late_processing_detail(policy_name)
    if not detail.get("ok"):
        raise HTTPException(status_code=404, detail=detail.get("error", "策略不存在"))
    return detail


@router.get("/late_handler/{policy_name}/batches")
def late_batches(policy_name: str) -> dict[str, Any]:
    items = _late_service().list_batches(policy_name)
    return {"items": items, "total": len(items)}


@router.get("/late_handler/{policy_name}/segments")
def late_segments(policy_name: str) -> dict[str, Any]:
    items = _late_service().list_segments(policy_name)
    return {"items": items, "total": len(items)}


@router.get("/late_handler/{policy_name}/bypass")
def late_bypass_rows(policy_name: str) -> dict[str, Any]:
    items = _late_service().list_bypass(policy_name)
    return {"items": items, "total": len(items)}


@router.post("/late_handler/{policy_name}/ingest")
def ingest_late_file(policy_name: str, payload: IngestPayload) -> dict[str, Any]:
    """5号入口：读取迟到事件文件 → 判断阈值 → 生成补数任务清单（幂等）。"""
    result = _late_service().ingest_late_events(
        policy_name,
        payload.content,
        filename=payload.filename,
        watermark=payload.watermark,
        segment_gap=payload.segment_gap,
        paused_strategy=payload.paused_strategy,
    )
    if not result.get("ok"):
        return JSONResponse(status_code=400, content=result)
    return result


@router.post("/late_handler/{policy_name}/verify")
def verify_late_file(policy_name: str, payload: ContentPayload) -> dict[str, Any]:
    """校验导出文件：覆盖全部异常区段、迟到明细齐全、只有一份可执行范围。"""
    return _late_service().verify_manifest_content(payload.content, policy_name=policy_name)


@router.post("/late_handler/{policy_name}/import")
def import_late_file(policy_name: str, payload: ContentPayload) -> dict[str, Any]:
    """接收其它页面回传的同源导出文件；重复回传不重复入库。"""
    result = _late_service().import_manifest(
        policy_name,
        payload.content,
        filename=payload.filename,
        paused_strategy=payload.paused_strategy,
    )
    if not result.get("ok"):
        return JSONResponse(status_code=400, content=result)
    return result


@router.get("/late_handler/{policy_name}/export")
def export_late_manifest(policy_name: str) -> StreamingResponse:
    """导出迟到旁路数据 + 补数任务清单（JSON 文件，可被其它页面回传）。"""
    result = _late_service().export_manifest(policy_name)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "策略不存在"))
    body = json.dumps(result["document"], ensure_ascii=False, indent=2)
    return StreamingResponse(
        iter([body]),
        media_type="application/json; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{result["filename"]}"',
            "X-Manifest-Checksum": result["checksum"],
        },
    )


@router.get("/late_handler/{policy_name}/export-detail.csv")
def export_late_detail_csv(policy_name: str) -> StreamingResponse:
    """导出迟到明细（CSV），用于人工核对每一个迟到事件。"""
    service = _late_service()
    rows = service.list_bypass(policy_name)
    return _csv_response(
        f"{late_bypass.safe_filename(policy_name)}_late_detail.csv",
        ["旁路ID", "事件ID", "源端", "事件时间", "水位", "区段", "批次", "版本", "迟到动作", "状态"],
        rows,
    )


@router.get("/late_handler/{policy_name}/batches.csv")
def export_late_batches_csv(policy_name: str) -> StreamingResponse:
    """导出批量回执（CSV）：每批实际采用的阈值与迟到比例。"""
    rows = _late_service().list_batches(policy_name)
    return _csv_response(
        f"{late_bypass.safe_filename(policy_name)}_late_batches.csv",
        ["批次号", "文件名", "总事件数", "迟到数", "迟到比例", "阈值百分比", "阈值来源", "允许延迟", "水位", "触发", "区段数", "作业", "导入时间"],
        rows,
    )


@router.get("/late_handler/{policy_name}/manifest.txt", response_class=PlainTextResponse)
def late_manifest_text(policy_name: str) -> PlainTextResponse:
    """把清单以纯文本返回，便于页面直接渲染或复制到其它系统。"""
    # 纯文本预览不登记导出台账，避免和真正的导出共用校验和时产生歧义。
    result = _late_service().build_manifest_document(
        policy_name, export_id=None, record_export=False,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "策略不存在"))
    return PlainTextResponse(json.dumps(result["document"], ensure_ascii=False, indent=2))


# ---- 补数任务 ----

@router.get("/backfill_job/console")
def backfill_console() -> dict[str, Any]:
    """补数页面：按策略列出区段、作业，并给出“每个异常区段都有作业”的结论。"""
    return _backfill_service().task_console()


@router.get("/backfill_job/coverage")
def backfill_coverage() -> dict[str, Any]:
    return _backfill_service().find_coverage_gaps()


@router.get("/backfill_job/pending-segments")
def backfill_pending_segments(policy: str | None = Query(default=None)) -> dict[str, Any]:
    items = _backfill_service().list_pending_segments(policy)
    return {"items": items, "total": len(items)}


@router.get("/backfill_job/tasks")
def backfill_tasks(policy: str | None = Query(default=None)) -> dict[str, Any]:
    """补数任务清单行：作业数量与未覆盖区段一致。"""
    items = _backfill_service().export_task_listing(policy)
    return {"items": items, "total": len(items)}


@router.get("/backfill_job/manifest")
def export_backfill_manifest(policy: str | None = Query(default=None)) -> StreamingResponse:
    """补数任务清单下载（JSON），含迟到明细、区段与作业覆盖。"""
    result = _backfill_service().export_task_manifest(policy)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "暂无可导出的补数任务"))
    body = json.dumps(result["document"], ensure_ascii=False, indent=2)
    return StreamingResponse(
        iter([body]),
        media_type="application/json; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{result["filename"]}"',
            "X-Manifest-Checksum": result["checksum"],
        },
    )


@router.get("/backfill_job/tasks.csv")
def export_backfill_tasks_csv(policy: str | None = Query(default=None)) -> StreamingResponse:
    """补数任务清单（CSV）：一行一个区段及其作业。"""
    rows = _backfill_service().export_task_listing(policy)
    return _csv_response(
        "backfill_tasks.csv",
        ["区段", "策略", "源端", "补数范围", "迟到事件数", "区段版本", "作业ID", "作业名称", "作业状态", "进度比例", "已覆盖"],
        rows,
    )


@router.post("/backfill_job/verify")
def verify_backfill_manifest(payload: ContentPayload) -> dict[str, Any]:
    """校验补数任务清单覆盖全部异常区段与迟到明细。"""
    return _backfill_service().verify_task_manifest(payload.content)


@router.post("/backfill_job/{job_id}/split")
def split_backfill_job(job_id: int) -> dict[str, Any]:
    """暂停状态下选择分段：关闭旧作业并新建分段作业。"""
    result = _backfill_service().split_paused_job(job_id)
    if not result.get("ok"):
        return JSONResponse(status_code=400, content=result)
    return result
