"""迟到旁路与补数清单的共享领域逻辑。

迟到处理（late_handler）和补数任务（backfill_job）围绕同一份数据工作：

    迟到事件（水位 + 允许延迟）
        └── 迟到旁路数据（按源端和事件时间聚合成区段，带版本）
                └── 补数任务清单（每个未覆盖区段一个可执行作业）
                        └── 导出文件（迟到明细 + 区段 + 作业，可回传校验）

本模块只放纯函数和共享常量：时间解析、阈值解析、事件归一化、区段切分、
文件摘要、清单构建与校验。需要读写 store 的动作留在两个领域服务里。

边界约定（两条都是“文件边界”最容易写错的地方）：

* 事件时间 ``== cutoff``（水位 - 允许延迟）视为准时，只有严格小于才算迟到；
* 迟到比例 ``== 阈值`` 不触发，只有严格大于阈值才进入迟到旁路。

两条都用严格比较，避免边界事件被重复投递或整个迟到区段被跳过。
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
from datetime import datetime, timezone
from typing import Any

# 阈值缺失或格式非法时使用的策略默认值：5%（不是猜测出来的 0.1 比例）。
DEFAULT_THRESHOLD_PERCENT = 5.0
DEFAULT_ALLOWED_LATENESS_SEC = 10.0
DEFAULT_SEGMENT_GAP_SEC = 60.0

MANIFEST_FORMAT = "late_bypass_manifest"
MANIFEST_VERSION = 1

# CSV 表头别名：导出文件用中文字段，外部系统也可能给英文表头。
_HEADER_ALIASES: dict[str, tuple[str, ...]] = {
    "event_id": ("事件ID", "事件编号", "event_id", "eventid", "id", "编号"),
    "source": ("源端", "数据源", "source", "source_name", "源端名称"),
    "event_time": ("事件时间", "event_time", "eventtime", "时间", "ts", "timestamp"),
    "watermark": ("水位", "当前水位", "watermark", "water_mark"),
    "segment": ("区段", "区段ID", "segment", "segment_id"),
    "is_late": ("迟到", "是否迟到", "迟到标记", "is_late", "late"),
    "payload": ("载荷", "payload", "内容", "data"),
    "policy": ("策略名称", "策略", "policy", "policy_name"),
}

_TRUE_WORDS = {"1", "true", "yes", "y", "是", "迟到", "late"}


class ManifestError(ValueError):
    """导入文件结构不可用。"""


# ---------------------------------------------------------------------------
# 基础解析
# ---------------------------------------------------------------------------

def parse_timestamp(value: Any, field: str = "事件时间") -> float:
    """解析 epoch 秒或 ISO-8601 时间字符串。无法解析时抛 ManifestError。"""
    if isinstance(value, bool):
        raise ManifestError(f"{field}格式错误：{value!r}")
    if isinstance(value, (int, float)):
        number = float(value)
        if not math.isfinite(number):
            raise ManifestError(f"{field}不是有限数值：{value!r}")
        return number

    text = str(value if value is not None else "").strip()
    if not text:
        raise ManifestError(f"{field}为空")
    try:
        number = float(text)
        if math.isfinite(number):
            return number
    except (TypeError, ValueError):
        pass

    raw = text.replace("/", "-").strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        moment = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ManifestError(f"{field}无法解析：{text}") from exc
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.timestamp()


def parse_threshold(policy: dict[str, Any] | None) -> dict[str, Any]:
    """解析策略阈值，缺失/非法时回退到策略默认值（5%）。

    阈值字段（``阈值百分比``）按百分数存储：``5`` 表示 5%，即比例 0.05。
    空字符串、占位符、负数、大于 100 的值都视为无效并回退默认值，
    不会把空阈值猜成 0.1 之类的“小比例”。
    """
    raw = (policy or {}).get("阈值百分比")
    text = "" if raw is None else str(raw).strip()
    if text:
        candidate = text[:-1].strip() if text.endswith("%") else text
        try:
            percent = float(candidate)
        except (TypeError, ValueError):
            percent = None
        if percent is not None and math.isfinite(percent) and 0 <= percent <= 100:
            return {
                "percent": round(percent, 4),
                "ratio": round(percent / 100, 6),
                "source": "策略配置",
                "raw": raw,
                "fallback": False,
            }
        reason = "阈值配置不是 0-100 的数值"
    else:
        reason = "阈值配置为空"
    return {
        "percent": DEFAULT_THRESHOLD_PERCENT,
        "ratio": round(DEFAULT_THRESHOLD_PERCENT / 100, 6),
        "source": "策略默认值",
        "raw": raw,
        "fallback": True,
        "reason": reason,
    }


def parse_allowed_lateness(policy: dict[str, Any] | None) -> dict[str, Any]:
    """解析允许延迟（秒），缺失/非法时回退默认 10 秒。"""
    raw = (policy or {}).get("允许延迟")
    text = "" if raw is None else str(raw).strip().rstrip("sS")
    if text:
        try:
            seconds = float(text)
        except (TypeError, ValueError):
            seconds = None
        if seconds is not None and math.isfinite(seconds) and seconds >= 0:
            return {"seconds": seconds, "source": "策略配置", "raw": raw, "fallback": False}
    return {
        "seconds": DEFAULT_ALLOWED_LATENESS_SEC,
        "source": "策略默认值",
        "raw": raw,
        "fallback": True,
    }


def parse_segment_gap(value: Any, default: float = DEFAULT_SEGMENT_GAP_SEC) -> float:
    """解析区段切分间隔（秒）。"""
    if value is None or str(value).strip() == "":
        return default
    try:
        gap = float(str(value).strip().rstrip("sS"))
    except (TypeError, ValueError):
        return default
    return gap if math.isfinite(gap) and gap >= 0 else default


def coerce_bool(value: Any) -> bool | None:
    """把“是否迟到”这类列解析成三态：True / False / None(未提供)。"""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().casefold()
    if not text:
        return None
    return text in _TRUE_WORDS


def _canonical_header(name: str) -> str | None:
    folded = name.strip().casefold()
    for canonical, aliases in _HEADER_ALIASES.items():
        if folded in {alias.casefold() for alias in aliases}:
            return canonical
    return None


# ---------------------------------------------------------------------------
# 文件解析
# ---------------------------------------------------------------------------

def parse_csv_content(content: str) -> list[dict[str, Any]]:
    """解析 CSV 文本，返回统一的英文键行。"""
    text = content.lstrip("﻿")
    if not text.strip():
        raise ManifestError("导入文件为空")
    try:
        dialect = csv.Sniffer().sniff(text[:4096], delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    if not reader.fieldnames:
        raise ManifestError("导入文件缺少表头")
    mapping = {field: _canonical_header(field) for field in reader.fieldnames}
    if "event_time" not in mapping.values():
        raise ManifestError("导入文件缺少事件时间列，可选表头：事件时间/event_time")
    rows: list[dict[str, Any]] = []
    for line_no, raw in enumerate(reader, start=2):
        row: dict[str, Any] = {"_line": line_no}
        for field, value in raw.items():
            canonical = mapping.get(field)
            if canonical:
                row[canonical] = value
        if not any(str(v or "").strip() for k, v in row.items() if not k.startswith("_")):
            continue
        row.setdefault("event_id", f"row-{line_no}")
        rows.append(row)
    if not rows:
        raise ManifestError("导入文件没有数据行")
    return rows


def parse_import_content(
    content: str,
    filename: str = "",
) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    """解析导入文件，返回 (kind, rows, meta)。

    kind: ``manifest``（本系统导出的 JSON 清单）或 ``events``（事件的 CSV/JSON）。
    """
    text = (content or "").lstrip("﻿").strip()
    if not text:
        raise ManifestError("导入文件为空")

    looks_json = text.startswith("{") or text.startswith("[") or filename.lower().endswith(".json")
    if looks_json:
        try:
            document = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ManifestError(f"JSON 文件无法解析：{exc.msg}") from exc
        if isinstance(document, list):
            return "events", document, {"filename": filename}
        if not isinstance(document, dict):
            raise ManifestError("JSON 顶层必须是对象或数组")
        if document.get("format") == MANIFEST_FORMAT or "segments" in document:
            return "manifest", [], {"filename": filename, "document": document}
        rows = document.get("entries") or document.get("events") or document.get("rows")
        if not isinstance(rows, list):
            raise ManifestError("JSON 文件既不是导出清单，也没有 entries/events 数组")
        meta = {key: value for key, value in document.items() if key not in ("entries", "events", "rows")}
        meta["filename"] = filename
        return "events", rows, meta

    return "events", parse_csv_content(text), {"filename": filename}


# ---------------------------------------------------------------------------
# 事件归一化与区段切分
# ---------------------------------------------------------------------------

def normalize_events(
    raw_events: list[dict[str, Any]],
    default_source: str = "unknown_source",
) -> list[dict[str, Any]]:
    """把各种来源的事件行归一化成固定结构。

    任一行时间无法解析都会让整份文件失败（导入按事务处理，不做部分入库）。
    同一文件里 ``源端 + 事件ID`` 重复出现的行按同一事件处理，只保留首次出现，
    避免一段迟到事件被拆进两个区段、各生成一条旁路记录。
    """
    events: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for index, row in enumerate(raw_events, start=1):
        if not isinstance(row, dict):
            raise ManifestError(f"第 {index} 行不是对象")
        event_time = parse_timestamp(row.get("event_time"), "事件时间")
        source = str(row.get("source") or default_source).strip() or default_source
        watermark_raw = row.get("watermark")
        watermark = None
        if watermark_raw is not None and str(watermark_raw).strip() != "":
            watermark = parse_timestamp(watermark_raw, "水位")
        event_id = str(
            row.get("event_id")
            or row.get("eventId")
            or row.get("id")
            or f"{source}-{event_time}-{index}"
        ).strip()
        identity = (source, event_id)
        if identity in seen:
            continue
        seen.add(identity)
        events.append({
            "event_id": event_id,
            "source": source,
            "event_time": event_time,
            "watermark": watermark,
            "is_late": coerce_bool(row.get("is_late")),
            "segment_hint": (
                str(row.get("segment")).strip()
                if row.get("segment") is not None
                else None
            ),
            "payload": row.get("payload"),
            "line": row.get("_line", index),
        })
    return events


def classify_events(
    events: list[dict[str, Any]],
    watermark: float,
    allowed_lateness: float,
) -> dict[str, Any]:
    """按水位和允许延迟判定迟到事件。

    ``event_time < watermark - allowed_lateness`` 才是迟到（边界相等算准时）。
    """
    cutoff = watermark - allowed_lateness
    late: list[dict[str, Any]] = []
    on_time: list[dict[str, Any]] = []
    for event in events:
        effective_watermark = event.get("watermark")
        if effective_watermark is None:
            effective_watermark = watermark
        is_late = event.get("is_late")
        if is_late is None:
            is_late = event["event_time"] < effective_watermark - allowed_lateness
        enriched = {
            **event,
            "effective_watermark": effective_watermark,
            "cutoff": effective_watermark - allowed_lateness,
            "late": bool(is_late),
        }
        (late if enriched["late"] else on_time).append(enriched)

    total = len(events)
    ratio = len(late) / max(total, 1)
    return {
        "events": [*late, *on_time],
        "late": late,
        "on_time": on_time,
        "watermark": watermark,
        "cutoff": cutoff,
        "late_ratio": ratio,
        "total_events": total,
        "late_count": len(late),
    }


def segment_late_events(
    late_events: list[dict[str, Any]],
    segment_gap: float = DEFAULT_SEGMENT_GAP_SEC,
) -> list[dict[str, Any]]:
    """把迟到事件按源端 + 连续事件时间切成区段。

    相邻事件时间差超过 ``segment_gap`` 秒、源端变化或显式区段号变化时切段。
    区段键只由源端和区段起止时间决定，重复导入同一批数据会落到同一个键上。
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for event in late_events:
        grouped.setdefault(event["source"], []).append(event)

    found: list[dict[str, Any]] = []
    for source, events in grouped.items():
        events.sort(key=lambda e: (e["event_time"], e["event_id"]))
        current: list[dict[str, Any]] = []
        current_hint: str | None = None
        for event in events:
            hint = event.get("segment_hint")
            if not current:
                current = [event]
                current_hint = hint
                continue
            previous = current[-1]
            hint_changed = (
                hint is not None and current_hint is not None and hint != current_hint
            )
            gap = abs(event["event_time"] - previous["event_time"])
            if hint_changed or gap > segment_gap:
                found.append(_build_segment(source, current, segment_gap))
                current = [event]
                current_hint = hint
            else:
                current.append(event)
                if current_hint is None:
                    current_hint = hint
        if current:
            found.append(_build_segment(source, current, segment_gap))

    # 同一个显式区段号可能被文件边界拆成多段：并回一段，避免重复区段。
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    segments: list[dict[str, Any]] = []
    for segment in found:
        hint = segment.get("显式区段")
        if hint is None:
            segments.append(segment)
            continue
        key = (segment["源端"], str(hint))
        existing = merged.get(key)
        if existing is None:
            merged[key] = segment
            continue
        existing["起始时间"] = min(existing["起始时间"], segment["起始时间"])
        existing["截止时间"] = max(existing["截止时间"], segment["截止时间"])
        existing["迟到事件数"] += segment["迟到事件数"]
        existing["事件列表"].extend(segment["事件列表"])
        existing["事件ID列表"].extend(segment["事件ID列表"])
        existing["水位"] = max(existing["水位"], segment["水位"])
    segments.extend(merged.values())

    segments.sort(key=lambda s: (s["源端"], s["起始时间"], s["截止时间"]))
    return segments


def _build_segment(
    source: str,
    events: list[dict[str, Any]],
    segment_gap: float,
) -> dict[str, Any]:
    start = min(e["event_time"] for e in events)
    end = max(e["event_time"] for e in events)
    hint = next((e["segment_hint"] for e in events if e.get("segment_hint")), None)
    key = (
        f"{source}|{hint}"
        if hint
        else f"{source}|{int(math.floor(start))}|{int(math.floor(max(end, start)))}"
    )
    return {
        "区段ID": key,
        "源端": source,
        "起始时间": start,
        "截止时间": end,
        "迟到事件数": len(events),
        "事件列表": [
            {
                "event_id": e["event_id"],
                "event_time": e["event_time"],
                "watermark": e.get("effective_watermark", 0),
            }
            for e in events
        ],
        "事件ID列表": [e["event_id"] for e in events],
        "水位": max(e.get("effective_watermark", 0) for e in events),
        "切分间隔": segment_gap,
        "显式区段": hint,
    }


def segment_range(segment: dict[str, Any]) -> tuple[str, str]:
    """补数位点范围：整秒，起止相等时右端 +1 避免空范围。"""
    start = int(math.floor(segment["起始时间"]))
    end = int(math.floor(segment["截止时间"]))
    if end <= start:
        end = start + 1
    return str(start), str(end)


def segment_start(segment: dict[str, Any]) -> int:
    return int(math.floor(segment.get("起始时间", 0) or 0))


def segment_end(segment: dict[str, Any]) -> int:
    start = segment_start(segment)
    end = int(math.floor(segment.get("截止时间", 0) or 0))
    return end if end > start else start + 1


# ---------------------------------------------------------------------------
# 清单构建与校验
# ---------------------------------------------------------------------------

def content_digest(text: str) -> str:
    """文件内容摘要：统一换行和 BOM，保证同一份文件重复上传摘要一致。"""
    normalized = (text or "").lstrip("﻿").replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def manifest_checksum(payload: dict[str, Any]) -> str:
    """清单内容校验和：只覆盖业务数据，不包含导出时间/导出 ID。"""
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_manifest(
    *,
    policy_name: str,
    policy: dict[str, Any] | None,
    threshold: dict[str, Any],
    allowed_lateness: dict[str, Any],
    watermark: float | None,
    batches: list[dict[str, Any]],
    bypass_rows: list[dict[str, Any]],
    jobs: list[dict[str, Any]],
    export_id: str = "",
    exported_at: float | None = None,
) -> dict[str, Any]:
    """把迟到旁路数据与补数作业组装成可回传的清单文件。"""
    active = [row for row in bypass_rows if row.get("状态", "生效") == "生效"]
    entries = [
        {
            "事件ID": row.get("事件ID", ""),
            "源端": row.get("源端", ""),
            "事件时间": row.get("事件时间", 0),
            "水位": row.get("水位", 0),
            "区段": row.get("区段", ""),
            "批次": row.get("批次", ""),
            "版本": row.get("版本", 1),
            "迟到": True,
        }
        for row in sorted(active, key=lambda r: (r.get("区段", ""), r.get("事件时间", 0), r.get("事件ID", "")))
    ]
    bypass = [
        {
            "旁路ID": row.get("旁路ID", ""),
            "事件ID": row.get("事件ID", ""),
            "源端": row.get("源端", ""),
            "事件时间": row.get("事件时间", 0),
            "水位": row.get("水位", 0),
            "区段": row.get("区段", ""),
            "批次": row.get("批次", ""),
            "版本": row.get("版本", 1),
            "状态": row.get("状态", ""),
            "迟到动作": row.get("迟到动作", ""),
        }
        for row in sorted(active, key=lambda r: (r.get("区段", ""), r.get("事件时间", 0), r.get("事件ID", "")))
    ]

    segment_map: dict[str, dict[str, Any]] = {}
    for row in active:
        key = row.get("区段", "")
        event_time = float(row.get("事件时间", 0) or 0)
        segment = segment_map.get(key)
        if segment is None:
            segment_map[key] = {
                "区段ID": key,
                "源端": row.get("源端", ""),
                "起始时间": event_time,
                "截止时间": event_time,
                "迟到事件数": 1,
                "版本": row.get("版本", 1),
            }
            continue
        segment["起始时间"] = min(segment["起始时间"], event_time)
        segment["截止时间"] = max(segment["截止时间"], event_time)
        segment["迟到事件数"] += 1
        segment["版本"] = max(segment["版本"], int(row.get("版本", 1) or 1))

    # 只导出真正归属到本策略区段的作业：没有区段归属的历史作业不构成补数范围。
    relevant_jobs = [
        job for job in jobs
        if any(key in (job.get("_segment_keys") or []) for key in segment_map)
    ]
    jobs_payload: list[dict[str, Any]] = []
    for job in relevant_jobs:
        keys = [key for key in (job.get("_segment_keys") or []) if key in segment_map]
        executable = (
            bool(job.get("executable", job.get("status") in ("待执行", "补数中")))
            and not job.get("_paused")
            and not job.get("_superseded_by")
        )
        jobs_payload.append({
            "作业ID": job.get("id"),
            "作业名称": job.get("作业名称", ""),
            "目标指标": job.get("目标指标", ""),
            "补数范围": job.get("补数范围", ""),
            "起始位点": job.get("起始位点", ""),
            "截止位点": job.get("截止位点", ""),
            "并行度": job.get("并行度", ""),
            "进度比例": job.get("进度比例", "0%"),
            "状态": job.get("status", ""),
            "可执行": executable,
            "区段": keys,
            "覆盖事件数": job.get("_covered_events", 0),
            "_paused": bool(job.get("_paused")),
            "_closed_reason": job.get("_closed_reason", ""),
        })

    for segment in segment_map.values():
        key = segment["区段ID"]
        covering = [item for item in jobs_payload if key in item["区段"]]
        executable_cover = [item for item in covering if item["可执行"]]
        segment["作业ID"] = executable_cover[0]["作业ID"] if executable_cover else None
        segment["作业数"] = len(covering)
        segment["可执行作业数"] = len(executable_cover)
        # 覆盖以“可执行作业”为准：暂停或已关闭的作业不构成待补范围。
        segment["已覆盖"] = bool(executable_cover)

    segments = sorted(segment_map.values(), key=lambda s: (s["源端"], s["起始时间"]))
    missing = [s["区段ID"] for s in segments if not s["已覆盖"]]
    duplicate_ranges = [s["区段ID"] for s in segments if s["可执行作业数"] > 1]
    total_events = sum(int(batch.get("总事件数", 0) or 0) for batch in batches) or len(entries)
    latest_batch = batches[-1] if batches else {}
    summary = {
        "total_events": total_events,
        "late_count": len(entries),
        "late_ratio": latest_batch.get("迟到比例", (len(entries) / total_events) if total_events else 0.0),
        "segment_count": len(segments),
        "covered_segments": len(segments) - len(missing),
        "uncovered_segments": len(missing),
        "job_count": len([item for item in jobs_payload if item["区段"]]),
        "executable_job_count": sum(1 for item in jobs_payload if item["可执行"]),
        "coverage_complete": not missing,
        "duplicate_executable_ranges": duplicate_ranges,
    }
    payload = {
        "policy": policy_name,
        "threshold_percent": threshold.get("percent"),
        "threshold_ratio": threshold.get("ratio"),
        "threshold_source": threshold.get("source"),
        "allowed_lateness": allowed_lateness.get("seconds"),
        "watermark": watermark,
        "entries": entries,
        "bypass": bypass,
        "segments": segments,
        "jobs": jobs_payload,
        "summary": summary,
    }
    document = {
        "format": MANIFEST_FORMAT,
        "format_version": MANIFEST_VERSION,
        "export_id": export_id,
        "exported_at": exported_at,
        "policy": policy_name,
        "policy_id": (policy or {}).get("id"),
        **payload,
        "coverage": {
            "complete": not missing,
            "missing_segments": missing,
            "duplicate_executable_ranges": duplicate_ranges,
        },
        "checksum": manifest_checksum(payload),
    }
    return document


def validate_manifest(
    document: dict[str, Any],
    *,
    known_export_checksums: set[str] | None = None,
    known_import_checksums: set[str] | None = None,
) -> dict[str, Any]:
    """校验清单：覆盖全部异常区段与迟到明细，且每个区段只有一份可执行范围。"""
    issues: list[str] = []
    if not isinstance(document, dict):
        return {
            "ok": False,
            "valid": False,
            "issues": ["清单必须是 JSON 对象"],
            "coverage": {},
            "summary": {},
        }
    if document.get("format") != MANIFEST_FORMAT:
        issues.append(f"文件格式不是 {MANIFEST_FORMAT}")
    if document.get("format_version") != MANIFEST_VERSION:
        issues.append(f"清单版本不受支持：{document.get('format_version')}")

    entries = document.get("entries")
    bypass = document.get("bypass")
    segments = document.get("segments")
    jobs = document.get("jobs")
    for name, value in (
        ("entries", entries), ("bypass", bypass), ("segments", segments), ("jobs", jobs),
    ):
        if not isinstance(value, list):
            issues.append(f"清单缺少数组字段 {name}")
    if issues:
        return {
            "ok": False,
            "valid": False,
            "issues": issues,
            "coverage": {},
            "summary": document.get("summary", {}),
        }

    entry_ids = {str(item.get("事件ID")) for item in entries if isinstance(item, dict)}
    bypass_ids = {str(item.get("事件ID")) for item in bypass if isinstance(item, dict)}
    missing_detail = sorted(entry_ids - bypass_ids)
    orphan_detail = sorted(bypass_ids - entry_ids)
    if missing_detail:
        issues.append(f"迟到明细缺少旁路记录：{', '.join(missing_detail[:5])}")
    if orphan_detail:
        issues.append(f"旁路记录不在迟到明细中：{', '.join(orphan_detail[:5])}")

    job_ids = {item.get("作业ID") for item in jobs if isinstance(item, dict)}
    for segment in segments:
        if not isinstance(segment, dict):
            issues.append("区段记录格式错误")
            continue
        key = segment.get("区段ID", "")
        job_id = segment.get("作业ID")
        if job_id is None or job_id not in job_ids:
            issues.append(f"区段 {key} 缺少补数作业")
            continue
        covering = [
            job for job in jobs
            if isinstance(job, dict)
            and job.get("作业ID") == job_id
            and key in (job.get("区段") or [])
        ]
        if not covering:
            issues.append(f"区段 {key} 的作业未登记区段归属")
        executable = [
            job for job in jobs
            if isinstance(job, dict) and key in (job.get("区段") or []) and job.get("可执行")
        ]
        if not executable:
            issues.append(f"区段 {key} 没有可执行补数作业")
        if len(executable) > 1:
            issues.append(f"区段 {key} 存在 {len(executable)} 个可执行范围")

    segment_count = len(segments)
    failed_segments = {
        segment.get("区段ID")
        for segment in segments
        if isinstance(segment, dict)
        and not any(
            isinstance(job, dict)
            and segment.get("区段ID") in (job.get("区段") or [])
            and job.get("可执行")
            for job in jobs
        )
    }
    covered = segment_count - len(failed_segments)
    coverage = {
        "complete": not failed_segments,
        "segment_count": segment_count,
        "covered_segments": covered,
        "missing_segments": [segment.get("区段ID") for segment in segments
                             if isinstance(segment, dict) and segment.get("区段ID") in failed_segments],
    }
    summary = document.get("summary") or {}
    actual_counts = {
        "late_count": len(entries),
        "segment_count": segment_count,
        "job_count": len(jobs),
    }
    for field, actual in actual_counts.items():
        declared = summary.get(field)
        if declared is not None and int(declared) != actual:
            issues.append(f"清单汇总 {field}={declared} 与实际 {actual} 不一致")

    stored_checksum = document.get("checksum")
    payload = {
        key: document.get(key)
        for key in (
            "policy", "threshold_percent", "threshold_ratio", "threshold_source",
            "allowed_lateness", "watermark", "entries", "bypass", "segments", "jobs", "summary",
        )
    }
    computed_checksum = manifest_checksum(payload)
    if stored_checksum and stored_checksum != computed_checksum:
        issues.append("清单校验和不一致，文件内容可能被修改")
    already_exported = bool(stored_checksum and known_export_checksums and stored_checksum in known_export_checksums)
    already_imported = bool(computed_checksum and known_import_checksums and computed_checksum in known_import_checksums)

    return {
        "ok": not issues,
        "valid": not issues,
        "issues": issues,
        "coverage": coverage,
        "summary": summary,
        "checksum": computed_checksum,
        "previously_exported": already_exported,
        "already_imported": already_imported,
        "duplicate": already_imported,
    }


def parse_manifest_text(content: str) -> dict[str, Any]:
    """解析清单文本（JSON），结构错误抛 ManifestError。"""
    text = (content or "").lstrip("﻿").strip()
    if not text:
        raise ManifestError("导入文件为空")
    try:
        document = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ManifestError(f"清单无法解析：{exc.msg}") from exc
    if not isinstance(document, dict):
        raise ManifestError("清单顶层必须是对象")
    return document


def safe_filename(text: str, default: str = "late_bypass") -> str:
    """生成只含安全字符的下载文件名；中文等字符全部退化成默认名前缀。"""
    base = re.sub(r"[^0-9A-Za-z_.-]+", "_", (text or "").strip()).strip("_")
    if not base or not re.search(r"[0-9A-Za-z]", base):
        base = default
    return base[:80]
