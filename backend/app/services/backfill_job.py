"""补数任务领域服务：管理实时指标的离线补数任务。

数据流角色: watermark_track / late_handler → backfill_job（消费者）

补数任务不自己猜范围，而是读取迟到处理写入的迟到旁路数据与区段登记，
再核对“每个异常区段是否都有可执行作业”：

* 作业按区段归属（``_segment_keys``），范围就是区段的起止位点；
* 同一区段最多一条可执行作业，暂停/关闭/分段的作业不参与覆盖；
* 清单中的作业数量必须与未覆盖区段数一致，否则导出会标记覆盖不完整。
"""
from __future__ import annotations

import time
from typing import Any

from app.services import late_bypass
from app.services.base import BaseModuleService
from app.store import store


class BackfillJobService(BaseModuleService):
    MODULE = "backfill_job"
    ENTITY = "补数作业"
    LIST_FIELDS = ["作业名称", "目标指标", "补数范围", "并行度", "起始位点", "截止位点", "进度比例"]
    REQUIRED_FIELDS = ["作业名称", "目标指标", "补数范围"]
    STATUS_ORDER = ["待执行", "补数中", "已完成", "部分失败"]
    ACTION_RULES = {"启动补数": "待执行", "暂停补数": "补数中"}
    NEGATIVE_ACTIONS = []

    BYPASS_TABLE = "late_bypass"
    SEGMENT_TABLE = "late_segments"

    # ---- 迟到旁路数据读取 ----

    def _late_policies(self) -> list[str]:
        """迟到处理侧实际产生过旁路数据的策略。"""
        policies = {
            row.get("策略") for row in store.aux(self.BYPASS_TABLE) if row.get("策略")
        }
        policies.update(
            row.get("策略") for row in store.aux(self.SEGMENT_TABLE) if row.get("策略")
        )
        return sorted(p for p in policies if p)

    def list_pending_segments(self, policy_name: str | None = None) -> list[dict[str, Any]]:
        """从迟到旁路数据读出待补范围：每个区段一条，附带作业覆盖情况。"""
        segments = [
            segment for segment in store.aux(self.SEGMENT_TABLE)
            if policy_name is None or segment.get("策略") == policy_name
        ]
        bypass = [
            row for row in store.aux(self.BYPASS_TABLE)
            if row.get("状态", "生效") == "生效"
            and (policy_name is None or row.get("策略") == policy_name)
        ]
        per_segment: dict[str, list[dict[str, Any]]] = {}
        for row in bypass:
            per_segment.setdefault(row.get("区段"), []).append(row)

        result = []
        for segment in sorted(segments, key=lambda s: (s.get("源端", ""), s.get("起始时间", 0))):
            key = segment.get("区段ID")
            events = per_segment.get(key, [])
            jobs = self._jobs_for_segment(key)
            executable = [job for job in jobs if self._is_executable(job)]
            start = int(min((row.get("事件时间", 0) for row in events), default=segment.get("起始时间", 0)))
            end = int(max((row.get("事件时间", 0) for row in events), default=segment.get("截止时间", 0)))
            if end <= start:
                end = start + 1
            result.append({
                "segment_id": key,
                "policy": segment.get("策略", ""),
                "source": segment.get("源端", ""),
                "start": start,
                "end": end,
                "range": f"{start}-{end}",
                "late_count": len(events) or int(segment.get("迟到事件数", 0)),
                "version": segment.get("版本", 1),
                "jobs": [
                    {
                        "job_id": job.get("id"),
                        "job_name": job.get("作业名称", ""),
                        "status": job.get("status", ""),
                        "range": job.get("补数范围", ""),
                        "progress": job.get("进度比例", "0%"),
                        "paused": bool(job.get("_paused")),
                        "executable": self._is_executable(job),
                    }
                    for job in jobs
                ],
                "covered": bool(executable),
                "job_id": executable[0].get("id") if executable else None,
            })
        return result

    def _jobs_for_segment(self, segment_id: str) -> list[dict[str, Any]]:
        return [
            job for job in store.rows(self.MODULE)
            if segment_id in (job.get("_segment_keys") or [])
            and not job.get("_superseded_by")
        ]

    @staticmethod
    def _is_executable(job: dict[str, Any]) -> bool:
        return (
            job.get("status") in ("待执行", "补数中")
            and not job.get("_paused")
            and not job.get("_superseded_by")
        )

    def task_console(self) -> dict[str, Any]:
        """补数页面数据：按策略列出区段、作业与覆盖结论。"""
        policies: dict[str, dict[str, Any]] = {}
        for policy in self._late_policies():
            segments = self.list_pending_segments(policy)
            uncovered = [segment["segment_id"] for segment in segments if not segment["covered"]]
            jobs = [
                job for job in store.rows(self.MODULE)
                if job.get("_policy") == policy and not job.get("_superseded_by")
            ]
            policies[policy] = {
                "policy": policy,
                "segments": segments,
                "jobs": [
                    {
                        "job_id": job.get("id"),
                        "job_name": job.get("作业名称", ""),
                        "status": job.get("status", ""),
                        "range": job.get("补数范围", ""),
                        "progress": job.get("进度比例", "0%"),
                        "parallelism": job.get("并行度", ""),
                        "paused": bool(job.get("_paused")),
                        "executable": self._is_executable(job),
                        "segments": job.get("_segment_keys", []),
                        "progress_log": job.get("_progress_log", []),
                    }
                    for job in jobs
                ],
                "coverage": {
                    "segment_count": len(segments),
                    "covered": len(segments) - len(uncovered),
                    "uncovered": uncovered,
                    "complete": not uncovered,
                },
            }
        return {
            "ok": True,
            "policies": policies,
            "pending_jobs": self.list_pending_backfills(),
            "totals": {
                "segments": sum(item["coverage"]["segment_count"] for item in policies.values()),
                "uncovered": sum(len(item["coverage"]["uncovered"]) for item in policies.values()),
            },
        }

    # ---- 与迟到处理共用的一致性校验 ----

    def find_coverage_gaps(self) -> dict[str, Any]:
        """核对每个异常区段是否都有可执行作业，返回缺口清单。"""
        gaps: list[dict[str, Any]] = []
        for policy in self._late_policies():
            for segment in self.list_pending_segments(policy):
                executable = [job for job in segment["jobs"] if job["executable"]]
                if not executable:
                    gaps.append({
                        "policy": policy,
                        "segment_id": segment["segment_id"],
                        "range": segment["range"],
                        "reason": "区段没有可执行补数作业",
                    })
                elif len(executable) > 1:
                    gaps.append({
                        "policy": policy,
                        "segment_id": segment["segment_id"],
                        "range": segment["range"],
                        "reason": f"区段存在 {len(executable)} 个可执行作业",
                    })
        return {
            "ok": not gaps,
            "gaps": gaps,
            "gap_count": len(gaps),
            "complete": not gaps,
        }

    # ---- 作业动作：暂停、恢复、分段 ----

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """在基类状态流转之外维护暂停标记，避免暂停的作业仍被算作可执行。"""
        job = store.find(self.MODULE, entry_id)
        if job is not None and job.get("_superseded_by"):
            return None, "作业已被同区段的新作业替代，不能再执行动作"
        if action == "暂停补数":
            if job is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            job["_paused"] = True
            job["_paused_at"] = time.time()
            job.setdefault("_progress_log", []).append({
                "时间": time.time(),
                "说明": "作业暂停，等待后续迟到事件决定扩大范围还是分段重建",
            })
            entry, message = super().run_action(entry_id, action)
            return entry, "补数作业已暂停，可执行范围暂时不可用"
        if action == "启动补数":
            if job is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            job["_paused"] = False
            return super().run_action(entry_id, action)
        return super().run_action(entry_id, action)

    def split_paused_job(self, job_id: int) -> dict[str, Any]:
        """关闭暂停中的旧作业，按区段重新开一条可执行作业。"""
        job = store.find(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "message": f"补数作业 {job_id} 不存在"}
        if not job.get("_paused"):
            return {"ok": False, "message": "只有暂停中的作业可以分段重建"}
        segment_keys = list(job.get("_segment_keys") or [])
        if len(segment_keys) <= 1:
            return {"ok": False, "message": "作业只覆盖一个区段，无需分段"}
        segment_id = segment_keys[0]
        segment = next(
            (row for row in store.aux(self.SEGMENT_TABLE) if row.get("区段ID") == segment_id),
            None,
        )
        if segment is None:
            return {"ok": False, "message": f"区段 {segment_id} 不存在，无法分段"}

        job["_paused"] = False
        job["status"] = "部分失败"
        job["abnormal"] = True
        job["_closed_reason"] = "paused_split"
        new_job_id = max(
            (int(row.get("id", 0)) for row in store.rows(self.MODULE)), default=0
        ) + 1
        job["_superseded_by"] = new_job_id
        start, end = late_bypass.segment_range(segment)
        new_job = {
            "id": new_job_id,
            "作业名称": f"late_backfill_split_{segment.get('源端', '')}_{start}_{end}",
            "目标指标": job.get("目标指标", f"late_data_{segment.get('源端', '')}"),
            "补数范围": f"{start}-{end}",
            "并行度": job.get("并行度", "1"),
            "起始位点": start,
            "截止位点": end,
            "进度比例": "0%",
            "status": "待执行",
            "pending": True,
            "abnormal": False,
            "_policy": job.get("_policy", ""),
            "_segment_keys": [segment_id],
            "_source": segment.get("源端", ""),
            "_covered_events": int(segment.get("迟到事件数", 0)),
            "_late_count": int(segment.get("迟到事件数", 0)),
            "_executable_range": f"{start}-{end}",
            "_split_from": job_id,
            "_progress_log": [{
                "时间": time.time(),
                "说明": f"由暂停作业 {job_id} 分段重建",
                "范围": f"{start}-{end}",
            }],
        }
        store.rows(self.MODULE).append(new_job)
        return {
            "ok": True,
            "message": "暂停作业已关闭并新建分段作业",
            "closed_job_id": job_id,
            "new_job_id": new_job_id,
        }

    def summarize_executable_ranges(self) -> dict[str, Any]:
        """给出每个区段当前唯一可执行范围，用于页面与导出对账。"""
        ranges: dict[str, dict[str, Any]] = {}
        duplicates: list[str] = []
        for segment in self.list_pending_segments():
            executable = [job for job in segment["jobs"] if job["executable"]]
            if len(executable) > 1:
                duplicates.append(segment["segment_id"])
            ranges[segment["segment_id"]] = {
                "range": executable[0]["range"] if executable else None,
                "job_id": executable[0]["job_id"] if executable else None,
                "executable_count": len(executable),
            }
        return {"ranges": ranges, "duplicate_segments": duplicates}

    # ---- 导出与回传校验 ----

    def export_task_manifest(self, policy_name: str | None = None) -> dict[str, Any]:
        """导出补数任务清单（含迟到旁路明细与区段覆盖），供其它页面接收。

        导出走与迟到处理同一个清单构建器，但不重复登记导出台账。
        """
        from app.services import service_for

        late_service = service_for("late_handler")
        if late_service is None:
            return {"ok": False, "error": "迟到处理服务不可用"}
        policy = policy_name
        if policy is None:
            policies = self._late_policies()
            if not policies:
                return {"ok": False, "error": "暂无迟到旁路数据，无法导出补数任务清单"}
            policy = policies[0]
        manifest = late_service.build_manifest_document(
            policy, export_id=None, record_export=False,
        )
        if not manifest.get("ok"):
            return manifest
        segments = self.list_pending_segments(policy)
        gaps = self.find_coverage_gaps()
        manifest["document"]["backfill_segments"] = segments
        manifest["document"]["backfill_coverage"] = {
            "segment_count": len(segments),
            "covered": sum(1 for segment in segments if segment["covered"]),
            "uncovered": [segment["segment_id"] for segment in segments if not segment["covered"]],
            "complete": not gaps["gaps"],
            "gap_count": gaps["gap_count"],
        }
        manifest["jobs"] = self.export_task_listing(policy)
        manifest["job_count"] = len(manifest["jobs"])
        manifest["coverage_gaps"] = gaps
        return manifest

    def export_task_listing(self, policy_name: str | None = None) -> list[dict[str, Any]]:
        """补数任务清单行：一行一个作业，附带它覆盖的区段与覆盖结论。

        作业数量必须与未覆盖区段数一致即“每个异常区段一条可执行作业”，
        因此这里以区段为主线输出，作业未覆盖的区段也会出现在清单里并标记。
        """
        segments = self.list_pending_segments(policy_name)
        rows: list[dict[str, Any]] = []
        for segment in segments:
            executable = [job for job in segment["jobs"] if job["executable"]]
            rows.append({
                "区段": segment["segment_id"],
                "策略": segment["policy"],
                "源端": segment["source"],
                "补数范围": segment["range"],
                "迟到事件数": segment["late_count"],
                "区段版本": segment["version"],
                "作业ID": executable[0]["job_id"] if executable else "",
                "作业名称": executable[0]["job_name"] if executable else "",
                "作业状态": executable[0]["status"] if executable else "未覆盖",
                "进度比例": executable[0]["progress"] if executable else "0%",
                "已覆盖": "是" if executable else "否",
            })
        return rows

    def verify_task_manifest(self, content: str) -> dict[str, Any]:
        """校验补数任务清单覆盖全部异常区段和迟到明细。"""
        from app.services import service_for

        late_service = service_for("late_handler")
        if late_service is None:
            return {"ok": False, "valid": False, "issues": ["迟到处理服务不可用"]}
        validation = late_service.verify_manifest_content(content, policy_name=None)
        document: dict[str, Any]
        try:
            document = late_bypass.parse_manifest_text(content)
        except late_bypass.ManifestError as exc:
            validation["issues"].append(str(exc))
            validation["ok"] = False
            validation["valid"] = False
            return validation

        segments = document.get("segments") or []
        jobs = document.get("jobs") or []
        segment_ids = {segment.get("区段ID") for segment in segments if isinstance(segment, dict)}
        job_segments = set()
        for job in jobs:
            if isinstance(job, dict):
                job_segments.update(job.get("区段") or [])
        missing_jobs = sorted(segment_ids - job_segments)
        if missing_jobs:
            validation["issues"].append(
                f"补数任务清单缺少区段作业：{', '.join(str(x) for x in missing_jobs[:5])}",
            )
        coverage_payload = document.get("backfill_coverage")
        if isinstance(coverage_payload, dict) and not coverage_payload.get("complete", True):
            validation["issues"].append(
                "补数任务清单标记了未覆盖区段："
                + ", ".join(str(x) for x in coverage_payload.get("uncovered", [])[:5]),
            )
        executable_per_segment: dict[str, int] = {}
        for job in jobs:
            if not isinstance(job, dict) or not job.get("可执行"):
                continue
            for key in job.get("区段") or []:
                executable_per_segment[key] = executable_per_segment.get(key, 0) + 1
        duplicated = [key for key, count in executable_per_segment.items() if count > 1]
        if duplicated:
            validation["issues"].append(
                f"区段存在多个可执行范围：{', '.join(duplicated[:5])}",
            )
        validation["ok"] = not validation["issues"]
        validation["valid"] = validation["ok"]
        validation["backfill"] = {
            "segment_count": len(segment_ids),
            "job_count": len(jobs),
            "missing_jobs": missing_jobs,
            "duplicate_ranges": duplicated,
        }
        return validation

    # ---- 跨模块数据管理 ----

    def validate_backfill_range(
        self, target_metric: str, start_offset: str, end_offset: str,
    ) -> dict[str, Any]:
        """校验补数范围的合法性：起止位点不能倒置、范围不能为0。"""
        issues = []
        try:
            start = int(start_offset)
        except (ValueError, TypeError):
            issues.append("起始位点必须为整数")
            start = 0
        try:
            end = int(end_offset)
        except (ValueError, TypeError):
            issues.append("截止位点必须为整数")
            end = 0

        if start > end:
            issues.append("起始位点不能大于截止位点")
        if start == end:
            issues.append("补数范围不能为空（起始位点等于截止位点）")

        # 检查是否与已有任务重复
        for r in store.rows(self.MODULE):
            if r.get("目标指标") == target_metric and r.get("status") in ("补数中",):
                existing_start = str(r.get("起始位点", ""))
                existing_end = str(r.get("截止位点", ""))
                if existing_start == start_offset and existing_end == end_offset:
                    issues.append(f"指标「{target_metric}」已有补数任务「{r.get('作业名称')}」在处理相同范围")

        return {
            "ok": len(issues) == 0,
            "target": target_metric,
            "range": f"{start_offset}-{end_offset}",
            "range_size": end - start if isinstance(start, int) and isinstance(end, int) else 0,
            "issues": issues,
        }

    def estimate_parallelism(self, range_size: int, data_rate: float = 1000.0) -> dict[str, Any]:
        """根据数据范围自动推算最优并行度。"""
        # 每个分区建议处理 10000 条，最少 1 并行
        optimal = max(1, min(32, (range_size * data_rate) // 10000))
        return {
            "range_size": range_size,
            "estimated_events": int(range_size * data_rate),
            "recommended_parallelism": optimal,
            "estimated_duration_sec": (range_size * data_rate) / (optimal * 100),
        }

    def track_progress(self, job_name: str) -> dict[str, Any]:
        """跟踪补数任务进度并计算完成百分比。"""
        job = None
        for r in store.rows(self.MODULE):
            if r.get("作业名称") == job_name:
                job = r
                break
        if job is None:
            return {"ok": False, "error": f"补数任务「{job_name}」不存在"}

        try:
            start = int(job.get("起始位点", "0"))
            end = int(job.get("截止位点", "0"))
            current = int(job.get("进度比例", "0").replace("%", ""))
        except (ValueError, TypeError):
            start, end, current = 0, 100, 0

        total_range = max(end - start, 1)
        progress_pct = min(100, max(0, current))

        # 模拟进度推进
        if job.get("status") == "补数中" and not job.get("_paused"):
            progress_pct = min(100, current + 15)
            job["进度比例"] = f"{progress_pct}%"
            if progress_pct >= 100:
                job["status"] = "已完成"
                job["pending"] = False

                # 验证补数结果：与聚合落库对比
                self._verify_with_aggregate_sink(job)

        return {
            "ok": True,
            "job": job_name,
            "status": job.get("status", ""),
            "progress_pct": progress_pct,
            "total_range": total_range,
            "completed_range": int(total_range * progress_pct / 100),
        }

    def _verify_with_aggregate_sink(self, job: dict[str, Any]) -> None:
        """补数完成后与聚合落库进行交叉验证。"""
        target = job.get("目标指标", "")
        matched = sum(
            1 for r in store.rows("aggregate_sink")
            if target in str(r.get("源指标集", ""))
        )
        if matched > 0:
            job["_verified_with_aggregate"] = matched
            job["_verified_at"] = time.time()

    def deduplicate_backfill_jobs(self) -> dict[str, Any]:
        """去重：移除目标指标相同的重复补数任务，保留最新。"""
        grouped: dict[str, list[dict[str, Any]]] = {}
        for r in store.rows(self.MODULE):
            metric = r.get("目标指标", "")
            if metric and r.get("id", 0) < 1000:
                grouped.setdefault(metric, []).append(r)

        deduplicated = 0
        for metric, jobs in grouped.items():
            if len(jobs) > 1:
                jobs.sort(key=lambda j: j.get("id", 0), reverse=True)
                for j in jobs[1:]:
                    if j.get("status") in ("待执行",):
                        j["status"] = "已完成"
                        j["_deduplicated"] = True
                        deduplicated += 1

        return {
            "ok": True,
            "metrics_with_duplicates": sum(1 for jobs in grouped.values() if len(jobs) > 1),
            "deduplicated_jobs": deduplicated,
        }

    def list_pending_backfills(self) -> list[dict[str, Any]]:
        """列出所有待执行和补数中的任务（暂停的单独标注，不算可执行）。"""
        pending = [
            r for r in store.rows(self.MODULE)
            if r.get("status") in ("待执行", "补数中") and not r.get("_superseded_by")
        ]
        result = []
        for j in pending:
            entry = {
                "job_id": j.get("id"),
                "job_name": j.get("作业名称", ""),
                "target": j.get("目标指标", ""),
                "range": j.get("补数范围", ""),
                "parallelism": j.get("并行度", ""),
                "progress": j.get("进度比例", "0%"),
                "status": j.get("status", ""),
                "paused": bool(j.get("_paused")),
                "executable": self._is_executable(j),
                "segments": j.get("_segment_keys", []),
            }
            # 检查水位追踪关联
            for wt in store.rows("watermark_track"):
                if wt.get("数据源") in str(j.get("目标指标", "")):
                    entry["watermark_status"] = wt.get("status", "")
                    break
            result.append(entry)
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["pending_jobs"] = sum(1 for r in rows if r.get("status") == "待执行")
        base["running_jobs"] = sum(1 for r in rows if r.get("status") == "补数中")
        base["completed_jobs"] = sum(1 for r in rows if r.get("status") == "已完成")
        base["partial_failures"] = sum(1 for r in rows if r.get("status") == "部分失败")
        base["deduplicated"] = sum(1 for r in rows if r.get("_deduplicated"))
        base["late_segments"] = len(store.aux(self.SEGMENT_TABLE))
        base["uncovered_segments"] = sum(
            1 for segment in self.list_pending_segments() if not segment["covered"]
        )
        return base
