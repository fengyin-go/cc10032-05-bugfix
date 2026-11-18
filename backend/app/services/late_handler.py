"""迟到处理领域服务：定义流式计算中迟到数据的处理策略。

数据流角色: watermark_track → late_handler（生产者）→ backfill_job（消费者）

一次迟到处理由三部分组成，任何一步失败都整批回滚，不改动已有旁路数据：

1. 读取迟到事件（CSV/JSON，或其它页面回传的同源导出清单）；
2. 用策略阈值判断是否越过阈值（阈值缺失回退策略默认值 5%，不猜测小数）；
3. 为每个未覆盖的迟到区段生成一条可执行补数作业，重复越过阈值只追加进度。

增强能力: 迟到计数统计、阈值报警、旁路输出调度、迟到数据标记、与补数联动触发、
文件导入导出、批量回执、同源清单回传校验、重复导入幂等。
"""
from __future__ import annotations

import copy
import time
from typing import Any

from app.services import late_bypass
from app.services.base import BaseModuleService
from app.store import store


class LateHandlerService(BaseModuleService):
    MODULE = "late_handler"
    ENTITY = "处理策略"
    LIST_FIELDS = ["策略名称", "允许延迟", "迟到动作", "旁路输出", "计数方式", "阈值百分比", "标记字段"]
    REQUIRED_FIELDS = ["策略名称", "允许延迟", "迟到动作"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用策略": "草稿", "停用策略": "已启用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    BYPASS_TABLE = "late_bypass"
    SEGMENT_TABLE = "late_segments"
    BATCH_TABLE = "late_batches"
    IMPORT_TABLE = "late_imports"
    EXPORT_TABLE = "late_exports"

    # ---- 策略解析 ----

    def find_policy(self, policy_name: str) -> dict[str, Any] | None:
        return next(
            (row for row in store.rows(self.MODULE) if row.get("策略名称") == policy_name),
            None,
        )

    def resolve_policy_settings(self, policy_name: str) -> dict[str, Any]:
        """解析策略的阈值与允许延迟，缺失字段回退策略默认值。"""
        policy = self.find_policy(policy_name)
        threshold = late_bypass.parse_threshold(policy)
        lateness = late_bypass.parse_allowed_lateness(policy)
        return {
            "policy": policy_name,
            "policy_id": (policy or {}).get("id"),
            "found": policy is not None,
            "threshold": threshold,
            "allowed_lateness": lateness,
            "迟到动作": (policy or {}).get("迟到动作", "bypass"),
            "标记字段": (policy or {}).get("标记字段", "_late_mark"),
        }

    # ---- 迟到统计（保留原有接口） ----

    def count_late_events(
        self, policy_name: str, events: list[dict[str, Any]], watermark: float,
    ) -> dict[str, Any]:
        """统计迟到事件数量并计算迟到率；边界相等（== cutoff）算准时。"""
        settings = self.resolve_policy_settings(policy_name)
        allowed_sec = float(settings["allowed_lateness"]["seconds"])

        cutoff = watermark - allowed_sec
        late = [e for e in events if e.get("event_time", 0) < cutoff]
        on_time = len(events) - len(late)
        late_ratio = len(late) / max(len(events), 1)

        if late:
            lats = [e["event_time"] for e in late if "event_time" in e]
            late_dist = {
                "min_ts": min(lats) if lats else 0,
                "max_ts": max(lats) if lats else 0,
                "count": len(late),
            }
        else:
            late_dist = None

        return {
            "ok": True,
            "policy": policy_name,
            "total_events": len(events),
            "on_time": on_time,
            "late_count": len(late),
            "late_ratio": round(late_ratio, 4),
            "allowed_lateness": allowed_sec,
            "watermark": watermark,
            "late_distribution": late_dist,
        }

    def check_threshold_trigger(
        self, policy_name: str, late_ratio: float,
    ) -> dict[str, Any]:
        """检查迟到率是否超过阈值，触发旁路输出。

        阈值取自策略「阈值百分比」，按百分数解析（5 → 5%）。
        配置为空或不是 0-100 的数值时回退策略默认值 5%，而不是把空值当成
        某个猜测的小比例；只有严格大于阈值才触发。
        """
        settings = self.resolve_policy_settings(policy_name)
        threshold = settings["threshold"]
        triggered = late_ratio > threshold["ratio"]

        action_taken = "none"
        if triggered:
            bypass_action = settings["迟到动作"]
            if bypass_action == "bypass":
                action_taken = "bypass_to_side_output"
            elif bypass_action == "drop":
                action_taken = "drop_late"
            elif bypass_action == "retry":
                action_taken = "retry_with_adjusted_watermark"

        return {
            "ok": True,
            "policy": policy_name,
            "late_ratio": late_ratio,
            "threshold": threshold["ratio"],
            "threshold_percent": threshold["percent"],
            "threshold_source": threshold["source"],
            "threshold_fallback": threshold["fallback"],
            "triggered": triggered,
            "action": settings["迟到动作"],
            "action_taken": action_taken,
        }

    def trigger_backfill_for_late(
        self, policy_name: str, late_count: int, source_name: str,
    ) -> dict[str, Any]:
        """按迟到数量补齐一个区段的补数作业（兼容旧调用的薄封装）。

        正常入口是 :meth:`ingest_late_events`，它会按事件时间切出真实区段。
        """
        if late_count <= 0:
            return {"ok": True, "triggered": False, "reason": "无迟到数据"}

        segment = {
            "区段ID": f"{source_name}|{late_count}",
            "源端": source_name,
            "起始时间": 0.0,
            "截止时间": float(late_count),
            "迟到事件数": late_count,
            "事件ID列表": [],
            "显式区段": None,
        }
        job, created = self._ensure_job_for_segment(
            policy_name=policy_name,
            segment=segment,
            batch_id="manual",
            settings=self.resolve_policy_settings(policy_name),
        )
        return {
            "ok": True,
            "triggered": True,
            "policy": policy_name,
            "late_count": late_count,
            "backfill_job_id": job.get("id"),
            "backfill_job_name": job.get("作业名称"),
            "job_created": created,
        }

    def list_late_statistics(self) -> list[dict[str, Any]]:
        """列出所有迟到处理策略的统计信息（含实际采用的阈值与迟到比例）。"""
        result = []
        for r in store.rows(self.MODULE):
            if r.get("id", 0) >= 100:
                continue
            name = r.get("策略名称", "")
            settings = self.resolve_policy_settings(name)
            batches = [
                b for b in store.aux(self.BATCH_TABLE) if b.get("策略") == name
            ]
            latest = batches[-1] if batches else {}
            linked_jobs = sum(
                1 for b in store.rows("backfill_job")
                if b.get("_policy") == name
                and b.get("status") in ("待执行", "补数中")
                and not b.get("_paused")
                and not b.get("_superseded_by")
            )
            result.append({
                "policy": name,
                "policy_id": r.get("id"),
                "action": r.get("迟到动作", ""),
                "threshold": settings["threshold"]["percent"],
                "threshold_percent": settings["threshold"]["percent"],
                "threshold_ratio": settings["threshold"]["ratio"],
                "threshold_source": settings["threshold"]["source"],
                "threshold_fallback": settings["threshold"]["fallback"],
                "allowed_lateness": settings["allowed_lateness"]["seconds"],
                "latest_late_ratio": latest.get("迟到比例", 0.0),
                "latest_triggered": latest.get("触发", False),
                "batches": len(batches),
                "status": r.get("status", ""),
                "linked_backfills": linked_jobs,
            })
        return result

    # ---- 文件入口：读取迟到事件 ----

    def ingest_late_events(
        self,
        policy_name: str,
        content: str,
        filename: str = "",
        *,
        watermark: float | None = None,
        segment_gap: float | None = None,
        paused_strategy: str = "extend",
    ) -> dict[str, Any]:
        """读取一份迟到事件文件，判断阈值并生成补数任务清单。

        整份文件按事务处理：解析、校验、旁路写入和作业创建全部成功才提交；
        任一步失败都回滚到导入前的状态，并原样保留上传文件（调用方可用同一份
        文件重试）。
        """
        try:
            return self._ingest_transaction(
                policy_name=policy_name,
                content=content,
                filename=filename,
                watermark=watermark,
                segment_gap=segment_gap,
                paused_strategy=paused_strategy,
            )
        except late_bypass.ManifestError as exc:
            return {
                "ok": False,
                "message": f"导入失败：{exc}",
                "filename": filename,
                "rolled_back": True,
                "committed": False,
            }

    def _ingest_transaction(
        self,
        *,
        policy_name: str,
        content: str,
        filename: str,
        watermark: float | None,
        segment_gap: float | None,
        paused_strategy: str,
    ) -> dict[str, Any]:
        settings = self.resolve_policy_settings(policy_name)
        if not settings["found"]:
            raise late_bypass.ManifestError(f"迟到处理策略「{policy_name}」不存在")

        kind, raw_rows, meta = late_bypass.parse_import_content(content, filename)
        if kind == "manifest":
            return self.import_manifest(
                policy_name,
                content,
                filename=filename,
                paused_strategy=paused_strategy,
            )

        digest = late_bypass.content_digest(content)
        duplicate = next(
            (
                batch for batch in store.aux(self.BATCH_TABLE)
                if batch.get("策略") == policy_name and batch.get("文件摘要") == digest
            ),
            None,
        )
        if duplicate is not None:
            return {
                "ok": True,
                "duplicate": True,
                "committed": False,
                "message": "同一份文件已导入过，本次不重复入库",
                "batch": copy.deepcopy(duplicate),
                "settings": self._settings_payload(settings),
            }

        default_source = str(meta.get("source") or policy_name or "unknown_source")
        events = late_bypass.normalize_events(raw_rows, default_source=default_source)
        effective_watermark = self._resolve_watermark(
            events, watermark, meta, policy_name=policy_name,
        )
        policy_row = self.find_policy(policy_name) or {}
        gap = late_bypass.parse_segment_gap(
            segment_gap if segment_gap is not None else policy_row.get("计数方式"),
        )
        classified = late_bypass.classify_events(
            events,
            effective_watermark,
            float(settings["allowed_lateness"]["seconds"]),
        )
        threshold = settings["threshold"]
        triggered = classified["late_ratio"] > threshold["ratio"]

        # 先算后写：只有越过阈值且有迟到事件才生成旁路数据和作业。
        counts_before = (
            len(store.aux(self.BATCH_TABLE)),
            len(store.aux(self.BYPASS_TABLE)),
        )
        aux_snapshot = store.snapshot_aux(self.SEGMENT_TABLE)
        jobs_before = copy.deepcopy(store.rows("backfill_job"))
        try:
            batch_id = self._next_batch_id(policy_name)
            new_bypass: list[dict[str, Any]] = []
            touched_segments: list[dict[str, Any]] = []
            job_results: list[dict[str, Any]] = []
            if triggered:
                segments = late_bypass.segment_late_events(classified["late"], gap)
                for segment in segments:
                    merged = self._merge_segment(segment, policy_name, batch_id, filename)
                    touched_segments.append(merged)
                    new_bypass.extend(
                        self._build_bypass_rows(
                            merged, policy_name, settings, batch_id, filename,
                        )
                    )
            existing_keys = {row.get("_key") for row in store.aux(self.BYPASS_TABLE)}
            fresh_rows = [row for row in new_bypass if row.get("_key") not in existing_keys]
            # 同一事件只保留一条旁路记录；已存在的旁路记录按区段刷成最新版本，
            # 重复导入同一批事件不再新增记录，也不会重复计入作业覆盖数。
            for row in fresh_rows:
                store.aux(self.BYPASS_TABLE).append(row)
            self._refresh_bypass_versions(policy_name)
            covered_by_segment: dict[str, int] = {}
            for segment in touched_segments:
                covered_by_segment[segment["区段ID"]] = sum(
                    1 for row in store.aux(self.BYPASS_TABLE)
                    if row.get("策略") == policy_name and row.get("区段") == segment["区段ID"]
                )
            for segment in touched_segments:
                job, created = self._ensure_job_for_segment(
                    policy_name=policy_name,
                    segment=segment,
                    batch_id=batch_id,
                    settings=settings,
                    paused_strategy=paused_strategy,
                    covered_events=covered_by_segment.get(segment["区段ID"], 0),
                )
                job_results.append({
                    "job_id": job.get("id"),
                    "job_name": job.get("作业名称", ""),
                    "created": created,
                    "range": job.get("补数范围", ""),
                    "status": job.get("status", ""),
                })

            receipt = self._build_batch_receipt(
                batch_id=batch_id,
                policy_name=policy_name,
                settings=settings,
                filename=filename,
                digest=digest,
                filename_kind=kind,
                total_events=classified["total_events"],
                late_count=classified["late_count"],
                late_ratio=classified["late_ratio"],
                watermark=effective_watermark,
                triggered=triggered,
                segments=touched_segments,
                job_results=job_results,
                new_bypass_count=len(new_bypass),
                duplicate=False,
            )
            store.aux(self.BATCH_TABLE).append(receipt)
            self._enforce_single_executable()
        except Exception:
            # 回滚到导入前：旁路、区段、作业、回执都不留半成品。
            self._truncate_aux(self.BATCH_TABLE, counts_before[0])
            self._truncate_aux(self.BYPASS_TABLE, counts_before[1])
            store.restore_aux(aux_snapshot)
            store._tables["backfill_job"] = jobs_before
            raise

        return {
            "ok": True,
            "duplicate": False,
            "committed": True,
            "message": "已越过阈值并生成补数任务清单"
            if triggered else "未越过阈值，仅更新迟到统计",
            "batch": copy.deepcopy(receipt),
            "settings": self._settings_payload(settings),
            "segments": [self._segment_payload(s) for s in touched_segments],
            "jobs": job_results,
        }

    @staticmethod
    def _truncate_aux(table: str, size: int) -> None:
        rows = store.aux(table)
        del rows[size:]

    def _resolve_watermark(
        self,
        events: list[dict[str, Any]],
        watermark: float | None,
        meta: dict[str, Any],
        policy_name: str | None = None,
    ) -> float:
        """确定本次判定使用的水位。

        顺序：显式水位 → 文件声明的水位 → 该策略上一批已知水位 → 事件时间最大值。

        补报文件通常只有事件、不带水位；如果直接拿文件里的最大事件时间当水位，
        整段补报事件都会被判成准时，迟到旁路数据因此少一段。沿用策略上一次的
        水位才符合“补报”的语义：它们本来就是在水位之后才到达的事件。
        """
        candidates: list[float] = []
        if watermark is not None:
            candidates.append(late_bypass.parse_timestamp(watermark, "水位"))
        raw_meta = meta.get("watermark")
        if raw_meta is not None and str(raw_meta).strip() != "":
            candidates.append(late_bypass.parse_timestamp(raw_meta, "水位"))
        candidates.extend(e["watermark"] for e in events if e.get("watermark") is not None)
        if candidates:
            return max(candidates + [max(e["event_time"] for e in events)])

        if policy_name:
            known = [
                float(batch.get("水位"))
                for batch in self.list_batches(policy_name)
                if batch.get("水位")
            ]
            if known:
                # 已知水位一定不低于本批事件时间，否则本批事件根本不是“补报”。
                return max(max(known), max(e["event_time"] for e in events))
        return max(e["event_time"] for e in events)

    def _next_batch_id(self, policy_name: str) -> str:
        existing = len(store.aux(self.BATCH_TABLE)) + 1
        return f"LB-{policy_name}-{existing:04d}"

    def _merge_segment(
        self,
        segment: dict[str, Any],
        policy_name: str,
        batch_id: str,
        filename: str,
        preferred_key: str | None = None,
    ) -> dict[str, Any]:
        """把新切出的区段与已有区段对齐。

        显式区段号相同、或调用方指定了回传文件里的区段号时，直接按该区段号
        归并；否则按源端 + 时间重叠（含切分间隔）归并。归并时范围取并集、
        版本递增，同一段迟到事件因此只有一个区段身份。
        """
        registry = store.aux(self.SEGMENT_TABLE)
        gap = float(segment.get("切分间隔") or late_bypass.DEFAULT_SEGMENT_GAP_SEC)
        match = None
        for existing in registry:
            if existing.get("策略") != policy_name:
                continue
            if preferred_key is not None and existing.get("区段ID") == preferred_key:
                match = existing
                break
            if existing.get("源端") != segment["源端"]:
                continue
            if segment.get("显式区段") is not None and existing.get("显式区段") == segment.get("显式区段"):
                match = existing
                break
            overlaps = (
                segment["起始时间"] <= float(existing.get("截止时间", 0)) + gap
                and float(existing.get("起始时间", 0)) <= segment["截止时间"] + gap
            )
            if overlaps:
                match = existing
                break

        if match is None:
            record = {
                "区段ID": preferred_key or segment["区段ID"],
                "源端": segment["源端"],
                "起始时间": segment["起始时间"],
                "截止时间": segment["截止时间"],
                "迟到事件数": int(segment["迟到事件数"]),
                "版本": 1,
                "策略": policy_name,
                "显式区段": segment.get("显式区段"),
                "首次批次": batch_id,
                "最近批次": batch_id,
                "来源文件": [filename] if filename else [],
                "事件ID列表": list(segment.get("事件ID列表", [])),
                "事件列表": copy.deepcopy(segment.get("事件列表") or []),
                "状态": "生效",
            }
            registry.append(record)
            return record

        # 已存在：范围取并集，版本递增（同一区段的第二种文件边界）。
        match["起始时间"] = min(float(match["起始时间"]), segment["起始时间"])
        match["截止时间"] = max(float(match["截止时间"]), segment["截止时间"])
        match["版本"] = int(match.get("版本", 1)) + 1
        match["最近批次"] = batch_id
        known = match.setdefault("事件ID列表", [])
        known_events = match.setdefault("事件列表", [])
        known_times = {item.get("event_id"): item.get("event_time") for item in known_events}
        for event in segment.get("事件列表") or []:
            if event["event_id"] not in known:
                known.append(event["event_id"])
            if event["event_id"] in known_times:
                # 同一事件在新文件里的时间以最新文件为准。
                for item in known_events:
                    if item.get("event_id") == event["event_id"]:
                        item["event_time"] = event["event_time"]
                        item["watermark"] = event.get("watermark", item.get("watermark", 0))
                        break
            else:
                known_events.append(copy.deepcopy(event))
                known_times[event["event_id"]] = event.get("event_time")
        # 事件数按去重后的事件列表重算：同一事件在第二份文件里再出现不算新增。
        match["迟到事件数"] = len(known) or int(match.get("迟到事件数", 0))
        files = match.setdefault("来源文件", [])
        if filename and filename not in files:
            files.append(filename)
        return match

    def _build_bypass_rows(
        self,
        segment: dict[str, Any],
        policy_name: str,
        settings: dict[str, Any],
        batch_id: str,
        filename: str,
    ) -> list[dict[str, Any]]:
        """按区段生成旁路数据行；版本跟随区段版本，重复事件由 _key 去重。

        事件时间取每条迟到事件自己的时间，不能用区段起点代替——否则整个区段
        的明细会被压成一个时间点，导出后无法与原始事件对账。
        """
        version = int(segment.get("版本", 1))
        # 事件时间一律取区段登记的迟到明细；只有旧数据缺少明细时才退回区段起点。
        segment_events = segment.get("事件列表") or [
            {"event_id": event_id, "event_time": segment["起始时间"], "watermark": segment.get("水位", 0)}
            for event_id in segment.get("事件ID列表", [])
        ]
        rows: list[dict[str, Any]] = []
        for event in segment_events:
            event_id = event["event_id"]
            # 去重键带源端：不同源端可以有同名事件，不能互相覆盖。
            key = f"{policy_name}|{segment['源端']}|{event_id}"
            rows.append({
                "_key": key,
                "旁路ID": f"{key}|v{version}",
                "事件ID": event_id,
                "源端": segment["源端"],
                "事件时间": event.get("event_time", segment["起始时间"]),
                "水位": float((event.get("watermark") or segment.get("水位") or 0)),
                "区段": segment["区段ID"],
                "批次": batch_id,
                "版本": version,
                "策略": policy_name,
                "迟到动作": settings["迟到动作"],
                "标记字段": settings["标记字段"],
                "状态": "生效",
                "来源文件": filename,
                "导入时间": time.time(),
            })
        return rows

    # ---- 补数作业联动 ----

    def _ensure_job_for_segment(
        self,
        *,
        policy_name: str,
        segment: dict[str, Any],
        batch_id: str,
        settings: dict[str, Any],
        paused_strategy: str = "extend",
        covered_events: int = 0,
    ) -> tuple[dict[str, Any], bool]:
        """同一迟到范围只保留一条可执行作业。

        * 已有待执行/补数中作业：扩展范围并追加进度，不新建作业；
        * 已有已暂停作业：``extend`` 策略下恢复并扩展原作业，``split`` 策略下
          关闭旧作业后新建分段作业，两种策略都保证页面只剩一份可执行范围。
        """
        start, end = late_bypass.segment_range(segment)
        source = segment["源端"]
        target = f"late_data_{source}"
        key = segment["区段ID"]

        candidates = [
            job for job in store.rows("backfill_job")
            if job.get("_policy") == policy_name and not job.get("_superseded_by")
            and key in (job.get("_segment_keys") or [])
        ]
        active = [
            job for job in candidates
            if job.get("status") in ("待执行", "补数中") and not job.get("_paused")
        ]
        paused = [job for job in candidates if job.get("_paused")]

        if active:
            job = active[0]
            self._extend_job(job, start, end, batch_id, covered_events, segment)
            # 同一区段多余的可执行作业收回，保证只有一份。
            for extra in active[1:]:
                extra["status"] = "部分失败"
                extra["_superseded_by"] = job.get("id")
                extra["_closed_reason"] = "duplicate_range"
            return job, False

        if paused:
            job = paused[0]
            if paused_strategy == "split":
                # 分段：关闭旧作业，新段一条可执行作业；旧作业此前覆盖的其它区段
                # 若没有别的可执行作业，也要补一条承接作业，不能留下无主区段。
                previous_keys = list(job.get("_segment_keys") or [])
                job["_paused"] = False
                job["status"] = "部分失败"
                job["abnormal"] = True
                job["_closed_reason"] = "paused_split"
                new_job = self._create_job(policy_name, segment, start, end, batch_id, covered_events, settings)
                job["_superseded_by"] = new_job["id"]
                carried = [
                    key for key in previous_keys
                    if key != segment["区段ID"] and not self._segment_has_executable(key, exclude={job.get("id")})
                ]
                if carried:
                    self._carry_over_segments(job, carried, batch_id, new_job)
                return new_job, True
            job["_paused"] = False
            job["status"] = "待执行"
            job["pending"] = True
            self._extend_job(job, start, end, batch_id, covered_events, segment,
                             reason="暂停中扩大范围并恢复执行")
            return job, False

        job = self._create_job(policy_name, segment, start, end, batch_id, covered_events, settings)
        return job, True

    def _create_job(
        self,
        policy_name: str,
        segment: dict[str, Any],
        start: str,
        end: str,
        batch_id: str,
        covered_events: int,
        settings: dict[str, Any],
    ) -> dict[str, Any]:
        job = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows("backfill_job")), default=0
            ) + 1,
            "作业名称": f"late_backfill_{segment['源端']}_{start}_{end}",
            "目标指标": f"late_data_{segment['源端']}",
            "补数范围": f"{start}-{end}",
            "并行度": str(min(max(covered_events, 1), 8)),
            "起始位点": start,
            "截止位点": end,
            "进度比例": "0%",
            "status": "待执行",
            "pending": True,
            "abnormal": False,
            "_policy": policy_name,
            "_triggered_by": policy_name,
            "_triggered_by_batch": batch_id,
            "_segment_keys": [segment["区段ID"]],
            "_source": segment["源端"],
            "_late_count": covered_events,
            "_covered_events": covered_events,
            "_executable_range": f"{start}-{end}",
            "_progress_log": [{
                "批次": batch_id,
                "时间": time.time(),
                "新增事件": covered_events,
                "范围": f"{start}-{end}",
                "说明": "首次越过阈值创建作业",
            }],
            "_late_threshold_percent": settings["threshold"]["percent"],
            "_late_threshold_source": settings["threshold"]["source"],
        }
        store.rows("backfill_job").append(job)
        return job

    def _segment_coverage(self, job: dict[str, Any]) -> int:
        """作业覆盖的迟到事件数 = 它归属的区段在旁路表里的去重事件数。"""
        policy = job.get("_policy")
        keys = set(job.get("_segment_keys") or [])
        return sum(
            1 for row in store.aux(self.BYPASS_TABLE)
            if row.get("策略") == policy and row.get("区段") in keys
        )

    def _extend_job(
        self,
        job: dict[str, Any],
        start: str,
        end: str,
        batch_id: str,
        covered_events: int,
        segment: dict[str, Any],
        reason: str = "补数完成前再次越过阈值，追加原作业进度",
    ) -> None:
        start_int = int(start)
        end_int = int(end)
        current_start = int(str(job.get("起始位点", start)) or start)
        current_end = int(str(job.get("截止位点", end)) or end)
        new_start = min(current_start, start_int)
        new_end = max(current_end, end_int)
        job["起始位点"] = str(new_start)
        job["截止位点"] = str(new_end)
        job["补数范围"] = f"{new_start}-{new_end}"
        job["_executable_range"] = job["补数范围"]
        segment_keys = job.setdefault("_segment_keys", [])
        if segment["区段ID"] not in segment_keys:
            segment_keys.append(segment["区段ID"])
        job["_triggered_by_batch"] = batch_id
        # 覆盖数按旁路表的去重事件重算，重复导入同一事件不会把计数累加。
        job["_covered_events"] = self._segment_coverage(job)
        job["_late_count"] = job["_covered_events"]
        job["并行度"] = str(min(max(int(job["_covered_events"]), 1), 8))
        job.setdefault("_progress_log", []).append({
            "批次": batch_id,
            "时间": time.time(),
            "新增事件": covered_events,
            "范围": job["补数范围"],
            "说明": reason,
        })

    def _refresh_bypass_versions(self, policy_name: str) -> None:
        """把区段内已有旁路记录同步到区段当前版本和最近批次。

        重复导入同一批事件时，区段版本会递增；旁路记录本身只保留一条，
        版本号随之刷新，导出文件因此能反映“这是同一区段的第二版”。
        """
        segment_versions = {
            row.get("区段ID"): (int(row.get("版本", 1)), row.get("最近批次", ""))
            for row in store.aux(self.SEGMENT_TABLE)
            if row.get("策略") == policy_name
        }
        for row in store.aux(self.BYPASS_TABLE):
            if row.get("策略") != policy_name:
                continue
            version, batch_id = segment_versions.get(row.get("区段"), (None, None))
            if version is None:
                continue
            row["版本"] = version
            row["旁路ID"] = f"{row.get('_key')}|v{version}"
            if batch_id:
                row["批次"] = batch_id

    def _segment_has_executable(self, segment_key: str, exclude: set[Any] | None = None) -> bool:
        exclude = exclude or set()
        return any(
            segment_key in (job.get("_segment_keys") or [])
            and job.get("status") in ("待执行", "补数中")
            and not job.get("_paused")
            and not job.get("_superseded_by")
            and job.get("id") not in exclude
            for job in store.rows("backfill_job")
        )

    def _carry_over_segments(
        self,
        closed_job: dict[str, Any],
        segment_keys: list[str],
        batch_id: str,
        replacing_job: dict[str, Any],
    ) -> dict[str, Any]:
        """给关闭作业遗留的区段补一条承接作业，保证每个区段都有可执行范围。"""
        sources = [
            row for row in store.aux(self.SEGMENT_TABLE)
            if row.get("区段ID") in segment_keys
        ]
        if not sources:
            return {}
        start = str(min(late_bypass.segment_start(row) for row in sources))
        end = str(max(late_bypass.segment_end(row) for row in sources))
        carried_job = {
            "id": max(
                (int(r.get("id", 0)) for r in store.rows("backfill_job")), default=0
            ) + 1,
            "作业名称": f"{closed_job.get('作业名称', 'late_backfill')}_carry_{start}_{end}",
            "目标指标": closed_job.get("目标指标", ""),
            "补数范围": f"{start}-{end}",
            "并行度": closed_job.get("并行度", "1"),
            "起始位点": start,
            "截止位点": end,
            "进度比例": closed_job.get("进度比例", "0%"),
            "status": "待执行",
            "pending": True,
            "abnormal": False,
            "_policy": closed_job.get("_policy", ""),
            "_triggered_by": closed_job.get("_policy", ""),
            "_triggered_by_batch": batch_id,
            "_segment_keys": list(segment_keys),
            "_source": sources[0].get("源端", ""),
            "_covered_events": sum(int(row.get("迟到事件数", 0)) for row in sources),
            "_executable_range": f"{start}-{end}",
            "_carried_from": closed_job.get("id"),
            "_progress_log": [{
                "批次": batch_id,
                "时间": time.time(),
                "范围": f"{start}-{end}",
                "说明": f"原作业分段关闭后承接遗留区段（新作业 {replacing_job.get('id')}）",
            }],
        }
        carried_job["_late_count"] = carried_job["_covered_events"]
        store.rows("backfill_job").append(carried_job)
        return carried_job

    def _enforce_single_executable(self) -> None:
        """兜底：同一区段最多保留一条可执行作业。

        保留最早创建的那条（重复越过阈值应当扩展原作业），其余标记为
        ``部分失败`` 并记录被哪条作业替代。作业可能跨多个区段，关闭一条
        会影响其它分组，所以逐轮重算直到稳定。
        """
        for _ in range(len(store.rows("backfill_job")) + 1):
            groups: dict[str, list[dict[str, Any]]] = {}
            for job in store.rows("backfill_job"):
                if job.get("_superseded_by"):
                    continue
                if job.get("status") not in ("待执行", "补数中") or job.get("_paused"):
                    continue
                for key in job.get("_segment_keys") or []:
                    groups.setdefault(key, []).append(job)
            duplicate_group = next(
                (members for members in groups.values() if len(members) > 1),
                None,
            )
            if duplicate_group is None:
                return
            keeper = min(duplicate_group, key=lambda j: int(j.get("id", 0)))
            for job in duplicate_group:
                if job is keeper:
                    continue
                job["status"] = "部分失败"
                job["_superseded_by"] = keeper.get("id")
                job["_closed_reason"] = "duplicate_range"

    # ---- 批量回执与分析 ----

    def _build_batch_receipt(
        self,
        *,
        batch_id: str,
        policy_name: str,
        settings: dict[str, Any],
        filename: str,
        digest: str,
        filename_kind: str,
        total_events: int,
        late_count: int,
        late_ratio: float,
        watermark: float,
        triggered: bool,
        segments: list[dict[str, Any]],
        job_results: list[dict[str, Any]],
        new_bypass_count: int,
        duplicate: bool,
    ) -> dict[str, Any]:
        threshold = settings["threshold"]
        return {
            "批次号": batch_id,
            "策略": policy_name,
            "文件名": filename,
            "文件类型": filename_kind,
            "文件摘要": digest,
            "导入时间": time.time(),
            "总事件数": total_events,
            "迟到数": late_count,
            "迟到比例": round(late_ratio, 4),
            "迟到百分比": round(late_ratio * 100, 2),
            "阈值百分比": threshold["percent"],
            "阈值比例": threshold["ratio"],
            "阈值来源": threshold["source"],
            "阈值回退": threshold["fallback"],
            "允许延迟": settings["allowed_lateness"]["seconds"],
            "水位": watermark,
            "触发": triggered,
            "旁路记录数": new_bypass_count,
            "区段数": len(segments),
            "区段": [segment["区段ID"] for segment in segments],
            "作业": [result["job_id"] for result in job_results],
            "新建作业数": sum(1 for result in job_results if result["created"]),
            "幂等重复": duplicate,
            "status": "已回执",
            "pending": False,
            "abnormal": False,
        }

    @staticmethod
    def _settings_payload(settings: dict[str, Any]) -> dict[str, Any]:
        return {
            "policy": settings["policy"],
            "threshold_percent": settings["threshold"]["percent"],
            "threshold_ratio": settings["threshold"]["ratio"],
            "threshold_source": settings["threshold"]["source"],
            "threshold_fallback": settings["threshold"]["fallback"],
            "allowed_lateness": settings["allowed_lateness"]["seconds"],
        }

    @staticmethod
    def _segment_payload(segment: dict[str, Any]) -> dict[str, Any]:
        return {
            "segment_id": segment.get("区段ID"),
            "source": segment.get("源端"),
            "start": segment.get("起始时间"),
            "end": segment.get("截止时间"),
            "late_count": segment.get("迟到事件数"),
            "version": segment.get("版本"),
            "batch": segment.get("最近批次"),
        }

    def list_batches(self, policy_name: str | None = None) -> list[dict[str, Any]]:
        batches = list(store.aux(self.BATCH_TABLE))
        if policy_name:
            batches = [b for b in batches if b.get("策略") == policy_name]
        return sorted(batches, key=lambda b: b.get("导入时间", 0), reverse=True)

    def list_bypass(self, policy_name: str | None = None) -> list[dict[str, Any]]:
        rows = [
            row for row in store.aux(self.BYPASS_TABLE)
            if row.get("状态", "生效") == "生效"
        ]
        if policy_name:
            rows = [row for row in rows if row.get("策略") == policy_name]
        return sorted(rows, key=lambda r: (r.get("区段", ""), r.get("事件时间", 0)))

    def list_segments(self, policy_name: str | None = None) -> list[dict[str, Any]]:
        """区段列表；返回副本并附带作业覆盖，避免把页面字段写回存储。"""
        segments = [
            copy.deepcopy(segment)
            for segment in store.aux(self.SEGMENT_TABLE)
            if policy_name is None or segment.get("策略") == policy_name
        ]
        for segment in segments:
            key = segment.get("区段ID")
            jobs = [
                job for job in store.rows("backfill_job")
                if key in (job.get("_segment_keys") or []) and not job.get("_superseded_by")
            ]
            segment["jobs"] = [
                {
                    "job_id": job.get("id"),
                    "job_name": job.get("作业名称", ""),
                    "status": job.get("status", ""),
                    "range": job.get("补数范围", ""),
                    "progress": job.get("进度比例", "0%"),
                    "paused": bool(job.get("_paused")),
                    "executable": (
                        job.get("status") in ("待执行", "补数中")
                        and not job.get("_paused")
                        and not job.get("_superseded_by")
                    ),
                }
                for job in jobs
            ]
        return segments

    def late_processing_detail(self, policy_name: str) -> dict[str, Any]:
        """迟到处理页面数据：实际采用的比例与阈值、批次回执、区段与作业覆盖。"""
        settings = self.resolve_policy_settings(policy_name)
        if not settings["found"]:
            return {"ok": False, "error": f"迟到处理策略「{policy_name}」不存在"}

        batches = self.list_batches(policy_name)
        segments = self.list_segments(policy_name)
        jobs = [
            job for job in store.rows("backfill_job")
            if job.get("_policy") == policy_name and not job.get("_superseded_by")
        ]
        uncovered = [
            segment["区段ID"] for segment in segments
            if not any(job["executable"] for job in segment["jobs"])
        ]
        latest = batches[0] if batches else {}
        return {
            "ok": True,
            "policy": policy_name,
            "policy_id": settings["policy_id"],
            "settings": self._settings_payload(settings),
            "latest_batch": latest,
            "actual_late_ratio": latest.get("迟到比例", 0.0),
            "actual_threshold_ratio": settings["threshold"]["ratio"],
            "actual_threshold_percent": settings["threshold"]["percent"],
            "batches": batches,
            "segments": segments,
            "jobs": [
                {
                    "job_id": job.get("id"),
                    "job_name": job.get("作业名称", ""),
                    "status": job.get("status", ""),
                    "range": job.get("补数范围", ""),
                    "progress": job.get("进度比例", "0%"),
                    "paused": bool(job.get("_paused")),
                    "executable": (
                        job.get("status") in ("待执行", "补数中")
                        and not job.get("_paused")
                        and not job.get("_superseded_by")
                    ),
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
            "bypass_count": len(self.list_bypass(policy_name)),
        }

    # ---- 导出与回传 ----

    def build_manifest_document(
        self,
        policy_name: str,
        *,
        export_id: str | None = None,
        record_export: bool = True,
    ) -> dict[str, Any]:
        """构建导出清单文档；``record_export=False`` 时不登记导出台账。"""
        settings = self.resolve_policy_settings(policy_name)
        if not settings["found"]:
            return {"ok": False, "error": f"迟到处理策略「{policy_name}」不存在"}

        bypass_rows = self.list_bypass(policy_name)
        batches = self.list_batches(policy_name)
        jobs = [
            job for job in store.rows("backfill_job")
            if job.get("_policy") == policy_name and not job.get("_superseded_by")
        ]
        export_id = export_id or f"EX-{policy_name}-{int(time.time() * 1000)}"
        exported_at = time.time()
        document = late_bypass.build_manifest(
            policy_name=policy_name,
            policy=self.find_policy(policy_name),
            threshold=settings["threshold"],
            allowed_lateness=settings["allowed_lateness"],
            watermark=(batches[0].get("水位") if batches else None),
            batches=batches,
            bypass_rows=bypass_rows,
            jobs=jobs,
            export_id=export_id,
            exported_at=exported_at,
        )
        if record_export:
            store.aux(self.EXPORT_TABLE).append({
                "导出ID": export_id,
                "策略": policy_name,
                "校验和": document["checksum"],
                "导出时间": exported_at,
                "区段数": document["summary"]["segment_count"],
                "作业数": document["summary"]["job_count"],
                "迟到明细数": document["summary"]["late_count"],
            })
        filename = f"{late_bypass.safe_filename(policy_name)}_late_manifest.json"
        return {
            "ok": True,
            "export_id": export_id,
            "filename": filename,
            "checksum": document["checksum"],
            "document": document,
            "summary": document["summary"],
            "coverage": document["coverage"],
        }

    def export_manifest(
        self,
        policy_name: str,
        *,
        export_id: str | None = None,
    ) -> dict[str, Any]:
        """导出迟到旁路数据 + 补数任务清单，供其它页面接收与校验。

        清单格式与导出台账同源：导出时登记校验和，其它页面回传时据此识别
        “同源导出文件”，重复回传不重复入库。
        """
        return self.build_manifest_document(
            policy_name, export_id=export_id, record_export=True,
        )

    def verify_manifest_content(
        self,
        content: str,
        *,
        policy_name: str | None = None,
    ) -> dict[str, Any]:
        """校验一份导出文件：覆盖全部异常区段、明细齐全、只有一份可执行范围。"""
        try:
            document = late_bypass.parse_manifest_text(content)
        except late_bypass.ManifestError as exc:
            return {"ok": False, "valid": False, "issues": [str(exc)]}

        known_exports = {
            row.get("校验和") for row in store.aux(self.EXPORT_TABLE) if row.get("校验和")
        }
        known_imports = {
            row.get("校验和") for row in store.aux(self.IMPORT_TABLE) if row.get("校验和")
        }
        result = late_bypass.validate_manifest(
            document,
            known_export_checksums=known_exports,
            known_import_checksums=known_imports,
        )
        if policy_name and document.get("policy") != policy_name:
            result["ok"] = False
            result["valid"] = False
            result["issues"].append(
                f"清单属于策略「{document.get('policy')}」，与「{policy_name}」不一致",
            )
        result["export_id"] = document.get("export_id")
        result["policy"] = document.get("policy")
        return result

    def import_manifest(
        self,
        policy_name: str,
        content: str,
        *,
        filename: str = "",
        paused_strategy: str = "extend",
    ) -> dict[str, Any]:
        """接收其它页面回传的同源导出文件：先校验，再幂等入库。"""
        settings = self.resolve_policy_settings(policy_name)
        if not settings["found"]:
            return {"ok": False, "message": f"迟到处理策略「{policy_name}」不存在"}

        document = late_bypass.parse_manifest_text(content)
        known_exports = {
            row.get("校验和") for row in store.aux(self.EXPORT_TABLE) if row.get("校验和")
        }
        known_imports = {
            row.get("校验和") for row in store.aux(self.IMPORT_TABLE) if row.get("校验和")
        }
        validation = late_bypass.validate_manifest(
            document,
            known_export_checksums=known_exports,
            known_import_checksums=known_imports,
        )
        if document.get("policy") and document.get("policy") != policy_name:
            validation["ok"] = False
            validation["issues"].append(
                f"清单属于策略「{document.get('policy')}」，与「{policy_name}」不一致",
            )
        if not validation["ok"]:
            # 校验不通过：不写任何旁路数据，原文件由调用方保留。
            return {
                "ok": False,
                "duplicate": False,
                "committed": False,
                "rolled_back": True,
                "message": "导出文件校验未通过，已退回原文件",
                "validation": validation,
            }

        checksum = validation.get("checksum") or ""
        if checksum and any(row.get("校验和") == checksum for row in store.aux(self.IMPORT_TABLE)):
            ledger_row = next(
                row for row in store.aux(self.IMPORT_TABLE) if row.get("校验和") == checksum
            )
            return {
                "ok": True,
                "duplicate": True,
                "committed": False,
                "message": "同源导出文件已接收过，本次不重复入库",
                "receipt": copy.deepcopy(ledger_row),
                "validation": validation,
            }

        entries = document.get("entries") or []
        grouped_events: dict[str, list[dict[str, Any]]] = {}
        for item in entries:
            grouped_events.setdefault(str(item.get("区段") or ""), []).append(item)
        digest = late_bypass.content_digest(content)
        counts_before = (
            len(store.aux(self.BATCH_TABLE)),
            len(store.aux(self.BYPASS_TABLE)),
            len(store.aux(self.IMPORT_TABLE)),
        )
        aux_snapshot = store.snapshot_aux(self.SEGMENT_TABLE)
        jobs_before = copy.deepcopy(store.rows("backfill_job"))
        try:
            source_default = str(document.get("policy") or policy_name)
            batch_id = self._next_batch_id(policy_name)
            effective_watermark = float(document.get("watermark") or 0)
            touched: list[dict[str, Any]] = []
            new_bypass: list[dict[str, Any]] = []
            for segment_key, items in grouped_events.items():
                raw_events = [
                    {
                        "event_id": item.get("事件ID"),
                        "source": item.get("源端") or source_default,
                        "event_time": item.get("事件时间"),
                        "watermark": item.get("水位"),
                        "segment": item.get("区段"),
                        "is_late": True,
                    }
                    for item in items
                ]
                events = late_bypass.normalize_events(raw_events, default_source=source_default)
                if events:
                    effective_watermark = max(
                        [effective_watermark, *(e["event_time"] for e in events)]
                    )
                # 回传文件以导出时的区段号为准，不重新切段：跨页面区段必须一致。
                segment = {
                    "区段ID": segment_key or f"{source_default}|{late_bypass.DEFAULT_SEGMENT_GAP_SEC}",
                    "源端": events[0]["source"] if events else source_default,
                    "起始时间": min((e["event_time"] for e in events), default=0.0),
                    "截止时间": max((e["event_time"] for e in events), default=0.0),
                    "迟到事件数": len(events),
                    "事件列表": [
                        {
                            "event_id": e["event_id"],
                            "event_time": e["event_time"],
                            "watermark": e.get("watermark") or item.get("水位") or 0,
                        }
                        for e, item in zip(events, items)
                    ],
                    "事件ID列表": [e["event_id"] for e in events],
                    "水位": max((float(e.get("watermark") or 0) for e in events), default=0.0),
                    "切分间隔": late_bypass.DEFAULT_SEGMENT_GAP_SEC,
                    "显式区段": None,
                }
                merged = self._merge_segment(
                    segment, policy_name, batch_id, filename, preferred_key=segment_key or None,
                )
                merged["最近批次"] = batch_id
                touched.append(merged)
                new_bypass.extend(
                    self._build_bypass_rows(merged, policy_name, settings, batch_id, filename)
                )
            existing_keys = {row.get("_key") for row in store.aux(self.BYPASS_TABLE)}
            fresh = [row for row in new_bypass if row.get("_key") not in existing_keys]
            for row in fresh:
                store.aux(self.BYPASS_TABLE).append(row)
            self._refresh_bypass_versions(policy_name)

            covered_by_segment: dict[str, int] = {}
            for segment in touched:
                covered_by_segment[segment["区段ID"]] = sum(
                    1 for row in store.aux(self.BYPASS_TABLE)
                    if row.get("策略") == policy_name and row.get("区段") == segment["区段ID"]
                )
            job_results = []
            for segment in touched:
                job, created = self._ensure_job_for_segment(
                    policy_name=policy_name,
                    segment=segment,
                    batch_id=batch_id,
                    settings=settings,
                    paused_strategy=paused_strategy,
                    covered_events=covered_by_segment.get(segment["区段ID"], 0),
                )
                job_results.append({
                    "job_id": job.get("id"),
                    "job_name": job.get("作业名称", ""),
                    "created": created,
                    "range": job.get("补数范围", ""),
                    "status": job.get("status", ""),
                })

            receipt = self._build_batch_receipt(
                batch_id=batch_id,
                policy_name=policy_name,
                settings=settings,
                filename=filename,
                digest=digest,
                filename_kind="manifest",
                total_events=len(events),
                late_count=len(events),
                late_ratio=1.0,
                watermark=effective_watermark,
                triggered=True,
                segments=touched,
                job_results=job_results,
                new_bypass_count=len(fresh),
                duplicate=False,
            )
            receipt["来源清单导出ID"] = document.get("export_id")
            receipt["清单校验和"] = checksum
            store.aux(self.BATCH_TABLE).append(receipt)
            store.aux(self.IMPORT_TABLE).append({
                "导入ID": f"IM-{policy_name}-{len(store.aux(self.IMPORT_TABLE)) + 1:04d}",
                "策略": policy_name,
                "来源": "同源导出文件",
                "文件名": filename,
                "导出ID": document.get("export_id"),
                "校验和": checksum,
                "批次号": batch_id,
                "导入时间": time.time(),
                "区段数": len(touched),
                "迟到明细数": len(entries),
                "是否重复": False,
                "结果": "已接收",
            })
            self._enforce_single_executable()
        except Exception:
            # 事务失败：旁路、区段、作业、回执、导入台账全部退回导入前。
            self._truncate_aux(self.BATCH_TABLE, counts_before[0])
            self._truncate_aux(self.BYPASS_TABLE, counts_before[1])
            self._truncate_aux(self.IMPORT_TABLE, counts_before[2])
            store.restore_aux(aux_snapshot)
            store._tables["backfill_job"] = jobs_before
            raise

        return {
            "ok": True,
            "duplicate": False,
            "committed": True,
            "message": "同源导出文件已接收并生成补数任务",
            "batch": copy.deepcopy(receipt),
            "validation": validation,
            "jobs": job_results,
        }

    def list_import_ledger(self) -> list[dict[str, Any]]:
        return sorted(
            store.aux(self.IMPORT_TABLE), key=lambda row: row.get("导入时间", 0), reverse=True,
        )

    def list_export_ledger(self) -> list[dict[str, Any]]:
        return sorted(
            store.aux(self.EXPORT_TABLE), key=lambda row: row.get("导出时间", 0), reverse=True,
        )

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["active"] = sum(1 for r in rows if r.get("status") == "已启用")
        base["bypass_records"] = len(self.list_bypass())
        base["segments"] = len(store.aux(self.SEGMENT_TABLE))
        base["batches"] = len(store.aux(self.BATCH_TABLE))
        base["threshold_fallbacks"] = sum(
            1 for r in rows if late_bypass.parse_threshold(r)["fallback"]
        )
        return base
