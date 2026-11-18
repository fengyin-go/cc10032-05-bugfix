"""迟到处理 → 补数任务链路的领域测试。

覆盖的顺序即故障复现顺序：
  1. 空阈值不应被猜成 0.1 比例，而应回退策略默认值 5%；
  2. 越过阈值的事件必须同时生成旁路数据和补数作业，整段迟到事件不能被漏掉；
  3. 同一份文件重复导入只生效一次；同一迟到范围只保留一条可执行作业；
  4. 补数完成前再次越过阈值只在原作业上追加进度；
  5. 暂停状态遇到新迟到事件：可扩大原作业范围，也可关闭旧作业新建分段作业，
     页面最终只剩一份可执行范围；
  6. 两种文件边界（CSV/JSON、显式区段号）生成同一区段的不同版本；
  7. 导出文件可校验补数任务清单覆盖全部异常区段与迟到明细，并能被同源回传。

测试统一传显式水位，让“哪些事件迟到”只由水位决定，“怎么切段”只由切分间隔
决定，避免一条用例同时验证两件事。
"""
from __future__ import annotations

import copy
import json

import pytest

from app.services import late_bypass
from app.services.backfill_job import BackfillJobService
from app.services.late_handler import LateHandlerService
from app.store import store

POLICY = "迟到联调策略"
WATERMARK = 100_000.0  # 所有事件都早于截止时间，全部按迟到处理

_BASELINE: dict[str, list[dict]] = {}


def _reset() -> None:
    store.reset_aux()
    for table, rows in _BASELINE.items():
        store._tables[table] = copy.deepcopy(rows)


@pytest.fixture(autouse=True)
def _clean_store():
    for table in ("backfill_job", "late_handler"):
        _BASELINE.setdefault(table, copy.deepcopy(store.rows(table)))
    _reset()
    yield
    _reset()


def _add_policy(threshold: str = "", allowed: str = "10", gap: str = "60") -> None:
    store.rows("late_handler").append({
        "id": 100,
        "策略名称": POLICY,
        "允许延迟": allowed,
        "迟到动作": "bypass",
        "旁路输出": "迟到旁路样例",
        "计数方式": gap,
        "阈值百分比": threshold,
        "标记字段": "_late_mark",
        "status": "已启用",
        "pending": False,
        "abnormal": False,
    })


def _csv(rows: list[tuple[float, str, str]]) -> str:
    body = "\n".join(f"{ts},{source},{event_id}" for ts, source, event_id in rows)
    return "事件时间,源端,事件ID\n" + body + "\n"


def _ingest(service, rows, filename="a.csv", watermark=WATERMARK, **kwargs):
    return service.ingest_late_events(
        POLICY, _csv(rows), filename=filename, watermark=watermark, **kwargs,
    )


def _jobs_for(policy: str = POLICY) -> list[dict]:
    return [job for job in store.rows("backfill_job") if job.get("_policy") == policy]


def _segments_for(policy: str = POLICY) -> list[dict]:
    return [s for s in store.aux("late_segments") if s.get("策略") == policy]


class TestThresholdResolution:
    """阈值缺失应回退策略默认值，而不是猜测一个小数。"""

    def test_empty_threshold_falls_back_to_default_ratio(self):
        _add_policy(threshold="")
        settings = LateHandlerService().resolve_policy_settings(POLICY)
        assert settings["threshold"]["fallback"] is True
        assert settings["threshold"]["percent"] == 5.0
        assert settings["threshold"]["ratio"] == 0.05
        assert settings["threshold"]["source"] == "策略默认值"

    @pytest.mark.parametrize("raw", ["迟到处理样例1", "-5", "abc", "120", "  ", None])
    def test_invalid_thresholds_fall_back(self, raw):
        resolved = late_bypass.parse_threshold({"阈值百分比": raw})
        assert resolved["fallback"] is True
        assert resolved["ratio"] == 0.05

    def test_threshold_boundary_is_strict(self):
        _add_policy(threshold="10")
        service = LateHandlerService()
        assert service.check_threshold_trigger(POLICY, 0.10)["triggered"] is False
        assert service.check_threshold_trigger(POLICY, 0.1001)["triggered"] is True

    def test_percent_value_is_not_treated_as_fraction(self):
        _add_policy(threshold="5")
        result = LateHandlerService().check_threshold_trigger(POLICY, 0.5)
        assert result["threshold"] == 0.05
        assert result["threshold_percent"] == 5.0
        assert result["triggered"] is True


class TestLateIngest:
    """读取迟到事件 → 判阈值 → 生成补数任务清单。"""

    def test_threshold_breach_creates_bypass_and_jobs(self):
        _add_policy(threshold="")
        service = LateHandlerService()
        result = _ingest(service, [(1000, "srcA", "a1"), (1001, "srcA", "a2")])

        assert result["ok"] is True
        assert result["batch"]["触发"] is True
        assert result["batch"]["迟到数"] == 2
        # 同一区段只生成一条作业
        assert len(result["jobs"]) == 1
        assert len(_jobs_for()) == 1
        assert {row["事件ID"] for row in service.list_bypass(POLICY)} == {"a1", "a2"}

    def test_below_threshold_updates_statistics_without_jobs(self):
        _add_policy(threshold="90")
        service = LateHandlerService()
        # 水位 5000、允许延迟 10：只有 1000 这一条迟到，比例 1/2 < 90%
        content = _csv([(1000, "srcA", "a1"), (5000, "srcA", "a2")])
        result = service.ingest_late_events(POLICY, content, filename="a.csv")

        assert result["ok"] is True
        assert result["batch"]["触发"] is False
        assert result["batch"]["迟到数"] == 1
        assert _jobs_for() == []
        assert service.list_bypass(POLICY) == []

    def test_boundary_event_at_cutoff_is_on_time(self):
        _add_policy(threshold="0", allowed="10")
        service = LateHandlerService()
        content = _csv([(4990, "srcA", "edge"), (5000, "srcA", "fresh")])
        result = service.ingest_late_events(POLICY, content, filename="a.csv")
        # 4990 == 5000 - 10，边界相等算准时，因此没有迟到事件。
        assert result["batch"]["迟到数"] == 0

    def test_repeated_import_is_idempotent(self):
        _add_policy(threshold="")
        service = LateHandlerService()
        first = _ingest(service, [(1000, "srcA", "a1"), (1001, "srcA", "a2")])
        second = _ingest(service, [(1000, "srcA", "a1"), (1001, "srcA", "a2")])

        assert first["committed"] is True
        assert second["duplicate"] is True
        assert second["committed"] is False
        assert len(_jobs_for()) == 1
        assert len(service.list_bypass(POLICY)) == 2
        assert len(service.list_batches(POLICY)) == 1

    def test_failed_import_rolls_back_and_keeps_original_file(self):
        _add_policy(threshold="")
        service = LateHandlerService()
        broken = "事件时间,源端,事件ID\n不是时间,srcA,a1\n"
        result = service.ingest_late_events(POLICY, broken, filename="bad.csv")

        assert result["ok"] is False
        assert result["rolled_back"] is True
        assert _jobs_for() == []
        assert service.list_bypass(POLICY) == []
        assert service.list_batches(POLICY) == []
        assert service.list_import_ledger() == []

    def test_transaction_failure_rolls_back_midway(self, monkeypatch):
        """写旁路后创建作业失败：旁路、区段、作业、回执都要退回导入前。"""
        _add_policy(threshold="")
        service = LateHandlerService()
        _ingest(service, [(1000, "srcA", "a1")])
        jobs_before = copy.deepcopy(_jobs_for())
        bypass_before = len(service.list_bypass(POLICY))
        segments_before = copy.deepcopy(_segments_for())
        batches_before = len(service.list_batches(POLICY))

        def boom(**_: object) -> None:
            raise RuntimeError("模拟作业创建失败")

        monkeypatch.setattr(service, "_ensure_job_for_segment", boom)
        content = _csv([(2000, "srcA", "a2")])
        with pytest.raises(RuntimeError):
            service.ingest_late_events(
                POLICY, content, filename="fail.csv", watermark=WATERMARK,
            )
        monkeypatch.undo()

        assert _jobs_for() == jobs_before
        assert len(service.list_bypass(POLICY)) == bypass_before
        assert _segments_for() == segments_before
        assert len(service.list_batches(POLICY)) == batches_before
        # 原文件没有被吞掉：换成正常实现后同一份内容可以重新导入。
        retried = _ingest(service, [(2000, "srcA", "a2")], filename="fail.csv")
        assert retried["ok"] is True
        assert len(service.list_bypass(POLICY)) == bypass_before + 1

    def test_reply_file_reuses_previous_watermark(self):
        """补报文件通常不带水位：沿用策略上一批水位，整段补报事件才不会漏判。"""
        _add_policy(threshold="")
        service = LateHandlerService()
        content = _csv([(1000, "srcA", "a1"), (9000, "srcA", "tail")])
        service.ingest_late_events(POLICY, content, filename="a.csv")
        # 第二批是补报：只有事件、没有水位。若拿事件时间当水位，1002 会被判成准时；
        # 沿用上一批水位 9000 才能识别出它仍然落在迟到区间内。
        result = _ingest(service, [(1002, "srcA", "a2")], filename="b.csv", watermark=None)
        assert result["batch"]["迟到数"] == 1
        assert result["batch"]["水位"] == 9000
        assert result["jobs"][0]["created"] is False

    def test_whole_late_range_enters_bypass(self):
        """一个文件里的多段迟到事件都要进入旁路，不能只统计最后一段。"""
        _add_policy(threshold="", gap="60")
        service = LateHandlerService()
        result = _ingest(service, [
            (1000, "srcA", "a1"), (1001, "srcA", "a2"),
            (3000, "srcA", "a3"), (3001, "srcA", "a4"),
            (9000, "srcA", "a5"),
        ])
        assert result["batch"]["区段数"] == 3
        assert len(result["jobs"]) == 3
        assert len(service.list_bypass(POLICY)) == 5

    def test_processing_view_reports_actual_ratio_and_threshold(self):
        _add_policy(threshold="")
        service = LateHandlerService()
        _ingest(service, [(1000, "srcA", "a1"), (1001, "srcA", "a2")])
        detail = service.late_processing_detail(POLICY)

        assert detail["ok"] is True
        assert detail["actual_threshold_percent"] == 5.0
        assert detail["actual_threshold_ratio"] == 0.05
        assert detail["actual_late_ratio"] == 1.0
        assert detail["coverage"]["complete"] is True
        assert detail["latest_batch"]["阈值来源"] == "策略默认值"


class TestSingleExecutableRange:
    """同一迟到范围只能创建一个待执行作业。"""

    def test_new_events_extend_existing_job(self):
        _add_policy(threshold="")
        service = LateHandlerService()
        _ingest(service, [(1000, "srcA", "a1"), (1001, "srcA", "a2")])
        job = _jobs_for()[0]
        assert job["补数范围"] == "1000-1001"

        result = _ingest(service, [(1002, "srcA", "a3")], filename="b.csv")
        assert result["jobs"][0]["created"] is False
        assert len(_jobs_for()) == 1
        job = _jobs_for()[0]
        assert job["补数范围"] == "1000-1002"
        assert job["_covered_events"] == 3
        assert len(job["_progress_log"]) == 2

    def test_job_progress_appended_before_completion(self):
        _add_policy(threshold="")
        service = LateHandlerService()
        _ingest(service, [(1000, "srcA", "a1"), (1001, "srcA", "a2")])
        job = _jobs_for()[0]
        job["status"] = "补数中"
        job["进度比例"] = "40%"

        _ingest(service, [(1002, "srcA", "a3")], filename="b.csv")
        job = _jobs_for()[0]
        assert job["status"] == "补数中"
        assert job["进度比例"] == "40%"  # 追加的是范围与进度记录，不是重置进度
        assert job["补数范围"] == "1000-1002"
        assert job["_progress_log"][-1]["批次"] == "LB-迟到联调策略-0002"

    def test_duplicate_executable_ranges_are_closed(self):
        _add_policy(threshold="")
        service = LateHandlerService()
        _ingest(service, [(1000, "srcA", "a1")])
        job = _jobs_for()[0]
        duplicate = copy.deepcopy(job)
        duplicate["id"] = 999
        duplicate["作业名称"] = "重复范围作业"
        store.rows("backfill_job").append(duplicate)

        service._enforce_single_executable()

        active = [
            j for j in _jobs_for()
            if j.get("status") == "待执行" and not j.get("_paused")
        ]
        assert len(active) == 1
        assert duplicate.get("_superseded_by") == job["id"]

    def test_repeated_import_does_not_double_count(self):
        _add_policy(threshold="")
        service = LateHandlerService()
        _ingest(service, [(1000, "srcA", "a1")])
        before = _jobs_for()[0]["_covered_events"]
        _ingest(service, [(1000, "srcA", "a1")], filename="b.csv")
        # 同一事件换了文件名再导入，不会因为摘要不同就重复计数。
        assert _jobs_for()[0]["_covered_events"] == before
        assert len(service.list_bypass(POLICY)) == 1


class TestPausedStrategy:
    """补报事件遇到暂停状态：扩大原范围或分段新建，最终只剩一份可执行范围。"""

    def _prepare(self):
        _add_policy(threshold="")
        service = LateHandlerService()
        backfill = BackfillJobService()
        _ingest(service, [(1000, "srcA", "a1"), (1001, "srcA", "a2")])
        job = _jobs_for()[0]
        backfill.run_action(job["id"], "暂停补数")
        return service, backfill, job

    def test_paused_job_extended_in_place(self):
        service, backfill, job = self._prepare()
        job_id = job["id"]
        assert backfill.summarize_executable_ranges()["ranges"]["srcA|1000|1001"]["executable_count"] == 0

        result = _ingest(service, [(1002, "srcA", "a3")], filename="b.csv", paused_strategy="extend")
        assert result["jobs"][0]["job_id"] == job_id
        assert result["jobs"][0]["created"] is False
        job = store.find("backfill_job", job_id)
        assert job["_paused"] is False
        assert job["status"] == "待执行"
        assert job["补数范围"] == "1000-1002"
        assert len(_jobs_for()) == 1

    def test_paused_job_split_creates_new_segment_job(self):
        service, backfill, old = self._prepare()
        old_id = old["id"]

        result = _ingest(service, [(1002, "srcA", "a3")], filename="b.csv", paused_strategy="split")
        new_id = result["jobs"][0]["job_id"]
        assert new_id != old_id
        assert result["jobs"][0]["created"] is True
        closed = store.find("backfill_job", old_id)
        assert closed["status"] == "部分失败"
        assert closed["_superseded_by"] == new_id

        ranges = backfill.summarize_executable_ranges()
        assert ranges["ranges"]["srcA|1000|1001"]["job_id"] == new_id
        assert ranges["ranges"]["srcA|1000|1001"]["executable_count"] == 1
        assert backfill.find_coverage_gaps()["ok"] is True

    def test_split_keeps_other_segments_covered(self):
        _add_policy(threshold="")
        service = LateHandlerService()
        backfill = BackfillJobService()
        _ingest(service, [(1000, "srcA", "a1"), (3000, "srcA", "a2")])
        first = [j for j in _jobs_for() if j["补数范围"].startswith("1000")][0]
        second = [j for j in _jobs_for() if j["补数范围"].startswith("3000")][0]
        # 让第一条作业同时覆盖第二段，模拟旧作业跨多区段的情况
        second["status"] = "部分失败"
        second["_superseded_by"] = first["id"]
        first["_segment_keys"].append(second["_segment_keys"][0])

        backfill.run_action(first["id"], "暂停补数")
        _ingest(service, [(1001, "srcA", "a3")], filename="b.csv", paused_strategy="split")

        gaps = backfill.find_coverage_gaps()
        assert gaps["ok"] is True, gaps
        covered = {
            seg["segment_id"]
            for seg in backfill.list_pending_segments(POLICY)
            if seg["covered"]
        }
        assert covered == {"srcA|1000|1000", "srcA|3000|3000"}

    def test_paused_job_action_flow(self):
        _add_policy(threshold="")
        service = LateHandlerService()
        backfill = BackfillJobService()
        _ingest(service, [(1000, "srcA", "a1")])
        job = _jobs_for()[0]

        entry, _ = backfill.run_action(job["id"], "启动补数")
        assert entry is not None
        assert entry["status"] == "待执行"

        entry, _ = backfill.run_action(job["id"], "暂停补数")
        assert entry is not None
        assert entry["_paused"] is True
        assert job["id"] in {item["job_id"] for item in backfill.list_pending_backfills()}


class TestFileBoundaryVersions:
    """两种文件边界在序号5下生成同一区段的不同版本。"""

    def test_two_file_boundaries_produce_two_versions(self):
        _add_policy(threshold="", gap="60")
        service = LateHandlerService()
        # 边界一：CSV，同一区段的 1000-1002
        _ingest(service, [(1000, "srcA", "a1"), (1001, "srcA", "a2"), (1002, "srcA", "a3")],
                filename="first.csv")
        # 边界二：JSON，同一区段续上 1003（间隔小于切分间隔）
        payload = json.dumps({
            "source": "srcA",
            "entries": [{"event_id": "a4", "source": "srcA", "event_time": 1003}],
        })
        result = service.ingest_late_events(
            POLICY, payload, filename="second.json", watermark=WATERMARK,
        )

        segments = _segments_for()
        assert len(segments) == 1
        segment = segments[0]
        assert segment["版本"] == 2
        assert segment["截止时间"] == 1003
        assert segment["来源文件"] == ["first.csv", "second.json"]
        assert len(result["jobs"]) == 1
        assert result["jobs"][0]["created"] is False

        bypass = service.list_bypass(POLICY)
        assert {row["版本"] for row in bypass} == {2}
        assert {row["事件ID"] for row in bypass} == {"a1", "a2", "a3", "a4"}

    def test_explicit_segment_hint_is_preserved(self):
        _add_policy(threshold="", gap="60")
        service = LateHandlerService()
        payload = json.dumps({
            "source": "srcA",
            "entries": [
                {"event_id": "s1", "event_time": 1000, "segment": "SEG-1"},
                {"event_id": "s2", "event_time": 9000, "segment": "SEG-1"},
                {"event_id": "s3", "event_time": 9500, "segment": "SEG-2"},
            ],
        })
        result = service.ingest_late_events(
            POLICY, payload, filename="hint.json", watermark=WATERMARK,
        )
        assert result["batch"]["区段数"] == 2
        segment_ids = {segment["区段ID"] for segment in _segments_for()}
        assert segment_ids == {"srcA|SEG-1", "srcA|SEG-2"}

    def test_second_file_boundary_does_not_duplicate_events(self):
        _add_policy(threshold="", gap="60")
        service = LateHandlerService()
        _ingest(service, [(1000, "srcA", "a1"), (1001, "srcA", "a2")], filename="first.csv")
        # 第二份文件里重复带上 a2，并新增 a3
        _ingest(service, [(1001, "srcA", "a2"), (1002, "srcA", "a3")], filename="second.csv")

        bypass = service.list_bypass(POLICY)
        assert [row["事件ID"] for row in bypass] == ["a1", "a2", "a3"]
        job = _jobs_for()[0]
        assert job["_covered_events"] == 3
        assert job["补数范围"] == "1000-1002"


class TestExportContract:
    """导出文件可校验补数任务清单覆盖全部异常区段和迟到明细。"""

    def _prepare(self):
        _add_policy(threshold="", gap="60")
        service = LateHandlerService()
        backfill = BackfillJobService()
        _ingest(
            service,
            [(1000, "srcA", "a1"), (1001, "srcA", "a2"), (3000, "srcA", "a3")],
            filename="a.csv",
        )
        manifest = service.export_manifest(POLICY)
        return service, backfill, manifest

    def test_manifest_covers_every_segment_and_detail(self):
        _, _, manifest = self._prepare()
        document = manifest["document"]
        assert document["summary"]["segment_count"] == 2
        assert document["summary"]["late_count"] == 3
        assert document["summary"]["coverage_complete"] is True
        assert document["summary"]["job_count"] == 2
        for segment in document["segments"]:
            assert segment["作业ID"] is not None
            assert segment["可执行作业数"] == 1
        assert {entry["事件ID"] for entry in document["entries"]} == {"a1", "a2", "a3"}

    def test_manifest_pins_actual_ratio_and_threshold(self):
        _, _, manifest = self._prepare()
        document = manifest["document"]
        assert document["threshold_percent"] == 5.0
        assert document["threshold_source"] == "策略默认值"
        assert document["summary"]["late_ratio"] == 1.0

    def test_verify_detects_missing_job(self):
        service, _, manifest = self._prepare()
        document = copy.deepcopy(manifest["document"])
        document["jobs"][0]["可执行"] = False
        result = service.verify_manifest_content(json.dumps(document, ensure_ascii=False))
        assert result["ok"] is False
        assert any("可执行" in issue or "作业" in issue for issue in result["issues"])

    def test_verify_detects_missing_detail(self):
        service, _, manifest = self._prepare()
        document = copy.deepcopy(manifest["document"])
        document["bypass"] = document["bypass"][1:]
        result = service.verify_manifest_content(json.dumps(document, ensure_ascii=False))
        assert result["ok"] is False
        assert any("明细" in issue or "旁路" in issue for issue in result["issues"])

    def test_tampered_checksum_is_rejected(self):
        service, _, manifest = self._prepare()
        document = copy.deepcopy(manifest["document"])
        document["entries"][0]["事件时间"] = 1234
        result = service.verify_manifest_content(json.dumps(document, ensure_ascii=False))
        assert result["ok"] is False
        assert any("校验和" in issue for issue in result["issues"])

    def test_same_source_export_round_trip(self):
        service, _, manifest = self._prepare()
        content = json.dumps(manifest["document"], ensure_ascii=False)
        assert service.verify_manifest_content(content)["ok"] is True

        first = service.import_manifest(POLICY, content, filename="manifest.json")
        assert first["ok"] is True
        assert first["committed"] is True
        second = service.import_manifest(POLICY, content, filename="manifest.json")
        assert second["ok"] is True
        assert second["duplicate"] is True

        assert len(service.list_import_ledger()) == 1
        assert len(_jobs_for()) == 2  # 仍是两个区段两条作业
        assert len(service.list_bypass(POLICY)) == 3

    def test_import_rejects_foreign_policy(self):
        service, _, manifest = self._prepare()
        mismatched = copy.deepcopy(manifest["document"])
        mismatched["policy"] = "另一个策略"
        result = service.import_manifest(POLICY, json.dumps(mismatched, ensure_ascii=False))
        assert result["ok"] is False
        assert result["rolled_back"] is True
        assert service.list_import_ledger() == []

    def test_invalid_manifest_returns_original_file(self):
        service, _, _ = self._prepare()
        broken = '{"format": "late_bypass_manifest", "entries": "not-a-list"}'
        result = service.import_manifest(POLICY, broken, filename="broken.json")
        assert result["ok"] is False
        assert any("数组" in issue for issue in result["validation"]["issues"])

    def test_task_listing_matches_segment_count(self):
        _, backfill, _ = self._prepare()
        rows = backfill.export_task_listing(POLICY)
        segments = backfill.list_pending_segments(POLICY)
        assert len(rows) == len(segments) == 2
        assert all(row["已覆盖"] == "是" for row in rows)
        assert backfill.find_coverage_gaps()["ok"] is True

    def test_console_reports_coverage_per_policy(self):
        _, backfill, _ = self._prepare()
        console = backfill.task_console()
        assert POLICY in console["policies"]
        coverage = console["policies"][POLICY]["coverage"]
        assert coverage["complete"] is True
        assert coverage["segment_count"] == 2
        assert console["totals"]["uncovered"] == 0

    def test_backfill_manifest_validates_like_late_manifest(self):
        _, backfill, _ = self._prepare()
        exported = backfill.export_task_manifest(POLICY)
        content = json.dumps(exported["document"], ensure_ascii=False)
        assert backfill.verify_task_manifest(content)["ok"] is True
        # 删掉一条作业后，补数清单校验必须失败
        broken = copy.deepcopy(exported["document"])
        broken["jobs"] = broken["jobs"][1:]
        verdict = backfill.verify_task_manifest(json.dumps(broken, ensure_ascii=False))
        assert verdict["ok"] is False


class TestPipelineIntegration:
    """管道链路与领域服务共用同一套作业创建规则。"""

    def test_pipeline_reuses_existing_late_job(self):
        from app.pipeline import check_watermark_delay, create_backfill_from_late

        events = [
            {"event_id": f"e{i}", "event_time": 1000 + i, "payload": {}} for i in range(3)
        ] + [{"event_id": "tail", "event_time": 5000, "payload": {}}]
        late, _ = check_watermark_delay("pipe_source", 5000, events, allowed_lateness_sec=10)
        first = create_backfill_from_late(late, "pipe_metric", "pipe_source")
        second = create_backfill_from_late(late, "pipe_metric", "pipe_source")
        assert first["created"] is True
        assert second["created"] is False
        assert first["job_id"] == second["job_id"]
