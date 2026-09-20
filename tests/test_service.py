"""核心归段逻辑测试：仅依赖标准库，可直接运行：

    python3 -m unittest tests.test_service -v
"""
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.db import init_db, read_only_session, session  # noqa: E402
from app import service  # noqa: E402

SILENCE = 10.0


class ServiceTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        init_db(self.db_path)

    def tearDown(self):
        self.tmp.cleanup()

    def submit(self, source, event_id, occurred_ts, severity="info"):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.ingest_event(
                conn,
                source=source,
                event_id=event_id,
                occurred_ts=occurred_ts,
                severity=severity,
                silence=SILENCE,
            )

    def get(self, segment_id):
        with session(self.db_path) as conn:
            return service.get_segment(conn, segment_id)

    def changes(self, after=0, limit=100):
        with session(self.db_path) as conn:
            return service.list_segment_changes(conn, after=after, limit=limit)

    def test_first_event_creates_segment(self):
        r = self.submit("s1", "e1", 1000.0, "warning")
        self.assertFalse(r["deduplicated"])
        seg = r["segment"]
        self.assertEqual(seg["event_count"], 1)
        self.assertEqual(seg["revision"], 1)
        self.assertEqual(seg["max_severity"], "warning")
        self.assertEqual(seg["started_at"], seg["ended_at"])

    def test_duplicate_event_id_is_idempotent(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e1", 1000.0)
        self.assertTrue(r2["deduplicated"])
        seg = self.get(r1["segment"]["id"])
        self.assertEqual(seg["event_count"], 1)
        self.assertEqual(seg["revision"], 1)

    def test_same_event_id_different_source_not_duplicate(self):
        self.submit("s1", "e1", 1000.0)
        r = self.submit("s2", "e1", 1000.0)
        self.assertFalse(r["deduplicated"])
        self.assertEqual(r["segment"]["source"], "s2")

    def test_out_of_order_event_extends_start_and_bumps_revision(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e2", 995.0)  # 更早到达的乱序事件
        self.assertEqual(r2["segment"]["id"], r1["segment"]["id"])
        seg = r2["segment"]
        self.assertEqual(seg["event_count"], 2)
        self.assertEqual(seg["revision"], 2)  # 边界变化 → 递增
        self.assertEqual(seg["started_at"], "1970-01-01T00:16:35+00:00")  # 995.0

    def test_inner_event_lower_severity_keeps_revision(self):
        self.submit("s1", "e1", 1000.0, "error")
        r = self.submit("s1", "e2", 1010.0, "info")  # 扩展 end → rev=2
        self.assertEqual(r["segment"]["revision"], 2)
        r = self.submit("s1", "e3", 1005.0, "debug")  # 严格位于段内、级别更低
        self.assertEqual(r["segment"]["event_count"], 3)
        self.assertEqual(r["segment"]["revision"], 2)  # 边界与级别未变
        self.assertEqual(r["segment"]["max_severity"], "error")

    def test_inner_event_higher_severity_bumps_revision(self):
        self.submit("s1", "e1", 1000.0, "info")
        r = self.submit("s1", "e2", 1003.0, "critical")
        self.assertEqual(r["segment"]["revision"], 2)
        self.assertEqual(r["segment"]["max_severity"], "critical")

    def test_event_beyond_silence_creates_new_segment(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e2", 1000.0 + SILENCE + 0.1)
        self.assertNotEqual(r1["segment"]["id"], r2["segment"]["id"])

    def test_event_at_exact_silence_boundary_joins_segment(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e2", 1000.0 + SILENCE)
        self.assertEqual(r1["segment"]["id"], r2["segment"]["id"])

    def test_late_event_bridges_two_segments(self):
        r1 = self.submit("s1", "e1", 0.0)          # 段 A [0, 0]
        r2 = self.submit("s1", "e2", 11.0)         # 11 > 0+10 → 段 B [11, 11]
        self.assertNotEqual(r1["segment"]["id"], r2["segment"]["id"])
        r3 = self.submit("s1", "e3", 5.0)          # 桥接 A 与 B
        seg = r3["segment"]
        self.assertEqual(seg["id"], r1["segment"]["id"])
        self.assertEqual(seg["event_count"], 3)
        self.assertEqual(seg["revision"], 2)
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn, "s1")), 1)

    def test_chain_merge_via_fixpoint(self):
        self.submit("s1", "e1", 0.0)               # 段 A
        self.submit("s1", "e2", 11.0)              # 段 B
        self.submit("s1", "e3", 22.0)              # 22 > 11+10 → 段 C
        self.submit("s1", "e4", 5.0)               # 合并 A+B → [0, 11]
        r = self.submit("s1", "e5", 16.0)          # 桥接 [0,11] 与 [22,22]
        seg = r["segment"]
        self.assertEqual(seg["event_count"], 5)
        with session(self.db_path) as conn:
            segs = service.list_segments(conn, "s1")
        self.assertEqual(len(segs), 1)
        self.assertTrue(segs[0]["ended_at"].startswith("1970-01-01T00:00:22"))

    def test_sources_are_isolated(self):
        self.submit("s1", "e1", 1000.0)
        r = self.submit("s2", "e2", 1001.0)  # 时间接近但 source 不同
        self.assertEqual(r["segment"]["event_count"], 1)
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn)), 2)

    def test_revision_never_decreases_after_merge(self):
        self.submit("s1", "e1", 0.0, "info")
        self.submit("s1", "e2", 1.0, "critical")   # 段 A rev=2
        self.submit("s1", "e3", 21.0, "info")      # 21 > 1+10 → 段 B rev=1
        r = self.submit("s1", "e4", 11.0)          # 桥接 A、B
        self.assertGreaterEqual(r["segment"]["revision"], 3)

    # ---- 变更流（segment_changes）----

    def test_create_emits_upsert_change(self):
        r = self.submit("s1", "e1", 1000.0, "warning")
        page = self.changes()
        self.assertEqual(len(page["changes"]), 1)
        ch = page["changes"][0]
        self.assertEqual(ch["change_id"], 1)
        self.assertEqual(ch["type"], "upsert")
        self.assertEqual(ch["segment_id"], r["segment"]["id"])
        self.assertEqual(ch["revision"], 1)
        self.assertIsNone(ch["merged_into"])
        self.assertEqual(ch["segment"], r["segment"])  # 快照与段状态一致
        self.assertEqual(page["next_after"], 1)

    def test_boundary_or_severity_change_emits_upsert(self):
        self.submit("s1", "e1", 1000.0, "info")
        self.submit("s1", "e2", 1005.0, "error")   # 扩 end + 提级 → rev 2
        page = self.changes()
        self.assertEqual([c["type"] for c in page["changes"]], ["upsert", "upsert"])
        second = page["changes"][1]
        self.assertEqual(second["revision"], 2)
        self.assertEqual(second["segment"]["max_severity"], "error")
        self.assertEqual(second["segment"]["event_count"], 2)
        ids = [c["change_id"] for c in page["changes"]]
        self.assertLess(ids[0], ids[1])            # 单调递增

    def test_count_only_increase_emits_no_change(self):
        self.submit("s1", "e1", 1000.0, "error")
        self.submit("s1", "e2", 1010.0)            # 扩边界 → upsert
        before = self.changes()["changes"]
        self.assertEqual(len(before), 2)
        self.submit("s1", "e3", 1005.0, "debug")   # 仅事件数 +1，revision 不变
        self.assertEqual(self.changes()["changes"], before)  # 无新增变更

    def test_duplicate_event_id_emits_no_change(self):
        self.submit("s1", "e1", 1000.0)
        before = self.changes()["changes"]
        r = self.submit("s1", "e1", 1000.0)        # 幂等重放
        self.assertTrue(r["deduplicated"])
        self.assertEqual(self.changes()["changes"], before)

    def test_merge_emits_tombstone_per_deleted_segment(self):
        r1 = self.submit("s1", "e1", 0.0)          # 段 A
        r2 = self.submit("s1", "e2", 11.0)         # 段 B
        r3 = self.submit("s1", "e3", 22.0)         # 段 C
        self.submit("s1", "e4", 5.0)               # 合并 A+B
        self.submit("s1", "e5", 16.0)              # 桥接 [A+B] 与 C → 三段合并
        page = self.changes()
        types = [c["type"] for c in page["changes"]]
        # 3 次创建 + (B 墓碑 + A upsert) + (C 墓碑 + A upsert)
        self.assertEqual(
            types,
            ["upsert", "upsert", "upsert", "merged", "upsert", "merged", "upsert"],
        )
        tombs = [c for c in page["changes"] if c["type"] == "merged"]
        self.assertEqual(
            {t["segment_id"] for t in tombs},
            {r2["segment"]["id"], r3["segment"]["id"]},
        )
        for t in tombs:                            # 每个墓碑都指向保留段 A
            self.assertEqual(t["merged_into"], r1["segment"]["id"])
            self.assertIsNone(t["segment"])
        final = page["changes"][-1]
        self.assertEqual(final["type"], "upsert")
        self.assertEqual(final["segment_id"], r1["segment"]["id"])
        self.assertEqual(final["segment"]["event_count"], 5)
        ids = [c["change_id"] for c in page["changes"]]
        self.assertEqual(ids, sorted(ids))         # 全局单调递增
        self.assertEqual(len(set(ids)), len(ids))

    def test_pagination_no_miss_no_dup_while_ingesting(self):
        for i in range(5):                         # 5 个独立段 → 5 条 upsert
            self.submit("s1", f"e{i}", 1000.0 + i * 100.0)

        page1 = self.changes(after=0, limit=2)
        self.assertEqual([c["change_id"] for c in page1["changes"]], [1, 2])
        self.assertEqual(page1["next_after"], 2)

        self.submit("s1", "e5", 2000.0)            # 分页期间又有新事件

        page2 = self.changes(after=page1["next_after"], limit=2)
        page3 = self.changes(after=page2["next_after"], limit=100)
        seen = [c["change_id"] for p in (page1, page2, page3) for c in p["changes"]]
        self.assertEqual(seen, [1, 2, 3, 4, 5, 6])  # 不漏读
        self.assertEqual(len(seen), len(set(seen)))  # 不重复

        empty = self.changes(after=page3["next_after"])  # 已读完：空页
        self.assertEqual(empty["changes"], [])
        self.assertEqual(empty["next_after"], page3["next_after"])  # 游标不动

    def test_empty_changes_next_after_echoes_after(self):
        page = self.changes(after=0)
        self.assertEqual(page, {"changes": [], "next_after": 0})
        page = self.changes(after=42)
        self.assertEqual(page, {"changes": [], "next_after": 42})

    # ---- 静默间隔重分段预演（纯读）----

    def preview(self, source, silence):
        with read_only_session(self.db_path) as conn:
            return service.preview_resegmentation(
                conn, source=source, silence=silence
            )

    def test_preview_smaller_interval_splits_segments(self):
        # 当前 SILENCE=10：0、8、20、28、40 → 三段 [0,8]、[20,28]、[40]
        a = self.submit("s1", "e1", 0.0)["segment"]
        self.submit("s1", "e2", 8.0)
        b = self.submit("s1", "e3", 20.0)["segment"]
        self.submit("s1", "e4", 28.0)
        c = self.submit("s1", "e5", 40.0)["segment"]

        r = self.preview("s1", 5.0)
        # 间隔 5：8→20(12)、28→40(12) 切开；0→8(8) 切开 → 五段
        sims = r["simulated_segments"]
        self.assertEqual([s["temp_id"] for s in sims], [1, 2, 3, 4, 5])
        self.assertEqual(
            [s["event_count"] for s in sims], [1, 1, 1, 1, 1]
        )
        self.assertTrue(all(s["source"] == "s1" for s in sims))

        by_id = {m["segment_id"]: m for m in r["mappings"]}
        self.assertEqual(by_id[a["id"]]["relation"], "split")
        self.assertEqual(by_id[a["id"]]["simulated_temp_ids"], [1, 2])
        self.assertEqual(by_id[b["id"]]["relation"], "split")
        self.assertEqual(by_id[b["id"]]["simulated_temp_ids"], [3, 4])
        self.assertEqual(by_id[c["id"]]["relation"], "unchanged")
        self.assertEqual(by_id[c["id"]]["simulated_temp_ids"], [5])

        summary = r["summary"]
        self.assertEqual(summary["split_existing_segment_count"], 2)
        self.assertEqual(summary["merged_existing_segment_count"], 0)
        self.assertEqual(summary["unchanged_existing_segment_count"], 1)
        self.assertEqual(summary["changed_existing_segment_count"], 2)
        self.assertEqual(summary["existing_segment_count"], 3)
        self.assertEqual(summary["simulated_segment_count"], 5)
        self.assertEqual(r["event_count"], 5)

    def test_preview_larger_interval_merges_segments(self):
        # 0、8、20、28 → 现有两段 [0,8]、[20,28]
        a = self.submit("s1", "e1", 0.0)["segment"]
        self.submit("s1", "e2", 8.0)
        b = self.submit("s1", "e3", 20.0)["segment"]
        self.submit("s1", "e4", 28.0)

        r = self.preview("s1", 100.0)
        sims = r["simulated_segments"]
        self.assertEqual(len(sims), 1)
        self.assertEqual(sims[0]["temp_id"], 1)
        self.assertEqual(sims[0]["event_count"], 4)
        self.assertEqual(sims[0]["started_at"], "1970-01-01T00:00:00+00:00")
        self.assertEqual(sims[0]["max_severity"], "info")

        by_id = {m["segment_id"]: m for m in r["mappings"]}
        self.assertEqual(by_id[a["id"]]["relation"], "merged")
        self.assertEqual(by_id[a["id"]]["simulated_temp_ids"], [1])
        self.assertEqual(by_id[b["id"]]["relation"], "merged")
        self.assertEqual(by_id[b["id"]]["simulated_temp_ids"], [1])
        self.assertEqual(r["summary"]["merged_existing_segment_count"], 2)
        self.assertEqual(r["summary"]["changed_existing_segment_count"], 2)
        self.assertEqual(r["summary"]["split_existing_segment_count"], 0)
        self.assertEqual(r["summary"]["unchanged_existing_segment_count"], 0)

    def test_preview_equal_interval_all_unchanged(self):
        a = self.submit("s1", "e1", 0.0)["segment"]
        self.submit("s1", "e2", 8.0)
        b = self.submit("s1", "e3", 20.0)["segment"]

        r = self.preview("s1", SILENCE)
        self.assertEqual(len(r["simulated_segments"]), 2)
        self.assertTrue(all(m["relation"] == "unchanged" for m in r["mappings"]))
        self.assertEqual(
            [m["simulated_temp_ids"] for m in r["mappings"]], [[1], [2]]
        )
        self.assertEqual(r["summary"]["changed_existing_segment_count"], 0)
        self.assertEqual(r["summary"]["unchanged_existing_segment_count"], 2)
        # 模拟段与现有段边界一致
        sims = {s["temp_id"]: s for s in r["simulated_segments"]}
        self.assertEqual(sims[1]["event_count"], 2)
        self.assertEqual(sims[2]["event_count"], 1)

    def test_preview_boundary_gap_equal_to_interval_joins(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", SILENCE)  # 恰好等于间隔 → 同段
        r = self.preview("s1", SILENCE)
        self.assertEqual(len(r["simulated_segments"]), 1)

    def test_preview_max_severity_and_timestamps(self):
        self.submit("s1", "e1", 100.0, "info")
        self.submit("s1", "e2", 103.0, "critical")
        self.submit("s1", "e3", 95.0, "warning")  # 乱序到达
        r = self.preview("s1", 1000.0)
        seg = r["simulated_segments"][0]
        self.assertEqual(seg["started_at"], "1970-01-01T00:01:35+00:00")  # 95
        self.assertEqual(seg["ended_at"], "1970-01-01T00:01:43+00:00")    # 103
        self.assertEqual(seg["max_severity"], "critical")
        self.assertEqual(seg["event_count"], 3)

    def test_preview_stable_sort_tiebreaks_same_timestamp_by_event_id(self):
        self.submit("s1", "b", 0.0)
        self.submit("s1", "a", 0.0)    # 同刻事件
        self.submit("s1", "c", 100.0)  # 超静默 → 第二段
        r = self.preview("s1", 5.0)
        self.assertEqual(
            [s["temp_id"] for s in r["simulated_segments"]], [1, 2]
        )
        # 同刻事件必在同一段；temp_id 编号按 (occurred_at, event_id) 次序确定
        self.assertEqual(r["simulated_segments"][0]["event_count"], 2)

    def test_preview_is_isolated_by_source(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s2", "e2", 0.0)
        self.submit("s2", "e3", 100.0)
        r = self.preview("s2", 5.0)
        self.assertEqual(r["source"], "s2")
        self.assertEqual(r["event_count"], 2)
        self.assertEqual(len(r["simulated_segments"]), 2)
        self.assertEqual(r["summary"]["existing_segment_count"], 2)
        self.assertTrue(all(s["source"] == "s2" for s in r["simulated_segments"]))

    def test_preview_unknown_source_is_empty(self):
        self.submit("s1", "e1", 0.0)
        r = self.preview("nope", 10.0)
        self.assertEqual(r["event_count"], 0)
        self.assertEqual(r["simulated_segments"], [])
        self.assertEqual(r["mappings"], [])
        self.assertEqual(
            r["summary"],
            {
                "changed_existing_segment_count": 0,
                "split_existing_segment_count": 0,
                "merged_existing_segment_count": 0,
                "unchanged_existing_segment_count": 0,
                "existing_segment_count": 0,
                "simulated_segment_count": 0,
            },
        )

    def test_preview_repeated_requests_are_identical(self):
        for i in range(6):
            self.submit("s1", f"e{i}", float(i) * 7)
        r1 = self.preview("s1", 5.0)
        r2 = self.preview("s1", 5.0)
        self.assertEqual(r1, r2)

    def test_preview_does_not_mutate_data(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 8.0)
        self.submit("s1", "e3", 20.0)
        with session(self.db_path) as conn:
            segs_before = service.list_segments(conn)
            changes_before = service.list_segment_changes(conn)
            events_before = conn.execute("SELECT * FROM events ORDER BY id").fetchall()

        for candidate in (0.0, 5.0, 100.0):
            self.preview("s1", candidate)

        with session(self.db_path) as conn:
            segs_after = service.list_segments(conn)
            changes_after = service.list_segment_changes(conn)
            events_after = conn.execute("SELECT * FROM events ORDER BY id").fetchall()
        self.assertEqual(segs_after, segs_before)
        self.assertEqual(changes_after, changes_before)
        self.assertEqual(
            [tuple(e) for e in events_after], [tuple(e) for e in events_before]
        )

    def test_preview_read_only_session_rejects_writes(self):
        from app.db import read_only_session

        self.submit("s1", "e1", 0.0)
        with read_only_session(self.db_path) as conn:
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute(
                    "INSERT INTO segments "
                    "(source, start_ts, end_ts, max_severity, event_count, "
                    " revision, created_at, updated_at) "
                    "VALUES ('x', 0, 0, 'info', 1, 1, 0, 0)"
                )
        # 回滚释放后库仍可正常写入
        self.submit("s1", "e2", 8.0)


if __name__ == "__main__":
    unittest.main()
