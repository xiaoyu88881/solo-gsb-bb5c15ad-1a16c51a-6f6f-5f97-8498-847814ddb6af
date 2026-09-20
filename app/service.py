"""事件归段核心逻辑（仅依赖标准库，便于独立测试）。

归段规则：
- 同一 source 下，事件 occurred_at 与某事件段的 [start, end] 相距不超过
  silence 秒（含段外前后各 silence 的扩展窗口）即归入该段；
- 乱序/迟到事件可能同时落在多个段的扩展窗口内，或桥接相邻段，
  此时把这些段合并为一段（链式传导，直到不再扩大）；
- (source, event_id) 唯一，重复提交直接返回原段，不改变任何计数；
- revision 初始为 1，仅当新事件导致段的起止边界或最高级别发生变化时递增，
  仅事件数增加不递增。

变更流（供下游增量同步）：
- 每次创建段、边界或最高级别变化（revision 递增）、多段合并，都在事件写入
  事务内向 segment_changes 追加记录，change_id 单调递增；
- 合并时为每个被删除段写一条指向保留段的 merged 墓碑；
- 仅事件数增加（revision 不变）不写 upsert；event_id 幂等重放不写任何记录。
"""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone

from . import severity as sev


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _row_to_segment(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "source": row["source"],
        "started_at": _iso(row["start_ts"]),
        "ended_at": _iso(row["end_ts"]),
        "max_severity": row["max_severity"],
        "event_count": row["event_count"],
        "revision": row["revision"],
    }


def get_segment(conn: sqlite3.Connection, segment_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM segments WHERE id = ?", (segment_id,)
    ).fetchone()
    return _row_to_segment(row) if row else None


def list_segments(conn: sqlite3.Connection, source: str | None = None) -> list[dict]:
    if source is None:
        rows = conn.execute("SELECT * FROM segments ORDER BY id").fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM segments WHERE source = ? ORDER BY id", (source,)
        ).fetchall()
    return [_row_to_segment(r) for r in rows]


def preview_resegment(
    conn: sqlite3.Connection, *, source: str, silence: float
) -> dict:
    """静默间隔重分段预演（只读，不写任何表）。

    调用方需持有只读事务（见 db.read_session）。在同一读事务内读取该 source
    的全部原始事件，按 (occurred_at, event_id) 稳定排序后，用候选间隔
    重新扫描分段：相邻事件时间差 > silence 处断开，<= silence 归入同一段
    （与在线归段“落在彼此 silence 窗口内即同段”的最终划分一致）。

    再依据事件成员关系，给出每个现有段到模拟段的映射：
    - unchanged：现有段与某模拟段事件集合完全相同；
    - split：现有段的事件散落到 >=2 个模拟段；
    - merged：现有段事件全部落入 1 个模拟段，但该模拟段还含其他现有段的事件。

    结果仅依赖已提交数据，重复调用确定一致。
    """
    # 同一读事务内：该来源的全部原始事件（稳定排序）+ 现有段
    event_rows = conn.execute(
        """
        SELECT event_id, occurred_at, severity, segment_id
        FROM events
        WHERE source = ?
        ORDER BY occurred_at, event_id
        """,
        (source,),
    ).fetchall()
    segment_rows = conn.execute(
        "SELECT id FROM segments WHERE source = ? ORDER BY id",
        (source,),
    ).fetchall()

    # 稳定排序后重新扫描分段
    groups: list[list[sqlite3.Row]] = []
    for row in event_rows:
        if (
            not groups
            or row["occurred_at"] - groups[-1][-1]["occurred_at"] > silence
        ):
            groups.append([row])
        else:
            groups[-1].append(row)

    # 构建模拟段（临时编号按时间升序从 1 开始）及其事件集合
    simulated: list[dict] = []
    sim_event_sets: list[set[tuple[float, str]]] = []
    # 事件 (occurred_at, event_id) -> 模拟段临时编号
    temp_by_event: dict[tuple[float, str], int] = {}
    for temp_id, members in enumerate(groups, start=1):
        start_ts = members[0]["occurred_at"]
        end_ts = members[-1]["occurred_at"]
        sim_event_sets.append(set())
        for m in members:
            key = (m["occurred_at"], m["event_id"])
            sim_event_sets[-1].add(key)
            temp_by_event[key] = temp_id
        simulated.append(
            {
                "temp_id": temp_id,
                "source": source,
                "started_at": _iso(start_ts),
                "ended_at": _iso(end_ts),
                "max_severity": sev.highest(*[m["severity"] for m in members]),
                "event_count": len(members),
            }
        )

    # 现有段 -> 其事件落入的模拟段（升序去重），按事件成员关系判定映射
    mappings: list[dict] = []
    split_count = merged_count = unchanged_count = 0
    for seg_row in segment_rows:
        seg_id = seg_row["id"]
        old_event_set = {
            (r["occurred_at"], r["event_id"])
            for r in event_rows
            if r["segment_id"] == seg_id
        }
        temp_ids = sorted({temp_by_event[k] for k in old_event_set})
        if len(temp_ids) >= 2:
            status = "split"
            split_count += 1
        elif len(temp_ids) == 1 and old_event_set == sim_event_sets[temp_ids[0] - 1]:
            # 事件集合完全相同（模拟段的事件全部来自该现有段）
            status = "unchanged"
            unchanged_count += 1
        else:
            # 全部落入一个模拟段，但该模拟段还含其他现有段的事件
            # （该 source 无事件的空段亦归此，模拟划分中没有对应段）
            status = "merged"
            merged_count += 1
        mappings.append(
            {"segment_id": seg_id, "status": status, "simulated_temp_ids": temp_ids}
        )

    return {
        "source": source,
        "silence_interval_seconds": silence,
        "simulated_segments": simulated,
        "mappings": mappings,
        "affected_count": split_count + merged_count,
        "unchanged_count": unchanged_count,
        "split_count": split_count,
        "merged_count": merged_count,
    }


def _record_change(
    conn: sqlite3.Connection,
    *,
    change_type: str,
    segment_id: int,
    now: float,
    merged_into: int | None = None,
    revision: int | None = None,
    source: str | None = None,
    start_ts: float | None = None,
    end_ts: float | None = None,
    max_severity: str | None = None,
    event_count: int | None = None,
) -> None:
    """在事件写入事务内追加一条变更记录（change_id 由 AUTOINCREMENT 保证单调递增）。"""
    conn.execute(
        """
        INSERT INTO segment_changes
            (type, segment_id, merged_into, revision, source,
             start_ts, end_ts, max_severity, event_count, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            change_type, segment_id, merged_into, revision, source,
            start_ts, end_ts, max_severity, event_count, now,
        ),
    )


def _row_to_change(row: sqlite3.Row) -> dict:
    change = {
        "change_id": row["change_id"],
        "type": row["type"],
        "segment_id": row["segment_id"],
        "merged_into": row["merged_into"],
        "revision": row["revision"],
        "segment": None,
    }
    if row["type"] == "upsert":
        change["segment"] = {
            "id": row["segment_id"],
            "source": row["source"],
            "started_at": _iso(row["start_ts"]),
            "ended_at": _iso(row["end_ts"]),
            "max_severity": row["max_severity"],
            "event_count": row["event_count"],
            "revision": row["revision"],
        }
    return change


def list_segment_changes(
    conn: sqlite3.Connection, *, after: int = 0, limit: int = 100
) -> dict:
    """按 change_id 升序返回变更流的一页：{"changes": [...], "next_after": int}。

    键集分页（change_id > after）：change_id 单调递增，分页期间新写入的变更
    只会获得更大的 change_id、出现在后续页，因此不漏读也不重复。
    next_after 可直接作为下一页的 after；空页时与入参 after 相同，便于轮询。
    """
    rows = conn.execute(
        """
        SELECT * FROM segment_changes
        WHERE change_id > ?
        ORDER BY change_id
        LIMIT ?
        """,
        (after, limit),
    ).fetchall()
    changes = [_row_to_change(r) for r in rows]
    next_after = changes[-1]["change_id"] if changes else after
    return {"changes": changes, "next_after": next_after}


def _find_merge_candidates(
    conn: sqlite3.Connection, source: str, occurred_ts: float, silence: float
) -> list[sqlite3.Row]:
    """找出应与新事件合并的所有段。

    新事件的初始窗口为 [occurred - silence, occurred + silence]；
    每并入一个段，窗口随该段边界继续向外扩展，循环直至收敛，
    从而正确处理一个迟到事件桥接多个段的链式情形。
    """
    lo, hi = occurred_ts - silence, occurred_ts + silence
    merged: dict[int, sqlite3.Row] = {}
    while True:
        rows = conn.execute(
            """
            SELECT * FROM segments
            WHERE source = ? AND start_ts <= ? AND end_ts >= ?
            ORDER BY id
            """,
            (source, hi, lo),
        ).fetchall()
        fresh = {r["id"]: r for r in rows if r["id"] not in merged}
        if not fresh:
            return [merged[k] for k in sorted(merged)]
        merged.update(fresh)
        lo = min([lo] + [r["start_ts"] - silence for r in fresh.values()])
        hi = max([hi] + [r["end_ts"] + silence for r in fresh.values()])


def ingest_event(
    conn: sqlite3.Connection,
    *,
    source: str,
    event_id: str,
    occurred_ts: float,
    severity: str,
    silence: float,
) -> dict:
    """写入一个事件并返回 {"deduplicated": bool, "segment": {...}}。

    调用方需持有事务（建议 BEGIN IMMEDIATE）并保证串行化。
    """
    now = time.time()

    dup = conn.execute(
        "SELECT segment_id FROM events WHERE source = ? AND event_id = ?",
        (source, event_id),
    ).fetchone()
    if dup:
        return {"deduplicated": True, "segment": get_segment(conn, dup["segment_id"])}

    candidates = _find_merge_candidates(conn, source, occurred_ts, silence)

    if not candidates:
        cur = conn.execute(
            """
            INSERT INTO segments
                (source, start_ts, end_ts, max_severity, event_count, revision,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, 1, 1, ?, ?)
            """,
            (source, occurred_ts, occurred_ts, severity, now, now),
        )
        segment_id = cur.lastrowid
        _record_change(
            conn,
            change_type="upsert",
            segment_id=segment_id,
            now=now,
            revision=1,
            source=source,
            start_ts=occurred_ts,
            end_ts=occurred_ts,
            max_severity=severity,
            event_count=1,
        )
    else:
        survivor = candidates[0]  # id 最小者作为保留段
        segment_id = survivor["id"]

        start_ts = min([occurred_ts] + [c["start_ts"] for c in candidates])
        end_ts = max([occurred_ts] + [c["end_ts"] for c in candidates])
        max_severity = sev.highest(severity, *[c["max_severity"] for c in candidates])
        event_count = 1 + sum(c["event_count"] for c in candidates)

        changed = (
            start_ts != survivor["start_ts"]
            or end_ts != survivor["end_ts"]
            or max_severity != survivor["max_severity"]
        )
        revision = max(c["revision"] for c in candidates) + (1 if changed else 0)

        others = [c["id"] for c in candidates[1:]]
        if others:
            marks = ",".join("?" * len(others))
            conn.execute(
                f"UPDATE events SET segment_id = ? WHERE segment_id IN ({marks})",
                [segment_id, *others],
            )
            conn.execute(f"DELETE FROM segments WHERE id IN ({marks})", others)
            # 每个被删除段一条 merged 墓碑，指向保留段
            for deleted_id in others:
                _record_change(
                    conn,
                    change_type="merged",
                    segment_id=deleted_id,
                    merged_into=segment_id,
                    now=now,
                )

        conn.execute(
            """
            UPDATE segments
            SET start_ts = ?, end_ts = ?, max_severity = ?,
                event_count = ?, revision = ?, updated_at = ?
            WHERE id = ?
            """,
            (start_ts, end_ts, max_severity, event_count, revision, now, segment_id),
        )
        if changed:
            # 边界或最高级别变化（revision 递增）才写 upsert；
            # 仅事件数增加时不产生变更记录
            _record_change(
                conn,
                change_type="upsert",
                segment_id=segment_id,
                now=now,
                revision=revision,
                source=source,
                start_ts=start_ts,
                end_ts=end_ts,
                max_severity=max_severity,
                event_count=event_count,
            )

    conn.execute(
        """
        INSERT INTO events (segment_id, source, event_id, occurred_at, severity, received_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (segment_id, source, event_id, occurred_ts, severity, now),
    )
    return {"deduplicated": False, "segment": get_segment(conn, segment_id)}
