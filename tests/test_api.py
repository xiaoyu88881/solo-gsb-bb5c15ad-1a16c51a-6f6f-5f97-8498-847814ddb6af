"""FastAPI 接口测试（需要 fastapi/httpx，随 requirements-dev.txt 安装）：

    pytest tests/test_api.py -v
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from app.config import Settings  # noqa: E402
from app.main import create_app  # noqa: E402


@pytest.fixture()
def client(tmp_path):
    settings = Settings(
        database_path=str(tmp_path / "api.db"),
        silence_interval_seconds=60.0,
    )
    with TestClient(create_app(settings)) as c:
        yield c


def test_submit_and_get_segment(client):
    resp = client.post("/events", json={
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "warning",
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["deduplicated"] is False
    seg = body["segment"]
    assert seg["event_count"] == 1
    assert seg["revision"] == 1
    assert seg["max_severity"] == "warning"
    assert seg["started_at"] == seg["ended_at"]

    got = client.get(f"/segments/{seg['id']}")
    assert got.status_code == 200
    assert got.json() == seg


def test_duplicate_submission_keeps_count(client):
    payload = {
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "info",
    }
    first = client.post("/events", json=payload).json()
    second = client.post("/events", json=payload).json()
    assert second["deduplicated"] is True
    assert second["segment"]["event_count"] == first["segment"]["event_count"] == 1


def test_late_out_of_order_event_bumps_revision(client):
    client.post("/events", json={
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:30Z", "severity": "info",
    })
    late = client.post("/events", json={
        "source": "gw-01", "event_id": "e2",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "error",
    }).json()
    seg = late["segment"]
    assert seg["revision"] == 2
    assert seg["max_severity"] == "error"
    # pydantic >=2.7 将 UTC 序列化为 ...Z，旧版为 ...+00:00，两者等价
    assert seg["started_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"
    assert seg["ended_at"].replace("Z", "+00:00") == "2026-09-20T08:00:30+00:00"
    assert seg["event_count"] == 2


def test_list_segments_filter_by_source(client):
    for source, eid in [("a", "e1"), ("b", "e2"), ("a", "e3")]:
        client.post("/events", json={
            "source": source, "event_id": eid,
            "occurred_at": "2026-09-20T08:00:00Z", "severity": "info",
        })
    assert len(client.get("/segments").json()) == 2
    segs = client.get("/segments", params={"source": "a"}).json()
    assert len(segs) == 1
    assert segs[0]["event_count"] == 2


def test_invalid_severity_rejected(client):
    resp = client.post("/events", json={
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "fatal-error",
    })
    assert resp.status_code == 422


def test_missing_field_rejected(client):
    resp = client.post("/events", json={"source": "gw-01", "event_id": "e1"})
    assert resp.status_code == 422


def test_get_missing_segment_404(client):
    assert client.get("/segments/9999").status_code == 404


def _post(client, source, event_id, occurred_at, severity="info"):
    resp = client.post("/events", json={
        "source": source, "event_id": event_id,
        "occurred_at": occurred_at, "severity": severity,
    })
    assert resp.status_code == 200
    return resp.json()


def test_segment_changes_empty_initially(client):
    assert client.get("/segment-changes").json() == {"changes": [], "next_after": 0}


def test_segment_changes_upsert_rules(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "warning")  # 创建 → upsert
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "error")    # 边界+级别 → upsert
    _post(client, "gw-01", "e3", "2026-09-20T08:00:10Z", "debug")    # 仅计数 → 无变更
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "error")    # 幂等重放 → 无变更

    page = client.get("/segment-changes").json()
    assert [c["type"] for c in page["changes"]] == ["upsert", "upsert"]
    first, second = page["changes"]
    assert first["change_id"] < second["change_id"]
    assert first["revision"] == 1
    assert second["revision"] == 2
    assert second["segment"]["max_severity"] == "error"
    assert second["segment"]["event_count"] == 2  # 变更时刻的快照（e3 未产生记录）
    assert page["next_after"] == second["change_id"]

    seg = client.get(f"/segments/{second['segment_id']}").json()
    assert seg["event_count"] == 3  # 段最终计数包含 e3


def test_segment_changes_merge_tombstone(client):
    a = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    b = _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z")["segment"]  # 超静默 → 新段
    _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z")                 # 桥接合并

    page = client.get("/segment-changes").json()
    assert [c["type"] for c in page["changes"]] == [
        "upsert", "upsert", "merged", "upsert",
    ]
    tomb = page["changes"][2]
    assert tomb["segment_id"] == b["id"]
    assert tomb["merged_into"] == a["id"]
    assert tomb["segment"] is None
    final = page["changes"][3]
    assert final["segment_id"] == a["id"]
    assert final["segment"]["event_count"] == 3


def test_segment_changes_pagination_while_ingesting(client):
    for i in range(3):  # 不同 source → 3 个独立段 → 3 条 upsert
        _post(client, f"gw-{i:02d}", "e1", "2026-09-20T08:00:00Z")

    p1 = client.get("/segment-changes", params={"after": 0, "limit": 2}).json()
    assert len(p1["changes"]) == 2
    assert p1["next_after"] == p1["changes"][-1]["change_id"]

    _post(client, "gw-03", "e1", "2026-09-20T08:00:00Z")  # 分页期间新写入

    p2 = client.get("/segment-changes", params={"after": p1["next_after"], "limit": 100})
    p2 = p2.json()
    ids = [c["change_id"] for c in p1["changes"] + p2["changes"]]
    assert ids == sorted(ids) and len(set(ids)) == 4  # 不漏读、不重复
    assert p2["next_after"] == ids[-1]

    # 空页：next_after 保持不变，可直接继续轮询
    p3 = client.get("/segment-changes", params={"after": p2["next_after"]}).json()
    assert p3 == {"changes": [], "next_after": p2["next_after"]}


def test_segment_changes_param_validation(client):
    assert client.get("/segment-changes", params={"after": -1}).status_code == 422
    assert client.get("/segment-changes", params={"limit": 0}).status_code == 422
    assert client.get("/segment-changes", params={"limit": 1001}).status_code == 422


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


# ---- 静默间隔重分段预演（只读）----

def test_resegment_preview_merge_split_and_readonly(client):
    # 服务 silence=60：0s、30s 同段 A；120s 与前段相距 90>60 → 段 B
    a = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z")
    b = _post(client, "gw-01", "e3", "2026-09-20T08:02:00Z")["segment"]

    # 候选间隔 120s：30->120 相距 90<=120 → A、B 合并为一个模拟段
    resp = client.post("/segments/resegment-preview", json={
        "source": "gw-01", "silence_interval_seconds": 120,
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["source"] == "gw-01"
    assert body["silence_interval_seconds"] == 120.0
    assert len(body["simulated_segments"]) == 1
    sim = body["simulated_segments"][0]
    assert sim["temp_id"] == 1
    assert sim["event_count"] == 3
    # pydantic >=2.7 将 UTC 序列化为 ...Z，旧版为 ...+00:00，两者等价
    assert sim["started_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"
    assert sim["ended_at"].replace("Z", "+00:00") == "2026-09-20T08:02:00+00:00"
    assert [m["segment_id"] for m in body["mappings"]] == [a["id"], b["id"]]
    assert all(m["status"] == "merged" for m in body["mappings"])
    assert all(m["simulated_temp_ids"] == [1] for m in body["mappings"])
    assert body["affected_count"] == 2
    assert (body["unchanged_count"], body["split_count"], body["merged_count"]) == (0, 0, 2)

    # 候选间隔 20s：0->30 断开、30->120 断开 → A(e1,e2) 拆为 temp1/temp2
    resp = client.post("/segments/resegment-preview", json={
        "source": "gw-01", "silence_interval_seconds": 20,
    })
    body = resp.json()
    assert [(s["temp_id"], s["event_count"]) for s in body["simulated_segments"]] == [
        (1, 1), (2, 1), (3, 1),
    ]
    amap = next(m for m in body["mappings"] if m["segment_id"] == a["id"])
    bmap = next(m for m in body["mappings"] if m["segment_id"] == b["id"])
    assert amap["status"] == "split" and amap["simulated_temp_ids"] == [1, 2]
    assert bmap["status"] == "unchanged" and bmap["simulated_temp_ids"] == [3]
    assert body["affected_count"] == 1
    assert (body["unchanged_count"], body["split_count"], body["merged_count"]) == (1, 1, 0)

    # 预演不改数据：现有段与变更流保持原样
    assert len(client.get("/segments", params={"source": "gw-01"}).json()) == 2
    page = client.get("/segment-changes").json()
    assert [c["type"] for c in page["changes"]] == ["upsert", "upsert", "upsert"]


def test_resegment_preview_same_silence_all_unchanged(client):
    seg = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "error")["segment"]
    body = client.post("/segments/resegment-preview", json={
        "source": "gw-01", "silence_interval_seconds": 60,
    }).json()
    assert len(body["simulated_segments"]) == 1
    sim = body["simulated_segments"][0]
    assert sim["event_count"] == 1
    assert sim["max_severity"] == "error"
    assert body["mappings"] == [
        {"segment_id": seg["id"], "status": "unchanged", "simulated_temp_ids": [1]}
    ]
    assert body["affected_count"] == 0
    assert body["unchanged_count"] == 1


def test_resegment_preview_is_deterministic(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post(client, "gw-01", "e2", "2026-09-20T08:01:00Z")
    payload = {"source": "gw-01", "silence_interval_seconds": 30}
    first = client.post("/segments/resegment-preview", json=payload).json()
    second = client.post("/segments/resegment-preview", json=payload).json()
    assert first == second


def test_resegment_preview_unknown_source_empty(client):
    body = client.post("/segments/resegment-preview", json={
        "source": "ghost", "silence_interval_seconds": 60,
    }).json()
    assert body == {
        "source": "ghost",
        "silence_interval_seconds": 60.0,
        "simulated_segments": [],
        "mappings": [],
        "affected_count": 0,
        "unchanged_count": 0,
        "split_count": 0,
        "merged_count": 0,
    }


def test_resegment_preview_param_validation(client):
    # 负间隔被拒
    resp = client.post("/segments/resegment-preview", json={
        "source": "gw-01", "silence_interval_seconds": -1,
    })
    assert resp.status_code == 422
    # 空 source 被拒
    resp = client.post("/segments/resegment-preview", json={
        "source": "", "silence_interval_seconds": 60,
    })
    assert resp.status_code == 422
    # 缺字段被拒
    resp = client.post("/segments/resegment-preview", json={"source": "gw-01"})
    assert resp.status_code == 422
