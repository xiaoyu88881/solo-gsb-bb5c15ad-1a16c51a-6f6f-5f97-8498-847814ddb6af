"""FastAPI 应用入口。"""
from __future__ import annotations

import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from .config import Settings
from .db import init_db, session
from .schemas import EventIn, IngestResponse, SegmentChangesPage, SegmentOut
from . import service

# SQLite 单写者：进程内串行化归段写入，配合事务保证合并一致性
_ingest_lock = threading.Lock()


def create_app(settings: Settings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_db(settings.database_path)  # 启动即完成数据库初始化（幂等）
        yield

    app = FastAPI(
        title="设备告警事件段聚合服务",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok"}

    @app.post("/events", response_model=IngestResponse)
    def submit_event(event: EventIn) -> dict:
        """提交告警事件；按 source + 静默间隔归段，event_id 幂等去重。"""
        with _ingest_lock, session(settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.ingest_event(
                conn,
                source=event.source,
                event_id=event.event_id,
                occurred_ts=event.occurred_at.timestamp(),
                severity=event.severity,
                silence=settings.silence_interval_seconds,
            )

    @app.get("/segments/{segment_id}", response_model=SegmentOut)
    def get_segment(segment_id: int) -> dict:
        """查询事件段详情：起止时间、最高级别、事件数、revision。"""
        with session(settings.database_path) as conn:
            segment = service.get_segment(conn, segment_id)
        if segment is None:
            raise HTTPException(status_code=404, detail="segment not found")
        return segment

    @app.get("/segments", response_model=list[SegmentOut])
    def list_segments(source: str | None = Query(default=None)) -> list[dict]:
        """列出事件段，可按 source 过滤。"""
        with session(settings.database_path) as conn:
            return service.list_segments(conn, source)

    @app.get("/segment-changes", response_model=SegmentChangesPage)
    def list_segment_changes(
        after: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict:
        """增量同步变更流：按 change_id 升序返回 upsert/merged 记录。

        用上一页的 next_after 作为下一页的 after；change_id 单调递增，
        分页期间新写入的变更不会导致漏读或重复。
        """
        with session(settings.database_path) as conn:
            return service.list_segment_changes(conn, after=after, limit=limit)

    return app


app = create_app(Settings.from_env())
