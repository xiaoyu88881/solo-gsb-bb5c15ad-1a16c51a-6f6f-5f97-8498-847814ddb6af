"""API 请求/响应模型。"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from . import severity as sev


class EventIn(BaseModel):
    source: str = Field(min_length=1, max_length=256, examples=["edge-gateway-01"])
    event_id: str = Field(min_length=1, max_length=256, examples=["evt-0001"])
    occurred_at: datetime = Field(examples=["2026-09-20T08:00:00Z"])
    severity: str = Field(examples=["warning"])

    @field_validator("severity")
    @classmethod
    def _normalize_severity(cls, v: str) -> str:
        try:
            return sev.normalize(v)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc

    @field_validator("occurred_at")
    @classmethod
    def _assume_utc(cls, v: datetime) -> datetime:
        # 无时区的时间按 UTC 处理
        return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v


class SegmentOut(BaseModel):
    id: int
    source: str
    started_at: datetime
    ended_at: datetime
    max_severity: str
    event_count: int
    revision: int


class IngestResponse(BaseModel):
    deduplicated: bool
    segment: SegmentOut


class SegmentChange(BaseModel):
    """一条段变更记录。

    - upsert：段创建或边界/最高级别变化，segment 为变更时刻的完整快照；
    - merged：墓碑，segment_id 段已被删除并并入 merged_into 指向的保留段。
    """

    change_id: int
    type: Literal["upsert", "merged"]
    segment_id: int
    merged_into: int | None = None
    revision: int | None = None
    segment: SegmentOut | None = None


class SegmentChangesPage(BaseModel):
    """变更流一页；next_after 作为下一页的 after 继续拉取。"""

    changes: list[SegmentChange]
    next_after: int


class ResegmentPreviewIn(BaseModel):
    """静默间隔重分段预演请求：指定来源与候选静默间隔（秒）。"""

    source: str = Field(min_length=1, max_length=256, examples=["edge-gateway-01"])
    silence_interval_seconds: float = Field(
        ge=0, examples=[120], description="候选静默间隔（秒）"
    )

    @field_validator("silence_interval_seconds")
    @classmethod
    def _must_be_finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("silence_interval_seconds 必须是有限数值")
        return v


class SimulatedSegmentOut(BaseModel):
    """按候选间隔重新计算得到的模拟段（仅存在于响应中，不入库）。"""

    temp_id: int = Field(description="模拟段临时编号，按时间升序从 1 开始")
    source: str
    started_at: datetime
    ended_at: datetime
    max_severity: str
    event_count: int


class SegmentMappingOut(BaseModel):
    """单个现有段到模拟段的映射，依据事件成员关系判定。"""

    segment_id: int
    status: Literal["unchanged", "split", "merged"]
    simulated_temp_ids: list[int] = Field(
        description="该现有段事件落入的模拟段临时编号（升序）"
    )


class ResegmentPreviewResponse(BaseModel):
    """重分段预演结果：模拟段、现有段映射与受影响段汇总。"""

    source: str
    silence_interval_seconds: float
    simulated_segments: list[SimulatedSegmentOut]
    mappings: list[SegmentMappingOut]
    affected_count: int = Field(
        description="受影响（split 或 merged）的现有段数"
    )
    unchanged_count: int
    split_count: int
    merged_count: int
