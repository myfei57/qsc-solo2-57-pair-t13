"""换热站。

换热站把一次网高温水与二次网回水按混水阀开度混合，送出设定点温度的二次侧供水。
批量调节按分区下发并可续作：计划按分区优先级固化落盘，逐站执行、逐站提交进度，
取消可直接续、掉线先核对现场再续，续作粒度（按站/按区）由各分区定死。站与分区
的对应关系来自 :class:`~heatnet.station.mapping.StationMapping`，分区拆分后必须
重建。
"""

from __future__ import annotations

from .batch import build_plan, build_steps, open_batch, resume_batch, run_batch
from .mapping import StationMapping
from .mix import apply_mix
from .progress import (
    STATION,
    ZONE,
    BatchProgress,
    BatchStep,
    BatchStore,
    StationState,
)
from .service import CancelToken, StationService
from .station import HeatStation
from .state import StationSnapshot

__all__ = [
    "STATION",
    "ZONE",
    "BatchProgress",
    "BatchStep",
    "BatchStore",
    "CancelToken",
    "HeatStation",
    "StationMapping",
    "StationService",
    "StationSnapshot",
    "StationState",
    "apply_mix",
    "build_plan",
    "build_steps",
    "open_batch",
    "resume_batch",
    "run_batch",
]
