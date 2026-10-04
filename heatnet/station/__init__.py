"""换热站。

换热站把一次网高温水与二次网回水按混水阀开度混合，送出设定点温度的二次侧供水。
批量调节按批次日志逐站下发，每一步先落日志再动阀、每站之前重新检查取消信号；取消
或掉线后按日志续作，已确认的站不再下发。站与分区的对应关系来自
:class:`~heatnet.station.mapping.StationMapping`，分区拆分后必须重建。
"""

from __future__ import annotations

from .batch import build_plan, run_batch
from .mapping import StationMapping
from .mix import apply_mix
from .service import CancelToken, StationService
from .station import HeatStation
from .state import StationSnapshot

__all__ = [
    "CancelToken",
    "HeatStation",
    "StationMapping",
    "StationService",
    "StationSnapshot",
    "apply_mix",
    "build_plan",
    "run_batch",
]
