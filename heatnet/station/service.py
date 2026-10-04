"""换热站批量调节服务。

批量调节按批次日志逐站下发，每处理一个站之前都要重新看一次取消信号：只在循环
开始前看一次、或者在循环之后补一次，客户端超时取消之后剩下的站照样会被改掉，
现场看到的是"取消之后还有几个站的阀位变了"。单站执行与批次计数都收敛在这里，
取消与续作的判定由 :mod:`~heatnet.station.resume` 统一负责。
"""

from __future__ import annotations

from ..store.state import DurableState
from ..valve.valve import Valve
from .mix import apply_mix
from .station import HeatStation


class CancelToken:
    """批量下发的取消信号。"""

    def __init__(self, reason: str = "client-timeout") -> None:
        self._cancelled = False
        self._reason = str(reason)

    def cancel(self) -> None:
        """置位取消。"""

        self._cancelled = True

    def cancelled(self) -> bool:
        """取消是否已经置位。"""

        return self._cancelled

    def reason(self) -> str:
        """返回取消原因。"""

        return self._reason


class StationService:
    """换热站执行器：单站调节、批量计数与设备索引。"""

    def __init__(
        self,
        stations: dict[str, HeatStation],
        valves: dict[str, Valve],
        durable: DurableState,
    ) -> None:
        self._stations = stations
        self._valves = valves
        self._durable = durable
        self._batches = 0
        self._aborted = 0

    @property
    def durable(self) -> DurableState:
        return self._durable

    def names(self) -> list[str]:
        """返回全部站名（排序后）。"""

        return sorted(self._stations)

    def get(self, name: str) -> HeatStation:
        """按名取站。"""

        try:
            return self._stations[name]
        except KeyError as error:
            raise KeyError(f"未登记换热站: {name}") from error

    def valve(self, name: str) -> Valve:
        """按名取站的混水阀。"""

        try:
            return self._valves[name]
        except KeyError as error:
            raise KeyError(f"换热站没有混水阀: {name}") from error

    def batches(self) -> int:
        """返回累计批量调节次数。"""

        return self._batches

    def aborted(self) -> int:
        """返回累计因取消而中断的次数。"""

        return self._aborted

    def note_batch(self) -> int:
        """记一次批量调节执行（含续作）。"""

        self._batches += 1
        return self._batches

    def note_aborted(self) -> int:
        """记一次因取消而中断。"""

        self._aborted += 1
        return self._aborted

    def apply_station(
        self,
        station_name: str,
        target_c: float,
        dt: float = 1.0,
        demand_pct: float | None = None,
    ) -> dict:
        """调节单个换热站；批次执行时按计划开度下发，缺省按当前温差现算。"""

        if dt <= 0:
            raise ValueError("步长必须为正")
        station = self.get(station_name)
        valve = self.valve(station_name)
        detail = apply_mix(station, valve, self._durable, target_c, dt, demand_pct=demand_pct)
        self._durable.save(
            f"station/{station_name}", {"zone": station.zone(), "target_c": target_c}
        )
        return detail
