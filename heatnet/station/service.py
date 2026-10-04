"""换热站批量调节服务。

批量调节沿一份固化的可续作计划逐站下发，每处理一个站之前都重新看一次中断信号：
只在循环开始前看一次、或在循环之后补一次，客户端超时取消之后剩下的站照样会被
改掉，现场看到的是"取消之后还有几个站的阀位变了"。

服务在普通下发之上还负责三件续作相关的事：进度逐站提交落盘、掉线恢复时读现场
实测阀位做核对、以及把已在位的站挡在幂等关外——续作时这些站只确认、不再动阀。
"""

from __future__ import annotations

from ..errors import InterlockError
from ..store.state import DurableState
from ..valve.valve import Valve
from .mix import apply_mix
from .progress import BatchProgress, BatchStore
from .station import HeatStation


class CancelToken:
    """批量下发的中断信号。

    ``kind`` 区分两种中断：``cancel`` 是客户端主动取消，计划内的部分失败，恢复时
    可直接续作；``offline`` 是掉线，属于非计划部分失败，恢复时必须先核对现场阀位
    再续，不能默认盘上记的就是现场真实状态。
    """

    CANCEL = "cancel"
    OFFLINE = "offline"

    def __init__(self, reason: str = "client-timeout", kind: str = CANCEL) -> None:
        self._cancelled = False
        self._reason = str(reason)
        if kind not in (self.CANCEL, self.OFFLINE):
            raise ValueError("中断类型必须是 cancel 或 offline")
        self._kind = kind

    def cancel(self) -> None:
        """置位中断。"""

        self._cancelled = True

    def cancelled(self) -> bool:
        """中断是否已经置位。"""

        return self._cancelled

    def reason(self) -> str:
        """返回中断原因。"""

        return self._reason

    def kind(self) -> str:
        """返回中断类型（cancel / offline）。"""

        return self._kind

    def offline(self) -> bool:
        """是否为掉线中断。"""

        return self._kind == self.OFFLINE


class StationService:
    """按分区批量调节换热站。"""

    def __init__(
        self,
        stations: dict[str, HeatStation],
        valves: dict[str, Valve],
        durable: DurableState,
        batches_store: BatchStore | None = None,
    ) -> None:
        self._stations = stations
        self._valves = valves
        self._durable = durable
        self._batches = batches_store
        self._batches_count = 0
        self._aborted = 0
        # 现场实测阀位覆盖表：正常情况下"现场阀位"就是内存阀门开度；掉线演练时由
        # 外部把现场人工扳动后的读数灌进来，核对逻辑必须读这张表，而不是只信内存。
        self._field: dict[str, float] = {}

    @property
    def durable(self) -> DurableState:
        return self._durable

    @property
    def batch_store(self) -> BatchStore | None:
        return self._batches

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

        return self._batches_count

    def aborted(self) -> int:
        """返回累计因取消或掉线而中断的批次数。"""

        return self._aborted

    def count_batch(self) -> int:
        """记一次开工。"""

        self._batches_count += 1
        return self._batches_count

    def count_abort(self) -> int:
        """记一次中断（取消或掉线）。"""

        self._aborted += 1
        return self._aborted

    # ---- 可续作批量 ---------------------------------------------------

    def set_field_reading(self, station_name: str, valve_pct: float) -> None:
        """灌入现场实测阀位，供掉线恢复核对（演练/巡检用）。"""

        self.get(station_name)
        self._field[station_name] = float(valve_pct)

    def field_position(self, station_name: str) -> float:
        """读取现场实际阀位：有实测读数时以现场为准，否则取内存阀门开度。"""

        self.valve(station_name)
        if station_name in self._field:
            return self._field[station_name]
        return self._valves[station_name].position_pct()

    def save_progress(self, progress: BatchProgress) -> str:
        """把批进度落盘；没有挂存储时拒绝，避免出现"看似可续、重启即丢"的批。"""

        if self._batches is None:
            raise InterlockError("批量下发没有挂进度存储，无法续作", component="station")
        return self._batches.save(progress)

    def load_progress(self, batch_id: str) -> BatchProgress:
        """按批次号读取进度，缺失即报错。"""

        if self._batches is None:
            raise InterlockError("批量下发没有挂进度存储", component="station")
        progress = self._batches.load(batch_id)
        if progress is None:
            raise KeyError(f"没有这批下发的进度: {batch_id}")
        return progress

    def active_progress(self) -> BatchProgress | None:
        """返回最近一批未完结的进度。"""

        if self._batches is None:
            return None
        return self._batches.active()

    def verify_station(self, progress: BatchProgress, station_name: str, tolerance: float = 1.5) -> dict:
        """核对一个已应用站的现场阀位是否仍是本批动作后记录的阀位。

        掉线属于部分失败：盘上记的阀位未必等于现场真实阀位（可能被人工扳动或设备
        漂移）。核对读现场读数，与"本批动作后落盘的实际阀位"比（不是与流量分配用
        的需求开度比——两者不是一个量）；落在到位容差内记为一致，否则记为漂移，由
        编排按续作粒度决定整区重放还是只纠这一站。
        """

        state = progress.applied.get(station_name)
        if state is None:
            raise KeyError(f"站尚未应用，无需核对: {station_name}")
        actual = self.field_position(station_name)
        match = abs(actual - state.valve_pct) <= tolerance
        progress.mark_verified(station_name, actual, match)
        return {
            "station": station_name,
            "target_pct": round(state.target_pct, 2),
            "recorded_pct": round(state.valve_pct, 2),
            "field_pct": round(actual, 2),
            "match": bool(match),
        }

    def prepare_cursors(self, progress: BatchProgress, offline: bool) -> dict[str, list[str]]:
        """续作前按各分区定死的粒度重置游标，返回每分区从游标起待处理的站名。"""

        return progress.prepare_cursors(offline)

    def apply_batch_station(
        self,
        progress: BatchProgress,
        station_name: str,
        dt: float = 1.0,
    ) -> dict:
        """按批计划对单个站应用一次，并提交该站进度。

        已经在位的站（已应用且未标记漂移）只做幂等确认，不再动阀，这是"已改好的
        不再下发"与"一个站不被前后两条指令各改一次"的落点；只有未应用、或掉线核对
        发现现场漂移的站才真正下发。
        """

        if dt <= 0:
            raise ValueError("步长必须为正")
        if not progress.needs_command(station_name):
            return {"station": station_name, "skipped": True, "steps": []}
        correcting = progress.is_applied(station_name)
        step = progress.step_of(station_name)
        station = self.get(station_name)
        valve = self.valve(station_name)
        detail = apply_mix(station, valve, self._durable, progress.target_c, dt)
        self._durable.save(
            f"station/{station_name}",
            {"zone": step.zone, "target_c": progress.target_c, "batch_id": progress.batch_id, "seq": step.seq},
        )
        progress.mark_applied(station_name, detail["valve_pct"])
        self.save_progress(progress)
        detail["batch_id"] = progress.batch_id
        detail["seq"] = step.seq
        detail["correcting"] = correcting
        return {
            "station": station_name,
            "zone": step.zone,
            "skipped": False,
            "correcting": correcting,
            "steps": [{"component": "station", "action": "adjust", "detail": detail}],
        }
