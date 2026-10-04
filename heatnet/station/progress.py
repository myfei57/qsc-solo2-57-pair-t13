"""批量下发的进度检查点。

一批下发在开工前固化成一份有序步骤计划（每步带全局序号 ``seq``），逐站执行、
逐站落盘进度。取消或掉线之后能不能"接着发、不重发"，全看这份检查点与现场阀位
是否对得上，因此它必须满足：

1. **计划不可变**：分区顺序、站顺序、每站目标与续作粒度在开工时写死；续作沿用
   同一份计划，不重新排序、不重新分配流量，避免同一站被前后两条指令各改一次。
2. **逐站提交**：一个站改完并落盘后，才把它记为已应用；崩溃只可能把游标停在某
   一步的"已动阀但未提交"上，恢复时对这一步做现场核对。
3. **续作粒度按分区定死**：每个分区带 ``station``（按站续作）或 ``zone``（整区
   重发）模式，随计划落盘，中途与跨重启都不可更改。
4. **每区一个游标**：``cursor[zone]`` 是该区"下一个要处理的站序号"。station 模式
   推进到最早未应用的站；zone 模式在没整区完成（或掉线核对未全过）时回退到分区
   起点。无论哪种模式，已经在位的站都只做幂等确认，不会再被物理动作改一次。

进度文档随进度原子替换（临时文件 + ``os.replace``），与阀门当前态共用同一套
原子存储，保证"内存、磁盘、审计"三份口径一致。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ..errors import InterlockError
from ..store.jsonfile import JsonStore

STATUS_RUNNING = "running"
STATUS_CANCELLED = "cancelled"
STATUS_OFFLINE = "offline"
STATUS_COMPLETED = "completed"

STATION = "station"
ZONE = "zone"
RESUME_MODES = (STATION, ZONE)

_TERMINAL = (STATUS_COMPLETED,)


@dataclass(frozen=True)
class BatchStep:
    """批计划里的一个站步骤。"""

    seq: int
    zone: str
    station: str
    target_pct: float

    def as_dict(self) -> dict:
        return {
            "seq": self.seq,
            "zone": self.zone,
            "station": self.station,
            "target_pct": round(self.target_pct, 4),
        }


@dataclass
class StationState:
    """一个站在本批里的执行状态。

    ``valve_pct`` 是应用后记录的阀位，掉线恢复核对时拿它跟现场实际阀位比对。
    ``verified`` 在掉线核对一致后置位；``mismatch`` 标记核对发现现场漂移，续作时
    必须重新驱动这一站纠偏，纠偏成功后清零。
    """

    seq: int
    target_pct: float
    valve_pct: float
    verified: bool = False
    mismatch: bool = False

    def as_dict(self) -> dict:
        return {
            "seq": self.seq,
            "target_pct": round(self.target_pct, 4),
            "valve_pct": round(self.valve_pct, 4),
            "verified": bool(self.verified),
            "mismatch": bool(self.mismatch),
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "StationState":
        return cls(
            seq=int(raw["seq"]),
            target_pct=float(raw.get("target_pct", 0.0)),
            valve_pct=float(raw.get("valve_pct", 0.0)),
            verified=bool(raw.get("verified", False)),
            mismatch=bool(raw.get("mismatch", False)),
        )


@dataclass
class BatchProgress:
    """一批下发的持久化进度。"""

    batch_id: str
    status: str
    steps: list[BatchStep]
    zones: list[str]
    zone_modes: dict[str, str]
    target_c: float
    demand_pct: float
    flows: list[dict] = field(default_factory=list)
    applied: dict[str, StationState] = field(default_factory=dict)
    cursor: dict[str, int] = field(default_factory=dict)
    reason: str = ""

    # ---- 构造 ---------------------------------------------------------

    @classmethod
    def create(
        cls,
        batch_id: str,
        steps: list[BatchStep],
        zone_modes: dict[str, str],
        target_c: float,
        demand_pct: float,
        flows: list[dict],
    ) -> "BatchProgress":
        """按有序步骤与分区模式新建一份运行中进度，每区游标从首站开始。"""

        zones: list[str] = []
        for step in steps:
            if step.zone not in zones:
                zones.append(step.zone)
        for zone, mode in zone_modes.items():
            if mode not in RESUME_MODES:
                raise InterlockError(f"未知续作粒度: {mode}", component="station")
        cursor = {zone: 0 for zone in zones}
        return cls(
            batch_id=str(batch_id),
            status=STATUS_RUNNING,
            steps=list(steps),
            zones=zones,
            zone_modes=dict(zone_modes),
            target_c=float(target_c),
            demand_pct=float(demand_pct),
            flows=list(flows),
            cursor=cursor,
        )

    # ---- 计划查询 -----------------------------------------------------

    def step_of(self, station: str) -> BatchStep:
        """按站名取它在本批里的步骤。"""

        for step in self.steps:
            if step.station == station:
                return step
        raise KeyError(f"站不在本批计划里: {station}")

    def zone_steps(self, zone: str) -> list[BatchStep]:
        """按计划顺序返回某分区的全部步骤。"""

        return [step for step in self.steps if step.zone == zone]

    def zone_mode(self, zone: str) -> str:
        """返回某分区定死的续作粒度。"""

        try:
            return self.zone_modes[zone]
        except KeyError as error:
            raise KeyError(f"分区没有续作粒度: {zone}") from error

    def resume_mode(self) -> str:
        """本批只涉及一种粒度时返回它，便于展示与校验。"""

        modes = {self.zone_modes[zone] for zone in self.zones}
        if len(modes) == 1:
            return next(iter(modes))
        return "mixed"

    # ---- 状态判定 -----------------------------------------------------

    def is_terminal(self) -> bool:
        """是否已完结（无需续作）。"""

        return self.status in _TERMINAL

    def is_applied(self, station: str) -> bool:
        """站是否已应用过本批指令。"""

        return station in self.applied

    def needs_command(self, station: str) -> bool:
        """站在续作时是否还需要真正下发一次。

        未应用的站要下发；已应用但掉线核对发现漂移的站要纠偏重发；已应用且在位
        （取消续作，或掉线核对一致）的站一律不再下发。
        """

        state = self.applied.get(station)
        return state is None or state.mismatch

    def applied_count(self) -> int:
        """已应用站数。"""

        return len(self.applied)

    def is_zone_done(self, zone: str) -> bool:
        """分区是否所有站都已应用且无待纠偏。"""

        members = self.zone_steps(zone)
        return bool(members) and all(
            step.station in self.applied and not self.applied[step.station].mismatch
            for step in members
        )

    def is_zone_verified(self, zone: str) -> bool:
        """分区是否所有已应用站都通过了现场核对（掉线续作用）。"""

        return all(
            self.applied[step.station].verified
            for step in self.zone_steps(zone)
            if step.station in self.applied
        )

    def first_unapplied(self) -> BatchStep | None:
        """计划顺序里最早一个仍需下发的步骤（未应用或待纠偏）。"""

        for step in self.steps:
            if self.needs_command(step.station):
                return step
        return None

    def applied_stations(self) -> list[str]:
        """按计划顺序返回已应用站名。"""

        return [step.station for step in self.steps if step.station in self.applied]

    # ---- 游标与状态迁移 -----------------------------------------------

    def earliest_pending(self, zone: str) -> int:
        """返回分区内最早一个仍需下发的站下标（没有则返回分区长度）。"""

        members = self.zone_steps(zone)
        for index, step in enumerate(members):
            if self.needs_command(step.station):
                return index
        return len(members)

    def advance_cursor(self, zone: str) -> int:
        """把某分区游标向后推到"下一个仍需下发的站"，返回新游标位置。"""

        members = self.zone_steps(zone)
        position = self.cursor.get(zone, 0)
        while position < len(members) and not self.needs_command(members[position].station):
            position += 1
        self.cursor[zone] = position
        return position

    def mark_applied(self, station: str, valve_pct: float) -> StationState:
        """登记一个站已应用（若此前是漂移纠偏，一并清掉 mismatch）。"""

        step = self.step_of(station)
        state = StationState(seq=step.seq, target_pct=step.target_pct, valve_pct=float(valve_pct))
        self.applied[station] = state
        self.advance_cursor(step.zone)
        if self.is_zone_done(step.zone):
            self.cursor[step.zone] = len(self.zone_steps(step.zone))
        if all(self.is_zone_done(zone) for zone in self.zones):
            self.status = STATUS_COMPLETED
        return state

    def mark_verified(self, station: str, valve_pct: float, match: bool) -> None:
        """登记掉线核对结果：一致置 verified，不一致置 mismatch 等待纠偏。"""

        state = self.applied[station]
        state.verified = bool(match)
        state.mismatch = not bool(match)
        state.valve_pct = float(valve_pct)

    def prepare_cursors(self, offline: bool) -> dict[str, list[str]]:
        """续作前按各分区定死的粒度重置游标，返回每个分区需要重发的站名。

        - ``station``：游标推到最早仍需下发的站。取消时即最早未应用站；掉线时还
          包含核对发现漂移、需要纠偏的已应用站。
        - ``zone``：分区没整区完成（取消），或掉线时任一站核对不一致，游标回退到
          分区起点按原顺序整区重放；已整区在位（掉线还须全部核对一致）则跳过。

        游标只决定"从哪一站开始看"；真正是否再动阀由 :meth:`needs_command` 与
        幂等应用把关，因此已经在位的站不会被再改一次。
        """

        replay: dict[str, list[str]] = {}
        for zone in self.zones:
            members = self.zone_steps(zone)
            done = self.is_zone_done(zone)
            if self.zone_modes[zone] == ZONE:
                replay_zone = not done or (offline and not self.is_zone_verified(zone))
                self.cursor[zone] = 0 if replay_zone else len(members)
            else:
                # station 模式：掉线核对可能把游标之前的站标成漂移，续作要回到最早
                # 仍需下发的站（前向只用于正常推进，恢复时必须能回头纠偏）。
                self.cursor[zone] = self.earliest_pending(zone)
            position = self.cursor[zone]
            replay[zone] = [step.station for step in members[position:]]
        return replay

    def suspend(self, status: str, reason: str) -> None:
        """把未完成的批挂起为取消或掉线；已完成不允许回退。"""

        if self.status == STATUS_COMPLETED:
            raise InterlockError("已完成的批次不能再挂起", component="station")
        if status not in (STATUS_CANCELLED, STATUS_OFFLINE):
            raise InterlockError(f"非法挂起状态: {status}", component="station")
        self.status = status
        self.reason = str(reason)

    def resume(self) -> None:
        """从取消或掉线恢复为运行中。"""

        if self.status not in (STATUS_CANCELLED, STATUS_OFFLINE):
            raise InterlockError(f"当前状态不允许续作: {self.status}", component="station")
        self.status = STATUS_RUNNING
        self.reason = ""

    # ---- 序列化 -------------------------------------------------------

    def as_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "status": self.status,
            "reason": self.reason,
            "target_c": round(self.target_c, 2),
            "demand_pct": round(self.demand_pct, 2),
            "resume_mode": self.resume_mode(),
            "zones": list(self.zones),
            "zone_modes": dict(self.zone_modes),
            "planned": len(self.steps),
            "applied_count": len(self.applied),
            "cursor": dict(self.cursor),
            "flows": list(self.flows),
            "steps": [step.as_dict() for step in self.steps],
            "applied": {name: state.as_dict() for name, state in self.applied.items()},
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "BatchProgress":
        steps = [
            BatchStep(
                seq=int(item["seq"]),
                zone=str(item["zone"]),
                station=str(item["station"]),
                target_pct=float(item.get("target_pct", 0.0)),
            )
            for item in raw.get("steps", [])
        ]
        zones = [str(item) for item in raw.get("zones", [])]
        progress = cls(
            batch_id=str(raw.get("batch_id", "")),
            status=str(raw.get("status", STATUS_RUNNING)),
            steps=steps,
            zones=zones,
            zone_modes={str(key): str(value) for key, value in raw.get("zone_modes", {}).items()},
            target_c=float(raw.get("target_c", 0.0)),
            demand_pct=float(raw.get("demand_pct", 0.0)),
            flows=list(raw.get("flows", [])),
            applied={
                str(name): StationState.from_dict(item)
                for name, item in raw.get("applied", {}).items()
            },
            cursor={str(key): int(value) for key, value in raw.get("cursor", {}).items()},
            reason=str(raw.get("reason", "")),
        )
        for zone in zones:
            progress.cursor.setdefault(zone, 0)
        return progress


class BatchStore:
    """批进度的原子文档存储。"""

    def __init__(self, store: JsonStore, prefix: str = "batch") -> None:
        self._store = store
        self._prefix = prefix.strip("/")

    @property
    def store(self) -> JsonStore:
        return self._store

    def doc_id(self, batch_id: str) -> str:
        """返回某批进度文档的标识。"""

        return f"{self._prefix}/{batch_id}"

    def save(self, progress: BatchProgress) -> str:
        """原子落盘一批进度，返回落盘路径。"""

        return self._store.write(self.doc_id(progress.batch_id), progress.as_dict())

    def load(self, batch_id: str) -> BatchProgress | None:
        """读取一批进度；文档缺失返回 ``None``。"""

        raw = self._store.read(self.doc_id(batch_id))
        if isinstance(raw, dict):
            return BatchProgress.from_dict(raw)
        return None

    def list_ids(self) -> list[str]:
        """列出全部已落盘的批次号（升序）。"""

        prefix = self._prefix + "/"
        return [doc[len(prefix) :] for doc in self._store.list_ids() if doc.startswith(prefix)]

    def active(self) -> BatchProgress | None:
        """返回最近一批未完结的进度（没有则 ``None``）。

        批次号按发放顺序升序，因此取未完结批次里序号最大的一个。进程崩溃时来不及
        把状态从 running 改成 offline，重启后正是靠这里把它找出来核对。
        """

        pending = [
            batch
            for batch_id in self.list_ids()
            if (batch := self.load(batch_id)) is not None and not batch.is_terminal()
        ]
        if not pending:
            return None
        return max(pending, key=lambda item: item.batch_id)
