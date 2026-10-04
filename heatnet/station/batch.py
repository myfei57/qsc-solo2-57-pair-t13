"""批量调节编排。

先按分区优先级排出下发顺序并算出计划，再交给一次网按站独立分配流量，最后按批次
日志逐站下发。流量分配与阀门动作分两步做，是为了让"计划里的流量"与"执行后的阀位"
可以分别核对；每一步动作都先落日志再执行，取消或掉线后才能按日志接着补，而不是
整批重发一遍。
"""

from __future__ import annotations

from ..eventbus import EventBus
from ..ns.zone import ZoneRegistry
from ..primary.network import PrimaryNetwork
from ..primary.order import priority_order
from .journal import BatchJournal
from .mapping import StationMapping
from .resume import BatchRunner
from .service import CancelToken, StationService


def build_plan(
    mapping: StationMapping,
    zones: ZoneRegistry,
    zone_names: list[str],
    demand_pct: float,
) -> list[tuple[str, float]]:
    """按分区优先级排出各站的调节计划。"""

    ordered = priority_order([(name, zones.get(name).setpoint_c) for name in zone_names])
    peak = max((setpoint for _, setpoint in ordered), default=0.0)
    plan: list[tuple[str, float]] = []
    for zone_name, setpoint in ordered:
        share = 1.0 if peak <= 0 else max(0.2, setpoint / peak)
        demand = max(0.0, min(100.0, float(demand_pct) * share))
        for station in mapping.stations_in(zone_name):
            plan.append((station, demand))
    return plan


def run_batch(
    service: StationService,
    mapping: StationMapping,
    zones: ZoneRegistry,
    network: PrimaryNetwork,
    zone_names: list[str],
    demand_pct: float,
    target_c: float,
    cancel: CancelToken | None = None,
    dt: float = 1.0,
    journal: BatchJournal | None = None,
    bus: EventBus | None = None,
    ts: float = 0.0,
) -> dict:
    """建批、按日志逐站下发、汇总结果。"""

    if dt <= 0:
        raise ValueError("步长必须为正")
    journal = journal or BatchJournal(service.durable.store)
    runner = BatchRunner(service, mapping, zones, network, journal, bus)
    plan = build_plan(mapping, zones, zone_names, demand_pct)
    batch = runner.open_batch(plan, zone_names, demand_pct, target_c, ts=ts)
    return runner.run(batch["batch_id"], cancel=cancel, dt=dt, ts=ts)
