"""批量调节编排（可续作）。

一批下发分三段：

1. **排计划**：按分区优先级排出固定的站顺序，算好每站流量与混水阀目标开度，连同
   各分区"定死"的续作粒度一起固化成进度文档落盘。计划一旦开工就不再变化——续作
   不重排、不重新分配流量，这是"续作时序不能乱"的前提。
2. **逐站执行**：一次网按站独立分配流量后，按计划顺序逐区逐站下发。每站先落盘
   阀门当前态、再改内存，改完把该站序号提交进进度并原子落盘；每一步之前重新看
   中断信号。
3. **中断与续作**：取消（cancel）可直接续；掉线（offline）属部分失败，恢复时先
   逐站核对现场阀位，再按分区粒度决定续作范围——``station`` 模式只补没改好的站，
   ``zone`` 模式把没整区改好的分区从头重发，已全部改好（且核对一致）的分区跳过。

任何续作都沿同一份计划顺序推进，已提交的站不会再收到第二条指令，因此不会出现
一个站被前后两条指令各改一次。
"""

from __future__ import annotations

from ..errors import InterlockError
from ..ids import IdFactory
from ..ns.zone import ZoneRegistry
from ..primary.flow import StationFlow, batch_flows
from ..primary.network import PrimaryNetwork
from ..primary.order import priority_order
from .mapping import StationMapping
from .progress import (
    STATUS_CANCELLED,
    STATUS_COMPLETED,
    STATUS_OFFLINE,
    BatchProgress,
    BatchStep,
)
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


def _zone_order(zones: ZoneRegistry, zone_names: list[str]) -> list[str]:
    ordered = priority_order([(name, zones.get(name).setpoint_c) for name in zone_names])
    return [name for name, _ in ordered]


def build_steps(
    mapping: StationMapping,
    zones: ZoneRegistry,
    zone_names: list[str],
    demand_pct: float,
) -> tuple[list[BatchStep], dict[str, str]]:
    """按分区优先级构造带序号的站步骤，并带出各分区定死的续作粒度。"""

    plan = build_plan(mapping, zones, zone_names, demand_pct)
    target_by_station = dict(plan)
    steps: list[BatchStep] = []
    zone_modes: dict[str, str] = {}
    seq = 0
    for zone_name in _zone_order(zones, zone_names):
        zone = zones.get(zone_name)
        zone_modes[zone_name] = zone.resume_mode
        for station in mapping.stations_in(zone_name):
            seq += 1
            steps.append(BatchStep(seq, zone_name, station, target_by_station[station]))
    return steps, zone_modes


def open_batch(
    service: StationService,
    mapping: StationMapping,
    zones: ZoneRegistry,
    network: PrimaryNetwork,
    zone_names: list[str],
    demand_pct: float,
    target_c: float,
    batch_ids: IdFactory,
) -> tuple[BatchProgress, list[StationFlow]]:
    """固化计划、分配流量并落盘一份运行中进度。

    存在未完结批次时拒绝再开新批：同一时刻只允许一条在途下发，避免两条下发交错
    给同一个站发指令。
    """

    if service.active_progress() is not None:
        active = service.active_progress()
        raise InterlockError(f"已有未完结的批量下发: {active.batch_id}", component="station")
    steps, zone_modes = build_steps(mapping, zones, zone_names, demand_pct)
    plan = [(step.station, step.target_pct) for step in steps]
    flows = batch_flows(network, plan)
    network.count_dispatch()
    progress = BatchProgress.create(
        batch_ids.next(),
        steps,
        zone_modes,
        target_c,
        demand_pct,
        [flow.as_dict() for flow in flows],
    )
    service.save_progress(progress)
    service.count_batch()
    return progress, flows


def _suspend(progress: BatchProgress, cancel: CancelToken, service: StationService) -> str:
    """按中断类型把批次挂起为取消或掉线并落盘，返回违规标记。"""

    if cancel.offline():
        progress.suspend(STATUS_OFFLINE, cancel.reason())
        violation = f"offline:{cancel.reason()}"
    else:
        progress.suspend(STATUS_CANCELLED, cancel.reason())
        violation = f"cancelled:{cancel.reason()}"
    service.save_progress(progress)
    service.count_abort()
    return violation


def _ordered_work(progress: BatchProgress) -> list[BatchStep]:
    """按"分区顺序 + 每区游标"拼出本程仍要看的站，保持全局时序不乱。"""

    work: list[BatchStep] = []
    for zone in progress.zones:
        members = progress.zone_steps(zone)
        work.extend(members[progress.cursor.get(zone, 0) :])
    return work


def drive(
    service: StationService,
    progress: BatchProgress,
    cancel: CancelToken | None = None,
    dt: float = 1.0,
) -> dict:
    """沿固定计划顺序推进一批下发，逐站检查中断信号。

    工作集由各分区游标决定（zone 模式可能从分区头重放）。每看一个站先过幂等关：
    已经在位的站只确认不动阀；需要下发的站在动作之前再查一次中断信号，保证取消
    之后不会再多改一个站。
    """

    steps: list[dict] = []
    adjusted: list[str] = []
    skipped: list[str] = []
    violations: list[str] = []
    for step in _ordered_work(progress):
        if not progress.needs_command(step.station):
            progress.advance_cursor(step.zone)
            skipped.append(step.station)
            continue
        if cancel is not None and cancel.cancelled():
            violations.append(_suspend(progress, cancel, service))
            break
        result = service.apply_batch_station(progress, step.station, dt)
        if result["skipped"]:
            skipped.append(step.station)
            continue
        steps.extend(result["steps"])
        adjusted.append(step.station)
    return {
        "steps": steps,
        "adjusted": adjusted,
        "skipped": skipped,
        "violations": violations,
    }


def verify_field(service: StationService, progress: BatchProgress, tolerance: float = 1.5) -> list[dict]:
    """掉线恢复前逐站核对现场阀位，返回每个已应用站的核对结果。"""

    checks: list[dict] = []
    for step in progress.steps:
        if progress.is_applied(step.station):
            checks.append(service.verify_station(progress, step.station, tolerance))
    service.save_progress(progress)
    return checks


def prepare_resume(service: StationService, progress: BatchProgress) -> dict:
    """续作前的现场核对与游标定位。

    取消批次直接续作（计划内中断，现场可信）；掉线批次必须先逐站核对现场，再按各
    分区定死的续作粒度定位游标：station 模式推到最早仍需下发的站（未应用或漂移），
    zone 模式没整区在位就回退到分区头整区重放。已经在位的站无论哪种模式都只做幂等
    确认，不再下发。
    """

    was_offline = progress.status == STATUS_OFFLINE
    checks: list[dict] = []
    if was_offline:
        checks = verify_field(service, progress)
    replay = service.prepare_cursors(progress, was_offline)
    progress.resume()
    service.save_progress(progress)
    return {
        "offline": was_offline,
        "checks": checks,
        "mismatches": [item["station"] for item in checks if not item["match"]],
        "replay": {zone: list(stations) for zone, stations in replay.items()},
        "rewound": [
            step.station
            for zone in progress.zones
            for step in progress.zone_steps(zone)[: progress.cursor.get(zone, 0)]
            if progress.needs_command(step.station)
        ],
    }


def _report(
    progress: BatchProgress,
    flow_dicts: list[dict],
    steps: list[dict],
    adjusted: list[str],
    violations: list[str],
) -> dict:
    pending = progress.first_unapplied()
    return {
        "ok": progress.status == STATUS_COMPLETED and not violations,
        "status": progress.status,
        "batch_id": progress.batch_id,
        "steps": steps,
        "violations": violations,
        "planned": len(progress.steps),
        "applied_count": progress.applied_count(),
        "applied": progress.applied_stations(),
        "remaining": [step.station for step in progress.steps if progress.needs_command(step.station)],
        "next": pending.station if pending else None,
        "adjusted": adjusted,
        "resume_mode": progress.resume_mode(),
        "zone_modes": dict(progress.zone_modes),
        "cursor": dict(progress.cursor),
        "flows": flow_dicts,
    }


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
    batch_ids: IdFactory | None = None,
) -> dict:
    """开一批可续作下发并沿计划推进到底或到中断点。"""

    id_factory = batch_ids or IdFactory("batch")
    progress, flows = open_batch(
        service, mapping, zones, network, zone_names, demand_pct, target_c, id_factory
    )
    flow_dicts = [flow.as_dict() for flow in flows]
    steps = [
        {"component": "primary", "action": "allocate_flow", "detail": flow.as_dict()} for flow in flows
    ]
    driven = drive(service, progress, cancel, dt)
    steps.extend(driven["steps"])
    return _report(progress, flow_dicts, steps, driven["adjusted"], driven["violations"])


def resume_batch(
    service: StationService,
    progress: BatchProgress,
    cancel: CancelToken | None = None,
    dt: float = 1.0,
) -> dict:
    """续作一批未完结下发：先按粒度核对/回退，再沿原计划顺序推进。"""

    prepared = prepare_resume(service, progress)
    driven = drive(service, progress, cancel, dt)
    report = _report(
        progress,
        progress.flows,
        driven["steps"],
        driven["adjusted"],
        driven["violations"],
    )
    report["resume"] = prepared
    return report
