"""调度下发编排。

调度把目标负荷折算成各站的开度需求，按分区优先级排出固定计划，交给一次网按站
独立分配流量，再逐区逐站下发；进度逐站落盘，取消或掉线之后可以续作，已改好的
站不再下发。流量基线可以按实测重新标定，标定之后联锁门限同步变化。
"""

from __future__ import annotations

from ..ids import IdFactory
from ..ns.zone import ZoneRegistry
from ..primary.network import PrimaryNetwork
from ..station.batch import open_batch, resume_batch, run_batch
from ..station.mapping import StationMapping
from ..station.progress import BatchProgress
from ..station.service import CancelToken, StationService


def _flow_fields(network: PrimaryNetwork) -> dict:
    return {
        "flow_t_h": round(network.flow_t_h(), 2),
        "threshold_t_h": round(network.threshold_t_h(), 2),
    }


def dispatch_demand(
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
    """按分区开一批可续作调度并推进。"""

    report = run_batch(
        service,
        mapping,
        zones,
        network,
        zone_names,
        demand_pct,
        target_c,
        cancel,
        dt,
        batch_ids,
    )
    return {
        **report,
        "demand_pct": round(float(demand_pct), 2),
        "target_c": round(float(target_c), 2),
        **_flow_fields(network),
    }


def resume_demand(
    service: StationService,
    network: PrimaryNetwork,
    progress: BatchProgress,
    cancel: CancelToken | None = None,
    dt: float = 1.0,
) -> dict:
    """续作一批未完结调度：掉线先核对现场，再按分区粒度续发。"""

    report = resume_batch(service, progress, cancel, dt)
    return {
        **report,
        "demand_pct": round(progress.demand_pct, 2),
        "target_c": round(progress.target_c, 2),
        **_flow_fields(network),
    }


def open_demand(
    service: StationService,
    mapping: StationMapping,
    zones: ZoneRegistry,
    network: PrimaryNetwork,
    zone_names: list[str],
    demand_pct: float,
    target_c: float,
    batch_ids: IdFactory,
) -> tuple[BatchProgress, dict]:
    """只固化计划与流量并落盘进度，不触碰任何站（供分步下发使用）。"""

    progress, flows = open_batch(
        service, mapping, zones, network, zone_names, demand_pct, target_c, batch_ids
    )
    return progress, {"flows": [flow.as_dict() for flow in flows], **_flow_fields(network)}


def recalibrate_flow(network: PrimaryNetwork, measured_t_h: float) -> dict:
    """按实测流量重新标定一次网流量基线。"""

    baseline = network.recalibrate(measured_t_h)
    return {
        "baseline_t_h": round(baseline, 2),
        "threshold_t_h": round(network.threshold_t_h(), 2),
        "recalibrations": network.recalibrations(),
    }
