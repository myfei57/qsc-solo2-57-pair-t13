"""换热站批量调节、取消语义、可续作进度与流量分批计算。"""

from __future__ import annotations

import pytest

from heatnet.config import default_config
from heatnet.ids import IdFactory
from heatnet.ns.zone import Zone, ZoneRegistry
from heatnet.primary.flow import batch_flows
from heatnet.primary.network import PrimaryNetwork
from heatnet.station.batch import build_plan, build_steps, open_batch, resume_batch, run_batch
from heatnet.station.mapping import StationMapping
from heatnet.station.mix import apply_mix
from heatnet.station.progress import (
    STATUS_CANCELLED,
    STATUS_COMPLETED,
    STATUS_OFFLINE,
    BatchStore,
)
from heatnet.station.service import CancelToken, StationService
from heatnet.station.station import HeatStation
from heatnet.store.jsonfile import JsonStore
from heatnet.store.state import DurableState
from heatnet.valve.valve import Valve


def _stack(tmp_path) -> tuple[ZoneRegistry, StationMapping, StationService, PrimaryNetwork]:
    zones = ZoneRegistry()
    zones.add(Zone(name="north", setpoint_c=62.0, resume_mode="station"))
    zones.add(Zone(name="south", setpoint_c=54.0, resume_mode="zone"))
    for station in ("st01", "st02", "st03"):
        zones.assign(station, "north")
    for station in ("st04", "st05"):
        zones.assign(station, "south")

    config = default_config()
    stations = {name: HeatStation(name, zones.zone_of(name), config.station) for name in zones.index()}
    valves = {name: Valve(f"{name}.mix", config.valve) for name in stations}
    store = JsonStore(str(tmp_path))
    service = StationService(stations, valves, DurableState(store), BatchStore(store))
    return zones, StationMapping(zones), service, PrimaryNetwork(config.primary)


class _CancelAfter(CancelToken):
    """处理若干个站之后再置位取消，用来验证逐步检查。"""

    def __init__(self, allowed: int, kind: str = CancelToken.CANCEL) -> None:
        super().__init__("mid-batch", kind)
        self._allowed = allowed
        self._seen = 0

    def cancelled(self) -> bool:
        self._seen += 1
        return self._seen > self._allowed


def test_batch_flows_are_independent_per_station():
    network = PrimaryNetwork(default_config().primary)
    plan = [("st01", 60.0), ("st02", 60.0), ("st03", 60.0)]
    flows = batch_flows(network, plan)
    values = [flow.flow_t_h for flow in flows]
    assert values[0] == pytest.approx(values[1])
    assert values[0] == pytest.approx(values[2])
    assert network.flow_t_h() == pytest.approx(sum(values))
    assert network.flow_t_h() < network.baseline_t_h()
    assert [flow.as_dict()["station"] for flow in flows] == ["st01", "st02", "st03"]
    assert network.dispatches() == 0


def test_run_batch_applies_to_every_station(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    report = run_batch(service, mapping, zones, network, ["north"], 60.0, 60.0, batch_ids=IdFactory("b"))
    assert report["ok"] is True
    assert report["adjusted"] == ["st01", "st02", "st03"]
    assert report["status"] == STATUS_COMPLETED
    for name in ("st01", "st02", "st03"):
        assert service.get(name).target_c() == pytest.approx(60.0)
        assert service.valve(name).position_pct() > 0.0


def test_cancelled_before_start_changes_nothing(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    token = CancelToken("client-timeout")
    token.cancel()
    report = run_batch(service, mapping, zones, network, ["north"], 60.0, 60.0, token, batch_ids=IdFactory("b"))
    assert report["ok"] is False
    assert report["status"] == STATUS_CANCELLED
    assert report["adjusted"] == []
    assert report["violations"] == ["cancelled:client-timeout"]
    assert service.valve("st01").moves() == 0


def test_cancellation_is_checked_at_every_station(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    report = run_batch(
        service, mapping, zones, network, ["north"], 60.0, 60.0, _CancelAfter(1), batch_ids=IdFactory("b")
    )
    assert report["ok"] is False
    assert report["adjusted"] == ["st01"]
    assert report["remaining"] == ["st02", "st03"]
    assert service.get("st02").target_c() == pytest.approx(service.get("st02").limits.setpoint_c)
    assert service.valve("st02").moves() == 0
    with pytest.raises(ValueError):
        service.apply_batch_station(service.active_progress(), "st02", 0.0)


def test_cancel_resume_continues_and_does_not_recommand_done_stations(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    run_batch(service, mapping, zones, network, ["north"], 60.0, 60.0, _CancelAfter(1), batch_ids=IdFactory("b"))
    progress = service.active_progress()
    report = resume_batch(service, progress)
    assert report["ok"] is True
    assert report["adjusted"] == ["st02", "st03"]
    assert {name: service.valve(name).moves() for name in ("st01", "st02", "st03")} == {
        "st01": 1,
        "st02": 1,
        "st03": 1,
    }
    assert service.active_progress() is None


def test_zone_mode_replays_zone_but_keeps_good_stations_idempotent(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    run_batch(service, mapping, zones, network, ["south"], 60.0, 60.0, _CancelAfter(1), batch_ids=IdFactory("b"))
    report = resume_batch(service, service.active_progress())
    assert report["ok"] is True
    # 整区从头重放，但已在位的 st04 只确认不动阀，st05 下发一次。
    assert report["adjusted"] == ["st05"]
    assert service.valve("st04").moves() == 1
    assert service.valve("st05").moves() == 1


def test_offline_resume_verifies_field_before_continuing(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    run_batch(
        service,
        mapping,
        zones,
        network,
        ["north"],
        60.0,
        60.0,
        _CancelAfter(2, kind=CancelToken.OFFLINE),
        batch_ids=IdFactory("b"),
    )
    progress = service.active_progress()
    assert progress.status == STATUS_OFFLINE
    report = resume_batch(service, progress)
    assert report["resume"]["offline"] is True
    assert report["resume"]["mismatches"] == []
    assert report["adjusted"] == ["st03"]
    assert all(service.valve(name).moves() == 1 for name in ("st01", "st02", "st03"))


def test_offline_drift_in_station_mode_only_corrects_drifted_station(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    run_batch(
        service,
        mapping,
        zones,
        network,
        ["north"],
        60.0,
        60.0,
        _CancelAfter(2, kind=CancelToken.OFFLINE),
        batch_ids=IdFactory("b"),
    )
    service.set_field_reading("st02", 3.0)
    report = resume_batch(service, service.active_progress())
    assert report["resume"]["mismatches"] == ["st02"]
    assert report["adjusted"] == ["st02", "st03"]
    assert service.valve("st01").moves() == 1
    assert service.valve("st02").moves() == 2
    assert service.valve("st03").moves() == 1


def test_offline_drift_in_zone_mode_replays_and_corrects(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    run_batch(
        service,
        mapping,
        zones,
        network,
        ["south"],
        60.0,
        60.0,
        _CancelAfter(1, kind=CancelToken.OFFLINE),
        batch_ids=IdFactory("b"),
    )
    service.set_field_reading("st04", 3.0)
    report = resume_batch(service, service.active_progress())
    assert report["adjusted"] == ["st04", "st05"]
    assert service.valve("st04").moves() == 2
    assert service.valve("st05").moves() == 1


def test_progress_is_persisted_and_reloadable(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    progress, _ = open_batch(
        service, mapping, zones, network, ["north"], 60.0, 60.0, IdFactory("b")
    )
    reloaded = service.load_progress(progress.batch_id)
    assert reloaded.batch_id == progress.batch_id
    assert [step.station for step in reloaded.steps] == ["st01", "st02", "st03"]
    assert reloaded.zone_mode("north") == "station"
    assert reloaded.cursor == {"north": 0}


def test_resume_mode_is_fixed_per_zone(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    steps, modes = build_steps(mapping, zones, ["north", "south"], 60.0)
    assert modes == {"north": "station", "south": "zone"}
    assert [step.zone for step in steps] == ["north", "north", "north", "south", "south"]
    with pytest.raises(Exception):
        zones.set_resume_mode("north", "bogus")


def test_run_batch_reports_flows_and_adjustments(tmp_path):
    zones, mapping, service, network = _stack(tmp_path)
    report = run_batch(service, mapping, zones, network, ["north", "south"], 60.0, 60.0)
    assert report["ok"] is True
    assert report["planned"] == 5
    assert set(report["adjusted"]) == {"st01", "st02", "st03", "st04", "st05"}
    assert len(report["flows"]) == 5
    assert network.dispatches() == 1
    assert network.flow_t_h() > 0.0


def test_build_plan_orders_by_setpoint(tmp_path):
    zones, mapping, _, _ = _stack(tmp_path)
    plan = build_plan(mapping, zones, ["south", "north"], 60.0)
    assert [station for station, _ in plan][:3] == ["st01", "st02", "st03"]
    south_demand = dict(plan)["st04"]
    assert south_demand < 60.0


def test_apply_mix_writes_station_state_to_store(tmp_path):
    config = default_config()
    station = HeatStation("st01", "north", config.station)
    valve = Valve("st01.mix", config.valve)
    durable = DurableState(JsonStore(str(tmp_path)))
    detail = apply_mix(station, valve, durable, 58.0, 1.0)
    assert detail["station"] == "st01"
    assert detail["valve_pct"] > 0.0
    assert durable.load("valve/st01.mix")["position_pct"] == pytest.approx(detail["valve_pct"])
    assert station.snapshot().as_dict()["target_c"] == pytest.approx(58.0)
