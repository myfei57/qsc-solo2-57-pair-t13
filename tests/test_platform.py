"""平台装配、控制循环、并发探测与跨重启一致性。"""

from __future__ import annotations

import pytest

from heatnet.app import build_platform
from heatnet.config import default_config
from heatnet.station.service import CancelToken


def test_startup_dispatch_and_cycle(online):
    for _ in range(120):
        online.cycle(dt=1.0)
    status = online.status()
    assert status["source"]["state"] == "online"
    assert status["source"]["quality_confirmed"] is True
    assert status["curve"]["stale"] is False
    assert status["primary"]["flow_t_h"] > 0.0
    assert status["pump"]["running"] is False
    assert status["stations"]["count"] == 6
    assert online.audit.summary()["total"] > 0


def test_dispatch_uses_priority_order_and_independent_flows(platform):
    platform.start()
    platform.push_weather(-8.0)
    platform.startup(-8.0, 60.0)
    report = platform.dispatch(60.0, 60.0)
    assert report["ok"] is True
    assert report["adjusted"][0] == "st01"
    assert len(report["flows"]) == 6
    assert platform.network.dispatches() == 1
    assert platform.network.flow_t_h() < platform.network.baseline_t_h()


def test_dispatch_honours_cancellation(platform):
    platform.start()
    platform.push_weather(-8.0)
    platform.startup(-8.0, 60.0)
    token = CancelToken("client-timeout")
    token.cancel()
    report = platform.dispatch(60.0, 60.0, ["north"], token)
    assert report["ok"] is False
    assert report["violations"] == ["cancelled:client-timeout"]
    assert platform.service.valve("st01").moves() == 0


class _CancelAfter(CancelToken):
    """处理若干个站之后再置位中断。"""

    def __init__(self, allowed, kind=CancelToken.CANCEL):
        super().__init__("mid-batch", kind)
        self._allowed = allowed
        self._seen = 0

    def cancelled(self):
        self._seen += 1
        return self._seen > self._allowed


def test_cancelled_dispatch_resumes_without_recommanding(platform):
    platform.start()
    platform.push_weather(-8.0)
    platform.startup(-8.0, 60.0)
    first = platform.dispatch(60.0, 60.0, ["north"], _CancelAfter(1))
    assert first["adjusted"] == ["st01"]
    assert platform.status()["dispatch"]["active"] is True
    resumed = platform.resume_dispatch()
    assert resumed["status"] == "completed"
    assert resumed["adjusted"] == ["st02", "st03"]
    assert {name: platform.valves[name].moves() for name in ("st01", "st02", "st03")} == {
        "st01": 1,
        "st02": 1,
        "st03": 1,
    }
    # 续作进度、实际阀位、审计三方一致
    assert resumed["applied_count"] == 3
    adjust_records = [
        item for item in platform.audit.records(limit=200) if item.action == "adjust"
    ]
    by_station = {item.asset: item.payload for item in adjust_records}
    assert set(by_station) == {"st01", "st02", "st03"}
    # 每站一条 adjust、同一批次、全局序号按计划顺序
    assert len(adjust_records) == 3
    assert {payload["batch_id"] for payload in by_station.values()} == {first["batch_id"]}
    assert [by_station[name]["seq"] for name in ("st01", "st02", "st03")] == [1, 2, 3]
    # 审计阀位 == 进度记录 == 实际阀位
    progress = platform.dispatch_progress(first["batch_id"])
    for name in ("st01", "st02", "st03"):
        recorded = progress["applied"][name]["valve_pct"]
        actual = platform.valves[name].position_pct()
        assert by_station[name]["valve_pct"] == round(recorded, 2)
        assert round(actual, 2) == round(recorded, 2)
    assert platform.status()["dispatch"]["active"] is False


def test_offline_dispatch_requires_verification_before_resume(platform):
    platform.start()
    platform.push_weather(-8.0)
    platform.startup(-8.0, 60.0)
    platform.dispatch(60.0, 60.0, ["north"], _CancelAfter(2, kind=CancelToken.OFFLINE))
    progress = platform.service.active_progress()
    assert progress.status == "offline"
    # 现场把 st02 扳偏：续作核对必须发现，且按站粒度只纠 st02
    platform.service.set_field_reading("st02", 2.0)
    resumed = platform.resume_dispatch()
    assert resumed["resume"]["offline"] is True
    assert resumed["resume"]["mismatches"] == ["st02"]
    assert resumed["adjusted"] == ["st02", "st03"]
    assert platform.valves["st01"].moves() == 1
    assert platform.valves["st02"].moves() == 2


def test_overlapping_dispatch_is_rejected_while_batch_open(platform):
    platform.start()
    platform.push_weather(-8.0)
    platform.startup(-8.0, 60.0)
    token = CancelToken("x")
    token.cancel()
    platform.dispatch(60.0, 60.0, ["north"], token)
    from heatnet.errors import InterlockError

    with pytest.raises(InterlockError):
        platform.dispatch(60.0, 60.0, ["south"])


def test_split_zone_rebuilds_mapping(platform):
    report = platform.split_zone("north", ["st02", "st03"], "north-west")
    assert report["ok"] is True
    assert platform.mapping.zone_of("st02") == "north-west"
    assert platform.mapping.generation() == 2
    assert platform.service.get("st02").zone() == "north-west"
    assert platform.zones.names() == ["east", "north", "north-west", "south"]


def test_replay_valves_skips_applied_commands(platform):
    platform.queue_commands("st01", [40.0, 65.0])
    first = platform.replay_valves(["st01"])
    assert first["replayed"] == 2
    second = platform.replay_valves(["st01"])
    assert second["replayed"] == 0
    assert platform.valves["st01"].position_pct() == pytest.approx(65.0)


def test_collect_heat_and_settle_quota(platform):
    for name in platform.meter_names():
        platform.collect_heat(name, 5.0)
    report = platform.quota_report()
    assert report["ledger"]["total_mwh"] == pytest.approx(30.0)
    assert report["accumulator"]["count"] == 6
    settled = platform.settle_quota()
    assert settled["period"] == 1
    assert len(settled["lines"]) == 6
    assert platform.ledger.periods() == 1


def test_concurrency_probe_reports_no_lost_state(platform):
    report = platform.concurrency_probe(rounds=24)
    assert report["ok"] is True
    assert report["source"]["mixed_pairs"] == 0
    assert report["meter"]["lost_increments"] == pytest.approx(0.0)


def test_trip_state_survives_restart(platform, clock):
    platform.start()
    platform.push_weather(-8.0)
    platform.trip("overtemperature")
    assert platform.source.state() == "tripped"

    restarted = build_platform(default_config(), clock=clock, storage_root=platform.store.root)
    assert restarted.source.state() == "tripped"
    assert restarted.source.trip_reason() == "overtemperature"
    assert restarted.status()["operations"] >= 1


def test_valve_position_survives_restart(platform, clock):
    platform.start()
    platform.queue_commands("st02", [55.0])
    platform.replay_valves(["st02"])

    restarted = build_platform(default_config(), clock=clock, storage_root=platform.store.root)
    assert restarted.valves["st02"].position_pct() == pytest.approx(55.0)
    assert restarted.checkpoint.load("st02") == 1


def test_snapshot_exposes_all_sections(online):
    snapshot = online.snapshot()
    for key in ("status", "topology", "namespaces", "zones", "mapping", "trend", "billing"):
        assert key in snapshot
    assert snapshot["topology"]["order"][0] == "weather"
    assert snapshot["namespaces"]["count"] == 25
    assert snapshot["status"]["stations"]["count"] == 6


def test_trend_collects_signals_after_cycles(online):
    for _ in range(5):
        online.cycle(dt=5.0)
    samples = online.trend.series("supply_c", limit=10)
    assert len(samples) == 5
    assert online.trend.latest("network_flow_t_h") is not None
    filled = online.trend.fill_gaps("supply_c", samples)
    assert len(filled) >= len(samples)


def test_forecast_and_billing_reports(platform):
    forecast = platform.outdoor_forecast(-10.0, steps=4)
    assert forecast["count"] == 4
    assert forecast["max_supply_c"] >= forecast["min_supply_c"]
    platform.collect_heat(platform.meter_names()[0], 40.0)
    lines = platform.billing(10.0)
    assert lines and lines[0]["overage_mwh"] > 0.0


def test_reset_state_clears_counters(platform):
    platform.start()
    platform.cycle(dt=1.0)
    assert platform.reset_state() is True
    assert platform.reset_state() is False


def test_note_error_writes_audit_entry(platform):
    from heatnet.errors import InterlockError

    platform.start()
    record = platform.note_error(InterlockError("联锁拒绝", component="source"))
    assert record["component"] == "source"
    assert platform.audit.summary()["by_severity"]["warning"] == 1
    assert platform.audit.count("error") == 1
