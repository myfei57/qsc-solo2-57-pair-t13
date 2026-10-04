"""批量下发的断点续作：取消续发、掉线核对、防重 fencing 与进度审计一致。"""

from __future__ import annotations

import pytest

from heatnet.app import build_platform
from heatnet.config import default_config
from heatnet.errors import InterlockError
from heatnet.station.journal import (
    BATCH_CANCELLED,
    BATCH_COMPLETED,
    BATCH_DISPATCHING,
    BATCH_INTERRUPTED,
    STEP_CONFIRMED,
    STEP_DISPATCHED,
)
from heatnet.station.resume import BatchRunner
from heatnet.station.service import CancelToken
from heatnet.valve.move import move_valve


class _CancelAfter(CancelToken):
    """处理若干个站之后再置位取消，用来模拟客户端中途取消。"""

    def __init__(self, allowed: int) -> None:
        super().__init__("mid-batch")
        self._allowed = allowed
        self._seen = 0

    def cancelled(self) -> bool:
        self._seen += 1
        return self._seen > self._allowed


def _started(platform):
    platform.start()
    platform.push_weather(-8.0)
    platform.startup(-8.0, 60.0)
    return platform


def _open_batch_id(platform) -> str:
    batch_id = platform._batch_runner().latest_open_batch_id()
    assert batch_id is not None
    return batch_id


def test_cancel_then_resume_completes_remaining_stations(platform):
    _started(platform)
    report = platform.dispatch(60.0, 60.0, ["north"], _CancelAfter(1))
    assert report["ok"] is False
    assert report["adjusted"] == ["st01"]
    assert report["batch_status"] == BATCH_CANCELLED
    assert platform.valves["st01"].moves() == 1

    resumed = platform.resume_dispatch()
    assert resumed["ok"] is True
    assert resumed["resumed"] is True
    assert resumed["epoch"] == 2
    assert resumed["adjusted"] == ["st02", "st03"]
    assert resumed["remaining"] == []
    assert resumed["batch_status"] == BATCH_COMPLETED
    # 已改好的站不再下发：st01 的动作次数没有增加。
    assert platform.valves["st01"].moves() == 1
    assert platform.valves["st02"].moves() == 1
    assert platform.valves["st03"].moves() == 1


def test_resume_keeps_original_step_order(platform):
    _started(platform)
    platform.dispatch(60.0, 60.0, None, _CancelAfter(2))
    resumed = platform.resume_dispatch()
    assert resumed["adjusted"] == ["st03", "st04", "st05", "st06"]
    steps = resumed["progress"]["steps"]
    assert [step["seq"] for step in steps] == [1, 2, 3, 4, 5, 6]
    assert [step["station"] for step in steps if step["epoch"] == 2] == [
        "st03",
        "st04",
        "st05",
        "st06",
    ]


def test_restart_marks_batch_interrupted_and_resume_reconciles(platform, clock):
    _started(platform)
    platform.dispatch(60.0, 60.0, ["north"], _CancelAfter(1))
    journal = platform.batch_journal
    batch_id = _open_batch_id(platform)
    batch = journal.load(batch_id)
    # 模拟掉线瞬间：st02 的指令已下发但确认丢失，批次停在下发中途。
    journal.mark(batch, 2, STEP_DISPATCHED)
    journal.set_status(batch, BATCH_DISPATCHING)

    restarted = build_platform(default_config(), clock=clock, storage_root=platform.store.root)
    progress = restarted.dispatch_progress()
    assert progress["open"] == 1
    assert progress["batches"][0]["status"] == BATCH_INTERRUPTED
    actions = [entry["action"] for entry in restarted.audit.entries(limit=200)]
    assert "dispatch_interrupted" in actions

    # 部分失败状态不允许绕过核对直接续发。
    runner = BatchRunner(
        restarted.service, restarted.mapping, restarted.zones, restarted.network, restarted.batch_journal
    )
    with pytest.raises(InterlockError):
        runner.run(batch_id)

    # st02 现场其实没改成：核对后退回待发，续作时补发。
    resumed = restarted.resume_dispatch()
    assert resumed["reconcile"]["checked"] == 1
    assert resumed["reconcile"]["verdicts"][0]["verdict"] == "redispatch"
    assert resumed["adjusted"] == ["st02", "st03"]
    assert resumed["batch_status"] == BATCH_COMPLETED


def test_reconcile_confirms_station_already_at_target(platform, clock):
    _started(platform)
    platform.dispatch(60.0, 60.0, ["north"], _CancelAfter(1))
    journal = platform.batch_journal
    batch_id = _open_batch_id(platform)
    batch = journal.load(batch_id)
    # 模拟掉线瞬间：st02 的指令已下发且现场已改成功，只是确认丢失。
    planned = float(batch["steps"][1]["valve_pct"])
    move_valve(platform.valves["st02"], platform.durable, planned)
    journal.mark(batch, 2, STEP_DISPATCHED)
    journal.set_status(batch, BATCH_DISPATCHING)

    restarted = build_platform(default_config(), clock=clock, storage_root=platform.store.root)
    moves_before = restarted.valves["st02"].moves()
    resumed = restarted.resume_dispatch()
    verdict = resumed["reconcile"]["verdicts"][0]
    assert verdict["verdict"] == "confirmed"
    assert verdict["actual_valve_pct"] == pytest.approx(planned, abs=0.01)
    # 核对确认到位的站不再下发，只补 st03。
    assert restarted.valves["st02"].moves() == moves_before
    assert resumed["adjusted"] == ["st03"]
    step2 = resumed["progress"]["steps"][1]
    assert step2["status"] == STEP_CONFIRMED
    assert step2["confirmed_by"] == "reconcile"


def test_open_batch_blocks_new_dispatch_until_resolved(platform):
    _started(platform)
    token = CancelToken("client-timeout")
    token.cancel()
    platform.dispatch(60.0, 60.0, ["north"], token)
    batch_id = _open_batch_id(platform)
    # 站被未完成批次占用时，不允许再开新批次（防止一个站被两条指令各改一次）。
    with pytest.raises(InterlockError):
        platform.dispatch(60.0, 60.0, ["north"])
    with pytest.raises(InterlockError):
        platform.dispatch(60.0, 60.0)

    aborted = platform.abort_dispatch(batch_id)
    assert aborted["status"] == "aborted"
    assert aborted["abandoned"] == ["st01", "st02", "st03"]
    report = platform.dispatch(60.0, 60.0, ["north"])
    assert report["ok"] is True
    assert report["adjusted"] == ["st01", "st02", "st03"]


def test_confirmed_step_cannot_be_dispatched_again(platform):
    _started(platform)
    platform.dispatch(60.0, 60.0, ["north"])
    journal = platform.batch_journal
    batch_id = platform.dispatch_progress()["batches"][0]["batch_id"]
    batch = journal.load(batch_id)
    assert batch["status"] == BATCH_COMPLETED
    with pytest.raises(InterlockError):
        journal.mark(batch, 1, STEP_DISPATCHED)


def test_resume_requires_open_batch(platform):
    _started(platform)
    with pytest.raises(InterlockError):
        platform.resume_dispatch()
    platform.dispatch(60.0, 60.0, ["north"])
    batch_id = platform.dispatch_progress()["batches"][0]["batch_id"]
    with pytest.raises(InterlockError):
        platform.resume_dispatch(batch_id)


def test_progress_valves_and_audit_stay_consistent(platform):
    _started(platform)
    platform.dispatch(60.0, 60.0, ["north"], _CancelAfter(1))
    platform.resume_dispatch()

    progress = platform.dispatch_progress()
    summary = progress["batches"][0]
    assert progress["open"] == 0
    assert summary["confirmed"] == 3
    assert summary["epoch"] == 2
    for step in summary["steps"]:
        assert step["status"] == STEP_CONFIRMED
        durable = platform.durable.load(f"valve/{step['station']}.mix")
        assert step["actual_valve_pct"] == pytest.approx(durable["position_pct"], abs=0.01)
        assert platform.valves[step["station"]].position_pct() == pytest.approx(
            durable["position_pct"], abs=0.01
        )
    actions = [entry["action"] for entry in platform.audit.entries(limit=300)]
    for expected in (
        "dispatch_batch_opened",
        "dispatch_batch_cancelled",
        "dispatch_resumed",
        "dispatch_reconciled",
        "dispatch_batch_completed",
    ):
        assert expected in actions
    status = platform.status()
    assert status["dispatch"]["open"] == 0
    assert status["dispatch"]["recent"][-1]["batch_id"] == summary["batch_id"]


def test_console_exposes_progress_resume_and_abort(platform):
    from heatnet.console.api import Api

    _started(platform)
    token = CancelToken("client-timeout")
    token.cancel()
    platform.dispatch(60.0, 60.0, ["north"], token)

    api = Api(platform)
    code, progress = api.handle("GET", "/api/dispatch/progress", {}, {})
    assert code == 200
    assert progress["open"] == 1
    assert progress["batches"][0]["remaining"] == ["st01", "st02", "st03"]

    code, resumed = api.handle("POST", "/api/dispatch/resume", {}, {})
    assert code == 200
    assert resumed["ok"] is True
    assert resumed["adjusted"] == ["st01", "st02", "st03"]

    code, progress = api.handle("GET", "/api/dispatch/progress", {}, {})
    assert code == 200
    assert progress["open"] == 0

    code, payload = api.handle("POST", "/api/dispatch/abort", {}, {})
    assert code == 400
    code, payload = api.handle("POST", "/api/dispatch/abort", {}, {"batch_id": "batch-missing"})
    assert code == 404
