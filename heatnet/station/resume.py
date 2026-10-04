"""批次续作编排：建批、按序下发、取消保留、掉线核对、断点续发。

续作粒度定死为"站级步骤"：批次计划在建批时落成步骤表，续作从第一个未完成步骤
按序号接着发，已确认的步骤只核对、不重发，整批重发不作为续作策略。分区只决定
步骤的排序（分区优先级在前、站内顺序在后），不是续作的原子单位。掉线按部分失败
处理：恢复后先把状态未知的步骤与现场阀位逐一核对，确认到位的转确认、不到位的
退回待发，然后才允许续发——进度、现场阀位与审计由此保持同一份事实。
"""

from __future__ import annotations

from ..errors import InterlockError
from ..eventbus import Event, EventBus
from ..ns.zone import ZoneRegistry
from ..primary.flow import batch_flows
from ..primary.network import PrimaryNetwork
from .journal import (
    BATCH_ABORTED,
    BATCH_CANCELLED,
    BATCH_COMPLETED,
    BATCH_DISPATCHING,
    BATCH_FAILED,
    BATCH_INTERRUPTED,
    OPEN_BATCH_STATUSES,
    STEP_CONFIRMED,
    STEP_DISPATCHED,
    STEP_FAILED,
    STEP_PENDING,
    TERMINAL_BATCH_STATUSES,
    BatchJournal,
)
from .mapping import StationMapping
from .service import CancelToken, StationService


class BatchRunner:
    """按批次日志执行与续作批量下发。"""

    def __init__(
        self,
        service: StationService,
        mapping: StationMapping,
        zones: ZoneRegistry,
        network: PrimaryNetwork,
        journal: BatchJournal,
        bus: EventBus | None = None,
    ) -> None:
        self._service = service
        self._mapping = mapping
        self._zones = zones
        self._network = network
        self._journal = journal
        self._bus = bus

    @property
    def journal(self) -> BatchJournal:
        return self._journal

    # ---- 建批 ---------------------------------------------------------

    def open_batch(
        self,
        plan: list[tuple[str, float]],
        zone_names: list[str],
        demand_pct: float,
        target_c: float,
        ts: float = 0.0,
    ) -> dict:
        """建批：排出步骤表并落盘，同时完成一次网流量分配。"""

        stations = [station for station, _ in plan]
        self._ensure_free(stations)
        flows = batch_flows(self._network, plan)
        self._network.count_dispatch()
        batch_id = self._journal.new_id()
        steps: list[dict] = []
        for seq, (station, _demand) in enumerate(plan, start=1):
            steps.append(
                {
                    "seq": seq,
                    "scope": "station",
                    "station": station,
                    "zone": self._mapping.zone_of(station),
                    "command_id": f"{batch_id}:{seq}",
                    "target_c": round(float(target_c), 2),
                    "valve_pct": round(self._service.get(station).demand_for(target_c), 2),
                    "status": STEP_PENDING,
                    "epoch": 1,
                }
            )
        batch = self._journal.create(
            batch_id,
            zones=list(zone_names),
            demand_pct=demand_pct,
            target_c=target_c,
            steps=steps,
            flows=[flow.as_dict() for flow in flows],
            ts=ts,
        )
        self._publish(
            "dispatch_batch_opened",
            batch_id=batch_id,
            zones=list(zone_names),
            steps=len(steps),
        )
        return batch

    # ---- 执行 ---------------------------------------------------------

    def run(
        self,
        batch_id: str,
        cancel: CancelToken | None = None,
        dt: float = 1.0,
        ts: float = 0.0,
    ) -> dict:
        """按步骤表从第一个待发步骤接着下发；取消只中断循环，已落盘步骤不动。"""

        if dt <= 0:
            raise ValueError("步长必须为正")
        batch = self._journal.load(batch_id)
        status = str(batch["status"])
        if status in TERMINAL_BATCH_STATUSES:
            raise InterlockError(f"批次已终结（{status}），不能续作", component="dispatch")
        if status in (BATCH_INTERRUPTED, BATCH_FAILED):
            raise InterlockError("批次处于部分失败状态，必须先核对现场再续作", component="dispatch")
        unknown = [
            step["seq"]
            for step in batch["steps"]
            if step["status"] in (STEP_DISPATCHED, STEP_FAILED)
        ]
        if unknown:
            raise InterlockError(
                f"步骤 {unknown} 尚未与现场核对，先核对再续作", component="dispatch"
            )
        if status == BATCH_CANCELLED:
            self._journal.set_status(batch, BATCH_DISPATCHING)
        self._service.note_batch()

        steps_report: list[dict] = []
        if not batch.get("flows_announced"):
            # 流量分配只在首发时记一次，续作不重复记账。
            steps_report.extend(
                {"component": "primary", "action": "allocate_flow", "detail": dict(flow)}
                for flow in batch.get("flows", [])
            )
            batch["flows_announced"] = True
            self._journal.save(batch)
        adjusted: list[str] = []
        violations: list[str] = []
        cancelled = False
        for step in batch["steps"]:
            if step["status"] == STEP_CONFIRMED:
                continue  # 已改好的站不再下发
            if cancel is not None and cancel.cancelled():
                cancelled = True
                violations.append(f"cancelled:{cancel.reason()}")
                self._service.note_aborted()
                self._journal.set_status(batch, BATCH_CANCELLED)
                self._publish(
                    "dispatch_batch_cancelled",
                    severity="warning",
                    batch_id=batch_id,
                    reason=cancel.reason(),
                    confirmed=self._confirmed_count(batch),
                )
                break
            seq = int(step["seq"])
            self._journal.mark(
                batch, seq, STEP_DISPATCHED, epoch=batch["epoch"], dispatched_ts=round(float(ts), 3)
            )
            try:
                detail = self._service.apply_station(
                    str(step["station"]),
                    float(step["target_c"]),
                    dt,
                    demand_pct=float(step["valve_pct"]),
                )
            except Exception:
                self._journal.mark(batch, seq, STEP_FAILED)
                self._journal.set_status(batch, BATCH_FAILED)
                raise
            self._journal.mark(
                batch,
                seq,
                STEP_CONFIRMED,
                confirmed_by="dispatch",
                actual_valve_pct=round(float(detail["valve_pct"]), 2),
                confirmed_ts=round(float(ts), 3),
            )
            adjusted.append(str(step["station"]))
            steps_report.append({"component": "station", "action": "adjust", "detail": detail})
        if not cancelled:
            self._journal.set_status(batch, BATCH_COMPLETED)
            self._publish(
                "dispatch_batch_completed", batch_id=batch_id, adjusted=self._confirmed_count(batch)
            )
        return self._report(batch, steps_report, adjusted, violations)

    # ---- 核对与续作 ----------------------------------------------------

    def reconcile(self, batch_id: str, ts: float = 0.0) -> dict:
        """把状态未知的步骤逐一与现场阀位核对：到位的转确认，不到位的退回待发。"""

        batch = self._journal.load(batch_id)
        verdicts: list[dict] = []
        for step in batch["steps"]:
            if step["status"] not in (STEP_DISPATCHED, STEP_FAILED):
                continue
            valve = self._service.valve(str(step["station"]))
            raw = self._service.durable.load(f"valve/{valve.name}")
            try:
                actual = float(raw.get("position_pct")) if raw else valve.position_pct()
            except (TypeError, ValueError):
                actual = valve.position_pct()
            seq = int(step["seq"])
            if valve.limits.settled(actual, float(step["valve_pct"])):
                self._journal.mark(
                    batch,
                    seq,
                    STEP_CONFIRMED,
                    confirmed_by="reconcile",
                    actual_valve_pct=round(actual, 2),
                    confirmed_ts=round(float(ts), 3),
                )
                verdict = "confirmed"
            else:
                self._journal.mark(batch, seq, STEP_PENDING, actual_valve_pct=round(actual, 2))
                verdict = "redispatch"
            verdicts.append(
                {
                    "seq": seq,
                    "station": step["station"],
                    "command_id": step["command_id"],
                    "verdict": verdict,
                    "planned_valve_pct": step["valve_pct"],
                    "actual_valve_pct": round(actual, 2),
                }
            )
        if batch["status"] not in TERMINAL_BATCH_STATUSES:
            if all(step["status"] == STEP_CONFIRMED for step in batch["steps"]):
                self._journal.set_status(batch, BATCH_COMPLETED)
            else:
                self._journal.set_status(batch, BATCH_CANCELLED)
        report = {
            "batch_id": batch_id,
            "checked": len(verdicts),
            "verdicts": verdicts,
            "status": batch["status"],
        }
        self._publish(
            "dispatch_reconciled",
            severity="notice" if not verdicts else "warning",
            batch_id=batch_id,
            checked=len(verdicts),
            confirmed=len([item for item in verdicts if item["verdict"] == "confirmed"]),
            redispatch=len([item for item in verdicts if item["verdict"] == "redispatch"]),
        )
        return report

    def resume(
        self,
        batch_id: str,
        cancel: CancelToken | None = None,
        dt: float = 1.0,
        ts: float = 0.0,
    ) -> dict:
        """续作一个未完成批次：世代加一、先核对现场、再按序补发。"""

        batch = self._journal.load(batch_id)
        if str(batch["status"]) in TERMINAL_BATCH_STATUSES:
            raise InterlockError(
                f"批次已终结（{batch['status']}），不能续作", component="dispatch"
            )
        epoch = self._journal.bump_epoch(batch)
        self._publish("dispatch_resumed", batch_id=batch_id, epoch=epoch)
        reconcile = self.reconcile(batch_id, ts=ts)
        if batch["status"] == BATCH_COMPLETED:
            # 核对发现现场已全部到位：一步都不用再发。
            report = self._report(batch, [], [], [])
            return {**report, "resumed": True, "reconcile": reconcile}
        report = self.run(batch_id, cancel=cancel, dt=dt, ts=ts)
        return {**report, "resumed": True, "reconcile": reconcile}

    def abort(self, batch_id: str, ts: float = 0.0) -> dict:
        """作废一个未完成批次：已确认的站保持现状，未发的站放弃并释放占用。"""

        batch = self._journal.load(batch_id)
        if str(batch["status"]) in TERMINAL_BATCH_STATUSES:
            raise InterlockError(
                f"批次已终结（{batch['status']}），不能作废", component="dispatch"
            )
        self._journal.set_status(batch, BATCH_ABORTED, aborted_ts=round(float(ts), 3))
        confirmed = [step["station"] for step in batch["steps"] if step["status"] == STEP_CONFIRMED]
        abandoned = [step["station"] for step in batch["steps"] if step["status"] != STEP_CONFIRMED]
        self._publish(
            "dispatch_batch_aborted",
            severity="warning",
            batch_id=batch_id,
            confirmed=len(confirmed),
            abandoned=len(abandoned),
        )
        return {
            "ok": True,
            "batch_id": batch_id,
            "status": BATCH_ABORTED,
            "confirmed": confirmed,
            "abandoned": abandoned,
            "steps": [],
            "violations": [],
        }

    # ---- 进度 ---------------------------------------------------------

    def progress(self, batch_id: str | None = None) -> dict:
        """从落盘日志派生续作进度，供控制台展示。"""

        if batch_id is not None:
            batches = [self._journal.load(batch_id)]
        else:
            batches = self._journal.list_all()
        summaries = [self._summarize(batch) for batch in batches]
        open_count = len(
            [batch for batch in batches if batch.get("status") in OPEN_BATCH_STATUSES]
        )
        return {"open": open_count, "batches": summaries}

    def latest_open_batch_id(self) -> str | None:
        """返回最近一个未完成批次的标识，没有时返回 ``None``。"""

        open_batches = self._journal.open_batches()
        if not open_batches:
            return None
        return str(open_batches[-1]["batch_id"])

    # ---- 内部 ---------------------------------------------------------

    def _ensure_free(self, stations: list[str]) -> None:
        """站级 fencing：任一站被未完成批次占用时，拒绝再建批次。"""

        wanted = set(stations)
        for batch in self._journal.open_batches():
            held = {str(step["station"]) for step in batch["steps"]}
            clash = sorted(wanted & held)
            if clash:
                raise InterlockError(
                    f"站 {', '.join(clash)} 仍被未完成批次 {batch['batch_id']} 占用，先续作或作废",
                    component="dispatch",
                )

    def _confirmed_count(self, batch: dict) -> int:
        return len([step for step in batch["steps"] if step["status"] == STEP_CONFIRMED])

    def _summarize(self, batch: dict) -> dict:
        steps = batch["steps"]
        counts = {
            status: len([step for step in steps if step["status"] == status])
            for status in (STEP_PENDING, STEP_DISPATCHED, STEP_CONFIRMED, STEP_FAILED)
        }
        return {
            "batch_id": batch["batch_id"],
            "status": batch["status"],
            "epoch": int(batch.get("epoch", 1)),
            "zones": list(batch.get("zones", [])),
            "target_c": batch.get("target_c"),
            "total": len(steps),
            "confirmed": counts[STEP_CONFIRMED],
            "pending": counts[STEP_PENDING],
            "dispatched": counts[STEP_DISPATCHED],
            "failed": counts[STEP_FAILED],
            "remaining": [
                str(step["station"]) for step in steps if step["status"] != STEP_CONFIRMED
            ],
            "steps": [dict(step) for step in steps],
        }

    def _report(
        self,
        batch: dict,
        steps_report: list[dict],
        adjusted: list[str],
        violations: list[str],
    ) -> dict:
        summary = self._summarize(batch)
        return {
            "ok": not violations,
            "batch_id": batch["batch_id"],
            "epoch": int(batch.get("epoch", 1)),
            "batch_status": batch["status"],
            "steps": steps_report,
            "violations": violations,
            "planned": len(batch["steps"]),
            "adjusted": adjusted,
            "remaining": summary["remaining"],
            "flows": list(batch.get("flows", [])),
            "progress": summary,
        }

    def _publish(self, action: str, severity: str = "notice", **payload: object) -> None:
        if self._bus is None:
            return
        self._bus.publish(
            Event.of("dispatch", "dispatch", action, severity=severity, **payload)
        )
