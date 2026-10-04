"""批量下发的步骤日志。

每个批次一份落盘文档：建批时把计划定死成步骤表（顺序、站、目标开度、幂等键），
执行中每一步先落"已下发"再动阀、动完落"已确认"并记下回读阀位。取消只中断循环，
已落盘的步骤不动；掉线后重启，批次里停在"已下发"的步骤就是状态未知的现场。续作
永远按步骤表接着走，确认过的步骤不再下发——这是"取消或掉线后能接着补"的唯一依据。
"""

from __future__ import annotations

import uuid

from ..errors import InterlockError
from ..store.jsonfile import JsonStore

# 步骤状态：pending → dispatched → confirmed / failed；核对可把 dispatched/failed 退回 pending。
STEP_PENDING = "pending"
STEP_DISPATCHED = "dispatched"
STEP_CONFIRMED = "confirmed"
STEP_FAILED = "failed"

# 批次状态：dispatching 是"正在下发"（重启后见到即按掉线处理）；cancelled /
# interrupted / failed 都是可续作的部分完成态；completed / aborted 是终态。
BATCH_DISPATCHING = "dispatching"
BATCH_CANCELLED = "cancelled"
BATCH_INTERRUPTED = "interrupted"
BATCH_FAILED = "failed"
BATCH_COMPLETED = "completed"
BATCH_ABORTED = "aborted"

OPEN_BATCH_STATUSES = (BATCH_DISPATCHING, BATCH_CANCELLED, BATCH_INTERRUPTED, BATCH_FAILED)
TERMINAL_BATCH_STATUSES = (BATCH_COMPLETED, BATCH_ABORTED)

# 合法步骤转换；confirmed 是终态——同一站同一步不会被第二条指令再改一次。
_STEP_TRANSITIONS = {
    STEP_PENDING: {STEP_DISPATCHED},
    STEP_DISPATCHED: {STEP_CONFIRMED, STEP_FAILED, STEP_PENDING},
    STEP_FAILED: {STEP_PENDING},
    STEP_CONFIRMED: set(),
}


class BatchJournal:
    """批次步骤日志的持久化封装（每批次一个 JSON 文档）。"""

    def __init__(self, store: JsonStore, prefix: str = "batch") -> None:
        self._store = store
        self._prefix = prefix.strip("/")

    @property
    def store(self) -> JsonStore:
        return self._store

    def new_id(self) -> str:
        """生成一个新的批次标识。"""

        return f"batch-{uuid.uuid4().hex[:12]}"

    def doc_id(self, batch_id: str) -> str:
        """返回某批次文档的标识。"""

        return f"{self._prefix}/{batch_id}"

    def create(
        self,
        batch_id: str,
        *,
        zones: list[str],
        demand_pct: float,
        target_c: float,
        steps: list[dict],
        flows: list[dict],
        ts: float,
    ) -> dict:
        """建批：把步骤表与流量分配一并落盘，返回批次文档。"""

        if self._store.exists(self.doc_id(batch_id)):
            raise InterlockError(f"批次已存在: {batch_id}", component="dispatch")
        batch = {
            "batch_id": batch_id,
            "epoch": 1,
            "status": BATCH_DISPATCHING,
            "zones": list(zones),
            "demand_pct": round(float(demand_pct), 2),
            "target_c": round(float(target_c), 2),
            "created_ts": round(float(ts), 3),
            "steps": [dict(step) for step in steps],
            "flows": [dict(flow) for flow in flows],
        }
        self.save(batch)
        return batch

    def load(self, batch_id: str) -> dict:
        """读取一个批次文档，缺失时抛 ``KeyError``。"""

        raw = self._store.read(self.doc_id(batch_id))
        if not isinstance(raw, dict):
            raise KeyError(f"未找到批次: {batch_id}")
        return raw

    def save(self, batch: dict) -> str:
        """把批次文档整体落盘，返回落盘路径。"""

        return self._store.write(self.doc_id(batch["batch_id"]), batch)

    def list_all(self) -> list[dict]:
        """返回全部批次（按建批时间、批次标识排序）。"""

        prefix = f"{self._prefix}/"
        batches: list[dict] = []
        for doc_id in self._store.list_ids():
            if not doc_id.startswith(prefix):
                continue
            raw = self._store.read(doc_id)
            if isinstance(raw, dict) and isinstance(raw.get("steps"), list):
                batches.append(raw)
        return sorted(
            batches,
            key=lambda item: (float(item.get("created_ts", 0.0)), str(item.get("batch_id", ""))),
        )

    def open_batches(self) -> list[dict]:
        """返回全部未完成批次（仍占用着站，等待续作或作废）。"""

        return [batch for batch in self.list_all() if batch.get("status") in OPEN_BATCH_STATUSES]

    def step(self, batch: dict, seq: int) -> dict:
        """按序号取批次里的一个步骤。"""

        for step in batch["steps"]:
            if int(step["seq"]) == int(seq):
                return step
        raise KeyError(f"批次 {batch['batch_id']} 没有步骤 {seq}")

    def mark(self, batch: dict, seq: int, status: str, **fields: object) -> dict:
        """推进一个步骤的状态：先校验转换合法，再落盘。"""

        step = self.step(batch, seq)
        current = str(step["status"])
        if status not in _STEP_TRANSITIONS.get(current, set()):
            raise InterlockError(
                f"步骤 {batch['batch_id']}:{seq} 不允许从 {current} 转到 {status}",
                component="dispatch",
            )
        step["status"] = status
        step.update(fields)
        self.save(batch)
        return step

    def set_status(self, batch: dict, status: str, **fields: object) -> dict:
        """更新批次状态并落盘。"""

        batch["status"] = status
        batch.update(fields)
        self.save(batch)
        return batch

    def bump_epoch(self, batch: dict) -> int:
        """续作前把批次世代加一：旧世代的在途指令一律作废。"""

        batch["epoch"] = int(batch.get("epoch", 1)) + 1
        self.save(batch)
        return int(batch["epoch"])
