# -*- coding: utf-8 -*-
# SPDX-License-Identifier: LGPL-2.1-or-later
#
# Cephadm Operations Registry
# --------------------------
# A lightweight persistence layer for tracking independent cephadm operations
# (placement changes, deployments, removals, upgrades, etc.) with progress,
# state, and causality.

from __future__ import annotations

import json
import time
import uuid
from typing import Any, Dict, List, Optional, TypeVar, TypedDict


class OperationRecord(TypedDict, total=False):
    op_id: str
    kind: str
    title: str
    service_type: str
    service_name: str
    requested_by: str
    source: str
    submitted_at: str
    updated_at: str
    state: str
    progress_done: int
    progress_total: int
    current_step: str
    blocked_by: List[str]
    details: Dict[str, Any]


T = TypeVar("T")


class OperationsRegistry:
    """
    Operations Registry

    Persists:
      • cephadm/operations/index   -> list of active op_ids
      • cephadm/operations/<id>    -> full operation record (active)
      • cephadm/operations/history -> ring buffer of completed/failed operations
    """

    INDEX_KEY: str = "cephadm/operations/index"
    HIST_KEY: str = "cephadm/operations/history"

    def __init__(self, mgr: Any, history_max: int = 200) -> None:
        self.mgr: Any = mgr
        self.history_max: int = history_max
        # Best-effort cleanup of stale no-op operations created before
        # we started guarding on progress_total > 0.
        self._cleanup_noop_operations()

    # --------------------------------------------------------------
    # Utils
    # --------------------------------------------------------------

    @staticmethod
    def _now() -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def _cleanup_noop_operations(self) -> None:
        """
        Remove operations from the active index that have progress_total == 0.

        These correspond to reconciles where cephadm didn't actually add/remove
        any daemons. Earlier code created ops for those; this cleans them up.
        """
        try:
            idx: List[str] = self._load_json(self.INDEX_KEY, [])
        except Exception:
            return

        if not idx:
            return

        keep: List[str] = []
        for op_id in idx:
            key = f"cephadm/operations/{op_id}"
            try:
                op = self._load_json(key, {})
            except Exception:
                # If we can't load it, drop from active index
                continue
            if not isinstance(op, dict):
                continue
            if int(op.get("progress_total", 0)) <= 0:
                # drop no-op active ops
                continue
            keep.append(op_id)

        self._dump_json(self.INDEX_KEY, keep)

    def _load_json(self, key: str, default: T) -> T:
        v = self.mgr.get_store(key)
        return json.loads(v) if v else default

    def _dump_json(self, key: str, obj: Any) -> None:
        self.mgr.set_store(key, json.dumps(obj))

    def guess_actor(self) -> str:
        """
        Best-effort way to identify who initiated the action.
        The cephadm module sets mgr.last_command_actor when a CLI/API call runs.
        """
        return getattr(self.mgr, "last_command_actor", "reconciler")

    # --------------------------------------------------------------
    # CRUD
    # --------------------------------------------------------------

    def begin_operation(
        self,
        *,
        kind: str,
        title: str,
        service_name: str,
        service_type: str,
        requested_by: Optional[str],
        progress_total: int,
        source: str = "reconcile",
        details: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Create and register a new operation.
        """
        op_id: str = str(uuid.uuid4())
        base_details: Dict[str, Any] = details.copy() if details else {}
        # step history: list of {when, message, progress_done}
        base_details.setdefault("steps", [])

        op: OperationRecord = {
            "op_id": op_id,
            "kind": kind,
            "title": title,
            "service_type": service_type,
            "service_name": service_name,
            "requested_by": requested_by or "unknown",
            "source": source,
            "submitted_at": self._now(),
            "updated_at": self._now(),
            "state": "running" if progress_total > 0 else "pending",
            "progress_done": 0,
            "progress_total": int(progress_total),
            "current_step": "Queued" if progress_total == 0 else "",
            "blocked_by": [],
            "details": base_details,
        }

        # Add to active index
        idx: List[str] = self._load_json(self.INDEX_KEY, [])
        idx.append(op_id)
        self._dump_json(self.INDEX_KEY, idx)

        # Store the record
        self._dump_json(f"cephadm/operations/{op_id}", op)

        return op_id

    def update_operation(
        self,
        op_id: str,
        *,
        current_step: Optional[str] = None,
        progress_done: Optional[int] = None,
        blocked_by: Optional[List[str]] = None,
    ) -> None:
        """
        Update progress, step, or blockers.
        """
        key: str = f"cephadm/operations/{op_id}"
        op: Optional[OperationRecord] = self._load_json(key, None)
        if not op:
            return

        # progress first, so the step entry sees the latest number
        if progress_done is not None:
            op["progress_done"] = int(progress_done)

        if current_step is not None:
            op["current_step"] = current_step
            # track step history
            details = op.setdefault("details", {})
            steps = details.setdefault("steps", [])
            # keep it simple: append message + when + current progress
            steps.append(
                {
                    "when": self._now(),
                    "message": current_step,
                    "progress_done": int(op.get("progress_done", 0)),
                }
            )

        if blocked_by is not None:
            op["blocked_by"] = blocked_by
            if blocked_by:
                op["state"] = "pending"

        # Auto-switch to running if we still have work and no blockers
        if (
            op.get("progress_total")
            and op.get("progress_done", 0) < op.get("progress_total", 0)
            and not op.get("blocked_by")
        ):
            op["state"] = "running"

        op["updated_at"] = self._now()
        self._dump_json(key, op)

    def complete_operation(self, op_id: str) -> None:
        self._finalize(op_id, "completed")

    def fail_operation(self, op_id: str, reason: str) -> None:
        self._finalize(op_id, "failed", reason)

    # --------------------------------------------------------------
    # Helpers
    # --------------------------------------------------------------

    def _finalize(
        self,
        op_id: str,
        state: str,
        reason: Optional[str] = None,
    ) -> None:
        """
        Move operation from active index into history buffer.
        """
        key: str = f"cephadm/operations/{op_id}"
        op: Optional[OperationRecord] = self._load_json(key, None)
        if not op:
            return

        op["state"] = state
        if reason:
            details = op.setdefault("details", {})
            details["error"] = reason
        op["updated_at"] = self._now()

        # Remove from active index
        idx: List[str] = self._load_json(self.INDEX_KEY, [])
        if op_id in idx:
            idx.remove(op_id)
            self._dump_json(self.INDEX_KEY, idx)

        # Add to history (bounded)
        hist: List[OperationRecord] = self._load_json(self.HIST_KEY, [])
        hist.append(op)
        if len(hist) > self.history_max:
            hist = hist[-self.history_max :]
        self._dump_json(self.HIST_KEY, hist)

        # We do not keep the final record under cephadm/operations/<id>
        # because it's now archived in history. If desired, keep it; optional.
        # For now, remove active record:
        #   self.mgr.set_store(key, None)
