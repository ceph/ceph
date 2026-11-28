# -*- coding: utf-8 -*-
"""Track independent cephadm operations (placement updates, service removals)."""

from __future__ import annotations

import json
import time
import uuid
from typing import Any, Dict, List, Optional, TypeVar

T = TypeVar('T')


class OperationsRegistry:
    """
    Lightweight KV-backed registry for active and recently completed operations.

    Store keys:
      cephadm/operations/index   -> list of active op_ids
      cephadm/operations/<id>    -> active operation record
      cephadm/operations/history -> bounded list of completed/failed ops
    """

    INDEX_KEY = 'cephadm/operations/index'
    HIST_KEY = 'cephadm/operations/history'

    def __init__(self, mgr: Any, history_max: int = 100) -> None:
        self.mgr = mgr
        self.history_max = history_max

    @staticmethod
    def _now() -> str:
        return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

    def _load_json(self, key: str, default: T) -> T:
        v = self.mgr.get_store(key)
        return json.loads(v) if v else default

    def _dump_json(self, key: str, obj: Any) -> None:
        self.mgr.set_store(key, json.dumps(obj))

    def begin_operation(
        self,
        *,
        kind: str,
        title: str,
        service_name: str,
        service_type: str,
        progress_total: int,
        source: str = 'reconcile',
        details: Optional[Dict[str, Any]] = None,
    ) -> str:
        op_id = str(uuid.uuid4())
        op: Dict[str, Any] = {
            'op_id': op_id,
            'kind': kind,
            'title': title,
            'service_type': service_type,
            'service_name': service_name,
            'source': source,
            'submitted_at': self._now(),
            'updated_at': self._now(),
            'state': 'running',
            'progress_done': 0,
            'progress_total': int(progress_total),
            'current_step': '',
            'details': details.copy() if details else {},
        }
        op['details'].setdefault('steps', [])

        idx: List[str] = self._load_json(self.INDEX_KEY, [])
        idx.append(op_id)
        self._dump_json(self.INDEX_KEY, idx)
        self._dump_json(f'cephadm/operations/{op_id}', op)
        return op_id

    def update_operation(
        self,
        op_id: str,
        *,
        current_step: Optional[str] = None,
        progress_done: Optional[int] = None,
    ) -> None:
        key = f'cephadm/operations/{op_id}'
        op = self._load_json(key, None)
        if not op:
            return

        if progress_done is not None:
            op['progress_done'] = int(progress_done)

        if current_step is not None:
            op['current_step'] = current_step
            steps = op.setdefault('details', {}).setdefault('steps', [])
            steps.append({
                'when': self._now(),
                'message': current_step,
                'progress_done': int(op.get('progress_done', 0)),
            })

        op['state'] = 'running'
        op['updated_at'] = self._now()
        self._dump_json(key, op)

    def complete_operation(self, op_id: str) -> None:
        self._finalize(op_id, 'completed')

    def fail_operation(self, op_id: str, reason: str) -> None:
        self._finalize(op_id, 'failed', reason)

    def _finalize(self, op_id: str, state: str, reason: Optional[str] = None) -> None:
        key = f'cephadm/operations/{op_id}'
        op = self._load_json(key, None)
        if not op:
            return

        op['state'] = state
        if reason:
            op.setdefault('details', {})['error'] = reason
        op['updated_at'] = self._now()

        idx: List[str] = self._load_json(self.INDEX_KEY, [])
        if op_id in idx:
            idx.remove(op_id)
            self._dump_json(self.INDEX_KEY, idx)

        hist: List[Dict[str, Any]] = self._load_json(self.HIST_KEY, [])
        hist.append(op)
        if len(hist) > self.history_max:
            hist = hist[-self.history_max:]
        self._dump_json(self.HIST_KEY, hist)
        self.mgr.set_store(key, None)

    def list_operations(self, show_all: bool = False, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        ops: List[Dict[str, Any]] = []

        idx: List[str] = self._load_json(self.INDEX_KEY, [])
        for op_id in reversed(idx):
            op = self._load_json(f'cephadm/operations/{op_id}', None)
            if isinstance(op, dict) and op.get('state') not in ('completed', 'failed'):
                ops.append(op)

        if show_all:
            hist: List[Dict[str, Any]] = self._load_json(self.HIST_KEY, [])
            for op in reversed(hist):
                if isinstance(op, dict):
                    ops.append(op)

        if limit is not None and limit > 0:
            ops = ops[:limit]
        return ops

    @staticmethod
    def format_operations(ops: List[Dict[str, Any]]) -> str:
        if not ops:
            return 'No operations found'

        lines: List[str] = []
        for n, op in enumerate(ops, 1):
            done = int(op.get('progress_done', 0))
            total = int(op.get('progress_total', 0))
            step = str(op.get('current_step') or '')
            title = str(op.get('title') or op.get('service_name') or '')
            state = str(op.get('state') or '')

            lines.append(f'==== Operation {n} ===============================')
            lines.append(f'Title: {title}')
            if op.get('service_name'):
                lines.append(f'Service: {op["service_name"]}')
            lines.append(f'State: {state}')
            status = f'Status: {done}/{total}'
            if step:
                status += f' — {step}'
            lines.append(status)
            lines.append(f'Submitted: {op.get("submitted_at", "")}')
            if state == 'failed':
                err = (op.get('details') or {}).get('error')
                if err:
                    lines.append(f'Error: {err}')
            lines.append('')
        return '\n'.join(lines).rstrip()
