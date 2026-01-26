# -*- coding: utf-8 -*-
import json
import re
from typing import Any, Dict, Optional, Union, List
import errno

from mgr_module import (
    MgrModule,
    CLICommand,
    HandleCommandResult,
)
from .secret_mgr import SecretMgr
from ceph_secrets_types import CephSecretException, SecretScope, parse_secret_path


KV_SPLIT_RE = re.compile(r"[,\s]+")

# A monotonic epoch bumped on any secret mutation (set/rm).
# Used by consumers (e.g., cephadm) to cheaply detect whether they need
# to refresh secret dependency versions.
SECRET_EPOCH_KEY = 'secret_store/v1/_epoch'


def _parse_data_arg(data: str) -> Dict[str, Any]:
    s = (data or "").strip()
    if not s:
        raise CephSecretException("--data must not be empty")

    # 1) JSON object support
    if s.startswith("{"):
        try:
            payload = json.loads(s)
        except Exception as e:
            raise CephSecretException(f"Invalid JSON for --data: {e}")
        if not isinstance(payload, dict):
            raise CephSecretException("Secret --data must be a JSON object")
        return payload

    # 2) k=v support (one or many pairs)
    parts = [p for p in KV_SPLIT_RE.split(s) if p]
    if not parts:
        raise CephSecretException("Invalid --data")

    out: Dict[str, Any] = {}
    for p in parts:
        if "=" not in p:
            raise CephSecretException(
                "Invalid --data. Use JSON ('{\"k\":\"v\"}') or k=v (multiple pairs separated by space or comma)."
            )
        k, v = p.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k:
            raise CephSecretException("Invalid --data: empty key in k=v")
        out[k] = v
    return out


class Module(MgrModule):
    """Standalone secrets mgr module.

    This module owns the mgr KV-store entries for secrets (namespace: mgr/secrets)
    and provides both:
      - RPC methods for other mgr modules via `remote(...)`
      - CLI commands: `ceph secret ...`

    Storage keys inside the mgr KV store are unchanged:
      secret_store/v1/<namespace>/<scope>/<target>/<name>

    """

    MODULE_OPTIONS: list = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.secret_mgr = SecretMgr(self)

    def _read_epoch(self) -> int:
        raw = self.get_store(SECRET_EPOCH_KEY)
        if raw is None:
            return 0
        try:
            return int(str(raw))
        except Exception:
            return 0

    def _bump_epoch(self) -> int:
        """Best-effort increment; mon KV store has no atomic increments."""
        epoch = self._read_epoch() + 1
        self.set_store(SECRET_EPOCH_KEY, str(epoch))
        return epoch

    def secret_get_epoch(self) -> int:
        """Return the current secrets epoch.

        Consumers (e.g., cephadm) can use this as a cheap change detector
        to avoid refreshing per-secret versions when nothing changed.
        """
        return self._read_epoch()

    def secret_get_versions(self, refs: List[Dict[str, str]]) -> Dict[str, Optional[int]]:
        """Batch-get versions for a list of secret identifiers.

        Each entry in `refs` must contain: namespace, scope, target, name.
        Returns a dict keyed by 'namespace:scope:target:name' -> version (or None).
        """
        out: Dict[str, Optional[int]] = {}
        for r in refs or []:
            try:
                ns = r.get('namespace', '')
                sc = r.get('scope', '')
                tgt = r.get('target', '')
                name = r.get('name', '')
                key = f"{ns}:{sc}:{tgt}:{name}"
                if not (ns and sc and tgt and name):
                    out[key] = None
                    continue
                ver = self.secret_get_version(namespace=ns, scope=sc, target=tgt, name=name)
                out[key] = ver
            except Exception:
                # never fail the whole batch for one bad entry
                out[key] = None
        return out

    # ---------------------- Private methods ----------------------

    def _secret_get(self,
                    namespace: str,
                    scope: str,
                    target: str,
                    name: str,
                    reveal: bool = False) -> Dict[str, Any]:
        sc = SecretScope.from_str(scope)
        rec = self.secret_mgr.store.get(namespace, sc, target, name)
        if rec is None:
            return {}
        return rec.to_json(include_data=reveal, include_internal=False)

    # ---------------------- RPC-ish helpers ----------------------

    def secret_ls(self,
                  namespace: str,
                  scope: str = '',
                  target: str = '',
                  show_values: bool = False,
                  show_internals: bool = False) -> Dict[str, Any]:
        sc = SecretScope.from_str(scope) if scope else None
        recs = self.secret_mgr.ls(namespace=namespace, scope=sc, target=target or None)
        out: Dict[str, Any] = {}
        for r in recs:
            key = f'{r.namespace}/{r.scope.value}/{r.target}/{r.name}'
            out[key] = r.to_json(include_data=bool(show_values), include_internal=show_internals)
        return out

    def secret_get_data(self,
                        namespace: str,
                        scope: str,
                        target: str,
                        name: str) -> Dict[str, Any]:
        sc = SecretScope.from_str(scope)
        rec = self.secret_mgr.store.get(namespace, sc, target, name)
        if rec is None:
            return {}
        return dict(rec.data)

    def secret_get_version(self,
                           namespace: str,
                           scope: Union[str, SecretScope],
                           target: str,
                           name: str) -> Optional[int]:
        sc = scope if isinstance(scope, SecretScope) else SecretScope.from_str(scope)
        rec = self.secret_mgr.store.get(namespace, sc, target, name)
        return rec.version if rec is not None else None
