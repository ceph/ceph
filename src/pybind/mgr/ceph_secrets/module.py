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
