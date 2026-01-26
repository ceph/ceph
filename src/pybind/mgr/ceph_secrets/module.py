# -*- coding: utf-8 -*-
import json
import re
from typing import Any, Dict, Optional, List, Tuple
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
                if not (ns and sc and name):  # target is optional for global scope
                    out[key] = None
                    continue
                ver = self.secret_get_version(namespace=ns, scope=sc, target=tgt, name=name)
                out[key] = ver
            except Exception as e:
                # never fail the whole batch for one bad entry
                self.log.warning("secret_get_versions: failed for %r: %s", key, e)
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
        ref = self.secret_mgr.make_ref(namespace, sc, target, name)
        try:
            rec = self.secret_mgr.get(ref)
        except CephSecretException:
            return {}
        return rec.to_json(include_data=reveal, include_internal=False)

    # ---------------------- RPC-ish helpers ----------------------

    def secret_ls(self,
                  namespace: Optional[str] = None,
                  scope: Optional[str] = None,
                  target: Optional[str] = None,
                  show_values: bool = False,
                  show_internals: bool = False) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        sc = SecretScope.from_str(scope) if scope else None
        good, bad = self.secret_mgr.ls(namespace=namespace, scope=sc, target=target)
        out_good: Dict[str, Any] = {}
        for r in good:
            key = f'{r.namespace}/{r.scope.value}/{r.target}/{r.name}'
            out_good[key] = r.to_json(include_data=bool(show_values), include_internal=show_internals)
        out_bad: Dict[str, Any] = {}
        for b in bad:
            out_bad[b.raw_key] = {'raw_key': b.raw_key, 'namespace': b.namespace, 'error': b.error}
        return out_good, out_bad

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
                           scope: str,
                           target: str,
                           name: str) -> Optional[int]:
        sc = SecretScope.from_str(scope)
        rec = self.secret_mgr.store.get(namespace, sc, target, name)
        return rec.version if rec is not None else None

    def secret_set_cli(self,
                       namespace: str,
                       scope: str,
                       target: str,
                       name: str,
                       data: str,
                       secret_type: str = 'Opaque') -> str:
        parsed_data = _parse_data_arg(data)
        self.secret_set_record(namespace=namespace, scope=scope, target=target, name=name,
                               data=parsed_data, secret_type=secret_type,
                               user_made=True, editable=True)
        return 'secret updated'

    def secret_set_record(self,
                          namespace: str,
                          scope: str,
                          target: str,
                          name: str,
                          data: Dict[str, Any],
                          secret_type: str = 'Opaque',
                          user_made: bool = True,
                          editable: bool = True) -> Dict[str, Any]:
        """Internal entrypoint (data is a dict)."""
        sc = SecretScope.from_str(scope)
        rec = self.secret_mgr.set(name, data, namespace=namespace, scope=sc,
                                  target=target or None,
                                  secret_type=secret_type,
                                  user_made=user_made,
                                  editable=editable)
        self._bump_epoch()
        return rec.to_json(include_data=True)

    def secret_rm(self,
                  namespace: str,
                  scope: str,
                  target: str,
                  name: str) -> str:
        sc = SecretScope.from_str(scope)
        existed = self.secret_mgr.store.rm(namespace, sc, target, name)
        if existed:
            self._bump_epoch()
        return 'removed' if existed else 'not found'

    def resolve_object(self, obj: Any) -> Any:
        return self.secret_mgr.resolve_object(obj)

    def scan_refs(self, obj: Any, namespace: str) -> Any:
        return self.secret_mgr.scan_refs(obj, namespace)

    def scan_unresolved_refs(self, obj: Any, namespace: str) -> Any:
        return self.secret_mgr.scan_unresolved_refs(obj, namespace)

    def import_raw_kv(self, entries: Dict[str, str], overwrite: bool = False) -> int:
        """Import raw KV entries into this module's store namespace.

        `entries` maps key -> raw JSON string.
        Keys MUST be in the module-local namespace (no leading 'mgr/<module>/').
        """
        imported = 0
        for k, v in entries.items():
            if not overwrite and self.get_store(k) is not None:
                continue
            self.set_store(k, v)
            imported += 1
        return imported

    # ---------------------- Module CLI commands ----------------------

    @CLICommand('secret ls', perm='r')
    def _cli_secret_ls(self,
                       namespace: Optional[str] = None,
                       scope: Optional[str] = None,
                       sec_target: Optional[str] = None,
                       reveal: bool = False,
                       show_internals: bool = False) -> HandleCommandResult:
        try:
            good, bad = self.secret_ls(namespace=namespace,
                                       scope=scope,
                                       target=sec_target,
                                       show_values=reveal,
                                       show_internals=show_internals)
            out = good | bad
            return HandleCommandResult(0, json.dumps(out, indent=2, sort_keys=True), '')
        except CephSecretException as e:
            return HandleCommandResult(-errno.EINVAL, f'secret error: {e}', '')

    @CLICommand('secret get', perm='r')
    def _cli_secret_get(self,
                        namespace: str,
                        scope: str,
                        sec_target: str,
                        sec_name: str,
                        reveal: bool = False) -> HandleCommandResult:
        try:
            res = self._secret_get(namespace=namespace, scope=scope, target=sec_target, name=sec_name, reveal=reveal)
            if not res:
                return HandleCommandResult(-errno.ENOENT, 'secret error: not found', '')
            return HandleCommandResult(0, json.dumps(res, indent=2, sort_keys=True), '')
        except CephSecretException as e:
            return HandleCommandResult(-errno.EINVAL, f'secret error: {e}', '')

    @CLICommand('secret set', perm='rw')
    def _cli_secret_set(self,
                        namespace: str,
                        scope: str,
                        sec_target: str,
                        sec_name: str,
                        data: str = '',
                        secret_type: str = 'Opaque',
                        inbuf: Optional[str] = None) -> HandleCommandResult:

        try:
            input_data = inbuf or data
            if not input_data:
                return HandleCommandResult(-errno.EINVAL, 'secret error: use --data or -i to provide secret data', '')

            msg = self.secret_set_cli(namespace=namespace, scope=scope, target=sec_target,
                                      name=sec_name, data=input_data, secret_type=secret_type)
            return HandleCommandResult(0, msg, '')
        except CephSecretException as e:
            return HandleCommandResult(-errno.EINVAL, f'secret error: {e}', '')

    @CLICommand('secret get-key', perm='r')
    def _cli_secret_get_by_path(self, path: str, reveal: bool = False) -> HandleCommandResult:
        try:
            ns, sc, target, name = parse_secret_path(path)
            res = self._secret_get(namespace=ns, scope=sc, target=target, name=name, reveal=reveal)
            if not res:
                return HandleCommandResult(-errno.ENOENT, 'secret error: not found', '')
            return HandleCommandResult(0, json.dumps(res, indent=2, sort_keys=True), '')
        except CephSecretException as e:
            return HandleCommandResult(-errno.EINVAL, f'secret error: {e}', '')

    @CLICommand('secret set-key', perm='rw')
    def _cli_secret_set_by_path(self,
                                path: str,
                                data: str = '',
                                secret_type: str = 'Opaque',
                                inbuf: Optional[str] = None) -> HandleCommandResult:
        try:
            input_data = inbuf or data
            if not input_data:
                return HandleCommandResult(-errno.EINVAL, 'secret error: use --data or -i to provide secret data', '')

            ns, sc, target, name = parse_secret_path(path)
            msg = self.secret_set_cli(namespace=ns, scope=sc.value, target=target,
                                      name=name, data=input_data,
                                      secret_type=secret_type)

            return HandleCommandResult(0, msg, '')
        except CephSecretException as e:
            return HandleCommandResult(-errno.EINVAL, f'secret error: {e}', '')

    @CLICommand('secret rm', perm='rw')
    def _cli_secret_rm(self,
                       namespace: str,
                       scope: str,
                       target: str,
                       name: str) -> HandleCommandResult:
        try:
            msg = self.secret_rm(namespace=namespace, scope=scope, target=target, name=name)
            if msg == 'not found':
                return HandleCommandResult(-errno.ENOENT, 'secret error: not found', '')
            return HandleCommandResult(0, msg, '')
        except CephSecretException as e:
            return HandleCommandResult(-errno.EINVAL, f'secret error: {e}', '')

    @CLICommand('secret rm-key', perm='rw')
    def _cli_secret_rm_by_path(self, path: str) -> HandleCommandResult:
        try:
            ns, sc, target, name = parse_secret_path(path)
            msg = self.secret_rm(namespace=ns, scope=sc.value, target=target, name=name)
            if msg == 'not found':
                return HandleCommandResult(-errno.ENOENT, 'secret error: not found', '')
            return HandleCommandResult(0, msg, '')
        except CephSecretException as e:
            return HandleCommandResult(-errno.EINVAL, f'secret error: {e}', '')
