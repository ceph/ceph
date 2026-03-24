# -*- coding: utf-8 -*-
import json
from typing import Any, Dict, Optional, List, Tuple
import errno

from mgr_module import (
    MgrModule,
    CLICommandBase,
    HandleCommandResult,
    Option
)
from .secret_mgr import SecretMgr
from .secret_backend import SecretStorageBackend
from ceph_secrets_types import CephSecretException, SecretScope, parse_secret_path


CephSecretsCLICommand = CLICommandBase.make_registry_subtype("CephSecretsCLICommand")


def _parse_data_arg(data: str) -> Dict[str, Any]:
    s = (data or "").strip()
    if not s:
        raise CephSecretException("--data must not be empty")

    # 1) JSON object support
    try:
        payload = json.loads(s)
    except Exception:
        raise CephSecretException("Invalid JSON for secret data")
    if not isinstance(payload, dict):
        raise CephSecretException("Secret --data must be a JSON object")
    return payload


class Module(MgrModule):
    """Standalone secrets mgr module.

    This module owns the mgr KV-store entries for secrets (namespace: mgr/secrets)
    and provides both:
      - RPC methods for other mgr modules via `remote(...)`
      - CLI commands: `ceph secret ...`

    Storage keys inside the mgr KV store:
      secret data:   secret_store/v1/<namespace>/<scope>/<target>/<name>
      epoch (meta):  secret_store/meta/<namespace>/_epoch

    Epoch is per-namespace: a mutation in namespace A does not affect the epoch
    of namespace B, so consumers only see changes relevant to their namespace.
    Epoch logic lives entirely in the store backend so future backends
    (e.g. Vault) can implement it natively.
    """
    CLICommand = CephSecretsCLICommand

    MODULE_OPTIONS = [
        Option(
            'secrets_backend',
            type='str',
            default='mon',
            desc='Secrets storage backend. Currently only "mon" (Mon KV store) is supported.',
        ),
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        backend_name: str = str(self.get_module_option('secrets_backend'))
        try:
            backend_cls = SecretStorageBackend._registry[backend_name]
            self.secret_mgr = SecretMgr(backend_cls(self))
        except KeyError as e:
            raise RuntimeError(f'ceph_secrets: {e}') from e

    # ------------------------------------------------------------------ epoch

    def secret_get_epoch(self, namespace: str) -> int:
        """Return the current epoch for *namespace*.

        Consumers (e.g., cephadm) can use this as a cheap change-detector:
        if the epoch hasn't changed since the last check, no secrets in this
        namespace have been mutated.
        """
        return self.secret_mgr.store.get_epoch(namespace)

    # ------------------------------------------------------------------ batch helpers

    def secret_get_versions(self, refs: List[Dict[str, str]]) -> Dict[str, Optional[int]]:
        """Batch-get versions for a list of secret identifiers.

        Each entry in `refs` must contain: namespace, scope, target, name.
        Returns a dict keyed by 'namespace:scope:target:name' -> version (or None).
        """
        out: Dict[str, Optional[int]] = {}
        for r in refs or []:
            ns = r.get('namespace', '')
            sc = r.get('scope', '')
            tgt = r.get('target', '')
            name = r.get('name', '')
            key = f"{ns}:{sc}:{tgt}:{name}"
            try:
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

    # ---------------------- helper methods ----------------------

    def secret_ls(
        self,
        namespace: Optional[str] = None,
        scope: Optional[str] = None,
        target: Optional[str] = None,
        show_values: bool = False,
        show_internals: bool = False
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        sc = SecretScope.from_str(scope) if scope else None
        good, bad = self.secret_mgr.ls(namespace=namespace, scope=sc, target=target)
        out_good: Dict[str, Any] = {}
        for r in good:
            if r.target:
                key = f'{r.namespace}/{r.scope.value}/{r.target}/{r.name}'
            else:
                key = f'{r.namespace}/{r.scope.value}/{r.name}'
            out_good[key] = r.to_json(include_data=bool(show_values), include_internal=show_internals)
        out_bad: Dict[str, Any] = {}
        for b in bad:
            out_bad[b.raw_key] = {'raw_key': b.raw_key, 'namespace': b.namespace, 'error': b.error}
        return out_good, out_bad

    def secret_get(self,
                   namespace: str,
                   scope: SecretScope,
                   target: str,
                   name: str,
                   reveal: bool = False) -> Dict[str, Any]:
        ref = self.secret_mgr.make_ref(namespace, scope, target, name)
        try:
            rec = self.secret_mgr.get(ref)
        except CephSecretException:
            return {}
        return rec.to_json(include_data=reveal, include_internal=False)

    def secret_get_version(self,
                           namespace: str,
                           scope: str,
                           target: str,
                           name: str) -> Optional[int]:
        sc = SecretScope.from_str(scope)
        ref = self.secret_mgr.make_ref(namespace, sc, target, name)
        try:
            rec = self.secret_mgr.get(ref)
        except CephSecretException:
            return None
        return rec.version if rec is not None else None

    def secret_set(self,
                   namespace: str,
                   scope: SecretScope,
                   target: str,
                   name: str,
                   data: Dict[str, Any],
                   secret_type: str = 'Opaque',
                   user_made: bool = True,
                   editable: bool = True) -> Dict[str, Any]:
        """Internal entrypoint (data is a dict)."""
        rec = self.secret_mgr.set(
            name, data, namespace=namespace, scope=scope,
            target=target or None,
            secret_type=secret_type,
            user_made=user_made,
            editable=editable)
        return rec.to_json(include_data=False, include_internal=False)

    def secret_rm(self,
                  namespace: str,
                  scope: SecretScope,
                  target: str,
                  name: str) -> bool:
        return self.secret_mgr.rm(namespace, scope, target, name)

    def resolve_object(self, obj: Any) -> Any:
        return self.secret_mgr.resolve_object(obj)

    def scan_refs(self, obj: Any, namespace: str) -> Any:
        return self.secret_mgr.scan_refs(obj, namespace)

    def scan_unresolved_refs(self, obj: Any, namespace: str) -> Any:
        return self.secret_mgr.scan_unresolved_refs(obj, namespace)

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
    def _cli_secret_get_by_path(self, path: str, reveal: bool = False) -> HandleCommandResult:
        try:
            ns, sc, target, name = parse_secret_path(path)
            res = self.secret_get(namespace=ns, scope=sc, target=target, name=name, reveal=reveal)
            if not res:
                return HandleCommandResult(-errno.ENOENT, 'secret error: not found', '')
            return HandleCommandResult(0, json.dumps(res, indent=2, sort_keys=True), '')
        except CephSecretException as e:
            return HandleCommandResult(-errno.EINVAL, f'secret error: {e}', '')

    @CLICommand('secret set', perm='rw')
    def _cli_secret_set_by_path(self,
                                path: str,
                                secret_type: str = 'Opaque',
                                inbuf: Optional[str] = None) -> HandleCommandResult:
        try:
            if not inbuf:
                return HandleCommandResult(-errno.EINVAL, 'secret error: use -i to provide secret data', '')

            parsed_data = _parse_data_arg(inbuf)
            ns, sc, target, name = parse_secret_path(path)
            msg = self.secret_set(namespace=ns, scope=sc, target=target,
                                  name=name, data=parsed_data,
                                  secret_type=secret_type)

            return HandleCommandResult(0, msg, '')
        except CephSecretException as e:
            return HandleCommandResult(-errno.EINVAL, f'secret error: {e}', '')

    @CLICommand('secret rm', perm='rw')
    def _cli_secret_rm_by_path(self, path: str) -> HandleCommandResult:
        try:
            ns, sc, target, name = parse_secret_path(path)
            existed = self.secret_rm(namespace=ns, scope=sc, target=target, name=name)
            if not existed:
                return HandleCommandResult(-errno.ENOENT, 'secret error: not found', '')
            return HandleCommandResult(0, 'secret removed correctly', '')
        except CephSecretException as e:
            return HandleCommandResult(-errno.EINVAL, f'secret error: {e}', '')
