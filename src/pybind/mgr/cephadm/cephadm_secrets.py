# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import hashlib
import logging
from typing import Any, Dict, List, Optional, Union

from ceph_secrets_client import CephSecretsClient
from ceph_secrets_types import SecretScope


CEPHADM_NAMESPACE = 'cephadm'

logger = logging.getLogger(__name__)


class CephadmSecrets:
    """cephadm-facing helper over the dedicated secrets mgr-module.

    - Keeps cephadm code readable
    - Preserves cephadm-specific legacy KV fallback/migration logic
    - Delegates canonical ops to secrets mgr-module via CephSecretsClient
    """

    def __init__(self, mgr: Any):
        self.mgr = mgr
        self.client = CephSecretsClient(mgr)

        # Cache of computed secret dependencies for specs, keyed by service name.
        # Entry: {"spec_hash": str, "epoch": int, "refs": List[SecretRef], "deps": List[str]}
        self._deps_cache: Dict[str, Dict[str, Any]] = {}

        # Best-effort one-way import of existing cephadm secret_store/v1/* entries
        # into the dedicated secrets module namespace. Idempotent (no overwrite).
        try:
            legacy = self.mgr.get_store_prefix('secret_store/v1/') or {}
            if legacy:
                self.client.import_raw_kv(entries=legacy, overwrite=False)
        except Exception as e:
            # If the secrets module is disabled/not present yet, callers will see
            # errors on first real usage.
            logger.error(f'Failed to import legacy secrets: {e}')

    # ---- canonical secret operations (delegated) ----

    def secret_get(self, namespace: str, scope: SecretScope, target: str, name: str) -> Dict[str, Any]:
        return self.client.secret_get_data(
            namespace=namespace,
            scope=scope,
            target=target,
            name=name,
        )

    def secret_get_version(self, namespace: str, scope: Union[str, SecretScope], target: str, name: str) -> Optional[int]:
        try:
            return self.client.secret_get_version(
                namespace=namespace,
                scope=scope,
                target=target,
                name=name,
            )
        except Exception as e:
            logger.error(f'Cannot get secret version : {e}')
            return None

    def set(
        self,
        name: str,
        data: Dict[str, Any],
        scope: Optional[Union[str, Any]] = SecretScope.GLOBAL,
        target: Optional[str] = '',
        secret_type: str = 'Opaque',
        user_made: bool = True,
        editable: bool = True,
    ) -> Dict[str, Any]:
        return self.client.secret_set_record(
            namespace=CEPHADM_NAMESPACE,
            scope=scope,
            target=target or '',
            name=name,
            data=data,
            secret_type=secret_type,
            user_made=user_made,
            editable=editable,
        )

    def rm(self, namespace: str, scope: Union[str, SecretScope], target: str, name: str) -> bool:
        return self.client.secret_rm(namespace=namespace, scope=scope, target=target, name=name)

    def find_unresolved_secrets(self, obj: Any) -> Any:
        return self.client.scan_unresolved_refs(obj=obj, namespace=CEPHADM_NAMESPACE)

    def _stable_json(self, obj: Any) -> str:
        # Deterministic JSON encoding for hashing.
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)

    def _spec_cache_key(self, spec_json: Dict[str, Any]) -> str:
        # Prefer stable service identity if present, else fall back to hash.
        stype = str(spec_json.get('service_type') or '')
        sid = str(spec_json.get('service_id') or '')
        if stype and sid:
            return f"{stype}.{sid}"
        # Some specs use 'service_name' already
        sname = spec_json.get('service_name')
        if isinstance(sname, str) and sname:
            return sname
        return ''

    def _get_secrets_epoch(self) -> int:
        # Prefer direct client method if available, else use mgr.remote.
        try:
            if hasattr(self.client, 'secret_get_epoch'):
                return int(self.client.secret_get_epoch())  # type: ignore
        except Exception:
            pass
        try:
            # Module name matches the python package: 'ceph_secrets'
            return int(self.mgr.remote('ceph_secrets', 'secret_get_epoch'))
        except Exception:
            # If the secrets module is unavailable, force refresh on each call.
            return -1

    def _get_versions_batch(self, refs: Any) -> Dict[str, Optional[int]]:
        # Try to use a batch RPC to avoid N remote calls.
        req = []
        for r in refs or []:
            try:
                req.append({
                    'namespace': r.namespace,
                    'scope': r.scope.value,
                    'target': r.target,
                    'name': r.name,
                })
            except Exception:
                continue
        if not req:
            return {}
        try:
            return self.mgr.remote('ceph_secrets', 'secret_get_versions', req)
        except Exception:
            # Fall back to per-secret calls
            out: Dict[str, Optional[int]] = {}
            for r in refs or []:
                key = f"{r.namespace}:{r.scope.value}:{r.target}:{r.name}"
                try:
                    out[key] = self.secret_get_version(r.namespace, r.scope, r.target, r.name)
                except Exception:
                    out[key] = None
            return out

    def deps_for_spec(self, spec: Any) -> List[str]:
        """Compute secret dependencies for a ServiceSpec with caching.

        - Cache secret refs by spec hash to avoid rescanning JSON every serve loop.
        - Use the secrets epoch to avoid per-secret version lookups when nothing changed.
        """
        spec_json = spec.to_json() if hasattr(spec, 'to_json') else spec
        if not isinstance(spec_json, dict):
            # Defensive: treat as generic object
            spec_json = {}
        stable = self._stable_json(spec_json)
        spec_hash = hashlib.sha256(stable.encode('utf-8')).hexdigest()

        cache_key = self._spec_cache_key(spec_json) or spec_hash
        epoch = self._get_secrets_epoch()

        cached = self._deps_cache.get(cache_key)
        if cached and cached.get('spec_hash') == spec_hash and cached.get('epoch') == epoch:
            return list(cached.get('deps') or [])

        # Reuse cached refs if spec unchanged; otherwise rescan
        if cached and cached.get('spec_hash') == spec_hash:
            refs = cached.get('refs') or []
        else:
            refs = self.client.scan_refs(obj=spec_json, namespace=CEPHADM_NAMESPACE)

        versions = self._get_versions_batch(refs)
        deps: List[str] = []
        for ref in sorted(refs, key=lambda x: (x.namespace, x.scope.value, x.target, x.name, x.key or '')):
            key = f"{ref.namespace}:{ref.scope.value}:{ref.target}:{ref.name}"
            sec_version = versions.get(key)
            if sec_version is None:
                logger.error(f'Secret not found: {ref.to_uri()}')
            else:
                deps.append(f'secret:{ref.namespace}:{ref.scope.value}:{ref.target}:{ref.name}:{sec_version}')

        self._deps_cache[cache_key] = {
            'spec_hash': spec_hash,
            'epoch': epoch,
            'refs': refs,
            'deps': deps,
        }
        return deps

    def deps_for_object(self, obj: Any) -> List[str]:
        """Backward-compatible helper for callers that pass raw objects/dicts.

        This does not cache by spec hash because the caller may not be a ServiceSpec.
        """
        refs = self.client.scan_refs(obj=obj, namespace=CEPHADM_NAMESPACE)
        versions = self._get_versions_batch(refs)
        deps: List[str] = []
        for ref in sorted(refs, key=lambda x: (x.namespace, x.scope.value, x.target, x.name, x.key or '')):
            key = f"{ref.namespace}:{ref.scope.value}:{ref.target}:{ref.name}"
            sec_version = versions.get(key)
            if sec_version is None:
                logger.error(f'Secret not found: {ref.to_uri()}')
            else:
                deps.append(f'secret:{ref.namespace}:{ref.scope.value}:{ref.target}:{ref.name}:{sec_version}')
        return deps

    def resolve_object(self, obj: Any) -> Any:
        return self.client.resolve_object(obj=obj, namespace=CEPHADM_NAMESPACE)

    def _load_basic_auth_secret(self, secret_name: str, target: str = "") -> Optional[Dict[str, str]]:
        """Read a basic-auth style secret from the canonical secrets module.

        This is cephadm convenience logic: it assumes the secret stores
        {username,password} under data keys (i.e. secret_get returns the data dict).
        """
        try:
            creds = self.secret_get(
                namespace=CEPHADM_NAMESPACE,
                scope=SecretScope.GLOBAL,
                target=target,
                name=secret_name,
            )
        except Exception as e:
            logger.error(f'Failed to get basic auth secrets : {e}')
            return None

        if not isinstance(creds, dict):
            return None

        if 'username' not in creds:
            logger.error(f'Invalid credentials for {secret_name}: username is missing')
            return None

        if 'password' not in creds:
            logger.error(f'Invalid credentials for {secret_name}: password is missing')
            return None

        u = creds['username']
        p = creds['password']
        if not u or not p:
            logger.error(f'Invalid credentials for {secret_name}: username/password is empty')
            return None

        return {'username': str(u), 'password': str(p)}

    def _load_basic_auth_legacy(self, user_key: str, password_key: str) -> Optional[Dict[str, str]]:
        u = self.mgr.get_store(user_key)
        p = self.mgr.get_store(password_key)
        if not u or not p:
            return None
        return {'username': str(u), 'password': str(p)}

    def get_legacy_registry_credentials(self) -> Optional[Dict[str, Any]]:
        """Dual-read registry credentials for upgrade compatibility.

        - Read canonical secret first (via secrets mgr-module).
        - Fall back to legacy cephadm KV key 'registry_credentials'.

        If legacy payload is valid, opportunistically migrate forward
        (without deleting the legacy key).
        """
        try:
            rec = self.secret_get(
                namespace=CEPHADM_NAMESPACE,
                scope=SecretScope.GLOBAL,
                target='',
                name='registry_credentials',
            )
            # Accept either:
            #  - data-only dict (recommended)
            #  - record dict with {"data": {...}} (if older API ever returned that)
            if isinstance(rec, dict):
                if isinstance(rec.get('data'), dict):
                    return rec['data']
                return rec
        except Exception:
            pass

        raw = self.mgr.get_store('registry_credentials')
        if raw is None:
            return None
        try:
            payload = json.loads(str(raw))
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None

        # opportunistic migrate forward (do not delete old key)
        try:
            self.set(
                name='registry_credentials',
                data=payload,
                secret_type='registry_credentials',
                user_made=True,
                editable=True,
            )
        except Exception:
            pass

        return payload
