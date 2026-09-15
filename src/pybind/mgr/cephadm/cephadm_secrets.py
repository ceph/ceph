# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import hashlib
import logging
from typing import Any, Dict, List, Optional, Union

from ceph_secrets_client import CephSecretsClient
from ceph_secrets_types import (
    CephSecretException,
    SecretRef,
    SecretScope,
    parse_secret_uri,
)


CEPHADM_NAMESPACE = 'cephadm'
logger = logging.getLogger(__name__)


class CephadmSecrets:
    """cephadm-facing helper over the dedicated secrets mgr-module.

    - Keeps cephadm code readable
    - Preserves cephadm-specific legacy KV fallback/migration logic
    - Delegates canonical ops to secrets mgr-module via CephSecretsClient
    """

    def __init__(self, mgr: Any) -> None:
        self.mgr = mgr
        self.client = CephSecretsClient(mgr)

        # Cache of computed secret dependencies for specs, keyed by service name.
        # Entry: {"spec_hash": str, "epoch": int, "refs": List[SecretRef], "deps": List[str]}
        self._deps_cache: Dict[str, Dict[str, Any]] = {}

        # TODO: migrate legacy secret_store/v1/* KV entries into this module.

    # ---- canonical secret operations (delegated) ----

    def secret_get_version(
        self,
        namespace: str,
        scope: Union[str, SecretScope],
        target: str,
        name: str,
    ) -> Optional[int]:
        try:
            return self.client.secret_get_version(
                namespace=namespace,
                scope=scope,
                target=target,
                name=name,
            )
        except Exception as e:
            logger.error('Cannot get secret version: %s', e)
            return None

    def set(
        self,
        name: str,
        data: Union[Dict[str, Any], str],
        scope: Optional[Union[str, SecretScope]] = SecretScope.GLOBAL,
        target: Optional[str] = '',
        user_made: bool = True,
        editable: bool = True,
    ) -> Dict[str, Any]:
        # The secrets module stores data as an opaque string; JSON-encode dicts.
        data_str = json.dumps(data, sort_keys=True) if not isinstance(data, str) else data
        return self.client.secret_set(
            namespace=CEPHADM_NAMESPACE,
            scope=scope,
            target=target or '',
            name=name,
            data=data_str,
            user_made=user_made,
            editable=editable,
        )

    def rm(
        self,
        namespace: str,
        scope: Union[str, SecretScope],
        target: str,
        name: str,
    ) -> bool:
        return self.client.secret_rm(
            namespace=namespace,
            scope=scope,
            target=target,
            name=name,
        )

    @staticmethod
    def _has_secret_refs(obj: Any) -> bool:
        """Return True if *obj* contains any secret:/ URI when serialised to JSON.

        Cheap pre-check to avoid remote calls to ceph_secrets when the spec
        has no secret references at all.  If ceph_secrets is disabled, specs
        without secret refs will still apply cleanly.
        """
        try:
            return 'secret:/' in json.dumps(obj)
        except Exception:
            return False

    def find_unresolved_secrets(self, obj: Any) -> Any:
        if not self._has_secret_refs(obj):
            return []
        return self.client.scan_unresolved_refs(obj=obj, namespace=CEPHADM_NAMESPACE)

    def assert_no_unresolved_refs(self, obj: Any) -> None:
        """Raise OrchestratorError if any secret:/ URI remains in *obj* after resolution.

        Called post-resolution in serve.py to enforce that no unresolved secret
        ref survives into the final deployed daemon configuration.
        """
        if not self._has_secret_refs(obj):
            return
        # At this point resolve_object has already run; any remaining secret:/
        # string is a ref that was not substituted.
        remaining = self.find_unresolved_secrets(obj)
        if remaining:
            from orchestrator import OrchestratorError
            uris = sorted(remaining)
            raise OrchestratorError(
                "Unresolved secret refs in final daemon config (secret:/ refs are "
                "only supported in fields rendered into final_config):\n  - "
                + "\n  - ".join(uris)
            )

    def resolve_object(self, obj: Any) -> Any:
        if not self._has_secret_refs(obj):
            return obj
        return self.client.resolve_object(obj=obj)

    # ---- deps tracking ----

    def _stable_json(self, obj: Any) -> str:
        return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=True)

    def _spec_cache_key(self, spec_json: Dict[str, Any]) -> str:
        stype = str(spec_json.get('service_type') or '')
        sid = str(spec_json.get('service_id') or '')
        if stype and sid:
            return f'{stype}.{sid}'
        sname = spec_json.get('service_name')
        if isinstance(sname, str) and sname:
            return sname
        return ''

    def _get_secrets_epoch(self) -> int:
        try:
            return int(self.client.secret_get_epoch(namespace=CEPHADM_NAMESPACE))
        except Exception:
            # If the secrets module is unavailable, force a full refresh on each call.
            return -1

    def _scan_refs(self, obj: Any) -> List[SecretRef]:
        """Return resolved SecretRef objects for all whole-value secret URIs in *obj*.

        scan_refs() returns URI strings; we parse them here so the rest of the
        dep-tracking code works with typed objects throughout.
        """
        if not self._has_secret_refs(obj):
            return []
        uri_strings: List[str] = self.client.scan_refs(obj=obj, namespace=CEPHADM_NAMESPACE)
        refs: List[SecretRef] = []
        for uri in uri_strings:
            try:
                refs.append(parse_secret_uri(uri))
            except CephSecretException:
                logger.warning('Skipping malformed secret URI from scan: %r', uri)
        return refs

    def _get_versions_batch(self, refs: List[SecretRef]) -> Dict[str, Optional[int]]:
        """Batch-fetch versions for a list of SecretRef objects.

        Returns a dict keyed by the canonical URI string of each ref so
        callers can look up results with ref.to_uri().
        """
        if not refs:
            return {}
        uris = [r.to_uri() for r in refs]
        try:
            # secret_get_versions takes List[str] URIs and returns {uri: Optional[int]}.
            return self.client.secret_get_versions(uris=uris)
        except Exception:
            # Fall back to per-secret calls keyed by the same URI strings.
            out: Dict[str, Optional[int]] = {}
            for r in refs:
                try:
                    out[r.to_uri()] = self.client.secret_get_version(
                        namespace=r.namespace,
                        scope=r.scope,
                        target=r.target,
                        name=r.name,
                    )
                except Exception:
                    out[r.to_uri()] = None
            return out

    def deps_for_spec(self, spec: Any) -> List[str]:
        """Compute secret dependencies for a ServiceSpec with caching.

        - Cache refs by spec hash to avoid rescanning JSON every serve loop.
        - Use the secrets epoch to skip per-secret version lookups when nothing changed.
        """
        spec_json = spec.to_json() if hasattr(spec, 'to_json') else spec
        if not isinstance(spec_json, dict):
            spec_json = {}

        if not self._has_secret_refs(spec_json):
            return []

        spec_hash = hashlib.sha256(
            self._stable_json(spec_json).encode('utf-8')
        ).hexdigest()

        cache_key = self._spec_cache_key(spec_json) or spec_hash
        epoch = self._get_secrets_epoch()

        cached = self._deps_cache.get(cache_key)
        if cached and cached.get('spec_hash') == spec_hash and cached.get('epoch') == epoch:
            return list(cached.get('deps') or [])

        # Reuse cached refs if the spec body is unchanged; only rescan on spec change.
        if cached and cached.get('spec_hash') == spec_hash:
            refs: List[SecretRef] = list(cached.get('refs') or [])
        else:
            refs = self._scan_refs(spec_json)

        deps = self._build_deps(refs)
        self._deps_cache[cache_key] = {
            'spec_hash': spec_hash,
            'epoch': epoch,
            'refs': refs,
            'deps': deps,
        }
        return deps

    def deps_for_object(self, obj: Any) -> List[str]:
        """Dep-tracking for callers that pass raw objects rather than ServiceSpecs.

        Not cached because callers may not be stable specs.
        """
        return self._build_deps(self._scan_refs(obj))

    def _build_deps(self, refs: List[SecretRef]) -> List[str]:
        versions = self._get_versions_batch(refs)
        deps: List[str] = []
        for ref in sorted(refs, key=lambda r: (r.namespace, r.scope.value, r.target, r.name)):
            uri = ref.to_uri()
            version = versions.get(uri)
            if version is None:
                logger.error('Secret not found: %s', uri)
            else:
                deps.append(f'{ref.to_uri()}:{version}')
        return deps

    # ---- cephadm-specific credential helpers ----

    def _load_basic_auth_secret(
        self,
        secret_name: str,
        scope: SecretScope = SecretScope.SERVICE,
        target: str = '',
    ) -> Optional[Dict[str, str]]:
        """Read a {username, password} secret from the canonical secrets module.

        Returns None if the secret does not exist (fallback to legacy is allowed).
        Raises ValueError if the secret exists but is corrupt (fail closed -- do
        not silently fall back to default credentials).
        """
        raw = self.client.secret_get_value(
            namespace=CEPHADM_NAMESPACE,
            scope=scope,
            target=target,
            name=secret_name,
        )
        # raw is None  -> secret does not exist; caller may fall back to legacy.
        # RuntimeError -> ceph_secrets module is unavailable; propagate, do not
        #                 silently fall back to default credentials.
        if raw is None:
            return None

        # Secret exists -- any problem from here is corruption, not absence.
        try:
            creds = json.loads(raw)
        except Exception as e:
            raise ValueError(
                f'Secret {secret_name!r} exists but contains invalid JSON: {e}'
            ) from e

        if not isinstance(creds, dict):
            raise ValueError(
                f'Secret {secret_name!r} exists but is not a JSON object'
            )

        u = creds.get('username') or ''
        p = creds.get('password') or ''
        if not u:
            raise ValueError(f'Secret {secret_name!r} is missing or has empty username')
        if not p:
            raise ValueError(f'Secret {secret_name!r} is missing or has empty password')

        return {'username': str(u), 'password': str(p)}

    def _load_basic_auth_legacy(
        self, user_key: str, password_key: str
    ) -> Optional[Dict[str, str]]:
        u = self.mgr.get_store(user_key)
        p = self.mgr.get_store(password_key)
        if not u or not p:
            return None
        return {'username': str(u), 'password': str(p)}

    def get_legacy_registry_credentials(self) -> Optional[Dict[str, Any]]:
        """Dual-read registry credentials for upgrade compatibility.

        Reads from the canonical secrets module first; falls back to the legacy
        cephadm KV key 'registry_credentials'.  If the legacy key holds a valid
        payload it is opportunistically migrated forward (without deleting it).
        """
        try:
            raw = self.client.secret_get_value(
                namespace=CEPHADM_NAMESPACE,
                scope=SecretScope.GLOBAL,
                target='',
                name='registry_credentials',
            )
            if raw is not None:
                payload = json.loads(raw)
                if isinstance(payload, dict):
                    return payload
        except Exception:
            pass

        raw_kv = self.mgr.get_store('registry_credentials')
        if raw_kv is None:
            return None
        try:
            payload = json.loads(str(raw_kv))
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None

        # Opportunistic migrate forward (do not delete the old key).
        try:
            self.set(
                name='registry_credentials',
                data=payload,
                user_made=True,
                editable=True,
            )
        except Exception:
            pass

        return payload
