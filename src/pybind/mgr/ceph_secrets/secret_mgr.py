# -*- coding: utf-8 -*-
import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from .secret_store import SecretRecord, BadSecretRecord
from ceph_secrets_types import (CephSecretException,
                                SecretURI,
                                SecretRef,
                                BadSecretURI,
                                SecretScope,
                                parse_secret_uri,
                                SECRET_URI_SCHEME)


_SECRET_URI_RE = re.compile(rf"{re.escape(SECRET_URI_SCHEME)}[^\s\"']*")


logger = logging.getLogger(__name__)


class SecretMgr:
    """
    Phase 1: Mon-store backend only.

    Resolution rule (less verbose for simple secrets):
      - If a secret URI does not specify a key:
          * If secret.data has exactly 1 key -> return that single value.
          * Otherwise -> return the dict as-is (and callers that require a string
            must reject/require ?key=...).
    """

    def __init__(self, store: Any):
        self.store = store

    def make_ref(
        self,
        namespace: str,
        scope: SecretScope,
        target: str = '',
        name: str = '',
        key: Optional[str] = None,
    ) -> SecretRef:
        return SecretRef(namespace=namespace, scope=scope, target=target, name=name, key=key)

    def get(self, ref: SecretRef) -> SecretRecord:
        rec = self.store.get(ref.namespace, ref.scope, ref.target, ref.name)
        if rec is None:
            raise CephSecretException(f"Secret not found: {ref.to_uri()}")
        return rec

    def get_value(self, ref: SecretRef) -> Any:

        rec = self.get(ref)

        # Explicit key selection
        if ref.key:
            if ref.key not in rec.data:
                raise CephSecretException(f"Secret key '{ref.key}' not present in {ref.to_uri()}")
            return rec.data[ref.key]

        # No key provided: if exactly one entry, return the single value
        if len(rec.data) == 1:
            return next(iter(rec.data.values()))

        return rec.data

    def set(
        self,
        name: str,
        data: Dict[str, Any],
        namespace: str,
        scope: Optional[SecretScope] = None,
        target: Optional[str] = None,
        secret_type: str = "Opaque",
        user_made: bool = True,
        editable: bool = True,
    ) -> SecretRecord:
        sc = scope or SecretScope.GLOBAL
        tgt = target or ""
        if sc != SecretScope.GLOBAL and not tgt:
            raise CephSecretException("target is required")
        if sc == SecretScope.GLOBAL and tgt:
            raise CephSecretException("target must be empty for global scope")
        return self.store.set(namespace, sc, tgt, name, data, secret_type, user_made, editable)

    def rm(self, namespace: str, scope: SecretScope, target: str, name: str) -> bool:
        return self.store.rm(namespace, scope, target, name)

    def ls(
        self,
        namespace: Optional[str] = None,
        scope: Optional[SecretScope] = None,
        target: Optional[str] = None,
    ) -> Tuple[List[SecretRecord], List[BadSecretRecord]]:
        return self.store.ls(namespace=namespace, scope=scope, target=target)

    def scan_unresolved_refs(self, obj: Any, namespace: str) -> Set[SecretURI]:
        """
        Return secret refs found in `obj` that cannot be fetched (i.e. self.get(ref) fails).

        Note: this only checks the secret record exists. It does NOT validate ref.key.
        """
        unresolved: Set[SecretURI] = set()
        for ref in self.scan_refs(obj, namespace):
            if isinstance(ref, SecretRef):
                try:
                    self.get(ref)
                except CephSecretException:
                    unresolved.add(ref)
            else:
                unresolved.add(ref)
        return unresolved

    def scan_refs(self, obj: Any, namespace: str) -> Set[SecretURI]:
        refs: Set[SecretURI] = set()

        def _scan(v: Any) -> None:
            if isinstance(v, dict):
                for vv in v.values():
                    _scan(vv)
            elif isinstance(v, (list, tuple)):
                for vv in v:
                    _scan(vv)
            elif isinstance(v, str) and SECRET_URI_SCHEME in v:
                for m in _SECRET_URI_RE.finditer(v):
                    uri = m.group(0)
                    try:
                        refs.add(parse_secret_uri(uri))
                    except Exception as e:
                        logger.warning("Failed to parse secret uri %r: %s", uri, e)
                        refs.add(BadSecretURI(raw=uri, namespace=namespace, error=str(e)))

        _scan(obj)
        return refs

    def resolve_object(self, obj: Any) -> Any:
        """
        Resolve secret references within nested dict/list structures.

        - If a string is exactly a secret URI, replace it with the referenced value.
          (No-key URIs will resolve to a scalar if secret.data has exactly one key.)
        - If a string contains embedded secret URIs, replace each URI by its *string* value.
          If a referenced secret resolves to a dict (ambiguous), require ?key=... instead.
        """

        def get_secret_value(uri: str) -> Any:
            try:
                parsed_secret = parse_secret_uri(uri)
            except CephSecretException as e:
                raise CephSecretException(f"Invalid secret URI {uri!r}: {e}") from e
            if not isinstance(parsed_secret, SecretRef):
                raise CephSecretException(f"Invalid secret URI {uri!r}")
            return self.get_value(parsed_secret)

        def _resolve_str(s: str) -> Any:
            s_strip = s.strip()

            # exact URI -> return value (can be scalar or dict depending on rule)
            if s_strip.startswith(SECRET_URI_SCHEME) and _SECRET_URI_RE.fullmatch(s_strip):
                return get_secret_value(s_strip)

            # embedded URIs -> must be string substitutions
            if SECRET_URI_SCHEME in s:
                def repl(m: re.Match) -> str:
                    uri = m.group(0)
                    val = get_secret_value(uri)
                    if not isinstance(val, str):
                        raise CephSecretException(
                            f"Secret {uri} resolved to non-string; cannot embed into string. "
                            f"Use ?key=... to select a single value."
                        )
                    return val
                return _SECRET_URI_RE.sub(repl, s)

            return s

        def _resolve(v: Any) -> Any:
            if isinstance(v, dict):
                return {k: _resolve(vv) for k, vv in v.items()}
            if isinstance(v, list):
                return [_resolve(vv) for vv in v]
            if isinstance(v, tuple):
                return tuple(_resolve(vv) for vv in v)
            if isinstance(v, str):
                return _resolve_str(v)
            return v

        return _resolve(obj)
