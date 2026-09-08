# -*- coding: utf-8 -*-
import logging
from typing import Any, List, Optional, Set, Union, Hashable, Protocol

from .secret_store import SecretRecord, SecretData
from ceph_secrets_types import (
    CephSecretException,
    CephSecretNotFoundError,
    SecretRef,
    BadSecretURI,
    SecretScope,
    parse_secret_uri,
    SECRET_SCHEME
)


_SECRET_URI_PREFIX = f'{SECRET_SCHEME}:/'


logger = logging.getLogger(__name__)


class SecretURI(Protocol, Hashable):
    def to_uri(self) -> str:
        ...


def _coerce_scope(scope: Union[SecretScope, str]) -> SecretScope:
    if isinstance(scope, SecretScope):
        return scope
    return SecretScope.from_str(str(scope))


class SecretMgr:
    """
    Phase 1: Mon-store backend only.

    Secret data is an opaque string.  Callers are responsible for any
    structure within it (e.g. JSON-encoding a dict before storing and
    decoding after retrieval).  resolve_object() substitutes a secret URI
    with the stored string directly.
    """

    def __init__(self, store: Any) -> None:
        self.store = store

    def make_ref(
        self,
        namespace: str,
        scope: Union[SecretScope, str],
        target: str = '',
        name: str = '',
    ) -> SecretRef:
        try:
            return SecretRef(
                namespace=namespace,
                scope=_coerce_scope(scope),
                target=target or '',
                name=name,
            )
        except ValueError as e:
            raise CephSecretException(str(e)) from e

    def get(self, ref: SecretRef) -> SecretRecord:
        rec = self.store.get(ref.namespace, ref.scope, ref.target, ref.name)
        if rec is None:
            raise CephSecretNotFoundError(f"Secret not found: {ref.to_uri()}")
        return rec

    def get_value(self, ref: SecretRef) -> str:
        """Return the opaque data string for a secret."""
        rec = self.get(ref)
        return rec.data

    def set(
        self,
        namespace: str,
        scope: Union[SecretScope, str],
        target: str,
        name: str,
        data: SecretData,
        user_made: bool = True,
        editable: bool = True,
    ) -> SecretRecord:
        if not isinstance(data, str):
            raise CephSecretException('Secret data must be a string')
        if data == '':
            raise CephSecretException('Secret data must not be empty')

        ref = self.make_ref(namespace, scope, target, name)
        return self.store.set(
            ref.namespace,
            ref.scope,
            ref.target,
            ref.name,
            data,
            user_made,
            editable,
        )

    def rm(
        self,
        namespace: str,
        scope: Union[SecretScope, str],
        target: str,
        name: str,
    ) -> bool:
        ref = self.make_ref(namespace, scope, target, name)
        return self.store.rm(ref.namespace, ref.scope, ref.target, ref.name)

    def ls(
        self,
        namespace: Optional[str] = None,
        scope: Optional[Union[SecretScope, str]] = None,
        target: Optional[str] = None,
    ) -> List[SecretRecord]:
        sc = _coerce_scope(scope) if scope else None
        return self.store.ls(namespace=namespace, scope=sc, target=target)

    def scan_unresolved_refs(self, obj: Any, namespace: str) -> Set[SecretURI]:
        """
        Return secret refs found in `obj` that cannot be fetched.
        """
        unresolved: Set[SecretURI] = set()
        for ref in self.scan_refs(obj, namespace):
            if isinstance(ref, SecretRef):
                try:
                    self.get_value(ref)
                except CephSecretException:
                    unresolved.add(ref)
            else:
                unresolved.add(ref)
        return unresolved

    def scan_refs(self, obj: Any, namespace: str) -> Set[SecretURI]:
        """Collect secret references from *obj*.

        A reference must be the *entire* (stripped) value of a string field —
        e.g. ``"secret:/ns/global/pw"``.  Embedded URIs inside a larger string
        (e.g. ``"Bearer secret:/..."``) are not supported and are surfaced as
        a BadSecretURI rather than silently ignored, so validation can flag
        them before deploy.
        """
        refs: Set[SecretURI] = set()

        def _scan(v: Any) -> None:
            if isinstance(v, dict):
                for vv in v.values():
                    _scan(vv)
            elif isinstance(v, (list, tuple)):
                for vv in v:
                    _scan(vv)
            elif isinstance(v, str):
                s = v.strip()
                if s.startswith(_SECRET_URI_PREFIX):
                    try:
                        refs.add(parse_secret_uri(s))
                    except Exception as e:
                        logger.warning("Failed to parse secret uri %r: %s", s, e)
                        refs.add(BadSecretURI(raw=s, namespace=namespace, error=str(e)))
                elif _SECRET_URI_PREFIX in s:
                    # Contains a secret URI but is not a whole-value reference.
                    err = ('embedded secret URIs are not supported; the entire '
                           'field value must be the secret URI')
                    logger.warning("Rejecting embedded secret uri in %r", v)
                    refs.add(BadSecretURI(raw=v, namespace=namespace, error=err))
                # otherwise: plain data, not a reference

        _scan(obj)
        return refs

    def _resolve_secret_uri(self, uri: str) -> str:
        """Resolve a single secret URI to its opaque data string."""
        try:
            parsed_secret = parse_secret_uri(uri)
        except CephSecretException as e:
            raise CephSecretException(f"Invalid secret URI {uri!r}: {e}") from e
        if not isinstance(parsed_secret, SecretRef):
            raise CephSecretException(f"Invalid secret URI {uri!r}")
        return self.get_value(parsed_secret)

    def _resolve(self, v: Any) -> Any:
        """Recursively resolve secret URIs within a nested structure."""
        if isinstance(v, dict):
            return {k: self._resolve(vv) for k, vv in v.items()}
        if isinstance(v, list):
            return [self._resolve(vv) for vv in v]
        if isinstance(v, tuple):
            return tuple(self._resolve(vv) for vv in v)
        if isinstance(v, str):
            s = v.strip()
            if s.startswith(_SECRET_URI_PREFIX):
                return self._resolve_secret_uri(s)
            if _SECRET_URI_PREFIX in s:
                # A field that embeds a secret URI inside other text would be
                # deployed verbatim (leaking the unresolved placeholder).  Fail
                # loudly rather than silently passing it through.
                raise CephSecretException(
                    f"Invalid secret reference {v!r}: embedded secret URIs are "
                    f"not supported; the entire field value must be the secret URI"
                )
        return v

    def resolve_object(self, obj: Any) -> Any:
        """Resolve secret references within nested dict/list/tuple structures.

        A string whose entire (stripped) value is a secret URI is replaced by
        the stored opaque data string for that secret.  Surrounding whitespace
        around an otherwise-clean URI is tolerated; non-secret strings are
        returned unchanged, preserving their original whitespace.

        A string that embeds a secret URI inside other text (e.g.
        ``"Bearer secret:/..."``) is rejected with CephSecretException, since
        partial substitution is not supported and silently emitting the literal
        URI would leak an unresolved placeholder into deployed configuration.
        """
        return self._resolve(obj)
