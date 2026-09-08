# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from ceph_secrets_types import (
    SecretScope,
    SecretRef,
    CephSecretException,
    CephSecretDataError,
    validate_secret_namespace,
)
from .secret_backend import SecretStorageBackend


logger = logging.getLogger(__name__)


# Secret data lives under this prefix:  secret_store/v1/<ns>/<scope>/...
SECRET_STORE_PREFIX = 'secret_store/v1/'

# Storage payload schema version. Stored records are strict: unknown or missing fields are rejected
SECRET_STORE_FORMAT_VERSION = 1

# Per-namespace epoch keys live under a completely separate prefix so they are
# never picked up by the secret data scan in ls().
# Key form:  secret_store/meta/<namespace>/_epoch
SECRET_META_PREFIX = 'secret_store/meta/'


JsonType = Union[
    None,
    bool,
    int,
    float,
    str,
    List["JsonType"],
    Dict[str, "JsonType"],
]

JsonDict = Dict[str, JsonType]

# Secret data is an opaque string. Callers are responsible for any structure
# within it, for example JSON-encoding a dict before storing and decoding after
# retrieval.
SecretData = str


def _checked_namespace(namespace: str) -> str:
    try:
        validate_secret_namespace(namespace)
    except ValueError as e:
        raise CephSecretDataError(str(e)) from e
    return namespace


def _epoch_key(namespace: str) -> str:
    namespace = _checked_namespace(namespace)
    return f'{SECRET_META_PREFIX}{namespace}/_epoch'


def _checked_ref(
    namespace: str,
    scope: Union[SecretScope, str],
    target: str,
    name: str,
) -> SecretRef:
    try:
        if not isinstance(scope, SecretScope):
            scope = SecretScope.from_str(str(scope))
        return SecretRef(
            namespace=namespace,
            scope=scope,
            target=target or '',
            name=name,
        )
    except (CephSecretException, ValueError) as e:
        raise CephSecretDataError(str(e)) from e


def _now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace('+00:00', 'Z')
    )


def _expect_object(label: str, value: Any) -> JsonDict:
    if not isinstance(value, dict):
        raise CephSecretDataError(
            f'{label} must be a JSON object (got {type(value).__name__})'
        )
    return value


def _reject_unknown_keys(label: str, payload: JsonDict, allowed: set[str]) -> None:
    unknown = set(payload) - allowed
    if unknown:
        raise CephSecretDataError(
            f'{label} contains unknown field(s): {", ".join(sorted(unknown))}'
        )


def _require_keys(label: str, payload: JsonDict, required: set[str]) -> None:
    missing = required - set(payload)
    if missing:
        raise CephSecretDataError(
            f'{label} is missing required field(s): {", ".join(sorted(missing))}'
        )


def _expect_bool(label: str, value: Any) -> bool:
    if not isinstance(value, bool):
        raise CephSecretDataError(f'{label} must be a boolean')
    return value


def _expect_str(label: str, value: Any) -> str:
    if not isinstance(value, str):
        raise CephSecretDataError(f'{label} must be a string')
    return value


def _expect_positive_int(label: str, value: Any) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise CephSecretDataError(f'{label} must be a positive integer')
    return value


def _ref_json(ref: SecretRef) -> JsonDict:
    return {
        'namespace': ref.namespace,
        'scope': ref.scope.value,
        'target': ref.target,
        'name': ref.name,
    }


@dataclass(frozen=True)
class SecretMetadata:
    version: int = 1
    created: str = ''
    updated: str = ''

    def __post_init__(self) -> None:
        _expect_positive_int('SecretMetadata.version', self.version)
        _expect_str('SecretMetadata.created', self.created)
        _expect_str('SecretMetadata.updated', self.updated)

    def to_json(self) -> JsonDict:
        return {
            'version': self.version,
            'created': self.created,
            'updated': self.updated,
        }

    @staticmethod
    def from_json(payload: JsonDict) -> 'SecretMetadata':
        payload = _expect_object('SecretMetadata', payload)
        allowed = {'version', 'created', 'updated'}
        _reject_unknown_keys('SecretMetadata', payload, allowed)
        _require_keys('SecretMetadata', payload, allowed)
        return SecretMetadata(
            version=_expect_positive_int('SecretMetadata.version', payload['version']),
            created=_expect_str('SecretMetadata.created', payload['created']),
            updated=_expect_str('SecretMetadata.updated', payload['updated']),
        )


@dataclass(frozen=True)
class SecretPolicy:
    user_made: bool = True
    editable: bool = True

    def __post_init__(self) -> None:
        _expect_bool('SecretPolicy.user_made', self.user_made)
        _expect_bool('SecretPolicy.editable', self.editable)

    def to_json(self) -> JsonDict:
        return {
            'user_made': self.user_made,
            'editable': self.editable,
        }

    @staticmethod
    def from_json(payload: JsonDict) -> 'SecretPolicy':
        payload = _expect_object('SecretPolicy', payload)
        allowed = {'user_made', 'editable'}
        _reject_unknown_keys('SecretPolicy', payload, allowed)
        _require_keys('SecretPolicy', payload, allowed)
        return SecretPolicy(
            user_made=_expect_bool('SecretPolicy.user_made', payload['user_made']),
            editable=_expect_bool('SecretPolicy.editable', payload['editable']),
        )


@dataclass
class SecretRecord:
    ref: SecretRef
    data: SecretData
    metadata: SecretMetadata = field(default_factory=SecretMetadata)
    policy: SecretPolicy = field(default_factory=SecretPolicy)

    def __post_init__(self) -> None:
        if not isinstance(self.ref, SecretRef):
            raise CephSecretDataError('SecretRecord.ref must be a SecretRef')
        if not isinstance(self.metadata, SecretMetadata):
            raise CephSecretDataError('SecretRecord.metadata must be SecretMetadata')
        if not isinstance(self.policy, SecretPolicy):
            raise CephSecretDataError('SecretRecord.policy must be SecretPolicy')
        if not isinstance(self.data, str):
            raise CephSecretDataError('SecretRecord.data must be a string')
        if self.data == '':
            raise CephSecretDataError('SecretRecord.data must not be empty')

    # Compatibility/readability helpers for key construction, sorting, and callers
    # that need the identity but should not mutate it directly.
    @property
    def namespace(self) -> str:
        return self.ref.namespace

    @property
    def scope(self) -> SecretScope:
        return self.ref.scope

    @property
    def target(self) -> str:
        return self.ref.target

    @property
    def name(self) -> str:
        return self.ref.name

    def ident(self) -> Tuple[str, str, str, str]:
        return self.ref.ident()

    def to_public_json(
        self,
        include_data: bool = False,
        include_policy: bool = False,
        include_ref: bool = False,
    ) -> JsonDict:
        result: JsonDict = {
            'metadata': self.metadata.to_json(),
        }
        if include_ref:
            result['ref'] = _ref_json(self.ref)
        if include_data:
            result['data'] = self.data
        if include_policy:
            result['policy'] = self.policy.to_json()
        return result

    def to_store_json(self) -> JsonDict:
        return {
            'format_version': SECRET_STORE_FORMAT_VERSION,
            'metadata': self.metadata.to_json(),
            'policy': self.policy.to_json(),
            'data': self.data,
        }

    @staticmethod
    def from_store_json(ref: SecretRef, payload: JsonDict) -> 'SecretRecord':
        payload = _expect_object('SecretRecord', payload)
        allowed = {'format_version', 'metadata', 'data', 'policy'}
        _reject_unknown_keys('SecretRecord', payload, allowed)
        _require_keys('SecretRecord', payload, allowed)

        format_version = payload['format_version']
        if (
            not isinstance(format_version, int)
            or isinstance(format_version, bool)
            or format_version != SECRET_STORE_FORMAT_VERSION
        ):
            raise CephSecretDataError(
                f'unsupported SecretRecord.format_version: {format_version!r}'
            )

        metadata = _expect_object('SecretRecord.metadata', payload['metadata'])
        data = _expect_str('SecretRecord.data', payload['data'])
        policy = _expect_object('SecretRecord.policy', payload['policy'])
        return SecretRecord(
            ref=ref,
            metadata=SecretMetadata.from_json(metadata),
            data=data,
            policy=SecretPolicy.from_json(policy),
        )


class SecretStoreMon(SecretStorageBackend):
    """
    Mon KV-store backed secret store.

    Secret data keys:
      secret_store/v1/<namespace>/<scope>/<target>/<secret_name>

    Stored payload shape:
      {
        "format_version": 1,
        "metadata": {"version": 1, "created": "...", "updated": "..."},
        "policy": {"user_made": true, "editable": true}
        "data": "<opaque string>",
      }

    Per-namespace epoch keys (never touched by ls()):
      secret_store/meta/<namespace>/_epoch
    """

    def __init__(self, mgr: Any) -> None:
        self.mgr = mgr

    # ------------------------------------------------------------------ epoch

    def get_epoch(self, namespace: str) -> int:
        """Return the current epoch for *namespace* (0 if never set)."""
        raw = self.mgr.get_store(_epoch_key(namespace))
        if raw is None:
            return 0
        try:
            return int(str(raw))
        except Exception:
            return 0

    def bump_epoch(self, namespace: str) -> int:
        """Increment and persist the epoch for *namespace*; return the new value.

        Best-effort: the mon KV store has no atomic read-modify-write, so there
        is a theoretical TOCTOU window under concurrent mutations.  This is
        acceptable because the epoch is used only as a cheap change-detector,
        not as a consistency guarantee.
        """
        epoch = self.get_epoch(namespace) + 1
        self.mgr.set_store(_epoch_key(namespace), str(epoch))
        return epoch

    # ------------------------------------------------------------------ helpers

    def _kv_key_from_ref(self, ref: SecretRef) -> str:
        if ref.scope == SecretScope.CUSTOM:
            return f'{SECRET_STORE_PREFIX}{ref.namespace}/{ref.scope.value}/{ref.name}'

        if ref.scope == SecretScope.GLOBAL:
            return f'{SECRET_STORE_PREFIX}{ref.namespace}/{ref.scope.value}/{ref.name}'

        return (
            f'{SECRET_STORE_PREFIX}{ref.namespace}/{ref.scope.value}/'
            f'{ref.target}/{ref.name}'
        )

    # ------------------------------------------------------------------ CRUD

    def get(self, namespace: str, scope: SecretScope, target: str, name: str) -> Optional[SecretRecord]:
        ref = _checked_ref(namespace, scope, target, name)
        k = self._kv_key_from_ref(ref)
        raw = self.mgr.get_store(k)
        if raw is None:
            return None
        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise CephSecretDataError(
                    f'expected a JSON object, got {type(payload).__name__}'
                )
            return SecretRecord.from_store_json(ref, payload)
        except CephSecretDataError:
            raise
        except Exception as e:
            msg = f"Corrupted secret entry at {k}: {e}"
            logger.warning(msg)
            raise CephSecretDataError(msg) from e

    def set(self,
            namespace: str,
            scope: SecretScope,
            target: str,
            name: str,
            data: SecretData,
            user_made: bool = True,
            editable: bool = True) -> SecretRecord:

        if not isinstance(data, str):
            raise CephSecretException('Secret data must be a string')
        if data == '':
            raise CephSecretException('Secret data must not be empty')

        ref = _checked_ref(namespace, scope, target, name)
        existing = None
        try:
            existing = self.get(ref.namespace, ref.scope, ref.target, ref.name)
            if existing and not existing.policy.editable:
                raise CephSecretException(f'Secret {ref.to_uri()} is not editable')
        except CephSecretDataError as e:
            raise CephSecretException(f'Secret {ref.to_uri()} is corrupted.') from e

        now = _now_iso()
        if existing:
            metadata = SecretMetadata(
                version=existing.metadata.version + 1,
                # Preserve the original creation timestamp across updates.
                created=existing.metadata.created or now,
                updated=now,
            )
        else:
            metadata = SecretMetadata(version=1, created=now, updated=now)

        rec = SecretRecord(
            ref=ref,
            metadata=metadata,
            policy=SecretPolicy(user_made=user_made, editable=editable),
            data=data,
        )
        k = self._kv_key_from_ref(ref)
        self.mgr.set_store(k, json.dumps(rec.to_store_json(), sort_keys=True))

        # Bump epoch after a successful write so consumers can detect any change
        # in this namespace without enumerating all secrets.
        self.bump_epoch(ref.namespace)
        return rec

    def rm(
        self,
        namespace: str,
        scope: SecretScope,
        target: str,
        name: str,
    ) -> bool:
        ref = _checked_ref(namespace, scope, target, name)
        k = self._kv_key_from_ref(ref)
        existed = self.mgr.get_store(k) is not None
        self.mgr.set_store(k, None)
        if existed:
            self.bump_epoch(ref.namespace)
        return existed

    def ls(
        self,
        namespace: Optional[str] = None,
        scope: Optional[SecretScope] = None,
        target: Optional[str] = None,
    ) -> List[SecretRecord]:
        """
        List secrets matching the given filters.

        Corrupt/malformed module-owned entries are data errors, not part of the
        normal listing contract, so they are raised as CephSecretDataError.
        """
        if namespace is not None:
            _checked_namespace(namespace)
        if scope is not None and not isinstance(scope, SecretScope):
            scope = SecretScope.from_str(str(scope))

        # list by prefix for efficiency
        prefix = SECRET_STORE_PREFIX
        if namespace:
            prefix += f'{namespace}/'
            if scope:
                prefix += f'{scope.value}/'
                if target and scope not in (SecretScope.GLOBAL, SecretScope.CUSTOM):
                    prefix += f'{target}/'
        items = self.mgr.get_store_prefix(prefix) or {}
        records: List[SecretRecord] = []

        for k, v in items.items():
            # k is full key: secret_store/v1/ns/scope/target/name
            suffix = k[len(SECRET_STORE_PREFIX):]
            parts = suffix.split('/')

            # reject keys with empty segments (double slash, trailing slash, etc.)
            if '' in parts:
                raise CephSecretDataError(f'{k}: empty path component in key')

            if len(parts) < 3:
                raise CephSecretDataError(f'{k}: unexpected key structure')

            ns, sc = parts[0], parts[1]

            try:
                sc_enum = SecretScope.from_str(sc)
            except Exception as e:
                raise CephSecretDataError(f'{k}: invalid scope {sc!r}: {e}') from e

            if sc_enum == SecretScope.CUSTOM:
                tgt = ''
                name = '/'.join(parts[2:])
            elif sc_enum == SecretScope.GLOBAL:
                if len(parts) != 3:
                    raise CephSecretDataError(f'{k}: unexpected global key structure')
                tgt = ''
                name = parts[2]
            else:
                if len(parts) != 4:
                    raise CephSecretDataError(
                        f'{k}: unexpected targeted-scope key structure'
                    )
                tgt = parts[2]
                name = parts[3]

            try:
                ref = SecretRef(ns, sc_enum, tgt, name)
            except ValueError as e:
                raise CephSecretDataError(f'{k}: invalid secret key: {e}') from e

            # apply caller filters
            if namespace and ref.namespace != namespace:
                continue
            if scope and ref.scope != scope:
                continue
            if target and ref.target != target:
                continue

            # parse payload
            try:
                payload = json.loads(v)
            except Exception as e:
                raise CephSecretDataError(f'{k}: invalid JSON payload: {e}') from e

            if not isinstance(payload, dict):
                raise CephSecretDataError(
                    f'{k}: payload is not a JSON object (got {type(payload).__name__})'
                )

            try:
                records.append(SecretRecord.from_store_json(ref, payload))
            except CephSecretDataError as e:
                raise CephSecretDataError(f'{k}: {e}') from e
            except Exception as e:
                raise CephSecretDataError(f'{k}: corrupted secret record: {e}') from e

        records.sort(key=lambda r: (r.ref.namespace, r.ref.scope.value, r.ref.target, r.ref.name))
        return records
