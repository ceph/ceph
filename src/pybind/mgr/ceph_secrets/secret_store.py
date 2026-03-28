# -*- coding: utf-8 -*-
from collections import OrderedDict
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ceph_secrets_types import SecretScope, CephSecretException, CephSecretDataError
from .secret_backend import SecretStorageBackend
import logging


logger = logging.getLogger(__name__)


# Secret data lives under this prefix:  secret_store/v1/<ns>/<scope>/...
SECRET_STORE_PREFIX = 'secret_store/v1/'

# Per-namespace epoch keys live under a completely separate prefix so they are
# never picked up by the secret data scan in ls().
# Key form:  secret_store/meta/<namespace>/_epoch
SECRET_META_PREFIX = 'secret_store/meta/'


def _epoch_key(namespace: str) -> str:
    return f'{SECRET_META_PREFIX}{namespace}/_epoch'


def _parse_ts(v: object) -> str:
    # Accept both legacy float epoch and ISO strings
    if v is None:
        return ''
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(float(v), tz=timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
    if isinstance(v, str):
        # basic validation; keep as-is if it looks like ISO
        return v
    logger.warning('Invalid timestamp type: %s', type(v).__name__)
    return ''


@dataclass(frozen=True)
class BadSecretRecord:
    raw_key: str    # full KV key: 'secret_store/v1/cephadm/global/broken'
    error: str      # human-readable reason: 'invalid JSON', 'corrupted fields', etc.
    namespace: str  # always extractable from the key, useful for filtering


@dataclass
class SecretRecord:
    namespace: str
    scope: SecretScope
    target: str
    name: str
    version: int
    secret_type: str
    data: Dict[str, Any]
    user_made: bool = True
    editable: bool = True
    created: str = ''
    updated: str = ''

    def ident(self) -> Tuple[str, str, str, str]:
        return (self.namespace, self.scope.value, self.target, self.name)

    def to_json(self, include_data: bool = True, include_internal: bool = False) -> Dict[str, Any]:
        d = OrderedDict([
            ('version', self.version),
            ('type', self.secret_type),
            ('created', self.created),
            ('updated', self.updated),
        ])
        if include_data:
            d['data'] = self.data
        else:
            # metadata-only: expose keys but not values
            d['keys'] = sorted(list(self.data.keys()))

        if include_internal:
            d['user_made'] = self.user_made
            d['editable'] = self.editable

        return d

    @staticmethod
    def from_json(namespace: str, scope: SecretScope, target: str, name: str, payload: Dict[str, Any]) -> 'SecretRecord':
        version = int(payload.get('version', 1))
        secret_type = str(payload.get('type', 'Opaque'))
        user_made = bool(payload.get('user_made', True))
        editable = bool(payload.get('editable', True))
        created = _parse_ts(payload.get('created', ''))
        updated = _parse_ts(payload.get('updated', ''))
        data = payload.get('data', {})
        if not isinstance(data, dict):
            raise CephSecretDataError('SecretRecord.data must be a JSON object')
        return SecretRecord(namespace, scope, target, name, version, secret_type, data, user_made, editable, created, updated)


class SecretStoreMon(SecretStorageBackend, backend_name='mon'):
    """
    Mon KV-store backed secret store.

    Secret data keys:
      secret_store/v1/<namespace>/<scope>/<target>/<secret_name>

    Per-namespace epoch keys (never touched by ls()):
      secret_store/meta/<namespace>/_epoch
    """

    def __init__(self, mgr: Any):
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

    def _kv_key(self, namespace: str, scope: SecretScope, target: str, name: str) -> str:
        # Avoid '/' in components for v1 simplicity
        for label, val in (('namespace', namespace), ('name', name)):
            if '/' in val:
                raise CephSecretDataError(f"{label} must not contain '/': {val!r}")
        if scope != SecretScope.GLOBAL:
            if '/' in target:
                raise CephSecretDataError(f"target must not contain '/': {target!r}")
            return f'{SECRET_STORE_PREFIX}{namespace}/{scope.value}/{target}/{name}'
        elif target:
            raise CephSecretDataError("target must be empty for global scope")

        return f'{SECRET_STORE_PREFIX}{namespace}/{scope.value}/{name}'

    # ------------------------------------------------------------------ CRUD

    def get(self, namespace: str, scope: SecretScope, target: str, name: str) -> Optional[SecretRecord]:
        k = self._kv_key(namespace, scope, target, name)
        raw = self.mgr.get_store(k)
        if raw is None:
            return None
        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise CephSecretDataError(f'expected a JSON object, got {type(payload).__name__}')
            return SecretRecord.from_json(namespace, scope, target, name, payload)
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
            data: Dict[str, Any],
            secret_type: str = 'Opaque',
            user_made: bool = True,
            editable: bool = True) -> SecretRecord:

        existing = None
        try:
            existing = self.get(namespace, scope, target, name)
            if existing and not existing.editable:
                raise CephSecretException(f'Secret {namespace}/{scope}/{target}/{name} is not editable')
        except CephSecretDataError as e:
            raise CephSecretException(f'Secret {namespace}/{scope}/{target}/{name} is corrupted.') from e

        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')
        if existing:
            version = existing.version + 1
            created = existing.created or now
        else:
            version = 1
            created = now
        rec = SecretRecord(
            namespace=namespace,
            scope=scope,
            target=target,
            name=name,
            version=version,
            secret_type=secret_type,
            data=data,
            user_made=user_made,
            editable=editable,
            created=created,
            updated=now,
        )
        k = self._kv_key(namespace, scope, target, name)
        v = rec.to_json(include_data=True, include_internal=True)
        self.mgr.set_store(k, json.dumps(v))
        self.bump_epoch(namespace)
        return rec

    def rm(self, namespace: str, scope: SecretScope, target: str, name: str) -> bool:
        k = self._kv_key(namespace, scope, target, name)
        existed = self.mgr.get_store(k) is not None
        self.mgr.set_store(k, None)
        if existed:
            self.bump_epoch(namespace)
        return existed

    def ls(
        self,
        namespace: Optional[str] = None,
        scope: Optional[SecretScope] = None,
        target: Optional[str] = None,
    ) -> Tuple[List[SecretRecord], List[BadSecretRecord]]:
        """
        List secrets matching the given filters.

        Returns:
          (good_records, bad_records)

        - good_records: parsed SecretRecord entries.
        - bad_records: entries that exist in the KV store but are malformed (bad key structure,
          unknown scope, wrong segment count for scope, or corrupted JSON payload).

        Notes:
          - If namespace/scope/target filters are provided, bad records are still reported
            only if they pass the same filters based on key-derived components.
          - Empty-path-component keys (e.g. double slashes) are reported as bad records.
          - Epoch keys live under secret_store/meta/ and are never scanned here.
        """
        # list by prefix for efficiency
        prefix = SECRET_STORE_PREFIX
        if namespace:
            prefix += f'{namespace}/'
            if scope:
                prefix += f'{scope.value}/'
                if target and scope != SecretScope.GLOBAL:
                    prefix += f'{target}/'
        items = self.mgr.get_store_prefix(prefix) or {}
        good_records: List[SecretRecord] = []
        bad_records: List[BadSecretRecord] = []

        for k, v in items.items():
            # k is full key: secret_store/v1/ns/scope/target/name
            suffix = k[len(SECRET_STORE_PREFIX):]
            parts = suffix.split('/')

            # reject keys with empty segments (double slash, trailing slash, etc.)
            if '' in parts:
                bad_records.append(BadSecretRecord(k, 'empty path component in key', parts[0] if parts else ''))
                continue

            if len(parts) == 3:
                ns, sc, name = parts[0], parts[1], parts[2]
                tgt = ''
            elif len(parts) == 4:
                ns, sc, tgt, name = parts[0], parts[1], parts[2], parts[3]
            else:
                bad_records.append(BadSecretRecord(k, 'unexpected key structure', parts[0]))
                continue

            try:
                sc_enum = SecretScope.from_str(sc)
            except Exception as e:
                bad_records.append(BadSecretRecord(k, f'invalid scope {sc!r}: {e}', ns))
                continue

            # cross-validate segment count against scope
            if sc_enum == SecretScope.GLOBAL and tgt:
                bad_records.append(BadSecretRecord(k, '4-part key but scope is global', ns))
                continue
            if sc_enum != SecretScope.GLOBAL and not tgt:
                bad_records.append(BadSecretRecord(k, '3-part key but scope is not global', ns))
                continue

            # apply caller filters
            if namespace and ns != namespace:
                continue
            if scope and sc_enum != scope:
                continue
            if target and tgt != target:
                continue

            # parse payload
            try:
                payload = json.loads(v)
                if not isinstance(payload, dict):
                    bad_records.append(BadSecretRecord(k, f'payload is not a JSON object (got {type(payload).__name__})', ns))
                    continue
                good_records.append(SecretRecord.from_json(ns, sc_enum, tgt, name, payload))
            except Exception as e:
                bad_records.append(BadSecretRecord(k, str(e), ns))

        good_records.sort(key=lambda r: (r.namespace, r.scope.value, r.target, r.name))
        bad_records.sort(key=lambda r: (r.namespace, r.raw_key))
        return good_records, bad_records
