from dataclasses import dataclass, field
import json
import logging
from typing import Any, Dict, List, Optional

from cephadm.tlsobject_types import TLSObjectException, TLSObjectManager, TLSObjectScope


logger = logging.getLogger(__name__)


VAULT_CERT_METADATA_STORE_PREFIX = 'cert_store.vault_metadata.'


def _optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _string_list(value: Any, field_name: str) -> List[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise TLSObjectException(f'Expected {field_name} in Vault certificate metadata to be a list')
    values: List[str] = []
    for item in value:
        item_str = _optional_str(item)
        if item_str:
            values.append(item_str)
    return values


@dataclass(frozen=True)
class VaultCertificateMetadata:
    """Metadata needed to renew a Vault-managed certificate.

    The TLS object store contains the current certificate/key PEM material. This
    metadata stores the issuer inputs that cannot be reconstructed reliably from
    PEM alone and are therefore required to issue a replacement certificate from
    Vault later.
    """

    cert_name: str
    key_name: str
    scope: TLSObjectScope
    common_name: str
    target: Optional[str] = None
    pki_mount: Optional[str] = None
    role: Optional[str] = None
    ttl: Optional[str] = None
    alt_names: List[str] = field(default_factory=list)
    ip_sans: List[str] = field(default_factory=list)
    ca_cert_name: Optional[str] = None
    managed_by: TLSObjectManager = TLSObjectManager.VAULT

    @property
    def store_target(self) -> str:
        return self.target or ''

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, 'scope', TLSObjectScope(self.scope))
        except ValueError as e:
            raise TLSObjectException(f'Invalid Vault certificate metadata scope: {self.scope}') from e

        try:
            object.__setattr__(self, 'managed_by', TLSObjectManager(self.managed_by))
        except ValueError as e:
            raise TLSObjectException(f'Invalid Vault certificate metadata managed_by: {self.managed_by}') from e

        if self.managed_by != TLSObjectManager.VAULT:
            raise TLSObjectException('Vault certificate metadata must be managed_by=vault')
        if not _optional_str(self.cert_name):
            raise TLSObjectException('Vault certificate metadata requires cert_name')
        if not _optional_str(self.key_name):
            raise TLSObjectException('Vault certificate metadata requires key_name')
        if not _optional_str(self.common_name):
            raise TLSObjectException('Vault certificate metadata requires common_name')
        if self.scope in (TLSObjectScope.SERVICE, TLSObjectScope.HOST) and not _optional_str(self.target):
            raise TLSObjectException(f'Vault certificate metadata with scope={self.scope.value} requires target')
        if self.scope == TLSObjectScope.GLOBAL and _optional_str(self.target):
            raise TLSObjectException('Vault certificate metadata with scope=global must not set target')

        object.__setattr__(self, 'cert_name', _optional_str(self.cert_name) or '')
        object.__setattr__(self, 'key_name', _optional_str(self.key_name) or '')
        object.__setattr__(self, 'common_name', _optional_str(self.common_name) or '')
        object.__setattr__(self, 'target', _optional_str(self.target))
        object.__setattr__(self, 'pki_mount', _optional_str(self.pki_mount))
        object.__setattr__(self, 'role', _optional_str(self.role))
        object.__setattr__(self, 'ttl', _optional_str(self.ttl))
        object.__setattr__(self, 'ca_cert_name', _optional_str(self.ca_cert_name))
        object.__setattr__(self, 'alt_names', _string_list(self.alt_names, 'alt_names'))
        object.__setattr__(self, 'ip_sans', _string_list(self.ip_sans, 'ip_sans'))

    def to_json(self) -> Dict[str, Any]:
        return {
            'cert_name': self.cert_name,
            'key_name': self.key_name,
            'ca_cert_name': self.ca_cert_name,
            'managed_by': self.managed_by.value,
            'scope': self.scope.value,
            'target': self.target,
            'pki_mount': self.pki_mount,
            'role': self.role,
            'ttl': self.ttl,
            'common_name': self.common_name,
            'alt_names': list(self.alt_names),
            'ip_sans': list(self.ip_sans),
        }

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> 'VaultCertificateMetadata':
        if not isinstance(data, dict):
            raise TLSObjectException('Vault certificate metadata must be a JSON object')
        allowed = {
            'cert_name',
            'key_name',
            'ca_cert_name',
            'managed_by',
            'scope',
            'target',
            'pki_mount',
            'role',
            'ttl',
            'common_name',
            'alt_names',
            'ip_sans',
        }
        unknown = set(data.keys()) - allowed
        if unknown:
            raise TLSObjectException(f'Got unknown field(s) for Vault certificate metadata: {sorted(unknown)}')
        return cls(
            cert_name=str(data.get('cert_name', '')),
            key_name=str(data.get('key_name', '')),
            ca_cert_name=_optional_str(data.get('ca_cert_name')),
            managed_by=data.get('managed_by', TLSObjectManager.VAULT.value),
            scope=data.get('scope', TLSObjectScope.UNKNOWN.value),
            target=_optional_str(data.get('target')),
            pki_mount=_optional_str(data.get('pki_mount')),
            role=_optional_str(data.get('role')),
            ttl=_optional_str(data.get('ttl')),
            common_name=str(data.get('common_name', '')),
            alt_names=_string_list(data.get('alt_names', []), 'alt_names'),
            ip_sans=_string_list(data.get('ip_sans', []), 'ip_sans'),
        )


class VaultCertificateMetadataStore:
    """Store scoped Vault certificate renewal metadata in mgr KV."""

    def __init__(self, mgr: Any) -> None:
        self.mgr = mgr
        self.metadata_by_cert: Dict[str, Dict[str, VaultCertificateMetadata]] = {}

    def _kv_key(self, cert_name: str) -> str:
        return VAULT_CERT_METADATA_STORE_PREFIX + cert_name

    def _set_store(self, cert_name: str, payload: Optional[Dict[str, Any]]) -> None:
        self.mgr.set_store(self._kv_key(cert_name), json.dumps(payload) if payload is not None else None)

    def save(self, metadata: VaultCertificateMetadata) -> None:
        self.metadata_by_cert.setdefault(metadata.cert_name, {})[metadata.store_target] = metadata
        self._persist_cert(metadata.cert_name)

    def get(self, cert_name: str, target: Optional[str] = None) -> Optional[VaultCertificateMetadata]:
        return self.metadata_by_cert.get(cert_name, {}).get(target or '')

    def remove(self, cert_name: str, target: Optional[str] = None) -> bool:
        target_key = target or ''
        if cert_name not in self.metadata_by_cert or target_key not in self.metadata_by_cert[cert_name]:
            return False
        del self.metadata_by_cert[cert_name][target_key]
        self._persist_cert(cert_name)
        return True

    def list(self) -> List[VaultCertificateMetadata]:
        metadata: List[VaultCertificateMetadata] = []
        for targets in self.metadata_by_cert.values():
            metadata.extend(targets.values())
        return metadata

    def _persist_cert(self, cert_name: str) -> None:
        targets = self.metadata_by_cert.get(cert_name, {})
        if not targets:
            self.metadata_by_cert.pop(cert_name, None)
            self._set_store(cert_name, None)
            return
        payload = {
            target: metadata.to_json()
            for target, metadata in targets.items()
        }
        self._set_store(cert_name, payload)

    def load(self) -> None:
        self.metadata_by_cert = {}
        for key, raw in self.mgr.get_store_prefix(VAULT_CERT_METADATA_STORE_PREFIX).items():
            cert_name = key[len(VAULT_CERT_METADATA_STORE_PREFIX):]
            if not raw:
                continue
            try:
                targets = json.loads(raw)
            except json.JSONDecodeError as e:
                logger.warning('VaultCertificateMetadataStore: cannot parse JSON for %r: %r', cert_name, e)
                continue
            if not isinstance(targets, dict):
                logger.warning(
                    'VaultCertificateMetadataStore: invalid data for %r. Expected dict but got %s',
                    cert_name,
                    type(targets).__name__,
                )
                continue
            for target, payload in targets.items():
                try:
                    metadata = VaultCertificateMetadata.from_json(payload)
                    self.metadata_by_cert.setdefault(metadata.cert_name, {})[metadata.store_target] = metadata
                    if metadata.cert_name != cert_name:
                        logger.warning(
                            'VaultCertificateMetadataStore: metadata cert_name %r does not match store key %r',
                            metadata.cert_name,
                            cert_name,
                        )
                    if metadata.store_target != target:
                        logger.warning(
                            'VaultCertificateMetadataStore: metadata target %r does not match store target %r for %r',
                            metadata.store_target,
                            target,
                            cert_name,
                        )
                except Exception as e:
                    logger.warning(
                        'VaultCertificateMetadataStore: failed to decode metadata for %r target %r: %r',
                        cert_name,
                        target,
                        e,
                    )
