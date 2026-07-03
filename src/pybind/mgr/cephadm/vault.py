from dataclasses import dataclass
from typing import Any, Dict, List, Optional


def _optional_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


@dataclass(frozen=True)
class VaultIssuerConfig:
    """Configuration used by certmgr to talk to a Vault PKI issuer.

    This object only models the resolved global Vault configuration. It does not
    perform any Vault API calls. Per-service overrides and issuance metadata will
    be layered on top in later commits.
    """

    addr: Optional[str] = None
    pki_mount: Optional[str] = None
    role: Optional[str] = None
    ttl: Optional[str] = None
    ca_cert: Optional[str] = None
    verify_tls: bool = True

    REQUIRED_FIELDS = ('addr', 'pki_mount', 'role')

    @classmethod
    def from_mgr(cls, mgr: Any) -> 'VaultIssuerConfig':
        return cls(
            addr=_optional_str(getattr(mgr, 'certmgr_vault_addr', None)),
            pki_mount=_optional_str(getattr(mgr, 'certmgr_vault_pki_mount', None)),
            role=_optional_str(getattr(mgr, 'certmgr_vault_role', None)),
            ttl=_optional_str(getattr(mgr, 'certmgr_vault_ttl', None)),
            ca_cert=_optional_str(getattr(mgr, 'certmgr_vault_cacert', None)),
            verify_tls=bool(getattr(mgr, 'certmgr_vault_verify_tls', True)),
        )

    def missing_required_fields(self) -> List[str]:
        return [field for field in self.REQUIRED_FIELDS if not getattr(self, field)]

    def is_configured(self) -> bool:
        return not self.missing_required_fields()

    def to_json(self) -> Dict[str, Any]:
        return {
            'addr': self.addr,
            'pki_mount': self.pki_mount,
            'role': self.role,
            'ttl': self.ttl,
            'ca_cert': self.ca_cert,
            'verify_tls': self.verify_tls,
        }
