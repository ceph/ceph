"""
Model definitions for certificate API responses.

These classes centralize the enums and response shapes so callers avoid
constructing ad-hoc dictionaries and can rely on consistent typing.
"""
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, Optional


class CertificateStatus(str, Enum):
    EXPIRED = 'expired'
    EXPIRING = 'expiring'
    VALID = 'valid'
    INVALID = 'invalid'
    NOT_CONFIGURED = 'not_configured'


class CertificateScope(str, Enum):
    SERVICE = 'service'
    HOST = 'host'
    GLOBAL = 'global'


@dataclass
class CertificateListEntry:
    STRING_FIELDS = (
        'cert_name', 'scope', 'signed_by', 'status',
        'expiry_date', 'issuer', 'common_name', 'target'
    )

    cert_name: str
    scope: str
    signed_by: str
    status: CertificateStatus
    days_to_expiration: Optional[int]
    expiry_date: Optional[str]
    issuer: Optional[str]
    common_name: Optional[str]
    target: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['status'] = self.status.value
        for key in self.STRING_FIELDS:
            if data.get(key) is None:
                data[key] = ''
        return data


@dataclass
class CertificateStatusResponse:
    STRING_FIELDS = (
        'cert_name', 'scope', 'status', 'signed_by',
        'certificate_source', 'expiry_date', 'issuer',
        'common_name', 'target', 'error'
    )
    BOOL_FIELDS = ('requires_certificate', 'has_certificate')

    cert_name: Optional[str]
    scope: Optional[str]
    requires_certificate: bool = True
    status: Optional[CertificateStatus] = None
    days_to_expiration: Optional[int] = None
    signed_by: Optional[str] = None
    has_certificate: bool = False
    certificate_source: Optional[str] = None
    expiry_date: Optional[str] = None
    issuer: Optional[str] = None
    common_name: Optional[str] = None
    target: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data['status'] = self.status.value if self.status else None
        for key in self.STRING_FIELDS:
            if data.get(key) is None:
                data[key] = ''
        for key in self.BOOL_FIELDS:
            if data.get(key) is None:
                data[key] = False
        return data


# Constants shared with cephadm certificate handling
CEPHADM_ROOT_CA_CERT = 'cephadm_root_ca_cert'
CEPHADM_SIGNED_CERT = 'cephadm-signed'


# Shared schema for certificate list API responses
CERTIFICATE_LIST_SCHEMA = [{
    'cert_name': (str, 'Certificate name'),
    'scope': (str, 'Certificate scope (SERVICE, HOST, or GLOBAL)'),
    'signed_by': (str, 'Certificate issuer (user or cephadm)'),
    'status': (str, 'Certificate status (valid, expiring, expired, invalid)'),
    'days_to_expiration': (int, 'Days remaining until expiration'),
    'expiry_date': (str, 'Certificate expiration date'),
    'issuer': (str, 'Certificate issuer distinguished name'),
    'common_name': (str, 'Certificate common name (CN)'),
    'target': (str, 'Certificate target (service name or hostname)'),
}]
