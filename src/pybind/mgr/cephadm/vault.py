from dataclasses import dataclass
import json
import ssl
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from cephadm.tlsobject_types import TLSCredentials


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


class VaultClientError(Exception):
    pass


class VaultAuthError(VaultClientError):
    pass


class VaultPKIClient:
    """Small dependency-free client for Vault's PKI issue endpoint.

    The client is intentionally limited to the API primitive certmgr needs for
    certificate issuance. It is not wired into certmgr renewal in this commit.
    """

    def __init__(self,
                 config: VaultIssuerConfig,
                 token: Optional[str],
                 timeout: int = 30) -> None:
        self.config = config
        self.token = _optional_str(token)
        self.timeout = timeout

    def issue_certificate(self,
                          common_name: str,
                          alt_names: Optional[List[str]] = None,
                          ip_sans: Optional[List[str]] = None,
                          ttl: Optional[str] = None,
                          mount: Optional[str] = None,
                          role: Optional[str] = None) -> TLSCredentials:
        """Issue a certificate from Vault PKI and return the PEM credentials."""
        self._validate_common_config()
        common_name = _optional_str(common_name) or ''
        if not common_name:
            raise VaultClientError('Vault certificate common_name is required')

        resolved_mount = _optional_str(mount) or self.config.pki_mount
        resolved_role = _optional_str(role) or self.config.role
        if not resolved_mount:
            raise VaultClientError('Vault PKI mount is required')
        if not resolved_role:
            raise VaultClientError('Vault PKI role is required')

        payload: Dict[str, str] = {'common_name': common_name}
        resolved_ttl = _optional_str(ttl) or self.config.ttl
        if resolved_ttl:
            payload['ttl'] = resolved_ttl
        if alt_names:
            payload['alt_names'] = ','.join(alt_names)
        if ip_sans:
            payload['ip_sans'] = ','.join(ip_sans)

        response = self._post_json(
            self._issue_url(resolved_mount, resolved_role),
            payload,
        )
        return self._parse_issue_response(response)

    def _validate_common_config(self) -> None:
        missing = self.config.missing_required_fields()
        if missing:
            raise VaultClientError(
                f'Missing required Vault issuer config fields: {", ".join(missing)}'
            )
        if not self.token:
            raise VaultClientError('Vault token is required')

    def _issue_url(self, mount: str, role: str) -> str:
        assert self.config.addr is not None
        base = self.config.addr.rstrip('/')
        quoted_mount = quote(mount.strip('/'), safe='/')
        quoted_role = quote(role.strip('/'), safe='')
        return f'{base}/v1/{quoted_mount}/issue/{quoted_role}'

    def _ssl_context(self) -> Optional[ssl.SSLContext]:
        if not self.config.verify_tls:
            return ssl._create_unverified_context()
        context = ssl.create_default_context()
        if self.config.ca_cert:
            if 'BEGIN CERTIFICATE' in self.config.ca_cert:
                context.load_verify_locations(cadata=self.config.ca_cert)
            else:
                context.load_verify_locations(cafile=self.config.ca_cert)
        return context

    def _post_json(self, url: str, payload: Dict[str, str]) -> Dict[str, Any]:
        assert self.token is not None
        data = json.dumps(payload).encode('utf-8')
        request = Request(
            url,
            data=data,
            headers={
                'Content-Type': 'application/json',
                'X-Vault-Token': self.token,
            },
            method='POST',
        )
        try:
            with urlopen(request, timeout=self.timeout, context=self._ssl_context()) as response:  # nosec
                raw_body = response.read()
        except HTTPError as e:
            message = self._format_http_error(e)
            if e.code in (401, 403):
                raise VaultAuthError(message) from e
            raise VaultClientError(message) from e
        except URLError as e:
            raise VaultClientError(f'Vault request failed: {e.reason}') from e
        except OSError as e:
            raise VaultClientError(f'Vault request failed: {e}') from e

        try:
            decoded = raw_body.decode('utf-8')
            loaded = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise VaultClientError(f'Vault returned invalid JSON: {e}') from e
        if not isinstance(loaded, dict):
            raise VaultClientError('Vault returned an unexpected non-object JSON response')
        return loaded

    def _format_http_error(self, error: HTTPError) -> str:
        details = ''
        try:
            raw_body = error.read()
            if raw_body:
                loaded = json.loads(raw_body.decode('utf-8'))
                if isinstance(loaded, dict) and loaded.get('errors'):
                    errors = loaded['errors']
                    if isinstance(errors, list):
                        details = ': ' + '; '.join(str(e) for e in errors)
                    else:
                        details = f': {errors}'
                else:
                    details = f': {loaded}'
        except Exception:
            details = ''
        if error.code in (401, 403):
            return (
                f'Vault authentication failed with HTTP {error.code}{details}. '
                'The Vault token may be invalid or expired; set a valid certmgr Vault token.'
            )
        return f'Vault request failed with HTTP {error.code}{details}'

    def _parse_issue_response(self, response: Dict[str, Any]) -> TLSCredentials:
        data = response.get('data')
        if not isinstance(data, dict):
            raise VaultClientError('Vault issue response is missing data object')

        cert = _optional_str(data.get('certificate'))
        key = _optional_str(data.get('private_key'))
        if not cert:
            raise VaultClientError('Vault issue response is missing certificate')
        if not key:
            raise VaultClientError('Vault issue response is missing private_key')

        ca_cert = self._extract_ca_cert(data)
        return TLSCredentials(cert=cert, key=key, ca_cert=ca_cert)

    def _extract_ca_cert(self, data: Dict[str, Any]) -> Optional[str]:
        ca_chain = data.get('ca_chain')
        if isinstance(ca_chain, list):
            chain = [_optional_str(ca) for ca in ca_chain]
            chain = [ca for ca in chain if ca]
            if chain:
                return '\n'.join(chain)
        return _optional_str(data.get('issuing_ca'))
