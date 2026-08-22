from typing import TYPE_CHECKING, Dict, Optional, Tuple
import logging

from cephadm.ssl_cert_utils import SSLConfigException
from cephadm.tlsobject_types import Cert, TLSObjectException, TLSObjectManager, TLSObjectScope
from cephadm.vault import VaultClientError, VaultPKIClient

if TYPE_CHECKING:
    from cephadm.cert_mgr import CertInfo, CertMgr

logger = logging.getLogger(__name__)


class CertificateIssuer:
    """Base interface for certmgr certificate lifecycle issuers.

    Issuers are responsible for renewing certificates owned by a specific
    TLSObjectManager. Only issuers that return True from supports_auto_fix()
    are allowed to participate in certmgr's automatic fix path.
    """

    managed_by: TLSObjectManager

    def __init__(self, managed_by: TLSObjectManager) -> None:
        self.managed_by = managed_by

    def supports_auto_fix(self) -> bool:
        return False

    def renew(self, cert_mgr: 'CertMgr', cert_info: 'CertInfo', cert_obj: Cert) -> bool:
        return False


class UnsupportedCertificateIssuer(CertificateIssuer):
    """Placeholder issuer for lifecycle owners that are not implemented yet."""

    def renew(self, cert_mgr: 'CertMgr', cert_info: 'CertInfo', cert_obj: Cert) -> bool:
        logger.debug(
            'Certificate issuer %s does not support automatic renewal for %s',
            self.managed_by,
            cert_info.cert_name,
        )
        return False


class CephadmCertificateIssuer(CertificateIssuer):
    """Issuer implementation for cephadm-managed self-signed certificates."""

    def __init__(self) -> None:
        super().__init__(TLSObjectManager.CEPHADM)

    def supports_auto_fix(self) -> bool:
        return True

    def renew(self, cert_mgr: 'CertMgr', cert_info: 'CertInfo', cert_obj: Cert) -> bool:
        try:
            logger.info(f'Renewing cephadm-signed certificate for {cert_info.cert_name}')
            new_cert, new_key = cert_mgr.ssl_certs.renew_cert(
                cert_obj.cert,
                cert_mgr.mgr.certificate_duration_days,
            )
            tlsobj_target = cert_mgr.cert_store.determine_tlsobject_target(
                cert_info.cert_name,
                cert_info.target,
            )
            cert_mgr.cert_store.save_tlsobject(
                cert_info.cert_name,
                new_cert,
                service_name=tlsobj_target.service,
                host=tlsobj_target.host,
                managed_by=TLSObjectManager.CEPHADM,
            )
            key_name = cert_info.cert_name.replace('_cert', '_key')
            cert_mgr.key_store.save_tlsobject(
                key_name,
                new_key,
                service_name=tlsobj_target.service,
                host=tlsobj_target.host,
                managed_by=TLSObjectManager.CEPHADM,
            )
            return True
        except SSLConfigException as e:
            logger.error(f'Error while trying to renew cephadm-signed certificate for {cert_info.cert_name}: {e}')
            return False


class VaultCertificateIssuer(CertificateIssuer):
    """Issuer implementation for Vault-managed certificates."""

    def __init__(self) -> None:
        super().__init__(TLSObjectManager.VAULT)

    def supports_auto_fix(self) -> bool:
        return True

    def _storage_target(self, scope: TLSObjectScope, target: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
        if scope == TLSObjectScope.SERVICE:
            return target, None
        if scope == TLSObjectScope.HOST:
            return None, target
        return None, None

    def renew(self, cert_mgr: 'CertMgr', cert_info: 'CertInfo', cert_obj: Cert) -> bool:
        metadata = cert_mgr.vault_cert_metadata_store.get(cert_info.cert_name, cert_info.target)
        if not metadata:
            logger.error(
                'Cannot renew Vault-managed certificate %s: missing Vault renewal metadata',
                cert_info,
            )
            return False

        try:
            logger.info('Renewing Vault-managed certificate for %s', cert_info.cert_name)
            client = VaultPKIClient(cert_mgr.get_vault_issuer_config(), cert_mgr.get_vault_token())
            creds = client.issue_certificate(
                common_name=metadata.common_name,
                alt_names=metadata.alt_names,
                ip_sans=metadata.ip_sans,
                ttl=metadata.ttl,
                mount=metadata.pki_mount,
                role=metadata.role,
            )
            service_name, host = self._storage_target(metadata.scope, metadata.target)
            cert_mgr.cert_store.save_tlsobject(
                metadata.cert_name,
                creds.cert,
                service_name=service_name,
                host=host,
                managed_by=TLSObjectManager.VAULT,
                editable=False,
            )
            cert_mgr.key_store.save_tlsobject(
                metadata.key_name,
                creds.key,
                service_name=service_name,
                host=host,
                managed_by=TLSObjectManager.VAULT,
                editable=False,
            )
            if metadata.ca_cert_name and creds.ca_cert:
                cert_mgr.cert_store.save_tlsobject(
                    metadata.ca_cert_name,
                    creds.ca_cert,
                    service_name=service_name,
                    host=host,
                    managed_by=TLSObjectManager.VAULT,
                    editable=False,
                )
            return True
        except (VaultClientError, TLSObjectException, SSLConfigException) as e:
            logger.error(
                'Error while trying to renew Vault-managed certificate for %s: %s',
                cert_info.cert_name,
                e,
            )
            return False


def build_certificate_issuers() -> Dict[TLSObjectManager, CertificateIssuer]:
    return {
        TLSObjectManager.CEPHADM: CephadmCertificateIssuer(),
        TLSObjectManager.USER: UnsupportedCertificateIssuer(TLSObjectManager.USER),
        TLSObjectManager.VAULT: VaultCertificateIssuer(),
        TLSObjectManager.ACME: UnsupportedCertificateIssuer(TLSObjectManager.ACME),
    }
