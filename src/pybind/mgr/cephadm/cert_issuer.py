from typing import TYPE_CHECKING, Dict
import logging

from cephadm.ssl_cert_utils import SSLConfigException
from cephadm.tlsobject_types import Cert, TLSObjectManager

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


def build_certificate_issuers() -> Dict[TLSObjectManager, CertificateIssuer]:
    return {
        TLSObjectManager.CEPHADM: CephadmCertificateIssuer(),
        TLSObjectManager.USER: UnsupportedCertificateIssuer(TLSObjectManager.USER),
        TLSObjectManager.VAULT: UnsupportedCertificateIssuer(TLSObjectManager.VAULT),
        TLSObjectManager.ACME: UnsupportedCertificateIssuer(TLSObjectManager.ACME),
    }
