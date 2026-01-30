"""
Certificate service for dashboard.

This service provides certificate management functionality following the
"Thin Controllers, Fat Services" pattern. All business logic for certificate
handling is contained here.
"""
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from .. import mgr
from ..model.certificate import CEPHADM_SIGNED_CERT, CertificateListEntry, \
    CertificateScope, CertificateStatus, CertificateStatusResponse
from .orchestrator import OrchClient


def _get_certificate_renewal_threshold_days() -> int:
    """
    Get the certificate renewal threshold days from cephadm config.
    Falls back to default value of 30 if config cannot be retrieved.

    :return: Number of days before expiration to consider certificate as expiring
    """
    threshold = mgr.get_module_option_ex('cephadm', 'certificate_renewal_threshold_days', 30)
    return int(threshold)


def _determine_certificate_status(remaining_days: int) -> CertificateStatus:
    """
    Determine certificate status based on remaining days until expiration.

    :param remaining_days: Number of days remaining until certificate expiration
    :return: Status string (CertificateStatus.EXPIRED, EXPIRING, or VALID)
    """
    renewal_threshold = _get_certificate_renewal_threshold_days()
    if remaining_days < 0:
        return CertificateStatus.EXPIRED
    if remaining_days < renewal_threshold:
        return CertificateStatus.EXPIRING
    return CertificateStatus.VALID


def _extract_certificate_basic_info(cert_details: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract basic certificate information from certificate details.

    :param cert_details: Dictionary containing certificate details
    :return: Dictionary with extracted information (validity, remaining_days, expiry_date,
             subject, issuer, common_name, issuer_str)
    """
    validity = cert_details.get('validity', {})
    remaining_days = validity.get('remaining_days', 0)
    expiry_date = validity.get('not_after')

    subject = cert_details.get('subject', {})
    issuer = cert_details.get('issuer', {})
    common_name = subject.get('commonName') or subject.get('CN')
    issuer_str = (issuer.get('commonName') or issuer.get('CN') or str(issuer)
                  if issuer else None)

    return {
        'validity': validity,
        'remaining_days': remaining_days,
        'expiry_date': expiry_date,
        'subject': subject,
        'issuer': issuer,
        'common_name': common_name,
        'issuer_str': issuer_str
    }


def _determine_signed_by(cert_name: str) -> str:
    """
    Determine if certificate is signed by cephadm or user based on certificate name.

    :param cert_name: Certificate name
    :return: 'cephadm' if cephadm-signed, 'user' otherwise
    """
    return 'cephadm' if cert_name and CEPHADM_SIGNED_CERT in cert_name else 'user'


def _build_certificate_list_entry(cert_name: str, cert_details: Dict[str, Any],
                                  cert_scope: CertificateScope, target: Optional[str] = None
                                  ) -> CertificateListEntry:
    """
    Build a certificate list entry from certificate details.

    :param cert_name: Certificate name
    :param cert_details: Certificate details dictionary
    :param cert_scope: Certificate scope ('GLOBAL', 'SERVICE', or 'HOST')
    :param target: Optional target (service name or hostname)
    :return: Certificate list entry dictionary
    """
    cert_info = _extract_certificate_basic_info(cert_details)
    remaining_days = cert_info['remaining_days']
    expiry_date = cert_info['expiry_date']
    common_name = cert_info['common_name']
    issuer_str = cert_info['issuer_str']

    status = _determine_certificate_status(remaining_days)
    signed_by = _determine_signed_by(cert_name)

    return CertificateListEntry(
        cert_name=cert_name,
        scope=cert_scope.value.upper(),
        signed_by=signed_by,
        status=status,
        days_to_expiration=remaining_days,
        expiry_date=expiry_date,
        issuer=issuer_str,
        common_name=common_name,
        target=target
    )


def _get_certificate_response_template(cert_name: Optional[str], cert_scope_str: Optional[str],
                                       target_key: Optional[str] = None
                                       ) -> CertificateStatusResponse:
    """
    Get a certificate response template with all keys initialized.

    :param cert_name: Certificate name (can be None)
    :param cert_scope_str: Certificate scope (can be None)
    :param target_key: Optional target key (service name or hostname)
    :return: Dictionary template with all certificate response keys
    """
    return CertificateStatusResponse(
        cert_name=cert_name,
        scope=cert_scope_str,
        target=target_key
    )


def _select_service_certificate(certificates: Any, service_name: str
                                ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Pick certificate details for a service-scoped certificate.
    """
    if not isinstance(certificates, dict):
        return (None, None)

    target_key: Optional[str] = None
    if service_name and service_name in certificates:
        target_key = service_name

    cert_details = certificates.get(target_key) if target_key else None
    return (cert_details if isinstance(cert_details, dict) else None, target_key)


def _select_host_certificate(certificates: Any, daemon_hostnames: Optional[List[str]]
                             ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Pick certificate details for a host-scoped certificate.
    """
    if not isinstance(certificates, dict) or not certificates:
        return (None, None)

    target_key: Optional[str] = None
    if daemon_hostnames:
        for hostname in daemon_hostnames:
            if hostname in certificates:
                target_key = hostname
                break
    if target_key is None:
        target_key = next(iter(certificates.keys()))

    cert_details = certificates.get(target_key)
    return (cert_details if isinstance(cert_details, dict) else None, target_key)


def _select_global_certificate(certificates: Any) -> Optional[Dict[str, Any]]:
    """
    Pick certificate details for a global certificate.
    """
    if isinstance(certificates, dict) and certificates:
        return certificates
    return None


def _get_certificate_status_for_service(service_type: str, service_name: str,
                                        cert_ls_data: Optional[Dict[str, Any]] = None,
                                        daemon_hostnames: Optional[List[str]] = None
                                        ) -> Dict[str, Any]:
    """
    Get certificate status information for a service using REQUIRES_CERTIFICATES mapping.

    :param service_type: The service type (e.g., 'rgw', 'grafana')
    :param service_name: The service name (e.g., 'rgw.myzone')
    :param cert_ls_data: Optional pre-fetched certificate list data (all certificates)
    :param daemon_hostnames: Optional list of hostnames where service daemons run
    :return: Dictionary with certificate status information
    """
    from ceph.deployment.service_spec import ServiceSpec

    cert_config = ServiceSpec.REQUIRES_CERTIFICATES.get(service_type)
    requires_cert = cert_config is not None

    if not requires_cert:
        response = _get_certificate_response_template(None, None)
        response.requires_certificate = False
        return response.to_dict()

    assert cert_config is not None
    cert_scope = CertificateScope(cert_config.get('scope', CertificateScope.SERVICE.value).lower())

    # Find certificate in cert_ls_data - try user-provided first, then cephadm-signed
    cert_details, _, cert_name, cert_scope_str = CertificateService.find_certificate_for_service(
        cert_ls_data, service_type, service_name, cert_scope, daemon_hostnames
    )

    return CertificateService.build_certificate_status_response(
        cert_details, cert_name, cert_scope_str
    )


def _find_certificate_in_data(
    cert_ls_data: Optional[Dict[str, Any]],
    cert_name: str,
    cert_scope: CertificateScope,
    service_name: str,
    daemon_hostnames: Optional[List[str]],
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Helper to locate certificate data inside cert_ls response.
    """
    if not cert_ls_data or cert_name not in cert_ls_data:
        return (None, None)

    cert_data = cert_ls_data[cert_name]
    certificates = cert_data.get('certificates', {})
    if cert_scope == CertificateScope.SERVICE:
        return _select_service_certificate(certificates, service_name)
    if cert_scope == CertificateScope.HOST:
        return _select_host_certificate(certificates, daemon_hostnames)
    if cert_scope == CertificateScope.GLOBAL:
        return (_select_global_certificate(certificates), None)
    return (None, None)


class CertificateService:
    """
    Certificate service class providing certificate management functionality.

    This class encapsulates all certificate-related operations following the
    "Thin Controllers, Fat Services" pattern.
    """

    @staticmethod
    def process_certificates_for_list(cert_ls_data: Dict[str, Any]
                                      ) -> List[Dict[str, Any]]:
        """
        Process certificate list data and return formatted certificate entries.

        :param cert_ls_data: Certificate list data from cert_ls
        :return: List of certificate entry dictionaries
        """

        certificates_list: List[CertificateListEntry] = []

        for cert_name, cert_data in cert_ls_data.items():
            try:
                cert_scope = CertificateScope(str(cert_data.get('scope', 'UNKNOWN')).lower())
            except (ValueError, AttributeError):
                cert_scope = CertificateScope.SERVICE
            certificates = cert_data.get('certificates', {})

            if cert_scope == CertificateScope.GLOBAL:
                cert_details = certificates if isinstance(certificates, dict) else {}
                if not isinstance(cert_details, dict) or 'Error' in cert_details:
                    continue
                certificates_list.append(
                    _build_certificate_list_entry(cert_name, cert_details, cert_scope)
                )
            else:
                # For SERVICE and HOST scope, iterate through targets
                for target, cert_details in certificates.items():
                    if isinstance(cert_details, dict) and 'Error' in cert_details:
                        continue
                    if not isinstance(cert_details, dict):
                        continue
                    certificates_list.append(
                        _build_certificate_list_entry(cert_name, cert_details, cert_scope, target)
                    )

        return [entry.to_dict() for entry in certificates_list]

    @staticmethod
    def find_certificate_for_service(cert_ls_data: Optional[Dict[str, Any]],
                                     service_type: str, service_name: str,
                                     cert_scope: CertificateScope,
                                     daemon_hostnames: Optional[List[str]] = None
                                     ) -> Tuple[Optional[Dict[str, Any]], Optional[str], str, str]:
        """
        Find certificate for a service, trying user-provided first, then cephadm-signed.

        :param cert_ls_data: Certificate list data from cert_ls
        :param service_type: The service type (e.g., 'rgw', 'grafana')
        :param service_name: The service name (e.g., 'rgw.myzone')
        :param cert_scope: Certificate scope from config ('SERVICE', 'HOST', or 'GLOBAL')
        :param daemon_hostnames: Optional list of hostnames where service daemons run
        :return: Tuple of (cert_details, target_key, cert_name, actual_scope)
        """
        user_cert_name = f"{service_type.replace('-', '_')}_ssl_cert"
        cephadm_cert_name = f"cephadm-signed_{service_type}_cert"
        cert_details = None
        target_key = None
        cert_name = user_cert_name
        actual_scope = cert_scope.value.upper()

        # Try user-provided certificate first
        if cert_ls_data and user_cert_name in cert_ls_data:
            cert_data = cert_ls_data[user_cert_name]
            cert_scope_from_data = cert_data.get('scope', cert_scope.value)
            try:
                cert_scope = CertificateScope(cert_scope_from_data.lower())
            except ValueError:
                cert_scope = CertificateScope.SERVICE
            actual_scope = cert_scope.value.upper()
            cert_details, target_key = _find_certificate_in_data(
                cert_ls_data, user_cert_name, cert_scope, service_name, daemon_hostnames)
            if cert_details:
                cert_name = user_cert_name

        # If user-provided cert not found, try cephadm-signed certificate
        if not cert_details and cert_ls_data and cephadm_cert_name in cert_ls_data:
            cert_details, target_key = _find_certificate_in_data(
                cert_ls_data, cephadm_cert_name, CertificateScope.HOST,
                service_name, daemon_hostnames)
            if cert_details:
                cert_name = cephadm_cert_name
                actual_scope = CertificateScope.HOST.value.upper()

        return (cert_details, target_key, cert_name, actual_scope)

    @staticmethod
    def fetch_certificates_for_service(orch: OrchClient, service_type: str,
                                       user_cert_name: str, cephadm_cert_name: str
                                       ) -> Dict[str, Any]:
        """
        Fetch certificates for a specific service, including missing ones.

        :param orch: Orchestrator client instance
        :param service_type: Service type for filter pattern
        :param user_cert_name: User-provided certificate name
        :param cephadm_cert_name: Cephadm-signed certificate name
        :return: Dictionary of certificate data
        """
        service_type_for_filter = service_type.replace('-', '_')
        filter_pattern = f'name=*{service_type_for_filter}*'

        cert_ls_result = orch.cert_store.cert_ls(
            filter_by=filter_pattern,
            show_details=True,
            include_cephadm_signed=True
        )
        cert_ls_data = cert_ls_result or {}

        missing_certs: List[str] = []
        if user_cert_name not in cert_ls_data:
            missing_certs.append(user_cert_name)
        if cephadm_cert_name not in cert_ls_data:
            missing_certs.append(cephadm_cert_name)

        # Fetch any missing certificates individually
        for cert_name in missing_certs:
            individual_result = orch.cert_store.cert_ls(
                filter_by=f'name={cert_name}',
                show_details=True,
                include_cephadm_signed=True
            )
            if individual_result and cert_name in individual_result:
                cert_ls_data[cert_name] = individual_result[cert_name]

        return cert_ls_data

    @staticmethod
    def get_daemon_hostnames(orch: OrchClient, service_name: str
                             ) -> Tuple[List[str], Optional[str]]:
        """
        Get daemon hostnames for a service.

        :param orch: Orchestrator client instance
        :param service_name: Service name
        :return: Tuple of (daemon_hostnames list, target_hostname or None)
        """
        daemons = orch.services.list_daemons(service_name=service_name)
        daemon_hostnames = [d.hostname for d in daemons if d.hostname]
        target_hostname = daemon_hostnames[0] if daemon_hostnames else None
        return (daemon_hostnames, target_hostname)

    @staticmethod
    def build_certificate_status_response(cert_details: Optional[Dict[str, Any]],
                                          cert_name: str, cert_scope_str: str,
                                          target_key: Optional[str] = None,
                                          include_target: bool = False,
                                          include_details: bool = False
                                          ) -> Dict[str, Any]:
        """
        Build certificate status response dictionary.

        :param cert_details: Certificate details dict or None
        :param cert_name: Certificate name
        :param cert_scope_str: Certificate scope
        :param target_key: Optional target key (service name or hostname)
        :param include_target: Whether to include 'target' field in response
        :param include_details: Whether to include detailed 'details' field in response
        :return: Dictionary with certificate status information
        """
        use_target = target_key if (include_target or (target_key and include_details)) else None
        response = _get_certificate_response_template(cert_name, cert_scope_str, use_target)

        if not cert_details:
            response.status = CertificateStatus.NOT_CONFIGURED
            response.has_certificate = False
            return response.to_dict()

        if isinstance(cert_details, dict) and 'Error' in cert_details:
            response.status = CertificateStatus.INVALID
            response.signed_by = _determine_signed_by(cert_name)
            response.has_certificate = True
            if include_details:
                response.error = cert_details.get('Error')
            return response.to_dict()

        cert_info = _extract_certificate_basic_info(cert_details)
        remaining_days = cert_info['remaining_days']
        expiry_date = cert_info['expiry_date']
        common_name = cert_info['common_name']
        issuer_str = cert_info['issuer_str']

        status = _determine_certificate_status(remaining_days)
        signed_by = _determine_signed_by(cert_name)

        response.status = status
        response.days_to_expiration = remaining_days
        response.signed_by = signed_by
        response.has_certificate = True
        response.certificate_source = 'reference'
        response.expiry_date = expiry_date
        response.issuer = issuer_str
        response.common_name = common_name

        if include_details:
            subject = cert_info['subject']
            issuer = cert_info['issuer']
            extensions = cert_details.get('extensions', {})
            san_entries = extensions.get('subjectAltName', {})

            response.details = {
                'subject': subject,
                'issuer': issuer,
                'san_entries': {
                    'dns_names': san_entries.get('DNS', []),
                    'ip_addresses': san_entries.get('IP', [])
                },
                'key_type': cert_details.get('public_key', {}).get('key_type'),
                'key_size': cert_details.get('public_key', {}).get('key_size'),
                'validity': {
                    'not_before': cert_info['validity'].get('not_before'),
                    'not_after': cert_info['validity'].get('not_after'),
                    'remaining_days': remaining_days
                },
                'extensions': extensions
            }

        return response.to_dict()

    @staticmethod
    def enrich_services_with_certificates(orch: Any, services: List[Dict[str, Any]],
                                          cert_ls_data: Dict[str, Any]) -> None:
        """
        Enrich a list of services with certificate status information.

        This function modifies the services list in place, adding a 'certificate'
        key to each service with its certificate status.

        :param orch: Orchestrator client instance
        :param services: List of service dictionaries to enrich (modified in place)
        :param cert_ls_data: Certificate list data from cert_ls
        """
        daemon_hosts_by_service: Dict[str, List[str]] = defaultdict(list)
        for daemon in orch.services.list_daemons():
            service_name = daemon.service_name()
            if service_name and daemon.hostname:
                daemon_hosts_by_service[service_name].append(daemon.hostname)

        for service in services:
            svc_name = service.get('service_name', '')
            daemon_hostnames = daemon_hosts_by_service.get(svc_name, [])

            service['certificate'] = _get_certificate_status_for_service(
                service.get('service_type', ''),
                svc_name,
                cert_ls_data,
                daemon_hostnames
            )

    @staticmethod
    def empty_response() -> Dict[str, Any]:
        """
        Build a standard response for services that do not require certificates.
        """
        return CertificateStatusResponse(
            cert_name=None,
            scope=None,
            requires_certificate=False
        ).to_dict()

    @staticmethod
    def fetch_all_certificates(orch: Any, filter_by: str = '',
                               show_details: bool = True,
                               include_cephadm_signed: bool = True) -> Dict[str, Any]:
        """
        Fetch all certificates from the certificate store.

        :param orch: Orchestrator client instance
        :param filter_by: Filter string for certificates (default: '')
        :param show_details: Whether to include certificate details (default: True)
        :param include_cephadm_signed: Whether to include cephadm-signed certs (default: True)
        :return: Dictionary of certificate data
        """
        cert_ls_result = orch.cert_store.cert_ls(
            filter_by=filter_by,
            show_details=show_details,
            include_cephadm_signed=include_cephadm_signed
        )
        return cert_ls_result or {}
