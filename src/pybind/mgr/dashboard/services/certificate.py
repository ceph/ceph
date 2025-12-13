"""
Certificate service for dashboard.

This service provides certificate management functionality following the
"Thin Controllers, Fat Services" pattern. All business logic for certificate
handling is contained here.
"""
import logging
from typing import Any, Dict, List, Optional, Tuple

from .. import mgr
from .orchestrator import OrchClient

logger = logging.getLogger(__name__)


def get_certificate_renewal_threshold_days() -> int:
    """
    Get the certificate renewal threshold days from cephadm config.
    Falls back to default value of 30 if config cannot be retrieved.

    :return: Number of days before expiration to consider certificate as expiring
    """
    threshold = mgr.get_module_option_ex('cephadm', 'certificate_renewal_threshold_days', 30)
    return int(threshold) if threshold is not None else 30


def determine_certificate_status(remaining_days: int) -> str:
    """
    Determine certificate status based on remaining days until expiration.

    :param remaining_days: Number of days remaining until certificate expiration
    :return: Status string ('expired', 'expiring', or 'valid')
    """
    renewal_threshold = get_certificate_renewal_threshold_days()
    if remaining_days < 0:
        return 'expired'
    if remaining_days < renewal_threshold:
        return 'expiring'
    return 'valid'


def extract_certificate_basic_info(cert_details: Dict[str, Any]) -> Dict[str, Any]:
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


def determine_signed_by(cert_name: str) -> str:
    """
    Determine if certificate is signed by cephadm or user based on certificate name.

    :param cert_name: Certificate name
    :return: 'cephadm' if cephadm-signed, 'user' otherwise
    """
    return 'cephadm' if cert_name and 'cephadm-signed' in cert_name else 'user'


def build_certificate_list_entry(cert_name: str, cert_details: Dict[str, Any],
                                 cert_scope: str, target: Optional[str] = None
                                 ) -> Dict[str, Any]:
    """
    Build a certificate list entry from certificate details.

    :param cert_name: Certificate name
    :param cert_details: Certificate details dictionary
    :param cert_scope: Certificate scope ('GLOBAL', 'SERVICE', or 'HOST')
    :param target: Optional target (service name or hostname)
    :return: Certificate list entry dictionary
    """
    cert_info = extract_certificate_basic_info(cert_details)
    remaining_days = cert_info['remaining_days']
    expiry_date = cert_info['expiry_date']
    subject = cert_info['subject']
    common_name = cert_info['common_name']
    issuer_str = cert_info['issuer_str']

    status = determine_certificate_status(remaining_days)
    signed_by = determine_signed_by(cert_name)

    return {
        'cert_name': cert_name,
        'scope': cert_scope,
        'signed_by': signed_by,
        'status': status,
        'days_to_expiration': remaining_days,
        'expiry_date': expiry_date,
        'issuer': issuer_str,
        'common_name': common_name,
        'target': target,
        'subject': subject,
        'key_type': cert_details.get('public_key', {}).get('key_type'),
        'key_size': cert_details.get('public_key', {}).get('key_size')
    }


def process_certificates_for_list(cert_ls_data: Dict[str, Any]
                                  ) -> List[Dict[str, Any]]:
    """
    Process certificate list data and return formatted certificate entries.

    :param cert_ls_data: Certificate list data from cert_ls
    :return: List of certificate entry dictionaries
    """
    certificates_list = []

    for cert_name, cert_data in cert_ls_data.items():
        cert_scope = cert_data.get('scope', 'UNKNOWN').upper()
        certificates = cert_data.get('certificates', {})

        if cert_scope == 'GLOBAL':
            cert_details = certificates if isinstance(certificates, dict) else {}
            if not isinstance(cert_details, dict) or 'Error' in cert_details:
                continue
            certificates_list.append(
                build_certificate_list_entry(cert_name, cert_details, cert_scope)
            )
        else:
            # For SERVICE and HOST scope, iterate through targets
            for target, cert_details in certificates.items():
                if isinstance(cert_details, dict) and 'Error' in cert_details:
                    continue
                if not isinstance(cert_details, dict):
                    continue
                certificates_list.append(
                    build_certificate_list_entry(cert_name, cert_details, cert_scope, target)
                )

    return certificates_list


def find_certificate_for_service(cert_ls_data: Optional[Dict[str, Any]],
                                 service_type: str, service_name: str,
                                 cert_scope_str: str,
                                 daemon_hostnames: Optional[List[str]] = None
                                 ) -> Tuple[Optional[Dict[str, Any]], Optional[str], str, str]:
    """
    Find certificate for a service, trying user-provided first, then cephadm-signed.

    :param cert_ls_data: Certificate list data from cert_ls
    :param service_type: The service type (e.g., 'rgw', 'grafana')
    :param service_name: The service name (e.g., 'rgw.myzone')
    :param cert_scope_str: Certificate scope from config ('SERVICE', 'HOST', or 'GLOBAL')
    :param daemon_hostnames: Optional list of hostnames where service daemons run
    :return: Tuple of (cert_details, target_key, cert_name, actual_scope)
    """
    def _find_in_data(cert_name: str, cert_scope: str) -> Tuple[
            Optional[Dict[str, Any]], Optional[str]]:
        """
        Helper function to find certificate details in cert_ls_data for a given certificate
        name and scope.

        :param cert_name: Name of the certificate to find
        :param cert_scope: Scope of the certificate ('SERVICE', 'HOST', or 'GLOBAL')
        :return: Tuple of (cert_details, target_key) or (None, None) if not found
        """
        if not cert_ls_data or cert_name not in cert_ls_data:
            return (None, None)

        cert_data = cert_ls_data[cert_name]
        certificates = cert_data.get('certificates', {})

        if cert_scope == 'SERVICE':
            # For SERVICE scope, match by service name
            if service_name in certificates:
                return (certificates[service_name], service_name)
        elif cert_scope == 'HOST':
            # For HOST scope, match by hostname
            if daemon_hostnames:
                for hostname in daemon_hostnames:
                    if hostname in certificates:
                        return (certificates[hostname], hostname)
            # If not found by hostname, get first available
            if certificates:
                target_key = next(iter(certificates.keys()))
                return (certificates[target_key], target_key)
        elif cert_scope == 'GLOBAL':
            # For GLOBAL scope, certificates dict contains the cert directly
            if isinstance(certificates, dict) and certificates:
                return (certificates, None)

        return (None, None)

    user_cert_name = f"{service_type.replace('-', '_')}_ssl_cert"
    cephadm_cert_name = f"cephadm-signed_{service_type}_cert"
    cert_details = None
    target_key = None
    cert_name = user_cert_name
    actual_scope = cert_scope_str

    # Try user-provided certificate first
    if cert_ls_data and user_cert_name in cert_ls_data:
        cert_data = cert_ls_data[user_cert_name]
        cert_scope_from_data = cert_data.get('scope', 'UNKNOWN').upper()
        # Use scope from certificate data if available, otherwise use from config
        actual_scope = (cert_scope_from_data if cert_scope_from_data != 'UNKNOWN'
                        else cert_scope_str)
        cert_details, target_key = _find_in_data(user_cert_name, actual_scope)
        if cert_details:
            cert_name = user_cert_name

    # If user-provided cert not found, try cephadm-signed certificate
    if not cert_details and cert_ls_data and cephadm_cert_name in cert_ls_data:
        # Cephadm-signed certificates are always HOST scope
        cert_details, target_key = _find_in_data(cephadm_cert_name, 'HOST')
        if cert_details:
            cert_name = cephadm_cert_name
            actual_scope = 'HOST'

    return (cert_details, target_key, cert_name, actual_scope)


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

    cert_ls_data = {}
    try:
        logger.debug(
            'Calling cert_ls with filter_by="%s", show_details=True, '
            'include_cephadm_signed=True',
            filter_pattern)
        cert_ls_result = orch.cert_store.cert_ls(
            filter_by=filter_pattern,
            show_details=True,
            include_cephadm_signed=True
        )
        cert_ls_data = cert_ls_result if cert_ls_result else {}

        # Verify we got both certificates, fetch individually if missing
        missing_certs = []
        if user_cert_name not in cert_ls_data:
            missing_certs.append(user_cert_name)
        if cephadm_cert_name not in cert_ls_data:
            missing_certs.append(cephadm_cert_name)

        # Fetch any missing certificates individually
        for cert_name in missing_certs:
            try:
                logger.debug('Fetching missing certificate individually: %s',
                             cert_name)
                individual_result = orch.cert_store.cert_ls(
                    filter_by=f'name={cert_name}',
                    show_details=True,
                    include_cephadm_signed=True
                )
                if individual_result and cert_name in individual_result:
                    cert_ls_data[cert_name] = individual_result[cert_name]
            except RuntimeError as e:
                logger.warning('Failed to fetch individual certificate %s: %s',
                               cert_name, str(e))

        logger.debug(
            'cert_ls returned %d certificate(s): %s',
            len(cert_ls_data) if cert_ls_data else 0,
            list(cert_ls_data.keys()) if cert_ls_data else [])
    except RuntimeError as e:
        logger.error('Failed to retrieve certificate information: %s',
                     str(e), exc_info=True)
        raise

    return cert_ls_data


def get_daemon_hostnames(orch: OrchClient, service_name: str) -> Tuple[List[str], Optional[str]]:
    """
    Get daemon hostnames for a service.

    :param orch: Orchestrator client instance
    :param service_name: Service name
    :return: Tuple of (daemon_hostnames list, target_hostname or None)
    """
    daemon_hostnames = []
    target_hostname = None
    try:
        daemons = orch.services.list_daemons(service_name=service_name)
        daemon_hostnames = [d.hostname for d in daemons if d.hostname]
        if daemon_hostnames:
            target_hostname = daemon_hostnames[0]
    except RuntimeError:
        pass
    return (daemon_hostnames, target_hostname)


def get_certificate_response_template(cert_name: Optional[str], cert_scope_str: Optional[str],
                                      target_key: Optional[str] = None
                                      ) -> Dict[str, Any]:
    """
    Get a certificate response template with all keys initialized.

    :param cert_name: Certificate name (can be None)
    :param cert_scope_str: Certificate scope (can be None)
    :param target_key: Optional target key (service name or hostname)
    :return: Dictionary template with all certificate response keys
    """
    response = {
        'cert_name': cert_name,
        'scope': cert_scope_str,
        'requires_certificate': True,
        'status': None,
        'days_to_expiration': None,
        'signed_by': None,
        'has_certificate': False,
        'certificate_source': None,
        'expiry_date': None,
        'issuer': None,
        'common_name': None
    }
    if target_key is not None:
        response['target'] = target_key
    return response


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

    # (detailed responses include target)
    use_target = target_key if (include_target or (target_key and include_details)) else None
    response = get_certificate_response_template(cert_name, cert_scope_str, use_target)

    if not cert_details:
        response.update({
            'status': 'not_configured',
            'signed_by': None,
            'has_certificate': False
        })
        return response

    if isinstance(cert_details, dict) and 'Error' in cert_details:
        response.update({
            'status': 'invalid',
            'signed_by': determine_signed_by(cert_name),
            'has_certificate': True
        })
        if include_details:
            response['error'] = cert_details.get('Error')
        return response

    # Extract certificate information
    cert_info = extract_certificate_basic_info(cert_details)
    remaining_days = cert_info['remaining_days']
    expiry_date = cert_info['expiry_date']
    common_name = cert_info['common_name']
    issuer_str = cert_info['issuer_str']

    # Determine status and signed_by
    status = determine_certificate_status(remaining_days)
    signed_by = determine_signed_by(cert_name)

    # Build base response
    response.update({
        'status': status,
        'days_to_expiration': remaining_days,
        'signed_by': signed_by,
        'has_certificate': True,
        'certificate_source': 'reference',
        'expiry_date': expiry_date,
        'issuer': issuer_str,
        'common_name': common_name
    })

    # Add detailed information if requested
    if include_details:
        subject = cert_info['subject']
        issuer = cert_info['issuer']
        extensions = cert_details.get('extensions', {})
        san_entries = extensions.get('subjectAltName', {})

        response['details'] = {
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

    return response


def get_certificate_status_for_service(service_type: str, service_name: str,
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

    # Check if service requires certificates using REQUIRES_CERTIFICATES mapping
    cert_config = ServiceSpec.REQUIRES_CERTIFICATES.get(service_type)
    requires_cert = cert_config is not None

    if not requires_cert:
        response = get_certificate_response_template(None, None)
        response.update({
            'status': None,
            'cert_name': None,
            'scope': None,
            'requires_certificate': False
        })
        return response

    assert cert_config is not None
    cert_scope_str = cert_config.get('scope', 'service').upper()  # 'service' -> 'SERVICE'

    # Find certificate in cert_ls_data - try user-provided first, then cephadm-signed
    cert_details, _, cert_name, cert_scope_str = find_certificate_for_service(
        cert_ls_data, service_type, service_name, cert_scope_str, daemon_hostnames
    )

    # Build certificate status response
    return build_certificate_status_response(
        cert_details, cert_name, cert_scope_str
    )
