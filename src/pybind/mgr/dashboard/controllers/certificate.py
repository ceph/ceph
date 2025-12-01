import logging
from typing import Any, Dict, List, Optional

import cherrypy
from ceph.deployment.service_spec import ServiceSpec

from ..security import Scope
from ..services.certificate import build_certificate_status_response, \
    fetch_certificates_for_service, find_certificate_for_service, \
    get_daemon_hostnames, process_certificates_for_list
from ..services.exception import handle_orchestrator_error
from ..services.orchestrator import OrchClient, OrchFeature
from ..tools import str_to_bool
from . import APIDoc, APIRouter, EndpointDoc, ReadPermission, RESTController
from ._version import APIVersion
from .orchestrator import raise_if_no_orchestrator

logger = logging.getLogger(__name__)


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
    'subject': (dict, 'Certificate subject details'),
    'key_type': (str, 'Public key type (RSA, ECC, etc.)'),
    'key_size': (int, 'Public key size in bits')
}]


@APIRouter('/certificate', Scope.HOSTS)
@APIDoc("Certificate Management API", "Certificate")
class Certificate(RESTController):

    @EndpointDoc("List All Certificates",
                 parameters={
                     'status': (str, 'Filter by certificate status '
                                     '(e.g., "expired", "expiring", "valid", "invalid")'),
                     'scope': (str, 'Filter by certificate scope '
                                    '(e.g., "SERVICE", "HOST", "GLOBAL")'),
                     'service_name': (str, 'Filter by certificate name '
                                      '(e.g., "rgw*")'),
                     'include_cephadm_signed': (bool, 'Include cephadm-signed certificates '
                                                'in the list (default: False)')
                 },
                 responses={200: CERTIFICATE_LIST_SCHEMA})
    @raise_if_no_orchestrator([OrchFeature.SERVICE_LIST])
    @RESTController.MethodMap(version=APIVersion(2, 0))  # type: ignore
    @ReadPermission
    @handle_orchestrator_error('certificate')
    def list(self, status: Optional[str] = None, scope: Optional[str] = None,
             service_name: Optional[str] = None,
             include_cephadm_signed: Any = False) -> List[Dict[str, Any]]:
        """
        List all certificates configured in the cluster.

        This endpoint returns a list of all certificates managed by certmgr,
        including both user-provided and cephadm-signed certificates.

        :param status: Filter by certificate status. Valid values: 'expired',
            'expiring', 'valid', 'invalid'
        :param scope: Filter by certificate scope. Valid values: 'SERVICE',
            'HOST', 'GLOBAL'
        :param service_name: Filter by certificate name. Supports wildcards
            (e.g., 'rgw*')
        :param include_cephadm_signed: If True, include cephadm-signed certificates.
            If False (default), only user-provided certificates are returned.
            Can be boolean or string ('true'/'false').
        :return: List of certificate objects with their details
        """
        orch = OrchClient.instance()

        if isinstance(include_cephadm_signed, str):
            include_cephadm_signed = str_to_bool(include_cephadm_signed)
        elif not isinstance(include_cephadm_signed, bool):
            include_cephadm_signed = bool(include_cephadm_signed)

        # Build filter_by string from separate parameters
        filter_parts = []
        if status:
            filter_parts.append(f'status={status.lower()}')
        if scope:
            filter_parts.append(f'scope={scope.lower()}')
        if service_name:
            filter_parts.append(f'name=*{service_name.lower()}*')

        filter_by = ','.join(filter_parts)

        cert_ls_data = None
        try:
            cert_ls_result = orch.cert_store.cert_ls(
                filter_by=filter_by or '',
                show_details=False,
                include_cephadm_signed=include_cephadm_signed
            )
            cert_ls_data = cert_ls_result if cert_ls_result else {}
        except RuntimeError as e:
            logger.error('Failed to retrieve certificate list: %s', str(e), exc_info=True)
            raise cherrypy.HTTPError(500, f'Failed to retrieve certificate list: {str(e)}')

        # Transform certificate data into a list format
        certificates_list = process_certificates_for_list(cert_ls_data)
        if status:
            status_lower = status.lower()
            certificates_list = [
                cert for cert in certificates_list
                if cert.get('status', '').lower() == status_lower
            ]

        if scope:
            scope_upper = scope.upper()
            certificates_list = [
                cert for cert in certificates_list
                if cert.get('scope', '').upper() == scope_upper
            ]

        if service_name:
            service_name_lower = service_name.lower()
            certificates_list = [
                cert for cert in certificates_list
                if service_name_lower in cert.get('target', '').lower()
                or service_name_lower in cert.get('cert_name', '').lower()
            ]

        cherrypy.response.headers['X-Total-Count'] = str(len(certificates_list))

        return certificates_list

    @raise_if_no_orchestrator([OrchFeature.SERVICE_LIST, OrchFeature.DAEMON_LIST])
    @ReadPermission
    @handle_orchestrator_error('certificate')
    def get(self, service_name: str) -> Dict[str, Any]:
        """
        Get detailed certificate information for a service.

        :param service_name: The service name, e.g. 'rgw.myzone'.
        :return: Detailed certificate information including full certificate details
        """
        orch = OrchClient.instance()

        # Get service information
        services = orch.services.get(service_name)
        if not services:
            raise cherrypy.HTTPError(404, 'Service {} not found'.format(service_name))

        service = services[0]
        service_type = service.service_type()
        service_name_full = service.spec.service_name()

        cert_config = ServiceSpec.REQUIRES_CERTIFICATES.get(service_type)
        if not cert_config:
            raise cherrypy.HTTPError(400, 'Service {} does not require certificates'
                                     .format(service_name))

        user_cert_name = f"{service_type.replace('-', '_')}_ssl_cert"
        cephadm_cert_name = f"cephadm-signed_{service_type}_cert"
        cert_scope_str = cert_config.get('scope', 'service').upper()

        cert_ls_data = fetch_certificates_for_service(orch, service_type, user_cert_name,
                                                      cephadm_cert_name)

        # Get daemon hostnames for HOST scope certificates
        daemon_hostnames, _ = get_daemon_hostnames(orch, service_name_full)

        # Find the certificate - try user-provided first, then cephadm-signed
        cert_details, target_key, cert_name, cert_scope_str = find_certificate_for_service(
            cert_ls_data, service_type, service_name_full, cert_scope_str, daemon_hostnames
        )

        return build_certificate_status_response(
            cert_details, cert_name or user_cert_name, cert_scope_str, target_key,
            include_target=True, include_details=True
        )

    @EndpointDoc("Get Root CA Certificate",
                 responses={
                     200: {
                         'certificate': (str, 'Root CA certificate in PEM format')
                     }
                 })
    @RESTController.Collection('GET', path='/root-ca')
    @raise_if_no_orchestrator([OrchFeature.SERVICE_LIST])
    @ReadPermission
    @handle_orchestrator_error('certificate')
    def root_ca(self) -> Dict[str, str]:
        """
        Get the cephadm root CA certificate.

        This endpoint returns the root Certificate Authority (CA) certificate
        used by cephadm to sign other certificates in the cluster.

        :return: Dictionary with certificate field containing root CA certificate in PEM format
        """
        orch = OrchClient.instance()

        # Root CA certificate name
        root_ca_cert_name = 'cephadm_root_ca_cert'

        # Get the root CA certificate
        # Root CA is GLOBAL scope, so no service_name or hostname needed
        try:
            if not hasattr(orch.cert_store, 'get_cert'):
                raise cherrypy.HTTPError(500, 'Certificate store get_cert method not available')
            root_ca_cert = orch.cert_store.get_cert(
                root_ca_cert_name,
                service_name=None,
                hostname=None,
                ignore_missing_exception=False
            )
        except Exception as e:
            raise cherrypy.HTTPError(500,
                                     f'Failed to retrieve root CA certificate: {str(e)}')

        if not root_ca_cert:
            raise cherrypy.HTTPError(404, 'Root CA certificate not found')

        return {'certificate': root_ca_cert}
