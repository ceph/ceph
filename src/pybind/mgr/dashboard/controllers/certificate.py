from typing import Any, Dict, List, Optional

from ceph.deployment.service_spec import ServiceSpec

from ..exceptions import DashboardException
from ..model.certificate import CEPHADM_ROOT_CA_CERT, CERTIFICATE_LIST_SCHEMA, \
    CertificateScope, CertificateStatus
from ..security import Scope
from ..services.certificate import CertificateService
from ..services.exception import handle_orchestrator_error
from ..services.orchestrator import OrchClient, OrchFeature
from ..tools import str_to_bool
from . import APIDoc, APIRouter, EndpointDoc, ReadPermission, RESTController
from .orchestrator import raise_if_no_orchestrator


@APIRouter('/service/certificate', Scope.HOSTS)
@APIDoc("Service Management API", "Service")
class Certificate(RESTController):

    @EndpointDoc("List All Certificates",
                 parameters={
                     'status': (str, 'Filter by certificate status '
                                     '(e.g., "expired", "expiring", "valid", "invalid")'),
                     'scope': (str, 'Filter by certificate scope '
                                    '(e.g., "service", "host", "global")'),
                     'service_type': (str, 'Filter by certificate type '
                                      '(e.g., "rgw*")'),
                     'include_cephadm_signed': (bool, 'Include cephadm-signed certificates '
                                                'in the list (default: False)')
                 },
                 responses={200: CERTIFICATE_LIST_SCHEMA})
    @raise_if_no_orchestrator([OrchFeature.SERVICE_LIST])
    @ReadPermission
    @handle_orchestrator_error('certificate')
    def list(self, status: Optional[str] = None, scope: Optional[str] = None,
             service_type: Optional[str] = None,
             include_cephadm_signed: bool = False) -> List[Dict]:
        """
        List all certificates configured in the cluster.

        This endpoint returns a list of all certificates managed by certmgr,
        including both user-provided and cephadm-signed certificates.

        :param status: Filter by certificate status. Valid values: 'expired',
            'expiring', 'valid', 'invalid'
        :param scope: Filter by certificate scope. Valid values: 'SERVICE',
            'HOST', 'GLOBAL'
        :param service_type: Filter by service type. Supports wildcards
            (e.g., 'rgw*')
        :param include_cephadm_signed: If True, include cephadm-signed certificates.
            If False (default), only user-provided certificates are returned.
        :return: List of certificate objects with their details
        """
        orch = OrchClient.instance()

        status_value = None
        scope_value = None

        if status:
            try:
                status_value = CertificateStatus(status.lower()).value
            except ValueError:
                valid_vals = ", ".join([s.value for s in CertificateStatus])
                raise DashboardException(
                    msg=f'Invalid status: {status}. Valid values are: {valid_vals}')

        if scope:
            try:
                scope_value = CertificateScope(scope.lower()).value
            except ValueError:
                valid_vals = ", ".join([s.value for s in CertificateScope])
                raise DashboardException(
                    msg=f'Invalid scope: {scope}. Valid values are: {valid_vals}')

        include_cephadm_signed = str_to_bool(include_cephadm_signed)

        filter_parts = []
        if status_value:
            filter_parts.append(f'status={status_value}')
        if scope_value:
            filter_parts.append(f'scope={scope_value}')
        if service_type:
            filter_parts.append(f'name=*{service_type.lower()}*')

        filter_by = ','.join(filter_parts)

        cert_ls_data = CertificateService.fetch_all_certificates(
            orch, filter_by=filter_by or '', show_details=False,
            include_cephadm_signed=include_cephadm_signed
        )

        # Transform certificate data into a list format
        # Note: Filtering is already done by cert_ls via filter_by parameter
        certificates_list = CertificateService.process_certificates_for_list(cert_ls_data)

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
            raise DashboardException(
                msg=f'Service {service_name} not found',
                http_status_code=404,
                component='certificate'
            )
        service = services[0]

        service_type = service.spec.service_type
        service_name_full = service.spec.service_name()

        cert_config = ServiceSpec.REQUIRES_CERTIFICATES.get(service_type)
        if not cert_config:
            return CertificateService.empty_response()

        user_cert_name = f"{service_type.replace('-', '_')}_ssl_cert"
        cephadm_cert_name = f"cephadm-signed_{service_type}_cert"
        cert_scope = CertificateScope(cert_config.get('scope', CertificateScope.SERVICE.value))

        cert_ls_data = CertificateService.fetch_certificates_for_service(
            orch, service_type, user_cert_name, cephadm_cert_name
        )

        daemon_hostnames, _ = CertificateService.get_daemon_hostnames(orch, service_name_full)

        # try user-provided first, then cephadm-signed
        cert_details, target_key, cert_name, cert_scope_str = \
            CertificateService.find_certificate_for_service(
                cert_ls_data, service_type, service_name_full, cert_scope, daemon_hostnames
            )

        return CertificateService.build_certificate_status_response(
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

        root_ca_cert_name = CEPHADM_ROOT_CA_CERT

        root_ca_cert = orch.cert_store.get_cert(root_ca_cert_name)

        if not root_ca_cert:
            raise DashboardException(
                msg='Root CA certificate not found',
                http_status_code=404,
                component='certificate'
            )

        return root_ca_cert
