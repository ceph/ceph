import json
import logging
from typing import Dict, List, Optional, Any

import cherrypy
from ceph.deployment.service_spec import ServiceSpec

from ..security import Scope
from ..services.exception import handle_custom_error, handle_orchestrator_error
from ..services.orchestrator import OrchClient, OrchFeature
from . import APIDoc, APIRouter, CreatePermission, DeletePermission, Endpoint, \
    ReadPermission, RESTController, Task, UpdatePermission
from ._version import APIVersion
from .orchestrator import raise_if_no_orchestrator
from .certificate import (_determine_certificate_status, _determine_signed_by,
                          _extract_certificate_basic_info, _find_certificate_in_data)

logger = logging.getLogger(__name__)


def service_task(name, metadata, wait_for=2.0):
    return Task("service/{}".format(name), metadata, wait_for)


def _get_certificate_status_for_service(service_type: str, service_name: str, 
                                        cert_ls_data: Optional[Dict[str, Any]] = None,
                                        daemon_hostnames: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Get certificate status information for a service using REQUIRES_CERTIFICATES mapping.
    
    :param service_type: The service type (e.g., 'rgw', 'grafana')
    :param service_name: The service name (e.g., 'rgw.myzone')
    :param cert_ls_data: Optional pre-fetched certificate list data (all certificates)
    :param daemon_hostnames: Optional list of hostnames where service daemons run
    :return: Dictionary with certificate status information
    """
    # Check if service requires certificates using REQUIRES_CERTIFICATES mapping
    cert_config = ServiceSpec.REQUIRES_CERTIFICATES.get(service_type)
    requires_cert = cert_config is not None
    
    if not requires_cert:
        return {
            'status': None,
            'days_to_expiration': None,
            'cert_name': None,
            'scope': None,
            'signed_by': None,
            'has_certificate': False,
            'requires_certificate': False,
            'certificate_source': None,
            'expiry_date': None,
            'issuer': None,
            'common_name': None
        }
    
    # Get certificate names - try user-provided first, then cephadm-signed
    user_cert_name = f"{service_type.replace('-', '_')}_ssl_cert"
    cephadm_cert_name = f"cephadm-signed_{service_type}_cert"
    cert_scope_str = cert_config.get('scope', 'service').upper()  # 'service' -> 'SERVICE'
    
    # Find certificate in cert_ls_data - try user-provided first, then cephadm-signed
    cert_details = None
    target_key = None
    cert_name = user_cert_name
    
    # Try user-provided certificate first
    if cert_ls_data and user_cert_name in cert_ls_data:
        cert_data = cert_ls_data[user_cert_name]
        cert_scope = cert_data.get('scope', 'UNKNOWN').upper()
        # Use scope from certificate data if available, otherwise use from config
        actual_scope = cert_scope if cert_scope != 'UNKNOWN' else cert_scope_str
        cert_details, target_key = _find_certificate_in_data(
            cert_ls_data, user_cert_name, actual_scope, service_name, daemon_hostnames
        )
        if cert_details:
            cert_name = user_cert_name
            cert_scope_str = actual_scope
    
    # If user-provided cert not found, try cephadm-signed certificate
    if not cert_details and cert_ls_data and cephadm_cert_name in cert_ls_data:
        # Cephadm-signed certificates are always HOST scope
        cert_details, target_key = _find_certificate_in_data(
            cert_ls_data, cephadm_cert_name, 'HOST', service_name, daemon_hostnames
        )
        if cert_details:
            cert_name = cephadm_cert_name
            cert_scope_str = 'HOST'
    
    # Build certificate status response
    if cert_details and isinstance(cert_details, dict) and 'Error' in cert_details:
        # Certificate exists but has errors
        return {
            'status': 'invalid',
            'days_to_expiration': None,
            'cert_name': cert_name,
            'scope': cert_scope_str,
            'signed_by': None,
            'has_certificate': True,
            'requires_certificate': True,
            'certificate_source': None,
            'expiry_date': None,
            'issuer': None,
            'common_name': None
        }
    
    if cert_details and isinstance(cert_details, dict):
        # Extract certificate information
        cert_info = _extract_certificate_basic_info(cert_details)
        remaining_days = cert_info['remaining_days']
        expiry_date = cert_info['expiry_date']
        common_name = cert_info['common_name']
        issuer_str = cert_info['issuer_str']
        
        # Determine status and signed_by
        status = _determine_certificate_status(remaining_days)
        signed_by = _determine_signed_by(cert_name)
        
        return {
            'status': status,
            'days_to_expiration': remaining_days,
            'cert_name': cert_name,
            'scope': cert_scope_str,
            'signed_by': signed_by,
            'has_certificate': True,
            'requires_certificate': True,
            'certificate_source': 'reference',  # Default, could be enhanced
            'expiry_date': expiry_date,
            'issuer': issuer_str,
            'common_name': common_name
        }
    
    # No certificate found but service requires one
    return {
        'status': 'not_configured',
        'days_to_expiration': None,
        'cert_name': cert_name,
        'scope': cert_scope_str,
        'signed_by': None,
        'has_certificate': False,
        'requires_certificate': True,
        'certificate_source': None,
        'expiry_date': None,
        'issuer': None,
        'common_name': None
    }


@APIRouter('/service', Scope.HOSTS)
@APIDoc("Service Management API", "Service")
class Service(RESTController):

    @Endpoint()
    @ReadPermission
    def known_types(self) -> List[str]:
        """
        Get a list of known service types, e.g. 'alertmanager',
        'node-exporter', 'osd' or 'rgw'.
        """
        return ServiceSpec.KNOWN_SERVICE_TYPES

    @raise_if_no_orchestrator([OrchFeature.SERVICE_LIST])
    @RESTController.MethodMap(version=APIVersion(2, 0))  # type: ignore
    def list(self, service_name: Optional[str] = None, offset: int = 0, limit: int = 5,
             search: str = '', sort: str = '+service_name') -> List[dict]:
        orch = OrchClient.instance()
        services, count = orch.services.list(service_name=service_name, offset=int(offset),
                                             limit=int(limit), search=search, sort=sort)
        
        # Get all certificates at once for better performance
        cert_ls_data = None
        try:
            logger.debug('Calling cert_ls in service list with filter_by="", show_details=True, include_cephadm_signed=True')
            cert_ls_result = orch.cert_store.cert_ls(
                filter_by='',
                show_details=True,
                include_cephadm_signed=True
            )
            cert_ls_data = cert_ls_result if cert_ls_result else {}
            logger.debug('cert_ls in service list returned %d certificate(s): %s',
                        len(cert_ls_data) if cert_ls_data else 0,
                        json.dumps(cert_ls_data, indent=2, default=str) if cert_ls_data else '{}')
        except Exception as e:
            # If certificate list fails, continue without certificate info
            logger.warning('Failed to retrieve certificate list in service list: %s', str(e), exc_info=True)
            cert_ls_data = {}
        
        # Get daemon information for HOST scope certificates
        daemon_map = {}
        if cert_ls_data:
            try:
                for service in services:
                    svc_name = service.get('service_name', '')
                    if svc_name:
                        try:
                            daemons = orch.services.list_daemons(service_name=svc_name)
                            daemon_map[svc_name] = [d.hostname for d in daemons if d.hostname]
                        except Exception:
                            daemon_map[svc_name] = []
            except Exception:
                pass
        
        # Add certificate status to each service
        for service in services:
            service_type = service.get('service_type', '')
            svc_name = service.get('service_name', '')
            daemon_hostnames = daemon_map.get(svc_name, [])
            service['certificate'] = _get_certificate_status_for_service(
                service_type, svc_name, cert_ls_data, daemon_hostnames
            )
        
        cherrypy.response.headers['X-Total-Count'] = count
        return services

    @raise_if_no_orchestrator([OrchFeature.SERVICE_LIST])
    def get(self, service_name: str) -> List[dict]:
        orch = OrchClient.instance()
        services = orch.services.get(service_name)
        if not services:
            raise cherrypy.HTTPError(404, 'Service {} not found'.format(service_name))
        
        service = services[0].to_json()
        service_type = service.get('service_type', '')
        
        # Get all certificates at once
        cert_ls_data = None
        try:
            logger.debug('Calling cert_ls for service %s with filter_by="", show_details=True, include_cephadm_signed=True',
                        service_name)
            cert_ls_result = orch.cert_store.cert_ls(
                filter_by='',
                show_details=True,
                include_cephadm_signed=True
            )
            cert_ls_data = cert_ls_result if cert_ls_result else {}
            logger.debug('cert_ls for service %s returned %d certificate(s): %s',
                        service_name,
                        len(cert_ls_data) if cert_ls_data else 0,
                        json.dumps(cert_ls_data, indent=2, default=str) if cert_ls_data else '{}')
        except Exception as e:
            logger.warning('Failed to retrieve certificate list for service %s: %s', service_name, str(e), exc_info=True)
            cert_ls_data = {}
        
        # Get daemon hostnames for HOST scope certificates
        daemon_hostnames = []
        try:
            daemons = orch.services.list_daemons(service_name=service_name)
            daemon_hostnames = [d.hostname for d in daemons if d.hostname]
        except Exception:
            pass
        
        # Get basic certificate status
        cert_status = _get_certificate_status_for_service(
            service_type, service_name, cert_ls_data, daemon_hostnames
        )
        
        service['certificate'] = cert_status
        return service

    @RESTController.Resource('GET')
    @raise_if_no_orchestrator([OrchFeature.DAEMON_LIST])
    def daemons(self, service_name: str) -> List[dict]:
        orch = OrchClient.instance()
        daemons = orch.services.list_daemons(service_name=service_name)
        return [d.to_dict() for d in daemons]

    @CreatePermission
    @handle_custom_error('service', exceptions=(ValueError, TypeError))
    @raise_if_no_orchestrator([OrchFeature.SERVICE_CREATE])
    @handle_orchestrator_error('service')
    @service_task('create', {'service_name': '{service_name}'})
    def create(self, service_spec: Dict, service_name: str):  # pylint: disable=W0613
        """
        :param service_spec: The service specification as JSON.
        :param service_name: The service name, e.g. 'alertmanager'.
        :return: None
        """

        OrchClient.instance().services.apply(service_spec, no_overwrite=True)

    @UpdatePermission
    @handle_custom_error('service', exceptions=(ValueError, TypeError))
    @raise_if_no_orchestrator([OrchFeature.SERVICE_CREATE])
    @handle_orchestrator_error('service')
    @service_task('edit', {'service_name': '{service_name}'})
    def set(self, service_spec: Dict, service_name: str):  # pylint: disable=W0613
        """
        :param service_spec: The service specification as JSON.
        :param service_name: The service name, e.g. 'alertmanager'.
        :return: None
        """

        OrchClient.instance().services.apply(service_spec, no_overwrite=False)

    @DeletePermission
    @raise_if_no_orchestrator([OrchFeature.SERVICE_DELETE])
    @handle_orchestrator_error('service')
    @service_task('delete', {'service_name': '{service_name}'})
    def delete(self, service_name: str):
        """
        :param service_name: The service name, e.g. 'mds' or 'crash.foo'.
        :return: None
        """
        orch = OrchClient.instance()
        orch.services.remove(service_name)

