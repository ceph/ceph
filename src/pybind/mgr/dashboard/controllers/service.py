import logging
from typing import Dict, List, Optional

import cherrypy
from ceph.deployment.service_spec import ServiceSpec

from ..security import Scope
from ..services.certificate import get_certificate_status_for_service
from ..services.exception import handle_custom_error, handle_orchestrator_error
from ..services.orchestrator import OrchClient, OrchFeature
from . import APIDoc, APIRouter, CreatePermission, DeletePermission, Endpoint, \
    ReadPermission, RESTController, Task, UpdatePermission
from ._version import APIVersion
from .orchestrator import raise_if_no_orchestrator

logger = logging.getLogger(__name__)


def service_task(name, metadata, wait_for=2.0):
    return Task("service/{}".format(name), metadata, wait_for)


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
            logger.debug('Calling cert_ls in service list with filter_by="", '
                         'show_details=True, include_cephadm_signed=True')
            cert_ls_result = orch.cert_store.cert_ls(
                filter_by='',
                show_details=True,
                include_cephadm_signed=True
            )
            cert_ls_data = cert_ls_result if cert_ls_result else {}
            logger.debug('cert_ls in service list returned %d certificate(s): %s',
                         len(cert_ls_data) if cert_ls_data else 0,
                         list(cert_ls_data.keys()) if cert_ls_data else [])
        except RuntimeError as e:
            # If certificate list fails, continue without certificate info
            logger.warning('Failed to retrieve certificate list in service list: %s',
                           str(e), exc_info=True)
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
                        except RuntimeError:
                            daemon_map[svc_name] = []
            except RuntimeError:
                pass

        # Add certificate status to each service
        for service in services:
            service_type = service.get('service_type', '')
            svc_name = service.get('service_name', '')
            daemon_hostnames = daemon_map.get(svc_name, [])
            service['certificate'] = get_certificate_status_for_service(
                service_type, svc_name, cert_ls_data, daemon_hostnames
            )

        cherrypy.response.headers['X-Total-Count'] = str(count)
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
            logger.debug('Calling cert_ls for service %s with filter_by="", '
                         'show_details=True, include_cephadm_signed=True',
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
                         list(cert_ls_data.keys()) if cert_ls_data else [])
        except RuntimeError as e:
            logger.warning('Failed to retrieve certificate list for service %s: %s',
                           service_name, str(e), exc_info=True)
            cert_ls_data = {}

        # Get daemon hostnames for HOST scope certificates
        daemon_hostnames = []
        try:
            daemons = orch.services.list_daemons(service_name=service_name)
            daemon_hostnames = [d.hostname for d in daemons if d.hostname]
        except RuntimeError:
            pass

        # Get basic certificate status
        cert_status = get_certificate_status_for_service(
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
