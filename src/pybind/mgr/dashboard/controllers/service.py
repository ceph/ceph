from typing import Dict, List, Optional

import cherrypy
from ceph.deployment.service_spec import ServiceSpec

from ..security import Scope
from ..services.exception import handle_custom_error, handle_orchestrator_error
from ..services.orchestrator import OrchClient, OrchFeature
from . import APIDoc, APIRouter, CreatePermission, DeletePermission, Endpoint, \
    ReadPermission, RESTController, Task, UpdatePermission
from .orchestrator import raise_if_no_orchestrator


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
    def list(self, service_name: Optional[str] = None) -> List[dict]:
        orch = OrchClient.instance()
        return [service.to_dict() for service in orch.services.list(service_name=service_name)]

    @raise_if_no_orchestrator([OrchFeature.SERVICE_LIST])
    def get(self, service_name: str) -> List[dict]:
        orch = OrchClient.instance()
        services = orch.services.get(service_name)
        if not services:
            raise cherrypy.HTTPError(404, 'Service {} not found'.format(service_name))
        return services[0].to_json()

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
