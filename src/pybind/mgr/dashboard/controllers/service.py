from typing import List, Optional, Dict
import cherrypy

from ceph.deployment.service_spec import ServiceSpec
from . import ApiController, RESTController, Task, Endpoint, ReadPermission
from . import CreatePermission, DeletePermission
from .orchestrator import raise_if_no_orchestrator
from ..exceptions import DashboardException
from ..security import Scope
from ..services.orchestrator import OrchClient
from ..services.exception import handle_orchestrator_error


def service_task(name, metadata, wait_for=2.0):
    return Task("service/{}".format(name), metadata, wait_for)


@ApiController('/service', Scope.HOSTS)
class Service(RESTController):

    @Endpoint()
    @ReadPermission
    def known_types(self) -> List[str]:
        """
        Get a list of known service types, e.g. 'alertmanager',
        'node-exporter', 'osd' or 'rgw'.
        """
        return ServiceSpec.KNOWN_SERVICE_TYPES

    @raise_if_no_orchestrator
    def list(self, service_name: Optional[str] = None) -> List[dict]:
        orch = OrchClient.instance()
        return [service.to_json() for service in orch.services.list(service_name)]

    @raise_if_no_orchestrator
    def get(self, service_name: str) -> List[dict]:
        orch = OrchClient.instance()
        services = orch.services.get(service_name)
        if not services:
            raise cherrypy.HTTPError(404, 'Service {} not found'.format(service_name))
        return services[0].to_json()

    @RESTController.Resource('GET')
    @raise_if_no_orchestrator
    def daemons(self, service_name: str) -> List[dict]:
        orch = OrchClient.instance()
        daemons = orch.services.list_daemons(service_name)
        return [d.to_json() for d in daemons]

    @CreatePermission
    @raise_if_no_orchestrator
    @handle_orchestrator_error('service')
    @service_task('create', {'service_name': '{service_name}'})
    def create(self, service_spec: Dict, service_name: str):  # pylint: disable=W0613
        """
        :param service_spec: The service specification as JSON.
        :param service_name: The service name, e.g. 'alertmanager'.
        :return: None
        """
        try:
            orch = OrchClient.instance()
            orch.services.apply(service_spec)
        except (ValueError, TypeError) as e:
            raise DashboardException(e, component='service')

    @DeletePermission
    @raise_if_no_orchestrator
    @handle_orchestrator_error('service')
    @service_task('delete', {'service_name': '{service_name}'})
    def delete(self, service_name: str):
        """
        :param service_name: The service name, e.g. 'mds' or 'crash.foo'.
        :return: None
        """
        orch = OrchClient.instance()
        orch.services.remove(service_name)
