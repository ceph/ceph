from typing import Dict, List, Optional

import cherrypy
from ceph.deployment.service_spec import ServiceSpec
from orchestrator_api import OrchClient, OrchFeature

from .. import mgr
from ..security import Scope
from ..services._paginate import ListPaginator
from ..services.exception import handle_custom_error, handle_orchestrator_error
from . import APIDoc, APIRouter, CreatePermission, DeletePermission, Endpoint, \
    ReadPermission, RESTController, Task, UpdatePermission
from ._version import APIVersion
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
    @RESTController.MethodMap(version=APIVersion(2, 0))  # type: ignore
    def list(self, service_name: Optional[str] = None, offset: int = 0, limit: int = 5,
             search: str = '', sort: str = '+service_name') -> List[dict]:
        orch = OrchClient(mgr).instance(mgr)
        services = orch.services.list(service_name=service_name)
        paginator = ListPaginator(offset=int(offset), limit=int(limit), sort=sort, search=search,
                                  input_list=services,
                                  searchable_params=['service_name', 'status.running',
                                                     'status.last_refreshed', 'status.size'],
                                  sortable_params=['service_name', 'status.running',
                                                   'status.last_refreshed', 'status.size'],
                                  default_sort='+service_name')
        services, count = list(paginator.list()), paginator.get_count()
        cherrypy.response.headers['X-Total-Count'] = count
        return services

    @raise_if_no_orchestrator([OrchFeature.SERVICE_LIST])
    def get(self, service_name: str) -> List[dict]:
        orch = OrchClient(mgr).instance(mgr)
        services = orch.services.get(service_name)
        if not services:
            raise cherrypy.HTTPError(404, 'Service {} not found'.format(service_name))
        return services[0].to_json()

    @RESTController.Resource('GET')
    @raise_if_no_orchestrator([OrchFeature.DAEMON_LIST])
    def daemons(self, service_name: str) -> List[dict]:
        orch = OrchClient(mgr).instance(mgr)
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

        OrchClient(mgr).instance(mgr).services.apply(service_spec, no_overwrite=True)

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

        OrchClient(mgr).instance(mgr).services.apply(service_spec, no_overwrite=False)

    @DeletePermission
    @raise_if_no_orchestrator([OrchFeature.SERVICE_DELETE])
    @handle_orchestrator_error('service')
    @service_task('delete', {'service_name': '{service_name}'})
    def delete(self, service_name: str):
        """
        :param service_name: The service name, e.g. 'mds' or 'crash.foo'.
        :return: None
        """
        orch = OrchClient(mgr).instance(mgr)
        orch.services.remove(service_name)
