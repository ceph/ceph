# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import ApiController, Endpoint, ReadPermission
from . import RESTController, Task
from ..security import Scope
from ..services.orchestrator import OrchClient


def orchestrator_task(name, metadata, wait_for=2.0):
    return Task("orchestrator/{}".format(name), metadata, wait_for)


@ApiController('/orchestrator')
class Orchestrator(RESTController):

    @Endpoint()
    @ReadPermission
    def status(self):
        return OrchClient.instance().status()


@ApiController('/orchestrator/inventory', Scope.HOSTS)
class OrchestratorInventory(RESTController):

    def list(self, hostname=None):
        orch = OrchClient.instance()
        result = []

        if orch.available():
            hosts = [hostname] if hostname else None
            inventory_nodes = orch.inventory.list(hosts)
            result = [node.to_json() for node in inventory_nodes]
        return result


@ApiController('/orchestrator/service', Scope.HOSTS)
class OrchestratorService(RESTController):
    def list(self, service_type=None, service_id=None, hostname=None):
        orch = OrchClient.instance()
        services = []

        if orch.available():
            services = [service.to_json() for service in orch.services.list(
                service_type, service_id, hostname)]
        return services
