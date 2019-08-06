# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy

from . import ApiController, Endpoint, ReadPermission
from . import RESTController, Task
from ..security import Scope
from ..services.orchestrator import OrchClient
from ..tools import wraps


def orchestrator_task(name, metadata, wait_for=2.0):
    return Task("orchestrator/{}".format(name), metadata, wait_for)


def raise_if_no_orchestrator(method):
    @wraps(method)
    def inner(self, *args, **kwargs):
        orch = OrchClient.instance()
        if not orch.available():
            raise cherrypy.HTTPError(503)
        return method(self, *args, **kwargs)
    return inner


@ApiController('/orchestrator')
class Orchestrator(RESTController):

    @Endpoint()
    @ReadPermission
    def status(self):
        return OrchClient.instance().status()


@ApiController('/orchestrator/inventory', Scope.HOSTS)
class OrchestratorInventory(RESTController):

    @raise_if_no_orchestrator
    def list(self, hostname=None):
        orch = OrchClient.instance()
        hosts = [hostname] if hostname else None
        inventory_nodes = orch.inventory.list(hosts)
        return [node.to_json() for node in inventory_nodes]


@ApiController('/orchestrator/service', Scope.HOSTS)
class OrchestratorService(RESTController):

    @raise_if_no_orchestrator
    def list(self, hostname=None):
        orch = OrchClient.instance()
        return [service.to_json() for service in orch.services.list(None, None, hostname)]
