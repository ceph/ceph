# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

from orchestrator import InventoryFilter
from orchestrator import OrchestratorClientMixin, raise_if_exception, OrchestratorError
from .. import mgr
from ..tools import wraps


logger = logging.getLogger('orchestrator')


# pylint: disable=abstract-method
class OrchestratorAPI(OrchestratorClientMixin):
    def __init__(self):
        super(OrchestratorAPI, self).__init__()
        self.set_mgr(mgr)

    def status(self):
        try:
            status, desc = super(OrchestratorAPI, self).available()
            logger.info("is orchestrator available: %s, %s", status, desc)
            return dict(available=status, description=desc)
        except (RuntimeError, OrchestratorError, ImportError):
            return dict(available=False,
                        description='Orchestrator is unavailable for unknown reason')

    def orchestrator_wait(self, completions):
        return self._orchestrator_wait(completions)


def wait_api_result(method):
    @wraps(method)
    def inner(self, *args, **kwargs):
        completion = method(self, *args, **kwargs)
        self.api.orchestrator_wait([completion])
        raise_if_exception(completion)
        return completion.result
    return inner


class ResourceManager(object):
    def __init__(self, api):
        self.api = api


class HostManger(ResourceManager):

    @wait_api_result
    def list(self):
        return self.api.get_hosts()

    def get(self, hostname):
        hosts = [host for host in self.list() if host.name == hostname]
        return hosts[0] if hosts else None

    @wait_api_result
    def add(self, hostname):
        return self.api.add_host(hostname)

    @wait_api_result
    def remove(self, hostname):
        return self.api.remove_host(hostname)


class InventoryManager(ResourceManager):

    @wait_api_result
    def list(self, hosts=None, refresh=False):
        node_filter = InventoryFilter(nodes=hosts) if hosts else None
        return self.api.get_inventory(node_filter=node_filter, refresh=refresh)


class ServiceManager(ResourceManager):

    @wait_api_result
    def list(self, service_type=None, service_id=None, node_name=None):
        return self.api.describe_service(service_type, service_id, node_name)

    def reload(self, service_type, service_ids):
        if not isinstance(service_ids, list):
            service_ids = [service_ids]

        completion_list = [self.api.service_action('reload', service_type,
                                                   service_name, service_id)
                           for service_name, service_id in service_ids]
        self.api.orchestrator_wait(completion_list)
        for c in completion_list:
            raise_if_exception(c)


class OrchClient(object):

    _instance = None

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self.api = OrchestratorAPI()

        self.hosts = HostManger(self.api)
        self.inventory = InventoryManager(self.api)
        self.services = ServiceManager(self.api)

    def available(self):
        return self.status()['available']

    def status(self):
        return self.api.status()
