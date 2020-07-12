# -*- coding: utf-8 -*-
from __future__ import absolute_import

import copy

from typing import List, Dict

import cherrypy

from mgr_util import merge_dicts
from orchestrator import HostSpec
from . import ApiController, RESTController, Task, Endpoint, ReadPermission, \
    UiApiController, BaseController
from .orchestrator import raise_if_no_orchestrator
from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from ..services.orchestrator import OrchClient
from ..services.ceph_service import CephService
from ..services.exception import handle_orchestrator_error


def host_task(name, metadata, wait_for=10.0):
    return Task("host/{}".format(name), metadata, wait_for)


def merge_hosts_by_hostname(ceph_hosts, orch_hosts):
    # type: (List[dict], List[HostSpec]) -> List[dict]
    """
    Merge Ceph hosts with orchestrator hosts by hostnames.

    :param ceph_hosts: hosts returned from mgr
    :type ceph_hosts: list of dict
    :param orch_hosts: hosts returned from ochestrator
    :type orch_hosts: list of HostSpec
    :return list of dict
    """
    hosts = copy.deepcopy(ceph_hosts)
    orch_hosts_map = {host.hostname: host.to_json() for host in orch_hosts}

    # Sort labels.
    for hostname in orch_hosts_map:
        orch_hosts_map[hostname]['labels'].sort()

    # Hosts in both Ceph and Orchestrator.
    for host in hosts:
        hostname = host['hostname']
        if hostname in orch_hosts_map:
            host = merge_dicts(host, orch_hosts_map[hostname])
            host['sources']['orchestrator'] = True
            orch_hosts_map.pop(hostname)

    # Hosts only in Orchestrator.
    orch_hosts_only = [
        merge_dicts(
            {
                'ceph_version': '',
                'services': [],
                'sources': {
                    'ceph': False,
                    'orchestrator': True
                }
            }, orch_hosts_map[hostname]) for hostname in orch_hosts_map
    ]
    hosts.extend(orch_hosts_only)
    return hosts


def get_hosts(from_ceph=True, from_orchestrator=True):
    """
    Get hosts from various sources.
    """
    ceph_hosts = []
    if from_ceph:
        ceph_hosts = [
            merge_dicts(
                server, {
                    'addr': '',
                    'labels': [],
                    'service_type': '',
                    'sources': {
                        'ceph': True,
                        'orchestrator': False
                    },
                    'status': ''
                }) for server in mgr.list_servers()
        ]
    if from_orchestrator:
        orch = OrchClient.instance()
        if orch.available():
            return merge_hosts_by_hostname(ceph_hosts, orch.hosts.list())
    return ceph_hosts


def get_host(hostname: str) -> Dict:
    """
    Get a specific host from Ceph or Orchestrator (if available).
    :param hostname: The name of the host to fetch.
    :raises: cherrypy.HTTPError: If host not found.
    """
    for host in get_hosts():
        if host['hostname'] == hostname:
            return host
    raise cherrypy.HTTPError(404)


@ApiController('/host', Scope.HOSTS)
class Host(RESTController):
    def list(self, sources=None):
        if sources is None:
            return get_hosts()
        _sources = sources.split(',')
        from_ceph = 'ceph' in _sources
        from_orchestrator = 'orchestrator' in _sources
        return get_hosts(from_ceph, from_orchestrator)

    @raise_if_no_orchestrator
    @handle_orchestrator_error('host')
    @host_task('create', {'hostname': '{hostname}'})
    def create(self, hostname):
        orch_client = OrchClient.instance()
        self._check_orchestrator_host_op(orch_client, hostname, True)
        orch_client.hosts.add(hostname)

    @raise_if_no_orchestrator
    @handle_orchestrator_error('host')
    @host_task('delete', {'hostname': '{hostname}'})
    def delete(self, hostname):
        orch_client = OrchClient.instance()
        self._check_orchestrator_host_op(orch_client, hostname, False)
        orch_client.hosts.remove(hostname)

    def _check_orchestrator_host_op(self, orch_client, hostname, add_host=True):
        """Check if we can adding or removing a host with orchestrator

        :param orch_client: Orchestrator client
        :param add: True for adding host operation, False for removing host
        :raise DashboardException
        """
        host = orch_client.hosts.get(hostname)
        if add_host and host:
            raise DashboardException(
                code='orchestrator_add_existed_host',
                msg='{} is already in orchestrator'.format(hostname),
                component='orchestrator')
        if not add_host and not host:
            raise DashboardException(
                code='orchestrator_remove_nonexistent_host',
                msg='Remove a non-existent host {} from orchestrator'.format(
                    hostname),
                component='orchestrator')

    @RESTController.Resource('GET')
    def devices(self, hostname):
        # (str) -> List
        return CephService.get_devices_by_host(hostname)

    @RESTController.Resource('GET')
    def smart(self, hostname):
        # type: (str) -> dict
        return CephService.get_smart_data_by_host(hostname)

    @RESTController.Resource('GET')
    @raise_if_no_orchestrator
    def daemons(self, hostname: str) -> List[dict]:
        orch = OrchClient.instance()
        daemons = orch.services.list_daemons(None, hostname)
        return [d.to_json() for d in daemons]

    @handle_orchestrator_error('host')
    def get(self, hostname: str) -> Dict:
        """
        Get the specified host.
        :raises: cherrypy.HTTPError: If host not found.
        """
        return get_host(hostname)

    @raise_if_no_orchestrator
    @handle_orchestrator_error('host')
    def set(self, hostname: str, labels: List[str]):
        """
        Update the specified host.
        Note, this is only supported when Ceph Orchestrator is enabled.
        :param hostname: The name of the host to be processed.
        :param labels: List of labels.
        """
        orch = OrchClient.instance()
        host = get_host(hostname)
        current_labels = set(host['labels'])
        # Remove labels.
        remove_labels = list(current_labels.difference(set(labels)))
        for label in remove_labels:
            orch.hosts.remove_label(hostname, label)
        # Add labels.
        add_labels = list(set(labels).difference(current_labels))
        for label in add_labels:
            orch.hosts.add_label(hostname, label)


@UiApiController('/host', Scope.HOSTS)
class HostUi(BaseController):
    @Endpoint('GET')
    @ReadPermission
    @handle_orchestrator_error('host')
    def labels(self) -> List[str]:
        """
        Get all host labels.
        Note, host labels are only supported when Ceph Orchestrator is enabled.
        If Ceph Orchestrator is not enabled, an empty list is returned.
        :return: A list of all host labels.
        """
        labels = []
        orch = OrchClient.instance()
        if orch.available():
            for host in orch.hosts.list():
                labels.extend(host.labels)
        labels.sort()
        return list(set(labels))  # Filter duplicate labels.
