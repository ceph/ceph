# -*- coding: utf-8 -*-

import os
import time
from collections import Counter
from typing import Dict, List, Optional

import cherrypy
from mgr_util import merge_dicts

from .. import mgr
from ..exceptions import DashboardException
from ..plugins.ttl_cache import ttl_cache, ttl_cache_invalidator
from ..security import Scope
from ..services._paginate import ListPaginator
from ..services.ceph_service import CephService
from ..services.exception import handle_orchestrator_error
from ..services.orchestrator import OrchClient, OrchFeature
from ..tools import TaskManager, merge_list_of_dicts_by_key, str_to_bool
from . import APIDoc, APIRouter, BaseController, Endpoint, EndpointDoc, \
    ReadPermission, RESTController, Task, UIRouter, UpdatePermission, \
    allow_empty_body
from ._version import APIVersion
from .orchestrator import raise_if_no_orchestrator

LIST_HOST_SCHEMA = {
    "hostname": (str, "Hostname"),
    "services": ([{
        "type": (str, "type of service"),
        "id": (str, "Service Id"),
    }], "Services related to the host"),
    "service_instances": ([{
        "type": (str, "type of service"),
        "count": (int, "Number of instances of the service"),
    }], "Service instances related to the host"),
    "ceph_version": (str, "Ceph version"),
    "addr": (str, "Host address"),
    "labels": ([str], "Labels related to the host"),
    "service_type": (str, ""),
    "sources": ({
        "ceph": (bool, ""),
        "orchestrator": (bool, "")
    }, "Host Sources"),
    "status": (str, "")
}

INVENTORY_SCHEMA = {
    "name": (str, "Hostname"),
    "addr": (str, "Host address"),
    "devices": ([{
        "rejected_reasons": ([str], ""),
        "available": (bool, "If the device can be provisioned to an OSD"),
        "path": (str, "Device path"),
        "sys_api": ({
            "removable": (str, ""),
            "ro": (str, ""),
            "vendor": (str, ""),
            "model": (str, ""),
            "rev": (str, ""),
            "sas_address": (str, ""),
            "sas_device_handle": (str, ""),
            "support_discard": (str, ""),
            "rotational": (str, ""),
            "nr_requests": (str, ""),
            "scheduler_mode": (str, ""),
            "partitions": ({
                "partition_name": ({
                    "start": (str, ""),
                    "sectors": (str, ""),
                    "sectorsize": (int, ""),
                    "size": (int, ""),
                    "human_readable_size": (str, ""),
                    "holders": ([str], "")
                }, "")
            }, ""),
            "sectors": (int, ""),
            "sectorsize": (str, ""),
            "size": (int, ""),
            "human_readable_size": (str, ""),
            "path": (str, ""),
            "locked": (int, "")
        }, ""),
        "lvs": ([{
            "name": (str, ""),
            "osd_id": (str, ""),
            "cluster_name": (str, ""),
            "type": (str, ""),
            "osd_fsid": (str, ""),
            "cluster_fsid": (str, ""),
            "osdspec_affinity": (str, ""),
            "block_uuid": (str, ""),
        }], ""),
        "human_readable_type": (str, "Device type. ssd or hdd"),
        "device_id": (str, "Device's udev ID"),
        "lsm_data": ({
            "serialNum": (str, ""),
            "transport": (str, ""),
            "mediaType": (str, ""),
            "rpm": (str, ""),
            "linkSpeed": (str, ""),
            "health": (str, ""),
            "ledSupport": ({
                "IDENTsupport": (str, ""),
                "IDENTstatus": (str, ""),
                "FAILsupport": (str, ""),
                "FAILstatus": (str, ""),
            }, ""),
            "errors": ([str], "")
        }, ""),
        "osd_ids": ([int], "Device OSD IDs")
    }], "Host devices"),
    "labels": ([str], "Host labels")
}


def host_task(name, metadata, wait_for=10.0):
    return Task("host/{}".format(name), metadata, wait_for)


def populate_service_instances(hostname, services):
    orch = OrchClient.instance()
    if orch.available():
        services = (daemon['daemon_type']
                    for daemon in (d.to_dict()
                                   for d in orch.services.list_daemons(hostname=hostname)))
    else:
        services = (daemon['type'] for daemon in services)
    return [{'type': k, 'count': v} for k, v in Counter(services).items()]


@ttl_cache(60, label='get_hosts')
def get_hosts(sources=None):
    """
    Get hosts from various sources.
    """
    from_ceph = True
    from_orchestrator = True
    if sources:
        _sources = sources.split(',')
        from_ceph = 'ceph' in _sources
        from_orchestrator = 'orchestrator' in _sources

    if from_orchestrator:
        orch = OrchClient.instance()
        if orch.available():
            hosts = [
                merge_dicts(
                    {
                        'ceph_version': '',
                        'services': [],
                        'sources': {
                            'ceph': False,
                            'orchestrator': True
                        }
                    }, host.to_json()) for host in orch.hosts.list()
            ]
            return hosts

    ceph_hosts = []
    if from_ceph:
        ceph_hosts = [
            merge_dicts(
                server, {
                    'addr': '',
                    'labels': [],
                    'sources': {
                        'ceph': True,
                        'orchestrator': False
                    },
                    'status': ''
                }) for server in mgr.list_servers()
        ]
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


def get_device_osd_map():
    """Get mappings from inventory devices to OSD IDs.

    :return: Returns a dictionary containing mappings. Note one device might
        shared between multiple OSDs.
        e.g. {
                 'node1': {
                     'nvme0n1': [0, 1],
                     'vdc': [0],
                     'vdb': [1]
                 },
                 'node2': {
                     'vdc': [2]
                 }
             }
    :rtype: dict
    """
    result: dict = {}
    for osd_id, osd_metadata in mgr.get('osd_metadata').items():
        hostname = osd_metadata.get('hostname')
        devices = osd_metadata.get('devices')
        if not hostname or not devices:
            continue
        if hostname not in result:
            result[hostname] = {}
        # for OSD contains multiple devices, devices is in `sda,sdb`
        for device in devices.split(','):
            if device not in result[hostname]:
                result[hostname][device] = [int(osd_id)]
            else:
                result[hostname][device].append(int(osd_id))
    return result


def get_inventories(hosts: Optional[List[str]] = None,
                    refresh: Optional[bool] = None) -> List[dict]:
    """Get inventories from the Orchestrator and link devices with OSD IDs.

    :param hosts: Hostnames to query.
    :param refresh: Ask the Orchestrator to refresh the inventories. Note the this is an
                    asynchronous operation, the updated version of inventories need to
                    be re-queried later.
    :return: Returns list of inventory.
    :rtype: list
    """
    do_refresh = False
    if refresh is not None:
        do_refresh = str_to_bool(refresh)
    orch = OrchClient.instance()
    inventory_hosts = [host.to_json()
                       for host in orch.inventory.list(hosts=hosts, refresh=do_refresh)]
    device_osd_map = get_device_osd_map()
    for inventory_host in inventory_hosts:
        host_osds = device_osd_map.get(inventory_host['name'])
        for device in inventory_host['devices']:
            if host_osds:  # pragma: no cover
                dev_name = os.path.basename(device['path'])
                device['osd_ids'] = sorted(host_osds.get(dev_name, []))
            else:
                device['osd_ids'] = []
    return inventory_hosts


@allow_empty_body
def add_host(hostname: str, addr: Optional[str] = None,
             labels: Optional[List[str]] = None,
             status: Optional[str] = None):
    orch_client = OrchClient.instance()
    host = Host()
    host.check_orchestrator_host_op(orch_client, hostname)
    orch_client.hosts.add(hostname, addr, labels)
    if status == 'maintenance':
        orch_client.hosts.enter_maintenance(hostname)


@APIRouter('/host', Scope.HOSTS)
@APIDoc("Get Host Details", "Host")
class Host(RESTController):
    @EndpointDoc("List Host Specifications",
                 parameters={
                     'sources': (str, 'Host Sources'),
                     'facts': (bool, 'Host Facts')
                 },
                 responses={200: LIST_HOST_SCHEMA})
    @RESTController.MethodMap(version=APIVersion(1, 3))
    def list(self, sources=None, facts=False, offset: int = 0,
             limit: int = 5, search: str = '', sort: str = ''):
        hosts = get_hosts(sources)
        params = ['hostname']
        paginator = ListPaginator(int(offset), int(limit), sort, search, hosts,
                                  searchable_params=params, sortable_params=params,
                                  default_sort='+hostname')
        # pylint: disable=unnecessary-comprehension
        hosts = [host for host in paginator.list()]
        orch = OrchClient.instance()
        cherrypy.response.headers['X-Total-Count'] = paginator.get_count()
        for host in hosts:
            if 'services' not in host:
                host['services'] = []
            host['service_instances'] = populate_service_instances(
                host['hostname'], host['services'])
        if str_to_bool(facts):
            if orch.available():
                if not orch.get_missing_features(['get_facts']):
                    hosts_facts = []
                    for host in hosts:
                        facts = orch.hosts.get_facts(host['hostname'])[0]
                        hosts_facts.append(facts)
                    return merge_list_of_dicts_by_key(hosts, hosts_facts, 'hostname')

                raise DashboardException(
                    code='invalid_orchestrator_backend',  # pragma: no cover
                    msg="Please enable the cephadm orchestrator backend "
                    "(try `ceph orch set backend cephadm`)",
                    component='orchestrator',
                    http_status_code=400)

            raise DashboardException(code='orchestrator_status_unavailable',  # pragma: no cover
                                     msg="Please configure and enable the orchestrator if you "
                                         "really want to gather facts from hosts",
                                     component='orchestrator',
                                     http_status_code=400)
        return hosts

    @raise_if_no_orchestrator([OrchFeature.HOST_LIST, OrchFeature.HOST_ADD])
    @handle_orchestrator_error('host')
    @host_task('add', {'hostname': '{hostname}'})
    @EndpointDoc('',
                 parameters={
                     'hostname': (str, 'Hostname'),
                     'addr': (str, 'Network Address'),
                     'labels': ([str], 'Host Labels'),
                     'status': (str, 'Host Status')
                 },
                 responses={200: None, 204: None})
    @RESTController.MethodMap(version=APIVersion.EXPERIMENTAL)
    def create(self, hostname: str,
               addr: Optional[str] = None,
               labels: Optional[List[str]] = None,
               status: Optional[str] = None):  # pragma: no cover - requires realtime env
        add_host(hostname, addr, labels, status)

    @raise_if_no_orchestrator([OrchFeature.HOST_LIST, OrchFeature.HOST_REMOVE])
    @handle_orchestrator_error('host')
    @host_task('remove', {'hostname': '{hostname}'})
    @allow_empty_body
    def delete(self, hostname):  # pragma: no cover - requires realtime env
        orch_client = OrchClient.instance()
        self.check_orchestrator_host_op(orch_client, hostname, False)
        orch_client.hosts.remove(hostname)

    def check_orchestrator_host_op(self, orch_client, hostname, add=True):  # pragma:no cover
        """Check if we can adding or removing a host with orchestrator

        :param orch_client: Orchestrator client
        :param add: True for adding host operation, False for removing host
        :raise DashboardException
        """
        host = orch_client.hosts.get(hostname)
        if add and host:
            raise DashboardException(
                code='orchestrator_add_existed_host',
                msg='{} is already in orchestrator'.format(hostname),
                component='orchestrator')
        if not add and not host:
            raise DashboardException(
                code='orchestrator_remove_nonexistent_host',
                msg='Remove a non-existent host {} from orchestrator'.format(hostname),
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
    @raise_if_no_orchestrator([OrchFeature.DEVICE_LIST])
    @handle_orchestrator_error('host')
    @EndpointDoc('Get inventory of a host',
                 parameters={
                     'hostname': (str, 'Hostname'),
                     'refresh': (str, 'Trigger asynchronous refresh'),
                 },
                 responses={200: INVENTORY_SCHEMA})
    def inventory(self, hostname, refresh=None):
        inventory = get_inventories([hostname], refresh)
        if inventory:
            return inventory[0]
        return {}

    @RESTController.Resource('POST')
    @UpdatePermission
    @raise_if_no_orchestrator([OrchFeature.DEVICE_BLINK_LIGHT])
    @handle_orchestrator_error('host')
    @host_task('identify_device', ['{hostname}', '{device}'], wait_for=2.0)
    def identify_device(self, hostname, device, duration):
        # type: (str, str, int) -> None
        """
        Identify a device by switching on the device light for N seconds.
        :param hostname: The hostname of the device to process.
        :param device: The device identifier to process, e.g. ``/dev/dm-0`` or
        ``ABC1234DEF567-1R1234_ABC8DE0Q``.
        :param duration: The duration in seconds how long the LED should flash.
        """
        orch = OrchClient.instance()
        TaskManager.current_task().set_progress(0)
        orch.blink_device_light(hostname, device, 'ident', True)
        for i in range(int(duration)):
            percentage = int(round(i / float(duration) * 100))
            TaskManager.current_task().set_progress(percentage)
            time.sleep(1)
        orch.blink_device_light(hostname, device, 'ident', False)
        TaskManager.current_task().set_progress(100)

    @RESTController.Resource('GET')
    @raise_if_no_orchestrator([OrchFeature.DAEMON_LIST])
    def daemons(self, hostname: str) -> List[dict]:
        orch = OrchClient.instance()
        daemons = orch.services.list_daemons(hostname=hostname)
        return [d.to_dict() for d in daemons]

    @handle_orchestrator_error('host')
    @RESTController.MethodMap(version=APIVersion(1, 2))
    def get(self, hostname: str) -> Dict:
        """
        Get the specified host.
        :raises: cherrypy.HTTPError: If host not found.
        """
        host = get_host(hostname)
        host['service_instances'] = populate_service_instances(
            host['hostname'], host['services'])
        return host

    @ttl_cache_invalidator('get_hosts')
    @raise_if_no_orchestrator([OrchFeature.HOST_LABEL_ADD,
                               OrchFeature.HOST_LABEL_REMOVE,
                               OrchFeature.HOST_MAINTENANCE_ENTER,
                               OrchFeature.HOST_MAINTENANCE_EXIT,
                               OrchFeature.HOST_DRAIN])
    @handle_orchestrator_error('host')
    @EndpointDoc('',
                 parameters={
                     'hostname': (str, 'Hostname'),
                     'update_labels': (bool, 'Update Labels'),
                     'labels': ([str], 'Host Labels'),
                     'maintenance': (bool, 'Enter/Exit Maintenance'),
                     'force': (bool, 'Force Enter Maintenance'),
                     'drain': (bool, 'Drain Host')
                 },
                 responses={200: None, 204: None})
    @RESTController.MethodMap(version=APIVersion.EXPERIMENTAL)
    def set(self, hostname: str, update_labels: bool = False,
            labels: List[str] = None, maintenance: bool = False,
            force: bool = False, drain: bool = False):
        """
        Update the specified host.
        Note, this is only supported when Ceph Orchestrator is enabled.
        :param hostname: The name of the host to be processed.
        :param update_labels: To update the labels.
        :param labels: List of labels.
        :param maintenance: Enter/Exit maintenance mode.
        :param force: Force enter maintenance mode.
        :param drain: Drain host
        """
        orch = OrchClient.instance()
        host = get_host(hostname)

        if maintenance:
            status = host['status']
            if status != 'maintenance':
                orch.hosts.enter_maintenance(hostname, force)

            if status == 'maintenance':
                orch.hosts.exit_maintenance(hostname)

        if drain:
            orch.hosts.drain(hostname)

        if update_labels:
            # only allow List[str] type for labels
            if not isinstance(labels, list):
                raise DashboardException(
                    msg='Expected list of labels. Please check API documentation.',
                    http_status_code=400,
                    component='orchestrator')
            current_labels = set(host['labels'])
            # Remove labels.
            remove_labels = list(current_labels.difference(set(labels)))
            for label in remove_labels:
                orch.hosts.remove_label(hostname, label)
            # Add labels.
            add_labels = list(set(labels).difference(current_labels))
            for label in add_labels:
                orch.hosts.add_label(hostname, label)


@UIRouter('/host', Scope.HOSTS)
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

    @Endpoint('GET')
    @ReadPermission
    @raise_if_no_orchestrator([OrchFeature.DEVICE_LIST])
    @handle_orchestrator_error('host')
    def inventory(self, refresh=None):
        return get_inventories(None, refresh)
