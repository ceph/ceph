# -*- coding: utf-8 -*-

import copy
import os
import time
from typing import Dict, List, Optional

import cherrypy
from mgr_util import merge_dicts
from orchestrator import HostSpec

from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from ..services.ceph_service import CephService
from ..services.exception import handle_orchestrator_error
from ..services.orchestrator import OrchClient, OrchFeature
from ..tools import TaskManager, str_to_bool
from . import ApiController, BaseController, ControllerDoc, Endpoint, \
    EndpointDoc, ReadPermission, RESTController, Task, UiApiController, \
    UpdatePermission, allow_empty_body
from .orchestrator import raise_if_no_orchestrator

LIST_HOST_SCHEMA = {
    "hostname": (str, "Hostname"),
    "services": ([{
        "type": (str, "type of service"),
        "id": (str, "Service Id"),
    }], "Services related to the host"),
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
            host.update(orch_hosts_map[hostname])
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
                    be re-qeuried later.
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


@ApiController('/host', Scope.HOSTS)
@ControllerDoc("Get Host Details", "Host")
class Host(RESTController):
    @EndpointDoc("List Host Specifications",
                 parameters={
                     'sources': (str, 'Host Sources'),
                 },
                 responses={200: LIST_HOST_SCHEMA})
    def list(self, sources=None):
        if sources is None:
            return get_hosts()
        _sources = sources.split(',')
        from_ceph = 'ceph' in _sources
        from_orchestrator = 'orchestrator' in _sources
        return get_hosts(from_ceph, from_orchestrator)

    @raise_if_no_orchestrator([OrchFeature.HOST_LIST, OrchFeature.HOST_CREATE])
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
    @RESTController.MethodMap(version='0.1')
    def create(self, hostname: str,
               addr: Optional[str] = None,
               labels: Optional[List[str]] = None,
               status: Optional[str] = None):  # pragma: no cover - requires realtime env
        add_host(hostname, addr, labels, status)

    @raise_if_no_orchestrator([OrchFeature.HOST_LIST, OrchFeature.HOST_DELETE])
    @handle_orchestrator_error('host')
    @host_task('delete', {'hostname': '{hostname}'})
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
    def get(self, hostname: str) -> Dict:
        """
        Get the specified host.
        :raises: cherrypy.HTTPError: If host not found.
        """
        return get_host(hostname)

    @raise_if_no_orchestrator([OrchFeature.HOST_LABEL_ADD,
                               OrchFeature.HOST_LABEL_REMOVE,
                               OrchFeature.HOST_MAINTENANCE_ENTER,
                               OrchFeature.HOST_MAINTENANCE_EXIT])
    @handle_orchestrator_error('host')
    @EndpointDoc('',
                 parameters={
                     'hostname': (str, 'Hostname'),
                     'update_labels': (bool, 'Update Labels'),
                     'labels': ([str], 'Host Labels'),
                     'maintenance': (bool, 'Enter/Exit Maintenance'),
                     'force': (bool, 'Force Enter Maintenance')
                 },
                 responses={200: None, 204: None})
    @RESTController.MethodMap(version='0.1')
    def set(self, hostname: str, update_labels: bool = False,
            labels: List[str] = None, maintenance: bool = False,
            force: bool = False):
        """
        Update the specified host.
        Note, this is only supported when Ceph Orchestrator is enabled.
        :param hostname: The name of the host to be processed.
        :param update_labels: To update the labels.
        :param labels: List of labels.
        :param maintenance: Enter/Exit maintenance mode.
        :param force: Force enter maintenance mode.
        """
        orch = OrchClient.instance()
        host = get_host(hostname)

        if maintenance:
            status = host['status']
            if status != 'maintenance':
                orch.hosts.enter_maintenance(hostname, force)

            if status == 'maintenance':
                orch.hosts.exit_maintenance(hostname)

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

    @Endpoint('GET')
    @ReadPermission
    @raise_if_no_orchestrator([OrchFeature.DEVICE_LIST])
    @handle_orchestrator_error('host')
    def inventory(self, refresh=None):
        return get_inventories(None, refresh)
