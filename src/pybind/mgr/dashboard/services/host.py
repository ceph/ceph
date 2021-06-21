import os
import time
import copy
from typing import Dict, List, Optional


import cherrypy
from mgr_util import merge_dicts
from orchestrator import HostSpec

from .. import mgr
from ..exceptions import DashboardException
from ..services.orchestrator import OrchClient
from ..tools import TaskManager, str_to_bool


"""class HostFacts(NamedTuple):
  memory_total_bytes: int
  hdd_capacity_bytes:int
  flash_capacity_bytes: int
  raw_capacity: int

  
class Host(NamedTuple):
  name: str
  labels: List[str]
  facts: HostFacts"""


class HostService(object):

    @staticmethod
    def _merge_hosts_by_hostname(ceph_hosts, orch_hosts):
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

    @staticmethod
    def _check_orchestrator_host_op(orch_client, hostname, add=True):  # pragma:no cover
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

    
    @staticmethod
    def get_hosts(sources=None):
        """
        Get hosts from various sources.
        """
        from_ceph = True
        from_orchestrator = True
        if sources is not None:
            _sources = sources.split(',')
            from_ceph = 'ceph' in _sources
            from_orchestrator = 'orchestrator' in _sources

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
                return HostService._merge_hosts_by_hostname(ceph_hosts, orch.hosts.list())
        return ceph_hosts

    @staticmethod
    def get_host(hostname: str) -> Dict:
        """
        Get a specific host from Ceph or Orchestrator (if available).
        :param hostname: The name of the host to fetch.
        :raises: cherrypy.HTTPError: If host not found.
        """
        for host in HostService.get_hosts():
            if host['hostname'] == hostname:
                return host
        raise cherrypy.HTTPError(404)


    @staticmethod
    def add_host(hostname: str, addr: Optional[str] = None,
                labels: Optional[List[str]] = None,
                status: Optional[str] = None):
        orch_client = OrchClient.instance()
        HostService._check_orchestrator_host_op(orch_client, hostname)
        orch_client.hosts.add(hostname, addr, labels)
        if status == 'maintenance':
            orch_client.hosts.enter_maintenance(hostname)


    @staticmethod
    def delete_host(hostname: str):
        orch_client = OrchClient.instance()
        HostService._check_orchestrator_host_op(orch_client, hostname, False)
        orch_client.hosts.remove(hostname)

    @staticmethod
    def update_host(hostname: str, update_labels: bool = False,
                labels: List[str] = None, maintenance: bool = False,
                force: bool = False):
        orch = OrchClient.instance()
        host = HostService.get_host(hostname)

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

    @staticmethod
    def get_labels() -> List[str]:
        labels = []
        orch = OrchClient.instance()
        if orch.available():
            for host in orch.hosts.list():
                labels.extend(host.labels)
        labels.sort()
        return list(set(labels))  # Filter duplicate labels.

    @staticmethod
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

    @staticmethod
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
        device_osd_map = HostService.get_device_osd_map()
        for inventory_host in inventory_hosts:
            host_osds = device_osd_map.get(inventory_host['name'])
            for device in inventory_host['devices']:
                if host_osds:  # pragma: no cover
                    dev_name = os.path.basename(device['path'])
                    device['osd_ids'] = sorted(host_osds.get(dev_name, []))
                else:
                    device['osd_ids'] = []
        
        return inventory_hosts

    @staticmethod
    def identify_device(hostname, device, duration):
        orch = OrchClient.instance()
        TaskManager.current_task().set_progress(0)
        orch.blink_device_light(hostname, device, 'ident', True)
        for i in range(int(duration)):
            percentage = int(round(i / float(duration) * 100))
            TaskManager.current_task().set_progress(percentage)
            time.sleep(1)
        orch.blink_device_light(hostname, device, 'ident', False)
        TaskManager.current_task().set_progress(100)

    @staticmethod
    def list_daemons(hostname: str) -> List[dict]:
        orch = OrchClient.instance()
        daemons = orch.services.list_daemons(hostname=hostname)
        return [d.to_dict() for d in daemons]


