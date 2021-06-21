# -*- coding: utf-8 -*-
from typing import Dict, List, Optional

from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from ..services.ceph_service import CephService
from ..services.exception import handle_orchestrator_error
from ..services.host import HostService
from ..services.orchestrator import OrchFeature
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


@ApiController('/host', Scope.HOSTS)
@ControllerDoc("Get Host Details", "Host")
class Host(RESTController):
    @EndpointDoc("List Host Specifications",
                 parameters={
                     'sources': (str, 'Host Sources'),
                 },
                 responses={200: LIST_HOST_SCHEMA})
    def list(self, sources=None):
        return HostService.get_hosts(sources)

    @raise_if_no_orchestrator([OrchFeature.HOST_LIST, OrchFeature.HOST_CREATE])
    @handle_orchestrator_error('host')
    @host_task('create', {'hostname': '{hostname}'})
    @EndpointDoc('',
                 parameters={
                     'hostname': (str, 'Hostname'),
                     'addr': (str, 'Network Address'),
                     'labels': ([str], 'Host Labels'),
                     'status': (str, 'Host Status')
                 },
                 responses={200: None, 204: None})
    @RESTController.MethodMap(version='0.1')
    @allow_empty_body
    def create(self, hostname: str,
               addr: Optional[str] = None,
               labels: Optional[List[str]] = None,
               status: Optional[str] = None):  # pragma: no cover - requires realtime env
        HostService.add_host(hostname, addr, labels, status)

    @raise_if_no_orchestrator([OrchFeature.HOST_LIST, OrchFeature.HOST_DELETE])
    @handle_orchestrator_error('host')
    @host_task('delete', {'hostname': '{hostname}'})
    @allow_empty_body
    def delete(self, hostname):  # pragma: no cover - requires realtime env
        HostService.delete_host(hostname)

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
        inventory = HostService.get_inventories([hostname], refresh)
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
        HostService.identify_device(hostname, device, duration)

    @RESTController.Resource('GET')
    @raise_if_no_orchestrator([OrchFeature.DAEMON_LIST])
    def daemons(self, hostname: str) -> List[dict]:
        return HostService.list_daemons(hostname)

    @handle_orchestrator_error('host')
    def get(self, hostname: str) -> Dict:
        """
        Get the specified host.
        :raises: cherrypy.HTTPError: If host not found.
        """
        return HostService.get_host(hostname)

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
        HostService.update_host(hostname, update_labels, labels, maintenance, force)


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
        return HostService.get_labels()

    @Endpoint('GET')
    @ReadPermission
    @raise_if_no_orchestrator([OrchFeature.DEVICE_LIST])
    @handle_orchestrator_error('host')
    def inventory(self, refresh=None):
        return HostService.get_inventories(None, refresh)
