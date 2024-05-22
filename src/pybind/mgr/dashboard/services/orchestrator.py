# -*- coding: utf-8 -*-

import logging
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple

from ceph.deployment.service_spec import ServiceSpec
from orchestrator import DaemonDescription, DeviceLightLoc, HostSpec, \
    InventoryFilter, OrchestratorClientMixin, OrchestratorError, OrchResult, \
    ServiceDescription, raise_if_exception

from .. import mgr
from ._paginate import ListPaginator

logger = logging.getLogger('orchestrator')


# pylint: disable=abstract-method
class OrchestratorAPI(OrchestratorClientMixin):
    def __init__(self):
        super(OrchestratorAPI, self).__init__()
        self.set_mgr(mgr)  # type: ignore

    def status(self):
        try:
            status, message, _module_details = super().available()
            logger.info("is orchestrator available: %s, %s", status, message)
            return dict(available=status, message=message)
        except (RuntimeError, OrchestratorError, ImportError) as e:
            return dict(
                available=False,
                message='Orchestrator is unavailable: {}'.format(str(e)))


def wait_api_result(method):
    @wraps(method)
    def inner(self, *args, **kwargs):
        completion = method(self, *args, **kwargs)
        raise_if_exception(completion)
        return completion.result
    return inner


class ResourceManager(object):
    def __init__(self, api):
        self.api = api


class HostManger(ResourceManager):
    @wait_api_result
    def list(self) -> List[HostSpec]:
        return self.api.get_hosts()

    @wait_api_result
    def enter_maintenance(self, hostname: str, force: bool = False):
        return self.api.enter_host_maintenance(hostname, force)

    @wait_api_result
    def exit_maintenance(self, hostname: str):
        return self.api.exit_host_maintenance(hostname)

    def get(self, hostname: str) -> Optional[HostSpec]:
        hosts = [host for host in self.list() if host.hostname == hostname]
        return hosts[0] if hosts else None

    @wait_api_result
    def add(self, hostname: str, addr: str, labels: List[str]):
        return self.api.add_host(HostSpec(hostname, addr=addr, labels=labels))

    @wait_api_result
    def get_facts(self, hostname: Optional[str] = None) -> List[Dict[str, Any]]:
        return self.api.get_facts(hostname)

    @wait_api_result
    def remove(self, hostname: str):
        return self.api.remove_host(hostname)

    @wait_api_result
    def add_label(self, host: str, label: str) -> OrchResult[str]:
        return self.api.add_host_label(host, label)

    @wait_api_result
    def remove_label(self, host: str, label: str) -> OrchResult[str]:
        return self.api.remove_host_label(host, label)

    @wait_api_result
    def drain(self, hostname: str):
        return self.api.drain_host(hostname)


class InventoryManager(ResourceManager):
    @wait_api_result
    def list(self, hosts=None, refresh=False):
        host_filter = InventoryFilter(hosts=hosts) if hosts else None
        return self.api.get_inventory(host_filter=host_filter, refresh=refresh)


class ServiceManager(ResourceManager):
    def list(self,
             service_type: Optional[str] = None,
             service_name: Optional[str] = None,
             offset: int = 0, limit: int = -1,
             sort: str = '+service_name', search: str = '') -> Tuple[List[Dict[Any, Any]], int]:
        services = self.api.describe_service(service_type, service_name)
        services = [service.to_dict() for service in services.result]
        paginator = ListPaginator(offset, limit, sort, search,
                                  input_list=services,
                                  searchable_params=['service_name', 'status.running',
                                                     'status.last_refreshed', 'status.size'],
                                  sortable_params=['service_name', 'status.running',
                                                   'status.last_refreshed', 'status.size'],
                                  default_sort='+service_name')
        return list(paginator.list()), paginator.get_count()

    @wait_api_result
    def get(self, service_name: str) -> ServiceDescription:
        return self.api.describe_service(None, service_name)

    @wait_api_result
    def list_daemons(self,
                     service_name: Optional[str] = None,
                     daemon_type: Optional[str] = None,
                     hostname: Optional[str] = None) -> List[DaemonDescription]:
        return self.api.list_daemons(service_name=service_name,
                                     daemon_type=daemon_type,
                                     host=hostname)

    def reload(self, service_type, service_ids):
        if not isinstance(service_ids, list):
            service_ids = [service_ids]

        completion_list = [
            self.api.service_action('reload', service_type, service_name,
                                    service_id)
            for service_name, service_id in service_ids
        ]
        self.api.orchestrator_wait(completion_list)
        for c in completion_list:
            raise_if_exception(c)

    @wait_api_result
    def apply(self,
              service_spec: Dict,
              no_overwrite: Optional[bool] = False) -> OrchResult[List[str]]:
        spec = ServiceSpec.from_json(service_spec)
        return self.api.apply([spec], no_overwrite)

    @wait_api_result
    def remove(self, service_name: str) -> List[str]:
        return self.api.remove_service(service_name)


class OsdManager(ResourceManager):
    @wait_api_result
    def create(self, drive_group_specs):
        return self.api.apply_drivegroups(drive_group_specs)

    @wait_api_result
    def remove(self, osd_ids, replace=False, force=False):
        return self.api.remove_osds(osd_ids, replace, force)

    @wait_api_result
    def removing_status(self):
        return self.api.remove_osds_status()


class DaemonManager(ResourceManager):
    @wait_api_result
    def action(self, daemon_name='', action='', image=None):
        return self.api.daemon_action(daemon_name=daemon_name, action=action, image=image)


class UpgradeManager(ResourceManager):
    @wait_api_result
    def list(self, image: Optional[str], tags: bool,
             show_all_versions: Optional[bool]) -> Dict[Any, Any]:
        return self.api.upgrade_ls(image, tags, show_all_versions)

    @wait_api_result
    def status(self):
        return self.api.upgrade_status()

    @wait_api_result
    def start(self, image: str, version: str, daemon_types: Optional[List[str]] = None,
              host_placement: Optional[str] = None, services: Optional[List[str]] = None,
              limit: Optional[int] = None) -> str:
        return self.api.upgrade_start(image, version, daemon_types, host_placement, services,
                                      limit)

    @wait_api_result
    def pause(self) -> str:
        return self.api.upgrade_pause()

    @wait_api_result
    def resume(self) -> str:
        return self.api.upgrade_resume()

    @wait_api_result
    def stop(self) -> str:
        return self.api.upgrade_stop()


class HardwareManager(ResourceManager):

    @wait_api_result
    def common(self, category: str, hostname: Optional[List[str]] = None) -> str:
        return self.api.node_proxy_common(category, hostname=hostname)


class OrchClient(object):

    _instance = None

    @classmethod
    def instance(cls):
        # type: () -> OrchClient
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self.api = OrchestratorAPI()

        self.hosts = HostManger(self.api)
        self.inventory = InventoryManager(self.api)
        self.services = ServiceManager(self.api)
        self.osds = OsdManager(self.api)
        self.daemons = DaemonManager(self.api)
        self.upgrades = UpgradeManager(self.api)
        self.hardware = HardwareManager(self.api)

    def available(self, features: Optional[List[str]] = None) -> bool:
        available = self.status()['available']
        if available and features is not None:
            return not self.get_missing_features(features)
        return available

    def status(self) -> Dict[str, Any]:
        status = self.api.status()
        status['features'] = {}
        if status['available']:
            status['features'] = self.api.get_feature_set()
        return status

    def get_missing_features(self, features: List[str]) -> List[str]:
        supported_features = {k for k, v in self.api.get_feature_set().items() if v['available']}
        return list(set(features) - supported_features)

    @wait_api_result
    def blink_device_light(self, hostname, device, ident_fault, on):
        # type: (str, str, str, bool) -> OrchResult[List[str]]
        return self.api.blink_device_light(
            ident_fault, on, [DeviceLightLoc(hostname, device, device)])


class OrchFeature(object):
    HOST_LIST = 'get_hosts'
    HOST_ADD = 'add_host'
    HOST_REMOVE = 'remove_host'
    HOST_LABEL_ADD = 'add_host_label'
    HOST_LABEL_REMOVE = 'remove_host_label'
    HOST_MAINTENANCE_ENTER = 'enter_host_maintenance'
    HOST_MAINTENANCE_EXIT = 'exit_host_maintenance'
    HOST_DRAIN = 'drain_host'

    SERVICE_LIST = 'describe_service'
    SERVICE_CREATE = 'apply'
    SERVICE_EDIT = 'apply'
    SERVICE_DELETE = 'remove_service'
    SERVICE_RELOAD = 'service_action'
    DAEMON_LIST = 'list_daemons'

    OSD_GET_REMOVE_STATUS = 'remove_osds_status'

    OSD_CREATE = 'apply_drivegroups'
    OSD_DELETE = 'remove_osds'

    DEVICE_LIST = 'get_inventory'
    DEVICE_BLINK_LIGHT = 'blink_device_light'

    DAEMON_ACTION = 'daemon_action'

    UPGRADE_LIST = 'upgrade_ls'
    UPGRADE_STATUS = 'upgrade_status'
    UPGRADE_START = 'upgrade_start'
    UPGRADE_PAUSE = 'upgrade_pause'
    UPGRADE_RESUME = 'upgrade_resume'
    UPGRADE_STOP = 'upgrade_stop'
