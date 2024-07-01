"""
This module wrap's Rook + Kubernetes APIs to expose the calls
needed to implement an orchestrator module.  While the orchestrator
module exposes an async API, this module simply exposes blocking API
call methods.

This module is runnable outside of ceph-mgr, useful for testing.
"""
import datetime
import threading
import logging
from contextlib import contextmanager
from time import sleep
import re
from orchestrator import OrchResult

import jsonpatch
from urllib.parse import urljoin
import json

# Optional kubernetes imports to enable MgrModule.can_run
# to behave cleanly.
from urllib3.exceptions import ProtocolError

from ceph.deployment.inventory import Device
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.service_spec import (
    ServiceSpec,
    NFSServiceSpec,
    RGWSpec,
    PlacementSpec,
    HostPlacementSpec,
    HostPattern,
)
from ceph.utils import datetime_now
from ceph.deployment.drive_selection.matchers import SizeMatcher
from nfs.cluster import create_ganesha_pool
from nfs.module import Module
from nfs.export import NFSRados
from mgr_module import NFS_POOL_NAME
from mgr_util import merge_dicts

from typing import Optional, Tuple, TypeVar, List, Callable, Any, cast, Generic, \
    Iterable, Dict, Iterator, Type

try:
    from kubernetes import client, watch
    from kubernetes.client.rest import ApiException
except ImportError:
    class ApiException(Exception):  # type: ignore
        status = 0

from .rook_client.ceph import cephfilesystem as cfs
from .rook_client.ceph import cephnfs as cnfs
from .rook_client.ceph import cephobjectstore as cos
from .rook_client.ceph import cephcluster as ccl
from .rook_client.ceph import cephrbdmirror as crbdm
from .rook_client._helper import CrdClass

import orchestrator

try:
    from rook.module import RookEnv, RookOrchestrator
except ImportError:
    pass  # just used for type checking.


T = TypeVar('T')
FuncT = TypeVar('FuncT', bound=Callable)

CrdClassT = TypeVar('CrdClassT', bound=CrdClass)


log = logging.getLogger(__name__)


def __urllib3_supports_read_chunked() -> bool:
    # There is a bug in CentOS 7 as it ships a urllib3 which is lower
    # than required by kubernetes-client
    try:
        from urllib3.response import HTTPResponse
        return hasattr(HTTPResponse, 'read_chunked')
    except ImportError:
        return False


_urllib3_supports_read_chunked = __urllib3_supports_read_chunked()

class ApplyException(orchestrator.OrchestratorError):
    """
    For failures to update the Rook CRDs, usually indicating
    some kind of interference between our attempted update
    and other conflicting activity.
    """


def threaded(f: Callable[..., None]) -> Callable[..., threading.Thread]:
    def wrapper(*args: Any, **kwargs: Any) -> threading.Thread:
        t = threading.Thread(target=f, args=args, kwargs=kwargs)
        t.start()
        return t

    return cast(Callable[..., threading.Thread], wrapper)


class DefaultFetcher():
    def __init__(self, storage_class: str, coreV1_api: 'client.CoreV1Api'):
        self.storage_class = storage_class
        self.coreV1_api = coreV1_api

    def fetch(self) -> None:
        self.inventory: KubernetesResource[client.V1PersistentVolumeList] = KubernetesResource(self.coreV1_api.list_persistent_volume)
        self.pvs_in_sc = [i for i in self.inventory.items if i.spec.storage_class_name == self.storage_class]

    def convert_size(self, size_str: str) -> int:
        units = ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "", "K", "M", "G", "T", "P", "E")
        coeff_and_unit = re.search('(\d+)(\D+)', size_str)
        assert coeff_and_unit is not None
        coeff = int(coeff_and_unit[1])
        unit = coeff_and_unit[2]
        try: 
            factor = units.index(unit) % 7
        except ValueError:
            log.error("PV size format invalid")
            raise
        size = coeff * (2 ** (10 * factor))
        return size

    def devices(self) -> Dict[str, List[Device]]:
        nodename_to_devices: Dict[str, List[Device]] = {}
        for i in self.pvs_in_sc:
            node, device = self.device(i)
            if node not in nodename_to_devices:
                nodename_to_devices[node] = []
            nodename_to_devices[node].append(device)
        return nodename_to_devices

    def device(self, i: 'client.V1PersistentVolume') -> Tuple[str, Device]:
        node = 'N/A'
        if i.spec.node_affinity:
            terms = i.spec.node_affinity.required.node_selector_terms
            if len(terms) == 1 and len(terms[0].match_expressions) == 1 and terms[0].match_expressions[0].key == 'kubernetes.io/hostname' and len(terms[0].match_expressions[0].values) == 1:
                node = terms[0].match_expressions[0].values[0]
        size = self.convert_size(i.spec.capacity['storage'])
        path = i.spec.host_path.path if i.spec.host_path else i.spec.local.path if i.spec.local else ('/dev/' + i.metadata.annotations['storage.openshift.com/device-name']) if i.metadata.annotations and 'storage.openshift.com/device-name' in i.metadata.annotations else ''
        state = i.spec.volume_mode == 'Block' and i.status.phase == 'Available'
        pv_name = i.metadata.name
        device = Device(
                path = path,
                sys_api = dict(
                    size = size,
                    node = node,
                    pv_name = pv_name
                ),
                available = state,
        )
        return (node, device)
        

class LSOFetcher(DefaultFetcher):
    def __init__(self, storage_class: 'str', coreV1_api: 'client.CoreV1Api', customObjects_api: 'client.CustomObjectsApi', nodenames: 'Optional[List[str]]' = None):
        super().__init__(storage_class, coreV1_api)
        self.customObjects_api = customObjects_api
        self.nodenames = nodenames

    def fetch(self) -> None:
        super().fetch()
        self.discovery: KubernetesCustomResource = KubernetesCustomResource(self.customObjects_api.list_cluster_custom_object,
                                                 group="local.storage.openshift.io",
                                                 version="v1alpha1",
                                                 plural="localvolumediscoveryresults")

    def predicate(self, item: 'client.V1ConfigMapList') -> bool:
            if self.nodenames is not None:
                return item['spec']['nodeName'] in self.nodenames
            else:
                return True

    def devices(self) -> Dict[str, List[Device]]:
        try:
            lso_discovery_results = [i for i in self.discovery.items if self.predicate(i)]
        except ApiException as dummy_e:
            log.error("Failed to fetch device metadata")
            raise
        self.lso_devices = {}
        for i in lso_discovery_results:
            drives = i['status']['discoveredDevices']
            for drive in drives:
                self.lso_devices[drive['deviceID'].split('/')[-1]] = drive
        nodename_to_devices: Dict[str, List[Device]] = {}
        for i in self.pvs_in_sc:
            node, device = (None, None)
            if (not i.metadata.annotations) or ('storage.openshift.com/device-id' not in i.metadata.annotations) or (i.metadata.annotations['storage.openshift.com/device-id'] not in self.lso_devices):
                node, device = super().device(i)
            else:
                node, device = self.device(i)
            if node not in nodename_to_devices:
                nodename_to_devices[node] = []
            nodename_to_devices[node].append(device)
        return nodename_to_devices
            
    def device(self, i: Any) -> Tuple[str, Device]:
        node = i.metadata.labels['kubernetes.io/hostname']
        device_discovery = self.lso_devices[i.metadata.annotations['storage.openshift.com/device-id']]
        pv_name = i.metadata.name
        vendor: str = device_discovery['model'].split()[0] if len(device_discovery['model'].split()) >= 1 else ''
        model: str = ' '.join(device_discovery['model'].split()[1:]) if len(device_discovery['model'].split()) > 1 else ''
        device = Device(
            path = device_discovery['path'],
            sys_api = dict(
                    size = device_discovery['size'],
                    rotational = '1' if device_discovery['property']=='Rotational' else '0',
                    node = node,
                    pv_name = pv_name,
                    model = model,
                    vendor = vendor
                ),
            available = device_discovery['status']['state']=='Available',
            device_id = device_discovery['deviceID'].split('/')[-1],
            lsm_data = dict(
                serialNum = device_discovery['serial']
            )
        )
        return (node, device)


class PDFetcher(DefaultFetcher):
    """ Physical Devices Fetcher"""
    def __init__(self, coreV1_api: 'client.CoreV1Api'):
        self.coreV1_api = coreV1_api

    def fetch(self) -> None:
        """ Collect the devices information from k8s configmaps"""
        self.dev_cms: KubernetesResource = KubernetesResource(self.coreV1_api.list_namespaced_config_map,
                                                              namespace='rook-ceph',
                                                              label_selector='app=rook-discover')

    def devices(self) -> Dict[str, List[Device]]:
        """ Return the list of devices found"""
        node_devices: Dict[str, List[Device]] = {}
        for i in self.dev_cms.items:
            devices_list: List[Device] = []
            for d in json.loads(i.data['devices']):
                devices_list.append(self.device(d)[1])
            node_devices[i.metadata.labels['rook.io/node']] = devices_list

        return node_devices

    def device(self, devData: Dict[str,str]) -> Tuple[str, Device]:
        """ Build an orchestrator device """
        if 'cephVolumeData' in devData and devData['cephVolumeData']:
            return "", Device.from_json(json.loads(devData['cephVolumeData']))
        else:
            return "", Device(
                path='/dev/' + devData['name'],
                sys_api=dict(
                    rotational='1' if devData['rotational'] else '0',
                    size=devData['size']
                ),
                available=False,
                rejected_reasons=['device data coming from ceph-volume not provided'],
            )


class KubernetesResource(Generic[T]):
    def __init__(self, api_func: Callable, **kwargs: Any) -> None:
        """
        Generic kubernetes Resource parent class

        The api fetch and watch methods should be common across resource types,

        Exceptions in the runner thread are propagated to the caller.

        :param api_func: kubernetes client api function that is passed to the watcher
        :param filter_func: signature: ``(Item) -> bool``.
        """
        self.kwargs = kwargs
        self.api_func = api_func

        # ``_items`` is accessed by different threads. I assume assignment is atomic.
        self._items: Dict[str, T] = dict()
        self.thread = None  # type: Optional[threading.Thread]
        self.exception: Optional[Exception] = None
        if not _urllib3_supports_read_chunked:
            logging.info('urllib3 is too old. Fallback to full fetches')

    def _fetch(self) -> str:
        """ Execute the requested api method as a one-off fetch"""
        response = self.api_func(**self.kwargs)
        metadata = response.metadata
        self._items = {item.metadata.name: item for item in response.items}
        log.info('Full fetch of {}. result: {}'.format(self.api_func, len(self._items)))
        return metadata.resource_version

    @property
    def items(self) -> Iterable[T]:
        """
        Returns the items of the request.
        Creates the watcher as a side effect.
        :return:
        """
        if self.exception:
            e = self.exception
            self.exception = None
            raise e  # Propagate the exception to the user.
        if not self.thread or not self.thread.is_alive():
            resource_version = self._fetch()
            if _urllib3_supports_read_chunked:
                # Start a thread which will use the kubernetes watch client against a resource
                log.debug("Attaching resource watcher for k8s {}".format(self.api_func))
                self.thread = self._watch(resource_version)

        return self._items.values()

    def get_item_name(self, item: Any) -> Any:
        try:
            return item.metadata.name
        except AttributeError:
                    raise AttributeError(
                        "{} doesn't contain a metadata.name. Unable to track changes".format(
                            self.api_func)) 
    @threaded
    def _watch(self, res_ver: Optional[str]) -> None:
        """ worker thread that runs the kubernetes watch """

        self.exception = None

        w = watch.Watch()

        try:
            # execute generator to continually watch resource for changes
            for event in w.stream(self.api_func, resource_version=res_ver, watch=True,
                                  **self.kwargs):
                self.health = ''
                item = event['object']
                name = self.get_item_name(item)

                log.info('{} event: {}'.format(event['type'], name))

                if event['type'] in ('ADDED', 'MODIFIED'):
                    self._items = merge_dicts(self._items, {name: item})
                elif event['type'] == 'DELETED':
                    self._items = {k:v for k,v in self._items.items() if k != name}
                elif event['type'] == 'BOOKMARK':
                    pass
                elif event['type'] == 'ERROR':
                    raise ApiException(str(event))
                else:
                    raise KeyError('Unknown watch event {}'.format(event['type']))
        except ProtocolError as e:
            if 'Connection broken' in str(e):
                log.info('Connection reset.')
                return
            raise
        except ApiException as e:
            log.exception('K8s API failed. {}'.format(self.api_func))
            self.exception = e
            raise
        except Exception as e:
            log.exception("Watcher failed. ({})".format(self.api_func))
            self.exception = e
            raise

class KubernetesCustomResource(KubernetesResource):
    def _fetch(self) -> str:
        response = self.api_func(**self.kwargs)
        metadata = response['metadata']
        self._items = {item['metadata']['name']: item for item in response['items']}
        log.info('Full fetch of {}. result: {}'.format(self.api_func, len(self._items)))
        return metadata['resourceVersion']

    def get_item_name(self, item: Any) -> Any:
        try:
            return item['metadata']['name']
        except AttributeError:
                    raise AttributeError(
                        "{} doesn't contain a metadata.name. Unable to track changes".format(
                            self.api_func))

class DefaultCreator():
    def __init__(self, inventory: 'Dict[str, List[Device]]', coreV1_api: 'client.CoreV1Api', storage_class: 'str'):
        self.coreV1_api = coreV1_api
        self.storage_class = storage_class
        self.inventory = inventory

    def device_to_device_set(self, drive_group: DriveGroupSpec, d: Device) -> ccl.StorageClassDeviceSetsItem:
        device_set = ccl.StorageClassDeviceSetsItem(
                    name=d.sys_api['pv_name'],
                    volumeClaimTemplates= ccl.VolumeClaimTemplatesList(),
                    count=1,
                    encrypted=drive_group.encrypted,
                    portable=False
                )
        device_set.volumeClaimTemplates.append(
            ccl.VolumeClaimTemplatesItem(
                metadata=ccl.Metadata(
                    name="data"
                ),
                spec=ccl.Spec(
                    storageClassName=self.storage_class,
                    volumeMode="Block",
                    accessModes=ccl.CrdObjectList(["ReadWriteOnce"]),
                    resources={
                        "requests":{
                                "storage": 1
                        }
                    },
                    volumeName=d.sys_api['pv_name']
                )
            )
        )
        return device_set

    def filter_devices(self, rook_pods: KubernetesResource, drive_group: DriveGroupSpec, matching_hosts: List[str]) -> List[Device]:
        device_list = []
        assert drive_group.data_devices is not None
        sizematcher: Optional[SizeMatcher] = None
        if drive_group.data_devices.size:
            sizematcher = SizeMatcher('size', drive_group.data_devices.size)
        limit = getattr(drive_group.data_devices, 'limit', None)
        count = 0
        all = getattr(drive_group.data_devices, 'all', None)
        paths = [device.path for device in drive_group.data_devices.paths]
        osd_list = []
        for pod in rook_pods.items:
            if (
                hasattr(pod, 'metadata') 
                and hasattr(pod.metadata, 'labels') 
                and 'osd' in pod.metadata.labels 
                and 'ceph.rook.io/DeviceSet' in pod.metadata.labels
            ):
                osd_list.append(pod.metadata.labels['ceph.rook.io/DeviceSet'])
        for _, node in self.inventory.items():
            for device in node:
                if device.sys_api['pv_name'] in osd_list:
                    count += 1
        for _, node in self.inventory.items():
            for device in node:
                if not limit or (count < limit):
                    if device.available:
                        if (
                            all 
                            or (
                                device.sys_api['node'] in matching_hosts
                                and ((sizematcher != None) or sizematcher.compare(device))
                                and (
                                    not drive_group.data_devices.paths
                                    or (device.path in paths)
                                )
                            )
                        ):
                            device_list.append(device)
                            count += 1
        
        return device_list

    def add_osds(self, rook_pods: KubernetesResource, drive_group: DriveGroupSpec, matching_hosts: List[str]) -> Any:
        to_create = self.filter_devices(rook_pods, drive_group,matching_hosts)
        assert drive_group.data_devices is not None
        def _add_osds(current_cluster, new_cluster):
            # type: (ccl.CephCluster, ccl.CephCluster) -> ccl.CephCluster
            if not hasattr(new_cluster.spec, 'storage') or not new_cluster.spec.storage:
                new_cluster.spec.storage = ccl.Storage()

            if not hasattr(new_cluster.spec.storage, 'storageClassDeviceSets') or not new_cluster.spec.storage.storageClassDeviceSets:
                new_cluster.spec.storage.storageClassDeviceSets = ccl.StorageClassDeviceSetsList()

            existing_scds = [
                scds.name for scds in new_cluster.spec.storage.storageClassDeviceSets
            ]
            for device in to_create:
                new_scds = self.device_to_device_set(drive_group, device)
                if new_scds.name not in existing_scds:
                    new_cluster.spec.storage.storageClassDeviceSets.append(new_scds)
            return new_cluster
        return _add_osds

class LSOCreator(DefaultCreator):
    def filter_devices(self, rook_pods: KubernetesResource, drive_group: DriveGroupSpec, matching_hosts: List[str]) -> List[Device]:
        device_list = []
        assert drive_group.data_devices is not None
        sizematcher = None
        if drive_group.data_devices.size:
            sizematcher = SizeMatcher('size', drive_group.data_devices.size)
        limit = getattr(drive_group.data_devices, 'limit', None)
        all = getattr(drive_group.data_devices, 'all', None)
        paths = [device.path for device in drive_group.data_devices.paths]
        vendor = getattr(drive_group.data_devices, 'vendor', None)
        model = getattr(drive_group.data_devices, 'model', None)
        count = 0
        osd_list = []
        for pod in rook_pods.items:
            if (
                hasattr(pod, 'metadata') 
                and hasattr(pod.metadata, 'labels') 
                and 'osd' in pod.metadata.labels 
                and 'ceph.rook.io/DeviceSet' in pod.metadata.labels
            ):
                osd_list.append(pod.metadata.labels['ceph.rook.io/DeviceSet'])
        for _, node in self.inventory.items():
            for device in node:
                if device.sys_api['pv_name'] in osd_list:
                    count += 1
        for _, node in self.inventory.items():
            for device in node:
                if not limit or (count < limit):
                    if device.available:
                        if (
                            all 
                            or (
                                device.sys_api['node'] in matching_hosts
                                and ((sizematcher != None) or sizematcher.compare(device))
                                and (
                                    not drive_group.data_devices.paths
                                    or device.path in paths
                                ) 
                                and (
                                    not vendor 
                                    or device.sys_api['vendor'] == vendor
                                )
                                and (
                                    not model 
                                    or device.sys_api['model'].startsWith(model)
                                )
                            )
                        ):
                            device_list.append(device)
                            count += 1
        return device_list

class DefaultRemover():
    def __init__(
        self,
        coreV1_api: 'client.CoreV1Api', 
        batchV1_api: 'client.BatchV1Api', 
        appsV1_api: 'client.AppsV1Api', 
        osd_ids: List[str], 
        replace_flag: bool, 
        force_flag: bool, 
        mon_command: Callable, 
        patch: Callable, 
        rook_env: 'RookEnv',
        inventory: Dict[str, List[Device]]
    ):
        self.batchV1_api = batchV1_api
        self.appsV1_api = appsV1_api
        self.coreV1_api = coreV1_api

        self.osd_ids = osd_ids
        self.replace_flag = replace_flag
        self.force_flag = force_flag

        self.mon_command = mon_command

        self.patch = patch
        self.rook_env = rook_env

        self.inventory = inventory
        self.osd_pods: KubernetesResource = KubernetesResource(self.coreV1_api.list_namespaced_pod, namespace='rook-ceph', label_selector='app=rook-ceph-osd')
        self.jobs: KubernetesResource = KubernetesResource(self.batchV1_api.list_namespaced_job, namespace='rook-ceph', label_selector='app=rook-ceph-osd-prepare')
        self.pvcs: KubernetesResource = KubernetesResource(self.coreV1_api.list_namespaced_persistent_volume_claim, namespace='rook-ceph')


    def remove_device_sets(self) -> str:
        self.to_remove: Dict[str, int] = {}
        self.pvc_to_remove: List[str] = []
        for pod in self.osd_pods.items:
            if (
                hasattr(pod, 'metadata') 
                and hasattr(pod.metadata, 'labels') 
                and 'osd' in pod.metadata.labels 
                and pod.metadata.labels['osd'] in self.osd_ids
            ):
                if pod.metadata.labels['ceph.rook.io/DeviceSet'] in self.to_remove:
                    self.to_remove[pod.metadata.labels['ceph.rook.io/DeviceSet']] = self.to_remove[pod.metadata.labels['ceph.rook.io/DeviceSet']] + 1
                else:
                    self.to_remove[pod.metadata.labels['ceph.rook.io/DeviceSet']] = 1
                self.pvc_to_remove.append(pod.metadata.labels['ceph.rook.io/pvc'])
        def _remove_osds(current_cluster, new_cluster):
            # type: (ccl.CephCluster, ccl.CephCluster) -> ccl.CephCluster
            assert new_cluster.spec.storage is not None and new_cluster.spec.storage.storageClassDeviceSets is not None
            for _set in new_cluster.spec.storage.storageClassDeviceSets:
                    if _set.name in self.to_remove:
                        if _set.count == self.to_remove[_set.name]:
                            new_cluster.spec.storage.storageClassDeviceSets.remove(_set)
                        else:
                            _set.count = _set.count - self.to_remove[_set.name]
            return new_cluster
        return self.patch(ccl.CephCluster, 'cephclusters', self.rook_env.cluster_name, _remove_osds)

    def check_force(self) -> None:
        if not self.force_flag:
            safe_args = {'prefix': 'osd safe-to-destroy',
                        'ids': [str(x) for x in self.osd_ids]}
            ret, out, err = self.mon_command(safe_args)
            if ret != 0:
                raise RuntimeError(err)

    def set_osds_down(self) -> None:
        down_flag_args = {
            'prefix': 'osd down',
            'ids': [str(x) for x in self.osd_ids]
        }
        ret, out, err = self.mon_command(down_flag_args)
        if ret != 0:
            raise RuntimeError(err)

    def scale_deployments(self) -> None:
        for osd_id in self.osd_ids:
            self.appsV1_api.patch_namespaced_deployment_scale(namespace='rook-ceph', name='rook-ceph-osd-{}'.format(osd_id), body=client.V1Scale(
                spec=client.V1ScaleSpec(
                    replicas=0
                )
            ))

    def set_osds_out(self) -> None:
        out_flag_args = {
            'prefix': 'osd out',
            'ids': [str(x) for x in self.osd_ids]
        }
        ret, out, err = self.mon_command(out_flag_args)
        if ret != 0:
            raise RuntimeError(err)
            
    def delete_deployments(self) -> None:
        for osd_id in self.osd_ids:
            self.appsV1_api.delete_namespaced_deployment(namespace='rook-ceph', name='rook-ceph-osd-{}'.format(osd_id), propagation_policy='Foreground')

    def clean_up_prepare_jobs_and_pvc(self) -> None:
        for job in self.jobs.items:
            if job.metadata.labels['ceph.rook.io/pvc'] in self.pvc_to_remove:
                self.batchV1_api.delete_namespaced_job(name=job.metadata.name, namespace='rook-ceph', propagation_policy='Foreground')
                self.coreV1_api.delete_namespaced_persistent_volume_claim(name=job.metadata.labels['ceph.rook.io/pvc'], namespace='rook-ceph', propagation_policy='Foreground')

    def purge_osds(self) -> None:
        for id in self.osd_ids:
            purge_args = {
                'prefix': 'osd purge-actual',
                'id': int(id),
                'yes_i_really_mean_it': True
            }
            ret, out, err = self.mon_command(purge_args)
            if ret != 0:
                raise RuntimeError(err)

    def destroy_osds(self) -> None:
        for id in self.osd_ids:
            destroy_args = {
                'prefix': 'osd destroy-actual',
                'id': int(id),
                'yes_i_really_mean_it': True
            }
            ret, out, err = self.mon_command(destroy_args)
            if ret != 0:
                raise RuntimeError(err)

    def remove(self) -> str:
        try:
            self.check_force()
        except Exception as e:
            log.exception("Error checking if OSDs are safe to destroy")
            return f"OSDs not safe to destroy or unable to check if they are safe to destroy: {e}"
        try:
            remove_result = self.remove_device_sets()
        except Exception as e:
            log.exception("Error patching ceph cluster CRD")
            return f"Not possible to modify Ceph cluster CRD: {e}"
        try:
            self.scale_deployments()
            self.delete_deployments()
            self.clean_up_prepare_jobs_and_pvc()
        except Exception as e:
            log.exception("Ceph cluster CRD patched, but error cleaning environment")
            return f"Error cleaning environment after removing OSDs from Ceph cluster CRD: {e}"
        try:
            self.set_osds_down()
            self.set_osds_out()
            if self.replace_flag:
                self.destroy_osds()
            else:
                self.purge_osds()
        except Exception as e:
            log.exception("OSDs removed from environment, but not able to remove OSDs from Ceph cluster")
            return f"Error removing OSDs from Ceph cluster: {e}"

        return remove_result



class RookCluster(object):
    # import of client.CoreV1Api must be optional at import time.
    # Instead allow mgr/rook to be imported anyway.
    def __init__(
        self,
        coreV1_api: 'client.CoreV1Api',
        batchV1_api: 'client.BatchV1Api',
        customObjects_api: 'client.CustomObjectsApi',
        storageV1_api: 'client.StorageV1Api',
        appsV1_api: 'client.AppsV1Api',
        rook_env: 'RookEnv',
        storage_class: 'str'
    ):
        self.rook_env = rook_env  # type: RookEnv
        self.coreV1_api = coreV1_api  # client.CoreV1Api
        self.batchV1_api = batchV1_api
        self.customObjects_api = customObjects_api
        self.storageV1_api = storageV1_api  # client.StorageV1Api
        self.appsV1_api = appsV1_api  # client.AppsV1Api
        self.storage_class = storage_class # type: str

        #  TODO: replace direct k8s calls with Rook API calls
        self.storage_classes : KubernetesResource = KubernetesResource(self.storageV1_api.list_storage_class)

        self.rook_pods: KubernetesResource[client.V1Pod] = KubernetesResource(self.coreV1_api.list_namespaced_pod,
                                            namespace=self.rook_env.namespace,
                                            label_selector="rook_cluster={0}".format(
                                                self.rook_env.namespace))
        self.nodes: KubernetesResource[client.V1Node] = KubernetesResource(self.coreV1_api.list_node)

    def rook_url(self, path: str) -> str:
        prefix = "/apis/ceph.rook.io/%s/namespaces/%s/" % (
            self.rook_env.crd_version, self.rook_env.namespace)
        return urljoin(prefix, path)

    def rook_api_call(self, verb: str, path: str, **kwargs: Any) -> Any:
        full_path = self.rook_url(path)
        log.debug("[%s] %s" % (verb, full_path))

        return self.coreV1_api.api_client.call_api(
            full_path,
            verb,
            auth_settings=['BearerToken'],
            response_type="object",
            _return_http_data_only=True,
            _preload_content=True,
            **kwargs)

    def rook_api_get(self, path: str, **kwargs: Any) -> Any:
        return self.rook_api_call("GET", path, **kwargs)

    def rook_api_delete(self, path: str) -> Any:
        return self.rook_api_call("DELETE", path)

    def rook_api_patch(self, path: str, **kwargs: Any) -> Any:
        return self.rook_api_call("PATCH", path,
                                  header_params={"Content-Type": "application/json-patch+json"},
                                  **kwargs)

    def rook_api_post(self, path: str, **kwargs: Any) -> Any:
        return self.rook_api_call("POST", path, **kwargs)

    def get_storage_class(self) -> 'client.V1StorageClass':
        matching_sc = [i for i in self.storage_classes.items if self.storage_class == i.metadata.name]
        if len(matching_sc) == 0:
            log.error(f"No storage class exists matching configured Rook orchestrator storage class which currently is <{self.storage_class}>. This storage class can be set in ceph config (mgr/rook/storage_class)")
            raise Exception('No storage class exists matching name provided in ceph config at mgr/rook/storage_class')
        return matching_sc[0]

    def get_discovered_devices(self, nodenames: Optional[List[str]] = None) -> Dict[str, List[Device]]:
        self.fetcher: Optional[DefaultFetcher] = None
        op_settings = self.coreV1_api.read_namespaced_config_map(name="rook-ceph-operator-config", namespace='rook-ceph').data
        if op_settings.get('ROOK_ENABLE_DISCOVERY_DAEMON', 'false').lower() == 'true':
            self.fetcher = PDFetcher(self.coreV1_api)
        else:
            storage_class = self.get_storage_class()
            if storage_class.metadata.labels and ('local.storage.openshift.io/owner-name' in storage_class.metadata.labels):
                self.fetcher = LSOFetcher(self.storage_class, self.coreV1_api, self.customObjects_api, nodenames)
            else:
                self.fetcher = DefaultFetcher(self.storage_class, self.coreV1_api)

        self.fetcher.fetch()
        return self.fetcher.devices()

    def get_osds(self) -> List:
        osd_pods: KubernetesResource = KubernetesResource(self.coreV1_api.list_namespaced_pod, namespace='rook-ceph', label_selector='app=rook-ceph-osd')
        return list(osd_pods.items)
        
    def get_nfs_conf_url(self, nfs_cluster: str, instance: str) -> Optional[str]:
        #
        # Fetch cephnfs object for "nfs_cluster" and then return a rados://
        # URL for the instance within that cluster. If the fetch fails, just
        # return None.
        #
        try:
            ceph_nfs = self.rook_api_get("cephnfses/{0}".format(nfs_cluster))
        except ApiException as e:
            log.info("Unable to fetch cephnfs object: {}".format(e.status))
            return None

        pool = ceph_nfs['spec']['rados']['pool']
        namespace = ceph_nfs['spec']['rados'].get('namespace', None)

        if namespace == None:
            url = "rados://{0}/conf-{1}.{2}".format(pool, nfs_cluster, instance)
        else:
            url = "rados://{0}/{1}/conf-{2}.{3}".format(pool, namespace, nfs_cluster, instance)
        return url

    def describe_pods(self,
                      service_type: Optional[str],
                      service_id: Optional[str],
                      nodename: Optional[str]) -> List[Dict[str, Any]]:
        """
        Go query the k8s API about deployment, containers related to this
        filesystem

        Example Rook Pod labels for a mgr daemon:
        Labels:         app=rook-ceph-mgr
                        pod-template-hash=2171958073
                        rook_cluster=rook
        And MDS containers additionally have `rook_filesystem` label

        Label filter is rook_cluster=<cluster namespace>
                        rook_file_system=<self.fs_name>
        """
        def predicate(item):
            # type: (client.V1Pod) -> bool
            metadata = item.metadata
            if service_type is not None:
                if metadata.labels['app'] != "rook-ceph-{0}".format(service_type):
                    return False

                if service_id is not None:
                    try:
                        k, v = {
                            "mds": ("rook_file_system", service_id),
                            "osd": ("ceph-osd-id", service_id),
                            "mon": ("mon", service_id),
                            "mgr": ("mgr", service_id),
                            "nfs": ("nfs", service_id),
                            "rgw": ("ceph_rgw", service_id),
                        }[service_type]
                    except KeyError:
                        raise orchestrator.OrchestratorValidationError(
                            '{} not supported'.format(service_type))
                    if metadata.labels[k] != v:
                        return False

            if nodename is not None:
                if item.spec.node_name != nodename:
                    return False
            return True

        refreshed = datetime_now()
        pods = [i for i in self.rook_pods.items if predicate(i)]

        pods_summary = []
        prefix = 'sha256:'

        for p in pods:
            d = p.to_dict()

            image_name = None
            for c in d['spec']['containers']:
                # look at the first listed container in the pod...
                image_name = c['image']
                break

            ls = d['status'].get('container_statuses')
            if not ls:
                # ignore pods with no containers
                continue
            image_id = ls[0]['image_id']
            image_id = image_id.split(prefix)[1] if prefix in image_id else image_id

            s = {
                "name": d['metadata']['name'],
                "hostname": d['spec']['node_name'],
                "labels": d['metadata']['labels'],
                'phase': d['status']['phase'],
                'container_image_name': image_name,
                'container_image_id': image_id,
                'refreshed': refreshed,
                # these may get set below...
                'started': None,
                'created': None,
            }

            # note: we want UTC
            if d['metadata'].get('creation_timestamp', None):
                s['created'] = d['metadata']['creation_timestamp'].astimezone(
                    tz=datetime.timezone.utc)
            if d['status'].get('start_time', None):
                s['started'] = d['status']['start_time'].astimezone(
                    tz=datetime.timezone.utc)

            pods_summary.append(s)

        return pods_summary

    def remove_pods(self, names: List[str]) -> List[str]:
        pods = [i for i in self.rook_pods.items]
        for p in pods:
            d = p.to_dict()
            daemon_type = d['metadata']['labels']['app'].replace('rook-ceph-','')
            daemon_id = d['metadata']['labels']['ceph_daemon_id']
            name = daemon_type + '.' + daemon_id
            if name in names:
                self.coreV1_api.delete_namespaced_pod(
                    d['metadata']['name'],
                    self.rook_env.namespace,
                    body=client.V1DeleteOptions()
                )
        return [f'Removed Pod {n}' for n in names]

    def get_node_names(self) -> List[str]:
        return [i.metadata.name for i in self.nodes.items]

    @contextmanager
    def ignore_409(self, what: str) -> Iterator[None]:
        try:
            yield
        except ApiException as e:
            if e.status == 409:
                # Idempotent, succeed.
                log.info("{} already exists".format(what))
            else:
                raise

    def apply_filesystem(self, spec: ServiceSpec, num_replicas: int,
                         leaf_type: str) -> str:
        # TODO use spec.placement
        # TODO warn if spec.extended has entries we don't kow how
        #      to action.
        all_hosts = self.get_hosts()
        def _update_fs(new: cfs.CephFilesystem) -> cfs.CephFilesystem:
            new.spec.metadataServer.activeCount = spec.placement.count or 1
            new.spec.metadataServer.placement = cfs.Placement(
                nodeAffinity=cfs.NodeAffinity(
                    requiredDuringSchedulingIgnoredDuringExecution=cfs.RequiredDuringSchedulingIgnoredDuringExecution(
                        nodeSelectorTerms=cfs.NodeSelectorTermsList(
                            [placement_spec_to_node_selector(spec.placement, all_hosts)]
                        )
                    )
                )
            )
            return new
        def _create_fs() -> cfs.CephFilesystem:
            fs = cfs.CephFilesystem(
                apiVersion=self.rook_env.api_name,
                metadata=dict(
                    name=spec.service_id,
                    namespace=self.rook_env.namespace,
                ),
                spec=cfs.Spec(
                    dataPools=cfs.DataPoolsList(
                        {
                            cfs.DataPoolsItem(
                                failureDomain=leaf_type,
                                replicated=cfs.Replicated(
                                    size=num_replicas
                                )
                            )
                        }
                    ),
                    metadataPool=cfs.MetadataPool(
                        failureDomain=leaf_type,
                        replicated=cfs.Replicated(
                            size=num_replicas
                        )
                    ),
                    metadataServer=cfs.MetadataServer(
                        activeCount=spec.placement.count or 1,
                        activeStandby=True,
                        placement=
                        cfs.Placement(
                            nodeAffinity=cfs.NodeAffinity(
                                requiredDuringSchedulingIgnoredDuringExecution=cfs.RequiredDuringSchedulingIgnoredDuringExecution(
                                    nodeSelectorTerms=cfs.NodeSelectorTermsList(
                                        [placement_spec_to_node_selector(spec.placement, all_hosts)]
                                    )
                                )
                            )
                        )
                    )
                )
            )
            return fs
        assert spec.service_id is not None
        return self._create_or_patch(
            cfs.CephFilesystem, 'cephfilesystems', spec.service_id,
            _update_fs, _create_fs)

    def get_matching_node(self, host: str) -> Any:
        matching_node = None
        for node in self.nodes.items:
            if node.metadata.labels['kubernetes.io/hostname'] == host:
                matching_node = node
        return matching_node

    def add_host_label(self, host: str, label: str) -> OrchResult[str]:
        matching_node = self.get_matching_node(host)
        if matching_node == None:
            return OrchResult(None, RuntimeError(f"Cannot add {label} label to {host}: host not found in cluster")) 
        matching_node.metadata.labels['ceph-label/'+ label] = ""
        self.coreV1_api.patch_node(host, matching_node)
        return OrchResult(f'Added {label} label to {host}')

    def remove_host_label(self, host: str, label: str) -> OrchResult[str]:
        matching_node = self.get_matching_node(host)
        if matching_node == None:
            return OrchResult(None, RuntimeError(f"Cannot remove {label} label from {host}: host not found in cluster")) 
        matching_node.metadata.labels.pop('ceph-label/' + label, None)
        self.coreV1_api.patch_node(host, matching_node)
        return OrchResult(f'Removed {label} label from {host}')

    def apply_objectstore(self, spec: RGWSpec, num_replicas: int, leaf_type: str) -> str:
        assert spec.service_id is not None

        name = spec.service_id

        if '.' in spec.service_id:
            # rook does not like . in the name.  this is could
            # there because it is a legacy rgw spec that was named
            # like $realm.$zone, except that I doubt there were any
            # users of this code.  Instead, focus on future users and
            # translate . to - (fingers crossed!) instead.
            name = spec.service_id.replace('.', '-')

        all_hosts = self.get_hosts()
        def _create_zone() -> cos.CephObjectStore:
            port = None
            secure_port = None
            if spec.ssl:
                secure_port = spec.get_port()
            else:
                port = spec.get_port()
            object_store = cos.CephObjectStore(
                    apiVersion=self.rook_env.api_name,
                    metadata=dict(
                        name=name,
                        namespace=self.rook_env.namespace
                    ),
                    spec=cos.Spec(
                        gateway=cos.Gateway(
                            port=port,
                            securePort=secure_port,
                            instances=spec.placement.count or 1,
                            placement=cos.Placement(
                                cos.NodeAffinity(
                                    requiredDuringSchedulingIgnoredDuringExecution=cos.RequiredDuringSchedulingIgnoredDuringExecution(
                                        nodeSelectorTerms=cos.NodeSelectorTermsList(
                                            [
                                                placement_spec_to_node_selector(spec.placement, all_hosts)
                                            ]
                                        )
                                    )
                                )
                            )
                        ),
                        dataPool=cos.DataPool(
                            failureDomain=leaf_type,
                            replicated=cos.Replicated(
                                size=num_replicas
                            )
                        ),
                        metadataPool=cos.MetadataPool(
                            failureDomain=leaf_type,
                            replicated=cos.Replicated(
                                size=num_replicas
                            )
                        )
                    )
                )
            if spec.rgw_zone:
                object_store.spec.zone=cos.Zone(
                            name=spec.rgw_zone
                        )
            return object_store
                

        def _update_zone(new: cos.CephObjectStore) -> cos.CephObjectStore:
            if new.spec.gateway:
                new.spec.gateway.instances = spec.placement.count or 1
            else: 
                new.spec.gateway=cos.Gateway(
                    instances=spec.placement.count or 1
                )
            return new
        return self._create_or_patch(
            cos.CephObjectStore, 'cephobjectstores', name,
            _update_zone, _create_zone)

    def apply_nfsgw(self, spec: NFSServiceSpec, mgr: 'RookOrchestrator') -> str:
        # TODO use spec.placement
        # TODO warn if spec.extended has entries we don't kow how
        #      to action.
        # TODO Number of pods should be based on the list of hosts in the
        #      PlacementSpec.
        assert spec.service_id, "service id in NFS service spec cannot be an empty string or None " # for mypy typing
        service_id = spec.service_id
        mgr_module = cast(Module, mgr)
        count = spec.placement.count or 1
        def _update_nfs(new: cnfs.CephNFS) -> cnfs.CephNFS:
            new.spec.server.active = count
            return new

        def _create_nfs() -> cnfs.CephNFS:
            rook_nfsgw = cnfs.CephNFS(
                    apiVersion=self.rook_env.api_name,
                    metadata=dict(
                        name=spec.service_id,
                        namespace=self.rook_env.namespace,
                        ),
                    spec=cnfs.Spec(
                        rados=cnfs.Rados(
                            namespace=service_id,
                            pool=NFS_POOL_NAME,
                            ),
                        server=cnfs.Server(
                            active=count
                            )
                        )
                    )


            return rook_nfsgw

        create_ganesha_pool(mgr)
        NFSRados(mgr_module.rados, service_id).write_obj('', f'conf-nfs.{spec.service_id}')
        return self._create_or_patch(cnfs.CephNFS, 'cephnfses', service_id,
                _update_nfs, _create_nfs)

    def rm_service(self, rooktype: str, service_id: str) -> str:
        self.customObjects_api.delete_namespaced_custom_object(group="ceph.rook.io", version="v1", namespace="rook-ceph", plural=rooktype, name=service_id)
        objpath = "{0}/{1}".format(rooktype, service_id)
        return f'Removed {objpath}'

    def get_resource(self, resource_type: str) -> Iterable:
        custom_objects: KubernetesCustomResource = KubernetesCustomResource(self.customObjects_api.list_namespaced_custom_object, group="ceph.rook.io", version="v1", namespace="rook-ceph", plural=resource_type)
        return custom_objects.items

    def can_create_osd(self) -> bool:
        current_cluster = self.rook_api_get(
            "cephclusters/{0}".format(self.rook_env.cluster_name))
        use_all_nodes = current_cluster['spec'].get('useAllNodes', False)

        # If useAllNodes is set, then Rook will not be paying attention
        # to anything we put in 'nodes', so can't do OSD creation.
        return not use_all_nodes

    def node_exists(self, node_name: str) -> bool:
        return node_name in self.get_node_names()

    def update_mon_count(self, newcount: Optional[int]) -> str:
        def _update_mon_count(current, new):
            # type: (ccl.CephCluster, ccl.CephCluster) -> ccl.CephCluster
            if newcount is None:
                raise orchestrator.OrchestratorError('unable to set mon count to None')
            if not new.spec.mon:
                raise orchestrator.OrchestratorError("mon attribute not specified in new spec")
            new.spec.mon.count = newcount
            return new
        return self._patch(ccl.CephCluster, 'cephclusters', self.rook_env.cluster_name, _update_mon_count)

    def add_osds(self, drive_group, matching_hosts):
        # type: (DriveGroupSpec, List[str]) -> str
        assert drive_group.objectstore in ("bluestore", "filestore")
        assert drive_group.service_id
        storage_class = self.get_storage_class()
        inventory = self.get_discovered_devices()
        creator: Optional[DefaultCreator] = None
        if (
            storage_class.metadata.labels
            and 'local.storage.openshift.io/owner-name' in storage_class.metadata.labels
        ):
            creator = LSOCreator(inventory, self.coreV1_api, self.storage_class)    
        else:
            creator = DefaultCreator(inventory, self.coreV1_api, self.storage_class)
        return self._patch(
            ccl.CephCluster,
            'cephclusters',
            self.rook_env.cluster_name,
            creator.add_osds(self.rook_pods, drive_group, matching_hosts)
        )

    def remove_osds(self, osd_ids: List[str], replace: bool, force: bool, mon_command: Callable) -> str:
        inventory = self.get_discovered_devices()
        self.remover = DefaultRemover(
            self.coreV1_api,
            self.batchV1_api, 
            self.appsV1_api, 
            osd_ids, 
            replace, 
            force, 
            mon_command, 
            self._patch, 
            self.rook_env,
            inventory
        )
        return self.remover.remove()

    def get_hosts(self) -> List[orchestrator.HostSpec]:
        ret = []
        for node in self.nodes.items:
            spec = orchestrator.HostSpec(
                node.metadata.name, 
                addr='/'.join([addr.address for addr in node.status.addresses]), 
                labels=[label.split('/')[1] for label in node.metadata.labels if label.startswith('ceph-label')],
            )
            ret.append(spec)
        return ret

    def create_zap_job(self, host: str, path: str) -> None:
        body = client.V1Job(
            api_version="batch/v1",
            metadata=client.V1ObjectMeta(
                name="rook-ceph-device-zap",
                namespace="rook-ceph"
            ),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        containers=[
                            client.V1Container(
                                name="device-zap",
                                image="rook/ceph:master",
                                command=["bash"],
                                args=["-c", f"ceph-volume raw list {path} && dd if=/dev/zero of=\"{path}\" bs=1M count=1 oflag=direct,dsync || ceph-volume lvm zap --destroy {path}"],
                                env=[
                                    client.V1EnvVar(
                                        name="ROOK_CEPH_USERNAME",
                                        value_from=client.V1EnvVarSource(
                                            secret_key_ref=client.V1SecretKeySelector(
                                                key="ceph-username",
                                                name="rook-ceph-mon"
                                            )
                                        )
                                    ),
                                    client.V1EnvVar(
                                        name="ROOK_CEPH_SECRET",
                                        value_from=client.V1EnvVarSource(
                                            secret_key_ref=client.V1SecretKeySelector(
                                                key="ceph-secret",
                                                name="rook-ceph-mon"
                                            )
                                        )
                                    )
                                ],
                                security_context=client.V1SecurityContext(
                                    run_as_user=0,
                                    privileged=True
                                ),
                                volume_mounts=[
                                    client.V1VolumeMount(
                                        mount_path="/etc/ceph",
                                        name="ceph-conf-emptydir"
                                    ),
                                    client.V1VolumeMount(
                                        mount_path="/etc/rook",
                                        name="rook-config"
                                    ),
                                    client.V1VolumeMount(
                                        mount_path="/dev",
                                        name="devices"
                                    )
                                ]
                            )
                        ],
                        volumes=[
                            client.V1Volume(
                                name="ceph-conf-emptydir",
                                empty_dir=client.V1EmptyDirVolumeSource()
                            ),
                            client.V1Volume(
                                name="rook-config",
                                empty_dir=client.V1EmptyDirVolumeSource()
                            ),
                            client.V1Volume(
                                name="devices",
                                host_path=client.V1HostPathVolumeSource(
                                    path="/dev"
                                )
                            ),
                        ],
                        node_selector={
                            "kubernetes.io/hostname": host
                        },
                        restart_policy="Never"
                    )
                )
            )
        )
        self.batchV1_api.create_namespaced_job('rook-ceph', body)

    def rbd_mirror(self, spec: ServiceSpec) -> None:
        service_id = spec.service_id or "default-rbd-mirror"
        all_hosts = self.get_hosts()
        def _create_rbd_mirror() -> crbdm.CephRBDMirror:
            return crbdm.CephRBDMirror(
                apiVersion=self.rook_env.api_name,
                metadata=dict(
                    name=service_id,
                    namespace=self.rook_env.namespace,
                ),
                spec=crbdm.Spec(
                    count=spec.placement.count or 1,
                    placement=crbdm.Placement(
                        nodeAffinity=crbdm.NodeAffinity(
                            requiredDuringSchedulingIgnoredDuringExecution=crbdm.RequiredDuringSchedulingIgnoredDuringExecution(
                                nodeSelectorTerms=crbdm.NodeSelectorTermsList(
                                    [
                                        placement_spec_to_node_selector(spec.placement, all_hosts)
                                    ]
                                )
                            )
                        )
                    )
                )
            )
        def _update_rbd_mirror(new: crbdm.CephRBDMirror) -> crbdm.CephRBDMirror:
            new.spec.count = spec.placement.count or 1
            new.spec.placement = crbdm.Placement(
                nodeAffinity=crbdm.NodeAffinity(
                    requiredDuringSchedulingIgnoredDuringExecution=crbdm.RequiredDuringSchedulingIgnoredDuringExecution(
                        nodeSelectorTerms=crbdm.NodeSelectorTermsList(
                            [
                                placement_spec_to_node_selector(spec.placement, all_hosts)
                            ]
                        )
                    )
                )
            )
            return new
        self._create_or_patch(crbdm.CephRBDMirror, 'cephrbdmirrors', service_id, _update_rbd_mirror, _create_rbd_mirror)
    def _patch(self, crd: Type, crd_name: str, cr_name: str, func: Callable[[CrdClassT, CrdClassT], CrdClassT]) -> str:
        current_json = self.rook_api_get(
            "{}/{}".format(crd_name, cr_name)
        )

        current = crd.from_json(current_json)
        new = crd.from_json(current_json)  # no deepcopy.

        new = func(current, new)

        patch = list(jsonpatch.make_patch(current_json, new.to_json()))

        log.info('patch for {}/{}: \n{}'.format(crd_name, cr_name, patch))

        if len(patch) == 0:
            return "No change"

        try:
            self.rook_api_patch(
                "{}/{}".format(crd_name, cr_name),
                body=patch)
        except ApiException as e:
            log.exception("API exception: {0}".format(e))
            raise ApplyException(
                "Failed to update {}/{}: {}".format(crd_name, cr_name, e))

        return "Success"

    def _create_or_patch(self,
                         crd: Type,
                         crd_name: str,
                         cr_name: str,
                         update_func: Callable[[CrdClassT], CrdClassT],
                         create_func: Callable[[], CrdClassT]) -> str:
        try:
            current_json = self.rook_api_get(
                "{}/{}".format(crd_name, cr_name)
            )
        except ApiException as e:
            if e.status == 404:
                current_json = None
            else:
                raise

        if current_json:
            new = crd.from_json(current_json)  # no deepcopy.

            new = update_func(new)

            patch = list(jsonpatch.make_patch(current_json, new.to_json()))

            log.info('patch for {}/{}: \n{}'.format(crd_name, cr_name, patch))

            if len(patch) == 0:
                return "No change"

            try:
                self.rook_api_patch(
                    "{}/{}".format(crd_name, cr_name),
                    body=patch)
            except ApiException as e:
                log.exception("API exception: {0}".format(e))
                raise ApplyException(
                    "Failed to update {}/{}: {}".format(crd_name, cr_name, e))
            return "Updated"
        else:
            new = create_func()
            with self.ignore_409("{} {} already exists".format(crd_name,
                                                               cr_name)):
                self.rook_api_post("{}/".format(crd_name),
                                   body=new.to_json())
            return "Created"
    def get_ceph_image(self) -> str:
        try:
            api_response = self.coreV1_api.list_namespaced_pod(self.rook_env.namespace,
                                                               label_selector="app=rook-ceph-mon",
                                                               timeout_seconds=10)
            if api_response.items:
                return api_response.items[-1].spec.containers[0].image
            else:
                raise orchestrator.OrchestratorError(
                        "Error getting ceph image. Cluster without monitors")
        except ApiException as e:
            raise orchestrator.OrchestratorError("Error getting ceph image: {}".format(e))


    def _execute_blight_job(self, ident_fault: str, on: bool, loc: orchestrator.DeviceLightLoc) -> str:
        operation_id = str(hash(loc))
        message = ""

        # job definition
        job_metadata = client.V1ObjectMeta(name=operation_id,
                                           namespace= self.rook_env.namespace,
                                           labels={"ident": operation_id})
        pod_metadata = client.V1ObjectMeta(labels={"ident": operation_id})
        pod_container = client.V1Container(name="ceph-lsmcli-command",
                                           security_context=client.V1SecurityContext(privileged=True),
                                           image=self.get_ceph_image(),
                                           command=["lsmcli",],
                                           args=['local-disk-%s-led-%s' % (ident_fault,'on' if on else 'off'),
                                                 '--path', loc.path or loc.dev,],
                                           volume_mounts=[client.V1VolumeMount(name="devices", mount_path="/dev"),
                                                          client.V1VolumeMount(name="run-udev", mount_path="/run/udev")])
        pod_spec = client.V1PodSpec(containers=[pod_container],
                                    active_deadline_seconds=30, # Max time to terminate pod
                                    restart_policy="Never",
                                    node_selector= {"kubernetes.io/hostname": loc.host},
                                    volumes=[client.V1Volume(name="devices",
                                                             host_path=client.V1HostPathVolumeSource(path="/dev")),
                                             client.V1Volume(name="run-udev",
                                                             host_path=client.V1HostPathVolumeSource(path="/run/udev"))])
        pod_template = client.V1PodTemplateSpec(metadata=pod_metadata,
                                                  spec=pod_spec)
        job_spec = client.V1JobSpec(active_deadline_seconds=60, # Max time to terminate job
                                    ttl_seconds_after_finished=10, # Alfa. Lifetime after finishing (either Complete or Failed)
                                    backoff_limit=0,
                                    template=pod_template)
        job = client.V1Job(api_version="batch/v1",
                           kind="Job",
                           metadata=job_metadata,
                           spec=job_spec)

        # delete previous job if it exists
        try:
            try:
                api_response = self.batchV1_api.delete_namespaced_job(operation_id,
                                                                      self.rook_env.namespace,
                                                                      propagation_policy="Background")
            except ApiException as e:
                if e.status != 404: # No problem if the job does not exist
                    raise

            # wait until the job is not present
            deleted = False
            retries = 0
            while not deleted and retries < 10:
                api_response = self.batchV1_api.list_namespaced_job(self.rook_env.namespace,
                                                                    label_selector="ident=%s" % operation_id,
                                                                    timeout_seconds=10)
                deleted = not api_response.items
                if retries > 5:
                    sleep(0.1)
                retries += 1
            if retries == 10 and not deleted:
                raise orchestrator.OrchestratorError(
                    "Light <{}> in <{}:{}> cannot be executed. Cannot delete previous job <{}>".format(
                            on, loc.host, loc.path or loc.dev, operation_id))

            # create the job
            api_response = self.batchV1_api.create_namespaced_job(self.rook_env.namespace, job)

            # get the result
            finished = False
            while not finished:
                api_response = self.batchV1_api.read_namespaced_job(operation_id,
                                                                    self.rook_env.namespace)
                finished = api_response.status.succeeded or api_response.status.failed
                if finished:
                    message = api_response.status.conditions[-1].message

            # get the result of the lsmcli command
            api_response=self.coreV1_api.list_namespaced_pod(self.rook_env.namespace,
                                                             label_selector="ident=%s" % operation_id,
                                                             timeout_seconds=10)
            if api_response.items:
                pod_name = api_response.items[-1].metadata.name
                message = self.coreV1_api.read_namespaced_pod_log(pod_name,
                                                                  self.rook_env.namespace)

        except ApiException as e:
            log.exception('K8s API failed. {}'.format(e))
            raise

        # Finally, delete the job.
        # The job uses <ttl_seconds_after_finished>. This makes that the TTL controller delete automatically the job.
        # This feature is in Alpha state, so extra explicit delete operations trying to delete the Job has been used strategically
        try:
            api_response = self.batchV1_api.delete_namespaced_job(operation_id,
                                                                  self.rook_env.namespace,
                                                                  propagation_policy="Background")
        except ApiException as e:
            if e.status != 404: # No problem if the job does not exist
                raise

        return message

    def blink_light(self, ident_fault, on, locs):
        # type: (str, bool, List[orchestrator.DeviceLightLoc]) -> List[str]
        return [self._execute_blight_job(ident_fault, on, loc) for loc in locs]

def placement_spec_to_node_selector(spec: PlacementSpec, all_hosts: List) -> ccl.NodeSelectorTermsItem:
    all_hostnames = [hs.hostname for hs in all_hosts]
    res = ccl.NodeSelectorTermsItem(matchExpressions=ccl.MatchExpressionsList())
    if spec.host_pattern and spec.host_pattern != "*":
        raise RuntimeError("The Rook orchestrator only supports a host_pattern of * for placements")
    if spec.label:
        res.matchExpressions.append(
            ccl.MatchExpressionsItem(
                key="ceph-label/" + spec.label,
                operator="Exists"
            )
        )
    if spec.hosts:
        host_list = [h.hostname for h in spec.hosts if h.hostname in all_hostnames]
        res.matchExpressions.append(
            ccl.MatchExpressionsItem(
                key="kubernetes.io/hostname",
                operator="In",
                values=ccl.CrdObjectList(host_list)
            )
        ) 
    if spec.host_pattern == "*" or (not spec.label and not spec.hosts and not spec.host_pattern):
        res.matchExpressions.append(
            ccl.MatchExpressionsItem(
                key="kubernetes.io/hostname",
                operator="Exists",
            )
        )
    return res
    
def node_selector_to_placement_spec(node_selector: ccl.NodeSelectorTermsItem) -> PlacementSpec:
    res = PlacementSpec()
    for expression in node_selector.matchExpressions:
        if expression.key.startswith("ceph-label/"):
            res.label = expression.key.split('/')[1]
        elif expression.key == "kubernetes.io/hostname":
            if expression.operator == "Exists":
                res.host_pattern = HostPattern("*")
            elif expression.operator == "In": 
                res.hosts = [HostPlacementSpec(hostname=value, network='', name='')for value in expression.values]
    return res
