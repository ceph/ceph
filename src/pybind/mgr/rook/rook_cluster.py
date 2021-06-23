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
import json
from contextlib import contextmanager
from time import sleep

import jsonpatch
from urllib.parse import urljoin

# Optional kubernetes imports to enable MgrModule.can_run
# to behave cleanly.
from urllib3.exceptions import ProtocolError

from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.service_spec import ServiceSpec, NFSServiceSpec, RGWSpec
from ceph.utils import datetime_now
from mgr_util import merge_dicts

from typing import Optional, TypeVar, List, Callable, Any, cast, Generic, \
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
from .rook_client._helper import CrdClass


import orchestrator


try:
    from rook.module import RookEnv
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
        # metadata is a client.V1ListMeta object type
        metadata = response.metadata  # type: client.V1ListMeta
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
                try:
                    name = item.metadata.name
                except AttributeError:
                    raise AttributeError(
                        "{} doesn't contain a metadata.name. Unable to track changes".format(
                            self.api_func))

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


class RookCluster(object):
    # import of client.CoreV1Api must be optional at import time.
    # Instead allow mgr/rook to be imported anyway.
    def __init__(self, coreV1_api: 'client.CoreV1Api', batchV1_api: 'client.BatchV1Api', rook_env: 'RookEnv'):
        self.rook_env = rook_env  # type: RookEnv
        self.coreV1_api = coreV1_api  # client.CoreV1Api
        self.batchV1_api = batchV1_api

        #  TODO: replace direct k8s calls with Rook API calls
        # when they're implemented
        self.inventory_maps: KubernetesResource[client.V1ConfigMapList] = KubernetesResource(self.coreV1_api.list_namespaced_config_map,
                                                 namespace=self.rook_env.operator_namespace,
                                                 label_selector="app=rook-discover")

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

    def get_discovered_devices(self, nodenames: Optional[List[str]] = None) -> Dict[str, dict]:
        def predicate(item: client.V1ConfigMapList) -> bool:
            if nodenames is not None:
                return item.metadata.labels['rook.io/node'] in nodenames
            else:
                return True

        try:
            result = [i for i in self.inventory_maps.items if predicate(i)]
        except ApiException as dummy_e:
            log.exception("Failed to fetch device metadata")
            raise

        nodename_to_devices = {}
        for i in result:
            drives = json.loads(i.data['devices'])
            nodename_to_devices[i.metadata.labels['rook.io/node']] = drives

        return nodename_to_devices

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
                            "ceph_nfs": ("ceph_nfs", service_id),
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

            image_id = d['status']['container_statuses'][0]['image_id']
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

    def apply_filesystem(self, spec: ServiceSpec) -> str:
        # TODO use spec.placement
        # TODO warn if spec.extended has entries we don't kow how
        #      to action.
        def _update_fs(new: cfs.CephFilesystem) -> cfs.CephFilesystem:
            new.spec.metadataServer.activeCount = spec.placement.count or 1
            return new

        def _create_fs() -> cfs.CephFilesystem:
            return cfs.CephFilesystem(
                apiVersion=self.rook_env.api_name,
                metadata=dict(
                    name=spec.service_id,
                    namespace=self.rook_env.namespace,
                ),
                spec=cfs.Spec(
                    None,
                    None,
                    metadataServer=cfs.MetadataServer(
                        activeCount=spec.placement.count or 1,
                        activeStandby=True
                    )
                )
            )
        assert spec.service_id is not None
        return self._create_or_patch(
            cfs.CephFilesystem, 'cephfilesystems', spec.service_id,
            _update_fs, _create_fs)

    def apply_objectstore(self, spec: RGWSpec) -> str:
        assert spec.service_id is not None

        name = spec.service_id

        if '.' in spec.service_id:
            # rook does not like . in the name.  this is could
            # there because it is a legacy rgw spec that was named
            # like $realm.$zone, except that I doubt there were any
            # users of this code.  Instead, focus on future users and
            # translate . to - (fingers crossed!) instead.
            name = spec.service_id.replace('.', '-')


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

    def apply_nfsgw(self, spec: NFSServiceSpec) -> str:
        # TODO use spec.placement
        # TODO warn if spec.extended has entries we don't kow how
        #      to action.
        # TODO Number of pods should be based on the list of hosts in the
        #      PlacementSpec.
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
                            namespace=self.rook_env.namespace,
                            pool=spec.pool
                            ),
                        server=cnfs.Server(
                            active=count
                            )
                        )
                    )

            if spec.namespace:
                rook_nfsgw.spec.rados.namespace = spec.namespace

            return rook_nfsgw

        assert spec.service_id is not None
        return self._create_or_patch(cnfs.CephNFS, 'cephnfses', spec.service_id,
                _update_nfs, _create_nfs)

    def rm_service(self, rooktype: str, service_id: str) -> str:

        objpath = "{0}/{1}".format(rooktype, service_id)

        try:
            self.rook_api_delete(objpath)
        except ApiException as e:
            if e.status == 404:
                log.info("{0} service '{1}' does not exist".format(rooktype, service_id))
                # Idempotent, succeed.
            else:
                raise

        return f'Removed {objpath}'

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
    """
    def add_osds(self, drive_group, matching_hosts):
        # type: (DriveGroupSpec, List[str]) -> str
        
        # Rook currently (0.8) can only do single-drive OSDs, so we
        # treat all drive groups as just a list of individual OSDs.
        
        block_devices = drive_group.data_devices.paths if drive_group.data_devices else []
        directories = drive_group.data_directories

        assert drive_group.objectstore in ("bluestore", "filestore")

        def _add_osds(current_cluster, new_cluster):
            # type: (ccl.CephCluster, ccl.CephCluster) -> ccl.CephCluster

            # FIXME: this is all not really atomic, because jsonpatch doesn't
            # let us do "test" operations that would check if items with
            # matching names were in existing lists.
            if not new_cluster.spec.storage:
                raise orchestrator.OrchestratorError('new_cluster missing storage attribute')

            if not hasattr(new_cluster.spec.storage, 'nodes'):
                new_cluster.spec.storage.nodes = ccl.NodesList()

            current_nodes = getattr(current_cluster.spec.storage, 'nodes', ccl.NodesList())
            matching_host = matching_hosts[0]
            
            if matching_host not in [n.name for n in current_nodes]:
                # FIXME: ccl.Config stopped existing since rook changed
                # their CRDs, check if config is actually necessary for this
                
                pd = ccl.NodesItem(
                    name=matching_host,
                    config=ccl.Config(
                        storeType=drive_group.objectstore
                    )
                )

                if block_devices:
                    pd.devices = ccl.DevicesList(
                        ccl.DevicesItem(name=d.path) for d in block_devices
                    )
                if directories:
                    pd.directories = ccl.DirectoriesList(
                        ccl.DirectoriesItem(path=p) for p in directories
                    )
                new_cluster.spec.storage.nodes.append(pd)
            else:
                for _node in new_cluster.spec.storage.nodes:
                    current_node = _node  # type: ccl.NodesItem
                    if current_node.name == matching_host:
                        if block_devices:
                            if not hasattr(current_node, 'devices'):
                                current_node.devices = ccl.DevicesList()
                            current_device_names = set(d.name for d in current_node.devices)
                            new_devices = [bd for bd in block_devices if bd.path not in current_device_names]
                            current_node.devices.extend(
                                ccl.DevicesItem(name=n.path) for n in new_devices
                            )

                        if directories:
                            if not hasattr(current_node, 'directories'):
                                current_node.directories = ccl.DirectoriesList()
                            new_dirs = list(set(directories) - set([d.path for d in current_node.directories]))
                            current_node.directories.extend(
                                ccl.DirectoriesItem(path=n) for n in new_dirs
                            )
            return new_cluster

        return self._patch(ccl.CephCluster, 'cephclusters', self.rook_env.cluster_name, _add_osds)
    """
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
