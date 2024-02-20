import datetime
import logging
import re
import threading
import functools
import os
import json

from ceph.deployment import inventory
from ceph.deployment.service_spec import ServiceSpec, NFSServiceSpec, RGWSpec, PlacementSpec
from ceph.utils import datetime_now

from typing import List, Dict, Optional, Callable, Any, TypeVar, Tuple, TYPE_CHECKING

try:
    from ceph.deployment.drive_group import DriveGroupSpec
except ImportError:
    pass  # just for type checking

try:
    from kubernetes import client, config
    from kubernetes.client.rest import ApiException

    kubernetes_imported = True

    # https://github.com/kubernetes-client/python/issues/895
    from kubernetes.client.models.v1_container_image import V1ContainerImage
    def names(self: Any, names: Any) -> None:
        self._names = names
    V1ContainerImage.names = V1ContainerImage.names.setter(names)

except ImportError:
    kubernetes_imported = False
    client = None
    config = None

from mgr_module import MgrModule, Option, NFS_POOL_NAME
import orchestrator
from orchestrator import handle_orch_error, OrchResult, raise_if_exception

from .rook_cluster import RookCluster

T = TypeVar('T')
FuncT = TypeVar('FuncT', bound=Callable)
ServiceSpecT = TypeVar('ServiceSpecT', bound=ServiceSpec)


class RookEnv(object):
    def __init__(self) -> None:
        # POD_NAMESPACE already exist for Rook 0.9
        self.namespace = os.environ.get('POD_NAMESPACE', 'rook-ceph')

        # ROOK_CEPH_CLUSTER_CRD_NAME is new is Rook 1.0
        self.cluster_name = os.environ.get('ROOK_CEPH_CLUSTER_CRD_NAME', self.namespace)

        self.operator_namespace = os.environ.get('ROOK_OPERATOR_NAMESPACE', self.namespace)
        self.crd_version = os.environ.get('ROOK_CEPH_CLUSTER_CRD_VERSION', 'v1')
        self.api_name = "ceph.rook.io/" + self.crd_version

    def api_version_match(self) -> bool:
        return self.crd_version == 'v1'

    def has_namespace(self) -> bool:
        return 'POD_NAMESPACE' in os.environ


class RookOrchestrator(MgrModule, orchestrator.Orchestrator):
    """
    Writes are a two-phase thing, firstly sending
    the write to the k8s API (fast) and then waiting
    for the corresponding change to appear in the
    Ceph cluster (slow)

    Right now, we are calling the k8s API synchronously.
    """

    MODULE_OPTIONS: List[Option] = [
        # TODO: configure k8s API addr instead of assuming local
        Option(
            'storage_class',
            type='str',
            default='local',
            desc='storage class name for LSO-discovered PVs',
        ),
    ]

    @staticmethod
    def can_run() -> Tuple[bool, str]:
        if not kubernetes_imported:
            return False, "`kubernetes` python module not found"
        if not RookEnv().api_version_match():
            return False, "Rook version unsupported."
        return True, ''

    def available(self) -> Tuple[bool, str, Dict[str, Any]]:
        if not kubernetes_imported:
            return False, "`kubernetes` python module not found", {}
        elif not self._rook_env.has_namespace():
            return False, "ceph-mgr not running in Rook cluster", {}

        try:
            self.k8s.list_namespaced_pod(self._rook_env.namespace)
        except ApiException as e:
            return False, "Cannot reach Kubernetes API: {}".format(e), {}
        else:
            return True, "", {}

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(RookOrchestrator, self).__init__(*args, **kwargs)

        self._initialized = threading.Event()
        self._k8s_CoreV1_api: Optional[client.CoreV1Api] = None
        self._k8s_BatchV1_api: Optional[client.BatchV1Api] = None
        self._k8s_CustomObjects_api: Optional[client.CustomObjectsApi] = None
        self._k8s_StorageV1_api: Optional[client.StorageV1Api] = None
        self._rook_cluster: Optional[RookCluster] = None
        self._rook_env = RookEnv()
        self._k8s_AppsV1_api: Optional[client.AppsV1Api] = None

        self.config_notify()
        if TYPE_CHECKING:
            self.storage_class = 'foo'

        self._shutdown = threading.Event()

    def config_notify(self) -> None:
        """
        This method is called whenever one of our config options is changed.

        TODO: this method should be moved into mgr_module.py
        """
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],  # type: ignore
                    self.get_module_option(opt['name']))  # type: ignore
            self.log.debug(' mgr option %s = %s',
                           opt['name'], getattr(self, opt['name']))  # type: ignore
        assert isinstance(self.storage_class, str)

        if self._rook_cluster:
            self._rook_cluster.storage_class_name = self.storage_class

    def shutdown(self) -> None:
        self._shutdown.set()

    @property
    def k8s(self):
        # type: () -> client.CoreV1Api
        self._initialized.wait()
        assert self._k8s_CoreV1_api is not None
        return self._k8s_CoreV1_api

    @property
    def rook_cluster(self):
        # type: () -> RookCluster
        self._initialized.wait()
        assert self._rook_cluster is not None
        return self._rook_cluster

    def serve(self) -> None:
        # For deployed clusters, we should always be running inside
        # a Rook cluster.  For development convenience, also support
        # running outside (reading ~/.kube config)

        if self._rook_env.has_namespace():
            config.load_incluster_config()
        else:
            self.log.warning("DEVELOPMENT ONLY: Reading kube config from ~")
            config.load_kube_config()

            # So that I can do port forwarding from my workstation - jcsp
            from kubernetes.client import configuration
            configuration.verify_ssl = False

        self._k8s_CoreV1_api = client.CoreV1Api()
        self._k8s_BatchV1_api = client.BatchV1Api()
        self._k8s_CustomObjects_api = client.CustomObjectsApi()
        self._k8s_StorageV1_api = client.StorageV1Api()
        self._k8s_AppsV1_api = client.AppsV1Api()

        try:
            # XXX mystery hack -- I need to do an API call from
            # this context, or subsequent API usage from handle_command
            # fails with SSLError('bad handshake').  Suspect some kind of
            # thread context setup in SSL lib?
            self._k8s_CoreV1_api.list_namespaced_pod(self._rook_env.namespace)
        except ApiException:
            # Ignore here to make self.available() fail with a proper error message
            pass

        assert isinstance(self.storage_class, str)

        self._rook_cluster = RookCluster(
            self._k8s_CoreV1_api,
            self._k8s_BatchV1_api,
            self._k8s_CustomObjects_api,
            self._k8s_StorageV1_api,
            self._k8s_AppsV1_api,
            self._rook_env,
            self.storage_class)

        self._initialized.set()
        self.config_notify()

    @handle_orch_error
    def get_inventory(self, host_filter: Optional[orchestrator.InventoryFilter] = None, refresh: bool = False) -> List[orchestrator.InventoryHost]:
        host_list = None
        if host_filter and host_filter.hosts:
            # Explicit host list
            host_list = host_filter.hosts
        elif host_filter and host_filter.labels:
            # TODO: query k8s API to resolve to host list, and pass
            # it into RookCluster.get_discovered_devices
            raise NotImplementedError()

        discovered_devs = self.rook_cluster.get_discovered_devices(host_list)

        result = []
        for host_name, host_devs in discovered_devs.items():
            devs = []
            for d in host_devs:
                devs.append(d)

            result.append(orchestrator.InventoryHost(host_name, inventory.Devices(devs)))

        return result

    @handle_orch_error
    def get_hosts(self):
        # type: () -> List[orchestrator.HostSpec]
        return self.rook_cluster.get_hosts()

    @handle_orch_error
    def describe_service(self,
                         service_type: Optional[str] = None,
                         service_name: Optional[str] = None,
                         refresh: bool = False) -> List[orchestrator.ServiceDescription]:
        now = datetime_now()

        # CephCluster
        cl = self.rook_cluster.rook_api_get(
            "cephclusters/{0}".format(self.rook_cluster.rook_env.cluster_name))
        self.log.debug('CephCluster %s' % cl)
        image_name = cl['spec'].get('cephVersion', {}).get('image', None)
        num_nodes = len(self.rook_cluster.get_node_names())

        def sum_running_pods(service_type: str, service_name: Optional[str] = None) -> int:
            all_pods = self.rook_cluster.describe_pods(None, None, None)
            if service_name is None:
                return sum(pod['phase'] == 'Running' for pod in all_pods if pod['labels']['app'] == f"rook-ceph-{service_type}")
            else:
                if service_type == 'mds':
                    key = 'rook_file_system'
                elif service_type == 'rgw':
                    key = 'rook_object_store'
                elif service_type == 'nfs':
                    key = 'ceph_nfs'
                else:
                    self.log.error(f"Unknow service type {service_type}")
                    return 0

                return sum(pod['phase'] == 'Running' \
                           for pod in all_pods \
                           if pod['labels']['app'] == f"rook-ceph-{service_type}" \
                           and service_name == pod['labels'][key])

        spec = {}
        if service_type == 'mon' or service_type is None:
            spec['mon'] = orchestrator.ServiceDescription(
                spec=ServiceSpec(
                    'mon',
                    placement=PlacementSpec(
                        count=cl['spec'].get('mon', {}).get('count', 1),
                    ),
                ),
                size=cl['spec'].get('mon', {}).get('count', 1),
                container_image_name=image_name,
                last_refresh=now,
                running=sum_running_pods('mon')
            )
        if service_type == 'mgr' or service_type is None:
            spec['mgr'] = orchestrator.ServiceDescription(
                spec=ServiceSpec(
                    'mgr',
                    placement=PlacementSpec.from_string('count:1'),
                ),
                size=1,
                container_image_name=image_name,
                last_refresh=now,
                running=sum_running_pods('mgr')
            )

        if (
            service_type == 'crash' or service_type is None
            and not cl['spec'].get('crashCollector', {}).get('disable', False)
        ):
            spec['crash'] = orchestrator.ServiceDescription(
                spec=ServiceSpec(
                    'crash',
                    placement=PlacementSpec.from_string('*'),
                ),
                size=num_nodes,
                container_image_name=image_name,
                last_refresh=now,
                running=sum_running_pods('crashcollector')
            )

        if service_type == 'mds' or service_type is None:
            # CephFilesystems
            all_fs = self.rook_cluster.get_resource("cephfilesystems")
            for fs in all_fs:
                fs_name = fs['metadata']['name']
                svc = 'mds.' + fs_name
                if svc in spec:
                    continue
                # FIXME: we are conflating active (+ standby) with count
                active = fs['spec'].get('metadataServer', {}).get('activeCount', 1)
                total_mds = active
                if fs['spec'].get('metadataServer', {}).get('activeStandby', False):
                    total_mds = active * 2
                spec[svc] = orchestrator.ServiceDescription(
                    spec=ServiceSpec(
                        service_type='mds',
                        service_id=fs['metadata']['name'],
                        placement=PlacementSpec(count=active),
                    ),
                    size=total_mds,
                    container_image_name=image_name,
                    last_refresh=now,
                    running=sum_running_pods('mds', fs_name)
                )

        if service_type == 'rgw' or service_type is None:
            # CephObjectstores
            all_zones = self.rook_cluster.get_resource("cephobjectstores")
            for zone in all_zones:
                zone_name = zone['metadata']['name']
                svc = 'rgw.' + zone_name
                if svc in spec:
                    continue
                active = zone['spec']['gateway']['instances'];
                if 'securePort' in zone['spec']['gateway']:
                    ssl = True
                    port = zone['spec']['gateway']['securePort']
                else:
                    ssl = False
                    port = zone['spec']['gateway']['port'] or 80
                rgw_zone = zone['spec'].get('zone', {}).get('name') or None
                spec[svc] = orchestrator.ServiceDescription(
                    spec=RGWSpec(
                        service_id=zone['metadata']['name'],
                        rgw_zone=rgw_zone,
                        ssl=ssl,
                        rgw_frontend_port=port,
                        placement=PlacementSpec(count=active),
                    ),
                    size=active,
                    container_image_name=image_name,
                    last_refresh=now,
                    running=sum_running_pods('rgw', zone_name)
                )

        if service_type == 'nfs' or service_type is None:
            # CephNFSes
            all_nfs = self.rook_cluster.get_resource("cephnfses")
            nfs_pods = self.rook_cluster.describe_pods('nfs', None, None)
            for nfs in all_nfs:
                # Starting with V.17.2.0, the 'rados' spec part in 'cephnfs' resources does not contain the 'pool' item
                if 'pool' in nfs['spec']['rados']:
                    if nfs['spec']['rados']['pool'] != NFS_POOL_NAME:
                        continue
                nfs_name = nfs['metadata']['name']
                svc = 'nfs.' + nfs_name
                if svc in spec:
                    continue
                active = nfs['spec'].get('server', {}).get('active')
                creation_timestamp = datetime.datetime.strptime(nfs['metadata']['creationTimestamp'], '%Y-%m-%dT%H:%M:%SZ')
                spec[svc] = orchestrator.ServiceDescription(
                    spec=NFSServiceSpec(
                        service_id=nfs_name,
                        placement=PlacementSpec(count=active),
                    ),
                    size=active,
                    last_refresh=now,
                    running=sum_running_pods('nfs', nfs_name),
                    created=creation_timestamp.astimezone(tz=datetime.timezone.utc)
                )
        if service_type == 'osd' or service_type is None:
            # OSDs
            # FIXME: map running OSDs back to their respective services...

            # the catch-all unmanaged
            all_osds = self.rook_cluster.get_osds()
            svc = 'osd'
            spec[svc] = orchestrator.ServiceDescription(
                spec=DriveGroupSpec(
                    unmanaged=True,
                    service_type='osd',
                ),
                size=len(all_osds),
                last_refresh=now,
                running=sum_running_pods('osd')
            )

        if service_type == 'rbd-mirror' or service_type is None:
            # rbd-mirrors
            all_mirrors = self.rook_cluster.get_resource("cephrbdmirrors")
            for mirror in all_mirrors:
                logging.warn(mirror)
                mirror_name = mirror['metadata']['name']
                svc = 'rbd-mirror.' + mirror_name
                if svc in spec:
                    continue
                spec[svc] = orchestrator.ServiceDescription(
                    spec=ServiceSpec(
                        service_id=mirror_name,
                        service_type="rbd-mirror",
                        placement=PlacementSpec(count=1),
                    ),
                    size=1,
                    last_refresh=now,
                    running=sum_running_pods('rbd-mirror', mirror_name)
                )

        for dd in self._list_daemons():
            if dd.service_name() not in spec:
                continue
            service = spec[dd.service_name()]
            if not service.container_image_id:
                service.container_image_id = dd.container_image_id
            if not service.container_image_name:
                service.container_image_name = dd.container_image_name
            if service.last_refresh is None or not dd.last_refresh or dd.last_refresh < service.last_refresh:
                service.last_refresh = dd.last_refresh
            if service.created is None or dd.created is None or dd.created < service.created:
                service.created = dd.created

        return [v for k, v in spec.items()]

    @handle_orch_error
    def list_daemons(self,
                     service_name: Optional[str] = None,
                     daemon_type: Optional[str] = None,
                     daemon_id: Optional[str] = None,
                     host: Optional[str] = None,
                     refresh: bool = False) -> List[orchestrator.DaemonDescription]:
        return self._list_daemons(service_name=service_name,
                                  daemon_type=daemon_type,
                                  daemon_id=daemon_id,
                                  host=host,
                                  refresh=refresh)

    def _list_daemons(self,
                      service_name: Optional[str] = None,
                      daemon_type: Optional[str] = None,
                      daemon_id: Optional[str] = None,
                      host: Optional[str] = None,
                      refresh: bool = False) -> List[orchestrator.DaemonDescription]:

        def _pod_to_servicename(pod: Dict[str, Any]) -> Optional[str]:
            if 'ceph_daemon_type' not in pod['labels']:
                return None
            daemon_type = pod['labels']['ceph_daemon_type']
            if daemon_type in ['mds', 'rgw', 'nfs', 'rbd-mirror']:
                if 'app.kubernetes.io/part-of' in pod['labels']:
                    service_name = f"{daemon_type}.{pod['labels']['app.kubernetes.io/part-of']}"
                else:
                    service_name = f"{daemon_type}"
            else:
                service_name = f"{daemon_type}"
            return service_name

        pods = self.rook_cluster.describe_pods(daemon_type, daemon_id, host)
        result = []
        for p in pods:
            pod_svc_name = _pod_to_servicename(p)
            sd = orchestrator.DaemonDescription(service_name=pod_svc_name)
            sd.hostname = p['hostname']

            # In Rook environments, the 'ceph-exporter' daemon is named 'exporter' whereas
            # in the orchestrator interface, it is named 'ceph-exporter'. The purpose of the
            # following adjustment is to ensure that the 'daemon_type' is correctly set.
            # Without this adjustment, the 'service_to_daemon_types' lookup would fail, as
            # it would be searching for a non-existent entry called 'exporter
            if p['labels']['app'] == 'rook-ceph-exporter':
                sd.daemon_type = 'ceph-exporter'
            else:
                sd.daemon_type = p['labels']['app'].replace('rook-ceph-', '')

            status = {
                'Pending': orchestrator.DaemonDescriptionStatus.starting,
                'Running': orchestrator.DaemonDescriptionStatus.running,
                'Succeeded': orchestrator.DaemonDescriptionStatus.stopped,
                'Failed': orchestrator.DaemonDescriptionStatus.error,
                'Unknown': orchestrator.DaemonDescriptionStatus.unknown,
            }[p['phase']]
            sd.status = status

            if 'ceph_daemon_id' in p['labels']:
                sd.daemon_id = p['labels']['ceph_daemon_id']
            elif 'ceph-osd-id' in p['labels']:
                sd.daemon_id = p['labels']['ceph-osd-id']
            else:
                # Unknown type -- skip it
                continue

            if service_name is not None and service_name != sd.service_name():
                continue
            sd.container_image_name = p['container_image_name']
            sd.container_image_id = p['container_image_id']
            sd.created = p['created']
            sd.last_configured = p['created']
            sd.last_deployed = p['created']
            sd.started = p['started']
            sd.last_refresh = p['refreshed']
            result.append(sd)

        return result

    def _get_pool_params(self) -> Tuple[int, str]:
        num_replicas = self.get_ceph_option('osd_pool_default_size')
        assert type(num_replicas) is int

        leaf_type_id = self.get_ceph_option('osd_crush_chooseleaf_type')
        assert type(leaf_type_id) is int
        crush = self.get('osd_map_crush')
        leaf_type = 'host'
        for t in crush['types']:
            if t['type_id'] == leaf_type_id:
                leaf_type = t['name']
                break
        return num_replicas, leaf_type

    @handle_orch_error
    def remove_service(self, service_name: str, force: bool = False) -> str:
        if service_name == 'rbd-mirror':
            return self.rook_cluster.rm_service('cephrbdmirrors', 'default-rbd-mirror')
        service_type, service_id = service_name.split('.', 1)
        if service_type == 'mds':
            return self.rook_cluster.rm_service('cephfilesystems', service_id)
        elif service_type == 'rgw':
            return self.rook_cluster.rm_service('cephobjectstores', service_id)
        elif service_type == 'nfs':
            ret, out, err = self.mon_command({
                'prefix': 'auth ls'
            })
            matches = re.findall(rf'client\.nfs-ganesha\.{service_id}\..*', out)
            for match in matches:
                self.check_mon_command({
                    'prefix': 'auth rm',
                    'entity': match
                })
            return self.rook_cluster.rm_service('cephnfses', service_id)
        elif service_type == 'rbd-mirror':
            return self.rook_cluster.rm_service('cephrbdmirrors', service_id)
        elif service_type == 'osd':
            return f'Removed {service_name}'
        elif service_type == 'ingress':
            self.log.info("{0} service '{1}' does not exist".format('ingress', service_id))
            return 'The Rook orchestrator does not currently support ingress'
        else:
            raise orchestrator.OrchestratorError(f'Service type {service_type} not supported')

    def zap_device(self, host: str, path: str) -> OrchResult[str]:
        try:
            self.rook_cluster.create_zap_job(host, path)
        except Exception as e:
            logging.error(e)
            return OrchResult(None, Exception("Unable to zap device: " + str(e.with_traceback(None))))
        return OrchResult(f'{path} on {host} zapped') 

    @handle_orch_error
    def apply_mon(self, spec):
        # type: (ServiceSpec) -> str
        if spec.placement.hosts or spec.placement.label:
            raise RuntimeError("Host list or label is not supported by rook.")

        return self.rook_cluster.update_mon_count(spec.placement.count)

    def apply_rbd_mirror(self, spec: ServiceSpec) -> OrchResult[str]:
        try:
            self.rook_cluster.rbd_mirror(spec)
            return OrchResult("Success")
        except Exception as e:
            return OrchResult(None, e)

    @handle_orch_error
    def apply_mds(self, spec):
        # type: (ServiceSpec) -> str
        num_replicas, leaf_type = self._get_pool_params()
        return self.rook_cluster.apply_filesystem(spec, num_replicas, leaf_type)

    @handle_orch_error
    def apply_rgw(self, spec):
        # type: (RGWSpec) -> str
        num_replicas, leaf_type = self._get_pool_params()
        return self.rook_cluster.apply_objectstore(spec, num_replicas, leaf_type)

    @handle_orch_error
    def apply_nfs(self, spec):
        # type: (NFSServiceSpec) -> str
        try:
            return self.rook_cluster.apply_nfsgw(spec, self)
        except Exception as e:
            logging.error(e)
            return "Unable to create NFS daemon, check logs for more traceback\n" + str(e.with_traceback(None))

    @handle_orch_error
    def remove_daemons(self, names: List[str]) -> List[str]:
        return self.rook_cluster.remove_pods(names)

    def add_host_label(self, host: str, label: str) -> OrchResult[str]:
        return self.rook_cluster.add_host_label(host, label)

    def remove_host_label(self, host: str, label: str, force: bool = False) -> OrchResult[str]:
        return self.rook_cluster.remove_host_label(host, label)

    @handle_orch_error
    def create_osds(self, drive_group: DriveGroupSpec) -> str:
        raise orchestrator.OrchestratorError('Creating OSDs is not supported by rook orchestrator. Please, use Rook operator.')

    @handle_orch_error
    def remove_osds(self,
                    osd_ids: List[str],
                    replace: bool = False,
                    force: bool = False,
                    zap: bool = False,
                    no_destroy: bool = False) -> str:
        raise orchestrator.OrchestratorError('Removing OSDs is not supported by rook orchestrator. Please, use Rook operator.')

    @handle_orch_error
    def blink_device_light(self, ident_fault: str, on: bool, locs: List[orchestrator.DeviceLightLoc]) -> List[str]:
        return self.rook_cluster.blink_light(ident_fault, on, locs)
