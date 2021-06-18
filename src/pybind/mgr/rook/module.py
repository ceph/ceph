import threading
import functools
import os
import json

from ceph.deployment import inventory
from ceph.deployment.service_spec import ServiceSpec, NFSServiceSpec, RGWSpec, PlacementSpec
from ceph.utils import datetime_now

from typing import List, Dict, Optional, Callable, Any, TypeVar, Tuple

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

from mgr_module import MgrModule, Option
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
        self._rook_cluster: Optional[RookCluster] = None
        self._rook_env = RookEnv()

        self._shutdown = threading.Event()

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

        try:
            # XXX mystery hack -- I need to do an API call from
            # this context, or subsequent API usage from handle_command
            # fails with SSLError('bad handshake').  Suspect some kind of
            # thread context setup in SSL lib?
            self._k8s_CoreV1_api.list_namespaced_pod(self._rook_env.namespace)
        except ApiException:
            # Ignore here to make self.available() fail with a proper error message
            pass

        self._rook_cluster = RookCluster(
            self._k8s_CoreV1_api,
            self._k8s_BatchV1_api,
            self._rook_env)

        self._initialized.set()

        while not self._shutdown.is_set():
            self._shutdown.wait(5)

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
                if 'cephVolumeData' in d and d['cephVolumeData']:
                    devs.append(inventory.Device.from_json(json.loads(d['cephVolumeData'])))
                else:
                    devs.append(inventory.Device(
                        path = '/dev/' + d['name'],
                        sys_api = dict(
                            rotational = '1' if d['rotational'] else '0',
                            size = d['size']
                        ),
                        available = False,
                        rejected_reasons=['device data coming from ceph-volume not provided'],
                    ))

            result.append(orchestrator.InventoryHost(host_name, inventory.Devices(devs)))

        return result

    @handle_orch_error
    def get_hosts(self):
        # type: () -> List[orchestrator.HostSpec]
        return [orchestrator.HostSpec(n) for n in self.rook_cluster.get_node_names()]

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
            )
        if not cl['spec'].get('crashCollector', {}).get('disable', False):
            spec['crash'] = orchestrator.ServiceDescription(
                spec=ServiceSpec(
                    'crash',
                    placement=PlacementSpec.from_string('*'),
                ),
                size=num_nodes,
                container_image_name=image_name,
                last_refresh=now,
            )

        if service_type == 'mds' or service_type is None:
            # CephFilesystems
            all_fs = self.rook_cluster.rook_api_get(
                "cephfilesystems/")
            self.log.debug('CephFilesystems %s' % all_fs)
            for fs in all_fs.get('items', []):
                svc = 'mds.' + fs['metadata']['name']
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
                    )

        if service_type == 'rgw' or service_type is None:
            # CephObjectstores
            all_zones = self.rook_cluster.rook_api_get(
                "cephobjectstores/")
            self.log.debug('CephObjectstores %s' % all_zones)
            for zone in all_zones.get('items', []):
                rgw_realm = zone['metadata']['name']
                rgw_zone = rgw_realm
                svc = 'rgw.' + rgw_realm + '.' + rgw_zone
                if svc in spec:
                    continue
                active = zone['spec']['gateway']['instances'];
                if 'securePort' in zone['spec']['gateway']:
                    ssl = True
                    port = zone['spec']['gateway']['securePort']
                else:
                    ssl = False
                    port = zone['spec']['gateway']['port'] or 80
                spec[svc] = orchestrator.ServiceDescription(
                    spec=RGWSpec(
                        service_id=rgw_realm + '.' + rgw_zone,
                        rgw_realm=rgw_realm,
                        rgw_zone=rgw_zone,
                        ssl=ssl,
                        rgw_frontend_port=port,
                        placement=PlacementSpec(count=active),
                    ),
                    size=active,
                    container_image_name=image_name,
                    last_refresh=now,
                )

        if service_type == 'nfs' or service_type is None:
            # CephNFSes
            all_nfs = self.rook_cluster.rook_api_get(
                "cephnfses/")
            self.log.warning('CephNFS %s' % all_nfs)
            for nfs in all_nfs.get('items', []):
                nfs_name = nfs['metadata']['name']
                svc = 'nfs.' + nfs_name
                if svc in spec:
                    continue
                active = nfs['spec'].get('server', {}).get('active')
                spec[svc] = orchestrator.ServiceDescription(
                    spec=NFSServiceSpec(
                        service_id=nfs_name,
                        pool=nfs['spec']['rados']['pool'],
                        namespace=nfs['spec']['rados'].get('namespace', None),
                        placement=PlacementSpec(count=active),
                    ),
                    size=active,
                    last_refresh=now,
                )

        for dd in self._list_daemons():
            if dd.service_name() not in spec:
                continue
            service = spec[dd.service_name()]
            service.running += 1
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
        pods = self.rook_cluster.describe_pods(daemon_type, daemon_id, host)
        self.log.debug('pods %s' % pods)
        result = []
        for p in pods:
            sd = orchestrator.DaemonDescription()
            sd.hostname = p['hostname']
            sd.daemon_type = p['labels']['app'].replace('rook-ceph-', '')
            status = {
                'Pending': orchestrator.DaemonDescriptionStatus.error,
                'Running': orchestrator.DaemonDescriptionStatus.running,
                'Succeeded': orchestrator.DaemonDescriptionStatus.stopped,
                'Failed': orchestrator.DaemonDescriptionStatus.error,
                'Unknown': orchestrator.DaemonDescriptionStatus.error,
            }[p['phase']]
            sd.status = status
            sd.status_desc = p['phase']

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

    @handle_orch_error
    def remove_service(self, service_name: str) -> str:
        service_type, service_name = service_name.split('.', 1)
        if service_type == 'mds':
            return self.rook_cluster.rm_service('cephfilesystems', service_name)
        elif service_type == 'rgw':
            return self.rook_cluster.rm_service('cephobjectstores', service_name)
        elif service_type == 'nfs':
            return self.rook_cluster.rm_service('cephnfses', service_name)
        else:
            raise orchestrator.OrchestratorError(f'Service type {service_type} not supported')

    @handle_orch_error
    def apply_mon(self, spec):
        # type: (ServiceSpec) -> str
        if spec.placement.hosts or spec.placement.label:
            raise RuntimeError("Host list or label is not supported by rook.")

        return self.rook_cluster.update_mon_count(spec.placement.count)

    @handle_orch_error
    def apply_mds(self, spec):
        # type: (ServiceSpec) -> str
        return self.rook_cluster.apply_filesystem(spec)

    @handle_orch_error
    def apply_rgw(self, spec):
        # type: (RGWSpec) -> str
        return self.rook_cluster.apply_objectstore(spec)

    @handle_orch_error
    def apply_nfs(self, spec):
        # type: (NFSServiceSpec) -> str
        return self.rook_cluster.apply_nfsgw(spec)

    @handle_orch_error
    def remove_daemons(self, names: List[str]) -> List[str]:
        return self.rook_cluster.remove_pods(names)
    """
    @handle_orch_error
    def create_osds(self, drive_group):
        # type: (DriveGroupSpec) -> str
        # Creates OSDs from a drive group specification.

        # $: ceph orch osd create -i <dg.file>

        # The drivegroup file must only contain one spec at a time.
        # 

        targets = []  # type: List[str]
        if drive_group.data_devices and drive_group.data_devices.paths:
            targets += [d.path for d in drive_group.data_devices.paths]
        if drive_group.data_directories:
            targets += drive_group.data_directories

        all_hosts = raise_if_exception(self.get_hosts())

        matching_hosts = drive_group.placement.filter_matching_hosts(lambda label=None, as_hostspec=None: all_hosts)

        assert len(matching_hosts) == 1

        if not self.rook_cluster.node_exists(matching_hosts[0]):
            raise RuntimeError("Node '{0}' is not in the Kubernetes "
                               "cluster".format(matching_hosts))

        # Validate whether cluster CRD can accept individual OSD
        # creations (i.e. not useAllDevices)
        if not self.rook_cluster.can_create_osd():
            raise RuntimeError("Rook cluster configuration does not "
                               "support OSD creation.")

        return self.rook_cluster.add_osds(drive_group, matching_hosts)

        # TODO: this was the code to update the progress reference:
        
        @handle_orch_error
        def has_osds(matching_hosts: List[str]) -> bool:

            # Find OSD pods on this host
            pod_osd_ids = set()
            pods = self.k8s.list_namespaced_pod(self._rook_env.namespace,
                                                label_selector="rook_cluster={},app=rook-ceph-osd".format(self._rook_env.cluster_name),
                                                field_selector="spec.nodeName={0}".format(
                                                    matching_hosts[0]
                                                )).items
            for p in pods:
                pod_osd_ids.add(int(p.metadata.labels['ceph-osd-id']))

            self.log.debug('pod_osd_ids={0}'.format(pod_osd_ids))

            found = []
            osdmap = self.get("osd_map")
            for osd in osdmap['osds']:
                osd_id = osd['osd']
                if osd_id not in pod_osd_ids:
                    continue

                metadata = self.get_metadata('osd', "%s" % osd_id)
                if metadata and metadata['devices'] in targets:
                    found.append(osd_id)
                else:
                    self.log.info("ignoring osd {0} {1}".format(
                        osd_id, metadata['devices'] if metadata else 'DNE'
                    ))

            return found is not None        
    """

    @handle_orch_error
    def blink_device_light(self, ident_fault: str, on: bool, locs: List[orchestrator.DeviceLightLoc]) -> List[str]:
        return self.rook_cluster.blink_light(ident_fault, on, locs)
