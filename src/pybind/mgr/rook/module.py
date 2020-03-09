import threading
import functools
import os

from ceph.deployment import inventory
from ceph.deployment.service_spec import ServiceSpec, NFSServiceSpec, RGWSpec

try:
    from typing import List, Dict, Optional, Callable, Any
    from ceph.deployment.drive_group import DriveGroupSpec
except ImportError:
    pass  # just for type checking

try:
    from kubernetes import client, config
    from kubernetes.client.rest import ApiException

    kubernetes_imported = True

    # https://github.com/kubernetes-client/python/issues/895
    from kubernetes.client.models.v1_container_image import V1ContainerImage
    def names(self, names):
        self._names = names
    V1ContainerImage.names = V1ContainerImage.names.setter(names)

except ImportError:
    kubernetes_imported = False
    client = None
    config = None

from mgr_module import MgrModule
import orchestrator

from .rook_cluster import RookCluster


class RookCompletion(orchestrator.Completion):
    def evaluate(self):
        self.finalize(None)


def deferred_read(f):
    # type: (Callable) -> Callable[..., RookCompletion]
    """
    Decorator to make RookOrchestrator methods return
    a completion object that executes themselves.
    """

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        return RookCompletion(on_complete=lambda _: f(*args, **kwargs))

    return wrapper


def write_completion(on_complete,  # type: Callable
                     message,  # type: str
                     mgr,
                     calc_percent=None  # type: Optional[Callable[[], RookCompletion]]
                     ):
    # type: (...) -> RookCompletion
    return RookCompletion.with_progress(
        message=message,
        mgr=mgr,
        on_complete=lambda _: on_complete(),
        calc_percent=calc_percent,
    )


class RookEnv(object):
    def __init__(self):
        # POD_NAMESPACE already exist for Rook 0.9
        self.namespace = os.environ.get('POD_NAMESPACE', 'rook-ceph')

        # ROOK_CEPH_CLUSTER_CRD_NAME is new is Rook 1.0
        self.cluster_name = os.environ.get('ROOK_CEPH_CLUSTER_CRD_NAME', self.namespace)

        self.operator_namespace = os.environ.get('ROOK_OPERATOR_NAMESPACE', self.namespace)
        self.crd_version = os.environ.get('ROOK_CEPH_CLUSTER_CRD_VERSION', 'v1')
        self.api_name = "ceph.rook.io/" + self.crd_version

    def api_version_match(self):
        return self.crd_version == 'v1'

    def has_namespace(self):
        return 'POD_NAMESPACE' in os.environ


class RookOrchestrator(MgrModule, orchestrator.Orchestrator):
    """
    Writes are a two-phase thing, firstly sending
    the write to the k8s API (fast) and then waiting
    for the corresponding change to appear in the
    Ceph cluster (slow)

    Right now, wre calling the k8s API synchronously.
    """

    MODULE_OPTIONS = [
        # TODO: configure k8s API addr instead of assuming local
    ]  # type: List[Dict[str, Any]]

    def process(self, completions):
        # type: (List[RookCompletion]) -> None

        if completions:
            self.log.info("process: completions={0}".format(orchestrator.pretty_print(completions)))

            for p in completions:
                p.evaluate()

    @staticmethod
    def can_run():
        if not kubernetes_imported:
            return False, "`kubernetes` python module not found"
        if not RookEnv().api_version_match():
            return False, "Rook version unsupported."
        return True, ''

    def available(self):
        if not kubernetes_imported:
            return False, "`kubernetes` python module not found"
        elif not self._rook_env.has_namespace():
            return False, "ceph-mgr not running in Rook cluster"

        try:
            self.k8s.list_namespaced_pod(self._rook_env.cluster_name)
        except ApiException as e:
            return False, "Cannot reach Kubernetes API: {}".format(e)
        else:
            return True, ""

    def __init__(self, *args, **kwargs):
        super(RookOrchestrator, self).__init__(*args, **kwargs)

        self._initialized = threading.Event()
        self._k8s = None
        self._rook_cluster = None
        self._rook_env = RookEnv()

        self._shutdown = threading.Event()

        self.all_progress_references = list()  # type: List[orchestrator.ProgressReference]

    def shutdown(self):
        self._shutdown.set()

    @property
    def k8s(self):
        # type: () -> client.CoreV1Api
        self._initialized.wait()
        assert self._k8s is not None
        return self._k8s

    @property
    def rook_cluster(self):
        # type: () -> RookCluster
        self._initialized.wait()
        assert self._rook_cluster is not None
        return self._rook_cluster

    def serve(self):
        # For deployed clusters, we should always be running inside
        # a Rook cluster.  For development convenience, also support
        # running outside (reading ~/.kube config)

        if self._rook_env.has_namespace():
            config.load_incluster_config()
            cluster_name = self._rook_env.cluster_name
        else:
            self.log.warning("DEVELOPMENT ONLY: Reading kube config from ~")
            config.load_kube_config()

            cluster_name = "rook-ceph"

            # So that I can do port forwarding from my workstation - jcsp
            from kubernetes.client import configuration
            configuration.verify_ssl = False

        self._k8s = client.CoreV1Api()

        try:
            # XXX mystery hack -- I need to do an API call from
            # this context, or subsequent API usage from handle_command
            # fails with SSLError('bad handshake').  Suspect some kind of
            # thread context setup in SSL lib?
            self._k8s.list_namespaced_pod(cluster_name)
        except ApiException:
            # Ignore here to make self.available() fail with a proper error message
            pass

        self._rook_cluster = RookCluster(
            self._k8s,
            self._rook_env)

        self._initialized.set()

        while not self._shutdown.is_set():
            # XXX hack (or is it?) to kick all completions periodically,
            # in case we had a caller that wait()'ed on them long enough
            # to get persistence but not long enough to get completion

            self.all_progress_references = [p for p in self.all_progress_references if not p.effective]
            for p in self.all_progress_references:
                p.update()

            self._shutdown.wait(5)

    def cancel_completions(self):
        for p in self.all_progress_references:
            p.fail()
        self.all_progress_references.clear()

    @deferred_read
    def get_inventory(self, host_filter=None, refresh=False):
        host_list = None
        if host_filter and host_filter.hosts:
            # Explicit host list
            host_list = host_filter.hosts
        elif host_filter and host_filter.labels:
            # TODO: query k8s API to resolve to host list, and pass
            # it into RookCluster.get_discovered_devices
            raise NotImplementedError()

        devs = self.rook_cluster.get_discovered_devices(host_list)

        result = []
        for host_name, host_devs in devs.items():
            devs = []
            for d in host_devs:
                dev = inventory.Device(
                    path='/dev/' + d['name'],
                    sys_api=dict(
                        rotational='1' if d['rotational'] else '0',
                        size=d['size']
                    ),
                    available=d['empty'],
                    rejected_reasons=[] if d['empty'] else ['not empty'],
                )
                devs.append(dev)

            result.append(orchestrator.InventoryHost(host_name, inventory.Devices(devs)))

        return result

    @deferred_read
    def get_hosts(self):
        # type: () -> List[orchestrator.HostSpec]
        return [orchestrator.HostSpec(n) for n in self.rook_cluster.get_node_names()]

    @deferred_read
    def list_daemons(self, daemon_type=None, daemon_id=None, host_name=None, refresh=False):

        pods = self.rook_cluster.describe_pods(daemon_type, daemon_id, host_name)

        result = []
        for p in pods:
            sd = orchestrator.DaemonDescription()
            sd.hostname = p['hostname']
            sd.container_id = p['name']
            sd.daemon_type = p['labels']['app'].replace('rook-ceph-', '')
            status = {
                'Pending': -1,
                'Running': 1,
                'Succeeded': 0,
                'Failed': -1,
                'Unknown': -1,
            }[p['phase']]
            sd.status = status
            sd.status_desc = p['phase']

            if sd.daemon_type == "osd":
                sd.daemon_id = "%s" % p['labels']["ceph-osd-id"]
            elif sd.daemon_type == "mds":
                pfx = "{0}-".format(p['labels']['rook_file_system'])
                sd.daemon_id = p['labels']['ceph_daemon_id'].replace(pfx, '', 1)
            elif sd.daemon_type == "mon":
                sd.daemon_id = p['labels']["mon"]
            elif sd.daemon_type == "mgr":
                sd.daemon_id = p['labels']["mgr"]
            elif sd.daemon_type == "nfs":
                sd.daemon_id = p['labels']['instance']
            elif sd.daemon_type == "rgw":
                sd.daemon_id = p['labels']['ceph_daemon_id']
            else:
                # Unknown type -- skip it
                continue

            result.append(sd)

        return result

    def _service_add_decorate(self, typename, spec, func):
        return write_completion(
            on_complete=lambda : func(spec),
            message="Creating {} services for {}".format(typename, spec.service_id),
            mgr=self
        )

    def add_mds(self, spec):
        # type: (ServiceSpec) -> RookCompletion
        return self._service_add_decorate('MDS', spec,
                                       self.rook_cluster.add_filesystem)

    def add_rgw(self, spec):
        # type: (RGWSpec) -> RookCompletion
        return self._service_add_decorate('RGW', spec,
                                       self.rook_cluster.add_objectstore)

    def add_nfs(self, spec):
        # type: (NFSServiceSpec) -> RookCompletion
        return self._service_add_decorate("NFS", spec,
                                          self.rook_cluster.add_nfsgw)

    def _service_rm_decorate(self, typename, name, func):
        return write_completion(
            on_complete=lambda : func(name),
            message="Removing {} services for {}".format(typename, name),
            mgr=self
        )

    def remove_service(self, service_type, service_name):
        if service_type == 'mds':
            return self._service_rm_decorate(
                'MDS', service_name, lambda: self.rook_cluster.rm_service(
                    'cephfilesystems', service_name)
            )
        elif service_type == 'rgw':
            return self._service_rm_decorate(
                'RGW', service_name, lambda: self.rook_cluster.rm_service('cephobjectstores', service_name)
            )
        elif service_type == 'nfs':
            return self._service_rm_decorate(
                'NFS', service_name, lambda: self.rook_cluster.rm_service('cephnfses', service_name)
            )

    def apply_mon(self, spec):
        # type: (ServiceSpec) -> RookCompletion
        if spec.placement.hosts or spec.placement.label:
            raise RuntimeError("Host list or label is not supported by rook.")

        return write_completion(
            lambda: self.rook_cluster.update_mon_count(spec.placement.count),
            "Updating mon count to {0}".format(spec.placement.count),
            mgr=self
        )

    def apply_mds(self, spec):
        # type: (ServiceSpec) -> RookCompletion
        num = spec.placement.count
        return write_completion(
            lambda: self.rook_cluster.update_mds_count(spec.service_id, num),
            "Updating MDS server count in {0} to {1}".format(spec.service_id, num),
            mgr=self
        )

    def apply_nfs(self, spec):
        # type: (NFSServiceSpec) -> RookCompletion
        num = spec.placement.count
        return write_completion(
            lambda: self.rook_cluster.update_nfs_count(spec.service_id, num),
            "Updating NFS server count in {0} to {1}".format(spec.service_id, num),
            mgr=self
        )

    def create_osds(self, drive_groups):
        # type: (List[DriveGroupSpec]) -> RookCompletion
        """ Creates OSDs from a drive group specification.

        Caveat: Currently limited to a single DriveGroup.
        The orchestrator_cli expects a single completion which
        ideally represents a set of operations. This orchestrator
        doesn't support this notion, yet. Hence it's only accepting
        a single DriveGroup for now.
        You can work around it by invoking:

        $: ceph orch osd create -i <dg.file>

        multiple times. The drivegroup file must only contain one spec at a time.
        """
        drive_group = drive_groups[0]

        targets = []  # type: List[str]
        if drive_group.data_devices and drive_group.data_devices.paths:
            targets += [d.path for d in drive_group.data_devices.paths]
        if drive_group.data_directories:
            targets += drive_group.data_directories

        def execute(all_hosts_):
            # type: (List[orchestrator.HostSpec]) -> orchestrator.Completion
            all_hosts = [h.hostname for h in all_hosts_]
            matching_hosts = drive_group.placement.pattern_matches_hosts(all_hosts)

            assert len(matching_hosts) == 1

            if not self.rook_cluster.node_exists(matching_hosts[0]):
                raise RuntimeError("Node '{0}' is not in the Kubernetes "
                                   "cluster".format(matching_hosts))

            # Validate whether cluster CRD can accept individual OSD
            # creations (i.e. not useAllDevices)
            if not self.rook_cluster.can_create_osd():
                raise RuntimeError("Rook cluster configuration does not "
                                   "support OSD creation.")

            return orchestrator.Completion.with_progress(
                message="Creating OSD on {0}:{1}".format(
                        matching_hosts,
                        targets),
                mgr=self,
                on_complete=lambda _:self.rook_cluster.add_osds(drive_group, all_hosts),
                calc_percent=lambda: has_osds(all_hosts)
            )

        @deferred_read
        def has_osds(all_hosts):
            matching_hosts = drive_group.placement.pattern_matches_hosts(all_hosts)

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
                        osd_id, metadata['devices']
                    ))

            return found is not None

        c = self.get_hosts().then(execute)
        return c
