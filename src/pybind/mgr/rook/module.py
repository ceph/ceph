import threading
import functools
import os
import uuid
try:
    from typing import List
except ImportError:
    pass  # just for type checking

try:
    from kubernetes import client, config
    from kubernetes.client.rest import ApiException

    kubernetes_imported = True
except ImportError:
    kubernetes_imported = False
    client = None
    config = None

from mgr_module import MgrModule
import orchestrator

from .rook_cluster import RookCluster


all_completions = []


class RookReadCompletion(orchestrator.ReadCompletion):
    """
    All reads are simply API calls: avoid spawning
    huge numbers of threads by just running them
    inline when someone calls wait()
    """

    def __init__(self, cb):
        super(RookReadCompletion, self).__init__()
        self.cb = cb
        self._result = None
        self._complete = False

        self.message = "<read op>"

        # XXX hacky global
        global all_completions
        all_completions.append(self)

    @property
    def result(self):
        return self._result

    @property
    def is_complete(self):
        return self._complete

    def execute(self):
        self._result = self.cb()
        self._complete = True


class RookWriteCompletion(orchestrator.WriteCompletion):
    """
    Writes are a two-phase thing, firstly sending
    the write to the k8s API (fast) and then waiting
    for the corresponding change to appear in the
    Ceph cluster (slow)
    """
    # XXX kubernetes bindings call_api already usefully has
    # a completion= param that uses threads.  Maybe just
    # use that?
    def __init__(self, execute_cb, complete_cb, message):
        super(RookWriteCompletion, self).__init__()
        self.execute_cb = execute_cb
        self.complete_cb = complete_cb

        # Executed means I executed my k8s API call, it may or may
        # not have succeeded
        self.executed = False

        # Result of k8s API call, this is set if executed==True
        self._result = None

        self.effective = False

        self.id = str(uuid.uuid4())

        self.message = message

        self.error = None

        # XXX hacky global
        global all_completions
        all_completions.append(self)

    @property
    def result(self):
        return self._result

    @property
    def is_persistent(self):
        return (not self.is_errored) and self.executed

    @property
    def is_effective(self):
        return self.effective

    @property
    def is_errored(self):
        return self.error is not None

    def execute(self):
        if not self.executed:
            self._result = self.execute_cb()
            self.executed = True

        if not self.effective:
            # TODO: check self.result for API errors
            if self.complete_cb is None:
                self.effective = True
            else:
                self.effective = self.complete_cb()


def deferred_read(f):
    """
    Decorator to make RookOrchestrator methods return
    a completion object that executes themselves.
    """

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        return RookReadCompletion(lambda: f(*args, **kwargs))

    return wrapper


class RookOrchestrator(MgrModule, orchestrator.Orchestrator):
    MODULE_OPTIONS = [
        # TODO: configure k8s API addr instead of assuming local
    ]

    def _progress(self, *args, **kwargs):
        try:
            self.remote("progress", *args, **kwargs)
        except ImportError:
            # If the progress module is disabled that's fine,
            # they just won't see the output.
            pass

    def wait(self, completions):
        self.log.info("wait: completions={0}".format(completions))

        incomplete = False

        # Our `wait` implementation is very simple because everything's
        # just an API call.
        for c in completions:
            if not isinstance(c, RookReadCompletion) and \
                    not isinstance(c, RookWriteCompletion):
                raise TypeError(
                    "wait() requires list of completions, not {0}".format(
                        c.__class__
                    ))

            if c.is_complete:
                continue

            if not c.is_read:
                self._progress("update", c.id, c.message, 0.5)

            try:
                c.execute()
            except Exception as e:
                self.log.exception("Completion {0} threw an exception:".format(
                    c.message
                ))
                c.error = e
                c._complete = True
                if not c.is_read:
                    self._progress("complete", c.id)
            else:
                if c.is_complete:
                    if not c.is_read:
                        self._progress("complete", c.id)

            if not c.is_complete:
                incomplete = True

        return not incomplete

    @staticmethod
    def can_run():
        if kubernetes_imported:
            return True, ""
        else:
            return False, "`kubernetes` python module not found"

    def available(self):
        if not kubernetes_imported:
            return False, "`kubernetes` python module not found"
        elif not self._in_cluster_name:
            return False, "ceph-mgr not running in Rook cluster"

        try:
            self.k8s.list_namespaced_pod(self.rook_cluster.cluster_name)
        except ApiException as e:
            return False, "Cannot reach Kubernetes API: {}".format(e)
        else:
            return True, ""

    def __init__(self, *args, **kwargs):
        super(RookOrchestrator, self).__init__(*args, **kwargs)

        self._initialized = threading.Event()
        self._k8s = None
        self._rook_cluster = None

        self._shutdown = threading.Event()

    def shutdown(self):
        self._shutdown.set()

    @property
    def k8s(self):
        self._initialized.wait()
        return self._k8s

    @property
    def rook_cluster(self):
        self._initialized.wait()
        return self._rook_cluster

    @property
    def _in_cluster_name(self):
        """
        Check if we appear to be running inside a Kubernetes/Rook
        cluster

        :return: str
        """
        if 'POD_NAMESPACE' in os.environ:
            return os.environ['POD_NAMESPACE']
        if 'ROOK_CLUSTER_NAME' in os.environ:
            return os.environ['ROOK_CLUSTER_NAME']

    def serve(self):
        # For deployed clusters, we should always be running inside
        # a Rook cluster.  For development convenience, also support
        # running outside (reading ~/.kube config)

        if self._in_cluster_name:
            config.load_incluster_config()
            cluster_name = self._in_cluster_name
        else:
            self.log.warning("DEVELOPMENT ONLY: Reading kube config from ~")
            config.load_kube_config()

            cluster_name = "rook"

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
            cluster_name)

        self._initialized.set()

        while not self._shutdown.is_set():
            # XXX hack (or is it?) to kick all completions periodically,
            # in case we had a caller that wait()'ed on them long enough
            # to get persistence but not long enough to get completion

            global all_completions
            self.wait(all_completions)
            all_completions = [c for c in all_completions if not c.is_complete]

            self._shutdown.wait(5)

        # TODO: watch Rook for config changes to complain/update if
        # things look a bit out of sync?

    @deferred_read
    def get_inventory(self, node_filter=None, refresh=False):
        node_list = None
        if node_filter and node_filter.nodes:
            # Explicit node list
            node_list = node_filter.nodes
        elif node_filter and node_filter.labels:
            # TODO: query k8s API to resolve to node list, and pass
            # it into RookCluster.get_discovered_devices
            raise NotImplementedError()

        devs = self.rook_cluster.get_discovered_devices(node_list)

        result = []
        for node_name, node_devs in devs.items():
            devs = []
            for d in node_devs:
                dev = orchestrator.InventoryDevice()

                # XXX CAUTION!  https://github.com/rook/rook/issues/1716
                # Passing this through for the sake of completeness but it
                # is not trustworthy!
                dev.blank = d['empty']
                dev.type = 'hdd' if d['rotational'] else 'ssd'
                dev.id = d['name']
                dev.size = d['size']

                if d['filesystem'] == "" and not d['rotational']:
                    # Empty or partitioned SSD
                    partitioned_space = sum(
                        [p['size'] for p in d['Partitions']])
                    dev.metadata_space_free = max(0, d[
                        'size'] - partitioned_space)

                devs.append(dev)

            result.append(orchestrator.InventoryNode(node_name, devs))

        return result

    @deferred_read
    def describe_service(self, service_type, service_id, nodename):

        assert service_type in ("mds", "osd", "mgr", "mon", "nfs", None), service_type + " unsupported"

        pods = self.rook_cluster.describe_pods(service_type, service_id, nodename)

        result = []
        for p in pods:
            sd = orchestrator.ServiceDescription()
            sd.nodename = p['nodename']
            sd.container_id = p['name']
            sd.service_type = p['labels']['app'].replace('rook-ceph-', '')

            if sd.service_type == "osd":
                sd.service_instance = "%s" % p['labels']["ceph-osd-id"]
            elif sd.service_type == "mds":
                sd.service = p['labels']['rook_file_system']
                pfx = "{0}-".format(sd.service)
                sd.service_instance = p['labels']['ceph_daemon_id'].replace(pfx, '', 1)
            elif sd.service_type == "mon":
                sd.service_instance = p['labels']["mon"]
            elif sd.service_type == "mgr":
                sd.service_instance = p['labels']["mgr"]
            elif sd.service_type == "nfs":
                sd.service = p['labels']['ceph_nfs']
                sd.service_instance = p['labels']['instance']
                sd.rados_config_location = self.rook_cluster.get_nfs_conf_url(sd.service, sd.service_instance)
            else:
                # Unknown type -- skip it
                continue

            result.append(sd)

        return result

    def _service_add_decorate(self, typename, spec, func):
        return RookWriteCompletion(lambda: func(spec), None,
                    "Creating {0} services for {1}".format(typename, spec.name))

    def add_stateless_service(self, service_type, spec):
        # assert isinstance(spec, orchestrator.StatelessServiceSpec)
        if service_type == "mds":
            return self._service_add_decorate("Filesystem", spec,
                                         self.rook_cluster.add_filesystem)
        elif service_type == "rgw" :
            return self._service_add_decorate("RGW", spec,
                                         self.rook_cluster.add_objectstore)
        elif service_type == "nfs" :
            return self._service_add_decorate("NFS", spec,
                                         self.rook_cluster.add_nfsgw)
        else:
            raise NotImplementedError(service_type)

    def remove_stateless_service(self, service_type, service_id):
        return RookWriteCompletion(
            lambda: self.rook_cluster.rm_service(service_type, service_id), None,
            "Removing {0} services for {1}".format(service_type, service_id))

    def create_osds(self, drive_group, all_hosts):
        # type: (orchestrator.DriveGroupSpec, List[str]) -> RookWriteCompletion

        assert len(drive_group.hosts(all_hosts)) == 1
        if not self.rook_cluster.node_exists(drive_group.hosts(all_hosts)[0]):
            raise RuntimeError("Node '{0}' is not in the Kubernetes "
                               "cluster".format(drive_group.hosts(all_hosts)))

        # Validate whether cluster CRD can accept individual OSD
        # creations (i.e. not useAllDevices)
        if not self.rook_cluster.can_create_osd():
            raise RuntimeError("Rook cluster configuration does not "
                               "support OSD creation.")

        def execute():
            return self.rook_cluster.add_osds(drive_group, all_hosts)

        def is_complete():
            # Find OSD pods on this host
            pod_osd_ids = set()
            pods = self._k8s.list_namespaced_pod("rook-ceph",
                                                 label_selector="rook_cluster=rook-ceph,app=rook-ceph-osd",
                                                 field_selector="spec.nodeName={0}".format(
                                                     drive_group.hosts(all_hosts)[0]
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
                if metadata and metadata['devices'] in drive_group.data_devices.paths:
                    found.append(osd_id)
                else:
                    self.log.info("ignoring osd {0} {1}".format(
                        osd_id, metadata['devices']
                    ))

            return found is not None

        return RookWriteCompletion(execute, is_complete,
                                   "Creating OSD on {0}:{1}".format(
                                       drive_group.hosts(all_hosts)[0],
                                       drive_group.data_devices.paths
                                   ))
