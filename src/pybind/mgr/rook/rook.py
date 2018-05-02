
import threading
import functools

from mgr_module import MgrModule

import orchestrator

try:
    from kubernetes import client, config
    kubernetes_imported = True
except ImportError:
    kubernetes_imported = False

from rook_cluster import RookCluster, RookFilesystem


class RookReadCompletion(orchestrator.ReadCompletion):
    """
    All reads are simply API calls: avoid spawning
    huge numbers of threads by just running them
    inline when someone calls wait()
    """

    def __init__(self, cb):
        self.cb = cb

    def execute(self):
        self.result = self.cb()


class RookWriteCompletion(orchestrator.WriteCompletion):
    def __init__(self, cb):
        self.cb = cb

    def execute(self):
        self.result = self.cb()


def deferred_read(f):
    """
    Decorator to make RookOrchestrator methods return
    a completion object that executes themselves.
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        return RookReadCompletion(lambda: f(*args, **kwargs))

    return wrapper


def deferred_write(f):
    """
    Decorator to make RookOrchestrator methods return
    a completion object that executes themselves.
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        return RookWriteCompletion(lambda: f(*args, **kwargs))

    return wrapper


class RookOrchestrator(MgrModule, orchestrator.Orchestrator):
    OPTIONS = [
            {}  # TODO: configure k8s API addr instead of assuming local
    ]

    def wait(self, completions):
        # Our `wait` implementation is very simple because everything's
        # just an API call.
        for c in completions:
            c.execute()

    @staticmethod
    def can_run(self):
        if kubernetes_imported:
            return True, ""
        else:
            return False, "kubernetes module not found"

    def __init__(self, *args, **kwargs):
        super(RookOrchestrator, self).__init__(*args, **kwargs)

        self._initialized = threading.Event()
        self._k8s = None
        self._rook_cluster = None

    @property
    def k8s(self):
        self._initialized.wait()
        return self._k8s

    @property
    def rook_cluster(self):
        self._initialized.wait()
        return self._rook_cluster

    def serve(self):
        config.load_kube_config();  # Useful for devel
        #config.load_incluster_config();  # Useful for IRL

        self.k8s = client.CoreV1Api()

        # FIXME: introspect
        cluster_name = "ceph"

        self._rook_cluster = RookCluster(
                self.k8s, 
                cluster_name)

        # In case Rook isn't already clued in to this ceph
        # cluster's existence, initialize it.
        self._rook_cluster.init_rook()

        self._initialized.set()

        # TODO: watch Rook for config changes to complain/update if
        # things look a bit out of sync?

    @deferred_read
    def get_inventory(self, node_filter=None):
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
                dev = orchestrator.InventoryBlockDevice()

                # XXX CAUTION!  https://github.com/rook/rook/issues/1716
                # Passing this through for the sake of completeness but it
                # is not trustworthy!
                dev.blank = d['empty']
                dev.type = 'hdd' if d['rotational'] else 'ssd'
                dev.id = d['name']
                dev.size = d['size']

                if d['filesystem'] == "" and not d['rotational']:
                    # Empty or partitioned SSD
                    partitioned_space = sum([p['size'] for p in d['Partitions']])
                    dev.metadata_space_free = max(0, d['size'] - partitioned_space)


            result.append(orchestrator.InventoryNode(node_name, devs))

        return result

    def _add_mds_service(spec):
        # TODO use spec.placement
        # TODO use spec.min_size, max_size
        # TODO warn if spec.extended has entries we don't kow how
        #      to action.
        pass

    def add_stateless_service(self, service_type, spec):
        assert isinstance(spec, orchestrator.StatelessServiceSpec)

        if service_type == "mds":
            return self._add_mds_service(spec)
        else:
            # TODO: RGW, NFS
            raise NotImplementedError(service_type)

    @deferred_read
    def describe_service(self, service_type, service_id):
        # Initially only support MDS (via Rook's filesystem entity)
        assert service_type == "mds"  # TODO

        # Assume "service ID" in MDS context will be a filesystem
        # ID.

        # Resolve filesystem ID to filesystem name, because Rook
        # labels using name

        #rfs = RookFilesystem(

