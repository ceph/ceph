
"""
ceph-mgr orchestrator interface

Please see the ceph-mgr module developer's guide for more information.
"""


class _Completion(object):
    def get_result(self):
        """
        Return the result of the operation that we were waited
        for.  Only valid after calling Orchestrator.wait() on this
        completion.
        """
        raise NotImplementedError()

    @property
    def is_read(self):
        raise NotImplementedError()

    @property
    def is_complete(self):
        raise NotImplementedError()

    @property
    def is_errored(self):
        raise NotImplementedError()


class ReadCompletion(_Completion):
    """
    ``Orchestrator`` implementations should inherit from this
    class to implement their own handles to operations in progress, and
    return an instance of their subclass from calls into methods.

    Read operations are
    """

    def __init__(self):
        pass

    @property
    def is_read(self):
        return True


class WriteCompletion(_Completion):
    """
    ``Orchestrator`` implementations should inherit from this
    class to implement their own handles to operations in progress, and
    return an instance of their subclass from calls into methods.
    """

    def __init__(self):
        pass

    @property
    def is_persistent(self):
        """
        Has the operation updated the orchestrator's configuration
        persistently?  Typically this would indicate that an update
        had been written to a manifest, but that the update
        had not necessarily been pushed out to the cluster.
        """
        raise NotImplementedError()

    @property
    def is_effective(self):
        """
        Has the operation taken effect on the cluster?  For example,
        if we were adding a service, has it come up and appeared
        in Ceph's cluster maps?
        """
        raise NotImplementedError()

    @property
    def is_complete(self):
        return self.is_errored or (self.is_persistent and self.is_effective)

    @property
    def is_read(self):
        return False


class Orchestrator(object):
    """
    Calls in this class may do long running remote operations, with time
    periods ranging from network latencies to package install latencies and large
    internet downloads.  For that reason, all are asynchronous, and return
    ``Completion`` objects.

    Implementations are not required to start work on an operation until
    the caller waits on the relevant Completion objects.  Callers making
    multiple updates should not wait on Completions until they're done
    sending operations: this enables implementations to batch up a series
    of updates when wait() is called on a set of Completion objects.

    Implementations are encouraged to keep reasonably fresh caches of
    the status of the system: it is better to serve a stale-but-recent
    result read of e.g. device inventory than it is to keep the caller waiting
    while you scan hosts every time.
    """

    def is_orchestrator_module(self):
        """
        Enable other modules to interrogate this module to discover
        whether it's usable as an orchestrator module.
        """
        return True

    def wait(self, completions):
        """
        Given a list of Completion instances, progress any which are
        incomplete.  Return a true if everything is done.

        Callers should inspect the detail of each completion to identify
        partial completion/progress information, and present that information
        to the user.

        For fast operations (e.g. reading from a database), implementations
        may choose to do blocking IO in this call.
        """
        raise NotImplementedError()

    def get_inventory(self, node_filter=None):
        """

        :param node_filter:
        :return: list of InventoryNode
        """
        raise NotImplementedError()

    def describe_service(self, service_type, service_id):
        """
        Describe a service (of any kind) that is already configured in
        the orchestrator.  For example, when viewing an OSD in the dashboard
        we might like to also display information about the orchestrator's
        view of the service (like the kubernetes pod ID).

        When viewing a CephFS filesystem in the dashboard, we would use this
        to display the pods being currently run for MDS daemons.
        """
        raise NotImplementedError()

    def create_osds(self, osd_spec):
        """
        Create one or more OSDs within a single Drive Group.

        The principal argument here is the drive_group member
        of OsdSpec: other fields are advisory/extensible for any
        finer-grained OSD feature enablement (choice of backing store,
        compression/encryption, etc).

        :param osd_spec: OsdCreationSpec
        :return:
        """
        raise NotImplementedError()

    def replace_osds(self, osd_spec):
        """
        Like create_osds, but the osd_id_claims must be fully
        populated.
        """
        raise NotImplementedError()

    def remove_osds(self, node, osd_ids):
        """
        :param node: A node name, must exist.
        :param osd_ids: list of OSD IDs

        Note that this can only remove OSDs that were successfully
        created (i.e. got an OSD ID).
        """
        raise NotImplementedError()

    def add_stateless_service(self, service_type, spec):
        assert isinstance(spec, StatelessServiceSpec)
        raise NotImplementedError()

    def update_stateless_service(self, service_type, id_, spec):
        assert isinstance(spec, StatelessServiceSpec)
        raise NotImplementedError()

    def remove_stateless_service(self, service_type, id_):
        raise NotImplementedError()

    def add_mon(self, node_name):
        """
        We operate on a node rather than a particular device: it is
        assumed/expected that proper SSD storage is already available
        and accessible in /var.

        :param node_name:
        :return:
        """
        raise NotImplementedError()

    def remove_mon(self, node_name):
        """

        :param node_name:
        :return:
        """
        raise NotImplementedError()

    def upgrade_start(self, upgrade_spec):
        assert isinstance(upgrade_spec, UpgradeSpec)
        raise NotImplementedError()

    def upgrade_status(self):
        """
        If an upgrade is currently underway, report on where
        we are in the process, or if some error has occurred.

        :return: UpgradeStatusSpec instance
        """
        raise NotImplementedError()

    def upgrade_available(self):
        """
        Report on what versions are available to upgrade to

        :return: List of strings
        """
        raise NotImplementedError()

    def add_stateful_service_rule(self, service_type, stateful_service_spec,
                                  placement_spec):
        """
        Stateful service rules serve two purposes:
         - Optionally delegate device selection to the orchestrator
         - Enable the orchestrator to auto-assimilate new hardware if it
           matches the placement spec, without any further calls from ceph-mgr.

        To create a confidence-inspiring UI workflow, use test_stateful_service_rule
        beforehand to show the user where stateful services will be placed
        if they proceed.
        """
        raise NotImplementedError()

    def test_stateful_service_rule(self, service_type, stateful_service_spec,
                                   placement_spec):
        """
        See add_stateful_service_rule.
        """
        raise NotImplementedError()

    def remove_stateful_service_rule(self, service_type, id_):
        """
        This will remove the *rule* but not the services that were
        created as a result.  Those should be converted into statically
        placed services as if they had been created with add_stateful_service,
        so that they can be removed with remove_stateless_service
        if desired.
        """
        raise NotImplementedError()


class UpgradeSpec(object):
    # Request to orchestrator to initiate an upgrade to a particular
    # version of Ceph
    def __init__(self):
        self.target_version = None


class UpgradeStatusSpec(object):
    # Orchestrator's report on what's going on with any ongoing upgrade
    def __init__(self):
        self.in_progress = False  # Is an upgrade underway?
        self.services_complete = []  # Which daemon types are fully updated?
        self.message = ""  # Freeform description


class PlacementSpec(object):
    """
    For APIs that need to specify a node subset
    """
    def __init__(self):
        self.label = None


class ServiceLocation(object):
    """
    See ServiceDescription
    """
    def __init__(self):
        # Node is at the same granularity as InventoryNode
        self.nodename = None

        # Not everyone runs in containers, but enough people do to
        # justify having this field here.
        self.container_id = None

        # The orchestrator will have picked some names for daemons,
        # typically either based on hostnames or on pod names.
        # This is the <foo> in mds.<foo>, the ID that will appear
        # in the FSMap/ServiceMap.
        self.daemon_name = None


class ServiceDescription(object):
    """
    For responding to queries about the status of a particular service,
    stateful or stateless.

    This is not about health or performance monitoring of services: it's
    about letting the orchestrator tell Ceph whether and where a 
    service is scheduled in the cluster.  When an orchestrator tells
    Ceph "it's running on node123", that's not a promise that the process
    is literally up this second, it's a description of where the orchestrator
    has decided the service should run.
    """

    def __init__(self):
        self.locations = []


class DriveGroupSpec(object):
    """
    Describe a drive group in the same form that ceph-volume
    understands.
    """
    def __init__(self, devices):
        self.devices = devices


class OsdCreationSpec(object):
    """
    Used during OSD creation.

    The drive names used here may be ephemeral.
    """
    def __init__(self):
        self.format = None  # filestore, bluestore

        self.node = None  # name of a node

        # List of device names
        self.drive_group = None

        # Optional: mapping of drive to OSD ID, used when the
        # created OSDs are meant to replace previous OSDs on
        # the same node.
        self.osd_id_claims = {}

        # Arbitrary JSON-serializable object.
        # Maybe your orchestrator knows how to do something
        # special like encrypting drives
        self.extended = {}


class StatelessServiceSpec(object):
    # Request to orchestrator for a group of stateless services
    # such as MDS, RGW, nfs gateway, iscsi gateway
    """
    Details of stateless service creation.

    This is *not* supposed to contain all the configuration
    of the services: it's just supposed to be enough information to
    execute the binaries.
    """

    def __init__(self):
        self.placement = PlacementSpec()

        # Give this set of statelss services a name: typically it would
        # be the name of a CephFS filesystem, RGW zone, etc.  Must be unique
        # within one ceph cluster.
        self.name = ""

        # Minimum and maximum number of service instances
        self.min_size = 1
        self.max_size = 1

        # Arbitrary JSON-serializable object.
        # Maybe you're using e.g. kubenetes and you want to pass through
        # some replicaset special sauce for autoscaling?
        self.extended = {}


class InventoryFilter(object):
    """
    When fetching inventory, use this filter to avoid unnecessarily
    scanning the whole estate.

    Typical use: filter by node when presenting UI workflow for configuring
                 a particular server.
                 filter by label when not all of estate is Ceph servers,
                 and we want to only learn about the Ceph servers.
                 filter by label when we are interested particularly
                 in e.g. OSD servers.

    """
    def __init__(self):
        self.labels = None  # Optional: get info about nodes matching labels
        self.nodes = None  # Optional: get info about certain named nodes only


class InventoryDevice(object):
    """
    When fetching inventory, block devices are reported in this format.

    Note on device identifiers: the format of this is up to the orchestrator,
    but the same identifier must also work when passed into StatefulServiceSpec.
    The identifier should be something meaningful like a device WWID or
    stable device node path -- not something made up by the orchestrator.

    "Extended" is for reporting any special configuration that may have
    already been done out of band on the block device.  For example, if
    the device has already been configured for encryption, report that
    here so that it can be indicated to the user.  The set of
    extended properties may differ between orchestrators.  An orchestrator
    is permitted to support no extended properties (only normal block
    devices)
    """
    def __init__(self):
        self.blank = False
        self.type = None  # 'ssd', 'hdd', 'nvme'
        self.id = None  # unique within a node (or globally if you like).
        self.size = None  # byte integer.
        self.extended = None  # arbitrary JSON-serializable object

        # If this drive is not empty, but is suitable for appending
        # additional journals, wals, or bluestore dbs, then report
        # how much space is available.
        self.metadata_space_free = None


class InventoryNode(object):
    def __init__(self, name, devices):
        assert isinstance(devices, list)
        self.name = name  # unique within cluster.  For example a hostname.
        self.devices = devices  # list of InventoryDevice
