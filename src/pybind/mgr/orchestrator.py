
"""
ceph-mgr orchestrator interface

This is a DRAFT for discussion.

Goal: enable UI workflows for cluster service management
      (such as creating OSDs, in addition to stateless services)
      using common concepts that are implemented by
      diverse backends such as Rook, DeepSea, ceph-ansible

Concepts:
    "Stateful service": a daemon that uses local storage, such as OSD or mon.
    "Stateless service": a daemon that doesn't use any local storage, such
                         as an MDS, RGW, nfs-ganesha, iSCSI gateway.
    "Label": arbitrary string tags that may be applied by administrators
             to nodes.  Typically administrators use labels to indicate
             which nodes

Constraints:
    1. The orchestrator is to be the source of truth for
       all the physical information, and will be queried directly
       as needed (i.e. no in-Ceph database of hardware etc).
    2. The orchestrator handles placement of collections of stateless
       services.
    3. The orchestrator accepts explicit placement of individual stateful
       services, and optionally also accepts label-based automatic placement.
       (i.e. it *must* support "create OSD at host1:/dev/sdb", and it *may*
        support "create OSDs on hosts with label=ceph-osd")
    4. Ceph is not responsible for installing anything on nodes, or hooking
       up hosts to the orchestrator: nodes and drives only exist to Ceph
       once they're exposed by the orchestrator.

Flexible features:
    1. Orchestrator may be aware of one or more "special features" for
       block devices, such as block-level encryption.  These special
       features may be reported during discovery or enabled during
       OSD creation.
    2. Orchestrator may not have a concept of labels: if it doesn't,
       any label-filtered calls will match everything.

Excluded functionality:
    1. No support for multipathed drives: all block devices are to be
    reported from one host only.
    2. No networking inventory or configuration.

This is a DRAFT for discussion.
"""


class Orchestrator(object):
    """
    All calls in this class are blocking on network IO and may
    take noticeable length of time to run.  Wrap calls appropriately
    in long-running task infrastructure when calling from interactive
    code.
    """

    # >>> From 21 March meeting
    def get_inventory(self, node_filter=None):
        # Return list of InventoryHost
        raise NotImplementedError()

    def add_stateful_service(self, service_type, spec):
        """
        service_type: one of mon, osd
        """
        assert isinstance(spec, StatefulServiceSpec)
        raise NotImplementedError()

    def remove_stateful_service(self, service_type, service_id):
        raise NotImplementedError()

    def add_stateless_service(self, service_type, spec):
        assert isinstance(spec, StatelessServiceSpec)
        raise NotImplementedError()

    def update_stateless_service(self, service_type, id_, spec):
        assert isinstance(spec, StatelessServiceSpec)
        raise NotImplementedError()

    def remove_stateless_service(self, service_type, id_):
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
    # <<< from 21 March meeting

    # >>> additional to 21mar: auto-placement of stateful services
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
    # <<<


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


class StatefulServiceSpec(object):
    """
    Details of stateful service creation.

    For OSDs this should map to what ceph-volume can do.
    """

    def __init__(self):
        self.service_type = None  # osd, mon

        self.format = None  # filestore, bluestore (meaningful for OSD only)

        self.hostname = None

        # Map role to device ID:
        #   Valid keys: data (mon)
        #   Valid keys: data, journal (filestore)
        #   Valid keys: data, wal, db (bluestore)
        self.block_devices = {}

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
        # be the name of a CephFS pool, RGW zone, etc.  Must be unique
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

    Typical use: filter by host when presenting UI workflow for configuring
                 a particular server.
                 filter by label when not all of estate is Ceph servers,
                 and we want to only learn about the Ceph servers.
                 filter by label when we are interested particularly
                 in e.g. OSD servers.

    """
    def __init__(self):
        self.labels = None  # Optional: get info about nodes matching labels
        self.hostnames = None  # Optional: get info about certain hosts only


class InventoryBlockDevice(object):
    """
    When fetching inventory, block devices are reported in this format.

    Note on device identifiers: the format of this is up to the orchestrator,
    but the same identifier must also work when passed into StatefulServiceSpec

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

        # TODO: should we report a "size_avail" to indicate a partially
        # used device (has some journal partitions but also room for more?)


class InventoryNode(object):
    def __init__(self):
        self.hostname = None  # unique within cluster
        self.devices = []  # list of InventoryBlockDevice
