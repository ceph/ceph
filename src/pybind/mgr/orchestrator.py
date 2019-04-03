
"""
ceph-mgr orchestrator interface

Please see the ceph-mgr module developer's guide for more information.
"""
import sys
import time
import fnmatch

try:
    from typing import TypeVar, Generic, List, Optional, Union, Tuple
    T = TypeVar('T')
    G = Generic[T]
except ImportError:
    T, G = object, object

import six

from mgr_util import format_bytes


class OrchestratorError(Exception):
    """
    General orchestrator specific error.

    Used for deployment, configuration or user errors.

    It's not intended for programming errors or orchestrator internal errors.
    """


class NoOrchestrator(OrchestratorError):
    """
    No orchestrator in configured.
    """
    def __init__(self, msg="No orchestrator configured (try `ceph orchestrator set backend`)"):
        super(NoOrchestrator, self).__init__(msg)


class OrchestratorValidationError(OrchestratorError):
    """
    Raised when an orchestrator doesn't support a specific feature.
    """


class _Completion(G):
    @property
    def result(self):
        # type: () -> T
        """
        Return the result of the operation that we were waited
        for.  Only valid after calling Orchestrator.wait() on this
        completion.
        """
        raise NotImplementedError()

    @property
    def exception(self):
        # type: () -> Optional[Exception]
        """
        Holds an exception object.
        """
        try:
            return self.__exception
        except AttributeError:
            return None

    @exception.setter
    def exception(self, value):
        self.__exception = value

    @property
    def is_read(self):
        # type: () -> bool
        raise NotImplementedError()

    @property
    def is_complete(self):
        # type: () -> bool
        raise NotImplementedError()

    @property
    def is_errored(self):
        # type: () -> bool
        """
        Has the completion failed. Default implementation looks for
        self.exception. Can be overwritten.
        """
        return self.exception is not None

    @property
    def should_wait(self):
        # type: () -> bool
        raise NotImplementedError()


def raise_if_exception(c):
    # type: (_Completion) -> None
    """
    :raises OrchestratorError: Some user error or a config error.
    :raises Exception: Some internal error
    """
    def copy_to_this_subinterpreter(r_obj):
        # This is something like `return pickle.loads(pickle.dumps(r_obj))`
        # Without importing anything.
        r_cls = r_obj.__class__
        if r_cls.__module__ == '__builtin__':
            return r_obj
        my_cls = getattr(sys.modules[r_cls.__module__], r_cls.__name__)
        if id(my_cls) == id(r_cls):
            return r_obj
        my_obj = my_cls.__new__(my_cls)
        for k,v in r_obj.__dict__.items():
            setattr(my_obj, k, copy_to_this_subinterpreter(v))
        return my_obj

    if c.exception is not None:
        raise copy_to_this_subinterpreter(c.exception)


class ReadCompletion(_Completion):
    """
    ``Orchestrator`` implementations should inherit from this
    class to implement their own handles to operations in progress, and
    return an instance of their subclass from calls into methods.
    """

    def __init__(self):
        pass

    @property
    def is_read(self):
        return True

    @property
    def should_wait(self):
        """Could the external operation be deemed as complete,
        or should we wait?
        We must wait for a read operation only if it is not complete.
        """
        return not self.is_complete


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
        # type: () -> bool
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

    @property
    def should_wait(self):
        """Could the external operation be deemed as complete,
        or should we wait?
        We must wait for a write operation only if we know
        it is not persistent yet.
        """
        return not self.is_persistent


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

        Subclasses do not need to override this.
        """
        return True

    def available(self):
        # type: () -> Tuple[Optional[bool], Optional[str]]
        """
        Report whether we can talk to the orchestrator.  This is the
        place to give the user a meaningful message if the orchestrator
        isn't running or can't be contacted.

        This method may be called frequently (e.g. every page load
        to conditionally display a warning banner), so make sure it's
        not too expensive.  It's okay to give a slightly stale status
        (e.g. based on a periodic background ping of the orchestrator)
        if that's necessary to make this method fast.

        Do not override this method if you don't have a meaningful
        status to return: the default None, None return value is used
        to indicate that a module is unable to indicate its availability.

        :return: two-tuple of boolean, string
        """
        return None, None

    def wait(self, completions):
        """
        Given a list of Completion instances, progress any which are
        incomplete.  Return a true if everything is done.

        Callers should inspect the detail of each completion to identify
        partial completion/progress information, and present that information
        to the user.

        For fast operations (e.g. reading from a database), implementations
        may choose to do blocking IO in this call.

        :rtype: bool
        """
        raise NotImplementedError()

    def add_host(self, host):
        # type: (str) -> WriteCompletion
        """
        Add a host to the orchestrator inventory.

        :param host: hostname
        """
        raise NotImplementedError()

    def remove_host(self, host):
        # type: (str) -> WriteCompletion
        """
        Remove a host from the orchestrator inventory.

        :param host: hostname
        """
        raise NotImplementedError()

    def get_hosts(self):
        # type: () -> ReadCompletion[List[InventoryNode]]
        """
        Report the hosts in the cluster.

        The default implementation is extra slow.

        :return: list of InventoryNodes
        """
        return self.get_inventory()

    def get_inventory(self, node_filter=None, refresh=False):
        # type: (InventoryFilter, bool) -> ReadCompletion[List[InventoryNode]]
        """
        Returns something that was created by `ceph-volume inventory`.

        :return: list of InventoryNode
        """
        raise NotImplementedError()

    def describe_service(self, service_type=None, service_id=None, node_name=None):
        # type: (str, str, str) -> ReadCompletion[List[ServiceDescription]]
        """
        Describe a service (of any kind) that is already configured in
        the orchestrator.  For example, when viewing an OSD in the dashboard
        we might like to also display information about the orchestrator's
        view of the service (like the kubernetes pod ID).

        When viewing a CephFS filesystem in the dashboard, we would use this
        to display the pods being currently run for MDS daemons.

        :return: list of ServiceDescription objects.
        """
        raise NotImplementedError()

    def service_action(self, action, service_type, service_name=None, service_id=None):
        # type: (str, str, str, str) -> WriteCompletion
        """
        Perform an action (start/stop/reload) on a service.

        Either service_name or service_id must be specified:

        * If using service_name, perform the action on that entire logical
          service (i.e. all daemons providing that named service).
        * If using service_id, perform the action on a single specific daemon
          instance.

        :param action: one of "start", "stop", "reload"
        :param service_type: e.g. "mds", "rgw", ...
        :param service_name: name of logical service ("cephfs", "us-east", ...)
        :param service_id: service daemon instance (usually a short hostname)
        :rtype: WriteCompletion
        """
        assert action in ["start", "stop", "reload"]
        assert service_name or service_id
        assert not (service_name and service_id)
        raise NotImplementedError()

    def create_osds(self, drive_group, all_hosts):
        # type: (DriveGroupSpec, List[str]) -> WriteCompletion
        """
        Create one or more OSDs within a single Drive Group.

        The principal argument here is the drive_group member
        of OsdSpec: other fields are advisory/extensible for any
        finer-grained OSD feature enablement (choice of backing store,
        compression/encryption, etc).

        :param drive_group: DriveGroupSpec
        :param all_hosts: TODO, this is required because the orchestrator methods are not composable
                Probably this parameter can be easily removed because each orchestrator can use
                the "get_inventory" method and the "drive_group.host_pattern" attribute
                to obtain the list of hosts where to apply the operation
        """
        raise NotImplementedError()

    def replace_osds(self, drive_group):
        # type: (DriveGroupSpec) -> WriteCompletion
        """
        Like create_osds, but the osd_id_claims must be fully
        populated.
        """
        raise NotImplementedError()

    def remove_osds(self, osd_ids):
        # type: (List[str]) -> WriteCompletion
        """
        :param osd_ids: list of OSD IDs

        Note that this can only remove OSDs that were successfully
        created (i.e. got an OSD ID).
        """
        raise NotImplementedError()

    def update_mgrs(self, num, hosts):
        # type: (int, List[str]) -> WriteCompletion
        """
        Update the number of cluster managers.

        :param num: requested number of managers.
        :param hosts: list of hosts (optional)
        """
        raise NotImplementedError()

    def update_mons(self, num, hosts):
        # type: (int, List[Tuple[str,str]]) -> WriteCompletion
        """
        Update the number of cluster monitors.

        :param num: requested number of monitors.
        :param hosts: list of hosts + network (optional)
        """
        raise NotImplementedError()

    def add_stateless_service(self, service_type, spec):
        # type: (str, StatelessServiceSpec) -> WriteCompletion
        """
        Installing and adding a completely new service to the cluster.

        This is not about starting services.
        """
        raise NotImplementedError()

    def update_stateless_service(self, service_type, spec):
        # type: (str, StatelessServiceSpec) -> WriteCompletion
        """
        This is about changing / redeploying existing services. Like for
        example changing the number of service instances.

        :rtype: WriteCompletion
        """
        raise NotImplementedError()

    def remove_stateless_service(self, service_type, id_):
        # type: (str, str) -> WriteCompletion
        """
        Uninstalls an existing service from the cluster.

        This is not about stopping services.
        """
        raise NotImplementedError()

    def upgrade_start(self, upgrade_spec):
        # type: (UpgradeSpec) -> WriteCompletion
        raise NotImplementedError()

    def upgrade_status(self):
        # type: () -> ReadCompletion[UpgradeStatusSpec]
        """
        If an upgrade is currently underway, report on where
        we are in the process, or if some error has occurred.

        :return: UpgradeStatusSpec instance
        """
        raise NotImplementedError()

    def upgrade_available(self):
        # type: () -> ReadCompletion[List[str]]
        """
        Report on what versions are available to upgrade to

        :return: List of strings
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
        # Node is at the same granularity as InventoryNode
        self.nodename = None

        # Not everyone runs in containers, but enough people do to
        # justify having this field here.
        self.container_id = None

        # Some services can be deployed in groups. For example, mds's can
        # have an active and standby daemons, and nfs-ganesha can run daemons
        # in parallel. This tag refers to a group of daemons as a whole.
        #
        # For instance, a cluster of mds' all service the same fs, and they
        # will all have the same service value (which may be the
        # Filesystem name in the FSMap).
        #
        # Single-instance services should leave this set to None
        self.service = None

        # The orchestrator will have picked some names for daemons,
        # typically either based on hostnames or on pod names.
        # This is the <foo> in mds.<foo>, the ID that will appear
        # in the FSMap/ServiceMap.
        self.service_instance = None

        # The type of service (osd, mon, mgr, etc.)
        self.service_type = None

        # Service version that was deployed
        self.version = None

        # Location of the service configuration when stored in rados
        # object. Format: "rados://<pool>/[<namespace/>]<object>"
        self.rados_config_location = None

        # If the service exposes REST-like API, this attribute should hold
        # the URL.
        self.service_url = None

        # Service status: -1 error, 0 stopped, 1 running
        self.status = None

        # Service status description when status == -1.
        self.status_desc = None

    def to_json(self):
        out = {
            'nodename': self.nodename,
            'container_id': self.container_id,
            'service': self.service,
            'service_instance': self.service_instance,
            'service_type': self.service_type,
            'version': self.version,
            'rados_config_location': self.rados_config_location,
            'service_url': self.service_url,
            'status': self.status,
            'status_desc': self.status_desc,
        }
        return {k: v for (k, v) in out.items() if v is not None}


class DeviceSelection(object):
    """
    Used within :class:`myclass.DriveGroupSpec` to specify the devices
    used by the Drive Group.

    Any attributes (even none) can be included in the device
    specification structure.
    """

    def __init__(self, paths=None, id_model=None, size=None, rotates=None, count=None):
        # type: (List[str], str, str, bool, int) -> None
        """
        ephemeral drive group device specification

        TODO: translate from the user interface (Drive Groups) to an actual list of devices.
        """
        if paths is None:
            paths = []

        #: List of absolute paths to the devices.
        self.paths = paths  # type: List[str]

        #: A wildcard string. e.g: "SDD*"
        self.id_model = id_model

        #: Size specification of format LOW:HIGH.
        #: Can also take the the form :HIGH, LOW:
        #: or an exact value (as ceph-volume inventory reports)
        self.size = size

        #: is the drive rotating or not
        self.rotates = rotates

        #: if this is present limit the number of drives to this number.
        self.count = count
        self.validate()

    def validate(self):
        props = [self.id_model, self.size, self.rotates, self.count]
        if self.paths and any(p is not None for p in props):
            raise DriveGroupValidationError('DeviceSelection: `paths` and other parameters are mutually exclusive')
        if not any(p is not None for p in [self.paths] + props):
            raise DriveGroupValidationError('DeviceSelection cannot be empty')

    @classmethod
    def from_json(cls, device_spec):
        return cls(**device_spec)


class DriveGroupValidationError(Exception):
    """
    Defining an exception here is a bit problematic, cause you cannot properly catch it,
    if it was raised in a different mgr module.
    """

    def __init__(self, msg):
        super(DriveGroupValidationError, self).__init__('Failed to validate Drive Group: ' + msg)

class DriveGroupSpec(object):
    """
    Describe a drive group in the same form that ceph-volume
    understands.
    """
    def __init__(self, host_pattern, data_devices=None, db_devices=None, wal_devices=None, journal_devices=None,
                 data_directories=None, osds_per_device=None, objectstore='bluestore', encrypted=False,
                 db_slots=None, wal_slots=None):
        # type: (str, Optional[DeviceSelection], Optional[DeviceSelection], Optional[DeviceSelection], Optional[DeviceSelection], Optional[List[str]], int, str, bool, int, int) -> ()

        # concept of applying a drive group to a (set) of hosts is tightly
        # linked to the drive group itself
        #
        #: An fnmatch pattern to select hosts. Can also be a single host.
        self.host_pattern = host_pattern

        #: A :class:`orchestrator.DeviceSelection`
        self.data_devices = data_devices

        #: A :class:`orchestrator.DeviceSelection`
        self.db_devices = db_devices

        #: A :class:`orchestrator.DeviceSelection`
        self.wal_devices = wal_devices

        #: A :class:`orchestrator.DeviceSelection`
        self.journal_devices = journal_devices

        #: Number of osd daemons per "DATA" device.
        #: To fully utilize nvme devices multiple osds are required.
        self.osds_per_device = osds_per_device

        #: A list of strings, containing paths which should back OSDs
        self.data_directories = data_directories

        #: ``filestore`` or ``bluestore``
        self.objectstore = objectstore

        #: ``true`` or ``false``
        self.encrypted = encrypted

        #: How many OSDs per DB device
        self.db_slots = db_slots

        #: How many OSDs per WAL device
        self.wal_slots = wal_slots

        # FIXME: needs ceph-volume support
        #: Optional: mapping of drive to OSD ID, used when the
        #: created OSDs are meant to replace previous OSDs on
        #: the same node.
        self.osd_id_claims = {}

    @classmethod
    def from_json(self, json_drive_group):
        """
        Initialize 'Drive group' structure

        :param json_drive_group: A valid json string with a Drive Group
               specification
        """
        args = {k: (DeviceSelection.from_json(v) if k.endswith('_devices') else v) for k, v in
                json_drive_group.items()}
        return DriveGroupSpec(**args)

    def hosts(self, all_hosts):
        return fnmatch.filter(all_hosts, self.host_pattern)

    def validate(self, all_hosts):
        if not isinstance(self.host_pattern, six.string_types):
            raise DriveGroupValidationError('host_pattern must be of type string')

        specs = [self.data_devices, self.db_devices, self.wal_devices, self.journal_devices]
        for s in filter(None, specs):
            s.validate()
        if self.objectstore not in ('filestore', 'bluestore'):
            raise DriveGroupValidationError("objectstore not in ('filestore', 'bluestore')")
        if not self.hosts(all_hosts):
            raise DriveGroupValidationError(
                "host_pattern '{}' does not match any hosts".format(self.host_pattern))


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

        # Count of service instances
        self.count = 1

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
    def __init__(self, labels=None, nodes=None):
        # type: (List[str], List[str]) -> None
        self.labels = labels  # Optional: get info about nodes matching labels
        self.nodes = nodes  # Optional: get info about certain named nodes only


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
    def __init__(self, blank=False, type=None, id=None, size=None,
                 rotates=False, available=False, dev_id=None, extended=None,
                 metadata_space_free=None):
        # type: (bool, str, str, int, bool, bool, str, dict, bool) -> None

        self.blank = blank

        #: 'ssd', 'hdd', 'nvme'
        self.type = type

        #: unique within a node (or globally if you like).
        self.id = id

        #: byte integer.
        self.size = size

        #: indicates if it is a spinning disk
        self.rotates = rotates

        #: can be used to create a new OSD?
        self.available = available

        #: vendor/model
        self.dev_id = dev_id

        #: arbitrary JSON-serializable object
        self.extended = extended if extended is not None else extended

        # If this drive is not empty, but is suitable for appending
        # additional journals, wals, or bluestore dbs, then report
        # how much space is available.
        self.metadata_space_free = metadata_space_free

    def to_json(self):
        return dict(type=self.type, blank=self.blank, id=self.id,
                    size=self.size, rotates=self.rotates,
                    available=self.available, dev_id=self.dev_id,
                    extended=self.extended)

    @classmethod
    def from_ceph_volume_inventory(cls, data):
        # TODO: change InventoryDevice itself to mirror c-v inventory closely!

        dev = InventoryDevice()
        dev.id = data["path"]
        dev.type = 'hdd' if data["sys_api"]["rotational"] == "1" else 'sdd/nvme'
        dev.size = data["sys_api"]["size"]
        dev.rotates = data["sys_api"]["rotational"] == "1"
        dev.available = data["available"]
        dev.dev_id = "%s/%s" % (data["sys_api"]["vendor"],
                                data["sys_api"]["model"])
        dev.extended = data
        return dev

    def pretty_print(self, only_header=False):
        """Print a human friendly line with the information of the device

        :param only_header: Print only the name of the device attributes

        Ex::

            Device Path           Type       Size    Rotates  Available Model
            /dev/sdc            hdd   50.00 GB       True       True ATA/QEMU

        """
        row_format = "  {0:<15} {1:>10} {2:>10} {3:>10} {4:>10} {5:<15}\n"
        if only_header:
            return row_format.format("Device Path", "Type", "Size", "Rotates",
                                     "Available", "Model")
        else:
            return row_format.format(str(self.id), self.type if self.type is not None else "",
                                     format_bytes(self.size if self.size is not None else 0, 5,
                                                  colored=False),
                                     str(self.rotates), str(self.available),
                                     self.dev_id if self.dev_id is not None else "")


class InventoryNode(object):
    """
    When fetching inventory, all Devices are groups inside of an
    InventoryNode.
    """
    def __init__(self, name, devices):
        # type: (str, List[InventoryDevice]) -> None
        assert isinstance(devices, list)
        self.name = name  # unique within cluster.  For example a hostname.
        self.devices = devices

    def to_json(self):
        return {'name': self.name, 'devices': [d.to_json() for d in self.devices]}


def _mk_orch_methods(cls):
    # Needs to be defined outside of for.
    # Otherwise meth is always bound to last key
    def shim(method_name):
        def inner(self, *args, **kwargs):
            return self._oremote(method_name, args, kwargs)
        return inner

    for meth in Orchestrator.__dict__:
        if not meth.startswith('_') and meth not in ['is_orchestrator_module']:
            setattr(cls, meth, shim(meth))
    return cls


@_mk_orch_methods
class OrchestratorClientMixin(Orchestrator):
    """
    A module that inherents from `OrchestratorClientMixin` can directly call
    all :class:`Orchestrator` methods without manually calling remote.

    Every interface method from ``Orchestrator`` is converted into a stub method that internally
    calls :func:`OrchestratorClientMixin._oremote`

    >>> class MyModule(OrchestratorClientMixin):
    ...    def func(self):
    ...        completion = self.add_host('somehost')  # calls `_oremote()`
    ...        self._orchestrator_wait([completion])
    ...        self.log.debug(completion.result)

    """
    def _oremote(self, meth, args, kwargs):
        """
        Helper for invoking `remote` on whichever orchestrator is enabled

        :raises RuntimeError: If the remote method failed.
        :raises NoOrchestrator:
        :raises ImportError: no `orchestrator_cli` module or backend not found.
        """
        try:
            o = self._select_orchestrator()
        except AttributeError:
            o = self.remote('orchestrator_cli', '_select_orchestrator')

        if o is None:
            raise NoOrchestrator()

        self.log.debug("_oremote {} -> {}.{}(*{}, **{})".format(self.module_name, o, meth, args, kwargs))
        return self.remote(o, meth, *args, **kwargs)

    def _orchestrator_wait(self, completions):
        # type: (List[_Completion]) -> None
        """
        Wait for completions to complete (reads) or
        become persistent (writes).

        Waits for writes to be *persistent* but not *effective*.

        :param completions: List of Completions
        :raises NoOrchestrator:
        :raises ImportError: no `orchestrator_cli` module or backend not found.
        """
        while not self.wait(completions):
            if any(c.should_wait for c in completions):
                time.sleep(5)
            else:
                break
