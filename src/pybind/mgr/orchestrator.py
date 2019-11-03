
"""
ceph-mgr orchestrator interface

Please see the ceph-mgr module developer's guide for more information.
"""
import copy
import sys
import time
import fnmatch
from functools import wraps
import uuid
import string
import random
import datetime

import six

from mgr_module import MgrModule, PersistentStoreDict
from mgr_util import format_bytes

try:
    from ceph.deployment.drive_group import DriveGroupSpec
    from typing import TypeVar, Generic, List, Optional, Union, Tuple, Iterator

    T = TypeVar('T')
    G = Generic[T]
except ImportError:
    T, G = object, object


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

    def result_str(self):
        if self.result is None:
            return ''
        return str(self.result)

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
        if r_cls.__module__ in ('__builtin__', 'builtins'):
            return r_obj
        my_cls = getattr(sys.modules[r_cls.__module__], r_cls.__name__)
        if id(my_cls) == id(r_cls):
            return r_obj
        if hasattr(r_obj, '__reduce__'):
            reduce_tuple = r_obj.__reduce__()
            if len(reduce_tuple) >= 2:
                return my_cls(*[copy_to_this_subinterpreter(a) for a in reduce_tuple[1]])
        my_obj = my_cls.__new__(my_cls)
        for k,v in r_obj.__dict__.items():
            setattr(my_obj, k, copy_to_this_subinterpreter(v))
        return my_obj

    if c.exception is not None:
        try:
            e = copy_to_this_subinterpreter(c.exception)
        except (KeyError, AttributeError):
            raise Exception(str(c.exception))
        raise e


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


class TrivialReadCompletion(ReadCompletion):
    """
    This is the trivial completion simply wrapping a result.
    """
    def __init__(self, result):
        super(TrivialReadCompletion, self).__init__()
        self._result = result

    @property
    def result(self):
        return self._result

    @property
    def is_complete(self):
        return True


class WriteCompletion(_Completion):
    """
    ``Orchestrator`` implementations should inherit from this
    class to implement their own handles to operations in progress, and
    return an instance of their subclass from calls into methods.
    """

    def __init__(self):
        self.progress_id = str(uuid.uuid4())

        #: if a orchestrator module can provide a more detailed
        #: progress information, it needs to also call ``progress.update()``.
        self.progress = 0.5

    def __str__(self):
        """
        ``__str__()`` is used for determining the message for progress events.
        """
        return super(WriteCompletion, self).__str__()

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

def _hide_in_features(f):
    f._hide_in_features = True
    return f

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

    @_hide_in_features
    def is_orchestrator_module(self):
        """
        Enable other modules to interrogate this module to discover
        whether it's usable as an orchestrator module.

        Subclasses do not need to override this.
        """
        return True

    @_hide_in_features
    def available(self):
        # type: () -> Tuple[bool, str]
        """
        Report whether we can talk to the orchestrator.  This is the
        place to give the user a meaningful message if the orchestrator
        isn't running or can't be contacted.

        This method may be called frequently (e.g. every page load
        to conditionally display a warning banner), so make sure it's
        not too expensive.  It's okay to give a slightly stale status
        (e.g. based on a periodic background ping of the orchestrator)
        if that's necessary to make this method fast.

        .. note::
            `True` doesn't mean that the desired functionality
            is actually available in the orchestrator. I.e. this
            won't work as expected::

                >>> if OrchestratorClientMixin().available()[0]:  # wrong.
                ...     OrchestratorClientMixin().get_hosts()

        :return: two-tuple of boolean, string
        """
        raise NotImplementedError()

    @_hide_in_features
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

    @_hide_in_features
    def get_feature_set(self):
        """Describes which methods this orchestrator implements

        .. note::
            `True` doesn't mean that the desired functionality
            is actually possible in the orchestrator. I.e. this
            won't work as expected::

                >>> api = OrchestratorClientMixin()
                ... if api.get_feature_set()['get_hosts']['available']:  # wrong.
                ...     api.get_hosts()

            It's better to ask for forgiveness instead::

                >>> try:
                ...     OrchestratorClientMixin().get_hosts()
                ... except (OrchestratorError, NotImplementedError):
                ...     ...


        :returns: Dict of API method names to ``{'available': True or False}``
        """
        module = self.__class__
        features = {a: {'available': getattr(Orchestrator, a, None) != getattr(module, a)}
                    for a in Orchestrator.__dict__
                    if not a.startswith('_') and not getattr(getattr(Orchestrator, a), '_hide_in_features', False)
                    }
        return features

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

    def describe_service(self, service_type=None, service_id=None, node_name=None, refresh=False):
        # type: (Optional[str], Optional[str], Optional[str], bool) -> ReadCompletion[List[ServiceDescription]]
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

    def remove_osds(self, osd_ids, destroy=False):
        # type: (List[str], bool) -> WriteCompletion
        """
        :param osd_ids: list of OSD IDs
        :param destroy: marks the OSD as being destroyed. See :ref:`orchestrator-osd-replace`

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

    def add_mds(self, spec):
        # type: (StatelessServiceSpec) -> WriteCompletion
        """Create a new MDS cluster"""
        raise NotImplementedError()

    def remove_mds(self, name):
        # type: (str) -> WriteCompletion
        """Remove an MDS cluster"""
        raise NotImplementedError()

    def update_mds(self, spec):
        # type: (StatelessServiceSpec) -> WriteCompletion
        """
        Update / redeploy existing MDS cluster
        Like for example changing the number of service instances.
        """
        raise NotImplementedError()

    def add_nfs(self, spec):
        # type: (NFSServiceSpec) -> WriteCompletion
        """Create a new MDS cluster"""
        raise NotImplementedError()

    def remove_nfs(self, name):
        # type: (str) -> WriteCompletion
        """Remove a NFS cluster"""
        raise NotImplementedError()

    def update_nfs(self, spec):
        # type: (NFSServiceSpec) -> WriteCompletion
        """
        Update / redeploy existing NFS cluster
        Like for example changing the number of service instances.
        """
        raise NotImplementedError()

    def add_rgw(self, spec):
        # type: (RGWSpec) -> WriteCompletion
        """Create a new MDS zone"""
        raise NotImplementedError()

    def remove_rgw(self, zone):
        # type: (str) -> WriteCompletion
        """Remove a RGW zone"""
        raise NotImplementedError()

    def update_rgw(self, spec):
        # type: (StatelessServiceSpec) -> WriteCompletion
        """
        Update / redeploy existing RGW zone
        Like for example changing the number of service instances.
        """
        raise NotImplementedError()

    @_hide_in_features
    def upgrade_start(self, upgrade_spec):
        # type: (UpgradeSpec) -> WriteCompletion
        raise NotImplementedError()

    @_hide_in_features
    def upgrade_status(self):
        # type: () -> ReadCompletion[UpgradeStatusSpec]
        """
        If an upgrade is currently underway, report on where
        we are in the process, or if some error has occurred.

        :return: UpgradeStatusSpec instance
        """
        raise NotImplementedError()

    @_hide_in_features
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
    def __init__(self, label=None, nodes=[]):
        self.label = label
        self.nodes = nodes


def handle_type_error(method):
    @wraps(method)
    def inner(cls, *args, **kwargs):
        try:
            return method(cls, *args, **kwargs)
        except TypeError as e:
            error_msg = '{}: {}'.format(cls.__name__, e)
        raise OrchestratorValidationError(error_msg)
    return inner


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

    def __init__(self, nodename=None, container_id=None, service=None, service_instance=None,
                 service_type=None, version=None, rados_config_location=None,
                 service_url=None, status=None, status_desc=None):
        # Node is at the same granularity as InventoryNode
        self.nodename = nodename

        # Not everyone runs in containers, but enough people do to
        # justify having this field here.
        self.container_id = container_id

        # Some services can be deployed in groups. For example, mds's can
        # have an active and standby daemons, and nfs-ganesha can run daemons
        # in parallel. This tag refers to a group of daemons as a whole.
        #
        # For instance, a cluster of mds' all service the same fs, and they
        # will all have the same service value (which may be the
        # Filesystem name in the FSMap).
        #
        # Single-instance services should leave this set to None
        self.service = service

        # The orchestrator will have picked some names for daemons,
        # typically either based on hostnames or on pod names.
        # This is the <foo> in mds.<foo>, the ID that will appear
        # in the FSMap/ServiceMap.
        self.service_instance = service_instance

        # The type of service (osd, mon, mgr, etc.)
        self.service_type = service_type

        # Service version that was deployed
        self.version = version

        # Location of the service configuration when stored in rados
        # object. Format: "rados://<pool>/[<namespace/>]<object>"
        self.rados_config_location = rados_config_location

        # If the service exposes REST-like API, this attribute should hold
        # the URL.
        self.service_url = service_url

        # Service status: -1 error, 0 stopped, 1 running
        self.status = status

        # Service status description when status == -1.
        self.status_desc = status_desc

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

    @classmethod
    @handle_type_error
    def from_json(cls, data):
        return cls(**data)


class StatelessServiceSpec(object):
    # Request to orchestrator for a group of stateless services
    # such as MDS, RGW, nfs gateway, iscsi gateway
    """
    Details of stateless service creation.

    Request to orchestrator for a group of stateless services
    such as MDS, RGW or iscsi gateway
    """
    # This structure is supposed to be enough information to
    # start the services.

    def __init__(self, name, placement=None, count=None):
        self.placement = PlacementSpec() if placement is None else placement

        #: Give this set of statelss services a name: typically it would
        #: be the name of a CephFS filesystem, RGW zone, etc.  Must be unique
        #: within one ceph cluster.
        self.name = name

        #: Count of service instances
        self.count = 1 if count is None else count

    def validate_add(self):
        if not self.name:
            raise OrchestratorValidationError('Cannot add Service: Name required')


class NFSServiceSpec(StatelessServiceSpec):
    def __init__(self, name, pool=None, namespace=None, count=1, placement=None):
        super(NFSServiceSpec, self).__init__(name, placement, count)

        #: RADOS pool where NFS client recovery data is stored.
        self.pool = pool

        #: RADOS namespace where NFS client recovery data is stored in the pool.
        self.namespace = namespace

    def validate_add(self):
        super(NFSServiceSpec, self).validate_add()

        if not self.pool:
            raise OrchestratorValidationError('Cannot add NFS: No Pool specified')


class RGWSpec(StatelessServiceSpec):
    """
    Settings to configure a (multisite) Ceph RGW

    """
    def __init__(self,
                 rgw_zone,  # type: str
                 hosts=None,  # type: Optional[List[str]]
                 rgw_multisite=None,  # type: Optional[bool]
                 rgw_zonemaster=None,  # type: Optional[bool]
                 rgw_zonesecondary=None,  # type: Optional[bool]
                 rgw_multisite_proto=None,  # type: Optional[str]
                 rgw_frontend_port=None,  # type: Optional[int]
                 rgw_zonegroup=None,  # type: Optional[str]
                 rgw_zone_user=None,  # type: Optional[str]
                 rgw_realm=None,  # type: Optional[str]
                 system_access_key=None,  # type: Optional[str]
                 system_secret_key=None,  # type: Optional[str]
                 count=None  # type: Optional[int]
                 ):
        # Regarding default values. Ansible has a `set_rgwspec_defaults` that sets
        # default values that makes sense for Ansible. Rook has default values implemented
        # in Rook itself. Thus we don't set any defaults here in this class.

        super(RGWSpec, self).__init__(name=rgw_zone, count=count)

        #: List of hosts where RGWs should run. Not for Rook.
        self.hosts = hosts

        #: is multisite
        self.rgw_multisite = rgw_multisite
        self.rgw_zonemaster = rgw_zonemaster
        self.rgw_zonesecondary = rgw_zonesecondary
        self.rgw_multisite_proto = rgw_multisite_proto
        self.rgw_frontend_port = rgw_frontend_port

        self.rgw_zonegroup = rgw_zonegroup
        self.rgw_zone_user = rgw_zone_user
        self.rgw_realm = rgw_realm

        self.system_access_key = system_access_key
        self.system_secret_key = system_secret_key

    @property
    def rgw_multisite_endpoint_addr(self):
        """Returns the first host. Not supported for Rook."""
        return self.hosts[0]

    @property
    def rgw_multisite_endpoints_list(self):
        return ",".join(["{}://{}:{}".format(self.rgw_multisite_proto,
                             host,
                             self.rgw_frontend_port) for host in self.hosts])

    def genkey(self, nchars):
        """ Returns a random string of nchars

        :nchars : Length of the returned string
        """
        # TODO Python 3: use Secrets module instead.

        return ''.join(random.choice(string.ascii_uppercase +
                                     string.ascii_lowercase +
                                     string.digits) for _ in range(nchars))

    @classmethod
    def from_json(cls, json_rgw_spec):
        # type: (dict) -> RGWSpec
        """
        Initialize 'RGWSpec' object data from a json structure
        :param json_rgw_spec: A valid dict with a the RGW settings
        """
        # TODO: also add PlacementSpec(**json_rgw_spec['placement'])
        args = {k:v for k, v in json_rgw_spec.items()}
        return RGWSpec(**args)


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
    @handle_type_error
    def from_json(cls, data):
        return cls(**data)

    @classmethod
    def from_ceph_volume_inventory(cls, data):
        # TODO: change InventoryDevice itself to mirror c-v inventory closely!

        dev = InventoryDevice()
        dev.id = data["path"]
        dev.type = 'hdd' if data["sys_api"]["rotational"] == "1" else 'ssd/nvme'
        dev.size = data["sys_api"]["size"]
        dev.rotates = data["sys_api"]["rotational"] == "1"
        dev.available = data["available"]
        dev.dev_id = "%s/%s" % (data["sys_api"]["vendor"],
                                data["sys_api"]["model"])
        dev.extended = data
        return dev

    @classmethod
    def from_ceph_volume_inventory_list(cls, datas):
        return [cls.from_ceph_volume_inventory(d) for d in datas]

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

    @classmethod
    def from_json(cls, data):
        try:
            _data = copy.deepcopy(data)
            name = _data.pop('name')
            devices = [InventoryDevice.from_json(device)
                       for device in _data.pop('devices')]
            if _data:
                error_msg = 'Unknown key(s) in Inventory: {}'.format(','.join(_data.keys()))
                raise OrchestratorValidationError(error_msg)
            return cls(name, devices)
        except KeyError as e:
            error_msg = '{} is required for {}'.format(e, cls.__name__)
            raise OrchestratorValidationError(error_msg)

    @classmethod
    def from_nested_items(cls, hosts):
        devs = InventoryDevice.from_ceph_volume_inventory_list
        return [cls(item[0], devs(item[1].data)) for item in hosts]


def _mk_orch_methods(cls):
    # Needs to be defined outside of for.
    # Otherwise meth is always bound to last key
    def shim(method_name):
        def inner(self, *args, **kwargs):
            completion = self._oremote(method_name, args, kwargs)
            self._update_completion_progress(completion, 0)
            return completion
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

    def set_mgr(self, mgr):
        # type: (MgrModule) -> None
        """
        Useable in the Dashbord that uses a global ``mgr``
        """

        self.__mgr = mgr  # Make sure we're not overwriting any other `mgr` properties

    def _oremote(self, meth, args, kwargs):
        """
        Helper for invoking `remote` on whichever orchestrator is enabled

        :raises RuntimeError: If the remote method failed.
        :raises OrchestratorError: orchestrator failed to perform
        :raises ImportError: no `orchestrator_cli` module or backend not found.
        """
        try:
            mgr = self.__mgr
        except AttributeError:
            mgr = self
        try:
            o = mgr._select_orchestrator()
        except AttributeError:
            o = mgr.remote('orchestrator_cli', '_select_orchestrator')

        if o is None:
            raise NoOrchestrator()

        mgr.log.debug("_oremote {} -> {}.{}(*{}, **{})".format(mgr.module_name, o, meth, args, kwargs))
        return mgr.remote(o, meth, *args, **kwargs)

    def _update_completion_progress(self, completion, force_progress=None):
        # type: (WriteCompletion, Optional[float]) -> None
        try:
            progress = force_progress if force_progress is not None else completion.progress
            if completion.is_complete:
                self.remote("progress", "complete", completion.progress_id)
            else:
                self.remote("progress", "update", completion.progress_id, str(completion), progress,
                            [("origin", "orchestrator")])
        except AttributeError:
            # No WriteCompletion. Ignore.
            pass
        except ImportError:
            # If the progress module is disabled that's fine,
            # they just won't see the output.
            pass

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
        for c in completions:
            self._update_completion_progress(c)
        while not self.wait(completions):
            if any(c.should_wait for c in completions):
                time.sleep(5)
            else:
                break
        for c in completions:
            self._update_completion_progress(c)


class OutdatableData(object):
    DATEFMT = '%Y-%m-%d %H:%M:%S.%f'

    def __init__(self, data=None, last_refresh=None):
        # type: (Optional[dict], Optional[datetime.datetime]) -> None
        self._data = data
        if data is not None and last_refresh is None:
            self.last_refresh = datetime.datetime.utcnow()
        else:
            self.last_refresh = last_refresh

    def json(self):
        if self.last_refresh is not None:
            timestr = self.last_refresh.strftime(self.DATEFMT)
        else:
            timestr = None

        return {
            "data": self._data,
            "last_refresh": timestr,
        }

    @property
    def data(self):
        return self._data

    # @data.setter
    # No setter, as it doesn't work as expected: It's not saved in store automatically

    @classmethod
    def time_from_string(cls, timestr):
        if timestr is None:
            return None
        # drop the 'Z' timezone indication, it's always UTC
        timestr = timestr.rstrip('Z')
        return datetime.datetime.strptime(timestr, cls.DATEFMT)


    @classmethod
    def from_json(cls, data):
        return cls(data['data'], cls.time_from_string(data['last_refresh']))

    def outdated(self, timeout_min=None):
        if timeout_min is None:
            timeout_min = 10
        if self.last_refresh is None:
            return True
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(
            minutes=timeout_min)
        return self.last_refresh < cutoff

    def __repr__(self):
        return 'OutdatableData(data={}, last_refresh={})'.format(self._data, self.last_refresh)


class OutdatableDictMixin(object):
    """
    Toolbox for implementing a cache. As every orchestrator has
    different needs, we cannot implement any logic here.
    """

    def __getitem__(self, item):
        # type: (str) -> OutdatableData
        return OutdatableData.from_json(super(OutdatableDictMixin, self).__getitem__(item))

    def __setitem__(self, key, value):
        # type: (str, OutdatableData) -> None
        val = None if value is None else value.json()
        super(OutdatableDictMixin, self).__setitem__(key, val)

    def items(self):
        # type: () -> Iterator[Tuple[str, OutdatableData]]
        for item in super(OutdatableDictMixin, self).items():
            k, v = item
            yield k, OutdatableData.from_json(v)

    def items_filtered(self, keys=None):
        if keys:
            return [(host, self[host]) for host in keys]
        else:
            return list(self.items())

    def any_outdated(self, timeout=None):
        items = self.items()
        if not list(items):
            return True
        return any([i[1].outdated(timeout) for i in items])

    def remove_outdated(self):
        outdated = [item[0] for item in self.items() if item[1].outdated()]
        for o in outdated:
            del self[o]

class OutdatablePersistentDict(OutdatableDictMixin, PersistentStoreDict):
    pass

class OutdatableDict(OutdatableDictMixin, dict):
    pass
