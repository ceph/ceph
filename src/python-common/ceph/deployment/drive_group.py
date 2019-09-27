import fnmatch
try:
    from typing import Optional, List, Dict
except ImportError:
    pass

import six


class DeviceSelection(object):
    """
    Used within :class:`ceph.deployment.drive_group.DriveGroupSpec` to specify the devices
    used by the Drive Group.

    Any attributes (even none) can be included in the device
    specification structure.
    """

    def __init__(self, paths=None, id_model=None, size=None, rotates=None, count=None):
        # type: (List[str], str, str, bool, int) -> None
        """
        ephemeral drive group device specification
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
    def __init__(self, msg):
        super(DriveGroupValidationError, self).__init__('Failed to validate Drive Group: ' + msg)

class DriveGroupSpec(object):
    """
    Describe a drive group in the same form that ceph-volume
    understands.
    """

    def __init__(self,
                 host_pattern,  # type: str
                 data_devices=None,  # type: Optional[DeviceSelection]
                 db_devices=None,  # type: Optional[DeviceSelection]
                 wal_devices=None,  # type: Optional[DeviceSelection]
                 journal_devices=None,  # type: Optional[DeviceSelection]
                 data_directories=None,  # type: Optional[List[str]]
                 osds_per_device=None,  # type: Optional[int]
                 objectstore='bluestore',  # type: str
                 encrypted=False,  # type: bool
                 db_slots=None,  # type: Optional[int]
                 wal_slots=None,  # type: Optional[int]
                 osd_id_claims=None,  # type: Optional[Dict[str, DeviceSelection]]
                 ):

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

        #: Optional: mapping of OSD id to DeviceSelection, used when the
        #: created OSDs are meant to replace previous OSDs on
        #: the same node. See :ref:`orchestrator-osd-replace`
        self.osd_id_claims = osd_id_claims

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