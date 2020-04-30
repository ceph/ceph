from ceph.deployment.inventory import Device
from ceph.deployment.service_spec import ServiceSpecValidationError, ServiceSpec, PlacementSpec

try:
    from typing import Optional, List, Dict, Any
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

    _supported_filters = [
            "paths", "size", "vendor", "model", "rotational", "limit", "all"
    ]

    def __init__(self,
                 paths=None,  # type: Optional[List[str]]
                 model=None,  # type: Optional[str]
                 size=None,  # type: Optional[str]
                 rotational=None,  # type: Optional[bool]
                 limit=None,  # type: Optional[int]
                 vendor=None,  # type: Optional[str]
                 all=False,  # type: bool
                 ):
        """
        ephemeral drive group device specification
        """
        #: List of Device objects for devices paths.
        self.paths = [] if paths is None else [Device(path) for path in paths]  # type: List[Device]

        #: A wildcard string. e.g: "SDD*" or "SanDisk SD8SN8U5"
        self.model = model

        #: Match on the VENDOR property of the drive
        self.vendor = vendor

        #: Size specification of format LOW:HIGH.
        #: Can also take the the form :HIGH, LOW:
        #: or an exact value (as ceph-volume inventory reports)
        self.size = size

        #: is the drive rotating or not
        self.rotational = rotational

        #: Limit the number of devices added to this Drive Group. Devices
        #: are used from top to bottom in the output of ``ceph-volume inventory``
        self.limit = limit

        #: Matches all devices. Can only be used for data devices
        self.all = all

        self.validate()

    def validate(self):
        # type: () -> None
        props = [self.model, self.vendor, self.size, self.rotational]  # type: List[Any]
        if self.paths and any(p is not None for p in props):
            raise DriveGroupValidationError(
                'DeviceSelection: `paths` and other parameters are mutually exclusive')
        is_empty = not any(p is not None and p != [] for p in [self.paths] + props)
        if not self.all and is_empty:
            raise DriveGroupValidationError('DeviceSelection cannot be empty')

        if self.all and not is_empty:
            raise DriveGroupValidationError(
                'DeviceSelection `all` and other parameters are mutually exclusive. {}'.format(
                    repr(self)))

    @classmethod
    def from_json(cls, device_spec):
        # type: (dict) -> DeviceSelection
        if not device_spec:
            return  # type: ignore
        for applied_filter in list(device_spec.keys()):
            if applied_filter not in cls._supported_filters:
                raise DriveGroupValidationError(
                    "Filtering for <{}> is not supported".format(applied_filter))

        return cls(**device_spec)

    def to_json(self):
        # type: () -> Dict[str, Any]
        c = self.__dict__.copy()
        if self.paths:
            c['paths'] = [p.path for p in self.paths]
        return c

    def __repr__(self):
        keys = [
            key for key in self._supported_filters + ['limit'] if getattr(self, key) is not None
        ]
        if 'paths' in keys and self.paths == []:
            keys.remove('paths')
        return "DeviceSelection({})".format(
            ', '.join('{}={}'.format(key, repr(getattr(self, key))) for key in keys)
        )

    def __eq__(self, other):
        return repr(self) == repr(other)


class DriveGroupValidationError(ServiceSpecValidationError):
    """
    Defining an exception here is a bit problematic, cause you cannot properly catch it,
    if it was raised in a different mgr module.
    """

    def __init__(self, msg):
        super(DriveGroupValidationError, self).__init__('Failed to validate Drive Group: ' + msg)


class DriveGroupSpec(ServiceSpec):
    """
    Describe a drive group in the same form that ceph-volume
    understands.
    """

    _supported_features = [
        "encrypted", "block_wal_size", "osds_per_device",
        "db_slots", "wal_slots", "block_db_size", "placement", "service_id", "service_type",
        "data_devices", "db_devices", "wal_devices", "journal_devices",
        "data_directories", "osds_per_device", "objectstore", "osd_id_claims",
        "journal_size", "unmanaged"
    ]

    def __init__(self,
                 placement=None,  # type: Optional[PlacementSpec]
                 service_id=None,  # type: str
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
                 osd_id_claims=None,  # type: Optional[Dict[str, List[str]]]
                 block_db_size=None,  # type: Optional[int]
                 block_wal_size=None,  # type: Optional[int]
                 journal_size=None,  # type: Optional[int]
                 service_type=None,  # type: Optional[str]
                 unmanaged=False,  # type: bool
                 ):
        assert service_type is None or service_type == 'osd'
        super(DriveGroupSpec, self).__init__('osd', service_id=service_id,
                                             placement=placement,
                                             unmanaged=unmanaged)

        #: A :class:`ceph.deployment.drive_group.DeviceSelection`
        self.data_devices = data_devices

        #: A :class:`ceph.deployment.drive_group.DeviceSelection`
        self.db_devices = db_devices

        #: A :class:`ceph.deployment.drive_group.DeviceSelection`
        self.wal_devices = wal_devices

        #: A :class:`ceph.deployment.drive_group.DeviceSelection`
        self.journal_devices = journal_devices

        #: Set (or override) the "bluestore_block_wal_size" value, in bytes
        self.block_wal_size = block_wal_size

        #: Set (or override) the "bluestore_block_db_size" value, in bytes
        self.block_db_size = block_db_size

        #: set journal_size is bytes
        self.journal_size = journal_size

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

        #: Optional: mapping of host -> List of osd_ids that should be replaced
        #: See :ref:`orchestrator-osd-replace`
        self.osd_id_claims = osd_id_claims or dict()

    @classmethod
    def _from_json_impl(cls, json_drive_group):
        # type: (dict) -> DriveGroupSpec
        """
        Initialize 'Drive group' structure

        :param json_drive_group: A valid json string with a Drive Group
               specification
        """
        # legacy json (pre Octopus)
        if 'host_pattern' in json_drive_group and 'placement' not in json_drive_group:
            json_drive_group['placement'] = {'host_pattern': json_drive_group['host_pattern']}
            del json_drive_group['host_pattern']

        for applied_filter in list(json_drive_group.keys()):
            if applied_filter not in cls._supported_features:
                raise DriveGroupValidationError(
                    "Feature <{}> is not supported".format(applied_filter))

        for key in ('block_wal_size', 'block_db_size', 'journal_size'):
            if key in json_drive_group:
                if isinstance(json_drive_group[key], six.string_types):
                    from ceph.deployment.drive_selection import SizeMatcher
                    json_drive_group[key] = SizeMatcher.str_to_byte(json_drive_group[key])

        if 'placement' in json_drive_group:
            json_drive_group['placement'] = PlacementSpec.from_json(json_drive_group['placement'])

        try:
            args = {k: (DeviceSelection.from_json(v) if k.endswith('_devices') else v) for k, v in
                    json_drive_group.items()}
            if not args:
                raise DriveGroupValidationError("Didn't find Drivegroup specs")
            return DriveGroupSpec(**args)
        except (KeyError, TypeError) as e:
            raise DriveGroupValidationError(str(e))

    def validate(self):
        # type: () -> None
        super(DriveGroupSpec, self).validate()

        if not isinstance(self.placement.host_pattern, six.string_types) and \
                self.placement.host_pattern is not None:
            raise DriveGroupValidationError('host_pattern must be of type string')

        specs = [self.data_devices, self.db_devices, self.wal_devices, self.journal_devices]
        for s in filter(None, specs):
            s.validate()
        for s in filter(None, [self.db_devices, self.wal_devices, self.journal_devices]):
            if s.all:
                raise DriveGroupValidationError("`all` is only allowed for data_devices")

        if self.objectstore not in ('filestore', 'bluestore'):
            raise DriveGroupValidationError("objectstore not in ('filestore', 'bluestore')")

        if self.block_wal_size is not None and type(self.block_wal_size) != int:
            raise DriveGroupValidationError('block_wal_size must be of type int')
        if self.block_db_size is not None and type(self.block_db_size) != int:
            raise DriveGroupValidationError('block_db_size must be of type int')

    def __repr__(self):
        keys = [
            key for key in self._supported_features if getattr(self, key) is not None
        ]
        if 'encrypted' in keys and not self.encrypted:
            keys.remove('encrypted')
        if 'objectstore' in keys and self.objectstore == 'bluestore':
            keys.remove('objectstore')
        return "DriveGroupSpec(name={}->{})".format(
            self.service_id,
            ', '.join('{}={}'.format(key, repr(getattr(self, key))) for key in keys)
        )

    def to_json(self):
        # type: () -> Dict[str, Any]
        c = self.__dict__.copy()
        if self.placement:
            c['placement'] = self.placement.to_json()
        if self.data_devices:
            c['data_devices'] = self.data_devices.to_json()
        if self.db_devices:
            c['db_devices'] = self.db_devices.to_json()
        if self.wal_devices:
            c['wal_devices'] = self.wal_devices.to_json()
        c['service_name'] = self.service_name()
        return c

    def __eq__(self, other):
        return repr(self) == repr(other)
