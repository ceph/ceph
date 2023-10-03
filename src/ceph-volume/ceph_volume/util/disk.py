import logging
import os
import re
import stat
import time
from ceph_volume import process
from ceph_volume.api import lvm
from ceph_volume.util.system import get_file_contents


logger = logging.getLogger(__name__)


# The blkid CLI tool has some oddities which prevents having one common call
# to extract the information instead of having separate utilities. The `udev`
# type of output is needed in older versions of blkid (v 2.23) that will not
# work correctly with just the ``-p`` flag to bypass the cache for example.
# Xenial doesn't have this problem as it uses a newer blkid version.


def get_partuuid(device):
    """
    If a device is a partition, it will probably have a PARTUUID on it that
    will persist and can be queried against `blkid` later to detect the actual
    device
    """
    out, err, rc = process.call(
        ['blkid', '-c', '/dev/null', '-s', 'PARTUUID', '-o', 'value', device]
    )
    return ' '.join(out).strip()


def _blkid_parser(output):
    """
    Parses the output from a system ``blkid`` call, requires output to be
    produced using the ``-p`` flag which bypasses the cache, mangling the
    names. These names are corrected to what it would look like without the
    ``-p`` flag.

    Normal output::

        /dev/sdb1: UUID="62416664-cbaf-40bd-9689-10bd337379c3" TYPE="xfs" [...]
    """
    # first spaced separated item is garbage, gets tossed:
    output = ' '.join(output.split()[1:])
    # split again, respecting possible whitespace in quoted values
    pairs = output.split('" ')
    raw = {}
    processed = {}
    mapping = {
        'UUID': 'UUID',
        'TYPE': 'TYPE',
        'PART_ENTRY_NAME': 'PARTLABEL',
        'PART_ENTRY_UUID': 'PARTUUID',
        'PART_ENTRY_TYPE': 'PARTTYPE',
        'PTTYPE': 'PTTYPE',
    }

    for pair in pairs:
        try:
            column, value = pair.split('=')
        except ValueError:
            continue
        raw[column] = value.strip().strip().strip('"')

    for key, value in raw.items():
        new_key = mapping.get(key)
        if not new_key:
            continue
        processed[new_key] = value

    return processed


def blkid(device):
    """
    The blkid interface to its CLI, creating an output similar to what is
    expected from ``lsblk``. In most cases, ``lsblk()`` should be the preferred
    method for extracting information about a device. There are some corner
    cases where it might provide information that is otherwise unavailable.

    The system call uses the ``-p`` flag which bypasses the cache, the caveat
    being that the keys produced are named completely different to expected
    names.

    For example, instead of ``PARTLABEL`` it provides a ``PART_ENTRY_NAME``.
    A bit of translation between these known keys is done, which is why
    ``lsblk`` should always be preferred: the output provided here is not as
    rich, given that a translation of keys is required for a uniform interface
    with the ``-p`` flag.

    Label name to expected output chart:

    cache bypass name               expected name

    UUID                            UUID
    TYPE                            TYPE
    PART_ENTRY_NAME                 PARTLABEL
    PART_ENTRY_UUID                 PARTUUID
    """
    out, err, rc = process.call(
        ['blkid', '-c', '/dev/null', '-p', device]
    )
    return _blkid_parser(' '.join(out))


def get_part_entry_type(device):
    """
    Parses the ``ID_PART_ENTRY_TYPE`` from the "low level" (bypasses the cache)
    output that uses the ``udev`` type of output. This output is intended to be
    used for udev rules, but it is useful in this case as it is the only
    consistent way to retrieve the GUID used by ceph-disk to identify devices.
    """
    out, err, rc = process.call(['blkid', '-c', '/dev/null', '-p', '-o', 'udev', device])
    for line in out:
        if 'ID_PART_ENTRY_TYPE=' in line:
            return line.split('=')[-1].strip()
    return ''


def get_device_from_partuuid(partuuid):
    """
    If a device has a partuuid, query blkid so that it can tell us what that
    device is
    """
    out, err, rc = process.call(
        ['blkid', '-c', '/dev/null', '-t', 'PARTUUID="%s"' % partuuid, '-o', 'device']
    )
    return ' '.join(out).strip()


def remove_partition(device):
    """
    Removes a partition using parted

    :param device: A ``Device()`` object
    """
    # Sometimes there's a race condition that makes 'ID_PART_ENTRY_NUMBER' be not present
    # in the output of `udevadm info --query=property`.
    # Probably not ideal and not the best fix but this allows to get around that issue.
    # The idea is to make it retry multiple times before actually failing.
    for i in range(10):
        udev_info = udevadm_property(device.path)
        partition_number = udev_info.get('ID_PART_ENTRY_NUMBER')
        if partition_number:
            break
        time.sleep(0.2)
    if not partition_number:
        raise RuntimeError('Unable to detect the partition number for device: %s' % device.path)

    process.run(
        ['parted', device.parent_device, '--script', '--', 'rm', partition_number]
    )


def _stat_is_device(stat_obj):
    """
    Helper function that will interpret ``os.stat`` output directly, so that other
    functions can call ``os.stat`` once and interpret that result several times
    """
    return stat.S_ISBLK(stat_obj)


def _lsblk_parser(line):
    """
    Parses lines in lsblk output. Requires output to be in pair mode (``-P`` flag). Lines
    need to be whole strings, the line gets split when processed.

    :param line: A string, with the full line from lsblk output
    """
    # parse the COLUMN="value" output to construct the dictionary
    pairs = line.split('" ')
    parsed = {}
    for pair in pairs:
        try:
            column, value = pair.split('=')
        except ValueError:
            continue
        parsed[column] = value.strip().strip().strip('"')
    return parsed


def device_family(device):
    """
    Returns a list of associated devices. It assumes that ``device`` is
    a parent device. It is up to the caller to ensure that the device being
    used is a parent, not a partition.
    """
    labels = ['NAME', 'PARTLABEL', 'TYPE']
    command = ['lsblk', '-P', '-p', '-o', ','.join(labels), device]
    out, err, rc = process.call(command)
    devices = []
    for line in out:
        devices.append(_lsblk_parser(line))

    return devices


def udevadm_property(device, properties=[]):
    """
    Query udevadm for information about device properties.
    Optionally pass a list of properties to return. A requested property might
    not be returned if not present.

    Expected output format::
        # udevadm info --query=property --name=/dev/sda                                  :(
        DEVNAME=/dev/sda
        DEVTYPE=disk
        ID_ATA=1
        ID_BUS=ata
        ID_MODEL=SK_hynix_SC311_SATA_512GB
        ID_PART_TABLE_TYPE=gpt
        ID_PART_TABLE_UUID=c8f91d57-b26c-4de1-8884-0c9541da288c
        ID_PATH=pci-0000:00:17.0-ata-3
        ID_PATH_TAG=pci-0000_00_17_0-ata-3
        ID_REVISION=70000P10
        ID_SERIAL=SK_hynix_SC311_SATA_512GB_MS83N71801150416A
        TAGS=:systemd:
        USEC_INITIALIZED=16117769
        ...
    """
    out = _udevadm_info(device)
    ret = {}
    for line in out:
        p, v = line.split('=', 1)
        if not properties or p in properties:
            ret[p] = v
    return ret


def _udevadm_info(device):
    """
    Call udevadm and return the output
    """
    cmd = ['udevadm', 'info', '--query=property', device]
    out, _err, _rc = process.call(cmd)
    return out


def lsblk(device, columns=None, abspath=False):
    result = []
    if not os.path.isdir(device):
        result = lsblk_all(device=device,
                           columns=columns,
                           abspath=abspath)
    if not result:
        logger.debug(f"{device} not found is lsblk report")
        return {}

    return result[0]

def lsblk_all(device='', columns=None, abspath=False):
    """
    Create a dictionary of identifying values for a device using ``lsblk``.
    Each supported column is a key, in its *raw* format (all uppercase
    usually).  ``lsblk`` has support for certain "columns" (in blkid these
    would be labels), and these columns vary between distributions and
    ``lsblk`` versions. The newer versions support a richer set of columns,
    while older ones were a bit limited.

    These are a subset of lsblk columns which are known to work on both CentOS 7 and Xenial:

         NAME  device name
        KNAME  internal kernel device name
        PKNAME internal kernel parent device name
      MAJ:MIN  major:minor device number
       FSTYPE  filesystem type
   MOUNTPOINT  where the device is mounted
        LABEL  filesystem LABEL
         UUID  filesystem UUID
           RO  read-only device
           RM  removable device
        MODEL  device identifier
         SIZE  size of the device
        STATE  state of the device
        OWNER  user name
        GROUP  group name
         MODE  device node permissions
    ALIGNMENT  alignment offset
       MIN-IO  minimum I/O size
       OPT-IO  optimal I/O size
      PHY-SEC  physical sector size
      LOG-SEC  logical sector size
         ROTA  rotational device
        SCHED  I/O scheduler name
      RQ-SIZE  request queue size
         TYPE  device type
      PKNAME   internal parent kernel device name
     DISC-ALN  discard alignment offset
    DISC-GRAN  discard granularity
     DISC-MAX  discard max bytes
    DISC-ZERO  discard zeroes data

    There is a bug in ``lsblk`` where using all the available (supported)
    columns will result in no output (!), in order to workaround this the
    following columns have been removed from the default reporting columns:

    * RQ-SIZE (request queue size)
    * MIN-IO  minimum I/O size
    * OPT-IO  optimal I/O size

    These should be available however when using `columns`. For example::

        >>> lsblk('/dev/sda1', columns=['OPT-IO'])
        {'OPT-IO': '0'}

    Normal CLI output, as filtered by the flags in this function will look like ::

        $ lsblk -P -o NAME,KNAME,PKNAME,MAJ:MIN,FSTYPE,MOUNTPOINT
        NAME="sda1" KNAME="sda1" MAJ:MIN="8:1" FSTYPE="ext4" MOUNTPOINT="/"

    :param columns: A list of columns to report as keys in its original form.
    :param abspath: Set the flag for absolute paths on the report
    """
    default_columns = [
        'NAME', 'KNAME', 'PKNAME', 'MAJ:MIN', 'FSTYPE', 'MOUNTPOINT', 'LABEL',
        'UUID', 'RO', 'RM', 'MODEL', 'SIZE', 'STATE', 'OWNER', 'GROUP', 'MODE',
        'ALIGNMENT', 'PHY-SEC', 'LOG-SEC', 'ROTA', 'SCHED', 'TYPE', 'DISC-ALN',
        'DISC-GRAN', 'DISC-MAX', 'DISC-ZERO', 'PKNAME', 'PARTLABEL'
    ]
    columns = columns or default_columns
    # -P       -> Produce pairs of COLUMN="value"
    # -p       -> Return full paths to devices, not just the names, when ``abspath`` is set
    # -o       -> Use the columns specified or default ones provided by this function
    base_command = ['lsblk', '-P']
    if abspath:
        base_command.append('-p')
    base_command.append('-o')
    base_command.append(','.join(columns))
    if device:
        base_command.append('--nodeps')
        base_command.append(device)

    out, err, rc = process.call(base_command)

    if rc != 0:
        raise RuntimeError(f"Error: {err}")

    result = []

    for line in out:
        result.append(_lsblk_parser(line))

    return result


def is_device(dev):
    """
    Boolean to determine if a given device is a block device (**not**
    a partition!)

    For example: /dev/sda would return True, but not /dev/sdc1
    """
    if not os.path.exists(dev):
        return False
    if not dev.startswith('/dev/'):
        return False
    if dev[len('/dev/'):].startswith('loop'):
        if not allow_loop_devices():
            return False

    # fallback to stat
    return _stat_is_device(os.lstat(dev).st_mode) and not is_partition(dev)


def is_partition(dev: str) -> bool:
    """
    Boolean to determine if a given device is a partition, like /dev/sda1
    """
    if not os.path.exists(dev):
        return False

    partitions = get_partitions()
    return dev.split("/")[-1] in partitions


def is_ceph_rbd(dev):
    """
    Boolean to determine if a given device is a ceph RBD device, like /dev/rbd0
    """
    return dev.startswith(('/dev/rbd'))


class BaseFloatUnit(float):
    """
    Base class to support float representations of size values. Suffix is
    computed on child classes by inspecting the class name
    """

    def __repr__(self):
        return "<%s(%s)>" % (self.__class__.__name__, self.__float__())

    def __str__(self):
        return "{size:.2f} {suffix}".format(
            size=self.__float__(),
            suffix=self.__class__.__name__.split('Float')[-1]
        )

    def as_int(self):
        return int(self.real)

    def as_float(self):
        return self.real


class FloatB(BaseFloatUnit):
    pass


class FloatMB(BaseFloatUnit):
    pass


class FloatGB(BaseFloatUnit):
    pass


class FloatKB(BaseFloatUnit):
    pass


class FloatTB(BaseFloatUnit):
    pass

class FloatPB(BaseFloatUnit):
    pass

class Size(object):
    """
    Helper to provide an interface for different sizes given a single initial
    input. Allows for comparison between different size objects, which avoids
    the need to convert sizes before comparison (e.g. comparing megabytes
    against gigabytes).

    Common comparison operators are supported::

        >>> hd1 = Size(gb=400)
        >>> hd2 = Size(gb=500)
        >>> hd1 > hd2
        False
        >>> hd1 < hd2
        True
        >>> hd1 == hd2
        False
        >>> hd1 == Size(gb=400)
        True

    The Size object can also be multiplied or divided::

        >>> hd1
        <Size(400.00 GB)>
        >>> hd1 * 2
        <Size(800.00 GB)>
        >>> hd1
        <Size(800.00 GB)>

    Additions and subtractions are only supported between Size objects::

        >>> Size(gb=224) - Size(gb=100)
        <Size(124.00 GB)>
        >>> Size(gb=1) + Size(mb=300)
        <Size(1.29 GB)>

    Can also display a human-readable representation, with automatic detection
    on best suited unit, or alternatively, specific unit representation::

        >>> s = Size(mb=2211)
        >>> s
        <Size(2.16 GB)>
        >>> s.mb
        <FloatMB(2211.0)>
        >>> print("Total size: %s" % s.mb)
        Total size: 2211.00 MB
        >>> print("Total size: %s" % s)
        Total size: 2.16 GB
    """

    @classmethod
    def parse(cls, size):
        if (len(size) > 2 and
            size[-2].lower() in ['k', 'm', 'g', 't', 'p'] and
            size[-1].lower() == 'b'):
            return cls(**{size[-2:].lower(): float(size[0:-2])})
        elif size[-1].lower() in ['b', 'k', 'm', 'g', 't', 'p']:
            return cls(**{size[-1].lower(): float(size[0:-1])})
        else:
            return cls(b=float(size))


    def __init__(self, multiplier=1024, **kw):
        self._multiplier = multiplier
        # create a mapping of units-to-multiplier, skip bytes as that is
        # calculated initially always and does not need to convert
        aliases = [
            [('k', 'kb', 'kilobytes'), self._multiplier],
            [('m', 'mb', 'megabytes'), self._multiplier ** 2],
            [('g', 'gb', 'gigabytes'), self._multiplier ** 3],
            [('t', 'tb', 'terabytes'), self._multiplier ** 4],
            [('p', 'pb', 'petabytes'), self._multiplier ** 5]
        ]
        # and mappings for units-to-formatters, including bytes and aliases for
        # each
        format_aliases = [
            [('b', 'bytes'), FloatB],
            [('kb', 'kilobytes'), FloatKB],
            [('mb', 'megabytes'), FloatMB],
            [('gb', 'gigabytes'), FloatGB],
            [('tb', 'terabytes'), FloatTB],
            [('pb', 'petabytes'), FloatPB],
        ]
        self._formatters = {}
        for key, value in format_aliases:
            for alias in key:
                self._formatters[alias] = value
        self._factors = {}
        for key, value in aliases:
            for alias in key:
                self._factors[alias] = value

        for k, v in kw.items():
            self._convert(v, k)
            # only pursue the first occurrence
            break

    def _convert(self, size, unit):
        """
        Convert any size down to bytes so that other methods can rely on bytes
        being available always, regardless of what they pass in, avoiding the
        need for a mapping of every permutation.
        """
        if unit in ['b', 'bytes']:
            self._b = size
            return
        factor = self._factors[unit]
        self._b = float(size * factor)

    def _get_best_format(self):
        """
        Go through all the supported units, and use the first one that is less
        than 1024. This allows to represent size in the most readable format
        available
        """
        for unit in ['b', 'kb', 'mb', 'gb', 'tb', 'pb']:
            if getattr(self, unit) > 1024:
                continue
            return getattr(self, unit)

    def __repr__(self):
        return "<Size(%s)>" % self._get_best_format()

    def __str__(self):
        return "%s" % self._get_best_format()

    def __format__(self, spec):
        return str(self._get_best_format()).__format__(spec)

    def __int__(self):
        return int(self._b)

    def __float__(self):
        return self._b

    def __lt__(self, other):
        if isinstance(other, Size):
            return self._b < other._b
        else:
            return self.b < other

    def __le__(self, other):
        if isinstance(other, Size):
            return self._b <= other._b
        else:
            return self.b <= other

    def __eq__(self, other):
        if isinstance(other, Size):
            return self._b == other._b
        else:
            return self.b == other

    def __ne__(self, other):
        if isinstance(other, Size):
            return self._b != other._b
        else:
            return self.b != other

    def __ge__(self, other):
        if isinstance(other, Size):
            return self._b >= other._b
        else:
            return self.b >= other

    def __gt__(self, other):
        if isinstance(other, Size):
            return self._b > other._b
        else:
            return self.b > other

    def __add__(self, other):
        if isinstance(other, Size):
            _b = self._b + other._b
            return Size(b=_b)
        raise TypeError('Cannot add "Size" object with int')

    def __sub__(self, other):
        if isinstance(other, Size):
            _b = self._b - other._b
            return Size(b=_b)
        raise TypeError('Cannot subtract "Size" object from int')

    def __mul__(self, other):
        if isinstance(other, Size):
            raise TypeError('Cannot multiply with "Size" object')
        _b = self._b * other
        return Size(b=_b)

    def __truediv__(self, other):
        if isinstance(other, Size):
            return self._b / other._b
        _b = self._b / other
        return Size(b=_b)

    def __div__(self, other):
        if isinstance(other, Size):
            return self._b / other._b
        _b = self._b / other
        return Size(b=_b)

    def __bool__(self):
        return self.b != 0

    def __nonzero__(self):
        return self.__bool__()

    def __getattr__(self, unit):
        """
        Calculate units on the fly, relies on the fact that ``bytes`` has been
        converted at instantiation. Units that don't exist will trigger an
        ``AttributeError``
        """
        try:
            formatter = self._formatters[unit]
        except KeyError:
            raise AttributeError('Size object has not attribute "%s"' % unit)
        if unit in ['b', 'bytes']:
            return formatter(self._b)
        try:
            factor = self._factors[unit]
        except KeyError:
            raise AttributeError('Size object has not attribute "%s"' % unit)
        return formatter(float(self._b) / factor)


def human_readable_size(size):
    """
    Take a size in bytes, and transform it into a human readable size with up
    to two decimals of precision.
    """
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    for suffix in suffixes:
        if size >= 1024:
            size = size / 1024
        else:
            break
    return "{size:.2f} {suffix}".format(
        size=size,
        suffix=suffix)


def size_from_human_readable(s):
    """
    Takes a human readable string and converts into a Size. If no unit is
    passed, bytes is assumed.
    """
    s = s.replace(' ', '')
    if s[-1].isdigit():
        return Size(b=float(s))
    n = float(s[:-1])
    if s[-1].lower() == 'p':
        return Size(pb=n)
    if s[-1].lower() == 't':
        return Size(tb=n)
    if s[-1].lower() == 'g':
        return Size(gb=n)
    if s[-1].lower() == 'm':
        return Size(mb=n)
    if s[-1].lower() == 'k':
        return Size(kb=n)
    return None


def get_partitions_facts(sys_block_path):
    partition_metadata = {}
    for folder in os.listdir(sys_block_path):
        folder_path = os.path.join(sys_block_path, folder)
        if os.path.exists(os.path.join(folder_path, 'partition')):
            contents = get_file_contents(os.path.join(folder_path, 'partition'))
            if contents:
                part = {}
                partname = folder
                part_sys_block_path = os.path.join(sys_block_path, partname)

                part['start'] = get_file_contents(part_sys_block_path + "/start", 0)
                part['sectors'] = get_file_contents(part_sys_block_path + "/size", 0)

                part['sectorsize'] = get_file_contents(
                    part_sys_block_path + "/queue/logical_block_size")
                if not part['sectorsize']:
                    part['sectorsize'] = get_file_contents(
                        part_sys_block_path + "/queue/hw_sector_size", 512)
                part['size'] = float(part['sectors']) * 512
                part['human_readable_size'] = human_readable_size(float(part['sectors']) * 512)
                part['holders'] = []
                for holder in os.listdir(part_sys_block_path + '/holders'):
                    part['holders'].append(holder)

                partition_metadata[partname] = part
    return partition_metadata


def is_mapper_device(device_name):
    return device_name.startswith(('/dev/mapper', '/dev/dm-'))


class AllowLoopDevices(object):
    allow = False
    warned = False

    @classmethod
    def __call__(cls):
        val = os.environ.get("CEPH_VOLUME_ALLOW_LOOP_DEVICES", "false").lower()
        if val not in ("false", 'no', '0'):
            cls.allow = True
            if not cls.warned:
                logger.warning(
                    "CEPH_VOLUME_ALLOW_LOOP_DEVICES is set in your "
                    "environment, so we will allow the use of unattached loop"
                    " devices as disks. This feature is intended for "
                    "development purposes only and will never be supported in"
                    " production. Issues filed based on this behavior will "
                    "likely be ignored."
                )
                cls.warned = True
        return cls.allow


allow_loop_devices = AllowLoopDevices()


def get_block_devs_sysfs(_sys_block_path='/sys/block', _sys_dev_block_path='/sys/dev/block', device=''):
    def holder_inner_loop():
        for holder in holders:
            # /sys/block/sdy/holders/dm-8/dm/uuid
            holder_dm_type = get_file_contents(os.path.join(_sys_block_path, dev, f'holders/{holder}/dm/uuid')).split('-')[0].lower()
            if holder_dm_type == 'mpath':
                return True

    # First, get devices that are _not_ partitions
    result = list()
    if not device:
        dev_names = os.listdir(_sys_block_path)
    else:
        dev_names = [device]
    for dev in dev_names:
        name = kname = os.path.join("/dev", dev)
        if not os.path.exists(name):
            continue
        type_ = 'disk'
        holders = os.listdir(os.path.join(_sys_block_path, dev, 'holders'))
        if holder_inner_loop():
            continue
        dm_dir_path = os.path.join(_sys_block_path, dev, 'dm')
        if os.path.isdir(dm_dir_path):
            dm_type = get_file_contents(os.path.join(dm_dir_path, 'uuid'))
            type_ = dm_type.split('-')[0].lower()
            basename = get_file_contents(os.path.join(dm_dir_path, 'name'))
            name = os.path.join("/dev/mapper", basename)
        if dev.startswith('loop'):
            if not allow_loop_devices():
                continue
            # Skip loop devices that are not attached
            if not os.path.exists(os.path.join(_sys_block_path, dev, 'loop')):
                continue
            type_ = 'loop'
        result.append([kname, name, type_])
    # Next, look for devices that _are_ partitions
    for item in os.listdir(_sys_dev_block_path):
        is_part = get_file_contents(os.path.join(_sys_dev_block_path, item, 'partition')) == "1"
        dev = os.path.basename(os.readlink(os.path.join(_sys_dev_block_path, item)))
        if not is_part:
            continue
        name = kname = os.path.join("/dev", dev)
        result.append([name, kname, "part"])
    return sorted(result, key=lambda x: x[0])


def get_devices(_sys_block_path='/sys/block', device=''):
    """
    Captures all available block devices as reported by lsblk.
    Additional interesting metadata like sectors, size, vendor,
    solid/rotational, etc. is collected from /sys/block/<device>

    Returns a dictionary, where keys are the full paths to devices.

    ..note:: loop devices, removable media, and logical volumes are never included.
    """

    device_facts = {}

    block_devs = get_block_devs_sysfs(_sys_block_path)

    block_types = ['disk', 'mpath']
    if allow_loop_devices():
        block_types.append('loop')

    for block in block_devs:
        devname = os.path.basename(block[0])
        diskname = block[1]
        if block[2] not in block_types:
            continue
        sysdir = os.path.join(_sys_block_path, devname)
        metadata = {}

        # If the device is ceph rbd it gets excluded
        if is_ceph_rbd(diskname):
            continue

        # If the mapper device is a logical volume it gets excluded
        if is_mapper_device(diskname):
            if lvm.get_device_lvs(diskname):
                continue

        # all facts that have no defaults
        # (<name>, <path relative to _sys_block_path>)
        facts = [('removable', 'removable'),
                 ('ro', 'ro'),
                 ('vendor', 'device/vendor'),
                 ('model', 'device/model'),
                 ('rev', 'device/rev'),
                 ('sas_address', 'device/sas_address'),
                 ('sas_device_handle', 'device/sas_device_handle'),
                 ('support_discard', 'queue/discard_granularity'),
                 ('rotational', 'queue/rotational'),
                 ('nr_requests', 'queue/nr_requests'),
                ]
        for key, file_ in facts:
            metadata[key] = get_file_contents(os.path.join(sysdir, file_))

        device_slaves = os.listdir(os.path.join(sysdir, 'slaves'))
        if device_slaves:
            metadata['device_nodes'] = ','.join(device_slaves)
        else:
            metadata['device_nodes'] = devname

        metadata['actuators'] = ""
        if os.path.isdir(sysdir + "/queue/independent_access_ranges/"):
            actuators = 0
            while os.path.isdir(sysdir + "/queue/independent_access_ranges/" + str(actuators)):
                actuators += 1
            metadata['actuators'] = actuators

        metadata['scheduler_mode'] = ""
        scheduler = get_file_contents(sysdir + "/queue/scheduler")
        if scheduler is not None:
            m = re.match(r".*?(\[(.*)\])", scheduler)
            if m:
                metadata['scheduler_mode'] = m.group(2)

        metadata['partitions'] = get_partitions_facts(sysdir)

        size = get_file_contents(os.path.join(sysdir, 'size'), 0)

        metadata['sectors'] = get_file_contents(os.path.join(sysdir, 'sectors'), 0)
        fallback_sectorsize = get_file_contents(sysdir + "/queue/hw_sector_size", 512)
        metadata['sectorsize'] = get_file_contents(sysdir +
                                                   "/queue/logical_block_size",
                                                   fallback_sectorsize)
        metadata['size'] = float(size) * 512
        metadata['human_readable_size'] = human_readable_size(metadata['size'])
        metadata['path'] = diskname
        metadata['type'] = block[2]

        # some facts from udevadm
        p = udevadm_property(sysdir)
        metadata['id_bus'] = p.get('ID_BUS', '')

        device_facts[diskname] = metadata
    return device_facts

def has_bluestore_label(device_path):
    isBluestore = False
    bluestoreDiskSignature = 'bluestore block device' # 22 bytes long

    # throws OSError on failure
    logger.info("opening device {} to check for BlueStore label".format(device_path))
    try:
        with open(device_path, "rb") as fd:
            # read first 22 bytes looking for bluestore disk signature
            signature = fd.read(22)
            if signature.decode('ascii', 'replace') == bluestoreDiskSignature:
                isBluestore = True
    except IsADirectoryError:
        logger.info(f'{device_path} is a directory, skipping.')

    return isBluestore
