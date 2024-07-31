import logging
import os
import re
import stat
import time
import json
from ceph_volume import process, allow_loop_devices
from ceph_volume.api import lvm
from ceph_volume.util.system import get_file_contents
from typing import Dict, List, Any, Union


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

    TYPE = lsblk(dev).get('TYPE')
    if TYPE:
        return TYPE in ['disk', 'mpath']

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


def get_block_devs_sysfs(_sys_block_path: str = '/sys/block', _sys_dev_block_path: str = '/sys/dev/block', device: str = '') -> List[List[str]]:
    def holder_inner_loop() -> bool:
        for holder in holders:
            # /sys/block/sdy/holders/dm-8/dm/uuid
            holder_dm_type: str = get_file_contents(os.path.join(_sys_block_path, dev, f'holders/{holder}/dm/uuid')).split('-')[0].lower()
            if holder_dm_type == 'mpath':
                return True

    # First, get devices that are _not_ partitions
    result: List[List[str]] = list()
    if not device:
        dev_names: List[str] = os.listdir(_sys_block_path)
    else:
        dev_names = [device]
    for dev in dev_names:
        name = kname = pname = os.path.join("/dev", dev)
        if not os.path.exists(name):
            continue
        type_: str = 'disk'
        holders: List[str] = os.listdir(os.path.join(_sys_block_path, dev, 'holders'))
        if holder_inner_loop():
            continue
        dm_dir_path: str = os.path.join(_sys_block_path, dev, 'dm')
        if os.path.isdir(dm_dir_path):
            dm_type: str = get_file_contents(os.path.join(dm_dir_path, 'uuid'))
            type_: List[str] = dm_type.split('-')[0].lower()
            basename: str = get_file_contents(os.path.join(dm_dir_path, 'name'))
            name: str = os.path.join("/dev/mapper", basename)
        if dev.startswith('loop'):
            if not allow_loop_devices():
                continue
            # Skip loop devices that are not attached
            if not os.path.exists(os.path.join(_sys_block_path, dev, 'loop')):
                continue
            type_ = 'loop'
        result.append([kname, name, type_, pname])
    # Next, look for devices that _are_ partitions
    partitions: Dict[str, str] = get_partitions()
    for partition in partitions.keys():
        name = kname = os.path.join("/dev", partition)
        result.append([name, kname, "part", partitions[partition]])
    return sorted(result, key=lambda x: x[0])

def get_partitions(_sys_dev_block_path: str ='/sys/dev/block') -> Dict[str, str]:
    """
    Retrieves a dictionary mapping partition system names to their parent device names.

    Args:
        _sys_dev_block_path (str, optional): The path to the system's block device directory.
                                             Defaults to '/sys/dev/block'.

    Returns:
        Dict[str, str]: A dictionary where the keys are partition system names, and the values are
                        the corresponding parent device names.
    """
    devices: List[str] = os.listdir(_sys_dev_block_path)
    result: Dict[str, str] = {}
    for device in devices:
        device_path: str = os.path.join(_sys_dev_block_path, device)
        is_partition: bool = int(get_file_contents(os.path.join(device_path, 'partition'), '0')) > 0
        if not is_partition:
            continue

        partition_sys_name: str = os.path.basename(os.path.realpath(device_path))
        parent_device_sys_name: str = os.path.realpath(device_path).split('/')[-2:-1][0]
        result[partition_sys_name] = parent_device_sys_name
    return result

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

    block_types = ['disk', 'mpath', 'lvm', 'part']
    if allow_loop_devices():
        block_types.append('loop')

    for block in block_devs:
        metadata: Dict[str, Any] = {}
        if block[2] == 'lvm':
            block[1] = UdevData(block[1]).slashed_path
        devname = os.path.basename(block[0])
        diskname = block[1]
        if block[2] not in block_types:
            continue
        sysdir = os.path.join(_sys_block_path, devname)
        if block[2] == 'part':
            sysdir = os.path.join(_sys_block_path, block[3], devname)

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

        device_slaves = []
        if block[2] != 'part':
            device_slaves = os.listdir(os.path.join(sysdir, 'slaves'))
            metadata['partitions'] = get_partitions_facts(sysdir)

        metadata['device_nodes'] = []
        if device_slaves:
            metadata['device_nodes'].extend(device_slaves)
        else:
            if block[2] == 'part':
                metadata['device_nodes'].append(block[3])
            else:
                metadata['device_nodes'].append(devname)

        metadata['actuators'] = None
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
        metadata['devname'] = devname
        metadata['type'] = block[2]
        metadata['parent'] = block[3]

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

def get_lvm_mappers(sys_block_path: str = '/sys/block') -> List[str]:
    """
    Retrieve a list of Logical Volume Manager (LVM) device mappers.

    This function scans the given system block path for device mapper (dm) devices
    and identifies those that are managed by LVM. For each LVM device found, it adds
    the corresponding paths to the result list.

    Args:
        sys_block_path (str, optional): The path to the system block directory. Defaults to '/sys/block'.

    Returns:
        List[str]: A list of strings representing the paths of LVM device mappers.
                   Each LVM device will have two entries: the /dev/mapper/ path and the /dev/ path.
    """
    result: List[str] = []
    for device in os.listdir(sys_block_path):
        path: str = os.path.join(sys_block_path, device, 'dm')
        uuid_path: str = os.path.join(path, 'uuid')
        name_path: str = os.path.join(path, 'name')

        if os.path.exists(uuid_path):
            with open(uuid_path, 'r') as f:
                mapper_type: str = f.read().split('-')[0]

            if mapper_type == 'LVM':
                with open(name_path, 'r') as f:
                    name: str = f.read()
                    result.append(f'/dev/mapper/{name.strip()}')
                    result.append(f'/dev/{device}')
    return result

def _dd_read(device: str, count: int, skip: int = 0) -> str:
    """Read bytes from a device

    Args:
        device (str): The device to read bytes from.
        count (int): The number of bytes to read.
        skip (int, optional): The number of bytes to skip at the beginning. Defaults to 0.

    Returns:
        str: A string containing the read bytes.
    """
    result: str = ''
    try:
        with open(device, 'rb') as b:
            b.seek(skip)
            data: bytes = b.read(count)
            result = data.decode('utf-8').replace('\x00', '')
    except OSError:
        logger.warning(f"Can't read from {device}")
        pass
    except UnicodeDecodeError:
        pass
    except Exception as e:
        logger.error(f"An error occurred while reading from {device}: {e}")
        raise

    return result

def _dd_write(device: str, data: Union[str, bytes], skip: int = 0) -> None:
    """Write bytes to a device

    Args:
        device (str): The device to write bytes to.
        data (str): The data to write to the device.
        skip (int, optional): The number of bytes to skip at the beginning. Defaults to 0.

    Raises:
        OSError: If there is an error opening or writing to the device.
        Exception: If any other error occurs during the write operation.
    """

    if isinstance(data, str):
        data = data.encode('utf-8')

    try:
        with open(device, 'r+b') as b:
            b.seek(skip)
            b.write(data)
    except OSError:
        logger.warning(f"Can't write to {device}")
        raise
    except Exception as e:
        logger.error(f"An error occurred while writing to {device}: {e}")
        raise

def get_bluestore_header(device: str) -> Dict[str, Any]:
    """Retrieve BlueStore header information from a given device.

    This function retrieves BlueStore header information from the specified 'device'.
    It first checks if the device exists. If the device does not exist, a RuntimeError
    is raised. Then, it calls the 'ceph-bluestore-tool' command to show the label
    information of the device. If the command execution is successful, it parses the
    JSON output containing the BlueStore header information and returns it as a dictionary.

    Args:
        device (str): The path to the device.

    Returns:
        Dict[str, Any]: A dictionary containing BlueStore header information.
    """
    data: Dict[str, Any] = {}

    if os.path.exists(device):
        out, err, rc = process.call([
            'ceph-bluestore-tool', 'show-label',
            '--dev', device], verbose_on_failure=False)
        if rc:
            logger.debug(f'device {device} is not BlueStore; ceph-bluestore-tool failed to get info from device: {out}\n{err}')
        else:
            data = json.loads(''.join(out))
    else:
        logger.warning(f'device {device} not found.')
    return data

def bluestore_info(device: str, bluestore_labels: Dict[str, Any]) -> Dict[str, Any]:
    """Build a dict representation of a BlueStore header

    Args:
        device (str): The path of the BlueStore device.
        bluestore_labels (Dict[str, Any]): Plain text output from `ceph-bluestore-tool show-label`

    Returns:
        Dict[str, Any]: Generated dict representation of the BlueStore header
    """
    result: Dict[str, Any] = {}
    result['osd_uuid'] = bluestore_labels[device]['osd_uuid']
    if bluestore_labels[device]['description'] == 'main':
        whoami = bluestore_labels[device]['whoami']
        result.update({
            'type': bluestore_labels[device].get('type', 'bluestore'),
            'osd_id': int(whoami),
            'ceph_fsid': bluestore_labels[device]['ceph_fsid'],
            'device': device,
        })
        if bluestore_labels[device].get('db_device_uuid', ''):
            result['db_device_uuid'] = bluestore_labels[device].get('db_device_uuid')
        if bluestore_labels[device].get('wal_device_uuid', ''):
            result['wal_device_uuid'] = bluestore_labels[device].get('wal_device_uuid')
    elif bluestore_labels[device]['description'] == 'bluefs db':
        result['device_db'] = device
    elif bluestore_labels[device]['description'] == 'bluefs wal':
        result['device_wal'] = device
    return result

def get_block_device_holders(sys_block: str = '/sys/block') -> Dict[str, Any]:
    """Get a dictionary of device mappers with their corresponding parent devices.

    This function retrieves information about device mappers and their parent devices
    from the '/sys/block' directory. It iterates through each directory within 'sys_block',
    and for each directory, it checks if a 'holders' directory exists. If so, it lists
    the contents of the 'holders' directory and constructs a dictionary where the keys
    are the device mappers and the values are their corresponding parent devices.

    Args:
        sys_block (str, optional): The path to the '/sys/block' directory. Defaults to '/sys/block'.

    Returns:
        Dict[str, Any]: A dictionary where keys are device mappers (e.g., '/dev/mapper/...') and
        values are their corresponding parent devices (e.g., '/dev/sdX').
    """
    result: Dict[str, Any] = {}
    for b in os.listdir(sys_block):
        path: str = os.path.join(sys_block, b, 'holders')
        if os.path.exists(path):
            for h in os.listdir(path):
                result[f'/dev/{h}'] = f'/dev/{b}'

    return result

def has_holders(device: str) -> bool:
    """Check if a given device has any associated holders.

    This function determines whether the specified device has associated holders
    (e.g., other devices that depend on it) by checking if the device's real path
    appears in the values of the dictionary returned by `get_block_device_holders`.

    Args:
        device (str): The path to the device (e.g., '/dev/sdX') to check.

    Returns:
        bool: True if the device has holders, False otherwise.
    """
    return os.path.realpath(device) in get_block_device_holders().values()

def get_parent_device_from_mapper(mapper: str, abspath: bool = True) -> str:
    """Get the parent device corresponding to a given device mapper.

    This function retrieves the parent device corresponding to a given device mapper
    from the dictionary returned by the 'get_block_device_holders' function. It first
    checks if the specified 'mapper' exists. If it does, it resolves the real path of
    the mapper using 'os.path.realpath'. Then, it attempts to retrieve the parent device
    from the dictionary. If the mapper is not found in the dictionary, an empty string
    is returned.

    Args:
        mapper (str): The path to the device mapper.
        abspath (bool, optional): If True (default), returns the absolute path of the parent device.
                                  If False, returns only the basename of the parent device.

    Returns:
        str: The parent device corresponding to the given device mapper, or an empty string
        if the mapper is not found in the dictionary of device mappers.
    """
    result: str = ''
    if os.path.exists(mapper):
        _mapper: str = os.path.realpath(mapper)
        try:
            result = get_block_device_holders()[_mapper]
            if not abspath:
                result = os.path.basename(result)
        except KeyError:
            pass
    return result

def get_lvm_mapper_path_from_dm(path: str, sys_block: str = '/sys/block') -> str:
    """Retrieve the logical volume path for a given device.

    This function takes the path of a device and returns the corresponding
    logical volume path by reading the 'dm/name' file within the sysfs
    directory.

    Args:
        path (str): The device path for which to retrieve the logical volume path.
        sys_block (str, optional): The base sysfs block directory. Defaults to '/sys/block'.

    Returns:
        str: The device mapper path in the 'dashed form' of '/dev/mapper/vg-lv'.
    """
    result: str = ''
    dev: str = os.path.basename(path)
    sys_block_path: str = os.path.join(sys_block, dev, 'dm/name')
    if os.path.exists(sys_block_path):
        with open(sys_block_path, 'r') as f:
            content: str = f.read()
            result = f'/dev/mapper/{content}'
    return result.strip()


class BlockSysFs:
    def __init__(self,
                 path: str,
                 sys_dev_block: str = '/sys/dev/block',
                 sys_block: str = '/sys/block') -> None:
        """
        Initializes a BlockSysFs object.

        Args:
            path (str): The path to the block device.
            sys_dev_block (str, optional): Path to the sysfs directory containing block devices.
                                           Defaults to '/sys/dev/block'.
            sys_block (str, optional): Path to the sysfs directory containing block information.
                                       Defaults to '/sys/block'.
        """
        self.path: str = path
        self.name: str = os.path.basename(os.path.realpath(self.path))
        self.sys_dev_block: str = sys_dev_block
        self.sys_block: str = sys_block

    @property
    def is_partition(self) -> bool:
        """
        Checks if the current block device is a partition.

        Returns:
            bool: True if it is a partition, False otherwise.
        """
        path: str = os.path.join(self.get_sys_dev_block_path, 'partition')
        return os.path.exists(path)

    @property
    def holders(self) -> List[str]:
        """
        Retrieves the holders of the current block device.

        Returns:
            List[str]: A list of holders (other devices) associated with this block device.
        """
        result: List[str] = []
        path: str = os.path.join(self.get_sys_dev_block_path, 'holders')
        if os.path.exists(path):
            result = os.listdir(path)
        return result

    @property
    def get_sys_dev_block_path(self) -> str:
        """
        Gets the sysfs path for the current block device.

        Returns:
            str: The sysfs path corresponding to this block device.
        """
        sys_dev_block_path: str = ''
        devices: List[str] = os.listdir(self.sys_dev_block)
        for device in devices:
            path = os.path.join(self.sys_dev_block, device)
            if os.path.realpath(path).split('/')[-1:][0] == self.name:
                sys_dev_block_path = path
        return sys_dev_block_path

    @property
    def has_active_mappers(self) -> bool:
        """
        Checks if there are any active device mappers for the current block device.

        Returns:
            bool: True if active mappers exist, False otherwise.
        """
        return len(self.active_mappers()) > 0

    @property
    def has_active_dmcrypt_mapper(self) -> bool:
        """
        Checks if there is an active dm-crypt (disk encryption) mapper for the current block device.

        Returns:
            bool: True if an active dm-crypt mapper exists, False otherwise.
        """
        return any(value.get('type') == 'CRYPT' for value in self.active_mappers().values())

    def active_mappers(self) -> Dict[str, Any]:
        """
        Retrieves information about active device mappers for the current block device.

        Returns:
            Dict[str, Any]: A dictionary containing details about active device mappers.
                            Keys are the holders, and values provide details like type,
                            dm-crypt metadata, and LVM UUIDs.
        """
        result: Dict[str, Any] = {}
        for holder in self.holders:
            path: str = os.path.join(self.sys_block, holder, 'dm/uuid')
            if os.path.exists(path):
                result[holder] = {}
                with open(path, 'r') as f:
                    content: str = f.read().strip()
                    content_split: List[str] = content.split('-', maxsplit=3)
                    mapper_type: str = content_split[0]
                    result[holder]['type'] = mapper_type
                    if mapper_type == 'CRYPT':
                        result[holder]['dmcrypt_type'] = content_split[1]
                        result[holder]['dmcrypt_uuid'] = content_split[2]
                        result[holder]['dmcrypt_mapping'] = content_split[3]
                    if mapper_type == 'LVM':
                        result[holder]['uuid'] = content_split[1]
        return result


class UdevData:
    """
    Class representing udev data for a specific device.
    This class extracts and stores relevant information about the device from udev files.

    Attributes:
    -----------
    path : str
        The initial device path (e.g., /dev/sda).
    realpath : str
        The resolved real path of the device.
    stats : os.stat_result
        The result of the os.stat() call to retrieve device metadata.
    major : int
        The device's major number.
    minor : int
        The device's minor number.
    udev_data_path : str
        The path to the udev metadata for the device (e.g., /run/udev/data/b<major>:<minor>).
    symlinks : List[str]
        A list of symbolic links pointing to the device.
    id : str
        A unique identifier for the device.
    environment : Dict[str, str]
        A dictionary containing environment variables extracted from the udev data.
    group : str
        The group associated with the device.
    queue : str
        The queue associated with the device.
    version : str
        The version of the device or its metadata.
    """
    def __init__(self, path: str) -> None:
        """Initialize an instance of the UdevData class and load udev information.

        Args:
            path (str): The path to the device to be analyzed (e.g., /dev/sda).

        Raises:
            RuntimeError: Raised if no udev data file is found for the specified device.
        """
        if not os.path.exists(path):
            raise RuntimeError(f'{path} not found.')
        self.path: str = path
        self.realpath: str = os.path.realpath(self.path)
        self.stats: os.stat_result = os.stat(self.realpath)
        self.major: int = os.major(self.stats.st_rdev)
        self.minor: int = os.minor(self.stats.st_rdev)
        self.udev_data_path: str = f'/run/udev/data/b{self.major}:{self.minor}'
        self.symlinks: List[str] = []
        self.id: str = ''
        self.environment: Dict[str, str] = {}
        self.group: str = ''
        self.queue: str = ''
        self.version: str = ''

        if not os.path.exists(self.udev_data_path):
            raise RuntimeError(f'No udev data could be retrieved for {self.path}')

        with open(self.udev_data_path, 'r') as f:
            content: str = f.read().strip()
            self.raw_data: List[str] = content.split('\n')

        for line in self.raw_data:
            data_type, data = line.split(':', 1)
            if data_type == 'S':
                self.symlinks.append(data)
            if data_type == 'I':
                self.id = data
            if data_type == 'E':
                key, value = data.split('=')
                self.environment[key] = value
            if data_type == 'G':
                self.group = data
            if data_type == 'Q':
                self.queue = data
            if data_type == 'V':
                self.version = data

    @property
    def is_dm(self) -> bool:
        """Check if the device is a device mapper (DM).

        Returns:
            bool: True if the device is a device mapper, otherwise False.
        """
        return 'DM_UUID' in self.environment.keys()

    @property
    def is_lvm(self) -> bool:
        """Check if the device is a Logical Volume Manager (LVM) volume.

        Returns:
            bool: True if the device is an LVM volume, otherwise False.
        """
        return self.environment.get('DM_UUID', '').startswith('LVM')

    @property
    def slashed_path(self) -> str:
        """Get the LVM path structured with slashes.

        Returns:
            str: A path using slashes if the device is an LVM volume (e.g., /dev/vgname/lvname),
                 otherwise the original path.
        """
        result: str = self.path
        if self.is_lvm:
            vg: str = self.environment.get('DM_VG_NAME', '')
            lv: str = self.environment.get('DM_LV_NAME', '')
            result = f'/dev/{vg}/{lv}'
        return result

    @property
    def dashed_path(self) -> str:
        """Get the LVM path structured with dashes.

        Returns:
            str: A path using dashes if the device is an LVM volume (e.g., /dev/mapper/vgname-lvname),
            otherwise the original path.
        """
        result: str = self.path
        if self.is_lvm:
            name: str = self.environment.get('DM_NAME', '')
            result = f'/dev/mapper/{name}'
        return result
