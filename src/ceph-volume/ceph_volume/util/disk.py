import logging
import os
import re
import stat
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
        ['blkid', '-s', 'PARTUUID', '-o', 'value', device]
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
        ['blkid', '-p', device]
    )
    return _blkid_parser(' '.join(out))


def get_part_entry_type(device):
    """
    Parses the ``ID_PART_ENTRY_TYPE`` from the "low level" (bypasses the cache)
    output that uses the ``udev`` type of output. This output is intended to be
    used for udev rules, but it is useful in this case as it is the only
    consistent way to retrieve the GUID used by ceph-disk to identify devices.
    """
    out, err, rc = process.call(['blkid', '-p', '-o', 'udev', device])
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
        ['blkid', '-t', 'PARTUUID="%s"' % partuuid, '-o', 'device']
    )
    return ' '.join(out).strip()


def remove_partition(device):
    """
    Removes a partition using parted

    :param device: A ``Device()`` object
    """
    parent_device = '/dev/%s' % device.disk_api['PKNAME']
    udev_info = udevadm_property(device.abspath)
    partition_number = udev_info.get('ID_PART_ENTRY_NUMBER')
    if not partition_number:
        raise RuntimeError('Unable to detect the partition number for device: %s' % device.abspath)

    process.run(
        ['parted', parent_device, '--script', '--', 'rm', partition_number]
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

        $ lsblk --nodeps -P -o NAME,KNAME,MAJ:MIN,FSTYPE,MOUNTPOINT
        NAME="sda1" KNAME="sda1" MAJ:MIN="8:1" FSTYPE="ext4" MOUNTPOINT="/"

    :param columns: A list of columns to report as keys in its original form.
    :param abspath: Set the flag for absolute paths on the report
    """
    default_columns = [
        'NAME', 'KNAME', 'MAJ:MIN', 'FSTYPE', 'MOUNTPOINT', 'LABEL', 'UUID',
        'RO', 'RM', 'MODEL', 'SIZE', 'STATE', 'OWNER', 'GROUP', 'MODE',
        'ALIGNMENT', 'PHY-SEC', 'LOG-SEC', 'ROTA', 'SCHED', 'TYPE', 'DISC-ALN',
        'DISC-GRAN', 'DISC-MAX', 'DISC-ZERO', 'PKNAME', 'PARTLABEL'
    ]
    device = device.rstrip('/')
    columns = columns or default_columns
    # --nodeps -> Avoid adding children/parents to the device, only give information
    #             on the actual device we are querying for
    # -P       -> Produce pairs of COLUMN="value"
    # -p       -> Return full paths to devices, not just the names, when ``abspath`` is set
    # -o       -> Use the columns specified or default ones provided by this function
    base_command = ['lsblk', '--nodeps', '-P']
    if abspath:
        base_command.append('-p')
    base_command.append('-o')
    base_command.append(','.join(columns))
    base_command.append(device)
    out, err, rc = process.call(base_command)

    if rc != 0:
        return {}

    return _lsblk_parser(' '.join(out))


def is_device(dev):
    """
    Boolean to determine if a given device is a block device (**not**
    a partition!)

    For example: /dev/sda would return True, but not /dev/sdc1
    """
    if not os.path.exists(dev):
        return False
    # use lsblk first, fall back to using stat
    TYPE = lsblk(dev).get('TYPE')
    if TYPE:
        return TYPE == 'disk'

    # fallback to stat
    return _stat_is_device(os.lstat(dev).st_mode)
    if stat.S_ISBLK(os.lstat(dev)):
        return True
    return False


def is_partition(dev):
    """
    Boolean to determine if a given device is a partition, like /dev/sda1
    """
    if not os.path.exists(dev):
        return False
    # use lsblk first, fall back to using stat
    TYPE = lsblk(dev).get('TYPE')
    if TYPE:
        return TYPE == 'part'

    # fallback to stat
    stat_obj = os.stat(dev)
    if _stat_is_device(stat_obj.st_mode):
        return False

    major = os.major(stat_obj.st_rdev)
    minor = os.minor(stat_obj.st_rdev)
    if os.path.exists('/sys/dev/block/%d:%d/partition' % (major, minor)):
        return True
    return False


def _map_dev_paths(_path, include_abspath=False, include_realpath=False):
    """
    Go through all the items in ``_path`` and map them to their absolute path::

        {'sda': '/dev/sda'}

    If ``include_abspath`` is set, then a reverse mapping is set as well::

        {'sda': '/dev/sda', '/dev/sda': 'sda'}

    If ``include_realpath`` is set then the same operation is done for any
    links found when listing, these are *not* reversed to avoid clashing on
    existing keys, but both abspath and basename can be included. For example::

        {
            'ceph-data': '/dev/mapper/ceph-data',
            '/dev/mapper/ceph-data': 'ceph-data',
            '/dev/dm-0': '/dev/mapper/ceph-data',
            'dm-0': '/dev/mapper/ceph-data'
        }


    In case of possible exceptions the mapping is returned empty, and the
    exception is logged.
    """
    mapping = {}
    try:
        dev_names = os.listdir(_path)
    except (OSError, IOError):
        logger.exception('unable to list block devices from: %s' % _path)
        return {}

    for dev_name in dev_names:
        mapping[dev_name] = os.path.join(_path, dev_name)

    if include_abspath:
        for k, v in list(mapping.items()):
            mapping[v] = k

    if include_realpath:
        for abspath in list(mapping.values()):
            if not os.path.islink(abspath):
                continue

            realpath = os.path.realpath(abspath)
            basename = os.path.basename(realpath)
            mapping[basename] = abspath
            if include_abspath:
                mapping[realpath] = abspath

    return mapping


def get_block_devs(sys_block_path="/sys/block", skip_loop=True):
    """
    Go through all the items in /sys/block and return them as a list.

    The ``sys_block_path`` argument is set for easier testing and is not
    required for proper operation.
    """
    devices = _map_dev_paths(sys_block_path).keys()
    if skip_loop:
        return [d for d in devices if not d.startswith('loop')]
    return list(devices)


def get_dev_devs(dev_path="/dev"):
    """
    Go through all the items in /dev and return them as a list.

    The ``dev_path`` argument is set for easier testing and is not
    required for proper operation.
    """
    return _map_dev_paths(dev_path, include_abspath=True)


def get_mapper_devs(mapper_path="/dev/mapper"):
    """
    Go through all the items in /dev and return them as a list.

    The ``dev_path`` argument is set for easier testing and is not
    required for proper operation.
    """
    return _map_dev_paths(mapper_path, include_abspath=True, include_realpath=True)


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
        >>> print "Total size: %s" % s.mb
        Total size: 2211.00 MB
        >>> print "Total size: %s" % s
        Total size: 2.16 GB
    """

    def __init__(self, multiplier=1024, **kw):
        self._multiplier = multiplier
        # create a mapping of units-to-multiplier, skip bytes as that is
        # calculated initially always and does not need to convert
        aliases = [
            [('kb', 'kilobytes'), self._multiplier],
            [('mb', 'megabytes'), self._multiplier ** 2],
            [('gb', 'gigabytes'), self._multiplier ** 3],
            [('tb', 'terabytes'), self._multiplier ** 4],
        ]
        # and mappings for units-to-formatters, including bytes and aliases for
        # each
        format_aliases = [
            [('b', 'bytes'), FloatB],
            [('kb', 'kilobytes'), FloatKB],
            [('mb', 'megabytes'), FloatMB],
            [('gb', 'gigabytes'), FloatGB],
            [('tb', 'terabytes'), FloatTB],
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
        for unit in ['b', 'kb', 'mb', 'gb', 'tb']:
            if getattr(self, unit) > 1024:
                continue
            return getattr(self, unit)

    def __repr__(self):
        return "<Size(%s)>" % self._get_best_format()

    def __str__(self):
        return "%s" % self._get_best_format()

    def __lt__(self, other):
        return self._b < other._b

    def __le__(self, other):
        return self._b <= other._b

    def __eq__(self, other):
        return self._b == other._b

    def __ne__(self, other):
        return self._b != other._b

    def __ge__(self, other):
        return self._b >= other._b

    def __gt__(self, other):
        return self._b > other._b

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
    suffixes = ['B', 'KB', 'MB', 'GB', 'TB']
    suffix_index = 0
    while size > 1024:
        suffix_index += 1
        size = size / 1024.0
    return "{size:.2f} {suffix}".format(
        size=size,
        suffix=suffixes[suffix_index])


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
                part['size'] = human_readable_size(float(part['sectors']) * 512)
                part['holders'] = []
                for holder in os.listdir(part_sys_block_path + '/holders'):
                    part['holders'].append(holder)

                partition_metadata[partname] = part
    return partition_metadata


def is_mapper_device(device_name):
    return device_name.startswith(('/dev/mapper', '/dev/dm-'))


def is_locked_raw_device(disk_path):
    """
    A device can be locked by a third party software like a database.
    To detect that case, the device is opened in Read/Write and exclusive mode
    """
    open_flags = (os.O_RDWR | os.O_EXCL)
    open_mode = 0
    fd = None

    try:
        fd = os.open(disk_path, open_flags, open_mode)
    except OSError:
        return 1

    try:
        os.close(fd)
    except OSError:
        return 1

    return 0


def get_devices(_sys_block_path='/sys/block', _dev_path='/dev', _mapper_path='/dev/mapper'):
    """
    Captures all available devices from /sys/block/, including its partitions,
    along with interesting metadata like sectors, size, vendor,
    solid/rotational, etc...

    Returns a dictionary, where keys are the full paths to devices.

    ..note:: dmapper devices get their path updated to what they link from, if
            /dev/dm-0 is linked by /dev/mapper/ceph-data, then the latter gets
            used as the key.

    ..note:: loop devices, removable media, and logical volumes are never included.
    """
    # Portions of this detection process are inspired by some of the fact
    # gathering done by Ansible in module_utils/facts/hardware/linux.py. The
    # processing of metadata and final outcome *is very different* and fully
    # imcompatible. There are ignored devices, and paths get resolved depending
    # on dm devices, loop, and removable media

    device_facts = {}

    block_devs = get_block_devs(_sys_block_path)
    dev_devs = get_dev_devs(_dev_path)
    mapper_devs = get_mapper_devs(_mapper_path)

    for block in block_devs:
        sysdir = os.path.join(_sys_block_path, block)
        metadata = {}

        # Ensure that the diskname is an absolute path and that it never points
        # to a /dev/dm-* device
        diskname = mapper_devs.get(block) or dev_devs.get(block)
        if not diskname:
            continue

        # If the mapper device is a logical volume it gets excluded
        if is_mapper_device(diskname):
            if lvm.is_lv(diskname):
                continue

        metadata['removable'] = get_file_contents(os.path.join(sysdir, 'removable'))
        # Is the device read-only ?
        metadata['ro'] = get_file_contents(os.path.join(sysdir, 'ro'))


        for key in ['vendor', 'model', 'rev', 'sas_address', 'sas_device_handle']:
            metadata[key] = get_file_contents(sysdir + "/device/" + key)

        for key in ['sectors', 'size']:
            metadata[key] = get_file_contents(os.path.join(sysdir, key), 0)

        for key, _file in [('support_discard', '/queue/discard_granularity')]:
            metadata[key] = get_file_contents(os.path.join(sysdir, _file))

        metadata['partitions'] = get_partitions_facts(sysdir)

        for key in ['rotational', 'nr_requests']:
            metadata[key] = get_file_contents(sysdir + "/queue/" + key)

        metadata['scheduler_mode'] = ""
        scheduler = get_file_contents(sysdir + "/queue/scheduler")
        if scheduler is not None:
            m = re.match(r".*?(\[(.*)\])", scheduler)
            if m:
                metadata['scheduler_mode'] = m.group(2)

        if not metadata['sectors']:
            metadata['sectors'] = 0
        size = metadata['sectors'] or metadata['size']
        metadata['sectorsize'] = get_file_contents(sysdir + "/queue/logical_block_size")
        if not metadata['sectorsize']:
            metadata['sectorsize'] = get_file_contents(sysdir + "/queue/hw_sector_size", 512)
        metadata['human_readable_size'] = human_readable_size(float(size) * 512)
        metadata['size'] = float(size) * 512
        metadata['path'] = diskname
        metadata['locked'] = is_locked_raw_device(metadata['path'])

        device_facts[diskname] = metadata
    return device_facts
