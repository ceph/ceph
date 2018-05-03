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
            if '1' in contents:
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

                partition_metadata[partname] = part
    return partition_metadata


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

        # If the mapper device is a logical volume it gets excluded
        if diskname.startswith(('/dev/mapper', '/dev/dm-')):
            if lvm.is_lv(diskname):
                continue

        # If the device reports itself as 'removable', get it excluded
        metadata['removable'] = get_file_contents(os.path.join(sysdir, 'removable'))
        if metadata['removable'] == '1':
            continue

        for key in ['vendor', 'model', 'sas_address', 'sas_device_handle']:
            metadata[key] = get_file_contents(sysdir + "/device/" + key)

        for key in ['sectors', 'size']:
            metadata[key] = get_file_contents(os.path.join(sysdir, key), 0)

        for key, _file in [('support_discard', '/queue/discard_granularity')]:
            metadata[key] = get_file_contents(os.path.join(sysdir, _file))

        metadata['partitions'] = get_partitions_facts(sysdir)

        metadata['rotational'] = get_file_contents(sysdir + "/queue/rotational")
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
        metadata['size'] = human_readable_size(float(size) * 512)

        device_facts[diskname] = metadata
    return device_facts
