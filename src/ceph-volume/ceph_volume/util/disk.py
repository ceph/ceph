import os
import stat
from ceph_volume import process


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


def _lsblk_type(device):
    """
    Helper function that will use the ``TYPE`` label output of ``lsblk`` to determine
    if a device is a partition or disk.
    It does not process the output to return a boolean, but it does process it to return the
    """
    out, err, rc = process.call(
        ['blkid', '-s', 'PARTUUID', '-o', 'value', device]
    )
    return ' '.join(out).strip()


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
