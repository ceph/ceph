import os
import stat
from ceph_volume import process


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


def lsblk(device, columns=None):
    """
    Create a dictionary of identifying values for a device using ``lsblk``.
    Each supported column is a key, in its *raw* format (all uppercase
    usually).  ``lsblk`` has support for certain "columns" (in blkid these
    would be labels), and these columns vary between distributions and
    ``lsblk`` versions. The newer versions support a richer set of columns,
    while older ones were a bit limited.

    These are the default lsblk columns reported which are safe to use for
    Ubuntu 14.04.5 LTS:

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
    """
    default_columns = [
        'NAME', 'KNAME', 'MAJ:MIN', 'FSTYPE', 'MOUNTPOINT', 'LABEL', 'UUID',
        'RO', 'RM', 'MODEL', 'SIZE', 'STATE', 'OWNER', 'GROUP', 'MODE',
        'ALIGNMENT', 'PHY-SEC', 'LOG-SEC', 'ROTA', 'SCHED', 'TYPE', 'DISC-ALN',
        'DISC-GRAN', 'DISC-MAX', 'DISC-ZERO'
    ]
    device = device.rstrip('/')
    columns = columns or default_columns
    # --nodeps -> Avoid adding children/parents to the device, only give information
    #             on the actual device we are querying for
    # -P       -> Produce pairs of COLUMN="value"
    # -o       -> Use the columns specified or default ones provided by this function
    command = ['lsblk', '--nodeps', '-P', '-o']
    command.append(','.join(columns))
    command.append(device)
    out, err, rc = process.call(command)

    if rc != 0:
        return {}

    # parse the COLUMN="value" output to construct the dictionary
    pairs = ' '.join(out).split()
    parsed = {}
    for pair in pairs:
        try:
            column, value = pair.split('=')
        except ValueError:
            continue
        parsed[column] = value.strip().strip().strip('"')
    return parsed


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
