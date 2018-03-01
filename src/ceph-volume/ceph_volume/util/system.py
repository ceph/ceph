import errno
import logging
import os
import pwd
import platform
import tempfile
import uuid
from ceph_volume import process, terminal
from . import as_string

logger = logging.getLogger(__name__)
mlogger = terminal.MultiLogger(__name__)

# TODO: get these out of here and into a common area for others to consume
if platform.system() == 'FreeBSD':
    FREEBSD = True
    DEFAULT_FS_TYPE = 'zfs'
    PROCDIR = '/compat/linux/proc'
    # FreeBSD does not have blockdevices any more
    BLOCKDIR = '/dev'
    ROOTGROUP = 'wheel'
else:
    FREEBSD = False
    DEFAULT_FS_TYPE = 'xfs'
    PROCDIR = '/proc'
    BLOCKDIR = '/sys/block'
    ROOTGROUP = 'root'


def generate_uuid():
    return str(uuid.uuid4())


def which(executable):
    """find the location of an executable"""
    locations = (
        '/usr/local/bin',
        '/bin',
        '/usr/bin',
        '/usr/local/sbin',
        '/usr/sbin',
        '/sbin',
    )

    for location in locations:
        executable_path = os.path.join(location, executable)
        if os.path.exists(executable_path) and os.path.isfile(executable_path):
            return executable_path
    mlogger.warning('Absolute path not found for executable: %s', executable)
    mlogger.warning('Ensure $PATH environment variable contains common executable locations')
    # fallback to just returning the argument as-is, to prevent a hard fail,
    # and hoping that the system might have the executable somewhere custom
    return executable


def get_ceph_user_ids():
    """
    Return the id and gid of the ceph user
    """
    try:
        user = pwd.getpwnam('ceph')
    except KeyError:
        # is this even possible?
        raise RuntimeError('"ceph" user is not available in the current system')
    return user[2], user[3]


def mkdir_p(path, chown=True):
    """
    A `mkdir -p` that defaults to chown the path to the ceph user
    """
    try:
        os.mkdir(path)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise
    if chown:
        uid, gid = get_ceph_user_ids()
        os.chown(path, uid, gid)


def chown(path, recursive=True):
    """
    ``chown`` a path to the ceph user (uid and guid fetched at runtime)
    """
    uid, gid = get_ceph_user_ids()
    if os.path.islink(path):
        path = os.path.realpath(path)
    if recursive:
        process.run(['chown', '-R', 'ceph:ceph', path])
    else:
        os.chown(path, uid, gid)


def is_binary(path):
    """
    Detect if a file path is a binary or not. Will falsely report as binary
    when utf-16 encoded. In the ceph universe there is no such risk (yet)
    """
    with open(path, 'rb') as fp:
        contents = fp.read(8192)
    if b'\x00' in contents:  # a null byte may signal binary
        return True
    return False


class tmp_mount(object):
    """
    Temporarily mount a device on a temporary directory,
    and unmount it upon exit

    When ``encrypted`` is set to ``True``, the exit method will call out to
    close the device so that it doesn't remain open after mounting. It is
    assumed that it will be open because otherwise it wouldn't be possible to
    mount in the first place
    """

    def __init__(self, device, encrypted=False):
        self.device = device
        self.path = None
        self.encrypted = encrypted

    def __enter__(self):
        self.path = tempfile.mkdtemp()
        process.run([
            'mount',
            '-v',
            self.device,
            self.path
        ])
        return self.path

    def __exit__(self, exc_type, exc_val, exc_tb):
        process.run([
            'umount',
            '-v',
            self.path
        ])
        if self.encrypted:
            # avoid a circular import from the encryption module
            from ceph_volume.util import encryption
            encryption.dmcrypt_close(self.device)


def unmount(path):
    """
    Removes mounts at the given path
    """
    process.run([
        'umount',
        '-v',
        path,
    ])


def path_is_mounted(path, destination=None):
    """
    Check if the given path is mounted
    """
    mounts = get_mounts(paths=True)
    realpath = os.path.realpath(path)
    mounted_locations = mounts.get(realpath, [])

    if destination:
        return destination in mounted_locations
    return mounted_locations != []


def device_is_mounted(dev, destination=None):
    """
    Check if the given device is mounted, optionally validating that a
    destination exists
    """
    plain_mounts = get_mounts(devices=True)
    realpath_mounts = get_mounts(devices=True, realpath=True)
    realpath_dev = os.path.realpath(dev) if dev.startswith('/') else dev
    destination = os.path.realpath(destination) if destination else None
    # plain mounts
    plain_dev_mounts = plain_mounts.get(dev, [])
    realpath_dev_mounts = plain_mounts.get(realpath_dev, [])
    # realpath mounts
    plain_dev_real_mounts = realpath_mounts.get(dev, [])
    realpath_dev_real_mounts = realpath_mounts.get(realpath_dev, [])

    mount_locations = [
        plain_dev_mounts,
        realpath_dev_mounts,
        plain_dev_real_mounts,
        realpath_dev_real_mounts
    ]

    for mounts in mount_locations:
        if mounts: # we have a matching mount
            if destination:
                if destination in mounts:
                    logger.info(
                        '%s detected as mounted, exists at destination: %s', dev, destination
                    )
                    return True
            else:
                logger.info('%s was found as mounted')
                return True
    logger.info('%s was not found as mounted')
    return False


def get_mounts(devices=False, paths=False, realpath=False):
    """
    Create a mapping of all available system mounts so that other helpers can
    detect nicely what path or device is mounted

    It ignores (most of) non existing devices, but since some setups might need
    some extra device information, it will make an exception for:

    - tmpfs
    - devtmpfs

    If ``devices`` is set to ``True`` the mapping will be a device-to-path(s),
    if ``paths`` is set to ``True`` then the mapping will be
    a path-to-device(s)

    :param realpath: Resolve devices to use their realpaths. This is useful for
    paths like LVM where more than one path can point to the same device
    """
    devices_mounted = {}
    paths_mounted = {}
    do_not_skip = ['tmpfs', 'devtmpfs']
    default_to_devices = devices is False and paths is False

    with open(PROCDIR + '/mounts', 'rb') as mounts:
        proc_mounts = mounts.readlines()

    for line in proc_mounts:
        fields = [as_string(f) for f in line.split()]
        if len(fields) < 3:
            continue
        if realpath:
            device = os.path.realpath(fields[0]) if fields[0].startswith('/') else fields[0]
        else:
            device = fields[0]
        path = os.path.realpath(fields[1])
        # only care about actual existing devices
        if not os.path.exists(device) or not device.startswith('/'):
            if device not in do_not_skip:
                continue
        if device in devices_mounted.keys():
            devices_mounted[device].append(path)
        else:
            devices_mounted[device] = [path]
        if path in paths_mounted.keys():
            paths_mounted[path].append(device)
        else:
            paths_mounted[path] = [device]

    # Default to returning information for devices if
    if devices is True or default_to_devices:
        return devices_mounted
    else:
        return paths_mounted
