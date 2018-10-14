import errno
import logging
import os
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
    mlogger.warning('Ensure $PATH environment variable contains '
                    'common executable locations')
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
        raise RuntimeError('"ceph" user is not available in the system')
    return user[2], user[3]


def get_file_contents(path, default=''):
    contents = default
    if not os.path.exists(path):
        return contents
    try:
        with open(path, 'r') as open_file:
            contents = open_file.read().strip()
    except Exception:
        logger.exception('Failed to read contents from: %s' % path)

    return contents


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
        process.run(['chown', '-h', 'ceph:ceph', path])
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


def get_mounts(devices=False, paths=False):
    """
    Create a mapping of all available system mounts so that other helpers can
    detect nicely what path or device is mounted

    It ignores (most of) non existing devices, but since some setups might need
    some extra device information, it will make an exception for:

    - tmpfs
    - devtmpfs

    If ``devices`` is set to ``True`` the mapping will be a device-to-path(s),
    if ``paths`` is set to ``True`` the mapping will be a path-to-device(s)
    for zfs mount the zpool part will also be considered a device.

    """
    devices_mounted = {}
    paths_mounted = {}
    do_not_skip = ['tmpfs', 'devtmpfs']
    type_not_skip = ['zfs']
    default_to_devices = not devices and not paths

    with open(PROCDIR + '/mounts', 'rb') as mounts:
        proc_mounts = mounts.readlines()

    for line in proc_mounts:
        fields = [as_string(f) for f in line.split()]
        if len(fields) < 3:
            continue
        device = str(fields[0])
        path = os.path.realpath(str(fields[1]))
        devtype = str(fields[2])

        # only care about actual existing devices
        if not os.path.exists(device) or not device.startswith('/'):
            if not (device in do_not_skip or devtype in type_not_skip):
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
# vim: expandtab smarttab shiftwidth=4 softtabstop=4
