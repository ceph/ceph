import errno
import os
import pwd
import platform
import uuid
from ceph_volume import process
from . import as_string


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


def is_mounted(source, destination=None):
    """
    Check if the given device is mounted, optionally validating destination.
    This relies on absolute path devices, it will ignore non-absolute
    entries like::

        tmpfs /run tmpfs rw,seclabel,nosuid,nodev,mode=755 0 0

    But will parse paths that are absolute like::

        /dev/sdc2 /boot xfs rw,attr2,inode64,noquota 0 0

    When destination is passed in, it will check that the entry where the
    source appears is mounted to where destination defines. This is useful so
    that an error message can report that a source is not mounted at an
    expected destination.
    """
    dev = os.path.realpath(source)
    with open(PROCDIR + '/mounts', 'rb') as proc_mounts:
        for line in proc_mounts:
            fields = line.split()
            if len(fields) < 3:
                continue
            mounted_device = fields[0]
            mounted_path = fields[1]
            if os.path.isabs(mounted_device) and os.path.exists(mounted_device):
                mounted_device = os.path.realpath(mounted_device)
                if as_string(mounted_device) == dev:
                    if destination:
                        destination = os.path.realpath(destination)
                        return destination == as_string(os.path.realpath(mounted_path))
                    else:
                        return True
    return False
