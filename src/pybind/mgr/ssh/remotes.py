# ceph-deploy ftw
import os
import errno
import tempfile
import shutil

def safe_makedirs(path, uid=-1, gid=-1):
    """ create path recursively if it doesn't exist """
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST:
            pass
        else:
            raise
    else:
        os.chown(path, uid, gid)

def write_conf(path, conf):
    if not os.path.exists(path):
        dirpath = os.path.dirname(path)
        if os.path.exists(dirpath):
            with open(path, "w") as f:
                f.write(conf)
            os.chmod(path, 0o644)
        else:
            raise RuntimeError(
                "{0} does not exist".format(dirpath))

def write_keyring(path, key, overwrite=False, uid=-1, gid=-1):
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        safe_makedirs(dirname, uid, gid)
    if not overwrite and os.path.exists(path):
        return
    with open(path, "wb") as f:
        f.write(key.encode('utf-8'))

def create_mon_path(path, uid=-1, gid=-1):
    """create the mon path if it does not exist"""
    if not os.path.exists(path):
        os.makedirs(path)
        os.chown(path, uid, gid);

def write_file(path, content, mode=0o644, directory=None, uid=-1, gid=-1):
    if directory:
        if path.startswith("/"):
            path = path[1:]
        path = os.path.join(directory, path)
    if os.path.exists(path):
        # Delete file in case we are changing its mode
        os.unlink(path)
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT, mode), 'wb') as f:
        f.write(content.encode('utf-8'))
    os.chown(path, uid, gid)

def path_getuid(path):
    return os.stat(path).st_uid

def path_getgid(path):
    return os.stat(path).st_gid

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

if __name__ == '__channelexec__':
    for item in channel:
        channel.send(eval(item))
