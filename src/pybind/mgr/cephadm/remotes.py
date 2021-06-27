# ceph-deploy ftw
import os
try:
    from typing import Optional
except ImportError:
    pass

PYTHONS = ['python3', 'python2', 'python']
PATH = [
    '/usr/bin',
    '/usr/local/bin',
    '/bin',
    '/usr/sbin',
    '/usr/local/sbin',
    '/sbin',
]


def choose_python():
    # type: () -> Optional[str]
    for e in PYTHONS:
        for b in PATH:
            p = os.path.join(b, e)
            if os.path.exists(p):
                return p
    return None


def write_file(path: str, content: bytes, mode: int, uid: int, gid: int,
               mkdir_p: bool = True) -> Optional[str]:
    try:
        if mkdir_p:
            dirname = os.path.dirname(path)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
        tmp_path = path + '.new'
        with open(tmp_path, 'wb') as f:
            os.fchown(f.fileno(), uid, gid)
            os.fchmod(f.fileno(), mode)
            f.write(content)
            os.fsync(f.fileno())
        os.rename(tmp_path, path)
    except Exception as e:
        return str(e)
    return None


if __name__ == '__channelexec__':
    for item in channel:  # type: ignore # noqa: F821
        channel.send(eval(item))  # type: ignore # noqa: F821
