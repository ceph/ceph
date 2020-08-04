# ceph-deploy ftw
import os
import errno
import tempfile
import shutil

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
    for e in PYTHONS:
        for b in PATH:
            p = os.path.join(b, e)
            if os.path.exists(p):
                return p
    return None


if __name__ == '__channelexec__':
    for item in channel:  # type: ignore
        channel.send(eval(item))  # type: ignore
