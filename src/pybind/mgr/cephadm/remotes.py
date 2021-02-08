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


if __name__ == '__channelexec__':
    for item in channel:  # type: ignore # noqa: F821
        channel.send(eval(item))  # type: ignore # noqa: F821
