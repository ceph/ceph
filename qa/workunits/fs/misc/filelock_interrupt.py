#!/usr/bin/python

import time
import fcntl
import errno
import signal
import struct

"""
introduced by Linux 3.15
"""
fcntl.F_OFD_GETLK=36
fcntl.F_OFD_SETLK=37
fcntl.F_OFD_SETLKW=38

def handler(signum, frame):
    pass

def main():
    f1 = open("testfile", 'w')
    f2 = open("testfile", 'w')

    fcntl.flock(f1, fcntl.LOCK_SH | fcntl.LOCK_NB)

    """
    is flock interruptable?
    """
    signal.signal(signal.SIGALRM, handler);
    signal.alarm(5);
    try:
        fcntl.flock(f2, fcntl.LOCK_EX)
    except IOError, e:
        if e.errno != errno.EINTR:
            raise
    else:
        raise RuntimeError("expect flock to block")

    fcntl.flock(f1, fcntl.LOCK_UN)

    lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 0, 10, 0, 0)
    try:
        fcntl.fcntl(f1, fcntl.F_OFD_SETLK, lockdata)
    except IOError, e:
        if e.errno != errno.EINVAL:
            raise
        else:
            print 'kernel does not support fcntl.F_OFD_SETLK'
            return

    lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 10, 10, 0, 0)
    fcntl.fcntl(f2, fcntl.F_OFD_SETLK, lockdata)

    """
    is poxis lock interruptable?
    """
    signal.signal(signal.SIGALRM, handler);
    signal.alarm(5);
    try:
        lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 0, 0, 0, 0)
        fcntl.fcntl(f2, fcntl.F_OFD_SETLKW, lockdata)
    except IOError, e:
        if e.errno != errno.EINTR:
            raise
    else:
        raise RuntimeError("expect posix lock to block")

    """
    file handler 2 should still hold lock on 10~10
    """
    try:
        lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 10, 10, 0, 0)
        fcntl.fcntl(f1, fcntl.F_OFD_SETLK, lockdata)
    except IOError, e:
        if e.errno == errno.EAGAIN:
            pass
    else:
        raise RuntimeError("expect file handler 2 to hold lock on 10~10")

    lockdata = struct.pack('hhllhh', fcntl.F_UNLCK, 0, 0, 0, 0, 0)
    fcntl.fcntl(f1, fcntl.F_OFD_SETLK, lockdata)
    fcntl.fcntl(f2, fcntl.F_OFD_SETLK, lockdata)

    print 'ok'

main()
