#!/usr/bin/python3

from contextlib import contextmanager
import errno
import fcntl
import signal
import struct

@contextmanager
def timeout(seconds):
    def timeout_handler(signum, frame):
        raise InterruptedError

    orig_handler = signal.signal(signal.SIGALRM, timeout_handler)
    try:
        signal.alarm(seconds)
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, orig_handler)


"""
introduced by Linux 3.15
"""
fcntl.F_OFD_GETLK = 36
fcntl.F_OFD_SETLK = 37
fcntl.F_OFD_SETLKW = 38


def main():
    f1 = open("testfile", 'w')
    f2 = open("testfile", 'w')

    fcntl.flock(f1, fcntl.LOCK_SH | fcntl.LOCK_NB)

    """
    is flock interruptible?
    """
    with timeout(5):
        try:
            fcntl.flock(f2, fcntl.LOCK_EX)
        except InterruptedError:
            pass
        else:
            raise RuntimeError("expect flock to block")

    fcntl.flock(f1, fcntl.LOCK_UN)

    lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 0, 10, 0, 0)
    try:
        fcntl.fcntl(f1, fcntl.F_OFD_SETLK, lockdata)
    except IOError as e:
        if e.errno != errno.EINVAL:
            raise
        else:
            print('kernel does not support fcntl.F_OFD_SETLK')
            return

    lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 10, 10, 0, 0)
    fcntl.fcntl(f2, fcntl.F_OFD_SETLK, lockdata)

    """
    is posix lock interruptible?
    """
    with timeout(5):
        try:
            lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 0, 0, 0, 0)
            fcntl.fcntl(f2, fcntl.F_OFD_SETLKW, lockdata)
        except InterruptedError:
            pass
        else:
            raise RuntimeError("expect posix lock to block")

    """
    file handler 2 should still hold lock on 10~10
    """
    try:
        lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 10, 10, 0, 0)
        fcntl.fcntl(f1, fcntl.F_OFD_SETLK, lockdata)
    except IOError as e:
        if e.errno == errno.EAGAIN:
            pass
    else:
        raise RuntimeError("expect file handler 2 to hold lock on 10~10")

    lockdata = struct.pack('hhllhh', fcntl.F_UNLCK, 0, 0, 0, 0, 0)
    fcntl.fcntl(f1, fcntl.F_OFD_SETLK, lockdata)
    fcntl.fcntl(f2, fcntl.F_OFD_SETLK, lockdata)

    print('ok')


main()
