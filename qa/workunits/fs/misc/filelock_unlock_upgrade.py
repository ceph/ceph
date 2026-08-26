#!/usr/bin/python3
#
# An owner that has a blocked lock request (e.g. LOCK_SH to LOCK_EX
# upgrade) may also be holding a second lock over the same range.  A
# real unlock from that owner has to drop both.  The MDS used to only
# cancel the pending request, stranding the held lock in held_locks
# with nobody left to release it, which blocked every other owner
# forever.
#

from contextlib import contextmanager
import ctypes
import fcntl
import signal
import threading
import time

FILENAME = "filelock_unlock_upgrade"

libc = ctypes.CDLL(None, use_errno=True)


def raw_flock(fd, operation):
    """flock(2) without CPython's PEP 475 EINTR retry loop.

    The MDS answers the cancelled upgrade with EINTR; fcntl.flock() would
    silently reissue it and paper over what we are trying to observe.
    """
    if libc.flock(ctypes.c_int(fd), ctypes.c_int(operation)) == 0:
        return 0
    return ctypes.get_errno()


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


def main():
    # two open file descriptions -> two distinct flock owners
    f1 = open(FILENAME, 'w')
    f2 = open(FILENAME, 'w')

    fcntl.flock(f1, fcntl.LOCK_SH | fcntl.LOCK_NB)
    fcntl.flock(f2, fcntl.LOCK_SH | fcntl.LOCK_NB)

    # A thread shares f1's file description, so it upgrades as the
    # same owner.  f2's shared lock makes it block, putting that owner
    # on the MDS waiting list while it still holds a shared lock.
    def upgrade():
        raw_flock(f1.fileno(), fcntl.LOCK_EX)

    t = threading.Thread(target=upgrade)
    t.start()
    time.sleep(2)

    # An unlock from an owner that is also waiting.  This triggered
    # the bug.  Manipulating f1 makes the flock() interrupts the
    # LOCK_EX in the thread (which then fails with EINTR).
    fcntl.flock(f1, fcntl.LOCK_UN)

    t.join(60)
    if t.is_alive():
        raise RuntimeError("blocked upgrade never completed")

    fcntl.flock(f2, fcntl.LOCK_UN)
    f1.close()
    f2.close()

    # No owner holds anything now, so a fresh owner must be granted
    # immediately.
    f3 = open(FILENAME, 'w')
    with timeout(60):
        try:
            fcntl.flock(f3, fcntl.LOCK_EX)
        except InterruptedError:
            raise RuntimeError("stale lock left behind by unlock-while-waiting")
    fcntl.flock(f3, fcntl.LOCK_UN)
    f3.close()

    print('ok')


main()
