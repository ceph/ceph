#!/usr/bin/python

import time
import os
import fcntl
import errno
import signal
import struct

def handler(signum, frame):
    pass

def lock_two(f1, f2):
    lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 0, 10, 0, 0)
    fcntl.fcntl(f1, fcntl.F_SETLKW, lockdata)
    time.sleep(10)

    # don't wait forever
    signal.signal(signal.SIGALRM, handler);
    signal.alarm(10);
    exitcode = 0;
    try:
        fcntl.fcntl(f2, fcntl.F_SETLKW, lockdata)
    except IOError, e:
        if e.errno == errno.EDEADLK:
            exitcode = 1
        elif e.errno == errno.EINTR:
            exitcode = 2
        else:
            exitcode = 3;
    os._exit(exitcode);

def main():
    pid1 = os.fork()
    if pid1 == 0:
        f1 = open("testfile1", 'w')
        f2 = open("testfile2", 'w')
        lock_two(f1, f2)

    pid2 = os.fork()
    if pid2 == 0:
        f1 = open("testfile2", 'w')
        f2 = open("testfile3", 'w')
        lock_two(f1, f2)

    pid3 = os.fork()
    if pid3 == 0:
        f1 = open("testfile3", 'w')
        f2 = open("testfile1", 'w')
        lock_two(f1, f2)

    deadlk_count = 0
    i = 0
    while i < 3:
        pid, status = os.wait();
        exitcode = status >> 8
        if exitcode == 1:
            deadlk_count = deadlk_count + 1;
        elif exitcode != 0:
            raise RuntimeError("unexpect exit code of child")
	i = i + 1

    if deadlk_count != 1:
        raise RuntimeError("unexpect count of EDEADLK")

    print 'ok'

main()
