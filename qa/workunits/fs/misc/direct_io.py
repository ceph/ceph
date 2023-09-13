#!/usr/bin/python3

import mmap
import os
import subprocess

def main():
    path = "testfile"
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_DIRECT, 0o644)

    ino = os.fstat(fd).st_ino
    obj_name = "{ino:x}.00000000".format(ino=ino)
    pool_name = os.getxattr(path, "ceph.file.layout.pool")

    buf = mmap.mmap(-1, 1)
    buf.write(b'1')
    os.write(fd, buf)

    proc = subprocess.Popen(['rados', '-p', pool_name, 'get', obj_name, 'tmpfile'])
    proc.wait()

    with open('tmpfile', 'rb') as tmpf:
        out = tmpf.read(1)
        if out != b'1':
            raise RuntimeError("data were not written to object store directly")

    with open('tmpfile', 'wb') as tmpf:
        tmpf.write(b'2')

    proc = subprocess.Popen(['rados', '-p', pool_name, 'put', obj_name, 'tmpfile'])
    proc.wait()

    os.lseek(fd, 0, os.SEEK_SET)
    out = os.read(fd, 1)
    if out != b'2':
        raise RuntimeError("data were not directly read from object store")

    os.close(fd)
    print('ok')


main()
