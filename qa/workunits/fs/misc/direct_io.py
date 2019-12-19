#!/usr/bin/python3

import json
import mmap
import os
import subprocess


def get_data_pool():
    cmd = ['ceph', 'fs', 'ls', '--format=json-pretty']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out = proc.communicate()[0]
    return json.loads(out)[0]['data_pools'][0]


def main():
    fd = os.open("testfile", os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_DIRECT, 0o644)

    ino = os.fstat(fd).st_ino
    obj_name = "{ino:x}.00000000".format(ino=ino)
    pool_name = get_data_pool()

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
