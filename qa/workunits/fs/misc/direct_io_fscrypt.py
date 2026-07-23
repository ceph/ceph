#!/usr/bin/python3

import mmap
import os
import subprocess
import sys

def main():
    # For this test, we're going to use the underlying fscrypt libraries to
    # transform data. For writes we will use file "file_in" where we write
    # plain text via file and be able to read back encrypted version via object.
    # Then we will use object_in where we will write encrypted data to the
    # object and read the plain text back via file.

    file_in = "file_in"
    object_in = "object_in"
    object_out = "object_out"

    fd = os.open(file_in, os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_DIRECT, 0o644)

    fscrypt_auth = ""

    try:
        fscrypt_auth = os.getxattr(file_in, "ceph.fscrypt.auth")
    except OSError:
        print(f"fscrypt not configured on {file_in}, exiting!")
        sys.exit(0)

    ino = os.fstat(fd).st_ino
    obj_name = "{ino:x}.00000000".format(ino=ino)
    pool_name = os.getxattr(file_in, "ceph.file.layout.pool")

    buf = mmap.mmap(-1, 1)
    buf.write(b'1')
    os.write(fd, buf)

    proc = subprocess.Popen(['rados', '-p', pool_name, 'get', obj_name, object_out])
    proc.wait()

    fd2 = os.open(object_in, os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_DIRECT, 0o644)

    # override value here with same as file_in so we can decrypt
    os.setxattr(object_in, "ceph.fscrypt.auth", fscrypt_auth)

    # write something other than 1 to the file
    buf = mmap.mmap(-1, 1)
    buf.write(b'2')
    os.write(fd2, buf)

    ino2 = os.fstat(fd2).st_ino
    obj_name2 = "{ino2:x}.00000000".format(ino2=ino2)
    proc = subprocess.Popen(['rados', '-p', pool_name, 'put', obj_name2, object_out])
    proc.wait()

    os.lseek(fd2, 0, os.SEEK_SET)
    out = os.read(fd2, 4096)
    if out != b'1':
        raise RuntimeError("data were not directly read from object store")

    os.close(fd)
    print('ok')


main()
