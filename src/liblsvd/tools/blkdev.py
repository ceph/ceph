#!/usr/bin/python3

import fcntl
import struct

# adapted from https://gist.github.com/shimarin/34f05caaf75d02966a30

S_IFMT  = 0o00170000
S_IFREG =  0o0100000
S_IFBLK =  0o0060000

def S_ISBLK(m):
    return (m & S_IFMT) == S_IFBLK
def S_ISREG(m):
    return (m & S_IFMT) == S_IFREG

def ioctl_read_uint64(fd, req):
    buf = bytearray(b'\0'*8)
    fcntl.ioctl(fd, req, buf)
    return struct.unpack('L',buf)[0]

def dev_get_size(fd):
    BLKGETSIZE64 = 0x80081272
    return ioctl_read_uint64(fd, BLKGETSIZE64)
    
