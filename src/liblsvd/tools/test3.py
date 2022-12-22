#!/usr/bin/python3

import lsvd_types as lsvd
import ctypes
import mkdisk
import mkcache

def c_super(fd):
    b = bytearray(os.pread(fd, 4096, 0))   # always first block
    return lsvd.j_super.from_buffer(b[0:lsvd.sizeof_j_super])

def c_w_super(fd, blk):
    b = bytearray(os.pread(fd, 4096, 4096*blk))
    return lsvd.j_write_super.from_buffer(b[0:lsvd.sizeof_j_write_super])

def c_r_super(fd, blk):
    b = bytearray(os.pread(fd, 4096, 4096*blk))
    return lsvd.j_read_super.from_buffer(b[0:lsvd.sizeof_j_read_super])

def c_hdr(fd, blk):
    b = bytearray(os.pread(fd, 4096, 4096*blk))
    o2 = lsvd.sizeof_j_hdr
    h = lsvd.j_hdr.from_buffer(b[0:o2])
    e = []
    if h.magic == lsvd.LSVD_MAGIC:
        n_exts = h.extent_len // lsvd.sizeof_j_extent
        e = (lsvd.j_extent*n_exts).from_buffer(b[o2:o2+h.extent_len])
    return [h, e]

