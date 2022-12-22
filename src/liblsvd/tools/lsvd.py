from ctypes import *
import os
from lsvd_types import *

dir = os.getcwd()
dir = '/home/pjd/lsvd-rbd'
lsvd_lib = CDLL("liblsvd.so")
assert lsvd_lib


# translation layer only, no read or write cache
#
class translate:
    def __init__(self, name, n, flush):
        if type(name) != bytes:
            name = bytes(name, 'utf-8')
        self.name = name
        p = c_void_p()
        sz = lsvd_lib.xlate_open(name, n, c_bool(flush), byref(p))
        self.lsvd = p
        self.nbytes = sz

    def close(self):
        lsvd_lib.xlate_close(self.lsvd)

    def write(self, offset, data):
        if type(data) != bytes:
            data = bytes(data, 'utf-8')
        nbytes = len(data)
        assert (nbytes % 512) == 0 and (offset % 512) == 0
        return lsvd_lib.xlate_write(self.lsvd, data, c_ulong(offset), c_uint(nbytes))

    def read(self, offset, nbytes):
        assert (nbytes % 512) == 0 and (offset % 512) == 0
        buf = (c_char * nbytes)()
        val = lsvd_lib.xlate_read(self.lsvd, buf, c_ulong(offset), c_uint(nbytes))
        return buf[0:nbytes]

    def flush(self):
        lsvd_lib.xlate_flush(self.lsvd)

    def mapsize(self):
        return lsvd_lib.xlate_size(self.lsvd)

    def getmap(self, base, limit):
        n_tuples = self.mapsize()
        tuples = (tuple * n_tuples)()
        n = lsvd_lib.xlate_getmap(self.lsvd, c_int(base), c_int(limit),
                                      c_int(n_tuples), byref(tuples))
        return [_ for _ in map(lambda x: [x.base, x.limit, x.obj, x.offset], tuples[0:n])]

    def frontier(self):
        return lsvd_lib.xlate_frontier(self.lsvd)

    def reset(self):
        lsvd_lib.xlate_reset(self.lsvd)

    def batch_seq(self):
        return lsvd_lib.xlate_seq(self.lsvd)

    def checkpoint(self):
        return lsvd_lib.xlate_checkpoint(self.lsvd)

    def fakemap_update(self, base, limit, obj, offset):
        lsvd_lib.fakemap_update(self.lsvd, c_int(base), c_int(limit),
                                    c_int(obj), c_int(offset))

    def fakemap_reset(self):
        lsvd_lib.fakemap_reset(self.lsvd)

        
def img_write(img, offset, data):
    if type(data) != bytes:
        data = bytes(data, 'utf-8')
    nbytes = len(data)
    assert (nbytes % 512) == 0 and (offset % 512) == 0
    val = lsvd_lib.dbg_lsvd_write(img, data, c_ulong(offset), c_uint(nbytes))
    return val

def img_read(img, offset, nbytes):
    assert (nbytes % 512) == 0 and (offset % 512) == 0
    buf = (c_char * nbytes)()
    val = lsvd_lib.dbg_lsvd_read(img, buf, c_ulong(offset), c_uint(nbytes))
    return buf[0:nbytes]

def img_flush(img):
    lsvd_lib.dbg_lsvd_flush(img)

LSVD_SUPER = 1
LSVD_DATA = 2
LSVD_CKPT = 3
LSVD_MAGIC = 0x4456534c


###############

class write_cache:
    def __init__(self, file):
        self._fd = os.open(file, os.O_RDWR | os.O_DIRECT)
        self.fd = os.open(file, os.O_RDONLY)
        data = os.pread(self.fd, 4096, 0)
        self.super = j_super.from_buffer(bytearray(data[0:sizeof(j_super)]))

    def init(self, xlate, blkno):
        p = c_void_p()
        lsvd_lib.wcache_open(xlate.lsvd, c_int(blkno), c_int(self._fd), byref(p))
        self.wcache = p
        
    def shutdown(self):
        os.close(self._fd)
        os.close(self.fd)
        lsvd_lib.wcache_close(self.wcache)

    def read(self, offset, nbytes):
        assert (nbytes % 512) == 0 and (offset % 512) == 0
        buf = (c_char * nbytes)()
        lsvd_lib.wcache_read(self.wcache, buf, c_ulong(offset), c_uint(nbytes))
        return buf[0:nbytes]
        
    def write(self, offset, data):
        if type(data) != bytes:
            data = bytes(data, 'utf-8')
        nbytes = len(data)
        assert (nbytes % 512) == 0 and (offset % 512) == 0
        val = lsvd_lib.wcache_write(self.wcache, data, c_ulong(offset), c_uint(nbytes))

    def getmap(self, base, limit):
        n_tuples = 128
        tuples = (tuple * n_tuples)()
        n = lsvd_lib.wcache_getmap(self.wcache, c_int(base), c_int(limit), c_int(n_tuples), byref(tuples))
        return list(map(lambda x: [x.base, x.limit, x.plba], tuples[0:n]))

    def getsuper(self):
        s = j_write_super()
        lsvd_lib.wcache_get_super(self.wcache, byref(s))
        return s

    def oldest(self, blk):
        exts = (j_extent * 32)()
        n = c_int()
        newer = lsvd_lib.wcache_oldest(self.wcache, c_int(blk), byref(exts), 32, byref(n))
        e = [(_.lba, _.len) for _ in exts[0:n.value]]
        return [newer, e]

    def checkpoint(self):
        lsvd_lib.wcache_write_ckpt(self.wcache)

def wcache_img_write(img, offset, data):
    if type(data) != bytes:
        data = bytes(data, 'utf-8')
    nbytes = len(data)
    assert (nbytes % 512) == 0 and (offset % 512) == 0
    val = lsvd_lib.wcache_img_write(img, data, c_ulong(offset), c_uint(nbytes))


class read_cache:
    def __init__(self, xlate, blkno, _fd):
        p = c_void_p()
        lsvd_lib.rcache_init(xlate.lsvd, c_uint(blkno), c_int(_fd), byref(p))
        self.rcache = p
        self.rsuper = j_read_super()
        lsvd_lib.rcache_getsuper(self.rcache, byref(self.rsuper))

    def close(self):
        lsvd_lib.rcache_shutdown(self.rcache)

    def read(self, offset, nbytes):
        assert (nbytes % 512) == 0 and (offset % 512) == 0
        buf = (c_char * nbytes)()
        lsvd_lib.rcache_read(self.rcache, buf, c_ulong(offset), c_ulong(nbytes))
        return buf[0:nbytes]

    def read2(self, offset, nbytes):
        assert (nbytes % 512) == 0 and (offset % 512) == 0
        buf = (c_char * nbytes)()
        lsvd_lib.rcache_read2(self.rcache, buf, c_ulong(offset), c_ulong(nbytes))
        return buf[0:nbytes]
    
    def add(self, obj, blk, data):
        if type(data) != bytes:
            data = bytes(data, 'utf-8')
        nbytes = len(data)
        assert nbytes == 65536
        lsvd_lib.rcache_add(self.rcache, c_int(obj), c_int(blk), data, c_int(nbytes))
        
    def getmap(self):
        n = self.rsuper.units
        k = (obj_offset * n)()
        v = (c_int * n)()
        m = lsvd_lib.rcache_getmap(self.rcache, byref(k), byref(v), n)
        keys = [[_.obj, _.offset] for _ in k[0:m]]
        vals = v[:]
        return list(zip(keys, vals))

    def flatmap(self):
        n = self.rsuper.units
        vals = (obj_offset * n)()
        n = lsvd_lib.rcache_get_flat(self.rcache, byref(vals), c_int(n))
        return [[_.obj,_.offset] for _ in vals[0:n]]

    def evict(self, n):
        lsvd_lib.rcache_evict(self.rcache, c_int(n))

def logbuf():
    buf = (c_char * 4096)()
    nbytes = lsvd_lib.get_logbuf(buf, c_int(4096))
    return (buf[0:nbytes]).decode('utf-8')

# RBD functions

def rbd_open(name):
    img = c_void_p()
    rv = lsvd_lib.rbd_open(None, c_char_p(bytes(name, 'utf-8')), byref(img), None)
    assert rv >= 0
    return img

def rbd_close(img):
    lsvd_lib.rbd_close(img)

def rbd_read(img, off, nbytes):
    buf = (c_char * nbytes)()
    lsvd_lib.rbd_read(img, c_ulong(off), c_ulong(nbytes), buf)
    return buf[0:nbytes]

def rbd_write(img, off, data):
    if type(data) != bytes:
        data = bytes(data, 'utf-8')
    nbytes = len(data)
    lsvd_lib.rbd_write(img, c_ulong(off), c_ulong(nbytes), data)
    
def rbd_flush(img):
    lsvd_lib.rbd_flush(img)



