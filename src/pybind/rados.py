"""librados Python ctypes wrapper
Copyright 2011, Hannu Valtonen <hannu.valtonen@ormod.com>
"""
from ctypes import CDLL, c_char_p, c_size_t, c_void_p,\
    create_string_buffer, byref, Structure, c_uint64, pointer
import ctypes
import errno
import time

class Error(Exception):
    def __init__(self, code):
        self.code = code
    def __repr__(self):
        return ("rados.Error(code=%d)" % self.code)

class PermissionError(Exception):
    pass

class ObjectNotFound(Exception):
    pass

class ObjectExists(Exception):
    pass

class IOError(Exception):
    pass

class NoSpace(Exception):
    pass

class IncompleteWriteError(Exception):
    pass

class PoolStateError(Exception):
    pass

class ObjectStateError(Exception):
    pass

def make_ex(ret, msg):
    ret = abs(ret)
    if (ret == errno.EPERM):
        return PermissionError(msg)
    elif (ret == errno.ENOENT):
        return ObjectNotFound(msg)
    elif (ret == errno.EIO):
        return IOError(msg)
    elif (ret == errno.ENOSPC):
        return NoSpace(msg)
    elif (ret == errno.EEXIST):
        return ObjectExists(msg)
    else:
        return Error(msg + (": error code %d" % ret))

class rados_pool_stat_t(Structure):
    _fields_ = [("num_bytes", c_uint64),
                ("num_kb", c_uint64),
                ("num_objects", c_uint64),
                ("num_object_clones", c_uint64),
                ("num_object_copies", c_uint64),
                ("num_objects_missing_on_primary", c_uint64),
                ("num_objects_unfound", c_uint64),
                ("num_objects_degraded", c_uint64),
                ("num_rd", c_uint64),
                ("num_rd_kb", c_uint64),
                ("num_wr", c_uint64),
                ("num_wr_kb", c_uint64)]

class Rados(object):
    """librados python wrapper"""
    def __init__(self):
        self.librados = CDLL('librados.so')
        ret = self.librados.rados_initialize(None)
        if ret != 0:
            raise Error("rados_initialize failed with error code: %d" % ret)
        self.initialized = True

    def __del__(self):
        if (self.__dict__.has_key("initialized") and self.initialized == True):
            self.librados.rados_deinitialize()

    def create_pool(self, pool_name):
        ret = self.librados.rados_create_pool(c_char_p(pool_name))
        if ret < 0:
            raise make_ex(ret, "error creating pool '%s'" % pool_name)

    def open_pool(self, pool_name):
        pool = c_void_p()
        ret = self.librados.rados_open_pool(c_char_p(pool_name), byref(pool))
        if ret < 0:
            raise make_ex(ret, "error opening pool '%s'" % pool_name)
        return Pool(pool_name, self.librados, pool)

class ObjectIterator(object):
    """rados.Pool Object iterator"""
    def __init__(self, pool):
        self.pool = pool
        self.ctx = c_void_p()
        ret = self.pool.librados.\
            rados_list_objects_open(self.pool.pool_id, byref(self.ctx))
        if ret < 0:
            raise make_ex(ret, "error iterating over the objects in pool '%s'" \
                % self.pool.pool_name)

    def __iter__(self):
        return self

    def next(self):
        key = c_char_p()
        ret = self.pool.librados.rados_list_objects_next(self.ctx, byref(key))
        if ret < 0:
            raise StopIteration()
        return Object(self.pool, key)

    def __del__(self):
        self.pool.librados.rados_list_objects_close(self.ctx)

class SnapIterator(object):
    """Snapshot iterator"""
    def __init__(self, pool):
        self.pool = pool
        # We don't know how big a buffer we need until we've called the
        # function. So use the exponential doubling strategy.
        num_snaps = 10
        while True:
            self.snaps = (ctypes.c_uint64 * num_snaps)()
            ret = self.pool.librados.rados_snap_list(self.pool.pool_id,
                                self.snaps, num_snaps)
            if (ret >= 0):
                self.max_snap = ret
                break
            elif (ret != -errno.ERANGE):
                raise make_ex(ret, "error calling rados_snap_list for \
pool '%s'" % self.pool.pool_name)
            num_snaps = num_snaps * 2;
        self.cur_snap = 0

    def __iter__(self):
        return self

    def next(self):
        if (self.cur_snap >= self.max_snap):
            raise StopIteration
        snap_id = self.snaps[self.cur_snap]
        name_len = 10
        while True:
            name = create_string_buffer(name_len)
            ret = self.pool.librados.rados_snap_get_name(self.pool.pool_id,\
                                snap_id, byref(name), name_len)
            if (ret == 0):
                name_len = ret
                break
            elif (ret != -errno.ERANGE):
                raise make_ex(ret, "rados_snap_get_name error")
            name_len = name_len * 2
        snap = Snap(self.pool, name.value, snap_id)
        self.cur_snap = self.cur_snap + 1
        return snap

class Snap(object):
    """Snapshot object"""
    def __init__(self, pool, name, snap_id):
        self.pool = pool
        self.name = name
        self.snap_id = snap_id

    def __str__(self):
        return "rados.Snap(pool=%s,name=%s,snap_id=%d)" \
            % (str(self.pool), self.name, self.snap_id)

class Pool(object):
    """rados.Pool object"""
    def __init__(self, name, librados, pool_id):
        self.name = name
        self.librados = librados
        self.pool_id = pool_id
        self.state = "open"

    def __del__(self):
        if (self.state == "open"):
            self.close()

    def require_pool_open(self):
        if self.state != "open":
            raise PoolStateError("The pool is %s" % self.state)

    def delete(self):
        self.require_pool_open()
        ret = self.librados.rados_delete_pool(self.pool_id)
        if ret < 0:
            raise make_ex(ret, "error deleting pool '%s'" % pool_name)
        self.state = "deleted"

    def close(self):
        self.require_pool_open()
        ret = self.librados.rados_close_pool(self.pool_id)
        if ret < 0:
            raise make_ex(ret, "error closing pool '%s'" % self.pool_id)
        self.state = "closed"

    def get_object(self, key):
        self.require_pool_open()
        return Object(self, key)

    def write(self, key, string_to_write, offset = 0):
        self.require_pool_open()
        length = len(string_to_write)
        ret = self.librados.rados_write(self.pool_id, c_char_p(key),
                    c_size_t(offset), c_char_p(string_to_write),
                    c_size_t(length))
        if ret == length:
            return ret
        elif ret < 0:
            raise make_ex(ret, "Pool.write(%s): failed to write %s" % \
                (self.name, key))
        elif ret < length:
            raise IncompleteWriteError("Wrote only %ld/%ld bytes" % (ret, length))
        else:
            raise make_ex("Pool.write(%s): logic error: rados_write \
returned %d, but %d was the maximum number of bytes it could have \
written." % (self.name, ret, length))

    def read(self, key, offset = 0, length = 8192):
        self.require_pool_open()
        ret_buf = create_string_buffer(length)
        ret = self.librados.rados_read(self.pool_id, c_char_p(key), c_size_t(offset),
                                        ret_buf, c_size_t(length))
        if ret < 0:
            raise make_ex("Pool.read(%s): failed to read %s" % (self.name, key))
        return ret_buf.value

    def get_stats(self):
        self.require_pool_open()
        stats = rados_pool_stat_t()
        ret = self.librados.rados_stat_pool(self.pool_id, byref(stats))
        if ret < 0:
            raise make_ex(ret, "Pool.get_stats(%s): get_stats failed" % self.name)
        return {'num_bytes': stats.num_bytes,
                'num_kb': stats.num_kb,
                'num_objects': stats.num_objects,
                'num_object_clones': stats.num_object_clones,
                'num_object_copies': stats.num_object_copies,
                "num_objects_missing_on_primary": stats.num_objects_missing_on_primary,
                "num_objects_unfound": stats.num_objects_unfound,
                "num_objects_degraded": stats.num_objects_degraded,
                "num_rd": stats.num_rd,
                "num_rd_kb": stats.num_rd_kb,
                "num_wr": stats.num_wr,
                "num_wr_kb": stats.num_wr_kb }

    def remove_object(self, key):
        self.require_pool_open()
        ret = self.librados.rados_remove(self.pool_id, c_char_p(key))
        if ret < 0:
            raise make_ex(ret, "Failed to remove '%s'" % key)
        return True

    def stat(self, key):
        self.require_pool_open()
        """Stat object, returns, size/timestamp"""
        psize = c_uint64()
        pmtime = c_uint64()

        ret = self.librados.rados_stat(self.pool_id, c_char_p(key), pointer(psize),
                                        pointer(pmtime))
        if ret < 0:
            raise make_ex(ret, "Failed to stat %r" % key)
        return psize.value, time.localtime(pmtime.value)

    def get_xattr(self, key, xattr_name):
        self.require_pool_open()
        ret_length = 4096
        ret_buf = create_string_buffer(ret_length)
        ret = self.librados.rados_getxattr(self.pool_id, c_char_p(key),
                    c_char_p(xattr_name), ret_buf, c_size_t(ret_length))
        if ret < 0:
            raise make_ex(ret, "Failed to get xattr %r" % xattr_name)
        return ret_buf.value

    def set_xattr(self, key, xattr_name, xattr_value):
        self.require_pool_open()
        ret = self.librados.rados_setxattr(self.pool_id, c_char_p(key),
                    c_char_p(xattr_name), c_char_p(xattr_value),
                    c_size_t(len(xattr_value)))
        if ret < 0:
            raise make_ex(ret, "Failed to set xattr %r" % xattr_name)
        return True

    def rm_xattr(self, key, xattr_name):
        self.require_pool_open()
        ret = self.librados.rados_rmxattr(self.pool_id, c_char_p(key), c_char_p(xattr_name))
        if ret < 0:
            raise make_ex(ret, "Failed to delete key %r xattr %r" %
                (key, xattr_name))
        return True

    def list_objects(self):
        return ObjectIterator(self)

    def list_snaps(self):
        return SnapIterator(self)

    def create_snap(self, snap_name):
        ret = self.librados.rados_snap_create(self.pool_id,
                            c_char_p(snap_name))
        if (ret != 0):
            raise make_ex(ret, "Failed to create snap %s" % snap_name)

    def remove_snap(self, snap_name):
        ret = self.librados.rados_snap_remove(self.pool_id,
                            c_char_p(snap_name))
        if (ret != 0):
            raise make_ex(ret, "Failed to remove snap %s" % snap_name)

    def lookup_snap(self, snap_name):
        snap_id = c_uint64()
        ret = self.librados.rados_snap_lookup(self.pool_id,\
                            c_char_p(snap_name), byref(snap_id))
        if (ret != 0):
            raise make_ex(ret, "Failed to lookup snap %s" % snap_name)
        return Snap(self, snap_name, snap_id)

class Object(object):
    """Rados object wrapper, makes the object look like a file"""
    def __init__(self, pool, key):
        self.key = key
        self.pool = pool
        self.offset = 0
        self.state = "exists"

    def __str__(self):
        return "rados.Object(pool=%s,key=%s)" % (str(self.pool), self.key.value)

    def require_object_exists(self):
        if self.state != "exists":
            raise ObjectStateError("The object is %s" % self.state)

    def read(self, length = 1024*1024):
        self.require_object_exists()
        ret = self.pool.read(self.key, self.offset, length)
        self.offset += len(ret)
        return ret

    def write(self, string_to_write):
        self.require_object_exists()
        ret = self.pool.write(self.key, string_to_write, self.offset)
        self.offset += ret
        return ret

    def remove(self):
        self.require_object_exists()
        self.pool.remove_object(self.key)
        self.state = "removed"

    def stat(self):
        self.require_object_exists()
        return self.pool.stat(self.key)

    def seek(self, position):
        self.require_object_exists()
        self.offset = position

    def get_xattr(self, xattr_name):
        self.require_object_exists()
        return self.pool.get_xattr(self.key, xattr_name)

    def set_xattr(self, xattr_name, xattr_value):
        self.require_object_exists()
        return self.pool.set_xattr(self.key, xattr_name, xattr_value)

    def rm_xattr(self, xattr_name):
        self.require_object_exists()
        return self.pool.rm_xattr(self.key, xattr_name)
