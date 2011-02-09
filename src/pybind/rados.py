"""librados Python ctypes wrapper
Copyright 2011, Hannu Valtonen <hannu.valtonen@ormod.com>
"""
from ctypes import CDLL, c_char_p, c_size_t, c_void_p,\
    create_string_buffer, byref, Structure, c_uint64, pointer
import time

class RadosError(Exception):
    pass

class ObjectNotFound(Exception):
    pass

class WriteError(Exception):
    pass

class IncompleteWriteError(Exception):
    pass

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

def object_deleted(method):
    def wrapper(self, *args, **kwargs):
        try:
            if self.deleted == False:
                return method(self, *args, **kwargs)
            else:
                raise ObjectNotFound("Object has been deleted")
        except:
            raise
    wrapper.__doc__ = method.__doc__
    wrapper.__name__ = method.__name__
    return wrapper

class Rados(object):
    """librados python wrapper"""
    def __init__(self):
        self.librados = CDLL('librados.so')
        ret = self.librados.rados_initialize(None)
        if ret != 0:
            raise RadosError("rados_initialize failed with error code: %d" \
                    % ret)
        self.initialized = True

    def __del__(self):
        if (self.__dict__.has_key("initialized") and self.initialized == True):
            self.librados.rados_deinitialize()

    def create_pool(self, pool_name):
        ret = self.librados.rados_create_pool(c_char_p(pool_name))
        if ret < 0:
            raise RadosError("pool %s couldn't be created" % pool_name)

    def delete_pool(self, pool):
        ret = self.librados.rados_delete_pool(pool)
        if ret < 0:
            raise RadosError("pool couldn't be deleted")

    def get_pool(self, pool_name):
        try:
            pool = self.open_pool(pool_name)
        except RadosError:
            self.create_pool(pool_name)
            pool = self.open_pool(pool_name)
        return pool

    def open_pool(self, pool_name):
        pool = c_void_p()
        ret = self.librados.rados_open_pool(c_char_p(pool_name), byref(pool))
        if ret < 0:
            raise RadosError("pool %s couldn't be opened" % pool_name)
        return Pool(self.librados, pool)

    def close_pool(self, pool):
        ret = self.librados.rados_close_pool(pool)
        if ret < 0:
            raise RadosError("pool couldn't be closed")

class Pool(object):
    """Pool object"""
    def __init__(self, librados, pool):
        self.librados = librados
        self.pool = pool
        self.deleted = False

    @object_deleted
    def get_object(self, key):
        return Object(self, key)

    @object_deleted
    def write(self, key, string_to_write, offset = 0):
        length = len(string_to_write)
        ret = self.librados.rados_write(self.pool, c_char_p(key),
                    c_size_t(offset), c_char_p(string_to_write),
                    c_size_t(length))
        if ret == length:
            return ret
        elif ret < 0:
            raise WriteError("Write failed completely")
        elif ret < length:
            raise IncompleteWriteError("Wrote only %ld/%ld bytes" % (ret, length))
        else:
            raise RadosError("something weird happened while writing")

    @object_deleted
    def read(self, key, offset = 0, length = 8192):
        ret_buf = create_string_buffer(length)
        ret = self.librados.rados_read(self.pool, c_char_p(key), c_size_t(offset),
                                        ret_buf, c_size_t(length))
        if ret < 0:
            raise RadosError("Read failure, object doesn't exist?")
        return ret_buf.value

    @object_deleted
    def get_stats(self):
        stats = rados_pool_stat_t()
        ret = self.librados.rados_stat_pool(self.pool, byref(stats))
        if ret < 0:
            raise RadosError("Couldn't get stats from pool")
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

    @object_deleted
    def remove_object(self, key):
        ret = self.librados.rados_remove(self.pool, c_char_p(key))
        if ret < 0:
            raise RadosError("Delete failure")
        return True

    @object_deleted
    def stat(self, key):
        """Stat object, returns, size/timestamp"""
        psize = c_uint64()
        pmtime = c_uint64()

        ret = self.librados.rados_stat(self.pool, c_char_p(key), pointer(psize),
                                        pointer(pmtime))
        if ret < 0:
            raise RadosError("Failed to stat %r" % key)
        return psize.value, time.localtime(pmtime.value)

    @object_deleted
    def get_xattr(self, key, xattr_name):
        ret_length = 4096
        ret_buf = create_string_buffer(ret_length)
        ret = self.librados.rados_getxattr(self.pool, c_char_p(key),
                    c_char_p(xattr_name), ret_buf, c_size_t(ret_length))
        if ret < 0:
            raise RadosError("Failed to get xattr %r" % xattr_name)
        return ret_buf.value

    @object_deleted
    def set_xattr(self, key, xattr_name, xattr_value):
        ret = self.librados.rados_setxattr(self.pool, c_char_p(key),
                    c_char_p(xattr_name), c_char_p(xattr_value),
                    c_size_t(len(xattr_value)))
        if ret < 0:
            raise RadosError("Failed to set xattr %r" % xattr_name)
        return True

    @object_deleted
    def rm_xattr(self, key, xattr_name):
        ret = self.librados.rados_rmxattr(self.pool, c_char_p(key), c_char_p(xattr_name))
        if ret < 0:
            raise RadosError("Failed to delete key %r xattr %r" % (key, xattr_name))
        return True

class Object(object):
    """Rados object wrapper, makes the object look like a file"""
    def __init__(self, pool, key):
        self.key = key
        self.pool = pool
        self.offset = 0
        self.deleted = False

    @object_deleted
    def read(self, length = 1024*1024):
        ret = self.pool.read(self.key, self.offset, length)
        self.offset += len(ret)
        return ret

    @object_deleted
    def write(self, string_to_write):
        ret = self.pool.write(self.key, string_to_write, self.offset)
        self.offset += ret
        return ret

    @object_deleted
    def remove(self):
        self.pool.remove_object(self.key)
        self.deleted = True

    @object_deleted
    def stat(self):
        return self.pool.stat(self.key)

    @object_deleted
    def seek(self, position):
        self.offset = position

    @object_deleted
    def get_xattr(self, xattr_name):
        return self.pool.get_xattr(self.key, xattr_name)

    @object_deleted
    def set_xattr(self, xattr_name, xattr_value):
        return self.pool.set_xattr(self.key, xattr_name, xattr_value)

    @object_deleted
    def rm_xattr(self, xattr_name):
        return self.pool.rm_xattr(self.key, xattr_name)
