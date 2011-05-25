"""librados Python ctypes wrapper
Copyright 2011, Hannu Valtonen <hannu.valtonen@ormod.com>
"""
from ctypes import CDLL, c_char_p, c_size_t, c_void_p, c_int, \
    create_string_buffer, byref, Structure, c_uint64, c_ubyte, c_byte, pointer
import ctypes
import datetime
import errno
import time

ANONYMOUS_AUID = 0xffffffffffffffff
ADMIN_AUID = 0

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

class RadosStateError(Exception):
    pass

class IoctxStateError(Exception):
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

class Version(object):
    def __init__(self, major, minor, extra):
        self.major = major
        self.minor = minor
        self.extra = extra

    def __str__(self):
        return "%d.%d.%d" % (self.major, self.minor, self.extra)

class Rados(object):
    """librados python wrapper"""
    def require_state(self, *args):
        for a in args:
            if self.state == a:
                return
        raise RadosStateError("You cannot perform that operation on a \
Rados object in state %s." % (self.state))

    def __init__(self, rados_id = None):
        self.librados = CDLL('librados.so')
        self.cluster = c_void_p()
        ret = self.librados.rados_create(byref(self.cluster), c_char_p(rados_id))
        if ret != 0:
            raise Error("rados_initialize failed with error code: %d" % ret)
        self.state = "configuring"

    def shutdown(self):
        if (self.__dict__.has_key("state") and self.state != "shutdown"):
            self.librados.rados_shutdown(self.cluster)
            self.state = "shutdown"

    def __del__(self):
        self.shutdown()

    def version(self):
        major = c_int(0)
        minor = c_int(0)
        extra = c_int(0)
        self.librados.rados_version(byref(major), byref(minor), byref(extra))
        return Version(major.value, minor.value, extra.value)

    def conf_read_file(self, path = None):
        self.require_state("configuring", "connected")
        ret = self.librados.rados_conf_read_file(self.cluster, c_char_p(path))
        if (ret != 0):
            raise make_ex(ret, "error calling conf_read_file")

    def conf_get(self, option):
        self.require_state("configuring", "connected")
        length = 20
        while True:
            ret_buf = create_string_buffer(length)
            ret = self.librados.rados_conf_get(self.cluster, option,
                                                ret_buf, c_size_t(length))
            if (ret == 0):
                return ret_buf.value
            elif (ret == -errno.ENAMETOOLONG):
                length = length * 2
            elif (ret == -errno.ENOENT):
                return None
            else:
                raise make_ex(ret, "error calling conf_get")

    def conf_set(self, option, val):
        self.require_state("configuring", "connected")
        ret = self.librados.rados_conf_set(self.cluster, c_char_p(option),
                                            c_char_p(val))
        if (ret != 0):
            raise make_ex(ret, "error calling conf_set")

    def connect(self):
        self.require_state("configuring")
        ret = self.librados.rados_connect(self.cluster)
        if (ret != 0):
            raise make_ex(ret, "error calling connect")
        self.state = "connected"

    # Returns true if the pool exists; false otherwise.
    def pool_exists(self, pool_name):
        self.require_state("connected")
        pool = c_void_p()
        ret = self.librados.rados_pool_lookup(self.cluster, c_char_p(pool_name))
        if (ret >= 0):
            return True
        elif (ret == -errno.ENOENT):
            return False
        else:
            raise make_ex(ret, "error looking up pool '%s'" % pool_name)

    def create_pool(self, pool_name, auid=None, crush_rule=None):
        self.require_state("connected")
        if (auid == None):
            if (crush_rule == None):
                ret = self.librados.rados_pool_create(
                            self.cluster, c_char_p(pool_name))
            else:
                ret = self.librados.rados_pool_create_with_all(
                            self.cluster, c_char_p(pool_name), c_ubyte(auid),
                            c_ubyte(crush_rule))
        elif (crush_rule == None):
            ret = self.librados.rados_pool_create_with_auid(
                        self.cluster, c_char_p(pool_name), c_ubyte(auid))
        else:
            ret = self.librados.rados_pool_create_with_crush_rule(
                        self.cluster, c_char_p(pool_name), c_ubyte(crush_rule))
        if ret < 0:
            raise make_ex(ret, "error creating pool '%s'" % pool_name)

    def delete_pool(self, pool_name):
        self.require_state("connected")
        ret = self.librados.rados_pool_delete(self.cluster, c_char_p(pool_name))
        if ret < 0:
            raise make_ex(ret, "error deleting pool '%s'" % pool_name)

    def open_ioctx(self, ioctx_name):
        self.require_state("connected")
        ioctx = c_void_p()
        ret = self.librados.rados_ioctx_create(self.cluster, c_char_p(ioctx_name), byref(ioctx))
        if ret < 0:
            raise make_ex(ret, "error opening ioctx '%s'" % ioctx_name)
        return Ioctx(ioctx_name, self.librados, ioctx)

class ObjectIterator(object):
    """rados.Ioctx Object iterator"""
    def __init__(self, ioctx):
        self.ioctx = ioctx
        self.ctx = c_void_p()
        ret = self.ioctx.librados.\
            rados_objects_list_open(self.ioctx.io, byref(self.ctx))
        if ret < 0:
            raise make_ex(ret, "error iterating over the objects in ioctx '%s'" \
                % self.ioctx.name)

    def __iter__(self):
        return self

    def next(self):
        key = c_char_p()
        ret = self.ioctx.librados.rados_objects_list_next(self.ctx, byref(key))
        if ret < 0:
            raise StopIteration()
        return Object(self.ioctx, key.value)

    def __del__(self):
        self.ioctx.librados.rados_objects_list_close(self.ctx)

class XattrIterator(object):
    """Extended attribute iterator"""
    def __init__(self, ioctx, it, oid):
        self.ioctx = ioctx
        self.it = it
        self.oid = oid
    def __iter__(self):
        return self
    def next(self):
        name_ = c_char_p(0)
        val_ = c_char_p(0)
        len_ = c_int(0)
        ret = self.ioctx.librados.\
            rados_getxattrs_next(self.it, byref(name_), byref(val_), byref(len_))
        if (ret != 0):
          raise make_ex(ret, "error iterating over the extended attributes \
in '%s'" % self.oid)
        if name_.value == None:
            raise StopIteration()
        name = ctypes.string_at(name_)
        val = ctypes.string_at(val_, len_)
        return (name, val)
    def __del__(self):
        self.ioctx.librados.rados_getxattrs_end(self.it)

class SnapIterator(object):
    """Snapshot iterator"""
    def __init__(self, ioctx):
        self.ioctx = ioctx
        # We don't know how big a buffer we need until we've called the
        # function. So use the exponential doubling strategy.
        num_snaps = 10
        while True:
            self.snaps = (ctypes.c_uint64 * num_snaps)()
            ret = self.ioctx.librados.rados_ioctx_snap_list(self.ioctx.io,
                                self.snaps, num_snaps)
            if (ret >= 0):
                self.max_snap = ret
                break
            elif (ret != -errno.ERANGE):
                raise make_ex(ret, "error calling rados_snap_list for \
ioctx '%s'" % self.ioctx.name)
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
            ret = self.ioctx.librados.rados_ioctx_snap_get_name(self.ioctx.io,\
                                snap_id, byref(name), name_len)
            if (ret == 0):
                name_len = ret
                break
            elif (ret != -errno.ERANGE):
                raise make_ex(ret, "rados_snap_get_name error")
            name_len = name_len * 2
        snap = Snap(self.ioctx, name.value, snap_id)
        self.cur_snap = self.cur_snap + 1
        return snap

class Snap(object):
    """Snapshot object"""
    def __init__(self, ioctx, name, snap_id):
        self.ioctx = ioctx
        self.name = name
        self.snap_id = snap_id

    def __str__(self):
        return "rados.Snap(ioctx=%s,name=%s,snap_id=%d)" \
            % (str(self.ioctx), self.name, self.snap_id)

    def get_timestamp(self):
        snap_time = c_long(0)
        ret = rados_ioctx_snap_get_stamp(self.ioctx.io, self.snap_id,
                                        byref(snap_time))
        if (ret != 0):
            raise make_ex(ret, "rados_ioctx_snap_get_stamp error")
        return date.fromtimestamp(snap_time)

class Ioctx(object):
    """rados.Ioctx object"""
    def __init__(self, name, librados, io):
        self.name = name
        self.librados = librados
        self.io = io
        self.state = "open"

    def __del__(self):
        if (self.state == "open"):
            self.close()

    def require_ioctx_open(self):
        if self.state != "open":
            raise IoctxStateError("The pool is %s" % self.state)

    def change_auid(self, auid):
        self.require_ioctx_open()
        ret = self.librados.rados_ioctx_pool_set_auid(self.io,\
                ctypes.c_int64(auid))
        if ret < 0:
            raise make_ex(ret, "error changing auid of '%s' to %lld" %\
                (self.name, auid))

    def set_locator_key(self, loc_key):
        self.require_ioctx_open()
        ret = self.librados.rados_ioctx_locator_set_key(self.io,\
                c_char_p(loc_key))
        if ret < 0:
            raise make_ex(ret, "error changing locator key of '%s' to '%s'" %\
                (self.name, loc_key))

    def close(self):
        self.require_ioctx_open()
        self.librados.rados_ioctx_destroy(self.io)
        self.state = "closed"

    def write(self, key, data, offset = 0):
        self.require_ioctx_open()
        length = len(data)
        ret = self.librados.rados_write(self.io, c_char_p(key),
                 c_char_p(data), c_size_t(length), c_uint64(offset))
        if ret == length:
            return ret
        elif ret < 0:
            raise make_ex(ret, "Ioctx.write(%s): failed to write %s" % \
                (self.name, key))
        elif ret < length:
            raise IncompleteWriteError("Wrote only %ld out of %ld bytes" % \
                (ret, length))
        else:
            raise make_ex("Ioctx.write(%s): logic error: rados_write \
returned %d, but %d was the maximum number of bytes it could have \
written." % (self.name, ret, length))

    def write_full(self, key, data, offset = 0):
        self.require_ioctx_open()
        length = len(data)
        ret = self.librados.rados_write_full(self.io, c_char_p(key),
                 c_char_p(data), c_size_t(length), c_uint64(offset))
        if ret == 0:
            return ret
        else:
            raise make_ex(ret, "Ioctx.write(%s): failed to write_full %s" % \
                (self.name, key))

    def read(self, key, length = 8192, offset = 0):
        self.require_ioctx_open()
        ret_buf = create_string_buffer(length)
        ret = self.librados.rados_read(self.io, c_char_p(key), ret_buf,
                c_size_t(length), c_uint64(offset))
        if ret < 0:
            raise make_ex("Ioctx.read(%s): failed to read %s" % (self.name, key))
        return ret_buf.value

    def get_stats(self):
        self.require_ioctx_open()
        stats = rados_pool_stat_t()
        ret = self.librados.rados_ioctx_pool_stat(self.io, byref(stats))
        if ret < 0:
            raise make_ex(ret, "Ioctx.get_stats(%s): get_stats failed" % self.name)
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
        self.require_ioctx_open()
        ret = self.librados.rados_remove(self.io, c_char_p(key))
        if ret < 0:
            raise make_ex(ret, "Failed to remove '%s'" % key)
        return True

    def stat(self, key):
        self.require_ioctx_open()
        """Stat object, returns, size/timestamp"""
        psize = c_uint64()
        pmtime = c_uint64()

        ret = self.librados.rados_stat(self.io, c_char_p(key), pointer(psize),
                                        pointer(pmtime))
        if ret < 0:
            raise make_ex(ret, "Failed to stat %r" % key)
        return psize.value, time.localtime(pmtime.value)

    def get_xattr(self, key, xattr_name):
        self.require_ioctx_open()
        ret_length = 4096
        ret_buf = create_string_buffer(ret_length)
        ret = self.librados.rados_getxattr(self.io, c_char_p(key),
                    c_char_p(xattr_name), ret_buf, c_size_t(ret_length))
        if ret < 0:
            raise make_ex(ret, "Failed to get xattr %r" % xattr_name)
        return ctypes.string_at(ret_buf, ret)

    def get_xattrs(self, oid):
        self.require_ioctx_open()
        it = c_void_p(0)
        ret = self.librados.rados_getxattrs(self.io, oid, byref(it))
        if ret != 0:
            raise make_ex(ret, "Failed to get rados xattrs for object %r" % oids)
        return XattrIterator(self, it, oid)

    def set_xattr(self, key, xattr_name, xattr_value):
        self.require_ioctx_open()
        ret = self.librados.rados_setxattr(self.io, c_char_p(key),
                    c_char_p(xattr_name), c_char_p(xattr_value),
                    c_size_t(len(xattr_value)))
        if ret < 0:
            raise make_ex(ret, "Failed to set xattr %r" % xattr_name)
        return True

    def rm_xattr(self, key, xattr_name):
        self.require_ioctx_open()
        ret = self.librados.rados_rmxattr(self.io, c_char_p(key), c_char_p(xattr_name))
        if ret < 0:
            raise make_ex(ret, "Failed to delete key %r xattr %r" %
                (key, xattr_name))
        return True

    def list_objects(self):
        self.require_ioctx_open()
        return ObjectIterator(self)

    def list_snaps(self):
        self.require_ioctx_open()
        return SnapIterator(self)

    def create_snap(self, snap_name):
        self.require_ioctx_open()
        ret = self.librados.rados_ioctx_snap_create(self.io,
                                    c_char_p(snap_name))
        if (ret != 0):
            raise make_ex(ret, "Failed to create snap %s" % snap_name)

    def remove_snap(self, snap_name):
        self.require_ioctx_open()
        ret = self.librados.rados_ioctx_snap_remove(self.io,
                                    c_char_p(snap_name))
        if (ret != 0):
            raise make_ex(ret, "Failed to remove snap %s" % snap_name)

    def lookup_snap(self, snap_name):
        self.require_ioctx_open()
        snap_id = c_uint64()
        ret = self.librados.rados_ioctx_snap_lookup(self.io,\
                            c_char_p(snap_name), byref(snap_id))
        if (ret != 0):
            raise make_ex(ret, "Failed to lookup snap %s" % snap_name)
        return Snap(self, snap_name, snap_id)

    def get_last_version(self):
        self.require_ioctx_open()
        return self.librados.rados_get_last_version(self.io)

class Object(object):
    """Rados object wrapper, makes the object look like a file"""
    def __init__(self, ioctx, key):
        self.key = key
        self.ioctx = ioctx
        self.offset = 0
        self.state = "exists"

    def __str__(self):
        return "rados.Object(ioctx=%s,key=%s)" % (str(self.ioctx), self.key.value)

    def require_object_exists(self):
        if self.state != "exists":
            raise ObjectStateError("The object is %s" % self.state)

    def read(self, length = 1024*1024):
        self.require_object_exists()
        ret = self.ioctx.read(self.key, self.offset, length)
        self.offset += len(ret)
        return ret

    def write(self, string_to_write):
        self.require_object_exists()
        ret = self.ioctx.write(self.key, string_to_write, self.offset)
        self.offset += ret
        return ret

    def remove(self):
        self.require_object_exists()
        self.ioctx.remove_object(self.key)
        self.state = "removed"

    def stat(self):
        self.require_object_exists()
        return self.ioctx.stat(self.key)

    def seek(self, position):
        self.require_object_exists()
        self.offset = position

    def get_xattr(self, xattr_name):
        self.require_object_exists()
        return self.ioctx.get_xattr(self.key, xattr_name)

    def get_xattrs(self, xattr_name):
        self.require_object_exists()
        return self.ioctx.get_xattrs(self.key, xattr_name)

    def set_xattr(self, xattr_name, xattr_value):
        self.require_object_exists()
        return self.ioctx.set_xattr(self.key, xattr_name, xattr_value)

    def rm_xattr(self, xattr_name):
        self.require_object_exists()
        return self.ioctx.rm_xattr(self.key, xattr_name)
