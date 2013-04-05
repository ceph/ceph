"""
This module is a thin wrapper around librados.

Copyright 2011, Hannu Valtonen <hannu.valtonen@ormod.com>
"""
from ctypes import CDLL, c_char_p, c_size_t, c_void_p, c_int, c_long, \
    create_string_buffer, byref, Structure, c_uint64, c_ubyte, pointer, \
    CFUNCTYPE
import threading
import ctypes
import errno
import time
from datetime import datetime

ANONYMOUS_AUID = 0xffffffffffffffff
ADMIN_AUID = 0

class Error(Exception):
    """ `Error` class, derived from `Exception` """
    pass

class PermissionError(Error):
    """ `PermissionError` class, derived from `Error` """
    pass

class ObjectNotFound(Error):
    """ `ObjectNotFound` class, derived from `Error` """
    pass

class NoData(Error):
    """ `NoData` class, derived from `Error` """
    pass

class ObjectExists(Error):
    """ `ObjectExists` class, derived from `Error` """
    pass

class IoError(Error):
    """ `IoError` class, derived from `Error` """
    pass

class NoSpace(Error):
    """ `NoSpace` class, derived from `Error` """
    pass

class IncompleteWriteError(Error):
    """ `IncompleteWriteError` class, derived from `Error` """
    pass

class RadosStateError(Error):
    """ `RadosStateError` class, derived from `Error` """
    pass

class IoctxStateError(Error):
    """ `IoctxStateError` class, derived from `Error` """
    pass

class ObjectStateError(Error):
    """ `ObjectStateError` class, derived from `Error` """
    pass

class LogicError(Error):
    """ `` class, derived from `Error` """
    pass

def make_ex(ret, msg):
    """
    Translate a librados return code into an exception.

    :param ret: the return code
    :type ret: int
    :param msg: the error message to use
    :type msg: str
    :returns: a subclass of :class:`Error`
    """

    errors = {
        errno.EPERM     : PermissionError,
        errno.ENOENT    : ObjectNotFound,
        errno.EIO       : IoError,
        errno.ENOSPC    : NoSpace,
        errno.EEXIST    : ObjectExists,
        errno.ENODATA   : NoData
        }
    ret = abs(ret)
    if ret in errors:
        return errors[ret](msg)
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

class rados_cluster_stat_t(Structure):
    _fields_ = [("kb", c_uint64),
                ("kb_used", c_uint64),
                ("kb_avail", c_uint64),
                ("num_objects", c_uint64)]

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

    def __init__(self, rados_id=None, conf=None, conffile=None):
        self.librados = CDLL('librados.so.2')
        self.cluster = c_void_p()
        self.rados_id = rados_id
        if rados_id is not None and not isinstance(rados_id, str):
            raise TypeError('rados_id must be a string or None')
        if conffile is not None and not isinstance(conffile, str):
            raise TypeError('conffile must be a string or None')
        ret = self.librados.rados_create(byref(self.cluster), c_char_p(rados_id))
        if ret != 0:
            raise Error("rados_initialize failed with error code: %d" % ret)
        self.state = "configuring"
        if conffile is not None:
            # read the default conf file when '' is given
            if conffile == '':
                conffile = None
            self.conf_read_file(conffile)
        if conf is not None:
            for key, value in conf.iteritems():
                self.conf_set(key, value)

    def shutdown(self):
        """
        Disconnects from the cluster.
        """
        if (self.__dict__.has_key("state") and self.state != "shutdown"):
            self.librados.rados_shutdown(self.cluster)
            self.state = "shutdown"

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type_, value, traceback):
        self.shutdown()
        return False

    def __del__(self):
        self.shutdown()

    def version(self):
        """
        Get the version number of the ``librados`` C library.
    
        :returns: a tuple of ``(major, minor, extra)`` components of the
                  librados version
        """
        major = c_int(0)
        minor = c_int(0)
        extra = c_int(0)
        self.librados.rados_version(byref(major), byref(minor), byref(extra))
        return Version(major.value, minor.value, extra.value)

    def conf_read_file(self, path=None):
        """
        Configure the cluster handle using a Ceph config file.

        :param path: path to the config file
        :type path: str
        """
        self.require_state("configuring", "connected")
        if path is not None and not isinstance(path, str):
            raise TypeError('path must be a string')
        ret = self.librados.rados_conf_read_file(self.cluster, c_char_p(path))
        if (ret != 0):
            raise make_ex(ret, "error calling conf_read_file")

    def conf_get(self, option):
        """
        Get the value of a configuration option

        :param option: which option to read
        :type option: str

        :returns: str - value of the option or None
        :raises: :class:`TypeError`
        """
        self.require_state("configuring", "connected")
        if not isinstance(option, str):
            raise TypeError('option must be a string')
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
        """
        Set the value of a configuration option

        :param option: which option to set
        :type option: str
        :param option: value of the option
        :type option: str

        :raises: :class:`TypeError`, :class:`ObjectNotFound`
        """
        self.require_state("configuring", "connected")
        if not isinstance(option, str):
            raise TypeError('option must be a string')
        if not isinstance(val, str):
            raise TypeError('val must be a string')
        ret = self.librados.rados_conf_set(self.cluster, c_char_p(option),
                                            c_char_p(val))
        if (ret != 0):
            raise make_ex(ret, "error calling conf_set")

    def connect(self):
        """
        Connect to the cluster.
        """
        self.require_state("configuring")
        ret = self.librados.rados_connect(self.cluster)
        if (ret != 0):
            raise make_ex(ret, "error calling connect")
        self.state = "connected"

    def get_cluster_stats(self):
        """
        Read usage info about the cluster
        
        This tells you total space, space used, space available, and number
        of objects. These are not updated immediately when data is written,
        they are eventually consistent.

        :returns: dict - contains the following keys:

            *``kb`` (int) - total space 

            *``kb_used`` (int) - space used

            *``kb_avail`` (int) - free space available

            *``num_objects`` (int) - number of objects

        """
        stats = rados_cluster_stat_t()
        ret = self.librados.rados_cluster_stat(self.cluster, byref(stats))
        if ret < 0:
            raise make_ex(
                ret, "Rados.get_cluster_stats(%s): get_stats failed" % self.rados_id)
        return {'kb': stats.kb,
                'kb_used': stats.kb_used,
                'kb_avail': stats.kb_avail,
                'num_objects': stats.num_objects}

    def pool_exists(self, pool_name):
        """
        Checks if a given pool exists.

        :param pool_name: name of the pool to check
        :type pool_name: str

        :raises: :class:`TypeError`, :class:`Error`
        :returns: bool - whether the pool exists, false otherwise.
        """
        self.require_state("connected")
        if not isinstance(pool_name, str):
            raise TypeError('pool_name must be a string')
        ret = self.librados.rados_pool_lookup(self.cluster, c_char_p(pool_name))
        if (ret >= 0):
            return True
        elif (ret == -errno.ENOENT):
            return False
        else:
            raise make_ex(ret, "error looking up pool '%s'" % pool_name)

    def create_pool(self, pool_name, auid=None, crush_rule=None):
        """
        Create a pool:
        - with default settings: if auid=None and crush_rule=None
        - owned by a specific auid: auid given and crush_rule=None
        - with a specific CRUSH rule: if auid=None and crush_rule given
        - with a specific CRUSH rule and auid: if auid and crush_rule given

        :param pool_name: name of the pool to create
        :type pool_name: str
        :param auid: the id of the owner of the new pool
        :type auid: int
        :param crush_rule: rule to use for placement in the new pool
        :type crush_rule: str

        :raises: :class:`TypeError`, :class:`Error`
        """
        self.require_state("connected")
        if not isinstance(pool_name, str):
            raise TypeError('pool_name must be a string')
        if crush_rule is not None and not isinstance(crush_rule, str):
            raise TypeError('cruse_rule must be a string')
        if (auid == None):
            if (crush_rule == None):
                ret = self.librados.rados_pool_create(
                            self.cluster, c_char_p(pool_name))
            else:
                ret = self.librados.rados_pool_create_with_crush_rule(
                            self.cluster, c_char_p(pool_name), c_ubyte(crush_rule))

        elif (crush_rule == None):
            ret = self.librados.rados_pool_create_with_auid(
                        self.cluster, c_char_p(pool_name), c_uint64(auid))
        else:
            ret = self.librados.rados_pool_create_with_all(
                        self.cluster, c_char_p(pool_name), c_uint64(auid),
                        c_ubyte(crush_rule))
        if ret < 0:
            raise make_ex(ret, "error creating pool '%s'" % pool_name)

    def delete_pool(self, pool_name):
        """
        Delete a pool and all data inside it.

        The pool is removed from the cluster immediately,
        but the actual data is deleted in the background.

        :param pool_name: name of the pool to delete 
        :type pool_name: str

        :raises: :class:`TypeError`, :class:`Error`
        """
        self.require_state("connected")
        if not isinstance(pool_name, str):
            raise TypeError('pool_name must be a string')
        ret = self.librados.rados_pool_delete(self.cluster, c_char_p(pool_name))
        if ret < 0:
            raise make_ex(ret, "error deleting pool '%s'" % pool_name)

    def list_pools(self):
        """
        Gets a list of pool names. 

        :returns: list - of pool names.
        """
        self.require_state("connected")
        size = c_size_t(512)
        while True:
            c_names = create_string_buffer(size.value)
            ret = self.librados.rados_pool_list(self.cluster,
                                                byref(c_names), size)
            if ret > size.value:
                size = c_size_t(ret)
            else:
                break
        return filter(lambda name: name != '', c_names.raw.split('\0'))

    def get_fsid(self):
        """
        Get the fsid of the cluster as a hexadecimal string.

        :raises: :class:`Error`
        :returns: str - cluster fsid
        """
        self.require_state("connected")
        fsid_len = 36
        fsid = create_string_buffer(fsid_len + 1)
        ret = self.librados.rados_cluster_fsid(self.cluster,
                                               byref(fsid),
                                               fsid_len + 1)
        if ret < 0:
            raise make_ex(ret, "error getting cluster fsid")
        return fsid.value

    def open_ioctx(self, ioctx_name):
        """
        Create an io context

        The io context allows you to perform operations within a particular
        pool. 

        :param ioctx_name: name of the pool 
        :type ioctx_name: str

        :raises: :class:`TypeError`, :class:`Error`
        :returns: Ioctx - Rados Ioctx object 
        """
        self.require_state("connected")
        if not isinstance(ioctx_name, str):
            raise TypeError('ioctx_name must be a string')
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
        """
        Get the next object name and locator in the pool

        :raises: StopIteration
        :returns: next rados.Ioctx Object
        """
        key = c_char_p()
        locator = c_char_p()
        ret = self.ioctx.librados.rados_objects_list_next(self.ctx, byref(key),
                                                          byref(locator))
        if ret < 0:
            raise StopIteration()
        return Object(self.ioctx, key.value, locator.value)

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
        """
        Get the next xattr on the object

        :raises: StopIteration
        :returns: pair - of name and value of the next Xattr
        """
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
            num_snaps = num_snaps * 2
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
            ret = self.ioctx.librados.rados_ioctx_snap_get_name(self.ioctx.io, \
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
        ret = self.ioctx.librados.rados_ioctx_snap_get_stamp(
            self.ioctx.io, self.snap_id,
            byref(snap_time))
        if (ret != 0):
            raise make_ex(ret, "rados_ioctx_snap_get_stamp error")
        return datetime.fromtimestamp(snap_time.value)

class Completion(object):
    """completion object"""
    def __init__(self, ioctx, rados_comp, oncomplete, onsafe):
        self.rados_comp = rados_comp
        self.oncomplete = oncomplete
        self.onsafe = onsafe
        self.ioctx = ioctx

    def wait_for_safe(self):
        return self.ioctx.librados.rados_aio_is_safe(
            self.rados_comp
            )

    def wait_for_complete(self):
        return self.ioctx.librados.rados_aio_is_complete(
            self.rados_comp
            )

    def get_return_value(self):
        return self.ioctx.librados.rados_aio_get_return_value(
            self.rados_comp)

    def __del__(self):
        self.ioctx.librados.rados_aio_release(
            self.rados_comp
            )

class Ioctx(object):
    """rados.Ioctx object"""
    def __init__(self, name, librados, io):
        self.name = name
        self.librados = librados
        self.io = io
        self.state = "open"
        self.locator_key = ""
        self.safe_cbs = {}
        self.complete_cbs = {}
        RADOS_CB = CFUNCTYPE(c_int, c_void_p, c_void_p)
        self.__aio_safe_cb_c = RADOS_CB(self.__aio_safe_cb)
        self.__aio_complete_cb_c = RADOS_CB(self.__aio_complete_cb)
        self.lock = threading.Lock()

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()
        return False

    def __del__(self):
        self.close()

    def __aio_safe_cb(self, completion, _):
        cb = None
        with self.lock:
            cb = self.safe_cbs[completion]
            del self.safe_cbs[completion]
        cb.onsafe(cb)
        return 0

    def __aio_complete_cb(self, completion, _):
        cb = None
        with self.lock:
            cb = self.complete_cbs[completion]
            del self.complete_cbs[completion]
        cb.oncomplete(cb)
        return 0

    def __get_completion(self, oncomplete, onsafe):
        completion = c_void_p(0)
        complete_cb = None
        safe_cb = None
        if oncomplete:
            complete_cb = self.__aio_complete_cb_c
        if onsafe:
            safe_cb = self.__aio_safe_cb_c
        ret = self.librados.rados_aio_create_completion(
            c_void_p(0),
            complete_cb,
            safe_cb,
            byref(completion)
            )
        if ret < 0:
            raise make_ex(ret, "error getting a completion")
        with self.lock:
            completion_obj = Completion(self, completion, oncomplete, onsafe)
            if oncomplete:
                self.complete_cbs[completion.value] = completion_obj
            if onsafe:
                self.safe_cbs[completion.value] = completion_obj
        return completion_obj

    def aio_write(self, object_name, to_write, offset=0,
                  oncomplete=None, onsafe=None):
        completion = self.__get_completion(oncomplete, onsafe)
        ret = self.librados.rados_aio_write(
            self.io,
            c_char_p(object_name),
            completion.rados_comp,
            c_char_p(to_write),
            c_size_t(len(to_write)),
            c_uint64(offset))
        if ret < 0:
            raise make_ex(ret, "error writing object %s" % object_name)
        return completion

    def aio_write_full(self, object_name, to_write,
                       oncomplete=None, onsafe=None):
        completion = self.__get_completion(oncomplete, onsafe)
        ret = self.librados.rados_aio_write_full(
            self.io,
            c_char_p(object_name),
            completion.rados_comp,
            c_char_p(to_write),
            c_size_t(len(to_write)))
        if ret < 0:
            raise make_ex(ret, "error writing object %s" % object_name)
        return completion

    def aio_append(self, object_name, to_append, oncomplete=None, onsafe=None):
        completion = self.__get_completion(oncomplete, onsafe)
        ret = self.librados.rados_aio_append(
            self.io,
            c_char_p(object_name),
            completion.rados_comp,
            c_char_p(to_append),
            c_size_t(len(to_append)))
        if ret < 0:
            raise make_ex(ret, "error appending to object %s" % object_name)
        return completion

    def aio_flush(self):
        ret = self.librados.rados_aio_flush(
            self.io)
        if ret < 0:
            raise make_ex(ret, "error flushing")

    def aio_read(self, object_name, length, offset, oncomplete):
        """
        oncomplete will be called with the returned read value as
        well as the completion:

        oncomplete(completion, data_read)
        """
        buf = create_string_buffer(length)
        def oncomplete_(completion):
            return oncomplete(completion, buf.value)
        completion = self.__get_completion(oncomplete_, None)
        ret = self.librados.rados_aio_read(
            self.io,
            c_char_p(object_name),
            completion.rados_comp,
            buf,
            c_size_t(length),
            c_uint64(offset))
        if ret < 0:
            raise make_ex(ret, "error reading %s" % object_name)
        return completion

    def require_ioctx_open(self):
        if self.state != "open":
            raise IoctxStateError("The pool is %s" % self.state)

    def change_auid(self, auid):
        self.require_ioctx_open()
        ret = self.librados.rados_ioctx_pool_set_auid(self.io, \
                ctypes.c_uint64(auid))
        if ret < 0:
            raise make_ex(ret, "error changing auid of '%s' to %d" %\
                (self.name, auid))

    def set_locator_key(self, loc_key):
        self.require_ioctx_open()
        if not isinstance(loc_key, str):
            raise TypeError('loc_key must be a string')
        self.librados.rados_ioctx_locator_set_key(self.io, \
                c_char_p(loc_key))
        self.locator_key = loc_key

    def get_locator_key(self):
        return self.locator_key

    def close(self):
        if self.state == "open":
            self.require_ioctx_open()
            self.librados.rados_ioctx_destroy(self.io)
            self.state = "closed"

    def write(self, key, data, offset=0):
        self.require_ioctx_open()
        if not isinstance(data, str):
            raise TypeError('data must be a string')
        length = len(data)
        ret = self.librados.rados_write(self.io, c_char_p(key),
                 c_char_p(data), c_size_t(length), c_uint64(offset))
        if ret == length:
            return ret
        elif ret < 0:
            raise make_ex(ret, "Ioctx.write(%s): failed to write %s" % \
                (self.name, key))
        elif ret < length:
            raise IncompleteWriteError("Wrote only %d out of %d bytes" % \
                (ret, length))
        else:
            raise LogicError("Ioctx.write(%s): rados_write \
returned %d, but %d was the maximum number of bytes it could have \
written." % (self.name, ret, length))

    def write_full(self, key, data):
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        if not isinstance(data, str):
            raise TypeError('data must be a string')
        length = len(data)
        ret = self.librados.rados_write_full(self.io, c_char_p(key),
                 c_char_p(data), c_size_t(length))
        if ret == 0:
            return ret
        else:
            raise make_ex(ret, "Ioctx.write(%s): failed to write_full %s" % \
                (self.name, key))

    def read(self, key, length=8192, offset=0):
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        ret_buf = create_string_buffer(length)
        ret = self.librados.rados_read(self.io, c_char_p(key), ret_buf,
                c_size_t(length), c_uint64(offset))
        if ret < 0:
            raise make_ex(ret, "Ioctx.read(%s): failed to read %s" % (self.name, key))
        return ctypes.string_at(ret_buf, ret)

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
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        ret = self.librados.rados_remove(self.io, c_char_p(key))
        if ret < 0:
            raise make_ex(ret, "Failed to remove '%s'" % key)
        return True

    def trunc(self, key, size):
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        ret = self.librados.rados_trunc(self.io, c_char_p(key), c_size_t(size))
        if ret < 0:
            raise make_ex(ret, "Ioctx.trunc(%s): failed to truncate %s" % (self.name, key))
        return ret

    def stat(self, key):
        """Stat object, returns, size/timestamp"""
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        psize = c_uint64()
        pmtime = c_uint64()

        ret = self.librados.rados_stat(self.io, c_char_p(key), pointer(psize),
                                        pointer(pmtime))
        if ret < 0:
            raise make_ex(ret, "Failed to stat %r" % key)
        return psize.value, time.localtime(pmtime.value)

    def get_xattr(self, key, xattr_name):
        self.require_ioctx_open()
        if not isinstance(xattr_name, str):
            raise TypeError('xattr_name must be a string')
        ret_length = 4096
        ret_buf = create_string_buffer(ret_length)
        ret = self.librados.rados_getxattr(self.io, c_char_p(key),
                    c_char_p(xattr_name), ret_buf, c_size_t(ret_length))
        if ret < 0:
            raise make_ex(ret, "Failed to get xattr %r" % xattr_name)
        return ctypes.string_at(ret_buf, ret)

    def get_xattrs(self, oid):
        self.require_ioctx_open()
        if not isinstance(oid, str):
            raise TypeError('oid must be a string')
        it = c_void_p(0)
        ret = self.librados.rados_getxattrs(self.io, oid, byref(it))
        if ret != 0:
            raise make_ex(ret, "Failed to get rados xattrs for object %r" % oid)
        return XattrIterator(self, it, oid)

    def set_xattr(self, key, xattr_name, xattr_value):
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        if not isinstance(xattr_name, str):
            raise TypeError('xattr_name must be a string')
        if not isinstance(xattr_value, str):
            raise TypeError('xattr_value must be a string')
        ret = self.librados.rados_setxattr(self.io, c_char_p(key),
                    c_char_p(xattr_name), c_char_p(xattr_value),
                    c_size_t(len(xattr_value)))
        if ret < 0:
            raise make_ex(ret, "Failed to set xattr %r" % xattr_name)
        return True

    def rm_xattr(self, key, xattr_name):
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        if not isinstance(xattr_name, str):
            raise TypeError('xattr_name must be a string')
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
        if not isinstance(snap_name, str):
            raise TypeError('snap_name must be a string')
        ret = self.librados.rados_ioctx_snap_create(self.io,
                                    c_char_p(snap_name))
        if (ret != 0):
            raise make_ex(ret, "Failed to create snap %s" % snap_name)

    def remove_snap(self, snap_name):
        self.require_ioctx_open()
        if not isinstance(snap_name, str):
            raise TypeError('snap_name must be a string')
        ret = self.librados.rados_ioctx_snap_remove(self.io,
                                    c_char_p(snap_name))
        if (ret != 0):
            raise make_ex(ret, "Failed to remove snap %s" % snap_name)

    def lookup_snap(self, snap_name):
        self.require_ioctx_open()
        if not isinstance(snap_name, str):
            raise TypeError('snap_name must be a string')
        snap_id = c_uint64()
        ret = self.librados.rados_ioctx_snap_lookup(self.io, \
                            c_char_p(snap_name), byref(snap_id))
        if (ret != 0):
            raise make_ex(ret, "Failed to lookup snap %s" % snap_name)
        return Snap(self, snap_name, snap_id)

    def get_last_version(self):
        self.require_ioctx_open()
        return self.librados.rados_get_last_version(self.io)

def set_object_locator(func):
    def retfunc(self, *args, **kwargs):
        if self.locator_key is not None:
            old_locator = self.ioctx.get_locator_key()
            self.ioctx.set_locator_key(self.locator_key)
            retval = func(self, *args, **kwargs)
            self.ioctx.set_locator_key(old_locator)
            return retval
        else:
            return func(self, *args, **kwargs)
    return retfunc

class Object(object):
    """Rados object wrapper, makes the object look like a file"""
    def __init__(self, ioctx, key, locator_key=None):
        self.key = key
        self.ioctx = ioctx
        self.offset = 0
        self.state = "exists"
        self.locator_key = locator_key

    def __str__(self):
        return "rados.Object(ioctx=%s,key=%s)" % (str(self.ioctx), self.key)

    def require_object_exists(self):
        if self.state != "exists":
            raise ObjectStateError("The object is %s" % self.state)

    @set_object_locator
    def read(self, length = 1024*1024):
        self.require_object_exists()
        ret = self.ioctx.read(self.key, length, self.offset)
        self.offset += len(ret)
        return ret

    @set_object_locator
    def write(self, string_to_write):
        self.require_object_exists()
        ret = self.ioctx.write(self.key, string_to_write, self.offset)
        self.offset += ret
        return ret

    @set_object_locator
    def remove(self):
        self.require_object_exists()
        self.ioctx.remove_object(self.key)
        self.state = "removed"

    @set_object_locator
    def stat(self):
        self.require_object_exists()
        return self.ioctx.stat(self.key)

    def seek(self, position):
        self.require_object_exists()
        self.offset = position

    @set_object_locator
    def get_xattr(self, xattr_name):
        self.require_object_exists()
        return self.ioctx.get_xattr(self.key, xattr_name)

    @set_object_locator
    def get_xattrs(self, xattr_name):
        self.require_object_exists()
        return self.ioctx.get_xattrs(self.key, xattr_name)

    @set_object_locator
    def set_xattr(self, xattr_name, xattr_value):
        self.require_object_exists()
        return self.ioctx.set_xattr(self.key, xattr_name, xattr_value)

    @set_object_locator
    def rm_xattr(self, xattr_name):
        self.require_object_exists()
        return self.ioctx.rm_xattr(self.key, xattr_name)
