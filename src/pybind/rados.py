"""
This module is a thin wrapper around librados.

Copyright 2011, Hannu Valtonen <hannu.valtonen@ormod.com>
"""
from ctypes import CDLL, c_char_p, c_size_t, c_void_p, c_char, c_int, c_long, \
    c_ulong, create_string_buffer, byref, Structure, c_uint64, c_ubyte, \
    pointer, CFUNCTYPE, c_int64, c_uint32, c_uint8
from ctypes.util import find_library
import ctypes
import errno
import threading
import time
import sys

from collections import Iterator
from datetime import datetime
from functools import wraps
from itertools import chain

ANONYMOUS_AUID = 0xffffffffffffffff
ADMIN_AUID = 0
LIBRADOS_ALL_NSPACES = '\001'

LIBRADOS_OP_FLAG_FADVISE_RANDOM = 0x4
LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL = 0x8
LIBRADOS_OP_FLAG_FADVISE_WILLNEED = 0x10
LIBRADOS_OP_FLAG_FADVISE_DONTNEED = 0x20
LIBRADOS_OP_FLAG_FADVISE_NOCACHE = 0x40


# Are we running Python 2.x
_python2 = sys.hexversion < 0x03000000


if _python2:
    str_type = basestring
else:
    str_type = str


class Error(Exception):
    """ `Error` class, derived from `Exception` """
    pass


class InterruptedOrTimeoutError(Error):
    """ `InterruptedOrTimeoutError` class, derived from `Error` """
    pass


class PermissionError(Error):
    """ `PermissionError` class, derived from `Error` """
    pass

class PermissionDeniedError(Error):
    """ deal with EACCES related. """
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


class ObjectBusy(Error):
    """ `ObjectBusy` class, derived from `Error` """
    pass


class IOError(Error):
    """ `IOError` class, derived from `Error` """
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


class TimedOut(Error):
    """ `TimedOut` class, derived from `Error` """
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
        errno.EIO       : IOError,
        errno.ENOSPC    : NoSpace,
        errno.EEXIST    : ObjectExists,
        errno.EBUSY     : ObjectBusy,
        errno.ENODATA   : NoData,
        errno.EINTR     : InterruptedOrTimeoutError,
        errno.ETIMEDOUT : TimedOut,
        errno.EACCES    : PermissionDeniedError
        }
    ret = abs(ret)
    if ret in errors:
        return errors[ret](msg)
    else:
        return Error(msg + (": errno %s" % errno.errorcode[ret]))


class rados_pool_stat_t(Structure):
    """ Usage information for a pool """
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
    """ Cluster-wide usage information """
    _fields_ = [("kb", c_uint64),
                ("kb_used", c_uint64),
                ("kb_avail", c_uint64),
                ("num_objects", c_uint64)]


class timeval(Structure):
    _fields_ = [("tv_sec", c_long), ("tv_usec", c_long)]


class Version(object):
    """ Version information """
    def __init__(self, major, minor, extra):
        self.major = major
        self.minor = minor
        self.extra = extra

    def __str__(self):
        return "%d.%d.%d" % (self.major, self.minor, self.extra)


class RadosThread(threading.Thread):
    def __init__(self, target, args=None):
        self.args = args
        self.target = target
        threading.Thread.__init__(self)

    def run(self):
        self.retval = self.target(*self.args)

# time in seconds between each call to t.join() for child thread
POLL_TIME_INCR = 0.5


def run_in_thread(target, args, timeout=0):
    interrupt = False

    countdown = timeout
    t = RadosThread(target, args)

    # allow the main thread to exit (presumably, avoid a join() on this
    # subthread) before this thread terminates.  This allows SIGINT
    # exit of a blocked call.  See below.
    t.daemon = True

    t.start()
    try:
        # poll for thread exit
        while t.is_alive():
            t.join(POLL_TIME_INCR)
            if timeout and t.is_alive():
                countdown = countdown - POLL_TIME_INCR
                if countdown <= 0:
                    raise KeyboardInterrupt

        t.join()        # in case t exits before reaching the join() above
    except KeyboardInterrupt:
        # ..but allow SIGINT to terminate the waiting.  Note: this
        # relies on the Linux kernel behavior of delivering the signal
        # to the main thread in preference to any subthread (all that's
        # strictly guaranteed is that *some* thread that has the signal
        # unblocked will receive it).  But there doesn't seem to be
        # any interface to create t with SIGINT blocked.
        interrupt = True

    if interrupt:
        t.retval = -errno.EINTR
    return t.retval


# helper to specify an optional argument, where in addition to `cls`, `None`
# is also acceptable
def opt(cls):
    return (cls, None)


# validate argument types of an instance method
# kwargs is an un-ordered dict, so use args instead
def requires(*types):
    def is_type_of(v, t):
        if t is None:
            return v is None
        else:
            return isinstance(v, t)

    def check_type(val, arg_name, arg_type):
        if isinstance(arg_type, tuple):
            if any(is_type_of(val, t) for t in arg_type):
                return
            type_names = ' or '.join('None' if t is None else t.__name__
                                     for t in arg_type)
            raise TypeError('%s must be %s' % (arg_name, type_names))
        else:
            if is_type_of(val, arg_type):
                return
            assert(arg_type is not None)
            raise TypeError('%s must be %s' % (arg_name, arg_type.__name__))

    def wrapper(f):
        @wraps(f)
        def validate_func(*args, **kwargs):
            # ignore the `self` arg
            pos_args = zip(args[1:], types)
            named_args = ((kwargs[name], (name, spec)) for name, spec in types
                          if name in kwargs)
            for arg_val, (arg_name, arg_type) in chain(pos_args, named_args):
                check_type(arg_val, arg_name, arg_type)
            return f(*args, **kwargs)
        return validate_func
    return wrapper


def cstr(val, encoding="utf-8"):
    """
    Create a C-style string from a Python string

    :param str val: Python string
    :param encoding: Encoding to use
    :rtype: c_char_p
    """
    if val is None:
        return c_char_p(None)

    if _python2 and isinstance(val, str):
        # Don't encode str on Python 2, as it's already an 8-bit string
        return c_char_p(val)
    else:
        return c_char_p(val.encode(encoding))


def decode_cstr(addr, size=-1, encoding="utf-8"):
    """
    Decode a C-style string into a Python string.

    Return None if a the C string is a NULL pointer.

    :param c_char_p addr: C-style string
    :param int: String size (assume NUL-terminated if size is -1)
    :param encoding: Encoding to use
    :rtype: str or None
    """
    if not addr:
        # NULL pointer
        return None

    return ctypes.string_at(addr, size).decode(encoding)


class Rados(object):
    """librados python wrapper"""
    def require_state(self, *args):
        """
        Checks if the Rados object is in a special state

        :raises: RadosStateError
        """
        if self.state in args:
            return
        raise RadosStateError("You cannot perform that operation on a \
Rados object in state %s." % self.state)

    @requires(('rados_id', opt(str_type)), ('name', opt(str_type)), ('clustername', opt(str_type)),
              ('conffile', opt(str_type)))
    def __init__(self, rados_id=None, name=None, clustername=None,
                 conf_defaults=None, conffile=None, conf=None, flags=0):
        library_path = find_library('rados')
        # maybe find_library can not find it correctly on all platforms,
        # so fall back to librados.so.2 in such case.
        self.librados = CDLL(library_path if library_path is not None else 'librados.so.2')

        self.parsed_args = []
        self.conf_defaults = conf_defaults
        self.conffile = conffile
        self.cluster = c_void_p()
        self.rados_id = rados_id
        if rados_id and name:
            raise Error("Rados(): can't supply both rados_id and name")
        elif rados_id:
            name = 'client.' + rados_id
        elif name is None:
            name = 'client.admin'
        if clustername is None:
            clustername = 'ceph'
        ret = run_in_thread(self.librados.rados_create2,
                            (byref(self.cluster), cstr(clustername),
                            cstr(name), c_uint64(flags)))

        if ret != 0:
            raise Error("rados_initialize failed with error code: %d" % ret)
        self.state = "configuring"
        # order is important: conf_defaults, then conffile, then conf
        if conf_defaults:
            for key, value in conf_defaults.items():
                self.conf_set(key, value)
        if conffile is not None:
            # read the default conf file when '' is given
            if conffile == '':
                conffile = None
            self.conf_read_file(conffile)
        if conf:
            for key, value in conf.items():
                self.conf_set(key, value)

    def shutdown(self):
        """
        Disconnects from the cluster.  Call this explicitly when a
        Rados.connect()ed object is no longer used.
        """
        if hasattr(self, "state") and self.state != "shutdown":
            run_in_thread(self.librados.rados_shutdown, (self.cluster,))
            self.state = "shutdown"

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type_, value, traceback):
        self.shutdown()
        return False

    def version(self):
        """
        Get the version number of the ``librados`` C library.

        :returns: a tuple of ``(major, minor, extra)`` components of the
                  librados version
        """
        major = c_int(0)
        minor = c_int(0)
        extra = c_int(0)
        run_in_thread(self.librados.rados_version,
                      (byref(major), byref(minor), byref(extra)))
        return Version(major.value, minor.value, extra.value)


    @requires(('path', opt(str_type)))
    def conf_read_file(self, path=None):
        """
        Configure the cluster handle using a Ceph config file.

        :param path: path to the config file
        :type path: str
        """
        self.require_state("configuring", "connected")
        ret = run_in_thread(self.librados.rados_conf_read_file,
                            (self.cluster, cstr(path)))
        if (ret != 0):
            raise make_ex(ret, "error calling conf_read_file")

    def conf_parse_argv(self, args):
        """
        Parse known arguments from args, and remove; returned
        args contain only those unknown to ceph
        """
        self.require_state("configuring", "connected")
        if not args:
            return
        # create instances of arrays of c_char_p's, both len(args) long
        # cretargs will always be a subset of cargs (perhaps identical)
        cargs = (c_char_p * len(args))(*map(cstr, args))
        cretargs = (c_char_p * len(args))()
        ret = run_in_thread(self.librados.rados_conf_parse_argv_remainder,
                            (self.cluster, len(args), cargs, cretargs))
        if ret:
            raise make_ex(ret, "error calling conf_parse_argv_remainder")

        # cretargs was allocated with fixed length; collapse return
        # list to eliminate any missing args
        retargs = [a.decode('utf-8') for a in cretargs if a is not None]
        self.parsed_args = args
        return retargs

    def conf_parse_env(self, var='CEPH_ARGS'):
        """
        Parse known arguments from an environment variable, normally
        CEPH_ARGS.
        """
        self.require_state("configuring", "connected")
        if not var:
            return
        ret = run_in_thread(self.librados.rados_conf_parse_env,
                            (self.cluster, cstr(var)))
        if (ret != 0):
            raise make_ex(ret, "error calling conf_parse_env")

    @requires(('option', str_type))
    def conf_get(self, option):
        """
        Get the value of a configuration option

        :param option: which option to read
        :type option: str

        :returns: str - value of the option or None
        :raises: :class:`TypeError`
        """
        self.require_state("configuring", "connected")
        length = 20
        while True:
            ret_buf = create_string_buffer(length)
            ret = run_in_thread(self.librados.rados_conf_get,
                                (self.cluster, cstr(option), ret_buf,
                                c_size_t(length)))
            if (ret == 0):
                return decode_cstr(ret_buf)
            elif (ret == -errno.ENAMETOOLONG):
                length = length * 2
            elif (ret == -errno.ENOENT):
                return None
            else:
                raise make_ex(ret, "error calling conf_get")

    @requires(('option', str_type), ('val', str_type))
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
        ret = run_in_thread(self.librados.rados_conf_set,
                            (self.cluster, cstr(option), cstr(val)))
        if (ret != 0):
            raise make_ex(ret, "error calling conf_set")

    def ping_monitor(self, mon_id):
        """
        Ping a monitor to assess liveness

        May be used as a simply way to assess liveness, or to obtain
        information about the monitor in a simple way even in the
        absence of quorum.

        :param mon_id: the ID portion of the monitor's name (i.e., mon.<ID>)
        :type mon_id: str
        :returns: the string reply from the monitor
        """

        self.require_state("configuring", "connected")

        outstrp = pointer(pointer(c_char()))
        outstrlen = c_long()

        ret = run_in_thread(self.librados.rados_ping_monitor,
                            (self.cluster, cstr(mon_id),
                             outstrp, byref(outstrlen)))

        my_outstr = outstrp.contents[:(outstrlen.value)]
        if outstrlen.value:
            run_in_thread(self.librados.rados_buffer_free, (outstrp.contents,))

        if ret != 0:
            raise make_ex(ret, "error calling ping_monitor")
        return decode_cstr(my_outstr)

    def connect(self, timeout=0):
        """
        Connect to the cluster.  Use shutdown() to release resources.
        """
        self.require_state("configuring")
        ret = run_in_thread(self.librados.rados_connect, (self.cluster,),
                            timeout)
        if (ret != 0):
            raise make_ex(ret, "error connecting to the cluster")
        self.state = "connected"

    def get_cluster_stats(self):
        """
        Read usage info about the cluster

        This tells you total space, space used, space available, and number
        of objects. These are not updated immediately when data is written,
        they are eventually consistent.

        :returns: dict - contains the following keys:

            - ``kb`` (int) - total space

            - ``kb_used`` (int) - space used

            - ``kb_avail`` (int) - free space available

            - ``num_objects`` (int) - number of objects

        """
        stats = rados_cluster_stat_t()
        ret = run_in_thread(self.librados.rados_cluster_stat,
                            (self.cluster, byref(stats)))
        if ret < 0:
            raise make_ex(
                ret, "Rados.get_cluster_stats(%s): get_stats failed" % self.rados_id)
        return {'kb': stats.kb,
                'kb_used': stats.kb_used,
                'kb_avail': stats.kb_avail,
                'num_objects': stats.num_objects}

    @requires(('pool_name', str_type))
    def pool_exists(self, pool_name):
        """
        Checks if a given pool exists.

        :param pool_name: name of the pool to check
        :type pool_name: str

        :raises: :class:`TypeError`, :class:`Error`
        :returns: bool - whether the pool exists, false otherwise.
        """
        self.require_state("connected")
        ret = run_in_thread(self.librados.rados_pool_lookup,
                            (self.cluster, cstr(pool_name)))
        if (ret >= 0):
            return True
        elif (ret == -errno.ENOENT):
            return False
        else:
            raise make_ex(ret, "error looking up pool '%s'" % pool_name)

    @requires(('pool_name', str_type))
    def pool_lookup(self, pool_name):
        """
        Returns a pool's ID based on its name.

        :param pool_name: name of the pool to look up
        :type pool_name: str

        :raises: :class:`TypeError`, :class:`Error`
        :returns: int - pool ID, or None if it doesn't exist
        """
        self.require_state("connected")
        ret = run_in_thread(self.librados.rados_pool_lookup,
                            (self.cluster, cstr(pool_name)))
        if (ret >= 0):
            return int(ret)
        elif (ret == -errno.ENOENT):
            return None
        else:
            raise make_ex(ret, "error looking up pool '%s'" % pool_name)

    @requires(('pool_id', int))
    def pool_reverse_lookup(self, pool_id):
        """
        Returns a pool's name based on its ID.

        :param pool_id: ID of the pool to look up
        :type pool_id: int

        :raises: :class:`TypeError`, :class:`Error`
        :returns: string - pool name, or None if it doesn't exist
        """
        self.require_state("connected")
        size = c_size_t(512)
        while True:
            c_name = create_string_buffer(size.value)
            ret = run_in_thread(self.librados.rados_pool_reverse_lookup,
                                (self.cluster, c_int64(pool_id), byref(c_name), size))
            if ret > size.value:
                size = c_size_t(ret)
            elif ret == -errno.ENOENT:
                return None
            elif ret < 0:
                raise make_ex(ret, "error reverse looking up pool '%s'" % pool_id)
            else:
                return decode_cstr(c_name.value)
                break

    @requires(('pool_name', str_type), ('auid', opt(int)), ('crush_rule', opt(int)))
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
        :type crush_rule: int

        :raises: :class:`TypeError`, :class:`Error`
        """
        self.require_state("connected")
        if auid is None:
            if crush_rule is None:
                ret = run_in_thread(self.librados.rados_pool_create,
                                    (self.cluster, cstr(pool_name)))
            else:
                ret = run_in_thread(self.librados.
                                    rados_pool_create_with_crush_rule,
                                    (self.cluster, cstr(pool_name),
                                     c_ubyte(crush_rule)))

        elif crush_rule is None:
            ret = run_in_thread(self.librados.rados_pool_create_with_auid,
                                (self.cluster, cstr(pool_name),
                                 c_uint64(auid)))
        else:
            ret = run_in_thread(self.librados.rados_pool_create_with_all,
                                (self.cluster, cstr(pool_name),
                                 c_uint64(auid), c_ubyte(crush_rule)))
        if ret < 0:
            raise make_ex(ret, "error creating pool '%s'" % pool_name)

    @requires(('pool_id', int))
    def get_pool_base_tier(self, pool_id):
        """
        Get base pool

        :returns: base pool, or pool_id if tiering is not configured for the pool
        """
        self.require_state("connected")
        base_tier = c_int64(0)
        ret = run_in_thread(self.librados.rados_pool_get_base_tier,
                            (self.cluster, c_int64(pool_id), byref(base_tier)))
        if ret < 0:
            raise make_ex(ret, "get_pool_base_tier(%d)" % pool_id)
        return base_tier.value

    @requires(('pool_name', str_type))
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
        ret = run_in_thread(self.librados.rados_pool_delete,
                            (self.cluster, cstr(pool_name)))
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
            ret = run_in_thread(self.librados.rados_pool_list,
                                (self.cluster, byref(c_names), size))
            if ret > size.value:
                size = c_size_t(ret)
            else:
                break

        return [decode_cstr(name) for name in c_names.raw.split(b'\0') if len(name) > 0]

    def get_fsid(self):
        """
        Get the fsid of the cluster as a hexadecimal string.

        :raises: :class:`Error`
        :returns: str - cluster fsid
        """
        self.require_state("connected")
        buf_len = 37
        fsid = create_string_buffer(buf_len)
        ret = run_in_thread(self.librados.rados_cluster_fsid,
                            (self.cluster, byref(fsid), c_size_t(buf_len)))
        if ret < 0:
            raise make_ex(ret, "error getting cluster fsid")
        return fsid.value

    @requires(('ioctx_name', str_type))
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
        ioctx = c_void_p()
        ret = run_in_thread(self.librados.rados_ioctx_create,
                            (self.cluster, cstr(ioctx_name), byref(ioctx)))
        if ret < 0:
            raise make_ex(ret, "error opening pool '%s'" % ioctx_name)
        return Ioctx(ioctx_name, self.librados, ioctx)

    def mon_command(self, cmd, inbuf, timeout=0, target=None):
        """
        mon_command[_target](cmd, inbuf, outbuf, outbuflen, outs, outslen)
        returns (int ret, string outbuf, string outs)
        """
        self.require_state("connected")
        outbufp = pointer(pointer(c_char()))
        outbuflen = c_long()
        outsp = pointer(pointer(c_char()))
        outslen = c_long()
        cmdarr = (c_char_p * len(cmd))(*map(cstr, cmd))

        if target:
            ret = run_in_thread(self.librados.rados_mon_command_target,
                                (self.cluster, cstr(target), cmdarr,
                                 len(cmd), c_char_p(inbuf), len(inbuf),
                                 outbufp, byref(outbuflen), outsp,
                                 byref(outslen)), timeout)
        else:
            ret = run_in_thread(self.librados.rados_mon_command,
                                (self.cluster, cmdarr, len(cmd),
                                 c_char_p(inbuf), len(inbuf),
                                 outbufp, byref(outbuflen), outsp, byref(outslen)),
                                timeout)

        # copy returned memory (ctypes makes a copy, not a reference)
        my_outbuf = outbufp.contents[:(outbuflen.value)]
        my_outs = decode_cstr(outsp.contents, outslen.value)

        # free callee's allocations
        if outbuflen.value:
            run_in_thread(self.librados.rados_buffer_free, (outbufp.contents,))
        if outslen.value:
            run_in_thread(self.librados.rados_buffer_free, (outsp.contents,))

        return (ret, my_outbuf, my_outs)

    def osd_command(self, osdid, cmd, inbuf, timeout=0):
        """
        osd_command(osdid, cmd, inbuf, outbuf, outbuflen, outs, outslen)
        returns (int ret, string outbuf, string outs)
        """
        self.require_state("connected")
        outbufp = pointer(pointer(c_char()))
        outbuflen = c_long()
        outsp = pointer(pointer(c_char()))
        outslen = c_long()
        cmdarr = (c_char_p * len(cmd))(*map(cstr, cmd))
        ret = run_in_thread(self.librados.rados_osd_command,
                            (self.cluster, osdid, cmdarr, len(cmd),
                             c_char_p(inbuf), len(inbuf),
                             outbufp, byref(outbuflen), outsp, byref(outslen)),
                            timeout)

        # copy returned memory (ctypes makes a copy, not a reference)
        my_outbuf = outbufp.contents[:(outbuflen.value)]
        my_outs = decode_cstr(outsp.contents, outslen.value)

        # free callee's allocations
        if outbuflen.value:
            run_in_thread(self.librados.rados_buffer_free, (outbufp.contents,))
        if outslen.value:
            run_in_thread(self.librados.rados_buffer_free, (outsp.contents,))

        return (ret, my_outbuf, my_outs)

    def pg_command(self, pgid, cmd, inbuf, timeout=0):
        """
        pg_command(pgid, cmd, inbuf, outbuf, outbuflen, outs, outslen)
        returns (int ret, string outbuf, string outs)
        """
        self.require_state("connected")
        outbufp = pointer(pointer(c_char()))
        outbuflen = c_long()
        outsp = pointer(pointer(c_char()))
        outslen = c_long()
        cmdarr = (c_char_p * len(cmd))(*map(cstr, cmd))
        ret = run_in_thread(self.librados.rados_pg_command,
                            (self.cluster, cstr(pgid), cmdarr, len(cmd),
                             c_char_p(inbuf), len(inbuf),
                             outbufp, byref(outbuflen), outsp, byref(outslen)),
                            timeout)

        # copy returned memory (ctypes makes a copy, not a reference)
        my_outbuf = outbufp.contents[:(outbuflen.value)]
        my_outs = decode_cstr(outsp.contents, outslen.value)

        # free callee's allocations
        if outbuflen.value:
            run_in_thread(self.librados.rados_buffer_free, (outbufp.contents,))
        if outslen.value:
            run_in_thread(self.librados.rados_buffer_free, (outsp.contents,))

        return (ret, my_outbuf, my_outs)

    def wait_for_latest_osdmap(self):
        self.require_state("connected")
        return run_in_thread(self.librados.rados_wait_for_latest_osdmap, (self.cluster,))

    def blacklist_add(self, client_address, expire_seconds=0):
        """
        Blacklist a client from the OSDs

        :param client_address: client address
        :type client_address: str
        :param expire_seconds: number of seconds to blacklist
        :type expire_seconds: int

        :raises: :class:`Error`
        """
        self.require_state("connected")
        ret = run_in_thread(self.librados.rados_blacklist_add,
                            (self.cluster, cstr(client_address),
                             c_uint32(expire_seconds)))
        if ret < 0:
            raise make_ex(ret, "error blacklisting client '%s'" % client_address)


class OmapIterator(Iterator):
    """Omap iterator"""
    def __init__(self, ioctx, ctx):
        self.ioctx = ioctx
        self.ctx = ctx

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        """
        Get the next key-value pair in the object
        :returns: next rados.OmapItem
        """
        key_ = c_char_p(0)
        val_ = c_char_p(0)
        len_ = c_int(0)
        ret = run_in_thread(self.ioctx.librados.rados_omap_get_next,
                      (self.ctx, byref(key_), byref(val_), byref(len_)))
        if (ret != 0):
            raise make_ex(ret, "error iterating over the omap")
        if key_.value is None:
            raise StopIteration()
        key = decode_cstr(key_)
        val = None
        if val_.value is not None:
            val = ctypes.string_at(val_, len_)
        return (key, val)

    def __del__(self):
        run_in_thread(self.ioctx.librados.rados_omap_get_end, (self.ctx,))


class ObjectIterator(Iterator):
    """rados.Ioctx Object iterator"""
    def __init__(self, ioctx):
        self.ioctx = ioctx
        self.ctx = c_void_p()
        ret = run_in_thread(self.ioctx.librados.rados_nobjects_list_open,
                            (self.ioctx.io, byref(self.ctx)))
        if ret < 0:
            raise make_ex(ret, "error iterating over the objects in ioctx '%s'"
                          % self.ioctx.name)

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        """
        Get the next object name and locator in the pool

        :raises: StopIteration
        :returns: next rados.Ioctx Object
        """
        key_ = c_char_p()
        locator_ = c_char_p()
        nspace_ = c_char_p()
        ret = run_in_thread(self.ioctx.librados.rados_nobjects_list_next,
                            (self.ctx, byref(key_), byref(locator_), byref(nspace_)))
        if ret < 0:
            raise StopIteration()

        key = decode_cstr(key_)
        locator = decode_cstr(locator_)
        nspace = decode_cstr(nspace_)
        return Object(self.ioctx, key, locator, nspace)

    def __del__(self):
        run_in_thread(self.ioctx.librados.rados_nobjects_list_close, (self.ctx,))


class XattrIterator(Iterator):
    """Extended attribute iterator"""
    def __init__(self, ioctx, it, oid):
        self.ioctx = ioctx
        self.it = it
        self.oid = oid

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        """
        Get the next xattr on the object

        :raises: StopIteration
        :returns: pair - of name and value of the next Xattr
        """
        name_ = c_char_p(0)
        val_ = c_char_p(0)
        len_ = c_int(0)
        ret = run_in_thread(self.ioctx.librados.rados_getxattrs_next,
                            (self.it, byref(name_), byref(val_), byref(len_)))
        if (ret != 0):
            raise make_ex(ret, "error iterating over the extended attributes \
in '%s'" % self.oid)
        if name_.value is None:
            raise StopIteration()
        name = decode_cstr(name_)
        val = ctypes.string_at(val_, len_)
        return (name, val)

    def __del__(self):
        run_in_thread(self.ioctx.librados.rados_getxattrs_end, (self.it,))


class SnapIterator(Iterator):
    """Snapshot iterator"""
    def __init__(self, ioctx):
        self.ioctx = ioctx
        # We don't know how big a buffer we need until we've called the
        # function. So use the exponential doubling strategy.
        num_snaps = 10
        while True:
            self.snaps = (ctypes.c_uint64 * num_snaps)()
            ret = run_in_thread(self.ioctx.librados.rados_ioctx_snap_list,
                                (self.ioctx.io, self.snaps, c_int(num_snaps)))
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
        return self.__next__()

    def __next__(self):
        """
        Get the next Snapshot

        :raises: :class:`Error`, StopIteration
        :returns: Snap - next snapshot
        """
        if (self.cur_snap >= self.max_snap):
            raise StopIteration
        snap_id = self.snaps[self.cur_snap]
        name_len = 10
        while True:
            name = create_string_buffer(name_len)
            ret = run_in_thread(self.ioctx.librados.rados_ioctx_snap_get_name,
                                (self.ioctx.io, c_uint64(snap_id), byref(name),
                                 c_int(name_len)))
            if (ret == 0):
                name_len = ret
                break
            elif (ret != -errno.ERANGE):
                raise make_ex(ret, "rados_snap_get_name error")
            name_len = name_len * 2
        snap = Snap(self.ioctx, decode_cstr(name), snap_id)
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
        """
        Find when a snapshot in the current pool occurred

        :raises: :class:`Error`
        :returns: datetime - the data and time the snapshot was created
        """
        snap_time = c_long(0)
        ret = run_in_thread(self.ioctx.librados.rados_ioctx_snap_get_stamp,
                            (self.ioctx.io, self.snap_id, byref(snap_time)))
        if (ret != 0):
            raise make_ex(ret, "rados_ioctx_snap_get_stamp error")
        return datetime.fromtimestamp(snap_time.value)


class Completion(object):
    """completion object"""
    def __init__(self, ioctx, rados_comp, oncomplete, onsafe,
                 complete_cb, safe_cb):
        self.rados_comp = rados_comp
        self.oncomplete = oncomplete
        self.onsafe = onsafe
        self.ioctx = ioctx
        self.complete_cb = complete_cb
        self.safe_cb = safe_cb

    def is_safe(self):
        """
        Is an asynchronous operation safe?

        This does not imply that the safe callback has finished.

        :returns: True if the operation is safe
        """
        return run_in_thread(self.ioctx.librados.rados_aio_is_safe,
                             (self.rados_comp,)) == 1

    def is_complete(self):
        """
        Has an asynchronous operation completed?

        This does not imply that the safe callback has finished.

        :returns: True if the operation is completed
        """
        return run_in_thread(self.ioctx.librados.rados_aio_is_complete,
                             (self.rados_comp,)) == 1

    def wait_for_safe(self):
        """
        Wait for an asynchronous operation to be marked safe

        This does not imply that the safe callback has finished.
        """
        run_in_thread(self.ioctx.librados.rados_aio_wait_for_safe,
                      (self.rados_comp,))

    def wait_for_complete(self):
        """
        Wait for an asynchronous operation to complete

        This does not imply that the complete callback has finished.
        """
        run_in_thread(self.ioctx.librados.rados_aio_wait_for_complete,
                      (self.rados_comp,))

    def wait_for_safe_and_cb(self):
        """
        Wait for an asynchronous operation to be marked safe and for
        the safe callback to have returned
        """
        run_in_thread(self.ioctx.librados.rados_aio_wait_for_safe_and_cb,
                      (self.rados_comp,))

    def wait_for_complete_and_cb(self):
        """
        Wait for an asynchronous operation to complete and for the
        complete callback to have returned

        :returns:  whether the operation is completed
        """
        return run_in_thread(
            self.ioctx.librados.rados_aio_wait_for_complete_and_cb,
            (self.rados_comp,)
        )

    def get_return_value(self):
        """
        Get the return value of an asychronous operation

        The return value is set when the operation is complete or safe,
        whichever comes first.

        :returns: int - return value of the operation
        """
        return run_in_thread(self.ioctx.librados.rados_aio_get_return_value,
                             (self.rados_comp,))

    def __del__(self):
        """
        Release a completion

        Call this when you no longer need the completion. It may not be
        freed immediately if the operation is not acked and committed.
        """
        run_in_thread(self.ioctx.librados.rados_aio_release,
                      (self.rados_comp,))


class WriteOpCtx(object):
    """write operation context manager"""
    def __init__(self, ioctx):
        self.ioctx = ioctx

    def __enter__(self):
        self.ioctx.librados.rados_create_write_op.restype = c_void_p
        ret = run_in_thread(self.ioctx.librados.rados_create_write_op, (None,))
        self.write_op = ret
        return ret

    def __exit__(self, type, msg, traceback):
        self.ioctx.librados.rados_release_write_op.argtypes = [c_void_p]
        run_in_thread(self.ioctx.librados.rados_release_write_op, (c_void_p(self.write_op),))


class ReadOpCtx(object):
    """read operation context manager"""
    def __init__(self, ioctx):
        self.ioctx = ioctx

    def __enter__(self):
        self.ioctx.librados.rados_create_read_op.restype = c_void_p
        ret = run_in_thread(self.ioctx.librados.rados_create_read_op, (None,))
        self.read_op = ret
        return ret

    def __exit__(self, type, msg, traceback):
        self.ioctx.librados.rados_release_read_op.argtypes = [c_void_p]
        run_in_thread(self.ioctx.librados.rados_release_read_op, (c_void_p(self.read_op),))


RADOS_CB = CFUNCTYPE(c_int, c_void_p, c_void_p)


class Ioctx(object):
    """rados.Ioctx object"""
    def __init__(self, name, librados, io):
        self.name = name
        self.librados = librados
        self.io = io
        self.state = "open"
        self.locator_key = ""
        self.nspace = ""
        self.safe_cbs = {}
        self.complete_cbs = {}
        self.lock = threading.Lock()

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()
        return False

    def __del__(self):
        self.close()

    def __aio_safe_cb(self, completion, _):
        """
        Callback to onsafe() for asynchronous operations
        """
        cb = None
        with self.lock:
            cb = self.safe_cbs[completion]
            del self.safe_cbs[completion]
        cb.onsafe(cb)
        return 0

    def __aio_complete_cb(self, completion, _):
        """
        Callback to oncomplete() for asynchronous operations
        """
        cb = None
        with self.lock:
            cb = self.complete_cbs[completion]
            del self.complete_cbs[completion]
        cb.oncomplete(cb)
        return 0

    def __get_completion(self, oncomplete, onsafe):
        """
        Constructs a completion to use with asynchronous operations

        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """
        completion = c_void_p(0)
        complete_cb = None
        safe_cb = None
        if oncomplete:
            complete_cb = RADOS_CB(self.__aio_complete_cb)
        if onsafe:
            safe_cb = RADOS_CB(self.__aio_safe_cb)
        ret = run_in_thread(self.librados.rados_aio_create_completion,
                            (c_void_p(0), complete_cb, safe_cb,
                             byref(completion)))
        if ret < 0:
            raise make_ex(ret, "error getting a completion")
        with self.lock:
            completion_obj = Completion(self, completion, oncomplete, onsafe,
                                        complete_cb, safe_cb)
            if oncomplete:
                self.complete_cbs[completion.value] = completion_obj
            if onsafe:
                self.safe_cbs[completion.value] = completion_obj
        return completion_obj

    def aio_write(self, object_name, to_write, offset=0,
                  oncomplete=None, onsafe=None):
        """
        Write data to an object asynchronously

        Queues the write and returns.

        :param object_name: name of the object
        :type object_name: str
        :param to_write: data to write
        :type to_write: str
        :param offset: byte offset in the object to begin writing at
        :type offset: int
        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """
        completion = self.__get_completion(oncomplete, onsafe)
        ret = run_in_thread(self.librados.rados_aio_write,
                            (self.io, cstr(object_name),
                             completion.rados_comp, c_char_p(to_write),
                             c_size_t(len(to_write)), c_uint64(offset)))
        if ret < 0:
            raise make_ex(ret, "error writing object %s" % object_name)
        return completion

    def aio_write_full(self, object_name, to_write,
                       oncomplete=None, onsafe=None):
        """
        Asychronously write an entire object

        The object is filled with the provided data. If the object exists,
        it is atomically truncated and then written.
        Queues the write and returns.

        :param object_name: name of the object
        :type object_name: str
        :param to_write: data to write
        :type to_write: str
        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """
        completion = self.__get_completion(oncomplete, onsafe)
        ret = run_in_thread(self.librados.rados_aio_write_full,
                            (self.io, cstr(object_name),
                             completion.rados_comp, c_char_p(to_write),
                             c_size_t(len(to_write))))
        if ret < 0:
            raise make_ex(ret, "error writing object %s" % object_name)
        return completion

    def aio_append(self, object_name, to_append, oncomplete=None, onsafe=None):
        """
        Asychronously append data to an object

        Queues the write and returns.

        :param object_name: name of the object
        :type object_name: str
        :param to_append: data to append
        :type to_append: str
        :param offset: byte offset in the object to begin writing at
        :type offset: int
        :param oncomplete: what to do when the write is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the write is safe and complete on storage
            on all replicas
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """
        completion = self.__get_completion(oncomplete, onsafe)
        ret = run_in_thread(self.librados.rados_aio_append,
                            (self.io, cstr(object_name),
                             completion.rados_comp, c_char_p(to_append),
                             c_size_t(len(to_append))))
        if ret < 0:
            raise make_ex(ret, "error appending to object %s" % object_name)
        return completion

    def aio_flush(self):
        """
        Block until all pending writes in an io context are safe

        :raises: :class:`Error`
        """
        ret = run_in_thread(self.librados.rados_aio_flush, (self.io,))
        if ret < 0:
            raise make_ex(ret, "error flushing")

    def aio_read(self, object_name, length, offset, oncomplete):
        """
        Asychronously read data from an object

        oncomplete will be called with the returned read value as
        well as the completion:

        oncomplete(completion, data_read)

        :param object_name: name of the object to read from
        :type object_name: str
        :param length: the number of bytes to read
        :type length: int
        :param offset: byte offset in the object to begin reading from
        :type offset: int
        :param oncomplete: what to do when the read is complete
        :type oncomplete: completion

        :raises: :class:`Error`
        :returns: completion object
        """
        buf = create_string_buffer(length)

        def oncomplete_(completion_v):
            return_value = completion_v.get_return_value()
            return oncomplete(completion_v,
                              ctypes.string_at(buf, return_value) if return_value >= 0 else None)

        completion = self.__get_completion(oncomplete_, None)
        ret = run_in_thread(self.librados.rados_aio_read,
                            (self.io, cstr(object_name),
                             completion.rados_comp, buf, c_size_t(length),
                             c_uint64(offset)))
        if ret < 0:
            raise make_ex(ret, "error reading %s" % object_name)
        return completion

    def aio_remove(self, object_name, oncomplete=None, onsafe=None):
        """
        Asychronously remove an object

        :param object_name: name of the object to remove
        :type object_name: str
        :param oncomplete: what to do when the remove is safe and complete in memory
            on all replicas
        :type oncomplete: completion
        :param onsafe:  what to do when the remove is safe and complete on storage
            on all replicas
        :type onsafe: completion

        :raises: :class:`Error`
        :returns: completion object
        """
        completion = self.__get_completion(oncomplete, onsafe)
        ret = run_in_thread(self.librados.rados_aio_remove,
                            (self.io, cstr(object_name),
                             completion.rados_comp))
        if ret < 0:
            raise make_ex(ret, "error removing %s" % object_name)
        return completion

    def require_ioctx_open(self):
        """
        Checks if the rados.Ioctx object state is 'open'

        :raises: IoctxStateError
        """
        if self.state != "open":
            raise IoctxStateError("The pool is %s" % self.state)

    def change_auid(self, auid):
        """
        Attempt to change an io context's associated auid "owner."

        Requires that you have write permission on both the current and new
        auid.

        :raises: :class:`Error`
        """
        self.require_ioctx_open()
        ret = run_in_thread(self.librados.rados_ioctx_pool_set_auid,
                            (self.io, ctypes.c_uint64(auid)))
        if ret < 0:
            raise make_ex(ret, "error changing auid of '%s' to %d"
                          % (self.name, auid))

    @requires(('loc_key', str_type))
    def set_locator_key(self, loc_key):
        """
        Set the key for mapping objects to pgs within an io context.

        The key is used instead of the object name to determine which
        placement groups an object is put in. This affects all subsequent
        operations of the io context - until a different locator key is
        set, all objects in this io context will be placed in the same pg.

        :param loc_key: the key to use as the object locator, or NULL to discard
            any previously set key
        :type loc_key: str

        :raises: :class:`TypeError`
        """
        self.require_ioctx_open()
        run_in_thread(self.librados.rados_ioctx_locator_set_key,
                      (self.io, cstr(loc_key)))
        self.locator_key = loc_key

    def get_locator_key(self):
        """
        Get the locator_key of context

        :returns: locator_key
        """
        return self.locator_key


    @requires(('nspace', str_type))
    def set_namespace(self, nspace):
        """
        Set the namespace for objects within an io context.

        The namespace in addition to the object name fully identifies
        an object. This affects all subsequent operations of the io context
        - until a different namespace is set, all objects in this io context
        will be placed in the same namespace.

        :param nspace: the namespace to use, or None/"" for the default namespace
        :type nspace: str

        :raises: :class:`TypeError`
        """
        self.require_ioctx_open()
        if nspace is None:
            nspace = ""
        run_in_thread(self.librados.rados_ioctx_set_namespace,
                      (self.io, cstr(nspace)))
        self.nspace = nspace

    def get_namespace(self):
        """
        Get the namespace of context

        :returns: namespace
        """
        return self.nspace

    def close(self):
        """
        Close a rados.Ioctx object.

        This just tells librados that you no longer need to use the io context.
        It may not be freed immediately if there are pending asynchronous
        requests on it, but you should not use an io context again after
        calling this function on it.
        """
        if self.state == "open":
            self.require_ioctx_open()
            run_in_thread(self.librados.rados_ioctx_destroy, (self.io,))
            self.state = "closed"


    @requires(('key', str_type), ('data', bytes))
    def write(self, key, data, offset=0):
        """
        Write data to an object synchronously

        :param key: name of the object
        :type key: str
        :param data: data to write
        :type data: bytes
        :param offset: byte offset in the object to begin writing at
        :type offset: int

        :raises: :class:`TypeError`
        :raises: :class:`LogicError`
        :returns: int - 0 on success
        """
        self.require_ioctx_open()
        length = len(data)
        ret = run_in_thread(self.librados.rados_write,
                            (self.io, cstr(key), c_char_p(data),
                             c_size_t(length), c_uint64(offset)))
        if ret == 0:
            return ret
        elif ret < 0:
            raise make_ex(ret, "Ioctx.write(%s): failed to write %s"
                          % (self.name, key))
        else:
            raise LogicError("Ioctx.write(%s): rados_write \
returned %d, but should return zero on success." % (self.name, ret))

    @requires(('key', str_type), ('data', bytes))
    def write_full(self, key, data):
        """
        Write an entire object synchronously.

        The object is filled with the provided data. If the object exists,
        it is atomically truncated and then written.

        :param key: name of the object
        :type key: str
        :param data: data to write
        :type data: bytes

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: int - 0 on success
        """
        self.require_ioctx_open()
        length = len(data)
        ret = run_in_thread(self.librados.rados_write_full,
                            (self.io, cstr(key), c_char_p(data),
                             c_size_t(length)))
        if ret == 0:
            return ret
        elif ret < 0:
            raise make_ex(ret, "Ioctx.write_full(%s): failed to write %s"
                          % (self.name, key))
        else:
            raise LogicError("Ioctx.write_full(%s): rados_write_full \
returned %d, but should return zero on success." % (self.name, ret))

    @requires(('key', str_type), ('data', bytes))
    def append(self, key, data):
        """
        Append data to an object synchronously

        :param key: name of the object
        :type key: str
        :param data: data to write
        :type data: bytes

        :raises: :class:`TypeError`
        :raises: :class:`LogicError`
        :returns: int - 0 on success
        """
        self.require_ioctx_open()
        length = len(data)
        ret = run_in_thread(self.librados.rados_append,
                            (self.io, cstr(key), c_char_p(data),
                             c_size_t(length)))
        if ret == 0:
            return ret
        elif ret < 0:
            raise make_ex(ret, "Ioctx.append(%s): failed to append %s"
                          % (self.name, key))
        else:
            raise LogicError("Ioctx.append(%s): rados_append \
returned %d, but should return zero on success." % (self.name, ret))

    @requires(('key', str_type))
    def read(self, key, length=8192, offset=0):
        """
        Read data from an object synchronously

        :param key: name of the object
        :type key: str
        :param length: the number of bytes to read (default=8192)
        :type length: int
        :param offset: byte offset in the object to begin reading at
        :type offset: int

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: str - data read from object
        """
        self.require_ioctx_open()
        ret_buf = create_string_buffer(length)
        ret = run_in_thread(self.librados.rados_read,
                            (self.io, cstr(key), ret_buf, c_size_t(length),
                             c_uint64(offset)))
        if ret < 0:
            raise make_ex(ret, "Ioctx.read(%s): failed to read %s" % (self.name, key))
        return ctypes.string_at(ret_buf, ret)

    @requires(('key', str_type), ('cls', str_type), ('method', str_type), ('data', bytes))
    def execute(self, key, cls, method, data, length=8192):
        """
        Execute an OSD class method on an object.

        :param key: name of the object
        :type key: str
        :param cls: name of the object class
        :type cls: str
        :param method: name of the method
        :type method: str
        :param data: input data
        :type data: bytes
        :param length: size of output buffer in bytes (default=8291)
        :type length: int

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: (ret, method output)
        """
        self.require_ioctx_open()
        ret_buf = create_string_buffer(length)
        ret = run_in_thread(self.librados.rados_exec,
                (self.io, cstr(key), cstr(cls), cstr(method),
                    c_char_p(data), c_size_t(len(data)), ret_buf,
                    c_size_t(length)))
        if ret < 0:
            raise make_ex(ret, "Ioctx.exec(%s): failed to exec %s:%s on %s" %
                    (self.name, cls, method, key))
        return ret, ctypes.string_at(ret_buf, min(ret, length))

    def get_stats(self):
        """
        Get pool usage statistics

        :returns: dict - contains the following keys:

            - ``num_bytes`` (int) - size of pool in bytes

            - ``num_kb`` (int) - size of pool in kbytes

            - ``num_objects`` (int) - number of objects in the pool

            - ``num_object_clones`` (int) - number of object clones

            - ``num_object_copies`` (int) - number of object copies

            - ``num_objects_missing_on_primary`` (int) - number of objets
                missing on primary

            - ``num_objects_unfound`` (int) - number of unfound objects

            - ``num_objects_degraded`` (int) - number of degraded objects

            - ``num_rd`` (int) - bytes read

            - ``num_rd_kb`` (int) - kbytes read

            - ``num_wr`` (int) - bytes written

            - ``num_wr_kb`` (int) - kbytes written
        """
        self.require_ioctx_open()
        stats = rados_pool_stat_t()
        ret = run_in_thread(self.librados.rados_ioctx_pool_stat,
                            (self.io, byref(stats)))
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
                "num_wr_kb": stats.num_wr_kb}

    @requires(('key', str_type))
    def remove_object(self, key):
        """
        Delete an object

        This does not delete any snapshots of the object.

        :param key: the name of the object to delete
        :type key: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: bool - True on success
        """
        self.require_ioctx_open()
        ret = run_in_thread(self.librados.rados_remove,
                            (self.io, cstr(key)))
        if ret < 0:
            raise make_ex(ret, "Failed to remove '%s'" % key)
        return True

    @requires(('key', str_type))
    def trunc(self, key, size):
        """
        Resize an object

        If this enlarges the object, the new area is logically filled with
        zeroes. If this shrinks the object, the excess data is removed.

        :param key: the name of the object to resize
        :type key: str
        :param size: the new size of the object in bytes
        :type size: int

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: int - 0 on success, otherwise raises error
        """

        self.require_ioctx_open()
        ret = run_in_thread(self.librados.rados_trunc,
                            (self.io, cstr(key), c_uint64(size)))
        if ret < 0:
            raise make_ex(ret, "Ioctx.trunc(%s): failed to truncate %s" % (self.name, key))
        return ret

    @requires(('key', str_type))
    def stat(self, key):
        """
        Get object stats (size/mtime)

        :param key: the name of the object to get stats from
        :type key: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: (size,timestamp)
        """
        self.require_ioctx_open()
        psize = c_uint64()
        pmtime = c_uint64()

        ret = run_in_thread(self.librados.rados_stat,
                            (self.io, cstr(key), pointer(psize),
                             pointer(pmtime)))
        if ret < 0:
            raise make_ex(ret, "Failed to stat %r" % key)
        return psize.value, time.localtime(pmtime.value)

    @requires(('key', str_type), ('xattr_name', str_type))
    def get_xattr(self, key, xattr_name):
        """
        Get the value of an extended attribute on an object.

        :param key: the name of the object to get xattr from
        :type key: str
        :param xattr_name: which extended attribute to read
        :type xattr_name: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: str - value of the xattr
        """
        self.require_ioctx_open()
        ret_length = 4096
        while ret_length < 4096 * 1024 * 1024:
            ret_buf = create_string_buffer(ret_length)
            ret = run_in_thread(self.librados.rados_getxattr,
                                (self.io, cstr(key), cstr(xattr_name),
                                 ret_buf, c_size_t(ret_length)))
            if (ret == -errno.ERANGE):
                ret_length *= 2
            elif ret < 0:
                raise make_ex(ret, "Failed to get xattr %r" % xattr_name)
            else:
                break
        return ctypes.string_at(ret_buf, ret)

    @requires(('oid', str_type))
    def get_xattrs(self, oid):
        """
        Start iterating over xattrs on an object.

        :param oid: the name of the object to get xattrs from
        :type oid: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: XattrIterator
        """
        self.require_ioctx_open()
        it = c_void_p(0)
        ret = run_in_thread(self.librados.rados_getxattrs,
                            (self.io, cstr(oid), byref(it)))
        if ret != 0:
            raise make_ex(ret, "Failed to get rados xattrs for object %r" % oid)
        return XattrIterator(self, it, oid)

    @requires(('key', str_type), ('xattr_name', str_type), ('xattr_value', bytes))
    def set_xattr(self, key, xattr_name, xattr_value):
        """
        Set an extended attribute on an object.

        :param key: the name of the object to set xattr to
        :type key: str
        :param xattr_name: which extended attribute to set
        :type xattr_name: str
        :param xattr_value: the value of the  extended attribute
        :type xattr_value: bytes

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: bool - True on success, otherwise raise an error
        """
        self.require_ioctx_open()
        ret = run_in_thread(self.librados.rados_setxattr,
                            (self.io, cstr(key), cstr(xattr_name),
                             c_char_p(xattr_value), c_size_t(len(xattr_value))))
        if ret < 0:
            raise make_ex(ret, "Failed to set xattr %r" % xattr_name)
        return True

    @requires(('key', str_type), ('xattr_name', str_type))
    def rm_xattr(self, key, xattr_name):
        """
        Removes an extended attribute on from an object.

        :param key: the name of the object to remove xattr from
        :type key: str
        :param xattr_name: which extended attribute to remove
        :type xattr_name: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: bool - True on success, otherwise raise an error
        """
        self.require_ioctx_open()
        ret = run_in_thread(self.librados.rados_rmxattr,
                            (self.io, cstr(key), cstr(xattr_name)))
        if ret < 0:
            raise make_ex(ret, "Failed to delete key %r xattr %r" %
                          (key, xattr_name))
        return True

    def list_objects(self):
        """
        Get ObjectIterator on rados.Ioctx object.

        :returns: ObjectIterator
        """
        self.require_ioctx_open()
        return ObjectIterator(self)

    def list_snaps(self):
        """
        Get SnapIterator on rados.Ioctx object.

        :returns: SnapIterator
        """
        self.require_ioctx_open()
        return SnapIterator(self)

    @requires(('snap_name', str_type))
    def create_snap(self, snap_name):
        """
        Create a pool-wide snapshot

        :param snap_name: the name of the snapshot
        :type snap_name: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()
        ret = run_in_thread(self.librados.rados_ioctx_snap_create,
                            (self.io, cstr(snap_name)))
        if (ret != 0):
            raise make_ex(ret, "Failed to create snap %s" % snap_name)

    @requires(('snap_name', str_type))
    def remove_snap(self, snap_name):
        """
        Removes a pool-wide snapshot

        :param snap_name: the name of the snapshot
        :type snap_name: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()
        ret = run_in_thread(self.librados.rados_ioctx_snap_remove,
                            (self.io, cstr(snap_name)))
        if (ret != 0):
            raise make_ex(ret, "Failed to remove snap %s" % snap_name)

    @requires(('snap_name', str_type))
    def lookup_snap(self, snap_name):
        """
        Get the id of a pool snapshot

        :param snap_name: the name of the snapshot to lookop
        :type snap_name: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: Snap - on success
        """
        self.require_ioctx_open()
        snap_id = c_uint64()
        ret = run_in_thread(self.librados.rados_ioctx_snap_lookup,
                            (self.io, cstr(snap_name), byref(snap_id)))
        if (ret != 0):
            raise make_ex(ret, "Failed to lookup snap %s" % snap_name)
        return Snap(self, snap_name, snap_id)

    def get_last_version(self):
        """
        Return the version of the last object read or written to.

        This exposes the internal version number of the last object read or
        written via this io context

        :returns: version of the last object used
        """
        self.require_ioctx_open()
        return run_in_thread(self.librados.rados_get_last_version, (self.io,))

    def create_write_op(self):
        """
        create write operation object.
        need call release_write_op after use
        """
        self.librados.rados_create_write_op.restype = c_void_p
        return run_in_thread(self.librados.rados_create_write_op, (None,))

    def create_read_op(self):
        """
        create read operation object.
        need call release_read_op after use
        """
        self.librados.rados_create_read_op.restype = c_void_p
        return run_in_thread(self.librados.rados_create_read_op, (None,))

    def release_write_op(self, write_op):
        """
        release memory alloc by create_write_op
        """
        self.librados.rados_release_write_op.argtypes = [c_void_p]
        run_in_thread(self.librados.rados_release_write_op, (c_void_p(write_op),))

    def release_read_op(self, read_op):
        """
        release memory alloc by create_read_op
        :para read_op: read_op object
        :type: int
        """
        self.librados.rados_release_read_op.argtypes = [c_void_p]
        run_in_thread(self.librados.rados_release_read_op, (c_void_p(read_op),))

    @requires(('write_op', int), ('keys', tuple), ('values', tuple))
    def set_omap(self, write_op, keys, values):
        """
        set keys values to write_op
        :para write_op: write_operation object
        :type write_op: int
        :para keys: a tuple of keys
        :type keys: tuple
        :para values: a tuple of values
        :type values: tuple
        """
        if len(keys) != len(values):
            raise Error("Rados(): keys and values must have the same number of items")
        key_num = len(keys)
        key_array_type = c_char_p*key_num
        key_array = key_array_type()
        key_array[:] = [cstr(key) for key in keys]

        value_array_type = c_char_p*key_num
        value_array = value_array_type()
        value_array[:] = values

        lens_array_type = c_size_t*key_num
        lens_array = lens_array_type()
        for index, value in enumerate(values):
            lens_array[index] = c_size_t(len(value))

        run_in_thread(self.librados.rados_write_op_omap_set,
                      (c_void_p(write_op), byref(key_array), byref(value_array),
                       byref(lens_array), c_int(key_num),))

    @requires(('write_op', int), ('oid', str_type), ('mtime', opt(int)), ('flags', opt(int)))
    def operate_write_op(self, write_op, oid, mtime=0, flags=0):
        """
        excute the real write operation
        :para write_op: write operation object
        :type write_op: int
        :para oid: object name
        :type oid: str
        :para mtime: the time to set the mtime to, 0 for the current time
        :type mtime: int
        :para flags: flags to apply to the entire operation
        :type flags: int
        """
        run_in_thread(self.librados.rados_write_op_operate,
                      (c_void_p(write_op), self.io, cstr(oid),
                       c_long(mtime), c_int(flags),))

    @requires(('read_op', int), ('oid', str_type), ('flag', opt(int)))
    def operate_read_op(self, read_op, oid, flag=0):
        """
        excute the real read operation
        :para read_op: read operation object
        :type read_op: int
        :para oid: object name
        :type oid: str
        :para flag: flags to apply to the entire operation
        :type flag: int
        """
        run_in_thread(self.librados.rados_read_op_operate,
                      (c_void_p(read_op), self.io, cstr(oid), c_int(flag),))

    @requires(('read_op', int), ('start_after', str_type), ('filter_prefix', str_type), ('max_return', int))
    def get_omap_vals(self, read_op, start_after, filter_prefix, max_return):
        """
        get the omap values
        :para read_op: read operation object
        :type read_op: int
        :para start_after: list keys starting after start_after
        :type start_after: str
        :para filter_prefix: list only keys beginning with filter_prefix
        :type filter_prefix: str
        :para max_return: list no more than max_return key/value pairs
        :type max_return: int
        :returns: an iterator over the the requested omap values, return value from this action
        """
        prval = c_int()
        iter_addr = c_void_p()
        run_in_thread(self.librados.rados_read_op_omap_get_vals,
                      (c_void_p(read_op), cstr(start_after),
                       cstr(filter_prefix), c_int(max_return),
                       byref(iter_addr), pointer(prval)))
        return OmapIterator(self, iter_addr), prval.value

    @requires(('read_op', int), ('start_after', str_type), ('max_return', int))
    def get_omap_keys(self, read_op, start_after, max_return):
        """
        get the omap keys
        :para read_op: read operation object
        :type read_op: int
        :para start_after: list keys starting after start_after
        :type start_after: str
        :para max_return: list no more than max_return key/value pairs
        :type max_return: int
        :returns: an iterator over the the requested omap values, return value from this action
        """
        prval = c_int()
        iter_addr = c_void_p()
        run_in_thread(self.librados.rados_read_op_omap_get_keys,
                      (c_void_p(read_op), cstr(start_after),
                       c_int(max_return), byref(iter_addr), pointer(prval)))
        return OmapIterator(self, iter_addr), prval.value

    @requires(('read_op', int), ('keys', tuple))
    def get_omap_vals_by_keys(self, read_op, keys):
        """
        get the omap values by keys
        :para read_op: read operation object
        :type read_op: int
        :para keys: input key tuple
        :type keys: tuple
        :returns: an iterator over the the requested omap values, return value from this action
        """
        prval = c_int()
        iter_addr = c_void_p()
        key_num = len(keys)
        key_array_type = c_char_p*key_num
        key_array = key_array_type()
        key_array[:] = [cstr(key) for key in keys]
        run_in_thread(self.librados.rados_read_op_omap_get_vals_by_keys,
                      (c_void_p(read_op), byref(key_array), c_int(key_num),
                       byref(iter_addr), pointer(prval)))
        return OmapIterator(self, iter_addr), prval.value

    @requires(('write_op', int), ('keys', tuple))
    def remove_omap_keys(self, write_op, keys):
        """
        remove omap keys specifiled
        :para write_op: write operation object
        :type write_op: int
        :para keys: input key tuple
        :type keys: tuple
        """
        key_num = len(keys)
        key_array_type = c_char_p*key_num
        key_array = key_array_type()
        key_array[:] = [cstr(key) for key in keys]
        run_in_thread(self.librados.rados_write_op_omap_rm_keys,
                      (c_void_p(write_op), byref(key_array), c_int(key_num)))

    @requires(('write_op', int))
    def clear_omap(self, write_op):
        """
        Remove all key/value pairs from an object
        :para write_op: write operation object
        :type write_op: int
        """
        run_in_thread(self.librados.rados_write_op_omap_clear,
                      (c_void_p(write_op),))

    @requires(('key', str_type), ('name', str_type), ('cookie', str_type), ('desc', str_type),
              ('duration', opt(int)), ('flags', int))
    def lock_exclusive(self, key, name, cookie, desc="", duration=None, flags=0):

        """
        Take an exclusive lock on an object

        :param key: name of the object
        :type key: str
        :param name: name of the lock
        :type name: str
        :param cookie: cookie of the lock
        :type cookie: str
        :param desc: description of the lock
        :type desc: str
        :param duration: duration of the lock in seconds
        :type duration: int
        :param flags: flags
        :type flags: int

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()

        ret = run_in_thread(self.librados.rados_lock_exclusive,
                            (self.io, cstr(key), cstr(name), cstr(cookie),
                             cstr(desc),
                             timeval(duration, None) if duration is None else None,
                             c_uint8(flags)))
        if ret < 0:
            raise make_ex(ret, "Ioctx.rados_lock_exclusive(%s): failed to set lock %s on %s" % (self.name, name, key))

    @requires(('key', str_type), ('name', str_type), ('cookie', str_type), ('tag', str_type),
              ('desc', str_type), ('duration', opt(int)), ('flags', int))
    def lock_shared(self, key, name, cookie, tag, desc="", duration=None, flags=0):

        """
        Take a shared lock on an object

        :param key: name of the object
        :type key: str
        :param name: name of the lock
        :type name: str
        :param cookie: cookie of the lock
        :type cookie: str
        :param tag: tag of the lock
        :type tag: str
        :param desc: description of the lock
        :type desc: str
        :param duration: duration of the lock in seconds
        :type duration: int
        :param flags: flags
        :type flags: int

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()

        ret = run_in_thread(self.librados.rados_lock_shared,
                            (self.io, cstr(key), cstr(name), cstr(cookie),
                             cstr(tag), cstr(desc),
                             timeval(duration, None) if duration is None else None,
                             c_uint8(flags)))
        if ret < 0:
            raise make_ex(ret, "Ioctx.rados_lock_exclusive(%s): failed to set lock %s on %s" % (self.name, name, key))

    @requires(('key', str_type), ('name', str_type), ('cookie', str_type))
    def unlock(self, key, name, cookie):

        """
        Release a shared or exclusive lock on an object

        :param key: name of the object
        :type key: str
        :param name: name of the lock
        :type name: str
        :param cookie: cookie of the lock
        :type cookie: str

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        """
        self.require_ioctx_open()

        ret = run_in_thread(self.librados.rados_unlock,
                            (self.io, cstr(key), cstr(name), cstr(cookie)))
        if ret < 0:
            raise make_ex(ret, "Ioctx.rados_lock_exclusive(%s): failed to set lock %s on %s" % (self.name, name, key))


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


def set_object_namespace(func):
    def retfunc(self, *args, **kwargs):
        if self.nspace is None:
            raise LogicError("Namespace not set properly in context")
        old_nspace = self.ioctx.get_namespace()
        self.ioctx.set_namespace(self.nspace)
        retval = func(self, *args, **kwargs)
        self.ioctx.set_namespace(old_nspace)
        return retval
    return retfunc


class Object(object):
    """Rados object wrapper, makes the object look like a file"""
    def __init__(self, ioctx, key, locator_key=None, nspace=None):
        self.key = key
        self.ioctx = ioctx
        self.offset = 0
        self.state = "exists"
        self.locator_key = locator_key
        self.nspace = "" if nspace is None else nspace

    def __str__(self):
        return "rados.Object(ioctx=%s,key=%s,nspace=%s,locator=%s)" % \
            (str(self.ioctx), self.key, "--default--"
             if self.nspace is "" else self.nspace, self.locator_key)

    def require_object_exists(self):
        if self.state != "exists":
            raise ObjectStateError("The object is %s" % self.state)

    @set_object_locator
    @set_object_namespace
    def read(self, length=1024 * 1024):
        self.require_object_exists()
        ret = self.ioctx.read(self.key, length, self.offset)
        self.offset += len(ret)
        return ret

    @set_object_locator
    @set_object_namespace
    def write(self, string_to_write):
        self.require_object_exists()
        ret = self.ioctx.write(self.key, string_to_write, self.offset)
        if ret == 0:
            self.offset += len(string_to_write)
        return ret

    @set_object_locator
    @set_object_namespace
    def remove(self):
        self.require_object_exists()
        self.ioctx.remove_object(self.key)
        self.state = "removed"

    @set_object_locator
    @set_object_namespace
    def stat(self):
        self.require_object_exists()
        return self.ioctx.stat(self.key)

    def seek(self, position):
        self.require_object_exists()
        self.offset = position

    @set_object_locator
    @set_object_namespace
    def get_xattr(self, xattr_name):
        self.require_object_exists()
        return self.ioctx.get_xattr(self.key, xattr_name)

    @set_object_locator
    @set_object_namespace
    def get_xattrs(self):
        self.require_object_exists()
        return self.ioctx.get_xattrs(self.key)

    @set_object_locator
    @set_object_namespace
    def set_xattr(self, xattr_name, xattr_value):
        self.require_object_exists()
        return self.ioctx.set_xattr(self.key, xattr_name, xattr_value)

    @set_object_locator
    @set_object_namespace
    def rm_xattr(self, xattr_name):
        self.require_object_exists()
        return self.ioctx.rm_xattr(self.key, xattr_name)

MONITOR_LEVELS = [
    "debug",
    "info",
    "warn", "warning",
    "err", "error",
    "sec",
    ]


class MonitorLog(object):
    """
    For watching cluster log messages.  Instantiate an object and keep
    it around while callback is periodically called.  Construct with
    'level' to monitor 'level' messages (one of MONITOR_LEVELS).
    arg will be passed to the callback.

    callback will be called with:
        arg (given to __init__)
        line (the full line, including timestamp, who, level, msg)
        who (which entity issued the log message)
        timestamp_sec (sec of a struct timespec)
        timestamp_nsec (sec of a struct timespec)
        seq (sequence number)
        level (string representing the level of the log message)
        msg (the message itself)
    callback's return value is ignored
    """

    def monitor_log_callback(self, arg, line, who, sec, nsec, seq, level, msg):
        """
        Local callback wrapper, in case we decide to do something
        """
        self.callback(arg, line, who, sec, nsec, seq, level, msg)
        return 0

    def __init__(self, cluster, level, callback, arg):
        if level not in MONITOR_LEVELS:
            raise LogicError("invalid monitor level " + level)
        if not callable(callback):
            raise LogicError("callback must be a callable function")
        self.level = level
        self.callback = callback
        self.arg = arg
        callback_factory = CFUNCTYPE(c_int,     # return type (really void)
                                     c_void_p,  # arg
                                     c_char_p,  # line
                                     c_char_p,  # who
                                     c_uint64,  # timestamp_sec
                                     c_uint64,  # timestamp_nsec
                                     c_ulong,   # seq
                                     c_char_p,  # level
                                     c_char_p)  # msg
        self.internal_callback = callback_factory(self.monitor_log_callback)

        r = run_in_thread(cluster.librados.rados_monitor_log,
                          (cluster.cluster, level, self.internal_callback, arg))
        if r:
            raise make_ex(r, 'error calling rados_monitor_log')
