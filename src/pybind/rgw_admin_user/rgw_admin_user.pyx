"""
Wrap librgw_admin_user -- borrows liberally from rgw.pyx
"""

from cpython cimport PyObject, ref, exc, array
from libc.stdint cimport *
from libcpp cimport bool
from libc.stdlib cimport malloc, realloc, free
from libc.string cimport const_char, const_uchar

from collections import namedtuple
import errno

cdef extern from "Python.h":
    # These are in cpython/string.pxd, but use "object" types instead of
    # PyObject*, which invokes assumptions in cpython that we need to
    # legitimately break to implement zero-copy string buffers in Image.read().
    # This is valid use of the Python API and documented as a special case.
    PyObject *PyBytes_FromStringAndSize(char *v, Py_ssize_t len) except NULL
    char* PyBytes_AsString(PyObject *string) except NULL
    int _PyBytes_Resize(PyObject **string, Py_ssize_t newsize) except -1
    void PyEval_InitThreads()

cdef extern from "rgw/librgw_admin_user.h" nogil:
    ctypedef void* librgw_admin_user_t

    # setup/teardown
    int librgw_admin_user_create(librgw_admin_user_t *rgw_admin_user, int argc,
    	char **argv)
    void librgw_admin_user_shutdown(librgw_admin_user_t rgw_admin_user)

    ctypedef const_char specialChar

    cdef struct rgw_user_info:
        const_char *uid
        const_char *display_name
        const_char *access_key
        const_char *secret_key
        const_char *email
        const_char *caps
        const_char *access
        bool admin
        bool system

    # get interface version
    void rgw_admin_user_version(int *major, int *minor, int *extra)

    # create a new rgw user
    int rgw_admin_create_user(librgw_admin_user_t rgw_admin_user,
    	const_char *uid, const_char *display_name,  const_char *access_key,
	const_char *secret_key, const_char *email, const_char *caps,
	const_char *access, bool admin, bool system)

    # get rgw user info
    int rgw_admin_user_info(librgw_admin_user_t rgw_admin_user, const_char *uid,
    rgw_user_info *user_info)

    # free user_info structure
    void rgw_admin_user_release_info(rgw_user_info* user_info)

UserInfo = namedtuple('UserInfo', ["uid", "display_name", "access_key", "secret_key", "email", "caps", "access", "admin", "system"])
    
class Error(Exception):
    pass

class OSError(Error):
    """ `OSError` class, derived from `Error` """
    def __init__(self, errno, strerror):
        self.errno = errno
        self.strerror = strerror

    def __str__(self):
        return '[Errno {0}] {1}'.format(self.errno, self.strerror)

class PermissionError(OSError):
    pass

class ObjectNotFound(OSError):
    pass

class NoData(Error):
    pass

class ObjectExists(Error):
    pass

class IOError(OSError):
    pass

class NoSpace(Error):
    pass

class InvalidValue(Error):
    pass

class OperationNotSupported(Error):
    pass

class IncompleteWriteError(Error):
    pass

class LibCephFSStateError(Error):
    pass

class WouldBlock(Error):
    pass

class OutOfRange(Error):
    pass

IF UNAME_SYSNAME == "FreeBSD":
    cdef errno_to_exception =  {
        errno.EPERM      : PermissionError,
        errno.ENOENT     : ObjectNotFound,
        errno.EIO        : IOError,
        errno.ENOSPC     : NoSpace,
        errno.EEXIST     : ObjectExists,
        errno.ENOATTR    : NoData,
        errno.EINVAL     : InvalidValue,
        errno.EOPNOTSUPP : OperationNotSupported,
        errno.ERANGE     : OutOfRange,
        errno.EWOULDBLOCK: WouldBlock,
    }
ELSE:
    cdef errno_to_exception =  {
        errno.EPERM      : PermissionError,
        errno.ENOENT     : ObjectNotFound,
        errno.EIO        : IOError,
        errno.ENOSPC     : NoSpace,
        errno.EEXIST     : ObjectExists,
        errno.ENODATA    : NoData,
        errno.EINVAL     : InvalidValue,
        errno.EOPNOTSUPP : OperationNotSupported,
        errno.ERANGE     : OutOfRange,
        errno.EWOULDBLOCK: WouldBlock,
    }

def cstr(val, name, encoding="utf-8", opt=False):
    """
    Create a byte string from a Python string

    :param basestring val: Python string
    :param str name: Name of the string parameter, for exceptions
    :param str encoding: Encoding to use
    :param bool opt: If True, None is allowed
    :rtype: bytes
    :raises: :class:`InvalidArgument`
    """
    if opt and val is None:
        return None
    if isinstance(val, bytes):
        return val
    elif isinstance(val, unicode):
        return val.encode(encoding)
    else:
        raise TypeError('%s must be a string' % name)


cdef make_ex(ret, msg):
    """
    Translate a return code into an exception.

    :param ret: the return code
    :type ret: int
    :param msg: the error message to use
    :type msg: str
    :returns: a subclass of :class:`Error`
    """
    ret = abs(ret)
    if ret in errno_to_exception:
        return errno_to_exception[ret](ret, msg)
    else:
        return Error(msg + (": error code %d" % ret))


cdef class LibRGWAdminUser(object):
    """librgw_admin_user python wrapper"""

    cdef public object state
    cdef librgw_admin_user_t handle

    def __cinit__(self):
        prog_name = "rgw_admin_user.pyx"
        cdef char* argv[1]
        argv[0] = prog_name

        PyEval_InitThreads()
        self.state = ""
        ret = librgw_admin_user_create(&self.handle, 1, argv)
        if ret == 0:
            self.state = "initialized"
        else:
            raise make_ex(ret, "error calling librgw_admin_user_create")

    def shutdown(self):
        """
        Uninitialize/cleanup the library handle.
        """
        if self.state in ["initialized"]:
            with nogil:
                librgw_admin_user_shutdown(self.handle)
            self.state = "shutdown"

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.shutdown()

    def __dealloc__(self):
        self.shutdown()

    def version(self):
        """
        Get the version number of the ``librgw_admin_user`` C library.

        :returns: a tuple of ``(major, minor, extra)`` components of the
                  librgw_admin_user version
        """
        cdef:
            int major = 0
            int minor = 0
            int extra = 0
        with nogil:
            rgw_admin_user_version(&major, &minor, &extra)
        return (major, minor, extra)

    def create_user(self, uid, display_name, access_key, secret_key, email,
                    caps, access, admin = False, system = False):
        """
        Create a user in the RGW cluster
        """

        #nogil discipline
        uid = cstr(uid, 'uid')
        display_name = cstr(display_name, '_display_name')
        access_key = cstr(access_key, 'access_key')
        secret_key = cstr(secret_key, 'secret_key')
        email = cstr(email, 'email')
        caps = cstr(caps, 'caps')
        access = cstr(access, 'access')

        cdef:
            const_char *_uid = uid
            const_char *_display_name = display_name
            const_char *_access_key = access_key
            const_char *_secret_key = secret_key
            const_char *_email = email
            const_char *_caps = caps
            const_char *_access = access
            bint _admin = admin
            bint _system = system

        with nogil:
            ret = rgw_admin_create_user(self.handle, _uid, _display_name,
                                        _access_key, _secret_key, _email,
                                        _caps, _access, _admin, _system)
        if ret < 0:
            raise make_ex(ret, "error in user_create '%s'" % (uid))
        return

    def user_info(self, uid):
        """
        Get user information for `uid` in the RGW cluster
        """

        #nogil discipline
        uid = cstr(uid, 'uid')

        cdef:
            const_char *_uid = uid
            rgw_user_info user_info

        with nogil:
            ret = rgw_admin_user_info(self.handle, _uid, &user_info)

        info = UserInfo(uid=user_info.uid,
                        display_name=user_info.display_name,
                        access_key=user_info.access_key,
                        secret_key=user_info.secret_key,
                        email=user_info.email,
                        caps=user_info.caps,
                        access=user_info.access,
                        admin=user_info.admin,
                        system=user_info.system)

        with nogil:
            rgw_admin_user_release_info(&user_info)

        return info
