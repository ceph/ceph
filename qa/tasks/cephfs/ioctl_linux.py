# This is a copy of the linux.py in python-ioctl project: https://github.com/olavmrk/python-ioctl

import ctypes
import fcntl
import platform

__all__ = (
    'IOC',
    'IO',
    'IOR',
    'IOW',
    'IOWR',
)

class _IoctlGeneric(object):
    _IOC_NRBITS = 8
    _IOC_TYPEBITS = 8
    _IOC_SIZEBITS = 14
    _IOC_DIRBITS = 2
    _IOC_NONE = 0
    _IOC_WRITE = 1
    _IOC_READ = 2

    @classmethod
    def ioc(cls, direction, request_type, request_nr, size):
        _IOC_NRSHIFT = 0
        _IOC_TYPESHIFT = _IOC_NRSHIFT + cls._IOC_NRBITS
        _IOC_SIZESHIFT = _IOC_TYPESHIFT + cls._IOC_TYPEBITS
        _IOC_DIRSHIFT = _IOC_SIZESHIFT + cls._IOC_SIZEBITS
        return (
            (direction << _IOC_DIRSHIFT) |
            (request_type << _IOC_TYPESHIFT) |
            (request_nr << _IOC_NRSHIFT) |
            (size << _IOC_SIZESHIFT)
            )

class _IoctlAlpha(_IoctlGeneric):
    _IOC_NRBITS = 8
    _IOC_TYPEBITS = 8
    _IOC_SIZEBITS = 13
    _IOC_DIRBITS = 3
    _IOC_NONE = 1
    _IOC_READ = 2
    _IOC_WRITE = 4

class _IoctlMips(_IoctlGeneric):
    _IOC_SIZEBITS = 13
    _IOC_DIRBITS = 3
    _IOC_NONE = 1
    _IOC_READ = 2
    _IOC_WRITE = 4

class _IoctlParisc(_IoctlGeneric):
    _IOC_NONE = 0
    _IOC_WRITE = 2
    _IOC_READ = 1

class _IoctlPowerPC(_IoctlGeneric):
    _IOC_SIZEBITS = 13
    _IOC_DIRBITS = 3
    _IOC_NONE = 1
    _IOC_READ = 2
    _IOC_WRITE = 4

class _IoctlSparc(_IoctlGeneric):
    _IOC_NRBITS = 8
    _IOC_TYPEBITS = 8
    _IOC_SIZEBITS = 13
    _IOC_DIRBITS = 3
    _IOC_NONE = 1
    _IOC_READ = 2
    _IOC_WRITE = 4

_machine_ioctl_map = {
    'alpha': _IoctlAlpha,
    'mips': _IoctlMips,
    'mips64': _IoctlMips,
    'parisc': _IoctlParisc,
    'parisc64': _IoctlParisc,
    'ppc': _IoctlPowerPC,
    'ppcle': _IoctlPowerPC,
    'ppc64': _IoctlPowerPC,
    'ppc64le': _IoctlPowerPC,
    'sparc': _IoctlSparc,
    'sparc64': _IoctlSparc,
}

def _machine_ioctl_calculator():
    machine = platform.machine()
    return _machine_ioctl_map.get(machine, _IoctlGeneric)

def _ioc_type_size(size):
    if isinstance(size, type) and issubclass(size, ctypes._SimpleCData):
        return ctypes.sizeof(size)
    elif isinstance(size, int):
        return size
    else:
        raise TypeError('Invalid type for size: {size_type}'.format(size_type=size.__class__.__name__))

def _ioc_request_type(request_type):
    if isinstance(request_type, int):
        return request_type
    if isinstance(request_type, str):
        if len(request_type) > 1:
            raise ValueError('request_type string too long.')
        elif len(request_type) == 0:
            raise ValueError('request_type cannot be an empty string.')
        return ord(request_type)
    else:
        raise ValueError('request_type must be an integer or a string, but was: {request_type_type}'
                         .format(request_type_type=request_type.__class__.__name__))

def IOC(direction, request_type, request_nr, size):
    """ Python implementation of the ``_IOC(...)`` macro from Linux.

    This is a portable implementation of the ``_IOC(...)`` macro from Linux.
    It takes a set of parameters, and calculates a ioctl request number based on those parameters.

    :param direction: Direction of data transfer in this ioctl. This can be one of:

                      * ``None``: No data transfer.
                      * ``'r'``: Read-only (input) data.
                      * ``'w'``: Write-only (output) data.
                      * ``'rw'``: Read-write (input and output) data.
    :param request_type: The ioctl request type. This can be specified as either a string ``'R'`` or an integer ``123``.
    :param request_nr: The ioctl request number. This is an integer.
    :param size: The number of data bytes transferred in this ioctl.
    :return: The calculated ioctl request number.
    """

    calc = _machine_ioctl_calculator()

    if direction is None:
        direction = calc._IOC_NONE
    elif direction == 'r':
        direction = calc._IOC_READ
    elif direction == 'w':
        direction = calc._IOC_WRITE
    elif direction == 'rw':
        direction = calc._IOC_READ | calc._IOC_WRITE
    else:
        raise ValueError('direction must be None, \'r\', \'w\' or \'rw\'.')

    request_type = _ioc_request_type(request_type)
    return calc.ioc(direction, request_type, request_nr, size)

def IO(request_type, request_nr):
    """ Python implementation of the ``_IO(...)`` macro from Linux.

    This is a portable implementation of the ``_IO(...)`` macro from Linux.
    The ``_IO(...)`` macro calculates a ioctl request number for ioctl request that do not transfer any data.

    :param request_type: The ioctl request type. This can be specified as either a string ``'R'`` or an integer ``123``.
    :param request_nr: The ioctl request number. This is an integer.
    :return: The calculated ioctl request number.
    """

    calc = _machine_ioctl_calculator()
    request_type = _ioc_request_type(request_type)
    return calc.ioc(calc._IOC_NONE, request_type, request_nr, 0)

def IOR(request_type, request_nr, size):
    """ Python implementation of the ``_IOR(...)`` macro from Linux.

    This is a portable implementation of the ``_IOR(...)`` macro from Linux.
    The `_IOR(...)`` macro calculates a ioctl request number for ioctl request that only pass read-only (input) data.

    :param request_type: The ioctl request type. This can be specified as either a string ``'R'`` or an integer ``123``.
    :param request_nr: The ioctl request number. This is an integer.
    :param size: The size of the associated data. This can either be an integer or a ctypes type.
    :return: The calculated ioctl request number.
    """

    calc = _machine_ioctl_calculator()
    request_type = _ioc_request_type(request_type)
    size = _ioc_type_size(size)
    return calc.ioc(calc._IOC_READ, request_type, request_nr, size)

def IOW(request_type, request_nr, size):
    """ Python implementation of the ``_IOW(...)`` macro from Linux.

    This is a portable implementation of the ``_IOW(...)`` macro from Linux.
    The ``_IOW(...)`` macro calculates a ioctl request number for ioctl request that only pass write-only (output) data.

    :param request_type: The ioctl request type. This can be specified as either a string ``'R'`` or an integer ``123``.
    :param request_nr: The ioctl request number. This is an integer.
    :param size: The size of the associated data. This can either be an integer or a ctypes type.
    :return: The calculated ioctl request number.
    """

    calc = _machine_ioctl_calculator()
    request_type = _ioc_request_type(request_type)
    size = _ioc_type_size(size)
    return calc.ioc(calc._IOC_WRITE, request_type, request_nr, size)

def IOWR(request_type, request_nr, size):
    """ Python implementation of the ``_IOWR(...)`` macro from Linux.

    This is a portable implementation of the ``_IOWR(...)`` macro from Linux.
    The ``_IOWR(...)`` macro calculates a ioctl request number for ioctl request that use the data for both reading (input) and writing (output).

    :param request_type: The ioctl request type. This can be specified as either a string ``'R'`` or an integer ``123``.
    :param request_nr: The ioctl request number. This is an integer.
    :param size: The size of the associated data. This can either be an integer or a ctypes type.
    :return: The calculated ioctl request number.
    """

    calc = _machine_ioctl_calculator()
    request_type = _ioc_request_type(request_type)
    size = _ioc_type_size(size)
    return calc.ioc(calc._IOC_READ|calc._IOC_WRITE, request_type, request_nr, size)
