class Error(Exception):
    """ `Error` class, derived from `Exception` """
    def __init__(self, message, errno=None):
        super(Exception, self).__init__(message)
        self.errno = errno

    def __str__(self):
        msg = super(Exception, self).__str__()
        if self.errno is None:
            return msg
        return '[errno {0}] {1}'.format(self.errno, msg)


class InvalidArgumentError(Error):
    pass


class OSError(Error):
    """ `OSError` class, derived from `Error` """
    pass


class InterruptedOrTimeoutError(OSError):
    """ `InterruptedOrTimeoutError` class, derived from `OSError` """
    pass


class PermissionError(OSError):
    """ `PermissionError` class, derived from `OSError` """
    pass


class PermissionDeniedError(OSError):
    """ deal with EACCES related. """
    pass


class ObjectNotFound(OSError):
    """ `ObjectNotFound` class, derived from `OSError` """
    pass


class NoData(OSError):
    """ `NoData` class, derived from `OSError` """
    pass


class ObjectExists(OSError):
    """ `ObjectExists` class, derived from `OSError` """
    pass


class ObjectBusy(OSError):
    """ `ObjectBusy` class, derived from `IOError` """
    pass


class IOError(OSError):
    """ `ObjectBusy` class, derived from `OSError` """
    pass


class NoSpace(OSError):
    """ `NoSpace` class, derived from `OSError` """
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


class TimedOut(OSError):
    """ `TimedOut` class, derived from `OSError` """
    pass
