class VolumeException(Exception):
    def __init__(self, errno=None, errmsg=None):
        assert errno or errmsg

        self.errno = errno
        self.errsmg = errmsg

        log.info(f'{self.__class__.__name__}: {str(self)}')

    def to_tuple(self):
        return self.errno, "", self.errmsg

    def __str__(self):
        return f'errno: {self.errno}, errmsg: {self.errmsg}'


class MetadataMgrException(VolumeException):
    pass


class IndexException(VolumeException):
    pass


class OpSmException(VolumeException):
    pass


class EvictionError(VolumeException):
    pass

class NotImplementedException(Exception):
    pass

class ClusterTimeout(Exception):
    """
    Exception indicating that we timed out trying to talk to the Ceph cluster,
    either to the mons, or to any individual daemon that the mons indicate ought
    to be up but isn't responding to us.
    """
    pass

class ClusterError(Exception):
    """
    Exception indicating that the cluster returned an error to a command that
    we thought should be successful based on our last knowledge of the cluster
    state.
    """
    def __init__(self, action, result_code, result_str):
        self._action = action
        self._result_code = result_code
        self._result_str = result_str

    def __str__(self):
        return "Error {0} (\"{1}\") while {2}".format(
            self._result_code, self._result_str, self._action)
