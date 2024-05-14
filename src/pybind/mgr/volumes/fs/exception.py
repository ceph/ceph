class VolumeException(Exception):
    def __init__(self, error_code, error_message):
        self.errno = error_code
        self.error_str = error_message

    def to_tuple(self):
        return self.errno, "", self.error_str

    def __str__(self):
        return "{0} ({1})".format(self.errno, self.error_str)

class MetadataMgrException(Exception):
    def __init__(self, error_code, error_message):
        self.errno = error_code
        self.error_str = error_message

    def __str__(self):
        return "{0} ({1})".format(self.errno, self.error_str)

class IndexException(Exception):
    def __init__(self, error_code, error_message):
        self.errno = error_code
        self.error_str = error_message

    def __str__(self):
        return "{0} ({1})".format(self.errno, self.error_str)

class OpSmException(Exception):
    def __init__(self, error_code, error_message):
        self.errno = error_code
        self.error_str = error_message

    def __str__(self):
        return "{0} ({1})".format(self.errno, self.error_str)

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

class EvictionError(Exception):
    pass
