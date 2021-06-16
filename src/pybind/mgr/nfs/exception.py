import errno


class NFSException(Exception):
    def __init__(self, errno, err_msg):
        super(NFSException, self).__init__(errno, err_msg)
        self.errno = errno
        self.err_msg = err_msg

    def __str__(self):
        return self.err_msg


class NFSInvalidOperation(NFSException):
    def __init__(self, err_msg):
        super(NFSInvalidOperation, self).__init__(-errno.EINVAL, err_msg)


class NFSObjectNotFound(NFSException):
    def __init__(self, err_msg):
        super(NFSObjectNotFound, self).__init__(-errno.ENOENT, err_msg)


class FSNotFound(NFSObjectNotFound):
    def __init__(self, fs_name):
        super(FSNotFound, self).__init__(f'filesystem {fs_name} not found')


class ClusterNotFound(NFSObjectNotFound):
    def __init__(self):
        super(ClusterNotFound, self).__init__('cluster does not exist')
