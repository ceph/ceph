import fcntl


class FileLock(object):
    def __init__(self, filename, noop=False):
        self.filename = filename
        self.file = None
        self.noop = noop

    def __enter__(self):
        if not self.noop:
            assert self.file is None
            self.file = open(self.filename, 'w')
            fcntl.lockf(self.file, fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.noop:
            assert self.file is not None
            fcntl.lockf(self.file, fcntl.LOCK_UN)
            self.file.close()
            self.file = None
