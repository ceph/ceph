class DirectoryMap(object):
    def __init__(self, version, **kwargs):
        self.version = version
        self.__dict__.update(kwargs)
