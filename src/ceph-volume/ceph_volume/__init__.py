from collections import namedtuple


class UnloadedConfig(object):
    """
    This class is used as the default value for conf.ceph so that if
    a configuration file is not successfully loaded then it will give
    a nice error message when values from the config are used.
    """
    def __getattr__(self, *a):
        raise RuntimeError("No valid ceph configuration file was loaded.")

conf = namedtuple('config', ['ceph', 'cluster', 'verbosity', 'path', 'log_path'])
conf.ceph = UnloadedConfig()

__version__ = "1.0.0"
