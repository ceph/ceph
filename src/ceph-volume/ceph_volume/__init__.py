from collections import namedtuple

conf = namedtuple('config', ['ceph', 'ceph_volume'])

conf.ceph_volume = {
    'verbosity': 'info',
}

__version__ = '0.0.1'
