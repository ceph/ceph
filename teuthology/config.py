import os
import yaml
import logging


def init_logging():
    log = logging.getLogger(__name__)
    return log

log = init_logging()


class YamlConfig(object):
    """
    A configuration object populated by parsing a yaml file, with optional
    default values.

    Note that modifying the _defaults attribute of an instance can potentially
    yield confusing results; if you need to do modify defaults, use the class
    variable or create a subclass.
    """
    _defaults = dict()

    def __init__(self, yaml_path=None):
        self.yaml_path = yaml_path
        if self.yaml_path:
            self.load()
        else:
            self._conf = dict()

    def load(self):
        if os.path.exists(self.yaml_path):
            self._conf = yaml.safe_load(file(self.yaml_path))
        else:
            log.debug("%s not found", self.yaml_path)
            self._conf = dict()

    def update(self, in_dict):
        """
        Update an existing configuration using dict.update()

        :param in_dict: The dict to use to update
        """
        self._conf.update(in_dict)

    @classmethod
    def from_dict(cls, in_dict):
        """
        Build a config object from a dict.

        :param in_dict: The dict to use
        :returns:       The config object
        """
        conf_obj = cls()
        conf_obj._conf = in_dict
        return conf_obj

    def to_dict(self):
        """
        :returns: A shallow copy of the configuration as a dict
        """
        return dict(self._conf)

    @classmethod
    def from_str(cls, in_str):
        """
        Build a config object from a string or yaml stream.

        :param in_str: The stream or string
        :returns:      The config object
        """
        conf_obj = cls()
        conf_obj._conf = yaml.safe_load(in_str)
        return conf_obj

    def to_str(self):
        """
        :returns: str(self)
        """
        return str(self)

    def __str__(self):
        return yaml.safe_dump(self._conf, default_flow_style=False).strip()

    def __getitem__(self, name):
        return self._conf.__getitem__(name)

    def __getattr__(self, name):
        return self._conf.get(name, self._defaults.get(name))

    def __setattr__(self, name, value):
        if name.endswith('_conf') or name in ('yaml_path'):
            object.__setattr__(self, name, value)
        else:
            self._conf[name] = value

    def __delattr__(self, name):
        del self._conf[name]


class TeuthologyConfig(YamlConfig):
    """
    This class is intended to unify teuthology's many configuration files and
    objects. Currently it serves as a convenient interface to
    ~/.teuthology.yaml and nothing else.
    """
    yaml_path = os.path.join(os.path.expanduser('~/.teuthology.yaml'))
    _defaults = {
        'archive_base': '/var/lib/teuthworker/archive',
        'automated_scheduling': False,
        'ceph_git_base_url': 'https://github.com/ceph/',
        'lock_server': 'http://teuthology.front.sepia.ceph.com/locker/lock',
        'max_job_time': 259200,  # 3 days
        'results_server': 'http://paddles.front.sepia.ceph.com/',
        'src_base_path': os.path.expanduser('~/src'),
        'verify_host_keys': True,
        'watchdog_interval': 600,
    }

    def __init__(self):
        super(TeuthologyConfig, self).__init__(self.yaml_path)


class JobConfig(YamlConfig):
    pass


config = TeuthologyConfig()
