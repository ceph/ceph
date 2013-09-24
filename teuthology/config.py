import os
import yaml
import logging

log = logging.getLogger(__name__)


class Config(object):
    """
    This class is intended to unify teuthology's many configuration files and
    objects. Currently it serves as a convenient interface to
    ~/.teuthology.yaml and nothing else.
    """
    teuthology_yaml = os.path.join(os.environ['HOME'], '.teuthology.yaml')

    def __init__(self):
        self.load_files()

    def load_files(self):
        if os.path.exists(self.teuthology_yaml):
            self.__conf = yaml.safe_load(file(self.teuthology_yaml))
        else:
            log.debug("%s not found", self.teuthology_yaml)
            self.__conf = {}

    # This property declaration exists mainly as an example; it is not
    # necessary unless you want to, say, define a set method and/or a
    # docstring.
    @property
    def lock_server(self):
        """
        The URL to your lock server. For example, Inktank uses:

            http://teuthology.front.sepia.ceph.com/locker/lock
        """
        return self.__conf.get('lock_server')

    @property
    def ceph_git_base_url(self):
        """
        The base URL to use for ceph-related git repositories.

        Defaults to https://github.com/ceph/
        """
        base_url = self.__conf.get('ceph_git_base_url')
        base_url = base_url or "https://github.com/ceph/"
        return base_url

    @property
    def verify_host_keys(self):
        """
        Whether or not we should verify ssh host keys.

        Defaults to True
        """
        return self.__conf.get('verify_host_keys', True)

    # This takes care of any and all of the rest.
    # If the parameter is defined, return it. Otherwise return None.
    def __getattr__(self, name):
        return self.__conf.get(name)

config = Config()
