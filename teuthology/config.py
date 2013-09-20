import os
import yaml
import logging

CONF_FILE = os.path.join(os.environ['HOME'], '.teuthology.yaml')

log = logging.getLogger(__name__)


class _Config(object):
    """
    This class is intended to unify teuthology's many configuration files and
    objects. Currently it serves as a convenient interface to
    ~/.teuthology.yaml and nothing else.
    """
    def __init__(self):
        if os.path.exists(CONF_FILE):
            self.__conf = yaml.safe_load(file(CONF_FILE))
        else:
            log.debug("%s not found", CONF_FILE)
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

    # This takes care of any and all of the rest.
    # If the parameter is defined, return it. Otherwise return None.
    def __getattr__(self, name):
        return self.__conf.get(name)

config = _Config()
