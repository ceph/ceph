#!/usr/bin/env python
import os
import yaml
import logging

CONF_FILE = os.path.join(os.environ['HOME'], '.teuthology.yaml')

log = logging.getLogger(__name__)


class _Config(object):
    def __init__(self):
        self.__conf = {}
        if not os.path.exists(CONF_FILE):
            log.debug("%s not found", CONF_FILE)
            return

        with file(CONF_FILE) as f:
            conf_obj = yaml.safe_load_all(f)
            for item in conf_obj:
                self.__conf.update(item)

    @property
    def lock_server(self):
        return self.__conf.get('lock_server')

    @property
    def queue_host(self):
        return self.__conf.get('queue_host')

    @property
    def queue_port(self):
        return self.__conf.get('queue_port')

    @property
    def sentry_dsn(self):
        return self.__conf.get('sentry_dsn')

config = _Config()
