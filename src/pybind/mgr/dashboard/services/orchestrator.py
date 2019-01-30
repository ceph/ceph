# -*- coding: utf-8 -*-
from __future__ import absolute_import

import time

from .. import mgr, logger
from ..settings import Settings


class NoOrchesrtatorConfiguredException(Exception):
    pass


class OrchClient(object):
    _instance = None

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = OrchClient()
        return cls._instance

    def _call(self, method, *args, **kwargs):
        if not Settings.ORCHESTRATOR_BACKEND:
            raise NoOrchesrtatorConfiguredException()
        return mgr.remote(Settings.ORCHESTRATOR_BACKEND, method, *args,
                          **kwargs)

    def _wait(self, completions):
        while not self._call("wait", completions):
            if any(c.should_wait for c in completions):
                time.sleep(5)
            else:
                break

    def list_service_info(self, service_type):
        completion = self._call("describe_service", service_type, None, None)
        self._wait([completion])
        return completion.result

    def available(self):
        if not Settings.ORCHESTRATOR_BACKEND:
            return False
        status, desc = self._call("available")
        logger.info("[ORCH] is orchestrator available: %s, %s", status, desc)
        return status

    def reload_service(self, service_type, service_ids):
        if not isinstance(service_ids, list):
            service_ids = [service_ids]

        completion_list = [self._call("update_stateless_service", service_type,
                                      service_id, None)
                           for service_id in service_ids]
        self._wait(completion_list)
