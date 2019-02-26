# -*- coding: utf-8 -*-
from __future__ import absolute_import

import time

from .. import mgr, logger


class NoOrchestratorConfiguredException(Exception):
    pass


class OrchClient(object):
    _instance = None

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = OrchClient()
        return cls._instance

    def _call(self, method, *args, **kwargs):
        _backend = mgr.get_module_option_ex("orchestrator_cli", "orchestrator")
        if not _backend:
            raise NoOrchestratorConfiguredException()
        return mgr.remote(_backend, method, *args, **kwargs)

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
        _backend = mgr.get_module_option_ex("orchestrator_cli", "orchestrator")
        if not _backend:
            return False
        status, desc = self._call("available")
        logger.info("[ORCH] is orchestrator available: %s, %s", status, desc)
        return status

    def reload_service(self, service_type, service_ids):
        if not isinstance(service_ids, list):
            service_ids = [service_ids]

        completion_list = [self._call("service_action", 'reload', service_type,
                                      service_name, service_id)
                           for service_name, service_id in service_ids]
        self._wait(completion_list)
