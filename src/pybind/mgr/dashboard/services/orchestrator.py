# -*- coding: utf-8 -*-
from __future__ import absolute_import

from orchestrator import OrchestratorClientMixin, raise_if_exception, OrchestratorError
from .. import mgr, logger


# pylint: disable=abstract-method
class OrchClient(OrchestratorClientMixin):
    def __init__(self):
        super(OrchClient, self).__init__()
        self.set_mgr(mgr)

    def list_service_info(self, service_type):
        # type: (str) -> list
        completion = self.describe_service(service_type, None, None)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return completion.result

    def available(self):
        try:
            status, desc = super(OrchClient, self).available()
            logger.info("[ORCH] is orchestrator available: %s, %s", status, desc)
            return status
        except (RuntimeError, OrchestratorError, ImportError):
            return False

    def reload_service(self, service_type, service_ids):
        if not isinstance(service_ids, list):
            service_ids = [service_ids]

        completion_list = [self.service_action('reload', service_type,
                                               service_name, service_id)
                           for service_name, service_id in service_ids]
        self._orchestrator_wait(completion_list)
        for c in completion_list:
            raise_if_exception(c)
