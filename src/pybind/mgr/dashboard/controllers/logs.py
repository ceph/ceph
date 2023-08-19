# -*- coding: utf-8 -*-

import collections
from typing import Any, Dict

from ..controllers.service import Service
from ..security import Scope
from ..services.ceph_service import CephService
from ..services.orchestrator import OrchClient
from ..tools import NotificationQueue
from . import APIDoc, APIRouter, BaseController, CreatePermission, Endpoint, \
    EndpointDoc, ReadPermission, UIRouter

LOG_BUFFER_SIZE = 30

LOGS_SCHEMA = {
    "clog": ([str], ""),
    "audit_log": ([{
        "name": (str, ""),
        "rank": (str, ""),
        "addrs": ({
            "addrvec": ([{
                "type": (str, ""),
                "addr": (str, "IP Address"),
                "nonce": (int, ""),
            }], ""),
        }, ""),
        "stamp": (str, ""),
        "seq": (int, ""),
        "channel": (str, ""),
        "priority": (str, ""),
        "message": (str, ""),
    }], "Audit log")
}


@APIRouter('/logs', Scope.LOG)
@APIDoc("Logs Management API", "Logs")
class Logs(BaseController):
    def __init__(self):
        super().__init__()
        self._log_initialized = False
        self.log_buffer = collections.deque(maxlen=LOG_BUFFER_SIZE)
        self.audit_buffer = collections.deque(maxlen=LOG_BUFFER_SIZE)

    def append_log(self, log_struct):
        if log_struct['channel'] == 'audit':
            self.audit_buffer.appendleft(log_struct)
        else:
            self.log_buffer.appendleft(log_struct)

    def load_buffer(self, buf, channel_name):
        lines = CephService.send_command(
            'mon', 'log last', channel=channel_name, num=LOG_BUFFER_SIZE, level='debug')
        for line in lines:
            buf.appendleft(line)

    def initialize_buffers(self):
        if not self._log_initialized:
            self._log_initialized = True

            self.load_buffer(self.log_buffer, 'cluster')
            self.load_buffer(self.audit_buffer, 'audit')

            NotificationQueue.register(self.append_log, 'clog')

    @Endpoint()
    @ReadPermission
    @EndpointDoc("Display Logs Configuration",
                 responses={200: LOGS_SCHEMA})
    def all(self):
        self.initialize_buffers()
        return dict(
            clog=list(self.log_buffer),
            audit_log=list(self.audit_buffer),
        )


@UIRouter('/logs')
class LogsStatus(BaseController):
    @EndpointDoc("Check Logs Status")
    @Endpoint()
    @ReadPermission
    def status(self):
        orch = OrchClient.instance()
        status: Dict[str, Any] = {'available': True, 'message': None}

        if not orch.status()['available']:
            return status

        if (not orch.services.get('grafana')
            or not orch.services.get('loki')
                or not orch.services.get('promtail')):
            status['available'] = False
            status['message'] = """You need to configure loki and promtail along with grafana
                                to use centralized logging. Please click on the configure
                                button below to get started"""
        return status

    @Endpoint('POST')
    @EndpointDoc("Configure Centralized Logging")
    @CreatePermission
    def configure(self):
        service = Service()
        orch = OrchClient.instance()

        loki_spec = {
            'service_type': 'loki'
        }

        promtail_spec = {
            'service_type': 'promtail'
        }

        grafana_spec = {
            'service_type': 'grafana'
        }

        if not orch.services.get('grafana'):
            service.create(grafana_spec, 'grafana')

        if not orch.services.get('loki'):
            service.create(loki_spec, 'loki')

        if not orch.services.get('promtail'):
            service.create(promtail_spec, 'promtail')

        CephService.send_command('mon', 'config set', who='global', name='log_to_file',
                                 value='true')
        CephService.send_command('mon', 'config set', who='global', name='mon_cluster_log_to_file',
                                 value='true')
