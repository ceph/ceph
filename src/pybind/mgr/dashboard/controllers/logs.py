# -*- coding: utf-8 -*-
from __future__ import absolute_import

import collections

from . import ApiController, Endpoint, BaseController, ReadPermission
from ..security import Scope
from ..services.ceph_service import CephService
from ..tools import NotificationQueue


LOG_BUFFER_SIZE = 30


@ApiController('/logs', Scope.LOG)
class Logs(BaseController):
    def __init__(self):
        super(Logs, self).__init__()
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
            'mon', 'log last', channel=channel_name, num=LOG_BUFFER_SIZE)
        for l in lines:
            buf.appendleft(l)

    def initialize_buffers(self):
        if not self._log_initialized:
            self._log_initialized = True

            self.load_buffer(self.log_buffer, 'cluster')
            self.load_buffer(self.audit_buffer, 'audit')

            NotificationQueue.register(self.append_log, 'clog')

    @Endpoint()
    @ReadPermission
    def all(self):
        self.initialize_buffers()
        return dict(
            clog=list(self.log_buffer),
            audit_log=list(self.audit_buffer),
        )
