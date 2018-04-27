# -*- coding: utf-8 -*-
from __future__ import absolute_import

import collections
import json

from . import ApiController, Endpoint, BaseController
from .. import mgr
from ..security import Permission, Scope
from ..services.ceph_service import CephService
from ..tools import NotificationQueue


LOG_BUFFER_SIZE = 30


@ApiController('/dashboard')
class Dashboard(BaseController):
    def __init__(self):
        super(Dashboard, self).__init__()

        self._log_initialized = False

        self.log_buffer = collections.deque(maxlen=LOG_BUFFER_SIZE)
        self.audit_buffer = collections.deque(maxlen=LOG_BUFFER_SIZE)

    def append_log(self, log_struct):
        if log_struct['channel'] == "audit":
            self.audit_buffer.appendleft(log_struct)
        else:
            self.log_buffer.appendleft(log_struct)

    def load_buffer(self, buf, channel_name):
        lines = CephService.send_command('mon', 'log last', channel=channel_name,
                                         num=LOG_BUFFER_SIZE)
        for l in lines:
            buf.appendleft(l)

    @Endpoint()
    def health(self):
        if not self._log_initialized:
            self._log_initialized = True

            self.load_buffer(self.log_buffer, "cluster")
            self.load_buffer(self.audit_buffer, "audit")

            NotificationQueue.register(self.append_log, 'clog')

        result = {
            "health": self.health_data(),
        }

        if self._has_permissions(Permission.READ, Scope.LOG):
            result['clog'] = list(self.log_buffer)
            result['audit_log'] = list(self.audit_buffer)

        if self._has_permissions(Permission.READ, Scope.MONITOR):
            result['mon_status'] = self.mon_status()

        if self._has_permissions(Permission.READ, Scope.CEPHFS):
            result['fs_map'] = mgr.get('fs_map')

        if self._has_permissions(Permission.READ, Scope.OSD):
            osd_map = self.osd_map()
            # Not needed, skip the effort of transmitting this to UI
            del osd_map['pg_temp']
            result['osd_map'] = osd_map

        if self._has_permissions(Permission.READ, Scope.MANAGER):
            result['mgr_map'] = mgr.get("mgr_map")

        if self._has_permissions(Permission.READ, Scope.POOL):
            pools = CephService.get_pool_list_with_stats()
            result['pools'] = pools

            df = mgr.get("df")
            df['stats']['total_objects'] = sum(
                [p['stats']['objects'] for p in df['pools']])
            result['df'] = df

        return result

    def mon_status(self):
        mon_status_data = mgr.get("mon_status")
        return json.loads(mon_status_data['json'])

    def osd_map(self):
        osd_map = mgr.get("osd_map")

        assert osd_map is not None

        osd_map['tree'] = mgr.get("osd_map_tree")
        osd_map['crush'] = mgr.get("osd_map_crush")
        osd_map['crush_map_text'] = mgr.get("osd_map_crush_map_text")
        osd_map['osd_metadata'] = mgr.get("osd_metadata")

        return osd_map

    def health_data(self):
        health_data = mgr.get("health")
        health = json.loads(health_data['json'])

        # Transform the `checks` dict into a list for the convenience
        # of rendering from javascript.
        checks = []
        for k, v in health['checks'].items():
            v['type'] = k
            checks.append(v)

        checks = sorted(checks, key=lambda c: c['severity'])

        health['checks'] = checks

        return health
