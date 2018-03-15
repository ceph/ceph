# -*- coding: utf-8 -*-
from __future__ import absolute_import

import collections
import json

import cherrypy
from mgr_module import CommandResult

from .. import mgr
from ..services.ceph_service import CephService
from ..tools import ApiController, AuthRequired, BaseController, NotificationQueue


LOG_BUFFER_SIZE = 30


@ApiController('dashboard')
@AuthRequired()
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
        result = CommandResult("")
        mgr.send_command(result, "mon", "", json.dumps({
            "prefix": "log last",
            "format": "json",
            "channel": channel_name,
            "num": LOG_BUFFER_SIZE
        }), "")
        r, outb, outs = result.wait()
        if r != 0:
            # Oh well. We won't let this stop us though.
            self.log.error("Error fetching log history (r={0}, \"{1}\")".format(
                r, outs))
        else:
            try:
                lines = json.loads(outb)
            except ValueError:
                self.log.error("Error decoding log history")
            else:
                for l in lines:
                    buf.appendleft(l)

    # pylint: disable=R0914
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def health(self):
        if not self._log_initialized:
            self._log_initialized = True

            self.load_buffer(self.log_buffer, "cluster")
            self.load_buffer(self.audit_buffer, "audit")

            NotificationQueue.register(self.append_log, 'clog')

        # Fuse osdmap with pg_summary to get description of pools
        # including their PG states

        osd_map = self.osd_map()

        pools = CephService.get_pool_list_with_stats()

        # Not needed, skip the effort of transmitting this
        # to UI
        del osd_map['pg_temp']

        df = mgr.get("df")
        df['stats']['total_objects'] = sum(
            [p['stats']['objects'] for p in df['pools']])

        return {
            "health": self.health_data(),
            "mon_status": self.mon_status(),
            "fs_map": mgr.get('fs_map'),
            "osd_map": osd_map,
            "clog": list(self.log_buffer),
            "audit_log": list(self.audit_buffer),
            "pools": pools,
            "mgr_map": mgr.get("mgr_map"),
            "df": df
        }

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
