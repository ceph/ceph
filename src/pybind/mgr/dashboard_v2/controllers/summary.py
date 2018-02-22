# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

import cherrypy

from ..tools import AuthRequired, ApiController, BaseController
from ..services.ceph_service import CephService


@ApiController('summary')
@AuthRequired()
class Summary(BaseController):
    def _rbd_pool_data(self):
        pool_names = [pool['pool_name'] for pool in CephService.get_pool_list('rbd')]
        return sorted(pool_names)

    def _health_status(self):
        health_data = self.mgr.get("health")
        return json.loads(health_data["json"])['status']

    def _filesystems(self):
        fsmap = self.mgr.get("fs_map")
        return [
            {
                "id": f['id'],
                "name": f['mdsmap']['fs_name']
            }
            for f in fsmap['filesystems']
        ]

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def default(self):
        return {
            'rbd_pools': self._rbd_pool_data(),
            'health_status': self._health_status(),
            'filesystems': self._filesystems(),
            'mgr_id': self.mgr.get_mgr_id(),
            'have_mon_connection': self.mgr.have_mon_connection()
        }
