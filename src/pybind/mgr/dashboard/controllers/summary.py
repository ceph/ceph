# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

import cherrypy

from . import AuthRequired, ApiController, BaseController
from .. import logger, mgr
from ..controllers.rbd_mirroring import get_daemons_and_pools
from ..services.ceph_service import CephService
from ..tools import TaskManager


@ApiController('summary')
@AuthRequired()
class Summary(BaseController):
    def _rbd_pool_data(self):
        pool_names = [pool['pool_name'] for pool in CephService.get_pool_list('rbd')]
        return sorted(pool_names)

    def _health_status(self):
        health_data = mgr.get("health")
        return json.loads(health_data["json"])['status']

    def _filesystems(self):
        fsmap = mgr.get("fs_map")
        return [
            {
                "id": f['id'],
                "name": f['mdsmap']['fs_name']
            }
            for f in fsmap['filesystems']
        ]

    def _rbd_mirroring(self):
        _, data = get_daemons_and_pools()

        if isinstance(data, Exception):
            logger.exception("Failed to get rbd-mirror daemons and pools")
            raise type(data)(str(data))
        else:
            daemons = data.get('daemons', [])
            pools = data.get('pools', {})

        warnings = 0
        errors = 0
        for daemon in daemons:
            if daemon['health_color'] == 'error':
                errors += 1
            elif daemon['health_color'] == 'warning':
                warnings += 1
        for _, pool in pools.items():
            if pool['health_color'] == 'error':
                errors += 1
            elif pool['health_color'] == 'warning':
                warnings += 1
        return {'warnings': warnings, 'errors': errors}

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def __call__(self):
        executing_t, finished_t = TaskManager.list_serializable()
        return {
            'rbd_pools': self._rbd_pool_data(),
            'health_status': self._health_status(),
            'filesystems': self._filesystems(),
            'rbd_mirroring': self._rbd_mirroring(),
            'mgr_id': mgr.get_mgr_id(),
            'have_mon_connection': mgr.have_mon_connection(),
            'executing_tasks': executing_t,
            'finished_tasks': finished_t
        }
