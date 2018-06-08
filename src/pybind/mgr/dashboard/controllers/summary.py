# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

from .. import mgr
from . import AuthRequired, ApiController, Endpoint, BaseController
from ..controllers.rbd_mirroring import get_daemons_and_pools
from ..tools import ViewCacheNoDataException
from ..tools import TaskManager


@ApiController('/summary')
@AuthRequired()
class Summary(BaseController):
    def _health_status(self):
        health_data = mgr.get("health")
        return json.loads(health_data["json"])['status']

    def _rbd_mirroring(self):
        try:
            _, data = get_daemons_and_pools()
        except ViewCacheNoDataException:
            return {}

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

    @Endpoint()
    def __call__(self):
        executing_t, finished_t = TaskManager.list_serializable()
        return {
            'health_status': self._health_status(),
            'rbd_mirroring': self._rbd_mirroring(),
            'mgr_id': mgr.get_mgr_id(),
            'have_mon_connection': mgr.have_mon_connection(),
            'executing_tasks': executing_t,
            'finished_tasks': finished_t,
            'version': mgr.version
        }
