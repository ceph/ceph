# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

from . import ApiController, Endpoint, BaseController
from .. import mgr
from ..security import Permission, Scope
from ..controllers.rbd_mirroring import get_daemons_and_pools
from ..exceptions import ViewCacheNoDataException
from ..tools import TaskManager


@ApiController('/summary')
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

    def _task_permissions(self, name):
        result = True
        if name == 'pool/create':
            result = self._has_permissions(Permission.CREATE, Scope.POOL)
        elif name == 'pool/edit':
            result = self._has_permissions(Permission.UPDATE, Scope.POOL)
        elif name == 'pool/delete':
            result = self._has_permissions(Permission.DELETE, Scope.POOL)
        elif name in [
                'rbd/create', 'rbd/copy', 'rbd/snap/create',
                'rbd/clone', 'rbd/trash/restore']:
            result = self._has_permissions(Permission.CREATE, Scope.RBD_IMAGE)
        elif name in [
                'rbd/edit', 'rbd/snap/edit', 'rbd/flatten',
                'rbd/snap/rollback']:
            result = self._has_permissions(Permission.UPDATE, Scope.RBD_IMAGE)
        elif name in [
                'rbd/delete', 'rbd/snap/delete', 'rbd/trash/move',
                'rbd/trash/remove', 'rbd/trash/purge']:
            result = self._has_permissions(Permission.DELETE, Scope.RBD_IMAGE)
        return result

    def _get_host(self):
        mgr_map = mgr.get('mgr_map')
        services = mgr_map['services']
        return services['dashboard']

    @Endpoint()
    def __call__(self):
        exe_t, fin_t = TaskManager.list_serializable()
        executing_tasks = [task for task in exe_t if self._task_permissions(task['name'])]
        finished_tasks = [task for task in fin_t if self._task_permissions(task['name'])]

        result = {
            'health_status': self._health_status(),
            'mgr_id': mgr.get_mgr_id(),
            'mgr_host': self._get_host(),
            'have_mon_connection': mgr.have_mon_connection(),
            'executing_tasks': executing_tasks,
            'finished_tasks': finished_tasks,
            'version': mgr.version
        }
        if self._has_permissions(Permission.READ, Scope.RBD_MIRRORING):
            result['rbd_mirroring'] = self._rbd_mirroring()
        return result
