# -*- coding: utf-8 -*-

import json

from .. import mgr
from ..controllers.rbd_mirroring import get_daemons_and_pools
from ..exceptions import ViewCacheNoDataException
from ..security import Permission, Scope
from ..services import progress
from ..tools import TaskManager
from . import APIDoc, APIRouter, BaseController, Endpoint, EndpointDoc

SUMMARY_SCHEMA = {
    "health_status": (str, ""),
    "mgr_id": (str, ""),
    "mgr_host": (str, ""),
    "have_mon_connection": (str, ""),
    "executing_tasks": ([str], ""),
    "finished_tasks": ([{
        "name": (str, ""),
        "metadata": ({
            "pool": (int, ""),
        }, ""),
        "begin_time": (str, ""),
        "end_time": (str, ""),
        "duration": (int, ""),
        "progress": (int, ""),
        "success": (bool, ""),
        "ret_value": (str, ""),
        "exception": (str, ""),
    }], ""),
    "version": (str, ""),
    "rbd_mirroring": ({
        "warnings": (int, ""),
        "errors": (int, "")
    }, "")
}


@APIRouter('/summary')
@APIDoc("Get Ceph Summary Details", "Summary")
class Summary(BaseController):
    def _health_status(self):
        health_data = mgr.get("health")
        return json.loads(health_data["json"])['status']

    def _rbd_mirroring(self):
        try:
            _, data = get_daemons_and_pools()
        except ViewCacheNoDataException:  # pragma: no cover
            return {}  # pragma: no cover

        daemons = data.get('daemons', [])
        pools = data.get('pools', {})

        warnings = 0
        errors = 0
        for daemon in daemons:
            if daemon['health_color'] == 'error':  # pragma: no cover
                errors += 1
            elif daemon['health_color'] == 'warning':  # pragma: no cover
                warnings += 1
        for _, pool in pools.items():
            if pool['health_color'] == 'error':  # pragma: no cover
                errors += 1
            elif pool['health_color'] == 'warning':  # pragma: no cover
                warnings += 1
        return {'warnings': warnings, 'errors': errors}

    def _task_permissions(self, name):  # pragma: no cover
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
        # type: () -> str
        services = mgr.get('mgr_map')['services']
        return services['dashboard'] if 'dashboard' in services else ''

    @Endpoint()
    @EndpointDoc("Display Summary",
                 responses={200: SUMMARY_SCHEMA})
    def __call__(self):
        exe_t, fin_t = TaskManager.list_serializable()
        executing_tasks = [task for task in exe_t if self._task_permissions(task['name'])]
        finished_tasks = [task for task in fin_t if self._task_permissions(task['name'])]

        e, f = progress.get_progress_tasks()
        executing_tasks.extend(e)
        finished_tasks.extend(f)

        executing_tasks.sort(key=lambda t: t['begin_time'], reverse=True)
        finished_tasks.sort(key=lambda t: t['end_time'], reverse=True)

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
