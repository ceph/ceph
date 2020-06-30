# -*- coding: utf-8 -*-
from __future__ import absolute_import
import json
import logging
import time

from ceph.deployment.drive_group import DriveGroupSpec, DriveGroupValidationError
from mgr_util import get_most_recent_rate

from . import ApiController, RESTController, Endpoint, Task
from . import CreatePermission, ReadPermission, UpdatePermission, DeletePermission
from .orchestrator import raise_if_no_orchestrator
from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope
from ..services.ceph_service import CephService, SendCommandError
from ..services.exception import handle_send_command_error, handle_orchestrator_error
from ..services.orchestrator import OrchClient
from ..tools import str_to_bool
try:
    from typing import Dict, List, Any, Union  # noqa: F401 pylint: disable=unused-import
except ImportError:
    pass  # For typing only


logger = logging.getLogger('controllers.osd')


def osd_task(name, metadata, wait_for=2.0):
    return Task("osd/{}".format(name), metadata, wait_for)


@ApiController('/osd', Scope.OSD)
class Osd(RESTController):
    def list(self):
        osds = self.get_osd_map()

        # Extending by osd stats information
        for stat in mgr.get('osd_stats')['osd_stats']:
            if stat['osd'] in osds:
                osds[stat['osd']]['osd_stats'] = stat

        # Extending by osd node information
        nodes = mgr.get('osd_map_tree')['nodes']
        for node in nodes:
            if node['type'] == 'osd' and node['id'] in osds:
                osds[node['id']]['tree'] = node

        # Extending by osd parent node information
        for host in [n for n in nodes if n['type'] == 'host']:
            for osd_id in host['children']:
                if osd_id >= 0 and osd_id in osds:
                    osds[osd_id]['host'] = host

        # Extending by osd histogram data
        for osd_id, osd in osds.items():
            osd['stats'] = {}
            osd['stats_history'] = {}
            osd_spec = str(osd_id)
            if 'osd' not in osd:
                continue
            for stat in ['osd.op_w', 'osd.op_in_bytes', 'osd.op_r', 'osd.op_out_bytes']:
                prop = stat.split('.')[1]
                rates = CephService.get_rates('osd', osd_spec, stat)
                osd['stats'][prop] = get_most_recent_rate(rates)
                osd['stats_history'][prop] = rates
            # Gauge stats
            for stat in ['osd.numpg', 'osd.stat_bytes', 'osd.stat_bytes_used']:
                osd['stats'][stat.split('.')[1]] = mgr.get_latest('osd', osd_spec, stat)

        return list(osds.values())

    @staticmethod
    def get_osd_map(svc_id=None):
        # type: (Union[int, None]) -> Dict[int, Union[dict, Any]]
        def add_id(osd):
            osd['id'] = osd['osd']
            return osd

        resp = {
            osd['osd']: add_id(osd)
            for osd in mgr.get('osd_map')['osds'] if svc_id is None or osd['osd'] == int(svc_id)
        }
        return resp if svc_id is None else resp[int(svc_id)]

    @staticmethod
    def _get_smart_data(osd_id):
        # type: (str) -> dict
        """Returns S.M.A.R.T data for the given OSD ID."""
        return CephService.get_smart_data_by_daemon('osd', osd_id)

    @RESTController.Resource('GET')
    def smart(self, svc_id):
        # type: (str) -> dict
        return self._get_smart_data(svc_id)

    @handle_send_command_error('osd')
    def get(self, svc_id):
        """
        Returns collected data about an OSD.

        :return: Returns the requested data. The `histogram` key may contain a
                 string with an error that occurred if the OSD is down.
        """
        try:
            histogram = CephService.send_command(
                'osd', srv_spec=svc_id, prefix='perf histogram dump')
        except SendCommandError as e:
            if 'osd down' in str(e):
                histogram = str(e)
            else:
                raise

        return {
            'osd_map': self.get_osd_map(svc_id),
            'osd_metadata': mgr.get_metadata('osd', svc_id),
            'histogram': histogram,
        }

    def set(self, svc_id, device_class):
        old_device_class = CephService.send_command('mon', 'osd crush get-device-class',
                                                    ids=[svc_id])
        old_device_class = old_device_class[0]['device_class']
        if old_device_class != device_class:
            CephService.send_command('mon', 'osd crush rm-device-class',
                                     ids=[svc_id])
            if device_class:
                CephService.send_command('mon', 'osd crush set-device-class', **{
                    'class': device_class,
                    'ids': [svc_id]
                })

    def _check_delete(self, osd_ids):
        # type: (List[str]) -> Dict[str, Any]
        """
        Check if it's safe to remove OSD(s).

        :param osd_ids: list of OSD IDs
        :return: a dictionary contains the following attributes:
            `safe`: bool, indicate if it's safe to remove OSDs.
            `message`: str, help message if it's not safe to remove OSDs.
        """
        _ = osd_ids
        health_data = mgr.get('health')  # type: ignore
        health = json.loads(health_data['json'])
        checks = health['checks'].keys()
        unsafe_checks = set(['OSD_FULL', 'OSD_BACKFILLFULL', 'OSD_NEARFULL'])
        failed_checks = checks & unsafe_checks
        msg = 'Removing OSD(s) is not recommended because of these failed health check(s): {}.'.\
            format(', '.join(failed_checks)) if failed_checks else ''
        return {
            'safe': not bool(failed_checks),
            'message': msg
        }

    @DeletePermission
    @raise_if_no_orchestrator
    @handle_orchestrator_error('osd')
    @osd_task('delete', {'svc_id': '{svc_id}'})
    def delete(self, svc_id, preserve_id=None, force=None):
        replace = False
        check = False
        try:
            if preserve_id is not None:
                replace = str_to_bool(preserve_id)
            if force is not None:
                check = not str_to_bool(force)
        except ValueError:
            raise DashboardException(
                component='osd', http_status_code=400, msg='Invalid parameter(s)')

        orch = OrchClient.instance()
        if check:
            logger.info('Check for removing osd.%s...', svc_id)
            check = self._check_delete([svc_id])
            if not check['safe']:
                logger.error('Unable to remove osd.%s: %s', svc_id, check['message'])
                raise DashboardException(component='osd', msg=check['message'])

        logger.info('Start removing osd.%s (replace: %s)...', svc_id, replace)
        orch.osds.remove([svc_id], replace)
        while True:
            removal_osds = orch.osds.removing_status()
            logger.info('Current removing OSDs %s', removal_osds)
            pending = [osd for osd in removal_osds if osd.osd_id == svc_id]
            if not pending:
                break
            logger.info('Wait until osd.%s is removed...', svc_id)
            time.sleep(60)

    @RESTController.Resource('POST', query_params=['deep'])
    @UpdatePermission
    def scrub(self, svc_id, deep=False):
        api_scrub = "osd deep-scrub" if str_to_bool(deep) else "osd scrub"
        CephService.send_command("mon", api_scrub, who=svc_id)

    @RESTController.Resource('POST')
    def mark_out(self, svc_id):
        CephService.send_command('mon', 'osd out', ids=[svc_id])

    @RESTController.Resource('POST')
    def mark_in(self, svc_id):
        CephService.send_command('mon', 'osd in', ids=[svc_id])

    @RESTController.Resource('POST')
    def mark_down(self, svc_id):
        CephService.send_command('mon', 'osd down', ids=[svc_id])

    @RESTController.Resource('POST')
    def reweight(self, svc_id, weight):
        """
        Reweights the OSD temporarily.

        Note that ‘ceph osd reweight’ is not a persistent setting. When an OSD
        gets marked out, the osd weight will be set to 0. When it gets marked
        in again, the weight will be changed to 1.

        Because of this ‘ceph osd reweight’ is a temporary solution. You should
        only use it to keep your cluster running while you’re ordering more
        hardware.

        - Craig Lewis (http://lists.ceph.com/pipermail/ceph-users-ceph.com/2014-June/040967.html)
        """
        CephService.send_command(
            'mon',
            'osd reweight',
            id=int(svc_id),
            weight=float(weight))

    @RESTController.Resource('POST')
    def mark_lost(self, svc_id):
        """
        Note: osd must be marked `down` before marking lost.
        """
        CephService.send_command(
            'mon',
            'osd lost',
            id=int(svc_id),
            yes_i_really_mean_it=True)

    def _create_bare(self, data):
        """Create a OSD container that has no associated device.

        :param data: contain attributes to create a bare OSD.
        :    `uuid`: will be set automatically if the OSD starts up
        :    `svc_id`: the ID is only used if a valid uuid is given.
        """
        try:
            uuid = data['uuid']
            svc_id = int(data['svc_id'])
        except (KeyError, ValueError) as e:
            raise DashboardException(e, component='osd', http_status_code=400)

        result = CephService.send_command(
            'mon', 'osd create', id=svc_id, uuid=uuid)
        return {
            'result': result,
            'svc_id': svc_id,
            'uuid': uuid,
        }

    @raise_if_no_orchestrator
    @handle_orchestrator_error('osd')
    def _create_with_drive_groups(self, drive_groups):
        """Create OSDs with DriveGroups."""
        orch = OrchClient.instance()
        try:
            dg_specs = [DriveGroupSpec.from_json(dg) for dg in drive_groups]
            orch.osds.create(dg_specs)
        except (ValueError, TypeError, DriveGroupValidationError) as e:
            raise DashboardException(e, component='osd')

    @CreatePermission
    @osd_task('create', {'tracking_id': '{tracking_id}'})
    def create(self, method, data, tracking_id):  # pylint: disable=W0622
        if method == 'bare':
            return self._create_bare(data)
        if method == 'drive_groups':
            return self._create_with_drive_groups(data)
        raise DashboardException(
            component='osd', http_status_code=400, msg='Unknown method: {}'.format(method))

    @RESTController.Resource('POST')
    def purge(self, svc_id):
        """
        Note: osd must be marked `down` before removal.
        """
        CephService.send_command('mon', 'osd purge-actual', id=int(svc_id),
                                 yes_i_really_mean_it=True)

    @RESTController.Resource('POST')
    def destroy(self, svc_id):
        """
        Mark osd as being destroyed. Keeps the ID intact (allowing reuse), but
        removes cephx keys, config-key data and lockbox keys, rendering data
        permanently unreadable.

        The osd must be marked down before being destroyed.
        """
        CephService.send_command(
            'mon', 'osd destroy-actual', id=int(svc_id), yes_i_really_mean_it=True)

    @Endpoint('GET', query_params=['ids'])
    @ReadPermission
    def safe_to_destroy(self, ids):
        """
        :type ids: int|[int]
        """

        ids = json.loads(ids)
        if isinstance(ids, list):
            ids = list(map(str, ids))
        else:
            ids = [str(ids)]

        try:
            result = CephService.send_command(
                'mon', 'osd safe-to-destroy', ids=ids, target=('mgr', ''))
            result['is_safe_to_destroy'] = set(result['safe_to_destroy']) == set(map(int, ids))
            return result

        except SendCommandError as e:
            return {
                'message': str(e),
                'is_safe_to_destroy': False,
            }

    @Endpoint('GET', query_params=['svc_ids'])
    @ReadPermission
    @raise_if_no_orchestrator
    @handle_orchestrator_error('osd')
    def safe_to_delete(self, svc_ids):
        """
        :type ids: int|[int]
        """
        check = self._check_delete(svc_ids)
        return {
            'is_safe_to_delete': check.get('safe', False),
            'message': check.get('message', '')
        }

    @RESTController.Resource('GET')
    def devices(self, svc_id):
        # (str) -> dict
        return CephService.send_command('mon', 'device ls-by-daemon', who='osd.{}'.format(svc_id))


@ApiController('/osd/flags', Scope.OSD)
class OsdFlagsController(RESTController):
    @staticmethod
    def _osd_flags():
        enabled_flags = mgr.get('osd_map')['flags_set']
        if 'pauserd' in enabled_flags and 'pausewr' in enabled_flags:
            # 'pause' is set by calling `ceph osd set pause` and unset by
            # calling `set osd unset pause`, but `ceph osd dump | jq '.flags'`
            # will contain 'pauserd,pausewr' if pause is set.
            # Let's pretend to the API that 'pause' is in fact a proper flag.
            enabled_flags = list(
                set(enabled_flags) - {'pauserd', 'pausewr'} | {'pause'})
        return sorted(enabled_flags)

    def list(self):
        return self._osd_flags()

    def bulk_set(self, flags):
        """
        The `recovery_deletes`, `sortbitwise` and `pglog_hardlimit` flags cannot be unset.
        `purged_snapshots` cannot even be set. It is therefore required to at
        least include those four flags for a successful operation.
        """
        assert isinstance(flags, list)

        enabled_flags = set(self._osd_flags())
        data = set(flags)
        added = data - enabled_flags
        removed = enabled_flags - data
        for flag in added:
            CephService.send_command('mon', 'osd set', '', key=flag)
        for flag in removed:
            CephService.send_command('mon', 'osd unset', '', key=flag)
        logger.info('Changed OSD flags: added=%s removed=%s', added, removed)

        return sorted(enabled_flags - removed | added)
