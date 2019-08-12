# -*- coding: utf-8 -*-
from __future__ import absolute_import
from . import ApiController, RESTController, UpdatePermission
from .. import mgr, logger
from ..security import Scope
from ..services.ceph_service import CephService, SendCommandError
from ..services.exception import handle_send_command_error
from ..tools import str_to_bool
try:
    from typing import Dict, List, Any, Union  # pylint: disable=unused-import
except ImportError:
    pass  # For typing only


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
                osd['stats'][prop] = CephService.get_rate('osd', osd_spec, stat)
                osd['stats_history'][prop] = CephService.get_rates('osd', osd_spec, stat)
            # Gauge stats
            for stat in ['osd.numpg', 'osd.stat_bytes', 'osd.stat_bytes_used']:
                osd['stats'][stat.split('.')[1]] = mgr.get_latest('osd', osd_spec, stat)

        return list(osds.values())

    @staticmethod
    def get_osd_map(svc_id=None):
        # type: (Union[int, None]) -> Dict[int, Union[Dict[str, Any], Any]]
        def add_id(osd):
            osd['id'] = osd['osd']
            return osd
        resp = {
            osd['osd']: add_id(osd)
            for osd in mgr.get('osd_map')['osds'] if svc_id is None or osd['osd'] == int(svc_id)
        }
        return resp if svc_id is None else resp[int(svc_id)]

    @handle_send_command_error('osd')
    def get(self, svc_id):
        """
        Returns collected data about an OSD.

        :return: Returns the requested data. The `histogram` key man contain a
                 string with an error that occurred when the OSD is down.
        """
        try:
            histogram = CephService.send_command('osd', srv_spec=svc_id,
                                                 prefix='perf histogram dump')
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

    def create(self, uuid=None, svc_id=None):
        """
        :param uuid: Will be set automatically if the OSD starts up.
        :param id: The ID is only used if a valid uuid is given.
        :return:
        """
        result = CephService.send_command(
            'mon', 'osd create', id=int(svc_id), uuid=uuid)
        return {
            'result': result,
            'svc_id': int(svc_id),
            'uuid': uuid,
        }

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

    @RESTController.Resource('GET')
    def safe_to_destroy(self, svc_id):
        """
        :type svc_id: int|[int]
        """
        if not isinstance(svc_id, list):
            svc_id = [svc_id]
        svc_id = list(map(str, svc_id))
        try:
            result = CephService.send_command(
                'mon', 'osd safe-to-destroy', ids=svc_id, target=('mgr', ''))
            result['is_safe_to_destroy'] = set(result['safe_to_destroy']) == set(map(int, svc_id))
            return result

        except SendCommandError as e:
            return {
                'message': str(e),
                'is_safe_to_destroy': False,
            }


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
