# -*- coding: utf-8 -*-
from __future__ import absolute_import

import datetime

from . import ApiController, RESTController, UpdatePermission
from .. import mgr, logger
from ..security import Scope
from ..services.ceph_service import CephService
from ..services.exception import handle_send_command_error
from ..tools import str_to_bool

WEEK_DAYS_TIME = 7 * 24 * 60 * 60


@ApiController('/osd', Scope.OSD)
class Osd(RESTController):
    def list(self):
        osds = self.get_osd_map()
        devices = mgr.get('devices')
        # Extending by osd stats information
        for s in mgr.get('osd_stats')['osd_stats']:
            osds[str(s['osd'])].update({'osd_stats': s})
        # Extending by osd node information
        nodes = mgr.get('osd_map_tree')['nodes']
        osd_tree = [(str(o['id']), o) for o in nodes if o['id'] >= 0]
        for o in osd_tree:
            osds[o[0]].update({'tree': o[1]})
        # Extending by osd parent node information
        hosts = [(h['name'], h) for h in nodes if h['id'] < 0]
        for h in hosts:
            for o_id in h[1]['children']:
                if o_id >= 0:
                    osds[str(o_id)]['host'] = h[1]
        # Extending by osd histogram data
        for o_id in osds:
            o = osds[o_id]
            o['stats'] = {}
            o['stats_history'] = {}
            o['devices'] = {}
            life_expectancy_weeks = None
            # Get device info
            devs = self.parse_osd_devices(o_id, devices)
            for dev_info in devs:
                dev_id = dev_info.get('dev_id')
                if life_expectancy_weeks is None:
                    life_expectancy_weeks = \
                        self.get_device_life_expectancy_weeks(dev_info)
                    if life_expectancy_weeks is not None:
                        o['devices'][dev_id] = \
                            {'life_expectancy_weeks': life_expectancy_weeks}
                else:
                    next_dev_life_expectancy_weeks = \
                        self.get_device_life_expectancy_weeks(dev_info)
                    if life_expectancy_weeks > next_dev_life_expectancy_weeks and next_dev_life_expectancy_weeks is not None:
                        life_expectancy_weeks = next_dev_life_expectancy_weeks
                    if next_dev_life_expectancy_weeks is not None:
                        o['devices'][dev_id] = \
                            {'life_expectancy_weeks':
                                 next_dev_life_expectancy_weeks}
            osd_spec = str(o['osd'])
            for s in ['osd.op_w', 'osd.op_in_bytes', 'osd.op_r', 'osd.op_out_bytes']:
                prop = s.split('.')[1]
                o['stats'][prop] = CephService.get_rate('osd', osd_spec, s)
                o['stats_history'][prop] = CephService.get_rates('osd', osd_spec, s)
            # Device life expectancy weeks over 6 weeks, the light is 'green'
            if life_expectancy_weeks is None:
                o['stats']['lf_s'] = 'unknown'
            elif life_expectancy_weeks >= 6:
                o['stats']['lf_s'] = 'green'
            elif 6 > life_expectancy_weeks >= 4:
                o['stats']['lf_s'] = 'yellow'
            elif life_expectancy_weeks < 4:
                o['stats']['lf_s'] = 'red'
            # Gauge stats
            for s in ['osd.numpg', 'osd.stat_bytes', 'osd.stat_bytes_used']:
                o['stats'][s.split('.')[1]] = mgr.get_latest('osd', osd_spec, s)
        return list(osds.values())

    def get_osd_map(self):
        osds = {}
        for osd in mgr.get('osd_map')['osds']:
            osd['id'] = osd['osd']
            osds[str(osd['id'])] = osd
        return osds

    def parse_osd_devices(self, osd_id, devices):
        result = []
        if str(osd_id).isdigit():
            osd_name = 'osd.%s' % osd_id
        else:
            osd_name = osd_id
        if devices:
            for dev in devices.get('devices', []):
                if osd_name in dev.get('daemons', []):
                    dev_id = dev.get('devid')
                    dev_info = {
                        'dev_id': dev_id,
                        'life_expectancy_max':
                            dev.get('life_expectancy_max', None),
                        'life_expectancy_min':
                            dev.get('life_expectancy_min', None)
                    }
                    result.append(dev_info)
        return result

    def get_device_life_expectancy_weeks(self, dev_info):
        life_expectancy_weeks = None
        try:
            if dev_info:
                i_to_day = None
                i_from_day = None
                from_day = dev_info.get('life_expectancy_min', '')
                to_day = dev_info.get('life_expectancy_max', '')
                if to_day:
                    i_to_day = int(datetime.datetime.strptime(to_day[:10], '%Y-%m-%d').strftime('%s'))
                if from_day:
                    i_from_day = int(datetime.datetime.strptime(from_day[:10], '%Y-%m-%d').strftime('%s'))
                if i_to_day and i_from_day:
                    life_expectancy_weeks = int((i_to_day - i_from_day) // WEEK_DAYS_TIME)
        except Exception as e:
            logger.error('failed to parse device %s life expectancy weeks, %s' % (dev_info, str(e)))
        return life_expectancy_weeks

    @handle_send_command_error('osd')
    def get(self, svc_id):
        histogram = CephService.send_command('osd', srv_spec=svc_id, prefix='perf histogram dump')
        return {
            'osd_map': self.get_osd_map()[svc_id],
            'osd_metadata': mgr.get_metadata('osd', svc_id),
            'histogram': histogram,
        }

    @RESTController.Resource('POST', query_params=['deep'])
    @UpdatePermission
    def scrub(self, svc_id, deep=False):
        api_scrub = "osd deep-scrub" if str_to_bool(deep) else "osd scrub"
        CephService.send_command("mon", api_scrub, who=svc_id)


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
        The `recovery_deletes` and `sortbitwise` flags cannot be unset.
        `purged_snapshots` cannot even be set. It is therefore required to at
        least include those three flags for a successful operation.
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
