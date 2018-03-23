# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

from mgr_module import CommandResult

from . import ApiController, AuthRequired, RESTController
from .. import logger, mgr
from ..services.ceph_service import CephService


@ApiController('osd')
@AuthRequired()
class Osd(RESTController):
    def list(self):
        osds = self.get_osd_map()
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
            osd_spec = str(o['osd'])
            for s in ['osd.op_w', 'osd.op_in_bytes', 'osd.op_r', 'osd.op_out_bytes']:
                prop = s.split('.')[1]
                o['stats'][prop] = CephService.get_rate('osd', osd_spec, s)
                o['stats_history'][prop] = CephService.get_rates('osd', osd_spec, s)
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

    def get(self, svc_id):
        result = CommandResult('')
        mgr.send_command(result, 'osd', svc_id,
                         json.dumps({
                             'prefix': 'perf histogram dump',
                         }),
                         '')
        r, outb, outs = result.wait()
        if r != 0:
            logger.warning('Failed to load histogram for OSD %s', svc_id)
            logger.debug(outs)
            histogram = outs
        else:
            histogram = json.loads(outb)
        return {
            'osd_map': self.get_osd_map()[svc_id],
            'osd_metadata': mgr.get_metadata('osd', svc_id),
            'histogram': histogram,
        }
