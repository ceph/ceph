from __future__ import absolute_import

import socket

from . import MetricsAgent
from ...common.db import DB_API
from ...models.metrics.dp import Ceph_OSD


class CephOSD_Agent(MetricsAgent):
    measurement = 'ceph_osd'
    # counter types
    PERFCOUNTER_LONGRUNAVG = 4
    PERFCOUNTER_COUNTER = 8
    PERFCOUNTER_HISTOGRAM = 0x10
    PERFCOUNTER_TYPE_MASK = ~3

    def _stattype_to_str(self, stattype):
        typeonly = stattype & self.PERFCOUNTER_TYPE_MASK
        if typeonly == 0:
            return 'gauge'
        if typeonly == self.PERFCOUNTER_LONGRUNAVG:
            # this lie matches the DaemonState decoding: only val, no counts
            return 'counter'
        if typeonly == self.PERFCOUNTER_COUNTER:
            return 'counter'
        if typeonly == self.PERFCOUNTER_HISTOGRAM:
            return 'histogram'
        return ''

    def _collect_data(self):
        # process data and save to 'self.data'
        obj_api = DB_API(self._ceph_context)
        perf_data = obj_api.get_all_perf_counters()
        if not perf_data and not isinstance(perf_data, dict):
            self._logger.error("unable to get all perf counters")
            return
        cluster_id = obj_api.get_cluster_id()
        for n_name, i_perf in perf_data.iteritems():
            if not n_name[0:3].lower() == 'osd':
                continue
            service_id = n_name[4:]
            d_osd = Ceph_OSD()
            stat_bytes = 0
            stat_bytes_used = 0
            d_osd.tags['cluster_id'] = cluster_id
            d_osd.tags['osd_id'] = n_name[4:]
            d_osd.fields['agenthost'] = socket.gethostname()
            d_osd.tags['agenthost_domain_id'] = \
                '%s_%s' % (cluster_id, d_osd.fields['agenthost'])
            d_osd.tags['host_domain_id'] = \
                '%s_%s' % (cluster_id,
                           obj_api.get_osd_hostname(d_osd.tags['osd_id']))
            for i_key, i_val in i_perf.iteritems():
                if i_key[:4] == 'osd.':
                    key_name = i_key[4:]
                else:
                    key_name = i_key
                if self._stattype_to_str(i_val['type']) == 'counter':
                    value = obj_api.get_rate('osd', service_id, i_key)
                else:
                    value = obj_api.get_latest('osd', service_id, i_key)
                if key_name == 'stat_bytes':
                    stat_bytes = value
                elif key_name == 'stat_bytes_used':
                    stat_bytes_used = value
                else:
                    d_osd.fields[key_name] = value

            if stat_bytes and stat_bytes_used:
                d_osd.fields['stat_bytes_used_percentage'] = \
                    round(float(stat_bytes_used) / float(stat_bytes) * 100, 4)
            else:
                d_osd.fields['stat_bytes_used_percentage'] = 0.0000
            self.data.append(d_osd)
