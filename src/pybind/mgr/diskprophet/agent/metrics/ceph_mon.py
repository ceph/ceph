from __future__ import absolute_import

import socket

from . import MetricsAgent
from ...common.db import DB_API
from ...models.metrics.dp import Ceph_MON


class CephMon_Agent(MetricsAgent):
    measurement = 'ceph_mon'

    def _collect_data(self):
        # process data and save to 'self.data'
        obj_api = DB_API(self._ceph_context)
        perf_data = obj_api.get_all_perf_counters()
        if not perf_data and not isinstance(perf_data, dict):
            self._logger.error("unable to get all perf counters")
            return
        cluster_id = obj_api.get_cluster_id()
        for n_name, i_perf in perf_data.iteritems():
            if not n_name[0:3].lower() == 'mon':
                continue
            d_mon = Ceph_MON()
            d_mon.tags['cluster_id'] = cluster_id
            d_mon.tags['mon_id'] = n_name[4:]
            d_mon.tags['agenthost'] = socket.gethostname()
            d_mon.tags['agenthost_domain_id']= \
                "%s_%s" % (cluster_id, d_mon.tags['agenthost'])
            d_mon.fields['num_sessions'] = \
                i_perf.get("mon.num_sessions", {}).get('value', 0)
            d_mon.fields['session_add'] = \
                i_perf.get('mon.session_add', {}).get('value', 0)
            d_mon.fields['session_rm'] = \
                i_perf.get('mon.session_rm', {}).get('value', 0)
            d_mon.fields['session_trim'] = \
                i_perf.get('mon.session_trim', {}).get('value', 0)
            d_mon.fields['num_elections'] = \
                i_perf.get('mon.num_elections', {}).get('value', 0)
            d_mon.fields['election_call'] = \
                i_perf.get('mon.election_call', {}).get('value', 0)
            d_mon.fields['election_win'] = \
                i_perf.get('mon.election_win', {}).get('value', 0)
            d_mon.fields['election_lose'] = \
                i_perf.get('election_lose', {}).get('value', 0)
            self.data.append(d_mon)
