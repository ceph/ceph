from __future__ import absolute_import

import socket

from . import MetricsAgent
from ...common.db import DB_API
from ...models.metrics.dp import Ceph_Pool


class CephPool_Agent(MetricsAgent):
    measurement = 'ceph_pool'

    def _collect_data(self):
        # process data and save to 'self.data'
        obj_api = DB_API(self._ceph_context)
        df_data = obj_api.get('df')
        cluster_id = obj_api.get_cluster_id()
        for pool in df_data.get("pools", []):
            d_pool = Ceph_Pool()
            p_name = pool.get("name")
            p_id = pool.get("id")
            d_pool.tags['cluster_id'] = cluster_id
            d_pool.tags['pool_id'] = p_id
            d_pool.tags['agenthost'] = socket.gethostname()
            d_pool.tags['agenthost_domain_id'] = \
                "%s_%s" % (cluster_id, d_pool.tags['agenthost'])
            d_pool.fields['pool_name'] = p_name
            d_pool.fields['bytes_used'] = \
                pool.get('stats', {}).get('bytes_used', 0)
            d_pool.fields['max_avail'] = \
                pool.get('stats', {}).get('max_avail', 0)
            d_pool.fields['objects'] = \
                pool.get('stats', {}).get('objects', 0)
            d_pool.fields['wr_bytes'] = \
                pool.get('stats', {}).get('wr_bytes', 0)
            d_pool.fields['dirty'] = \
                pool.get('stats', {}).get('dirty', 0)
            d_pool.fields['rd_bytes'] = \
                pool.get('stats', {}).get('rd_bytes', 0)
            d_pool.fields['raw_bytes_used'] = \
                pool.get('stats', {}).get('raw_bytes_used', 0)
            self.data.append(d_pool)
