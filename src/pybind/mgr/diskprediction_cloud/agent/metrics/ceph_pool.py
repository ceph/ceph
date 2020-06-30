from __future__ import absolute_import

import socket

from . import MetricsAgent, MetricsField
from ...common.clusterdata import ClusterAPI


class CephPool(MetricsField):
    """ Ceph pool structure """
    measurement = 'ceph_pool'

    def __init__(self):
        super(CephPool, self).__init__()
        self.tags['cluster_id'] = None
        self.tags['pool_id'] = None
        self.fields['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.fields['bytes_used'] = None
        self.fields['max_avail'] = None
        self.fields['objects'] = None
        self.fields['wr_bytes'] = None
        self.fields['dirty'] = None
        self.fields['rd_bytes'] = None
        self.fields['stored_raw'] = None


class CephPoolAgent(MetricsAgent):
    measurement = 'ceph_pool'

    def _collect_data(self):
        # process data and save to 'self.data'
        obj_api = ClusterAPI(self._module_inst)
        df_data = obj_api.module.get('df')
        cluster_id = obj_api.get_cluster_id()
        for pool in df_data.get('pools', []):
            d_pool = CephPool()
            p_id = pool.get('id')
            d_pool.tags['cluster_id'] = cluster_id
            d_pool.tags['pool_id'] = p_id
            d_pool.fields['agenthost'] = socket.gethostname()
            d_pool.tags['agenthost_domain_id'] = cluster_id
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
            d_pool.fields['stored_raw'] = \
                pool.get('stats', {}).get('stored_raw', 0)
            self.data.append(d_pool)
