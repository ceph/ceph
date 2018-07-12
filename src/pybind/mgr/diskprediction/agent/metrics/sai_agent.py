# vim: tabstop=4 shiftwidth=4 softtabstop=4
from __future__ import absolute_import

import socket
import time

from . import MetricsAgent
from ...common import DP_MGR_STAT_FAILED, DP_MGR_STAT_WARNING
from ...common.db import DB_API
from ...models.metrics.dp import SAI_Agent


class SAI_AgentAgent(MetricsAgent):
    measurement = 'sai_agent'

    def _collect_data(self):
        mgr_id = []
        c_data = SAI_Agent()
        obj_api = DB_API(self._ceph_context)
        svc_data = obj_api.get_server(socket.gethostname())
        cluster_state = obj_api.get_health_status()
        if not svc_data:
            raise Exception('unable to get %s service info' % socket.gethostname())
        # Filter mgr id
        for s in svc_data.get('services', []):
            if s.get('type', '') == 'mgr':
                mgr_id.append(s.get('id'))

        for _id in mgr_id:
            mgr_meta = obj_api.get_mgr_metadata(_id)
            cluster_id = obj_api.get_cluster_id()
            c_data.fields['cluster_domain_id'] = str(cluster_id)
            c_data.fields['agenthost'] = str(socket.gethostname())
            c_data.tags['agenthost_domain_id'] = \
                str("%s_%s" % (cluster_id, c_data.fields['agenthost']))
            c_data.fields['heartbeat_interval'] = \
                obj_api.get_configuration('diskprediction_upload_metrics_interval')
            c_data.fields['host_ip'] = str(mgr_meta.get('addr', '127.0.0.1'))
            c_data.fields['host_name'] = str(socket.gethostname())
            if obj_api.module.status in [DP_MGR_STAT_WARNING, DP_MGR_STAT_FAILED]:
                c_data.fields['is_error'] = bool(True)
            else:
                c_data.fields['is_error'] = bool(False)
            if cluster_state in ['HEALTH_ERR', 'HEALTH_WARN']:
                c_data.fields['is_ceph_error'] = bool(True)
                c_data.fields['needs_warning'] = bool(True)
                c_data.fields['is_error'] = bool(True)
            else:
                c_data.fields['is_ceph_error'] = bool(False)
            c_data.fields['send'] = int(time.time() * 1000)
            self.data.append(c_data)
