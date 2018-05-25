from __future__ import absolute_import

import socket

from . import MetricsAgent, AGENT_VERSION
from ...common.db import DB_API
from ...models.metrics.dp import SAI_Cluster


class SAI_CluserAgent(MetricsAgent):
    measurement = 'sai_cluster'

    def _collect_data(self):
        c_data = SAI_Cluster()
        obj_api = DB_API(self._ceph_context)
        cluster_id = obj_api.get_cluster_id()

        c_data.tags['domain_id'] = str(cluster_id)
        c_data.fields['agent_version'] = AGENT_VERSION
        c_data.fields['host_domain_id'] = "%s_%s" % (str(cluster_id), str(socket.gethostname()))
        c_data.tags['agenthost'] = str(socket.gethostname())
        c_data.tags['agenthost_domain_id'] = \
            str("%s_%s" % (cluster_id, c_data.tags['agenthost']))
        c_data.fields['name'] = 'ceph mgr plugin'
        self.data.append(c_data)
