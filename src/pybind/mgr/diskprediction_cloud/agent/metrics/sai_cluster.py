from __future__ import absolute_import

import socket

from . import AGENT_VERSION, MetricsAgent, MetricsField
from ...common.clusterdata import ClusterAPI


class SAIClusterFields(MetricsField):
    """ SAI Host structure """
    measurement = 'sai_cluster'

    def __init__(self):
        super(SAIClusterFields, self).__init__()
        self.tags['domain_id'] = None
        self.fields['agenthost'] = None
        self.fields['agenthost_domain_id'] = None
        self.fields['name'] = None
        self.fields['agent_version'] = str(AGENT_VERSION)


class SAICluserAgent(MetricsAgent):
    measurement = 'sai_cluster'

    def _collect_data(self):
        c_data = SAIClusterFields()
        obj_api = ClusterAPI(self._module_inst)
        cluster_id = obj_api.get_cluster_id()

        c_data.tags['domain_id'] = str(cluster_id)
        c_data.tags['host_domain_id'] = '%s_%s' % (str(cluster_id), str(socket.gethostname()))
        c_data.fields['agenthost'] = str(socket.gethostname())
        c_data.tags['agenthost_domain_id'] = cluster_id
        c_data.fields['name'] = 'Ceph mgr plugin'
        self.data.append(c_data)
