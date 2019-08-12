from __future__ import absolute_import

import socket

from . import MetricsAgent, MetricsField
from ...common.clusterdata import ClusterAPI


class CephMON(MetricsField):
    """ Ceph monitor structure """
    measurement = 'ceph_mon'

    def __init__(self):
        super(CephMON, self).__init__()
        self.tags['cluster_id'] = None
        self.tags['mon_id'] = None
        self.fields['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.fields['num_sessions'] = None
        self.fields['session_add'] = None
        self.fields['session_rm'] = None
        self.fields['session_trim'] = None
        self.fields['num_elections'] = None
        self.fields['election_call'] = None
        self.fields['election_win'] = None
        self.fields['election_lose'] = None


class CephErasureProfile(MetricsField):
    """ Ceph osd erasure profile """
    measurement = 'ceph_erasure_profile'

    def __init__(self):
        super(CephErasureProfile, self).__init__()
        self.tags['cluster_id'] = None
        self.fields['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.tags['host_domain_id'] = None
        self.fields['name'] = None


class CephOsdTree(MetricsField):
    """ Ceph osd tree map """
    measurement = 'ceph_osd_tree'

    def __init__(self):
        super(CephOsdTree, self).__init__()
        self.tags['cluster_id'] = None
        self.fields['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.tags['host_domain_id'] = None
        self.fields['name'] = None


class CephOSD(MetricsField):
    """ Ceph osd structure """
    measurement = 'ceph_osd'

    def __init__(self):
        super(CephOSD, self).__init__()
        self.tags['cluster_id'] = None
        self.tags['osd_id'] = None
        self.fields['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.tags['host_domain_id'] = None
        self.fields['op_w'] = None
        self.fields['op_in_bytes'] = None
        self.fields['op_r'] = None
        self.fields['op_out_bytes'] = None
        self.fields['op_wip'] = None
        self.fields['op_latency'] = None
        self.fields['op_process_latency'] = None
        self.fields['op_r_latency'] = None
        self.fields['op_r_process_latency'] = None
        self.fields['op_w_in_bytes'] = None
        self.fields['op_w_latency'] = None
        self.fields['op_w_process_latency'] = None
        self.fields['op_w_prepare_latency'] = None
        self.fields['op_rw'] = None
        self.fields['op_rw_in_bytes'] = None
        self.fields['op_rw_out_bytes'] = None
        self.fields['op_rw_latency'] = None
        self.fields['op_rw_process_latency'] = None
        self.fields['op_rw_prepare_latency'] = None
        self.fields['op_before_queue_op_lat'] = None
        self.fields['op_before_dequeue_op_lat'] = None


class CephMonOsdAgent(MetricsAgent):
    measurement = 'ceph_mon_osd'

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

    def _generage_osd_erasure_profile(self, cluster_id):
        obj_api = ClusterAPI(self._module_inst)
        osd_map = obj_api.module.get('osd_map')
        if osd_map:
            for n, n_value in osd_map.get('erasure_code_profiles', {}).items():
                e_osd = CephErasureProfile()
                e_osd.fields['name'] = n
                e_osd.tags['cluster_id'] = cluster_id
                e_osd.fields['agenthost'] = socket.gethostname()
                e_osd.tags['agenthost_domain_id'] = cluster_id
                e_osd.tags['host_domain_id'] = '%s_%s' % (cluster_id, socket.gethostname())
                for k in n_value.keys():
                    e_osd.fields[k] = str(n_value[k])
                self.data.append(e_osd)

    def _generate_osd_tree(self, cluster_id):
        obj_api = ClusterAPI(self._module_inst)
        osd_tree = obj_api.module.get('osd_map_tree')
        if osd_tree:
            for node in osd_tree.get('nodes', []):
                n_node = CephOsdTree()
                n_node.tags['cluster_id'] = cluster_id
                n_node.fields['agenthost'] = socket.gethostname()
                n_node.tags['agenthost_domain_id'] = cluster_id
                n_node.tags['host_domain_id'] = '%s_%s' % (cluster_id, socket.gethostname())
                n_node.fields['children'] = ','.join(str(x) for x in node.get('children', []))
                n_node.fields['type_id'] = str(node.get('type_id', ''))
                n_node.fields['id'] = str(node.get('id', ''))
                n_node.fields['name'] = str(node.get('name', ''))
                n_node.fields['type'] = str(node.get('type', ''))
                n_node.fields['reweight'] = float(node.get('reweight', 0.0))
                n_node.fields['crush_weight'] = float(node.get('crush_weight', 0.0))
                n_node.fields['primary_affinity'] = float(node.get('primary_affinity', 0.0))
                n_node.fields['device_class'] = str(node.get('device_class', ''))
                self.data.append(n_node)

    def _generate_osd(self, cluster_id, service_name, perf_counts):
        obj_api = ClusterAPI(self._module_inst)
        service_id = service_name[4:]
        d_osd = CephOSD()
        stat_bytes = 0
        stat_bytes_used = 0
        d_osd.tags['cluster_id'] = cluster_id
        d_osd.tags['osd_id'] = service_name[4:]
        d_osd.fields['agenthost'] = socket.gethostname()
        d_osd.tags['agenthost_domain_id'] = cluster_id
        d_osd.tags['host_domain_id'] = \
            '%s_%s' % (cluster_id,
                       obj_api.get_osd_hostname(d_osd.tags['osd_id']))

        for i_key, i_val in perf_counts.items():
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
                d_osd.fields[key_name] = float(value)

        if stat_bytes and stat_bytes_used:
            d_osd.fields['stat_bytes_used_percentage'] = \
                round((float(stat_bytes_used) / float(stat_bytes)) * 100, 4)
        else:
            d_osd.fields['stat_bytes_used_percentage'] = 0.0000
        self.data.append(d_osd)

    def _generate_mon(self, cluster_id, service_name, perf_counts):
        d_mon = CephMON()
        d_mon.tags['cluster_id'] = cluster_id
        d_mon.tags['mon_id'] = service_name[4:]
        d_mon.fields['agenthost'] = socket.gethostname()
        d_mon.tags['agenthost_domain_id'] = cluster_id
        d_mon.fields['num_sessions'] = \
            perf_counts.get('mon.num_sessions', {}).get('value', 0)
        d_mon.fields['session_add'] = \
            perf_counts.get('mon.session_add', {}).get('value', 0)
        d_mon.fields['session_rm'] = \
            perf_counts.get('mon.session_rm', {}).get('value', 0)
        d_mon.fields['session_trim'] = \
            perf_counts.get('mon.session_trim', {}).get('value', 0)
        d_mon.fields['num_elections'] = \
            perf_counts.get('mon.num_elections', {}).get('value', 0)
        d_mon.fields['election_call'] = \
            perf_counts.get('mon.election_call', {}).get('value', 0)
        d_mon.fields['election_win'] = \
            perf_counts.get('mon.election_win', {}).get('value', 0)
        d_mon.fields['election_lose'] = \
            perf_counts.get('election_lose', {}).get('value', 0)
        self.data.append(d_mon)

    def _collect_data(self):
        # process data and save to 'self.data'
        obj_api = ClusterAPI(self._module_inst)
        perf_data = obj_api.module.get_all_perf_counters(services=('mon', 'osd'))
        if not perf_data and not isinstance(perf_data, dict):
            self._logger.error('unable to get all perf counters')
            return
        cluster_id = obj_api.get_cluster_id()
        for n_name, i_perf in perf_data.items():
            if n_name[0:3].lower() == 'mon':
                self._generate_mon(cluster_id, n_name, i_perf)
            elif n_name[0:3].lower() == 'osd':
                self._generate_osd(cluster_id, n_name, i_perf)
        self._generage_osd_erasure_profile(cluster_id)
        self._generate_osd_tree(cluster_id)
