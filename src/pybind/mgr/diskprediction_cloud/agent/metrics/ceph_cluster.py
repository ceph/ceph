from __future__ import absolute_import

import socket

from . import MetricsAgent, MetricsField
from ...common.clusterdata import ClusterAPI


class CephCluster(MetricsField):
    """ Ceph cluster structure """
    measurement = 'ceph_cluster'

    def __init__(self):
        super(CephCluster, self).__init__()
        self.tags['cluster_id'] = None
        self.fields['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.fields['cluster_health'] = ''
        self.fields['num_mon'] = None
        self.fields['num_mon_quorum'] = None
        self.fields['num_osd'] = None
        self.fields['num_osd_up'] = None
        self.fields['num_osd_in'] = None
        self.fields['osd_epoch'] = None
        self.fields['osd_bytes'] = None
        self.fields['osd_bytes_used'] = None
        self.fields['osd_bytes_avail'] = None
        self.fields['num_pool'] = None
        self.fields['num_pg'] = None
        self.fields['num_pg_active_clean'] = None
        self.fields['num_pg_active'] = None
        self.fields['num_pg_peering'] = None
        self.fields['num_object'] = None
        self.fields['num_object_degraded'] = None
        self.fields['num_object_misplaced'] = None
        self.fields['num_object_unfound'] = None
        self.fields['num_bytes'] = None
        self.fields['num_mds_up'] = None
        self.fields['num_mds_in'] = None
        self.fields['num_mds_failed'] = None
        self.fields['mds_epoch'] = None


class CephClusterAgent(MetricsAgent):
    measurement = 'ceph_cluster'

    def _collect_data(self):
        # process data and save to 'self.data'
        obj_api = ClusterAPI(self._module_inst)
        cluster_id = obj_api.get_cluster_id()

        c_data = CephCluster()
        cluster_state = obj_api.get_health_status()
        c_data.tags['cluster_id'] = cluster_id
        c_data.fields['cluster_health'] = str(cluster_state)
        c_data.fields['agenthost'] = socket.gethostname()
        c_data.tags['agenthost_domain_id'] = cluster_id
        c_data.fields['osd_epoch'] = obj_api.get_osd_epoch()
        c_data.fields['num_mon'] = len(obj_api.get_mons())
        c_data.fields['num_mon_quorum'] = \
            len(obj_api.get_mon_status().get('quorum', []))

        osds = obj_api.get_osds()
        num_osd_up = 0
        num_osd_in = 0
        for osd_data in osds:
            if osd_data.get('up'):
                num_osd_up = num_osd_up + 1
            if osd_data.get('in'):
                num_osd_in = num_osd_in + 1
        if osds:
            c_data.fields['num_osd'] = len(osds)
        else:
            c_data.fields['num_osd'] = 0
        c_data.fields['num_osd_up'] = num_osd_up
        c_data.fields['num_osd_in'] = num_osd_in
        c_data.fields['num_pool'] = len(obj_api.get_osd_pools())

        df_stats = obj_api.module.get('df').get('stats', {})
        total_bytes = df_stats.get('total_bytes', 0)
        total_used_bytes = df_stats.get('total_used_bytes', 0)
        total_avail_bytes = df_stats.get('total_avail_bytes', 0)
        c_data.fields['osd_bytes'] = total_bytes
        c_data.fields['osd_bytes_used'] = total_used_bytes
        c_data.fields['osd_bytes_avail'] = total_avail_bytes
        if total_bytes and total_avail_bytes:
            c_data.fields['osd_bytes_used_percentage'] = \
                round((float(total_used_bytes) / float(total_bytes)) * 100, 4)
        else:
            c_data.fields['osd_bytes_used_percentage'] = 0.0000

        pg_stats = obj_api.module.get('pg_dump').get('pg_stats', [])
        num_bytes = 0
        num_object = 0
        num_object_degraded = 0
        num_object_misplaced = 0
        num_object_unfound = 0
        num_pg_active = 0
        num_pg_active_clean = 0
        num_pg_peering = 0
        for pg_data in pg_stats:
            num_pg_active = num_pg_active + len(pg_data.get('acting'))
            if 'active+clean' in pg_data.get('state'):
                num_pg_active_clean = num_pg_active_clean + 1
            if 'peering' in pg_data.get('state'):
                num_pg_peering = num_pg_peering + 1

            stat_sum = pg_data.get('stat_sum', {})
            num_object = num_object + stat_sum.get('num_objects', 0)
            num_object_degraded = \
                num_object_degraded + stat_sum.get('num_objects_degraded', 0)
            num_object_misplaced = \
                num_object_misplaced + stat_sum.get('num_objects_misplaced', 0)
            num_object_unfound = \
                num_object_unfound + stat_sum.get('num_objects_unfound', 0)
            num_bytes = num_bytes + stat_sum.get('num_bytes', 0)

        c_data.fields['num_pg'] = len(pg_stats)
        c_data.fields['num_object'] = num_object
        c_data.fields['num_object_degraded'] = num_object_degraded
        c_data.fields['num_object_misplaced'] = num_object_misplaced
        c_data.fields['num_object_unfound'] = num_object_unfound
        c_data.fields['num_bytes'] = num_bytes
        c_data.fields['num_pg_active'] = num_pg_active
        c_data.fields['num_pg_active_clean'] = num_pg_active_clean
        c_data.fields['num_pg_peering'] = num_pg_active_clean

        filesystems = obj_api.get_file_systems()
        num_mds_in = 0
        num_mds_up = 0
        num_mds_failed = 0
        mds_epoch = 0
        for fs_data in filesystems:
            num_mds_in = \
                num_mds_in + len(fs_data.get('mdsmap', {}).get('in', []))
            num_mds_up = \
                num_mds_up + len(fs_data.get('mdsmap', {}).get('up', {}))
            num_mds_failed = \
                num_mds_failed + len(fs_data.get('mdsmap', {}).get('failed', []))
            mds_epoch = mds_epoch + fs_data.get('mdsmap', {}).get('epoch', 0)
        c_data.fields['num_mds_in'] = num_mds_in
        c_data.fields['num_mds_up'] = num_mds_up
        c_data.fields['num_mds_failed'] = num_mds_failed
        c_data.fields['mds_epoch'] = mds_epoch
        self.data.append(c_data)
