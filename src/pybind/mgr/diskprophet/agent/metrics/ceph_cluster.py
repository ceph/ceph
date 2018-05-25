from __future__ import absolute_import

import socket

from . import MetricsAgent
from ...common.db import DB_API
from ...models.metrics.dp import Ceph_Cluster


class CephCluster_Agent(MetricsAgent):
    measurement = 'ceph_cluster'

    def _collect_data(self):
        # process data and save to 'self.data'
        obj_api = DB_API(self._ceph_context)
        cluster_id = obj_api.get_cluster_id()

        c_data = Ceph_Cluster()
        c_data.tags['cluster_id'] = cluster_id
        c_data.tags['agenthost'] = socket.gethostname()
        c_data.tags['agenthost_domain_id'] = \
            "%s_%s" % (cluster_id, c_data.tags['agenthost'])
        c_data.fields['osd_epoch'] = obj_api.get_osd_epoch()
        c_data.fields['num_osd'] = obj_api.get_max_osd()
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
        c_data.fields['num_osd_up'] = num_osd_up
        c_data.fields['num_osd_in'] = num_osd_in
        c_data.fields['num_pool'] = len(obj_api.get_osd_pools())

        df_stats = obj_api.get_df_stats()
        c_data.fields['osd_bytes'] = df_stats.get('total_bytes', 0)
        c_data.fields['osd_bytes_used'] = df_stats.get('total_used_bytes', 0)
        c_data.fields['osd_bytes_avail'] = df_stats.get('total_avail_bytes', 0)

        pg_stats = obj_api.get_pg_stats()
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
