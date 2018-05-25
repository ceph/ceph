from __future__ import absolute_import

from . import MetricsField


class CPU(MetricsField):
    """ CPU structure """
    measurement = 'cpu'

    def __init__(self):
        super(CPU, self).__init__()
        self.tags['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.tags['cpu'] = None
        self.fields['usage_guest'] = None
        self.fields['usage_guest_nice'] = None
        self.fields['usage_idle'] = None
        self.fields['usage_iowait'] = None
        self.fields['usage_irq'] = None
        self.fields['usage_nice'] = None
        self.fields['usage_softirq'] = None
        self.fields['usage_steal'] = None
        self.fields['usage_system'] = None
        self.fields['usage_user'] = None

    def __str__(self):
        return super(CPU, self).__str__()


class DiskIO(MetricsField):
    """ DiskIO structure """
    measurement = 'diskio'

    def __init__(self):
        super(DiskIO, self).__init__()
        self.tags['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.tags['name'] = None
        self.fields['reads'] = None
        self.fields['writes'] = None
        self.fields['read_bytes'] = None
        self.fields['write_bytes'] = None
        self.fields['read_time'] = None
        self.fields['write_time'] = None
        self.fields['io_time'] = None
        self.fields['weighted_io_time'] = None
        self.fields['iops_in_progress'] = None

    def __str__(self):
        return super(DiskIO, self).__str__()


class MEM(MetricsField):
    """ MEM structure """
    measurement = 'mem'

    def __init__(self):
        super(MEM, self).__init__()
        self.tags['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.fields['active'] = None
        self.fields['available'] = None
        self.fields['available_percent'] = None
        self.fields['buffered'] = None
        self.fields['cached'] = None
        self.fields['free'] = None
        self.fields['inactive'] = None
        self.fields['total'] = None
        self.fields['used'] = None
        self.fields['used_percent'] = None

    def __str__(self):
        return super(MEM, self).__str__()


class NET(MetricsField):
    """ NET structure """
    measurement = 'net'

    def __init__(self):
        super(NET, self).__init__()
        self.tags['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.tags['interface'] = None
        self.fields['bytes_recv'] = None
        self.fields['bytes_sent'] = None
        self.fields['drop_in'] = None
        self.fields['drop_out'] = None
        self.fields['err_in'] = None
        self.fields['err_out'] = None
        self.fields['packets_recv'] = None
        self.fields['packets_sent'] = None

    def __str__(self):
        return super(NET, self).__str__()


class SAI_Host(MetricsField):
    """ SAI Host structure """
    measurement = 'sai_host'

    def __init__(self):
        super(SAI_Host, self).__init__()
        self.tags['domain_id'] = None
        self.tags['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.fields['cluster_domain_id'] = None
        self.fields['name'] = None
        self.fields['host_ip'] = None
        self.fields['host_ipv6'] = None
        self.fields['host_uuid'] = None
        self.fields['os_type'] = 'ceph'
        self.fields['os_name'] = None
        self.fields['os_version'] = None


class SAI_Disk(MetricsField):
    """ SAI Disk structure """
    measurement = 'sai_disk'

    def __init__(self):
        super(SAI_Disk, self).__init__()
        self.tags['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.tags['disk_domain_id'] = None
        self.tags['disk_name'] = None
        self.tags['disk_wwn'] = None
        self.tags['primary_key'] = None
        self.fields['cluster_domain_id'] = None
        self.fields['host_domain_id'] = None
        self.fields['model'] = None
        self.fields['serial_number'] = None
        self.fields['size'] = None
        self.fields['vendor'] = None

        """disk_status
        0: unknown  1: good     2: failure
        """
        self.fields['disk_status'] = 0

        """disk_type
        0: unknown  1: HDD      2: SSD      3: SSD NVME
        4: SSD SAS  5: SSD SATA 6: HDD SAS  7: HDD SATA
        """
        self.fields['disk_type'] = 0

    def __str__(self):
        return super(SAI_Disk, self).__str__()


class SAI_Disk_Smart(MetricsField):
    """ SAI DiskSmart structure """
    measurement = 'sai_disk_smart'

    def __init__(self):
        super(SAI_Disk_Smart, self).__init__()
        self.tags['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.tags['disk_domain_id'] = None
        self.tags['disk_name'] = None
        self.tags['disk_wwn'] = None
        self.tags['primary_key'] = None
        self.fields['cluster_domain_id'] = None
        self.fields['host_domain_id'] = None

    def __str__(self):
        return super(SAI_Disk_Smart, self).__str__()


class Ceph_Cluster(MetricsField):
    """ Ceph cluster structure """
    measurement = 'ceph_cluster'

    def __init__(self):
        super(Ceph_Cluster, self).__init__()
        self.tags['cluster_id'] = None
        self.tags['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
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


class Ceph_MON(MetricsField):
    """ Ceph monitor structure """
    measurement = 'ceph_mon'

    def __init__(self):
        super(Ceph_MON, self).__init__()
        self.tags['cluster_id'] = None
        self.tags['mon_id'] = None
        self.tags['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.fields['num_sessions'] = None
        self.fields['session_add'] = None
        self.fields['session_rm'] = None
        self.fields['session_trim'] = None
        self.fields['num_elections'] = None
        self.fields['election_call'] = None
        self.fields['election_win'] = None
        self.fields['election_lose'] = None


class Ceph_Pool(MetricsField):
    """ Ceph pool structure """
    measurement = 'ceph_pool'

    def __init__(self):
        super(Ceph_Pool, self).__init__()
        self.tags['cluster_id'] = None
        self.tags['pool_id'] = None
        self.tags['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.fields['bytes_used'] = None
        self.fields['max_avail'] = None
        self.fields['objects'] = None
        self.fields['wr_bytes'] = None
        self.fields['dirty'] = None
        self.fields['rd_bytes'] = None
        self.fields['raw_bytes_used'] = None


class Ceph_OSD(MetricsField):
    """ Ceph osd structure """
    measurement = 'ceph_osd'

    def __init__(self):
        super(Ceph_OSD, self).__init__()
        self.tags['cluster_id'] = None
        self.tags['osd_id'] = None
        self.tags['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
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


class DB_Relay(MetricsField):
    """ DB Relay structure """
    measurement = 'db_relay'

    def __init__(self):
        super(DB_Relay, self).__init__()
        self.tags['agenthost'] = None
        self.tags['agenthost_domain_id'] = None
        self.tags['dc_tag'] = 'na'
        self.tags['host'] = None
        self.fields['cmd'] = None
