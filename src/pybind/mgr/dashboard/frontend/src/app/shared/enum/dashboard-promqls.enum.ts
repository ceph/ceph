export enum UtilizationCardQueries {
  USEDCAPACITY = 'ceph_cluster_total_used_bytes',
  WRITEIOPS = 'sum(rate(ceph_pool_wr[1m]))',
  READIOPS = 'sum(rate(ceph_pool_rd[1m]))',
  READLATENCY = 'avg_over_time(ceph_osd_apply_latency_ms[1m])',
  WRITELATENCY = 'avg_over_time(ceph_osd_commit_latency_ms[1m])',
  READCLIENTTHROUGHPUT = 'sum(rate(ceph_pool_rd_bytes[1m]))',
  WRITECLIENTTHROUGHPUT = 'sum(rate(ceph_pool_wr_bytes[1m]))',
  RECOVERYBYTES = 'sum(rate(ceph_osd_recovery_bytes[1m]))'
}

export enum CapacityCardQueries {
  OSD_NEARFULL = 'ceph_osd_nearfull_ratio',
  OSD_FULL = 'ceph_osd_full_ratio'
}

export enum RgwPromqls {
  RGW_REQUEST_PER_SECOND = 'sum(rate(ceph_rgw_req[1m]))',
  AVG_GET_LATENCY = '(sum(rate(ceph_rgw_op_get_obj_lat_sum[1m])) / sum(rate(ceph_rgw_op_get_obj_lat_count[1m]))) * 1000',
  AVG_PUT_LATENCY = '(sum(rate(ceph_rgw_op_put_obj_lat_sum[1m])) / sum(rate(ceph_rgw_op_put_obj_lat_count[1m]))) * 1000',
  GET_BANDWIDTH = 'sum(rate(ceph_rgw_op_get_obj_bytes[1m]))',
  PUT_BANDWIDTH = 'sum(rate(ceph_rgw_op_put_obj_bytes[1m]))'
}

export enum MultiClusterPromqls {
  ALERTS_COUNT = 'count(ALERTS{alertstate="firing"}) or vector(0)',
  CLUSTER_COUNT = 'count(ceph_health_status) or vector(0)',
  HEALTH_OK_COUNT = 'count(ceph_health_status==0) or vector(0)',
  HEALTH_WARNING_COUNT = 'count(ceph_health_status==1) or vector(0)',
  HEALTH_ERROR_COUNT = 'count(ceph_health_status==2) or vector(0)',
  TOTAL_CLUSTERS_CAPACITY = 'sum(ceph_cluster_total_bytes) or vector(0)',
  TOTAL_USED_CAPACITY = 'sum(ceph_cluster_by_class_total_used_bytes) or vector(0)',
  HEALTH_STATUS = 'ceph_health_status',
  MGR_METADATA = 'ceph_mgr_metadata',
  TOTAL_CAPACITY = 'ceph_cluster_total_bytes',
  USED_CAPACITY = 'ceph_cluster_total_used_bytes',
  POOLS = 'count by (cluster) (ceph_pool_metadata) or vector(0)',
  OSDS = 'count by (cluster) (ceph_osd_metadata) or vector(0)',
  CRITICAL_ALERTS_COUNT = 'count(ALERTS{alertstate="firing",severity="critical"}) or vector(0)',
  WARNING_ALERTS_COUNT = 'count(ALERTS{alertstate="firing",severity="warning"}) or vector(0)',
  ALERTS = 'ALERTS{alertstate="firing"}',
  HOSTS = 'sum by (hostname, cluster) (group by (hostname, cluster) (ceph_osd_metadata)) or vector(0)',
  TOTAL_HOSTS = 'count by (cluster) (ceph_osd_metadata) or vector(0)',
  CLUSTER_ALERTS = 'count by (cluster) (ALERTS{alertstate="firing"}) or vector(0)',
  FEDERATE_UP_METRIC = 'up'
}

export enum MultiClusterPromqlsForClusterUtilization {
  CLUSTER_CAPACITY_UTILIZATION = 'topk(5, ceph_cluster_total_used_bytes)',
  CLUSTER_IOPS_UTILIZATION = 'topk(5, sum by (cluster) (rate(ceph_pool_wr[1m])) + sum by (cluster) (rate(ceph_pool_rd[1m])) )',
  CLUSTER_THROUGHPUT_UTILIZATION = 'topk(5, sum by (cluster) (rate(ceph_pool_wr_bytes[1m])) + sum by (cluster) (rate(ceph_pool_rd_bytes[1m])) )'
}

export enum MultiClusterPromqlsForPoolUtilization {
  POOL_CAPACITY_UTILIZATION = 'topk(5, ceph_pool_bytes_used/ceph_pool_max_avail * on(pool_id, cluster) group_left(instance, name) ceph_pool_metadata)',
  POOL_IOPS_UTILIZATION = 'topk(5, (rate(ceph_pool_rd[1m]) + rate(ceph_pool_wr[1m])) * on(pool_id, cluster) group_left(instance, name) ceph_pool_metadata )',
  POOL_THROUGHPUT_UTILIZATION = 'topk(5, (irate(ceph_pool_rd_bytes[1m]) + irate(ceph_pool_wr_bytes[1m])) * on(pool_id, cluster) group_left(instance, name) ceph_pool_metadata )'
}

export const AllStoragetypesQueries = {
  READIOPS: `sum(rate(ceph_osd_op_r[1m]))`,

  WRITEIOPS: `sum(rate(ceph_osd_op_w[1m]))`,

  READCLIENTTHROUGHPUT: `sum(rate(ceph_osd_op_r_out_bytes[1m]))`,

  WRITECLIENTTHROUGHPUT: `sum(rate(ceph_osd_op_w_in_bytes[1m]))`,

  READLATENCY: 'avg_over_time(ceph_osd_apply_latency_ms[1m])',

  WRITELATENCY: 'avg_over_time(ceph_osd_commit_latency_ms[1m])'
};

export const NvmeofPromqls = {
  NVMEOF_READ_BYTES: 'sum(rate(ceph_nvmeof_bdev_read_bytes_total[1m]))',
  NVMEOF_WRITE_BYTES: 'sum(rate(ceph_nvmeof_bdev_written_bytes_total[1m]))',
  NVMEOF_COMBINED_BYTES:
    'sum(rate(ceph_nvmeof_bdev_read_bytes_total[1m])) + sum(rate(ceph_nvmeof_bdev_written_bytes_total[1m]))'
};

export const NvmeofResourcePromqls = {
  NVMEOF_GATEWAY_GROUPS: 'count(count by (group) (ceph_nvmeof_gateway_info)) or vector(0)',
  NVMEOF_SUBSYSTEMS: 'count(count by (nqn) (ceph_nvmeof_subsystem_metadata)) or vector(0)',
  NVMEOF_NAMESPACES:
    'count(count by (nqn, nsid) (ceph_nvmeof_subsystem_namespace_metadata)) or vector(0)',
  NVMEOF_HOSTS: 'count(count by (hostname) (ceph_nvmeof_gateway_info)) or vector(0)',
  NVMEOF_ACTIVE_CONNECTIONS: 'sum(ceph_nvmeof_host_connection_state) or vector(0)'
};
