export enum Promqls {
  USEDCAPACITY = 'ceph_cluster_total_used_bytes',
  WRITEIOPS = 'sum(rate(ceph_pool_wr[1m]))',
  READIOPS = 'sum(rate(ceph_pool_rd[1m]))',
  READLATENCY = 'avg_over_time(ceph_osd_apply_latency_ms[1m])',
  WRITELATENCY = 'avg_over_time(ceph_osd_commit_latency_ms[1m])',
  READCLIENTTHROUGHPUT = 'sum(rate(ceph_pool_rd_bytes[1m]))',
  WRITECLIENTTHROUGHPUT = 'sum(rate(ceph_pool_wr_bytes[1m]))',
  RECOVERYBYTES = 'sum(rate(ceph_osd_recovery_bytes[1m]))'
}

export enum RgwPromqls {
  RGW_REQUEST_PER_SECOND = 'sum(rate(ceph_rgw_req[1m]))',
  AVG_GET_LATENCY = 'sum(rate(ceph_rgw_op_get_obj_lat_sum[1m])) / sum(rate(ceph_rgw_op_get_obj_lat_count[1m]))',
  AVG_PUT_LATENCY = 'sum(rate(ceph_rgw_op_put_obj_lat_sum[1m])) / sum(rate(ceph_rgw_op_put_obj_lat_count[1m]))',
  GET_BANDWIDTH = 'sum(rate(ceph_rgw_op_get_obj_bytes[1m]))',
  PUT_BANDWIDTH = 'sum(rate(ceph_rgw_op_put_obj_bytes[1m]))'
}
