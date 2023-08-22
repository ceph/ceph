export enum Promqls {
  USEDCAPACITY = 'ceph_cluster_total_used_bytes',
  IPS = 'sum(rate(ceph_osd_op_w_in_bytes[1m]))',
  OPS = 'sum(rate(ceph_osd_op_r_out_bytes[1m]))',
  READLATENCY = 'avg_over_time(ceph_osd_apply_latency_ms[1m])',
  WRITELATENCY = 'avg_over_time(ceph_osd_commit_latency_ms[1m])',
  READCLIENTTHROUGHPUT = 'sum(rate(ceph_pool_rd_bytes[1m]))',
  WRITECLIENTTHROUGHPUT = 'sum(rate(ceph_pool_wr_bytes[1m]))',
  RECOVERYBYTES = 'sum(rate(ceph_osd_recovery_bytes[1m]))'
}

export enum RgwPromqls {
  RGW_REQUEST_PER_SECOND = 'sum(rate(ceph_rgw_req[1m]))',
  AVG_GET_LATENCY = 'sum(rate(ceph_rgw_get_initial_lat_sum[1m])) / sum(rate(ceph_rgw_get_initial_lat_count[1m]))',
  AVG_PUT_LATENCY = 'sum(rate(ceph_rgw_put_initial_lat_sum[1m])) / sum(rate(ceph_rgw_put_initial_lat_count[1m]))',
  GET_BANDWIDTH = 'sum(rate(ceph_rgw_get_b[1m]))',
  PUT_BANDWIDTH = 'sum(rate(ceph_rgw_put_b[1m]))'
}
