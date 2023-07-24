export enum Promqls {
  USEDCAPACITY = 'ceph_cluster_total_used_bytes',
  IPS = 'sum(rate(ceph_osd_op_w_in_bytes[$interval]))',
  OPS = 'sum(rate(ceph_osd_op_r_out_bytes[$interval]))',
  READLATENCY = 'avg_over_time(ceph_osd_apply_latency_ms[$interval])',
  WRITELATENCY = 'avg_over_time(ceph_osd_commit_latency_ms[$interval])',
  READCLIENTTHROUGHPUT = 'sum(rate(ceph_pool_rd_bytes[$interval]))',
  WRITECLIENTTHROUGHPUT = 'sum(rate(ceph_pool_wr_bytes[$interval]))',
  RECOVERYBYTES = 'sum(rate(ceph_osd_recovery_bytes[$interval]))'
}
