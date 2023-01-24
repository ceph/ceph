export enum Promqls {
  USEDCAPACITY = 'ceph_cluster_total_used_bytes',
  IPS = 'sum(irate(ceph_osd_op_w_in_bytes[1m]))',
  OPS = 'sum(irate(ceph_osd_op_r_out_bytes[1m]))',
  READLATENCY = 'avg(ceph_osd_apply_latency_ms)',
  WRITELATENCY = 'avg(ceph_osd_commit_latency_ms)',
  READCLIENTTHROUGHPUT = 'sum(irate(ceph_pool_rd_bytes[1m]))',
  WRITECLIENTTHROUGHPUT = 'sum(irate(ceph_pool_wr_bytes[1m]))',
  RECOVERYBYTES = 'ceph_osd_recovery_bytes'
}
