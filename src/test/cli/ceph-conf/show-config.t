  $ ceph-conf -n osd.0 --show-config -c /dev/null | grep ceph-osd | grep -v tmp_file_template
  admin_socket = /var/run/ceph/ceph-osd.0.asok
  log_file = /var/log/ceph/ceph-osd.0.log
  mon_debug_dump_location = /var/log/ceph/ceph-osd.0.tdump
  rgw_ops_log_file_path = /var/log/ceph/ops-log-ceph-osd.0.log
  $ CEPH_ARGS="--fsid 96a3abe6-7552-4635-a79b-f3c096ff8b95" ceph-conf -n osd.0 --show-config -c /dev/null | grep fsid
  fsid = 96a3abe6-7552-4635-a79b-f3c096ff8b95
