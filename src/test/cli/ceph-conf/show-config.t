  $ ceph-conf -n osd.0 --show-config -c /dev/null | grep ceph-osd
  admin_socket = /var/run/ceph/ceph-osd.0.asok
  log_file = /var/log/ceph/ceph-osd.0.log
  mon_debug_dump_location = /var/log/ceph/ceph-osd.0.tdump
  $ CEPH_ARGS="--fsid 96a3abe6-7552-4635-a79b-f3c096ff8b95" ceph-conf -n osd.0 --show-config -c /dev/null | grep fsid
  fsid = 96a3abe6-7552-4635-a79b-f3c096ff8b95
