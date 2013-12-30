
# should reflect daemon defaults

  $ ceph-conf -n osd.0 --show-config-value log_file -c /dev/null
  /var/log/ceph/ceph-osd.0.log
  $ CEPH_ARGS="--fsid 96a3abe6-7552-4635-a79b-f3c096ff8b95" ceph-conf -n osd.0 --show-config-value fsid -c /dev/null
  96a3abe6-7552-4635-a79b-f3c096ff8b95
  $ ceph-conf -n osd.0 --show-config-value INVALID -c /dev/null
  failed to get config option 'INVALID': option not found
  [1]
