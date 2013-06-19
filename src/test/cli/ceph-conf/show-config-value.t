
# should reflect daemon defaults

  $ ceph-conf -n osd.0 --show-config-value log_file -c /dev/null
  /var/log/ceph/ceph-osd.0.log
