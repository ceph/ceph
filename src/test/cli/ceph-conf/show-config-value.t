
# should reflect daemon defaults

  $ ceph-conf -n osd.0 --show-config-value log_file -c /dev/null
  /var/log/ceph/ceph-osd.0.log
  $ CEPH_ARGS="--fsid 96a3abe6-7552-4635-a79b-f3c096ff8b95" ceph-conf -n osd.0 --show-config-value fsid -c /dev/null
  96a3abe6-7552-4635-a79b-f3c096ff8b95
  $ ceph-conf -n osd.0 --show-config-value INVALID -c /dev/null
  failed to get config option 'INVALID': option not found
  [1]

  $ cat > $TESTDIR/ceph.conf <<EOF
  > [global]
  >     mon_host = \$public_network
  >     public_network = \$mon_host
  > EOF
  $ ceph-conf --show-config-value mon_host -c $TESTDIR/ceph.conf
  variable expansion loop at mon_host=$public_network
  expansion stack:
  public_network=$mon_host
  mon_host=$public_network
  $mon_host
  $ rm $TESTDIR/ceph.conf

Name option test to strip the PID
=================================
  $ cat > $TESTDIR/ceph.conf <<EOF
  > [client]
  >     admin socket = \$name.\$pid.asok
  > [global]
  >     admin socket = \$name.asok
  > EOF
  $ ceph-conf --name client.admin --pid 133423 --show-config-value admin_socket -c $TESTDIR/ceph.conf
  client.admin.133423.asok
  $ ceph-conf --name mds.a --show-config-value admin_socket -c $TESTDIR/ceph.conf
  mds.a.asok
  $ ceph-conf --name osd.0 --show-config-value admin_socket -c $TESTDIR/ceph.conf
  osd.0.asok
  $ rm $TESTDIR/ceph.conf
