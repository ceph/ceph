# setup
  $ cat >foo.conf <<'EOF'
  > ; ---------------------
  > [group cephnet]
  > 	addr = 10.3.14.0/24
  > 
  > [global]
  > 	pid file = /home/sage/ceph/src/out/$name.pid
  > 
  > [osd]
  > 	osd data = /mnt/osd$id
  > [osd3]
  > 	host = cosd3
  > EOF

To extract the value of the "osd data" option for the osd0 daemon,

  $ cconf -c foo.conf "osd data" --name osd.0
  /mnt/osd0

This is equivalent to doing specifying sections [osd0], [osd.0],
[osd], or [global], in that order of preference:

# TODO the "admin" here seems like an actual bug

  $ cconf -c foo.conf "osd data" -s osd0 -s osd.0 -s osd -s global
  /mnt/osdadmin

To list all sections that begin with osd:

  $ cconf -c foo.conf -l osd
  osd
  osd3
