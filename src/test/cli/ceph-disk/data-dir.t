  $ uuid=$(uuidgen)
  $ export CEPH_CONF=$TESTDIR/ceph.conf
  $ echo "[global]\nfsid = $uuid\nosd_data = $TESTDIR/osd-data\n" > $CEPH_CONF
  $ osd_data=$(ceph-conf osd_data)
  $ mkdir $osd_data
# a failure to create the fsid file implies the magic file is not created
  $ mkdir $osd_data/fsid
  $ ceph-disk --verbose prepare $osd_data 2>&1 | grep 'Is a directory'
  OSError: [Errno 21] Is a directory
  $ ! [ -f $osd_data/magic ]
  $ rmdir $osd_data/fsid
# successfully prepare the OSD
  $ ceph-disk --verbose prepare $osd_data
  DEBUG:ceph-disk:Preparing osd data dir .* (re)
  $ grep $uuid $osd_data/ceph_fsid > /dev/null
  $ [ -f $osd_data/magic ]
# will not override an existing OSD
  $ echo "[global]\nfsid = $(uuidgen)\nosd_data = $TESTDIR/osd-data\n" > $CEPH_CONF
  $ ceph-disk --verbose prepare $osd_data
  DEBUG:ceph-disk:Data dir .* already exists (re)
  $ grep $uuid $osd_data/ceph_fsid > /dev/null
  $ rm -fr $osd_data $TESTDIR/ceph.conf
