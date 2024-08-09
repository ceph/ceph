MNT=/mnt
CEPH=bin/ceph
RADOS=bin/rados
FS=a
DIR1_MDS_OBJ=""
DIR2_MDS_OBJ=""
DIR3_MDS_OBJ=""
DIR1_FILE1_DATA_OBJ=""
DIR2_HL_FILE1_DATA_OBJ=""

function flush_mds_journal () {
  echo "flush mds.0 journal"
  $CEPH tell mds.0 flush journal > /dev/null 2>&1
}

function get_data_object () {
  $RADOS -p cephfs.a.meta getomapval $1 $2 /tmp/a
  pinode=$(bin/ceph-dencoder type 'inode_t<std::allocator>' skip 25 import /tmp/a decode dump_json | jq '.ino')
  hex_pinode=$(printf "%x" $pinode)
  dataobject=$(echo $hex_pinode.00000000)
  echo "$dataobject"
}

function fuse_mount () {
  echo "--------------------------------------------------------------------------------------------------"
  echo "Fuse mount at $MNT"
  sudo umount -f $MNT
  sleep 3
  sudo bin/ceph-fuse -c ./ceph.conf $MNT 2>/dev/null
}

function clean_data () {
  echo "--------------------------------------------------------------------------------------------------"
  echo "clean data ..."
  echo "remove snapshot ..."
  rmdir $MNT/dir1/.snap/*
  rmdir $MNT/dir2/.snap/*
  rmdir $MNT/dir3/.snap/*
  echo "rm -rf $MNT"
  rm -rf $MNT/*
  flush_mds_journal
  sleep 3
}

function create_sample_files () {
  echo "mkdir /dir1"
  mkdir $MNT/dir1
  flush_mds_journal
  declare -g DIR1_MDS_OBJ=$($RADOS -p cephfs.a.meta ls | egrep "([0-9]|[a-f]){11}.")
  echo "DIR1_MDS_OBJ: $DIR1_MDS_OBJ"

  echo "mkdir /dir2"
  mkdir $MNT/dir2
  flush_mds_journal
  declare -g DIR2_MDS_OBJ=$($RADOS -p cephfs.a.meta ls | egrep "([0-9]|[a-f]){11}." | grep -v "${DIR1_MDS_OBJ}")
  echo "DIR2_MDS_OBJ: $DIR2_MDS_OBJ"

  echo "mkdir /dir3"
  mkdir $MNT/dir3
  flush_mds_journal
  declare -g DIR3_MDS_OBJ=$($RADOS -p cephfs.a.meta ls | egrep "([0-9]|[a-f]){11}." | grep -v "${DIR1_MDS_OBJ}" | grep -v "${DIR2_MDS_OBJ}")
  echo "DIR3_MDS_OBJ: $DIR3_MDS_OBJ"

  echo "create /dir1/file1"
  echo "data " > /mnt/dir1/file1
  echo "ln /dir1/file1 /dir2/hl_file1"
  ln /mnt/dir1/file1 /mnt/dir2/hl_file1
  flush_mds_journal

  DIR1_FILE1_DATA_OBJ=$(get_data_object $DIR1_MDS_OBJ "file1_head")
  DIR2_HL_FILE1_DATA_OBJ=$(get_data_object $DIR2_MDS_OBJ "hl_file1_head")
  echo "DIR1_FILE1_DATA_OBJ: $DIR1_FILE1_DATA_OBJ"
  echo "DIR2_HL_FILE1_DATA_OBJ: $DIR2_HL_FILE1_DATA_OBJ"
}

function setup_test_bed () {
  fuse_mount
  clean_data
  create_sample_files
}

setup_test_bed

#create snaps
mkdir /mnt/dir1/.snap/dir1_snap0
mkdir /mnt/dir2/.snap/dir2_snap0

#List rados snaps - observe no snaps at rados yet
snap_count=$($RADOS -p cephfs.a.data listsnaps $DIR1_FILE1_DATA_OBJ --format=json-pretty | jq '.clones[0].snapshots | length')
if [ "$snap_count" -ne 0 ]; then
  echo "FAIL - snapshots should be empty before write"
fi

#COW - Write data - rados takes snapshot on next write
echo "append " >> /mnt/dir1/file1

#List rados snaps - observe snaps
snap_count=$($RADOS -p cephfs.a.data listsnaps $DIR1_FILE1_DATA_OBJ --format=json-pretty | jq '.clones[0].snapshots | length')
if [ "$snap_count" -ne 2 ]; then
  echo "FAIL - object snapshots should have been taken after write"
fi

#Take snaps on dir3 - not related to hardlink file
echo "data ...." > /mnt/dir3/dir3_file1
mkdir /mnt/dir3/.snap/dir3_snap0
mkdir /mnt/dir3/.snap/dir3_snap1

#Write data to hardlink file - observe snapshots
echo "append last " >> /mnt/dir1/file1

#List rados snaps - observe snaps
snap_count=$($RADOS -p cephfs.a.data listsnaps $DIR1_FILE1_DATA_OBJ --format=json-pretty | jq '.clones[0].snapshots | length')
if [ "$snap_count" -ne 2 ]; then
  echo "FAIL - snapshot on dir3 should not snapshot the hardlink object"
fi

echo "SUCCESS"
