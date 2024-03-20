MNT=/mnt
CEPH=bin/ceph
RADOS=bin/rados
FS=a
MAX_FILES=100

function flush_mds_journal () {
  echo "flush mds.0 journal"
  $CEPH tell mds.0 flush journal > /dev/null 2>&1
  echo "flush mds.1 journal"
  $CEPH tell mds.1 flush journal > /dev/null 2>&1
  echo "flush mds.2 journal"
  $CEPH tell mds.2 flush journal > /dev/null 2>&1
}

function fuse_mount () {
  echo "--------------------------------------------------------------------------------------------------"
  echo "Fuse mount at $MNT"
  sudo umount -f $MNT
  sudo umount -f $MNT
  sleep 3
  sudo bin/ceph-fuse -c ./ceph.conf $MNT 2>/dev/null
}

function clean_data () {
  echo "--------------------------------------------------------------------------------------------------"
  echo "clean data ..."
  echo "rm -rf $MNT"
  rm -rf $MNT/*
  flush_mds_journal
  sleep 3
}

function set_max_mds () {
  echo "set max_mds to $1"
  $CEPH fs set $FS max_mds $1
  flush_mds_journal
  echo "Wait 10 secs for other mds to become active"
  sleep 10
}

function create_dirs_and_pin () {
  echo "mkdir /dir1"
  mkdir $MNT/dir1
  flush_mds_journal

  echo "mkdir /dir2"
  mkdir $MNT/dir2
  flush_mds_journal

  echo "mkdir /dir3"
  mkdir $MNT/dir3
  flush_mds_journal

  rank0=$($CEPH fs get $FS 2>/dev/null | grep "mds\." | grep "{0" | awk '{print $1}')
  echo "static pin /dir1 to rank 0 - $rank0"
  setfattr -n ceph.dir.pin -v 0 $MNT/dir1
  rank1=$($CEPH fs get $FS 2>/dev/null | grep "mds\." | grep "{1" | awk '{print $1}')
  echo "static pin /dir2 to rank 1 - $rank1"
  setfattr -n ceph.dir.pin -v 1 $MNT/dir2
  rank2=$($CEPH fs get $FS 2>/dev/null | grep "mds\." | grep "{2" | awk '{print $1}')
  echo "static pin /dir3 to rank 2 - $rank2"
  setfattr -n ceph.dir.pin -v 2 $MNT/dir3
}

function stress_test_hardlink () {
  echo "create /dir1/init_migration1"
  echo "data init_migration1" > $MNT/dir1/init_migration1

  echo "create /dir2/init_migration1"
  echo "data init_migration2" > $MNT/dir2/init_migration2

  echo "create /dir3/init_migration1"
  echo "data init_migration2" > $MNT/dir3/init_migration3

  echo "Wait 30 seconds for subtree migration"
  sleep 30

  echo "create /dir1/sub_dir1"
  mkdir $MNT/dir1/sub_dir1
  flush_mds_journal

  echo "create /dir2/sub_dir2"
  mkdir $MNT/dir2/sub_dir2
  flush_mds_journal

  echo "create /dir3/sub_dir3"
  mkdir $MNT/dir3/sub_dir3
  flush_mds_journal

  echo "create /dir1/sub_dir1/dir1_file1"
  echo "Initial data " > $MNT/dir1/sub_dir1/dir1_file1
  flush_mds_journal

  # Create 100 hardlinks in each directory
  for i in $(seq 1 $MAX_FILES)
  do
    ln $MNT/dir1/sub_dir1/dir1_file1 $MNT/dir2/sub_dir2/dir2_hl_file1_$i
    ln $MNT/dir1/sub_dir1/dir1_file1 $MNT/dir3/sub_dir3/dir3_hl_file1_$i
    ln $MNT/dir1/sub_dir1/dir1_file1 $MNT/dir1/sub_dir1/dir1_hl_file1_$i
  done
  flush_mds_journal

  num_data_pool_obj=$($RADOS -p cephfs.a.data ls --format=json-pretty | jq -r '. | length')
  if [ "$num_data_pool_obj" -ne 304 ]; then
    echo "FAIL - Number of data pool objects didn't match. Expected 304 but found $num_data_pool_obj"
    exit
  fi

  # Remove all the hardlinks, primary first to also test linkmerge
  rm -f $MNT/dir1/sub_dir1/dir1_file1
  for i in $(seq 1 $MAX_FILES)
  do
    rm -f $MNT/dir1/sub_dir1/dir1_hl_file1_$i
    rm -f $MNT/dir1/sub_dir2/dir2_hl_file1_$i
    rm -f $MNT/dir1/sub_dir3/dir3_hl_file1_$i
  done

  rm -rf $MNT/dir1/init_migration1
  rm -rf $MNT/dir2/init_migration2
  rm -rf $MNT/dir3/init_migration3

  flush_mds_journal
}

function setup_test_bed () {
  fuse_mount
  clean_data
  create_dirs_and_pin
}

if [ "$1" == "repeat" ]; then
  echo "Cleanup and repeating the test"
else
  echo "Setup ..."
  set_max_mds 3
fi

setup_test_bed
stress_test_hardlink
clean_data

num_data_pool_obj=$($RADOS -p cephfs.a.data ls --format=json-pretty | jq -r '. | length')
if [ "$num_data_pool_obj" -ne 0 ]; then
  echo "FAIL - All data pool objects should be deleted"
  exit
fi


echo "SUCCESS"
