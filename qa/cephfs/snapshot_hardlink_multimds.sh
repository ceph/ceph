MNT=/mnt
CEPH=bin/ceph
RADOS=bin/rados
FS=a
DIR1_MDS_OBJ=""
DIR2_MDS_OBJ=""

function flush_mds_journal () {
  echo "flush mds.a journal"
  $CEPH tell mds.a flush journal > /dev/null 2>&1
  echo "flush mds.b journal"
  $CEPH tell mds.b flush journal > /dev/null 2>&1
}

function validate_inode_size () {
  isize=$($RADOS -p cephfs.a.meta getomapval $1 $2 2>/dev/null | grep value | awk '{ gsub(/[^0-9]+/, ""); print $0 }')
  if [ $isize -ne 512 ]; then
    echo "FAIL - secondary link inode size didn't match for $1 $2- EXITING TESTCASE"
    exit -1
  else
    echo "referent inode size matched for $1  /$3/$2 - 512"
  fi
}

function validate_data_inode_and_bt () {
  $RADOS -p cephfs.a.meta getomapval $1 $2 /tmp/a
  refinode=$(bin/ceph-dencoder type 'inode_t<std::allocator>' skip 25 import /tmp/a decode dump_json | jq '.ino')
  hex_refinode=$(printf "%x" $refinode)
  dataobject=$(echo $hex_refinode.00000000)
  $RADOS -p cephfs.a.data getxattr $dataobject parent > /tmp/a
  bt=$(bin/ceph-dencoder type 'inode_backtrace_t' import /tmp/a decode dump_json)
  bt_ino=$(echo $bt | jq '.ino')
  if [ $refinode -ne $bt_ino ]; then
    echo "FAIL - referent inode number of /$3/$4  didn't match in the backtrace - EXITING TESTCASE"
    exit -1
  else
    echo "referent inode number of /$3/$4 from metadata inode matches the backtrace inode number"
  fi

  #Verify backtrace
  p0=$(echo $bt | jq '.ancestors[0].dname')
  p1=$(echo $bt | jq '.ancestors[1].dname')
  if [ "$p0" != \"$4\" ]; then
    echo "FAIL - dname "$p0" didn't match $4 in the backtrace - EXITING TESTCASE"
    exit -1
  else
    echo "basename of /$3/$4  matches in the backtrace"
  fi
  if [ "$p1" != \"$3\" ]; then
    echo "FAIL - dname "$p1" didn't match "$3" in the backtrace - EXITING TESTCASE"
    exit -1
  else
    echo "parent of /$3/$4 matches in the backtrace"
  fi
  parentino=$(echo $bt | jq '.ancestors[1].dirino')
  if [ $parentino -ne 1 ]; then
    echo "FAIL - parentino $parentino didn't match root inode number in the backtrace for multimds link - EXITING TESTCASE"
    exit -1
  else
    echo "parent of /$3 matches the root inode in the backtrace"
  fi
}

function create_link_and_validate () {
  echo "--------------------------------------------------------------------------------------------------"
  echo "MULTIMDS - LINK"
  echo "create /dir1/file1"
  echo "data ..." > $MNT/dir1/file1
  echo "ln /dir1/file1 /dir2/hl_file1"
  ln $MNT/dir1/file1 $MNT/dir2/hl_file1
  flush_mds_journal
  validate_inode_size $DIR2_MDS_OBJ "hl_file1_head" "dir2"
  validate_data_inode_and_bt $DIR2_MDS_OBJ "hl_file1_head" "dir2" "hl_file1"
}

function create_dirs_and_pin () {
  echo "--------------------------------------------------------------------------------------------------"
  echo "create /dir1, /dir2 and pin /dir1 to rank 0 and /dir2 to rank 1"
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
  rank0=$($CEPH fs get $FS 2>/dev/null | grep "mds\." | grep "{0" | awk '{print $1}')
  echo "static pin /dir1 to rank 0 - $rank0"
  setfattr -n ceph.dir.pin -v 0 $MNT/dir1
  rank1=$($CEPH fs get $FS 2>/dev/null | grep "mds\." | grep "{1" | awk '{print $1}')
  echo "static pin /dir2 to rank 1 - $rank1"
  setfattr -n ceph.dir.pin -v 1 $MNT/dir2
}

function fuse_mount () {
  echo "--------------------------------------------------------------------------------------------------"
  echo "Fuse mount at $MNT"
  sudo umount -f $MNT
  sleep 3
  sudo bin/ceph-fuse -c ./ceph.conf $MNT 2>/dev/null
}

function fuse_umount () {
  echo "--------------------------------------------------------------------------------------------------"
  echo "Fuse umount at $MNT"
  sudo umount -f $MNT
  sleep 3
}

function clean_data () {
  echo "--------------------------------------------------------------------------------------------------"
  echo "clean data ..."
  echo "rm -rf $MNT"
  rm -rf $MNT/*
  flush_mds_journal
  sleep 5
}

function set_max_mds () {
  echo "set max_mds to $1"
  $CEPH fs set $FS max_mds $1
  flush_mds_journal
  echo "Wait 5 secs for other mds to become active"
  sleep 5
}

function create_sample_files () {
  echo "Create some files in /dir1 and /dir2 to trigger subtree migration"
  for i in {1..10};do touch $MNT/dir1/file-$i;done
  for j in {1..10};do touch $MNT/dir2/file-$j;done
  rm -rf $MNT/dir1/file-*
  rm -rf $MNT/dir2/file-*
  flush_mds_journal
}

set_max_mds 2
fuse_mount
clean_data
create_dirs_and_pin
create_sample_files
echo "Wait for subtree migration"
sleep 10
create_link_and_validate
echo "--------------------------------------------------------------------------------------------------"
echo ""
