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
  echo "flush mds.a journal"
  $CEPH tell mds.a flush journal > /dev/null 2>&1
  echo "flush mds.b journal"
  $CEPH tell mds.b flush journal > /dev/null 2>&1
  echo "flush mds.c journal"
  $CEPH tell mds.c flush journal > /dev/null 2>&1
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

function get_data_object () {
  $RADOS -p cephfs.a.meta getomapval $1 $2 /tmp/a
  pinode=$(bin/ceph-dencoder type 'inode_t<std::allocator>' skip 25 import /tmp/a decode dump_json | jq '.ino')
  hex_pinode=$(printf "%x" $pinode)
  dataobject=$(echo $hex_pinode.00000000)
  echo "$dataobject"
}

function validate_mpool_omap_deletion () {
  $RADOS -p cephfs.a.meta getomapval $1 $2_head /tmp/a
  if [ $? -ne 0 ];then
    echo "/$3/$2's omap key deleted"
  else
    echo "FAIL - /$3/$2's omap key/val still present"
    exit -1
  fi
}

function validate_dpool_object_deletion () {
  $RADOS -p cephfs.a.data getxattr $1 parent > /tmp/a
  if [ $? -ne 0 ];then
    echo "$2's data object deleted after unlink"
  else
    echo "FAIL - $2's data object still present after unlink"
    exit -1
  fi
}

function validate_unlink_link_merge() {
  echo "--------------------------------------------------------------------------------------------------"
  echo "MULTIMDS - UNLINK - LINKMERGE"
  echo "Create file dir1/file1 and links dir1/hl_file1 dir2/hl_file1 dir3/hl_file1"
  echo "create /dir1/file1"
  echo "data ..." > $MNT/dir1/file1
  echo "ln /dir1/file1 /dir2/hl_file1"
  ln $MNT/dir1/file1 $MNT/dir2/hl_file1
  echo "ln /dir1/file1 /dir3/hl_file1"
  ln $MNT/dir1/file1 $MNT/dir3/hl_file1
  echo "ln /dir1/file1 /dir1/hl_file1"
  ln $MNT/dir1/file1 $MNT/dir1/hl_file1
  flush_mds_journal

  echo "remove primary link - /dir1/file1 to cause stray reintegration"
  rm -f $MNT/dir1/file1
  flush_mds_journal
  validate_mpool_omap_deletion $DIR1_MDS_OBJ "file1" "dir1"
  validate_dpool_object_deletion $DIR1_FILE1_DATA_OBJ "/dir1/file1"

  echo "remove other links"
  echo "remove /dir1/hl_file1"
  rm -f $MNT/dir1/hl_file1
  flush_mds_journal
  validate_mpool_omap_deletion $DIR1_MDS_OBJ "hl_file1" "dir1"
  validate_dpool_object_deletion $DIR1_HL_FILE1_DATA_OBJ "/dir1/hl_file1"

  echo "remove /dir2/hl_file1"
  rm -f $MNT/dir2/hl_file1
  flush_mds_journal
  validate_mpool_omap_deletion $DIR2_MDS_OBJ "hl_file1" "dir2"
  validate_dpool_object_deletion $DIR2_HL_FILE1_DATA_OBJ "/dir2/hl_file1"

  echo "remove /dir3/hl_file1"
  rm -f $MNT/dir3/hl_file1
  flush_mds_journal
  validate_mpool_omap_deletion $DIR3_MDS_OBJ "hl_file1" "dir3"
  validate_dpool_object_deletion $DIR3_HL_FILE1_DATA_OBJ "/dir3/hl_file1"
  echo "multi-mds unlink-stray reintegration success"
}

function validate_unlink () {
  echo "--------------------------------------------------------------------------------------------------"
  echo "MULTIMDS - UNLINK"
  echo "unlink /dir2/hl_file1 (secondary link first to avoid stray reintegration)"
  rm -f $MNT/dir2/hl_file1
  rm -f $MNT/dir1/hl_file1
  rm -f $MNT/dir3/hl_file1
  echo "unlink /dir1/file1 (primary link)"
  rm -f $MNT/dir1/file1
  flush_mds_journal
  echo "Sleep for 5 seconds for unlink to clean up the objects"
  sleep 5
  validate_mpool_omap_deletion $DIR1_MDS_OBJ "file1" "dir1"
  validate_mpool_omap_deletion $DIR2_MDS_OBJ "hl_file1" "dir2"
  validate_mpool_omap_deletion $DIR1_MDS_OBJ "hl_file1" "dir1"
  validate_mpool_omap_deletion $DIR3_MDS_OBJ "hl_file1" "dir3"

  validate_dpool_object_deletion $DIR1_FILE1_DATA_OBJ "/dir1/file1"
  validate_dpool_object_deletion $DIR2_HL_FILE1_DATA_OBJ "/dir2/hl_file1"
  validate_dpool_object_deletion $DIR1_HL_FILE1_DATA_OBJ "/dir1/hl_file1"
  validate_dpool_object_deletion $DIR3_HL_FILE1_DATA_OBJ "/dir3/hl_file1"
  echo "multi-mds unlink success"
}

function validate_rename () {
  echo "--------------------------------------------------------------------------------------------------"
  echo "MULTIMDS - RENAME"
  #create source files for rename
  echo "create /dir1/file1"
  echo "data ..." > $MNT/dir1/file1
  echo "ln /dir1/file1 /dir2/hl_file1"
  ln $MNT/dir1/file1 $MNT/dir2/hl_file1
  echo "ln /dir1/file1 /dir2/hl_file2"
  ln $MNT/dir1/file1 $MNT/dir2/hl_file2
  echo "ln /dir1/file1 /dir2/hl_file3"
  ln $MNT/dir1/file1 $MNT/dir2/hl_file3
  flush_mds_journal

  #rename
  echo "rename /dir2/hl_file1 /dir1/dir1_hl_file1"
  mv $MNT/dir2/hl_file1 $MNT/dir1/dir1_hl_file1
  echo "rename /dir2/hl_file2 /dir2/dir2_hl_file2"
  mv $MNT/dir2/hl_file2 $MNT/dir2/dir2_hl_file2
  echo "rename /dir2/hl_file3 /dir3/dir3_hl_file3"
  mv $MNT/dir2/hl_file3 $MNT/dir3/dir3_hl_file3
  flush_mds_journal

  validate_inode_size $DIR1_MDS_OBJ "dir1_hl_file1_head" "dir1"
  validate_data_inode_and_bt $DIR1_MDS_OBJ "dir1_hl_file1_head" "dir1" "dir1_hl_file1"

  validate_inode_size $DIR2_MDS_OBJ "dir2_hl_file2_head" "dir2"
  validate_data_inode_and_bt $DIR2_MDS_OBJ "dir2_hl_file2_head" "dir2" "dir2_hl_file2"

  validate_inode_size $DIR3_MDS_OBJ "dir3_hl_file3_head" "dir3"
  validate_data_inode_and_bt $DIR3_MDS_OBJ "dir3_hl_file3_head" "dir3" "dir3_hl_file3"

  echo "multi-mds rename success"
}

function validate_link () {
  echo "--------------------------------------------------------------------------------------------------"
  echo "MULTIMDS - LINK"
  echo "create /dir1/file1"
  echo "data ..." > $MNT/dir1/file1
  echo "ln /dir1/file1 /dir2/hl_file1"
  echo "mds-0 to mds-1"
  ln $MNT/dir1/file1 $MNT/dir2/hl_file1
  flush_mds_journal

  DIR1_FILE1_DATA_OBJ=$(get_data_object $DIR1_MDS_OBJ "file1_head")
  DIR2_HL_FILE1_DATA_OBJ=$(get_data_object $DIR2_MDS_OBJ "hl_file1_head")
  echo "DIR1_FILE1_DATA_OBJ: $DIR1_FILE1_DATA_OBJ"
  echo "DIR2_HL_FILE1_DATA_OBJ: $DIR2_HL_FILE1_DATA_OBJ"

  validate_inode_size $DIR2_MDS_OBJ "hl_file1_head" "dir2"
  validate_data_inode_and_bt $DIR2_MDS_OBJ "hl_file1_head" "dir2" "hl_file1"
  echo "multi-mds link (mds-0 to mds-1) success"

  echo "mds-0 to mds-2"
  ln $MNT/dir1/file1 $MNT/dir3/hl_file1
  flush_mds_journal

  DIR1_FILE1_DATA_OBJ=$(get_data_object $DIR1_MDS_OBJ "file1_head")
  DIR3_HL_FILE1_DATA_OBJ=$(get_data_object $DIR3_MDS_OBJ "hl_file1_head")
  echo "DIR1_FILE1_DATA_OBJ: $DIR1_FILE1_DATA_OBJ"
  echo "DIR3_HL_FILE1_DATA_OBJ: $DIR3_HL_FILE1_DATA_OBJ"

  validate_inode_size $DIR3_MDS_OBJ "hl_file1_head" "dir3"
  validate_data_inode_and_bt $DIR3_MDS_OBJ "hl_file1_head" "dir3" "hl_file1"
  echo "multi-mds link (mds-0 to mds-2) success"

  echo "mds-0 to mds-0"
  ln $MNT/dir1/file1 $MNT/dir1/hl_file1
  flush_mds_journal

  DIR1_FILE1_DATA_OBJ=$(get_data_object $DIR1_MDS_OBJ "file1_head")
  DIR1_HL_FILE1_DATA_OBJ=$(get_data_object $DIR1_MDS_OBJ "hl_file1_head")
  echo "DIR1_FILE1_DATA_OBJ: $DIR1_FILE1_DATA_OBJ"
  echo "DIR1_HL_FILE1_DATA_OBJ: $DIR1_HL_FILE1_DATA_OBJ"

  validate_inode_size $DIR1_MDS_OBJ "hl_file1_head" "dir1"
  validate_data_inode_and_bt $DIR1_MDS_OBJ "hl_file1_head" "dir1" "hl_file1"
  echo "multi-mds link (mds-0 to mds-0) success"
}

function create_dirs_and_pin () {
  echo "--------------------------------------------------------------------------------------------------"
  echo "create /dir1, /dir2 , /dir3 and pin /dir1 to rank 0, /dir2 to rank 1, /dir3 to rank2"
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
  echo "Wait 10 secs for other mds to become active"
  sleep 10
}

function create_sample_files () {
  echo "Create some files in /dir1, /dir2  and /dir3 to trigger subtree migration"
  for i in {1..10};do touch $MNT/dir1/file-$i;done
  for j in {1..10};do touch $MNT/dir2/file-$j;done
  for j in {1..10};do touch $MNT/dir3/file-$j;done
  rm -rf $MNT/dir1/file-*
  rm -rf $MNT/dir2/file-*
  rm -rf $MNT/dir3/file-*
  flush_mds_journal
}

function setup_test_bed () {
  fuse_mount
  clean_data
  create_dirs_and_pin
  create_sample_files
  echo "Wait for subtree migration"
  sleep 10
}

if [ "$1" == "repeat" ]; then
  echo "Cleanup and repeating the test"
else
  echo "Setup ..."
  set_max_mds 3
fi
setup_test_bed
validate_link
validate_unlink
validate_unlink_link_merge
#validate_rename

echo "--------------------------------------------------------------------------------------------------"
echo ""
