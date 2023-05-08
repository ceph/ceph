#fuse mount
echo "--------------------------------------------------------------------------------------------------"
echo "Fuse mount at /mnt"
sudo umount -f /mnt
sleep 5
sudo bin/ceph-fuse -c ./ceph.conf /mnt 2>/dev/null


#LINK - Create a link
echo "--------------------------------------------------------------------------------------------------"
echo "LINK"
#Create a file
echo "create a file - /mnt/file1"
echo "efghi" > /mnt/file1
echo "create a link ln /mnt/file1 /mnt/hl_file1"
ln /mnt/file1 /mnt/hl_file1
mkdir /mnt/dir1
echo "create a link ln /mnt/file1 /mnt/dir1/hl_file2"
ln /mnt/file1 /mnt/dir1/hl_file2

#Flush the journal
echo "flush the journal"
bin/ceph tell mds.a flush journal > /dev/null 2>&1

#Verify inode size for secondary link at root
isize=$(bin/rados -p cephfs.a.meta getomapval 1.00000000 hl_file1_head 2>/dev/null | grep value | awk '{ gsub(/[^0-9]+/, ""); print $0 }')
if [ $isize -ne 512 ]; then
  echo "FAIL - secondary link inode size didn't match - EXITING TESTCASE"
  exit -1
else
  echo "/mnt/hl_file1 referent inode size matched - 512"
fi

#Verify data inode and backtrace at root
bin/rados -p cephfs.a.meta getomapval 1.00000000 hl_file1_head /tmp/a
secinode=$(bin/ceph-dencoder type 'inode_t<std::allocator>' skip 25 import /tmp/a decode dump_json | jq '.ino')
hex_secinode=$(printf "%x" $secinode)
hl_file1_dataobject=$(echo $hex_secinode.00000000)
bin/rados -p cephfs.a.data getxattr $hl_file1_dataobject parent > /tmp/a
bt=$(bin/ceph-dencoder type 'inode_backtrace_t' import /tmp/a decode dump_json)
bt_ino=$(echo $bt | jq '.ino')
if [ $secinode -ne $bt_ino ]; then
  echo "FAIL - secondary inode number didn't match in the backtrace - EXITING TESTCASE"
  exit -1
else
  echo "/mnt/hl_file1 referent inode number from metadata inode matches the backtrace inode number"
fi

dname=$(bin/ceph-dencoder type 'inode_backtrace_t' import /tmp/a decode dump_json | jq '.ancestors[0].dname')
if [ "$dname" != '"hl_file1"' ]; then
  echo "FAIL - dname "$dname" didn't match '"hl_file1"' in the backtrace - EXITING TESTCASE"
  exit -1
else
  echo "basename of /mnt/hl_file1 matches in the backtrace"
fi
parentino=$(bin/ceph-dencoder type 'inode_backtrace_t' import /tmp/a decode dump_json | jq '.ancestors[0].dirino')
if [ $parentino -ne 1 ]; then
  echo "FAIL - parentino $parentino didn't match root inode number in the backtrace - EXITING TESTCASE"
  exit -1
else
  echo "parent of /mnt/hl_file1 matches the root inode in the backtrace"
fi

echo "OK - link at root"
echo "--------------------------------------------------------------------------------------------------"

#Verify inode size for secondary link at subdir
dir1_object=$(bin/rados -p cephfs.a.meta ls | egrep "([0-9]|[a-f]){11}.")
isize=$(bin/rados -p cephfs.a.meta getomapval $dir1_object hl_file2_head 2>/dev/null | grep value | awk '{ gsub(/[^0-9]+/, ""); print $0 }')
if [ $isize -ne 512 ]; then
  echo "FAIL - secondary link inode size didn't match - EXITING TESTCASE"
  exit -1
else
  echo "/mnt/dir1/hl_file2 referent inode size matched - 512"
fi

#Verify data inode and backtrace at subdir
bin/rados -p cephfs.a.meta getomapval $dir1_object hl_file2_head /tmp/a
secinode=$(bin/ceph-dencoder type 'inode_t<std::allocator>' skip 25 import /tmp/a decode dump_json | jq '.ino')
hex_secinode=$(printf "%x" $secinode)
dir1_hl_file2_dataobject=$(echo $hex_secinode.00000000)
bin/rados -p cephfs.a.data getxattr $dir1_hl_file2_dataobject parent > /tmp/a
bt=$(bin/ceph-dencoder type 'inode_backtrace_t' import /tmp/a decode dump_json)
bt_ino=$(echo $bt | jq '.ino')
if [ $secinode -ne $bt_ino ]; then
  echo "FAIL - secondary inode number at subdir didn't match in the backtrace - EXITING TESTCASE"
  exit -1
else
  echo "/mnt/dir1/hl_file2 referent inode number from metadata inode matches the backtrace inode number"
fi

#Verify backtrace '/dir1/hl_file2'
p0=$(bin/ceph-dencoder type 'inode_backtrace_t' import /tmp/a decode dump_json | jq '.ancestors[0].dname')
p1=$(bin/ceph-dencoder type 'inode_backtrace_t' import /tmp/a decode dump_json | jq '.ancestors[1].dname')
if [ "$p0" != '"hl_file2"' ]; then
  echo "FAIL - dname "$p0" didn't match '"hl_file2"' in the backtrace - EXITING TESTCASE"
  exit -1
else
  echo "basename of /mnt/dir1/hl_file2 matches in the backtrace"
fi
if [ "$p1" != '"dir1"' ]; then
  echo "FAIL - dname "$p0" didn't match '"dir1"' in the backtrace - EXITING TESTCASE"
  exit -1
else
  echo "parent of /mnt/dir1/hl_file2 matches in the backtrace"
fi
parentino=$(bin/ceph-dencoder type 'inode_backtrace_t' import /tmp/a decode dump_json | jq '.ancestors[1].dirino')
if [ $parentino -ne 1 ]; then
  echo "FAIL - parentino $parentino didn't match root inode number in the backtrace for subdir link - EXITING TESTCASE"
  exit -1
else
  echo "parent of /mnt/dir1 matches the root inode in the backtrace"
fi

echo "OK - link at subdir"
echo "--------------------------------------------------------------------------------------------------"

##--------------------------------------------------------------------------------------------------------##
#mds restart
sudo umount -f /mnt
if df /mnt | grep -q "ceph-fuse";then
  echo "unmount failed"
  exit -1
else
  echo "unmount successful"
fi
echo "mds restart - waiting for mds to come up"
../src/stop.sh >/dev/null 2>&1
MDS=1 OSD=1 MON=1 ../src/vstart.sh -d >/dev/null 2>&1
echo "sleep for 5 seconds after cluster up for mds to come up, so mount is successful"
sleep 5
sudo bin/ceph-fuse -c ./ceph.conf /mnt
if df /mnt | grep -q "ceph-fuse";then
  echo "mount successful"
else
  echo "mount failed"
  exit -1
fi

#Validate the data
orig_data="efghi"
data=`cat /mnt/file1`
hl_data1=`cat /mnt/hl_file1`
hl_data2=`cat /mnt/dir1/hl_file2`

if [ "$data" != "$orig_data" ] || [ "$hl_data1" != "$orig_data" ] || [ "$hl_data2" != "$orig_data" ];then
  echo "FAIL - data read from primary /file1 didn't match after mds restart"
  exit -1
else
  echo "data read from primary /file1 matches after mds restart"
fi

if [ "$hl_data1" != "$orig_data" ] || [ "$hl_data2" != "$orig_data" ];then
  echo "FAIL - data read from secondary /hl_file1 didn't match after mds restart"
  exit -1
else
  echo "data read from secondary /hl_file1 matches after mds restart"
fi

if [ "$hl_data2" != "$orig_data" ];then
  echo "FAIL - data read from secondary /dir1/hl_file2 didn't match after mds restart"
  exit -1
else
  echo "data read from primary /dir1/hl_file2 matches after mds restart"
fi

echo "OK - data read passed after mds restart"
echo "--------------------------------------------------------------------------------------------------"
##--------------------------------------------------------------------------------------------------------##

#UNLINK of hardlink file

echo "UNLINK"
#remember primary inode object
bin/rados -p cephfs.a.meta getomapval 1.00000000 file1_head /tmp/a
pinode=$(bin/ceph-dencoder type 'inode_t<std::allocator>' skip 25 import /tmp/a decode dump_json | jq '.ino')
hex_pinode=$(printf "%x" $pinode)
file1_dataobject=$(echo $hex_pinode.00000000)

echo "unlink /mnt/file1"
rm -f /mnt/file1
echo "unlink /mnt/hl_file1"
rm -f /mnt/hl_file1
echo "unlink /mnt/dir1/hl_file2"
rm -f /mnt/dir1/hl_file2
#flush the journal
bin/ceph tell mds.a flush journal >/dev/null 2>&1


echo "Sleep for 10 seconds for unlink to clean up the objects"
sleep 10
bin/rados -p cephfs.a.meta getomapval 1.00000000 file1_head > /dev/null 2>&1
if [ $? -ne 0 ];then
  echo "/file1 file1_head omap key deleted"
else
  echo "FAIL - /file1's omap key still present"
  exit -1
fi
bin/rados -p cephfs.a.meta getomapval 1.00000000 hl_file1_head > /dev/null 2>&1
if [ $? -ne 0 ];then
  echo "/hl_file1 hl_file1_head omap key deleted"
else
  echo "FAIL - /hl_file1's omap key still present"
  exit -1
fi
bin/rados -p cephfs.a.meta getomapval $dir1_object hl_file2_head > /dev/null 2>&1
if [ $? -ne 0 ];then
  echo "/dir1/hl_file2 hl_file2_head omap key deleted"
else
  echo "FAIL - /dir1/hl_file2's omap key still present"
  exit -1
fi

bin/rados -p cephfs.a.data getxattr $file1_dataobject parent > /tmp/a
if [ $? -ne 0 ]; then
 echo "/file1 data object deleted after unlink"
else
 echo "FAIL - /file1 data object still found after unlink"
 exit -1
fi
bin/rados -p cephfs.a.data getxattr $hl_file1_dataobject parent > /tmp/a
if [ $? -ne 0 ]; then
 echo "/hl_file1 data object deleted after unlink"
else
 echo "FAIL - /hl_file1 data object still found after unlink"
 exit -1
fi
bin/rados -p cephfs.a.data getxattr $dir1_hl_file2_dataobject parent > /tmp/a
if [ $? -ne 0 ]; then
 echo "/dir1/hl_file2 data object deleted after unlink"
else
 echo "FAIL - /dir1/hl_file2 data object still found after unlink"
 exit -1
fi

echo "OK - unlink success"
echo "--------------------------------------------------------------------------------------------------"


#rename
echo "RENAME"
#Create a file
echo "create a file - /mnt/file1"
echo "efghi" > /mnt/file1
#create a link
echo "create a link - ln /mnt/file1 /mnt/hl_file2"
ln /mnt/file1 /mnt/hl_file1
ln /mnt/file1 /mnt/hl_file2
mkdir /mnt/dir1
#rename at root level
mv /mnt/hl_file1 /mnt/rn_hl_file1
#flush the journal
bin/ceph tell mds.a flush journal >/dev/null 2>&1

#Verify inode size for renamed secondary link at root
isize=$(bin/rados -p cephfs.a.meta getomapval 1.00000000 rn_hl_file1_head 2>/dev/null | grep value | awk '{ gsub(/[^0-9]+/, ""); print $0 }')
if [ $isize -ne 512 ]; then
  echo "FAIL - renamed secondary link inode size didn't match - EXITING TESTCASE"
  exit -1
else
  echo "rename /hl_file1 /rn_hl_file1 - renamed referent inode size matched - 512"
fi

#Verify renamed data inode and backtrace at root
bin/rados -p cephfs.a.meta getomapval 1.00000000 rn_hl_file1_head /tmp/a
secinode=$(bin/ceph-dencoder type 'inode_t<std::allocator>' skip 25 import /tmp/a decode dump_json | jq '.ino')
hex_secinode=$(printf "%x" $secinode)
rn_hl_file1_dataobject=$(echo $hex_secinode.00000000)
bin/rados -p cephfs.a.data getxattr $rn_hl_file1_dataobject parent > /tmp/a
bt=$(bin/ceph-dencoder type 'inode_backtrace_t' import /tmp/a decode dump_json)
bt_ino=$(echo $bt | jq '.ino')
if [ $secinode -ne $bt_ino ]; then
  echo "FAIL - secondary inode number didn't match in the backtrace - EXITING TESTCASE"
  exit -1
else
  echo "/mnt/hl_file1 referent inode number from metadata inode matches the backtrace inode number"
fi

dname=$(echo $bt | jq '.ancestors[0].dname')
if [ "$dname" != '"rn_hl_file1"' ]; then
  echo "FAIL - dname "$dname" didn't match '"rn_hl_file1"' in the backtrace - EXITING TESTCASE"
  exit -1
else
  echo "basename of /rn_hl_file1 matches in the backtrace"
fi
parentino=$(echo $bt | jq '.ancestors[0].dirino')
if [ $parentino -ne 1 ]; then
  echo "FAIL - parentino $parentino didn't match root inode number in the backtrace - EXITING TESTCASE"
  exit -1
else
  echo "parent of /rn_hl_file1 matches the root inode in the backtrace"
fi

echo "OK - rename at root"
echo "--------------------------------------------------------------------------------------------------"


#rename the hardlink file at subdir
echo "rename the hardlink file /mnt/hl_file2 to /mnt/dir1/hl_file2"
mv /mnt/hl_file2 /mnt/dir1/rn_hl_file2
#flush the journal
bin/ceph tell mds.a flush journal >/dev/null 2>&1

#Verify renamed data inode and backtrace at subdir
bin/rados -p cephfs.a.meta getomapval $dir1_object rn_hl_file2_head /tmp/a
secinode=$(bin/ceph-dencoder type 'inode_t<std::allocator>' skip 25 import /tmp/a decode dump_json | jq '.ino')
hex_secinode=$(printf "%x" $secinode)
dir1_rn_hl_file2_dataobject=$(echo $hex_secinode.00000000)
bin/rados -p cephfs.a.data getxattr $dir1_rn_hl_file2_dataobject parent > /tmp/a
bt=$(bin/ceph-dencoder type 'inode_backtrace_t' import /tmp/a decode dump_json)
bt_ino=$(echo $bt | jq '.ino')
if [ $secinode -ne $bt_ino ]; then
  echo "FAIL - renamed secondary inode number at subdir didn't match in the backtrace - EXITING TESTCASE"
  exit -1
else
  echo "/dir1/rn_hl_file2 renamed referent inode number from metadata inode matches the backtrace inode number"
fi

#Verify backtrace '/dir1/rn_hl_file2'
p0=$(echo $bt | jq '.ancestors[0].dname')
p1=$(echo $bt | jq '.ancestors[1].dname')
if [ "$p0" != '"rn_hl_file2"' ]; then
  echo "FAIL - dname "$p0" didn't match '"rn_hl_file2"' in the backtrace - EXITING TESTCASE"
  exit -1
else
  echo "basename of /dir1/rn_hl_file2 matches in the backtrace"
fi
if [ "$p1" != '"dir1"' ]; then
  echo "FAIL - dname "$p0" didn't match '"dir1"' in the backtrace - EXITING TESTCASE"
  exit -1
else
  echo "parent of /dir1/rn_hl_file2 matches in the backtrace"
fi
parentino=$(bin/ceph-dencoder type 'inode_backtrace_t' import /tmp/a decode dump_json | jq '.ancestors[1].dirino')
if [ $parentino -ne 1 ]; then
  echo "FAIL - parentino $parentino didn't match root inode number in the backtrace for renamed subdir link - EXITING TESTCASE"
  exit -1
else
  echo "parent of /dir1 matches the root inode in the backtrace"
fi
echo "OK - rename at subdir"
echo "--------------------------------------------------------------------------------------------------"

#cleanup
echo "CLEANUP"
rm -rf /mnt/*
#flush the journal
bin/ceph tell mds.a flush journal >/dev/null 2>&1

data_objcount=$(bin/rados -p cephfs.a.data ls 2>/dev/null | wc -l)
if [ $data_objcount -ne 0 ];then
  echo "OK - cleanup all data objects are deleted"
else
  echo "FAIL - $data_objcount data objects still present"
  exit -1
fi
