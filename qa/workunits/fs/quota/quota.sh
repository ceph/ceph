#!/usr/bin/env bash

set -ex

function expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

function write_file()
{
	set +x
	for ((i=1;i<=$2;i++))
	do
		dd if=/dev/zero of=$1 bs=1M count=1 conv=notrunc oflag=append 2>/dev/null >/dev/null
		if [ $? != 0 ]; then
			echo Try to write $(($i * 1048576))
			set -x
			return 1
		fi
		sleep 0.05
	done
	set -x
	return 0
}

mkdir quota-test
cd quota-test

# bytes
setfattr . -n ceph.quota.max_bytes -v 100M
expect_false write_file big 1000     # 1g
expect_false write_file second 10
setfattr . -n ceph.quota.max_bytes -v 0
dd if=/dev/zero of=third bs=1M count=10
dd if=/dev/zero of=big2 bs=1M count=100


rm -rf *

# files
setfattr . -n ceph.quota.max_files -v 5
mkdir ok
touch ok/1
touch ok/2
touch 3
expect_false touch shouldbefail     #  5 files will include the "."
expect_false touch ok/shouldbefail  #  5 files will include the "."
setfattr . -n ceph.quota.max_files -v 0
touch shouldbecreated
touch shouldbecreated2


rm -rf *

# mix
mkdir bytes bytes/files

setfattr bytes -n ceph.quota.max_bytes -v 10M
setfattr bytes/files -n ceph.quota.max_files -v 5
dd if=/dev/zero of=bytes/files/1 bs=1M count=4
dd if=/dev/zero of=bytes/files/2 bs=1M count=4
expect_false write_file bytes/files/3 1000
expect_false write_file bytes/files/4 1000
expect_false write_file bytes/files/5 1000
stat --printf="%n %s\n" bytes/files/1 #4M
stat --printf="%n %s\n" bytes/files/2 #4M
stat --printf="%n %s\n" bytes/files/3 #bigger than 2M
stat --printf="%n %s\n" bytes/files/4 #should be zero
expect_false stat bytes/files/5       #shouldn't be exist




rm -rf *

#mv
mkdir files limit
truncate files/file -s 10G
setfattr limit -n ceph.quota.max_bytes -v 1M
expect_false mv files limit/



rm -rf *

#limit by ancestor

mkdir -p ancestor/p1/p2/parent/p3
setfattr ancestor -n ceph.quota.max_bytes -v 1M
setfattr ancestor/p1/p2/parent -n ceph.quota.max_bytes -v 1G
expect_false write_file ancestor/p1/p2/parent/p3/file1 900 #900m
stat --printf="%n %s\n" ancestor/p1/p2/parent/p3/file1


#get/set attribute

setfattr -n ceph.quota.max_bytes -v 0 .
setfattr -n ceph.quota.max_bytes -v 1 .
setfattr -n ceph.quota.max_bytes -v 9223372036854775807 .
expect_false setfattr -n ceph.quota.max_bytes -v 9223372036854775808 .
expect_false setfattr -n ceph.quota.max_bytes -v -1 .
expect_false setfattr -n ceph.quota.max_bytes -v -9223372036854775808 .
expect_false setfattr -n ceph.quota.max_bytes -v -9223372036854775809 .

setfattr -n ceph.quota.max_bytes -v 0 .
setfattr -n ceph.quota.max_bytes -v 1Ti .
setfattr -n ceph.quota.max_bytes -v 8388607Ti .
expect_false setfattr -n ceph.quota.max_bytes -v 8388608Ti .
expect_false setfattr -n ceph.quota.max_bytes -v -1Ti .
expect_false setfattr -n ceph.quota.max_bytes -v -8388609Ti .
expect_false setfattr -n ceph.quota.max_bytes -v -8388610Ti .

setfattr -n ceph.quota.max_files -v 0 .
setfattr -n ceph.quota.max_files -v 1 .
setfattr -n ceph.quota.max_files -v 9223372036854775807 .
expect_false setfattr -n ceph.quota.max_files -v 9223372036854775808 .
expect_false setfattr -n ceph.quota.max_files -v -1 .
expect_false setfattr -n ceph.quota.max_files -v -9223372036854775808 .
expect_false setfattr -n ceph.quota.max_files -v -9223372036854775809 .

setfattr -n ceph.quota -v "max_bytes=0 max_files=0" .
setfattr -n ceph.quota -v "max_bytes=1 max_files=0" .
setfattr -n ceph.quota -v "max_bytes=0 max_files=1" .
setfattr -n ceph.quota -v "max_bytes=1 max_files=1" .
expect_false setfattr -n ceph.quota -v "max_bytes=-1 max_files=0" .
expect_false setfattr -n ceph.quota -v "max_bytes=0 max_files=-1" .
expect_false setfattr -n ceph.quota -v "max_bytes=-1 max_files=-1" .

#addme

cd ..
rm -rf quota-test

echo OK
