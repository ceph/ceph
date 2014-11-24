#!/bin/bash -ex

function expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

mkdir quota-test
cd quota-test

# bytes
setfattr . -n ceph.quota.max_bytes -v 100000000  # 100m
expect_false dd if=/dev/zero of=big bs=1M count=1000     # 1g
expect_false dd if=/dev/zero of=second bs=1M count=10
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

setfattr bytes -n ceph.quota.max_bytes -v 10000000   #10m
setfattr bytes/files -n ceph.quota.max_files -v 5
dd if=/dev/zero of=bytes/files/1 bs=1M count=4
dd if=/dev/zero of=bytes/files/2 bs=1M count=4
expect_false dd if=/dev/zero of=bytes/files/3 bs=1M count=1000
expect_false dd if=/dev/zero of=bytes/files/4 bs=1M count=1000
expect_false dd if=/dev/zero of=bytes/files/5 bs=1M count=1000
stat --printf="%n %s\n" bytes/files/1 #4M
stat --printf="%n %s\n" bytes/files/2 #4M
stat --printf="%n %s\n" bytes/files/3 #bigger than 2M
stat --printf="%n %s\n" bytes/files/4 #should be zero
expect_false stat bytes/files/5       #shouldn't be exist

mv bytes/files .
dd if=/dev/zero of=files/3 bs=1M count=4
dd if=/dev/zero of=files/4 bs=1M count=4
expect_false dd if=/dev/zero of=files/5 bs=1M count=4



rm -rf *

#mv
mkdir files limit
dd if=/dev/zero of=files/file bs=1M count=100
setfattr limit -n ceph.quota.max_bytes -v 1000000 #1m
expect_false mv files limit/



rm -rf *

#limit by ancestor

mkdir -p ancestor/p1/p2/parent/p3
setfattr ancestor -n ceph.quota.max_bytes -v 1000000
setfattr ancestor/p1/p2/parent -n ceph.quota.max_bytes -v 1000000000 #1g
expect_false dd if=/dev/zero of=ancestor/p1/p2/parent/p3/file1 bs=1M count=900 #900m
stat --printf="%n %s\n" ancestor/p1/p2/parent/p3/file1

#addme

cd ..
rm -rf quota-test

echo OK
