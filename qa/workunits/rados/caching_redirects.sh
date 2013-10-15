#!/bin/bash -x

set -e

expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}


#create pools, set up tier relationship
ceph osd pool create base_pool 2
ceph osd pool create partial_wrong 2
ceph osd pool create wrong_cache 2
ceph osd tier add base_pool partial_wrong
ceph osd tier add base_pool wrong_cache

# populate base_pool with some data
echo "foo" > foo.txt
echo "bar" > bar.txt
echo "baz" > baz.txt
rados -p base_pool put fooobj foo.txt
rados -p base_pool put barobj bar.txt
# fill in wrong_cache backwards so we can tell we read from it
rados -p wrong_cache put fooobj bar.txt
rados -p wrong_cache put barobj foo.txt
# partial_wrong gets barobj backwards so we can check promote and non-promote
rados -p partial_wrong put barobj foo.txt

# get the objects back before setting a caching pool
rados -p base_pool get fooobj tmp.txt
diff -q tmp.txt foo.txt
rados -p base_pool get barobj tmp.txt
diff -q tmp.txt bar.txt

# set up redirect and make sure we get backwards results
ceph osd tier set-overlay base_pool wrong_cache
ceph osd tier cache-mode wrong_cache writeback
rados -p base_pool get fooobj tmp.txt
diff -q tmp.txt bar.txt
rados -p base_pool get barobj tmp.txt
diff -q tmp.txt foo.txt

# switch cache pools and make sure we're doing promote
ceph osd tier remove-overlay base_pool
ceph osd tier set-overlay base_pool partial_wrong
ceph osd tier cache-mode partial_wrong writeback
rados -p base_pool get fooobj tmp.txt
diff -q tmp.txt foo.txt # hurray, it promoted!
rados -p base_pool get barobj tmp.txt
diff -q tmp.txt foo.txt # yep, we read partial_wrong's local object!

# try a nonexistent object and make sure we get an error
expect_false rados -p base_pool get bazobj tmp.txt

# drop the cache entirely and make sure contents are still the same
ceph osd tier remove-overlay base_pool
rados -p base_pool get fooobj tmp.txt
diff -q tmp.txt foo.txt
rados -p base_pool get barobj tmp.txt
diff -q tmp.txt bar.txt

# create an empty cache pool and make sure it has objects after reading
ceph osd pool create empty_cache 2

touch empty.txt
rados -p empty_cache ls > tmp.txt
diff -q tmp.txt empty.txt

ceph osd tier add base_pool empty_cache
ceph osd tier set-overlay base_pool empty_cache
ceph osd tier cache-mode empty_cache writeback
rados -p base_pool get fooobj tmp.txt
rados -p base_pool get barobj tmp.txt
expect_false rados -p base_pool get bazobj tmp.txt

rados -p empty_cache ls > tmp.txt
expect_false diff -q tmp.txt empty.txt
rados -p base_pool ls > tmp2.txt
diff -q tmp.txt tmp2.txt