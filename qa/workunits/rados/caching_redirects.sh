#!/bin/bash -x

set -e

expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}


#create pools, set up tier relationship
ceph osd pool create base_pool 2
ceph osd pool create partial_cache 2
ceph osd pool create data_cache 2
ceph osd tier add base_pool partial_cache
ceph osd tier add base_pool data_cache

# populate base_pool and data_cache with some data
echo "foo" > foo.txt
echo "bar" > bar.txt
echo "baz" > baz.txt
rados -p base_pool put fooobj foo.txt
rados -p base_pool put barobj bar.txt
# data_cache is backwards so we can tell we read from it
rados -p data_cache put fooobj bar.txt
rados -p data_cache put barobj foo.txt
# partial_cache gets barobj backwards
rados -p partial_cache put barobj foo.txt

# get the objects back before setting a caching pool
rados -p base_pool get fooobj tmp.txt
diff -q tmp.txt foo.txt
rados -p base_pool get barobj tmp.txt
diff -q tmp.txt bar.txt

# set up redirect and make sure we get redirect-based results
ceph osd tier set-overlay base_pool partial_cache
ceph osd tier cache-mode partial_cache writeback
rados -p base_pool get fooobj tmp.txt
diff -q tmp.txt foo.txt
rados -p base_pool get barobj tmp.txt
diff -q tmp.txt foo.txt

# try a nonexistent object and make sure we get an error
expect_false rados -p base_pool get bazobj tmp.txt

# switch cache pools and make sure contents differ
ceph osd tier remove-overlay base_pool
ceph osd tier set-overlay base_pool data_cache
ceph osd tier cache-mode data_cache writeback
rados -p base_pool get fooobj tmp.txt
diff -q tmp.txt bar.txt
rados -p base_pool get barobj tmp.txt
diff -q tmp.txt foo.txt

# drop the cache entirely and make sure contents are still the same
ceph osd tier remove-overlay base_pool
rados -p base_pool get fooobj tmp.txt
diff -q tmp.txt foo.txt
rados -p base_pool get barobj tmp.txt
diff -q tmp.txt bar.txt
