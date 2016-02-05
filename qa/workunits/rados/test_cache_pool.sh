#!/bin/bash -x

set -e

expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

# create pools, set up tier relationship
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

# cleanup
ceph osd tier remove-overlay base_pool
ceph osd tier remove base_pool wrong_cache
ceph osd tier remove base_pool partial_wrong
ceph osd tier remove base_pool empty_cache
ceph osd pool delete base_pool base_pool --yes-i-really-really-mean-it
ceph osd pool delete empty_cache empty_cache --yes-i-really-really-mean-it
ceph osd pool delete wrong_cache wrong_cache --yes-i-really-really-mean-it
ceph osd pool delete partial_wrong partial_wrong --yes-i-really-really-mean-it

## set of base, cache
ceph osd pool create base 8
ceph osd pool create cache 8

ceph osd tier add base cache
ceph osd tier cache-mode cache writeback
ceph osd tier set-overlay base cache

# cache-flush, cache-evict
rados -p base put foo /etc/passwd
expect_false rados -p base cache-evict foo
expect_false rados -p base cache-flush foo
expect_false rados -p cache cache-evict foo
rados -p cache cache-flush foo
rados -p cache cache-evict foo
rados -p cache ls - | wc -l | grep 0

# cache-try-flush, cache-evict
rados -p base put foo /etc/passwd
expect_false rados -p base cache-evict foo
expect_false rados -p base cache-flush foo
expect_false rados -p cache cache-evict foo
rados -p cache cache-try-flush foo
rados -p cache cache-evict foo
rados -p cache ls - | wc -l | grep 0

# cache-flush-evict-all
rados -p base put bar /etc/passwd
rados -p cache ls - | wc -l | grep 1
expect_false rados -p base cache-flush-evict-all
rados -p cache cache-flush-evict-all
rados -p cache ls - | wc -l | grep 0

# cache-try-flush-evict-all
rados -p base put bar /etc/passwd
rados -p cache ls - | wc -l | grep 1
expect_false rados -p base cache-flush-evict-all
rados -p cache cache-try-flush-evict-all
rados -p cache ls - | wc -l | grep 0

# cleanup
ceph osd tier remove-overlay base
ceph osd tier remove base cache

ceph osd pool delete cache cache --yes-i-really-really-mean-it
ceph osd pool delete base base --yes-i-really-really-mean-it

# test multiple_tiering
ceph osd pool create base 2 2
ceph osd pool create middle 2 2
ceph osd pool create top 2 2

ceph osd tier add-cache base middle 2000
ceph osd tier add-cache middle top 2000
ceph osd pool set middle min_read_recency_for_promote 0
ceph osd pool set middle min_write_recency_for_promote 0
ceph osd pool set top min_read_recency_for_promote 0
ceph osd pool set top min_write_recency_for_promote 0

### middle writeback, top writeback
# put new obj
echo "bar" > /tmp/bar.txt
rados -p base put bar /tmp/bar.txt
rados -p top ls | grep bar
rados -p base get bar /tmp/exbar.txt
diff /tmp/bar.txt /tmp/exbar.txt

echo "foo" > /tmp/foo.txt
rados -p base put foo /tmp/foo.txt

# flush_evict
rados -p top cache-flush-evict-all
rados -p top ls | wc -l | grep -w 0
rados -p middle cache-flush-evict-all
rados -p top ls | wc -l | grep -w 0
rados -p middle ls | wc -l | grep -w 0
rados -p base ls | wc -l | grep -w 2

# object was on basepool, get or put
rados -p base get foo /tmp/exfoo.txt
diff /tmp/foo.txt /tmp/exfoo.txt
rados -p top ls | grep foo
rados -p middle ls | grep foo

echo "newbar" > /tmp/newbar.txt
rados -p base put bar /tmp/newbar.txt
rados -p top ls | grep bar
rados -p middle ls | grep bar
rados -p base get bar /tmp/exnewbar.txt
diff /tmp/newbar.txt /tmp/exnewbar.txt

### middle forward, top writeback
rados -p middle cache-flush-evict-all
rados -p middle ls | wc -l | grep -w 0
ceph osd tier cache-mode middle forward
rados -p top cache-flush-evict-all
rados -p middle ls | wc -l | grep -w 0
rados -p top ls | wc -l | grep -w 0

### both forward
ceph osd tier cache-mode top forward
rados -p base put bar /tmp/bar.txt
rados -p middle ls | wc -l | grep -w 0
rados -p top ls | wc -l | grep -w 0
rm /tmp/exbar.txt
rados -p base get bar /tmp/exbar.txt
git diff /tmp/bar.txt /tmp/exbar.txt
rados -p middle ls | wc -l | grep -w 0
rados -p top ls | wc -l | grep -w 0

### middle writeback, top forward
ceph osd tier cache-mode middle writeback
rados -p base put bar /tmp/newbar.txt
rados -p middle ls | grep bar
rados -p top ls | wc -l | grep -w 0
rm /tmp/exnewbar.txt
rados -p base get bar /tmp/exnewbar.txt
diff /tmp/newbar.txt /tmp/exnewbar.txt
rados -p top ls | wc -l | grep -w 0

### middle forward, top writeback
ceph osd tier cache-mode middle forward
ceph osd tier cache-mode top writeback
rm /tmp/exfoo.txt
rados -p base get foo /tmp/exfoo.txt
diff /tmp/foo.txt /tmp/exfoo.txt
rados -p top ls | grep foo
! rados -p middle ls | grep foo

rados -p base put bar /tmp/bar.txt
rm /tmp/exbar.txt
rados -p base get bar /tmp/exbar.txt
git diff /tmp/bar.txt /tmp/exbar.txt
rados -p top ls | grep bar

### both forward
ceph osd tier cache-mode top forward
rados -p top cache-flush-evict-all
rados -p top ls | wc -l | grep -w 0
rados -p middle ls | wc -l | grep -w 1
rados -p middle ls | grep bar

rados -p middle cache-flush-evict-all
rados -p top ls | wc -l | grep -w 0
rados -p middle ls | wc -l | grep -w 0
rados -p base ls | wc -l | grep -w 2

# clean up
ceph osd tier remove-overlay middle
ceph osd tier remove middle top
ceph osd tier remove-overlay base
ceph osd tier remove base middle
ceph osd pool delete base base --yes-i-really-really-mean-it
ceph osd pool delete middle middle --yes-i-really-really-mean-it
ceph osd pool delete top top --yes-i-really-really-mean-it

echo OK
