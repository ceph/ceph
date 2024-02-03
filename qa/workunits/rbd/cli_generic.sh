#!/usr/bin/env bash
set -ex

. $(dirname $0)/../../standalone/ceph-helpers.sh

export RBD_FORCE_ALLOW_V1=1

# make sure rbd pool is EMPTY.. this is a test script!!
rbd ls | wc -l | grep -v '^0$' && echo "nonempty rbd pool, aborting!  run this script on an empty test cluster only." && exit 1

IMGS="testimg1 testimg2 testimg3 testimg4 testimg5 testimg6 testimg-diff1 testimg-diff2 testimg-diff3 foo foo2 bar bar2 test1 test2 test3 test4 clone2"

expect_fail() {
    "$@" && return 1 || return 0
}

tiered=0
if ceph osd dump | grep ^pool | grep "'rbd'" | grep tier; then
    tiered=1
fi

remove_images() {
    for img in $IMGS
    do
        (rbd snap purge $img || true) >/dev/null 2>&1
        (rbd rm $img || true) >/dev/null 2>&1
    done
}

test_others() {
    echo "testing import, export, resize, and snapshots..."
    TMP_FILES="/tmp/img1 /tmp/img1.new /tmp/img2 /tmp/img2.new /tmp/img3 /tmp/img3.new /tmp/img-diff1.new /tmp/img-diff2.new /tmp/img-diff3.new /tmp/img1.snap1 /tmp/img1.snap1 /tmp/img-diff1.snap1"

    remove_images
    rm -f $TMP_FILES

    # create an image
    dd if=/bin/sh of=/tmp/img1 bs=1k count=1 seek=10
    dd if=/bin/dd of=/tmp/img1 bs=1k count=10 seek=100
    dd if=/bin/rm of=/tmp/img1 bs=1k count=100 seek=1000
    dd if=/bin/ls of=/tmp/img1 bs=1k seek=10000
    dd if=/bin/ln of=/tmp/img1 bs=1k seek=100000

    # import, snapshot
    rbd import $RBD_CREATE_ARGS /tmp/img1 testimg1
    rbd resize testimg1 --size=256 --allow-shrink
    rbd export testimg1 /tmp/img2
    rbd snap create testimg1 --snap=snap1
    rbd resize testimg1 --size=128 && exit 1 || true   # shrink should fail
    rbd resize testimg1 --size=128 --allow-shrink
    rbd export testimg1 /tmp/img3

    # info
    rbd info testimg1 | grep 'size 128 MiB'
    rbd info --snap=snap1 testimg1 | grep 'size 256 MiB'

    # export-diff
    rm -rf /tmp/diff-testimg1-1 /tmp/diff-testimg1-2
    rbd export-diff testimg1 --snap=snap1 /tmp/diff-testimg1-1
    rbd export-diff testimg1 --from-snap=snap1 /tmp/diff-testimg1-2

    # import-diff
    rbd create $RBD_CREATE_ARGS --size=1 testimg-diff1
    rbd import-diff --sparse-size 8K /tmp/diff-testimg1-1 testimg-diff1
    rbd import-diff --sparse-size 8K /tmp/diff-testimg1-2 testimg-diff1

    # info
    rbd info testimg1 | grep 'size 128 MiB'
    rbd info --snap=snap1 testimg1 | grep 'size 256 MiB'
    rbd info testimg-diff1 | grep 'size 128 MiB'
    rbd info --snap=snap1 testimg-diff1 | grep 'size 256 MiB'

    # make copies
    rbd copy testimg1 --snap=snap1 testimg2
    rbd copy testimg1 testimg3
    rbd copy testimg-diff1 --sparse-size 768K --snap=snap1 testimg-diff2
    rbd copy testimg-diff1 --sparse-size 768K testimg-diff3

    # verify the result
    rbd info testimg2 | grep 'size 256 MiB'
    rbd info testimg3 | grep 'size 128 MiB'
    rbd info testimg-diff2 | grep 'size 256 MiB'
    rbd info testimg-diff3 | grep 'size 128 MiB'

    # deep copies
    rbd deep copy testimg1 testimg4
    rbd deep copy testimg1 --snap=snap1 testimg5
    rbd info testimg4 | grep 'size 128 MiB'
    rbd info testimg5 | grep 'size 256 MiB'
    rbd snap ls testimg4 | grep -v 'SNAPID' | wc -l | grep 1
    rbd snap ls testimg4 | grep '.*snap1.*'

    rbd export testimg1 /tmp/img1.new
    rbd export testimg2 /tmp/img2.new
    rbd export testimg3 /tmp/img3.new
    rbd export testimg-diff1 /tmp/img-diff1.new
    rbd export testimg-diff2 /tmp/img-diff2.new
    rbd export testimg-diff3 /tmp/img-diff3.new

    cmp /tmp/img2 /tmp/img2.new
    cmp /tmp/img3 /tmp/img3.new
    cmp /tmp/img2 /tmp/img-diff2.new
    cmp /tmp/img3 /tmp/img-diff3.new

    # rollback
    rbd snap rollback --snap=snap1 testimg1
    rbd snap rollback --snap=snap1 testimg-diff1
    rbd info testimg1 | grep 'size 256 MiB'
    rbd info testimg-diff1 | grep 'size 256 MiB'
    rbd export testimg1 /tmp/img1.snap1
    rbd export testimg-diff1 /tmp/img-diff1.snap1
    cmp /tmp/img2 /tmp/img1.snap1
    cmp /tmp/img2 /tmp/img-diff1.snap1

    # test create, copy of zero-length images
    rbd rm testimg2
    rbd rm testimg3
    rbd create testimg2 -s 0
    rbd cp testimg2 testimg3
    rbd deep cp testimg2 testimg6

    # remove snapshots
    rbd snap rm --snap=snap1 testimg1
    rbd snap rm --snap=snap1 testimg-diff1
    rbd info --snap=snap1 testimg1 2>&1 | grep 'error setting snapshot context: (2) No such file or directory'
    rbd info --snap=snap1 testimg-diff1 2>&1 | grep 'error setting snapshot context: (2) No such file or directory'

    # sparsify
    rbd sparsify testimg1

    remove_images
    rm -f $TMP_FILES
}

test_rename() {
    echo "testing rename..."
    remove_images

    rbd create --image-format 1 -s 1 foo
    rbd create --image-format 2 -s 1 bar
    rbd rename foo foo2
    rbd rename foo2 bar 2>&1 | grep exists
    rbd rename bar bar2
    rbd rename bar2 foo2 2>&1 | grep exists

    ceph osd pool create rbd2 8
    rbd pool init rbd2
    rbd create -p rbd2 -s 1 foo
    rbd rename rbd2/foo rbd2/bar
    rbd -p rbd2 ls | grep bar
    rbd rename rbd2/bar foo
    rbd rename --pool rbd2 foo bar
    ! rbd rename rbd2/bar --dest-pool rbd foo
    rbd rename --pool rbd2 bar --dest-pool rbd2 foo
    rbd -p rbd2 ls | grep foo
    ceph osd pool rm rbd2 rbd2 --yes-i-really-really-mean-it

    remove_images
}

test_ls() {
    echo "testing ls..."
    remove_images

    rbd create --image-format 1 -s 1 test1
    rbd create --image-format 1 -s 1 test2
    rbd ls | grep test1
    rbd ls | grep test2
    rbd ls | wc -l | grep 2
    # look for fields in output of ls -l without worrying about space
    rbd ls -l | grep 'test1.*1 MiB.*1'
    rbd ls -l | grep 'test2.*1 MiB.*1'

    rbd rm test1
    rbd rm test2

    rbd create --image-format 2 -s 1 test1
    rbd create --image-format 2 -s 1 test2
    rbd ls | grep test1
    rbd ls | grep test2
    rbd ls | wc -l | grep 2
    rbd ls -l | grep 'test1.*1 MiB.*2'
    rbd ls -l | grep 'test2.*1 MiB.*2'

    rbd rm test1
    rbd rm test2

    rbd create --image-format 2 -s 1 test1
    rbd create --image-format 1 -s 1 test2
    rbd ls | grep test1
    rbd ls | grep test2
    rbd ls | wc -l | grep 2
    rbd ls -l | grep 'test1.*1 MiB.*2'
    rbd ls -l | grep 'test2.*1 MiB.*1'
    remove_images

    # test that many images can be shown by ls
    for i in $(seq -w 00 99); do
	rbd create image.$i -s 1
    done
    rbd ls | wc -l | grep 100
    rbd ls -l | grep image | wc -l | grep 100
    for i in $(seq -w 00 99); do
	rbd rm image.$i
    done

    for i in $(seq -w 00 99); do
	rbd create image.$i --image-format 2 -s 1
    done
    rbd ls | wc -l | grep 100
    rbd ls -l | grep image |  wc -l | grep 100
    for i in $(seq -w 00 99); do
	rbd rm image.$i
    done
}

test_remove() {
    echo "testing remove..."
    remove_images

    rbd remove "NOT_EXIST" && exit 1 || true	# remove should fail
    rbd create --image-format 1 -s 1 test1
    rbd rm test1
    rbd ls | wc -l | grep "^0$"

    rbd create --image-format 2 -s 1 test2
    rbd rm test2
    rbd ls | wc -l | grep "^0$"

    # check that remove succeeds even if it's
    # interrupted partway through. simulate this
    # by removing some objects manually.

    # remove with header missing (old format)
    rbd create --image-format 1 -s 1 test1
    rados rm -p rbd test1.rbd
    rbd rm test1
    rbd ls | wc -l | grep "^0$"

    if [ $tiered -eq 0 ]; then
        # remove with header missing
	rbd create --image-format 2 -s 1 test2
	HEADER=$(rados -p rbd ls | grep '^rbd_header')
	rados -p rbd rm $HEADER
	rbd rm test2
	rbd ls | wc -l | grep "^0$"

        # remove with id missing
	rbd create --image-format 2 -s 1 test2
	rados -p rbd rm rbd_id.test2
	rbd rm test2
	rbd ls | wc -l | grep "^0$"

        # remove with header and id missing
	rbd create --image-format 2 -s 1 test2
	HEADER=$(rados -p rbd ls | grep '^rbd_header')
	rados -p rbd rm $HEADER
	rados -p rbd rm rbd_id.test2
	rbd rm test2
	rbd ls | wc -l | grep "^0$"
    fi

    # remove with rbd_children object missing (and, by extension,
    # with child not mentioned in rbd_children)
    rbd create --image-format 2 -s 1 test2
    rbd snap create test2@snap
    rbd snap protect test2@snap
    rbd clone test2@snap clone --rbd-default-clone-format 1

    rados -p rbd rm rbd_children
    rbd rm clone
    rbd ls | grep clone | wc -l | grep '^0$'

    rbd snap unprotect test2@snap
    rbd snap rm test2@snap
    rbd rm test2
}

test_locking() {
    echo "testing locking..."
    remove_images

    rbd create $RBD_CREATE_ARGS -s 1 test1
    rbd lock list test1 | wc -l | grep '^0$'
    rbd lock add test1 id
    rbd lock list test1 | grep ' 1 '
    LOCKER=$(rbd lock list test1 | tail -n 1 | awk '{print $1;}')
    rbd lock remove test1 id $LOCKER
    rbd lock list test1 | wc -l | grep '^0$'

    rbd lock add test1 id --shared tag
    rbd lock list test1 | grep ' 1 '
    rbd lock add test1 id --shared tag
    rbd lock list test1 | grep ' 2 '
    rbd lock add test1 id2 --shared tag
    rbd lock list test1 | grep ' 3 '
    rbd lock list test1 | tail -n 1 | awk '{print $2, $1;}' | xargs rbd lock remove test1
    if rbd info test1 | grep -qE "features:.*exclusive"
    then
      # new locking functionality requires all locks to be released
      while [ -n "$(rbd lock list test1)" ]
      do
        rbd lock list test1 | tail -n 1 | awk '{print $2, $1;}' | xargs rbd lock remove test1
      done
    fi
    rbd rm test1
}

test_pool_image_args() {
    echo "testing pool and image args..."
    remove_images

    ceph osd pool delete test test --yes-i-really-really-mean-it || true
    ceph osd pool create test 32
    rbd pool init test
    truncate -s 1 /tmp/empty /tmp/empty@snap

    rbd ls | wc -l | grep 0
    rbd create -s 1 test1
    rbd ls | grep -q test1
    rbd import --image test2 /tmp/empty
    rbd ls | grep -q test2
    rbd --dest test3 import /tmp/empty
    rbd ls | grep -q test3
    rbd import /tmp/empty foo
    rbd ls | grep -q foo

    # should fail due to "destination snapname specified"
    rbd import --dest test/empty@snap /tmp/empty && exit 1 || true
    rbd import /tmp/empty test/empty@snap && exit 1 || true
    rbd import --image test/empty@snap /tmp/empty && exit 1 || true
    rbd import /tmp/empty@snap && exit 1 || true

    rbd ls test | wc -l | grep 0
    rbd import /tmp/empty test/test1
    rbd ls test | grep -q test1
    rbd -p test import /tmp/empty test2
    rbd ls test | grep -q test2
    rbd --image test3 -p test import /tmp/empty
    rbd ls test | grep -q test3
    rbd --image test4 -p test import /tmp/empty
    rbd ls test | grep -q test4
    rbd --dest test5 -p test import /tmp/empty
    rbd ls test | grep -q test5
    rbd --dest test6 --dest-pool test import /tmp/empty
    rbd ls test | grep -q test6
    rbd --image test7 --dest-pool test import /tmp/empty
    rbd ls test | grep -q test7
    rbd --image test/test8 import /tmp/empty
    rbd ls test | grep -q test8
    rbd --dest test/test9 import /tmp/empty
    rbd ls test | grep -q test9
    rbd import --pool test /tmp/empty
    rbd ls test | grep -q empty

    # copy with no explicit pool goes to pool rbd
    rbd copy test/test9 test10
    rbd ls test | grep -qv test10
    rbd ls | grep -q test10
    rbd copy test/test9 test/test10
    rbd ls test | grep -q test10
    rbd copy --pool test test10 --dest-pool test test11
    rbd ls test | grep -q test11
    rbd copy --dest-pool rbd --pool test test11 test12
    rbd ls | grep test12
    rbd ls test | grep -qv test12

    rm -f /tmp/empty /tmp/empty@snap
    ceph osd pool delete test test --yes-i-really-really-mean-it

    for f in foo test1 test10 test12 test2 test3 ; do
	rbd rm $f
    done
}

test_clone() {
    echo "testing clone..."
    remove_images
    rbd create test1 $RBD_CREATE_ARGS -s 1
    rbd snap create test1@s1
    rbd snap protect test1@s1

    ceph osd pool create rbd2 8
    rbd pool init rbd2
    rbd clone test1@s1 rbd2/clone
    rbd -p rbd2 ls | grep clone
    rbd -p rbd2 ls -l | grep clone | grep test1@s1
    rbd ls | grep -v clone
    rbd flatten rbd2/clone
    rbd snap create rbd2/clone@s1
    rbd snap protect rbd2/clone@s1
    rbd clone rbd2/clone@s1 clone2
    rbd ls | grep clone2
    rbd ls -l | grep clone2 | grep rbd2/clone@s1
    rbd -p rbd2 ls | grep -v clone2

    rbd rm clone2
    rbd snap unprotect rbd2/clone@s1
    rbd snap rm rbd2/clone@s1
    rbd rm rbd2/clone
    rbd snap unprotect test1@s1
    rbd snap rm test1@s1
    rbd rm test1
    ceph osd pool rm rbd2 rbd2 --yes-i-really-really-mean-it
}

test_trash() {
    echo "testing trash..."
    remove_images

    rbd create $RBD_CREATE_ARGS -s 1 test1
    rbd create $RBD_CREATE_ARGS -s 1 test2
    rbd ls | grep test1
    rbd ls | grep test2
    rbd ls | wc -l | grep 2
    rbd ls -l | grep 'test1.*2.*'
    rbd ls -l | grep 'test2.*2.*'

    rbd trash mv test1
    rbd ls | grep test2
    rbd ls | wc -l | grep 1
    rbd ls -l | grep 'test2.*2.*'

    rbd trash ls | grep test1
    rbd trash ls | wc -l | grep 1
    rbd trash ls -l | grep 'test1.*USER.*'
    rbd trash ls -l | grep -v 'protected until'

    ID=`rbd trash ls | cut -d ' ' -f 1`
    rbd trash rm $ID

    rbd trash mv test2
    ID=`rbd trash ls | cut -d ' ' -f 1`
    rbd info --image-id $ID | grep "rbd image 'test2'"

    rbd trash restore $ID
    rbd ls | grep test2
    rbd ls | wc -l | grep 1
    rbd ls -l | grep 'test2.*2.*'

    rbd trash mv test2 --expires-at "3600 sec"
    rbd trash ls | grep test2
    rbd trash ls | wc -l | grep 1
    rbd trash ls -l | grep 'test2.*USER.*protected until'

    rbd trash rm $ID 2>&1 | grep 'Deferment time has not expired'
    rbd trash rm --image-id $ID --force

    rbd create $RBD_CREATE_ARGS -s 1 test1
    rbd snap create test1@snap1
    rbd snap protect test1@snap1
    rbd trash mv test1

    rbd trash ls | grep test1
    rbd trash ls | wc -l | grep 1
    rbd trash ls -l | grep 'test1.*USER.*'
    rbd trash ls -l | grep -v 'protected until'

    ID=`rbd trash ls | cut -d ' ' -f 1`
    rbd snap ls --image-id $ID | grep -v 'SNAPID' | wc -l | grep 1
    rbd snap ls --image-id $ID | grep '.*snap1.*'

    rbd snap unprotect --image-id $ID --snap snap1
    rbd snap rm --image-id $ID --snap snap1
    rbd snap ls --image-id $ID | grep -v 'SNAPID' | wc -l | grep 0

    rbd trash restore $ID
    rbd snap create test1@snap1
    rbd snap create test1@snap2
    rbd snap ls --image-id $ID | grep -v 'SNAPID' | wc -l | grep 2
    rbd snap purge --image-id $ID
    rbd snap ls --image-id $ID | grep -v 'SNAPID' | wc -l | grep 0

    rbd rm --rbd_move_to_trash_on_remove=true --rbd_move_to_trash_on_remove_expire_seconds=3600 test1
    rbd trash ls | grep test1
    rbd trash ls | wc -l | grep 1
    rbd trash ls -l | grep 'test1.*USER.*protected until'
    rbd trash rm $ID 2>&1 | grep 'Deferment time has not expired'
    rbd trash rm --image-id $ID --force

    remove_images
}

test_purge() {
    echo "testing trash purge..."
    remove_images

    rbd trash ls | wc -l | grep 0
    rbd trash purge

    rbd create $RBD_CREATE_ARGS --size 256 testimg1
    rbd create $RBD_CREATE_ARGS --size 256 testimg2
    rbd trash mv testimg1
    rbd trash mv testimg2
    rbd trash ls | wc -l | grep 2
    rbd trash purge
    rbd trash ls | wc -l | grep 0

    rbd create $RBD_CREATE_ARGS --size 256 testimg1
    rbd create $RBD_CREATE_ARGS --size 256 testimg2
    rbd trash mv testimg1 --expires-at "1 hour"
    rbd trash mv testimg2 --expires-at "3 hours"
    rbd trash ls | wc -l | grep 2
    rbd trash purge
    rbd trash ls | wc -l | grep 2
    rbd trash purge --expired-before "now + 2 hours"
    rbd trash ls | wc -l | grep 1
    rbd trash ls | grep testimg2
    rbd trash purge --expired-before "now + 4 hours"
    rbd trash ls | wc -l | grep 0

    rbd create $RBD_CREATE_ARGS --size 256 testimg1
    rbd snap create testimg1@snap  # pin testimg1
    rbd create $RBD_CREATE_ARGS --size 256 testimg2
    rbd create $RBD_CREATE_ARGS --size 256 testimg3
    rbd trash mv testimg1
    rbd trash mv testimg2
    rbd trash mv testimg3
    rbd trash ls | wc -l | grep 3
    rbd trash purge 2>&1 | grep 'some expired images could not be removed'
    rbd trash ls | wc -l | grep 1
    rbd trash ls | grep testimg1
    ID=$(rbd trash ls | awk '{ print $1 }')
    rbd snap purge --image-id $ID
    rbd trash purge
    rbd trash ls | wc -l | grep 0

    rbd create $RBD_CREATE_ARGS --size 256 testimg1
    rbd create $RBD_CREATE_ARGS --size 256 testimg2
    rbd snap create testimg2@snap  # pin testimg2
    rbd create $RBD_CREATE_ARGS --size 256 testimg3
    rbd trash mv testimg1
    rbd trash mv testimg2
    rbd trash mv testimg3
    rbd trash ls | wc -l | grep 3
    rbd trash purge 2>&1 | grep 'some expired images could not be removed'
    rbd trash ls | wc -l | grep 1
    rbd trash ls | grep testimg2
    ID=$(rbd trash ls | awk '{ print $1 }')
    rbd snap purge --image-id $ID
    rbd trash purge
    rbd trash ls | wc -l | grep 0

    rbd create $RBD_CREATE_ARGS --size 256 testimg1
    rbd create $RBD_CREATE_ARGS --size 256 testimg2
    rbd create $RBD_CREATE_ARGS --size 256 testimg3
    rbd snap create testimg3@snap  # pin testimg3
    rbd trash mv testimg1
    rbd trash mv testimg2
    rbd trash mv testimg3
    rbd trash ls | wc -l | grep 3
    rbd trash purge 2>&1 | grep 'some expired images could not be removed'
    rbd trash ls | wc -l | grep 1
    rbd trash ls | grep testimg3
    ID=$(rbd trash ls | awk '{ print $1 }')
    rbd snap purge --image-id $ID
    rbd trash purge
    rbd trash ls | wc -l | grep 0

    # test purging a clone with a chain of parents
    rbd create $RBD_CREATE_ARGS --size 256 testimg1
    rbd snap create testimg1@snap
    rbd clone --rbd-default-clone-format=2 testimg1@snap testimg2
    rbd snap rm testimg1@snap
    rbd create $RBD_CREATE_ARGS --size 256 testimg3
    rbd snap create testimg2@snap
    rbd clone --rbd-default-clone-format=2 testimg2@snap testimg4
    rbd clone --rbd-default-clone-format=2 testimg2@snap testimg5
    rbd snap rm testimg2@snap
    rbd snap create testimg4@snap
    rbd clone --rbd-default-clone-format=2 testimg4@snap testimg6
    rbd snap rm testimg4@snap
    rbd trash mv testimg1
    rbd trash mv testimg2
    rbd trash mv testimg3
    rbd trash mv testimg4
    rbd trash ls | wc -l | grep 4
    rbd trash purge 2>&1 | grep 'some expired images could not be removed'
    rbd trash ls | wc -l | grep 3
    rbd trash ls | grep testimg1
    rbd trash ls | grep testimg2
    rbd trash ls | grep testimg4
    rbd trash mv testimg6
    rbd trash ls | wc -l | grep 4
    rbd trash purge 2>&1 | grep 'some expired images could not be removed'
    rbd trash ls | wc -l | grep 2
    rbd trash ls | grep testimg1
    rbd trash ls | grep testimg2
    rbd trash mv testimg5
    rbd trash ls | wc -l | grep 3
    rbd trash purge
    rbd trash ls | wc -l | grep 0

    rbd create $RBD_CREATE_ARGS --size 256 testimg1
    rbd snap create testimg1@snap
    rbd clone --rbd-default-clone-format=2 testimg1@snap testimg2
    rbd snap rm testimg1@snap
    rbd create $RBD_CREATE_ARGS --size 256 testimg3
    rbd snap create testimg3@snap  # pin testimg3
    rbd snap create testimg2@snap
    rbd clone --rbd-default-clone-format=2 testimg2@snap testimg4
    rbd clone --rbd-default-clone-format=2 testimg2@snap testimg5
    rbd snap rm testimg2@snap
    rbd snap create testimg4@snap
    rbd clone --rbd-default-clone-format=2 testimg4@snap testimg6
    rbd snap rm testimg4@snap
    rbd trash mv testimg1
    rbd trash mv testimg2
    rbd trash mv testimg3
    rbd trash mv testimg4
    rbd trash ls | wc -l | grep 4
    rbd trash purge 2>&1 | grep 'some expired images could not be removed'
    rbd trash ls | wc -l | grep 4
    rbd trash mv testimg6
    rbd trash ls | wc -l | grep 5
    rbd trash purge 2>&1 | grep 'some expired images could not be removed'
    rbd trash ls | wc -l | grep 3
    rbd trash ls | grep testimg1
    rbd trash ls | grep testimg2
    rbd trash ls | grep testimg3
    rbd trash mv testimg5
    rbd trash ls | wc -l | grep 4
    rbd trash purge 2>&1 | grep 'some expired images could not be removed'
    rbd trash ls | wc -l | grep 1
    rbd trash ls | grep testimg3
    ID=$(rbd trash ls | awk '{ print $1 }')
    rbd snap purge --image-id $ID
    rbd trash purge
    rbd trash ls | wc -l | grep 0

    # test purging a clone with a chain of auto-delete parents
    rbd create $RBD_CREATE_ARGS --size 256 testimg1
    rbd snap create testimg1@snap
    rbd clone --rbd-default-clone-format=2 testimg1@snap testimg2
    rbd snap rm testimg1@snap
    rbd create $RBD_CREATE_ARGS --size 256 testimg3
    rbd snap create testimg2@snap
    rbd clone --rbd-default-clone-format=2 testimg2@snap testimg4
    rbd clone --rbd-default-clone-format=2 testimg2@snap testimg5
    rbd snap rm testimg2@snap
    rbd snap create testimg4@snap
    rbd clone --rbd-default-clone-format=2 testimg4@snap testimg6
    rbd snap rm testimg4@snap
    rbd rm --rbd_move_parent_to_trash_on_remove=true testimg1
    rbd rm --rbd_move_parent_to_trash_on_remove=true testimg2
    rbd trash mv testimg3
    rbd rm --rbd_move_parent_to_trash_on_remove=true testimg4
    rbd trash ls | wc -l | grep 4
    rbd trash purge 2>&1 | grep 'some expired images could not be removed'
    rbd trash ls | wc -l | grep 3
    rbd trash ls | grep testimg1
    rbd trash ls | grep testimg2
    rbd trash ls | grep testimg4
    rbd trash mv testimg6
    rbd trash ls | wc -l | grep 4
    rbd trash purge 2>&1 | grep 'some expired images could not be removed'
    rbd trash ls | wc -l | grep 2
    rbd trash ls | grep testimg1
    rbd trash ls | grep testimg2
    rbd trash mv testimg5
    rbd trash ls | wc -l | grep 3
    rbd trash purge
    rbd trash ls | wc -l | grep 0

    rbd create $RBD_CREATE_ARGS --size 256 testimg1
    rbd snap create testimg1@snap
    rbd clone --rbd-default-clone-format=2 testimg1@snap testimg2
    rbd snap rm testimg1@snap
    rbd create $RBD_CREATE_ARGS --size 256 testimg3
    rbd snap create testimg3@snap  # pin testimg3
    rbd snap create testimg2@snap
    rbd clone --rbd-default-clone-format=2 testimg2@snap testimg4
    rbd clone --rbd-default-clone-format=2 testimg2@snap testimg5
    rbd snap rm testimg2@snap
    rbd snap create testimg4@snap
    rbd clone --rbd-default-clone-format=2 testimg4@snap testimg6
    rbd snap rm testimg4@snap
    rbd rm --rbd_move_parent_to_trash_on_remove=true testimg1
    rbd rm --rbd_move_parent_to_trash_on_remove=true testimg2
    rbd trash mv testimg3
    rbd rm --rbd_move_parent_to_trash_on_remove=true testimg4
    rbd trash ls | wc -l | grep 4
    rbd trash purge 2>&1 | grep 'some expired images could not be removed'
    rbd trash ls | wc -l | grep 4
    rbd trash mv testimg6
    rbd trash ls | wc -l | grep 5
    rbd trash purge 2>&1 | grep 'some expired images could not be removed'
    rbd trash ls | wc -l | grep 3
    rbd trash ls | grep testimg1
    rbd trash ls | grep testimg2
    rbd trash ls | grep testimg3
    rbd trash mv testimg5
    rbd trash ls | wc -l | grep 4
    rbd trash purge 2>&1 | grep 'some expired images could not be removed'
    rbd trash ls | wc -l | grep 1
    rbd trash ls | grep testimg3
    ID=$(rbd trash ls | awk '{ print $1 }')
    rbd snap purge --image-id $ID
    rbd trash purge
    rbd trash ls | wc -l | grep 0
}

test_deep_copy_clone() {
    echo "testing deep copy clone..."
    remove_images

    rbd create testimg1 $RBD_CREATE_ARGS --size 256
    rbd snap create testimg1 --snap=snap1
    rbd snap protect testimg1@snap1
    rbd clone testimg1@snap1 testimg2
    rbd snap create testimg2@snap2
    rbd deep copy testimg2 testimg3
    rbd info testimg3 | grep 'size 256 MiB'
    rbd info testimg3 | grep 'parent: rbd/testimg1@snap1'
    rbd snap ls testimg3 | grep -v 'SNAPID' | wc -l | grep 1
    rbd snap ls testimg3 | grep '.*snap2.*'
    rbd info testimg2 | grep 'features:.*deep-flatten' || rbd snap rm testimg2@snap2
    rbd info testimg3 | grep 'features:.*deep-flatten' || rbd snap rm testimg3@snap2
    rbd flatten testimg2
    rbd flatten testimg3
    rbd snap unprotect testimg1@snap1
    rbd snap purge testimg2
    rbd snap purge testimg3
    rbd rm testimg2
    rbd rm testimg3

    rbd snap protect testimg1@snap1
    rbd clone testimg1@snap1 testimg2
    rbd snap create testimg2@snap2
    rbd deep copy --flatten testimg2 testimg3
    rbd info testimg3 | grep 'size 256 MiB'
    rbd info testimg3 | grep -v 'parent:'
    rbd snap ls testimg3 | grep -v 'SNAPID' | wc -l | grep 1
    rbd snap ls testimg3 | grep '.*snap2.*'
    rbd info testimg2 | grep 'features:.*deep-flatten' || rbd snap rm testimg2@snap2
    rbd flatten testimg2
    rbd snap unprotect testimg1@snap1

    remove_images
}

test_clone_v2() {
    echo "testing clone v2..."
    remove_images

    rbd create $RBD_CREATE_ARGS -s 1 test1
    rbd snap create test1@1
    rbd clone --rbd-default-clone-format=1 test1@1 test2 && exit 1 || true
    rbd clone --rbd-default-clone-format=2 test1@1 test2
    rbd clone --rbd-default-clone-format=2 test1@1 test3

    rbd snap protect test1@1
    rbd clone --rbd-default-clone-format=1 test1@1 test4

    rbd children test1@1 | sort | tr '\n' ' ' | grep -E "test2.*test3.*test4"
    rbd children --descendants test1 | sort | tr '\n' ' ' | grep -E "test2.*test3.*test4"

    rbd remove test4
    rbd snap unprotect test1@1

    rbd snap remove test1@1
    rbd snap list --all test1 | grep -E "trash \(1\) *$"

    rbd snap create test1@2
    rbd rm test1 2>&1 | grep 'image has snapshots'

    rbd snap rm test1@2
    rbd rm test1 2>&1 | grep 'linked clones'

    rbd rm test3
    rbd rm test1 2>&1 | grep 'linked clones'

    rbd flatten test2
    rbd snap list --all test1 | wc -l | grep '^0$'
    rbd rm test1
    rbd rm test2

    rbd create $RBD_CREATE_ARGS -s 1 test1
    rbd snap create test1@1
    rbd snap create test1@2
    rbd clone test1@1 test2 --rbd-default-clone-format 2
    rbd clone test1@2 test3 --rbd-default-clone-format 2
    rbd snap rm test1@1
    rbd snap rm test1@2
    expect_fail rbd rm test1
    rbd rm test1 --rbd-move-parent-to-trash-on-remove=true
    rbd trash ls -a | grep test1
    rbd rm test2
    rbd trash ls -a | grep test1
    rbd rm test3
    rbd trash ls -a | expect_fail grep test1
}

test_thick_provision() {
    echo "testing thick provision..."
    remove_images

    # Try to create small and large thick-pro image and
    # check actual size. (64M and 4G)

    # Small thick-pro image test
    rbd create $RBD_CREATE_ARGS --thick-provision -s 64M test1
    count=0
    ret=""
    while [ $count -lt 10 ]
    do
        rbd du|grep test1|tr -s " "|cut -d " " -f 4-5|grep '^64 MiB' && ret=$?
        if [ "$ret" = "0" ]
        then
            break;
        fi
        count=`expr $count + 1`
        sleep 2
    done
    rbd du
    if [ "$ret" != "0" ]
    then
        exit 1
    fi
    rbd rm test1
    rbd ls | grep test1 | wc -l | grep '^0$'

    # Large thick-pro image test
    rbd create $RBD_CREATE_ARGS --thick-provision -s 4G test1
    count=0
    ret=""
    while [ $count -lt 10 ]
    do
        rbd du|grep test1|tr -s " "|cut -d " " -f 4-5|grep '^4 GiB' && ret=$?
        if [ "$ret" = "0" ]
        then
            break;
        fi
        count=`expr $count + 1`
        sleep 2
    done
    rbd du
    if [ "$ret" != "0" ]
    then
        exit 1
    fi
    rbd rm test1
    rbd ls | grep test1 | wc -l | grep '^0$'
}

test_namespace() {
    echo "testing namespace..."
    remove_images

    rbd namespace ls | wc -l | grep '^0$'
    rbd namespace create rbd/test1
    rbd namespace create --pool rbd --namespace test2
    rbd namespace create --namespace test3
    expect_fail rbd namespace create rbd/test3

    rbd namespace list | grep 'test' | wc -l | grep '^3$'

    expect_fail rbd namespace remove --pool rbd missing

    rbd create $RBD_CREATE_ARGS --size 1G rbd/test1/image1

    # default test1 ns to test2 ns clone
    rbd bench --io-type write --io-pattern rand --io-total 32M --io-size 4K rbd/test1/image1
    rbd snap create rbd/test1/image1@1
    rbd clone --rbd-default-clone-format 2 rbd/test1/image1@1 rbd/test2/image1
    rbd snap rm rbd/test1/image1@1
    cmp <(rbd export rbd/test1/image1 -) <(rbd export rbd/test2/image1 -)
    rbd rm rbd/test2/image1

    # default ns to test1 ns clone
    rbd create $RBD_CREATE_ARGS --size 1G rbd/image2
    rbd bench --io-type write --io-pattern rand --io-total 32M --io-size 4K rbd/image2
    rbd snap create rbd/image2@1
    rbd clone --rbd-default-clone-format 2 rbd/image2@1 rbd/test2/image2
    rbd snap rm rbd/image2@1
    cmp <(rbd export rbd/image2 -) <(rbd export rbd/test2/image2 -)
    expect_fail rbd rm rbd/image2
    rbd rm rbd/test2/image2
    rbd rm rbd/image2

    # v1 clones are supported within the same namespace
    rbd create $RBD_CREATE_ARGS --size 1G rbd/test1/image3
    rbd snap create rbd/test1/image3@1
    rbd snap protect rbd/test1/image3@1
    rbd clone --rbd-default-clone-format 1 rbd/test1/image3@1 rbd/test1/image4
    rbd rm rbd/test1/image4
    rbd snap unprotect rbd/test1/image3@1
    rbd snap rm rbd/test1/image3@1
    rbd rm rbd/test1/image3

    rbd create $RBD_CREATE_ARGS --size 1G --namespace test1 image2
    expect_fail rbd namespace remove rbd/test1

    rbd group create rbd/test1/group1
    rbd group image add rbd/test1/group1 rbd/test1/image1
    rbd group rm rbd/test1/group1

    rbd trash move rbd/test1/image1
    ID=`rbd trash --namespace test1 ls | cut -d ' ' -f 1`
    rbd trash rm rbd/test1/${ID}

    rbd remove rbd/test1/image2

    rbd namespace remove --pool rbd --namespace test1
    rbd namespace remove --namespace test3

    rbd namespace list | grep 'test' | wc -l | grep '^1$'
    rbd namespace remove rbd/test2
}

get_migration_state() {
    local image=$1

    rbd --format xml status $image |
        $XMLSTARLET sel -t -v '//status/migration/state'
}

test_migration() {
    echo "testing migration..."
    remove_images
    ceph osd pool create rbd2 8
    rbd pool init rbd2

    # Convert to new format
    rbd create --image-format 1 -s 128M test1
    rbd info test1 | grep 'format: 1'
    rbd migration prepare test1 --image-format 2
    test "$(get_migration_state test1)" = prepared
    rbd info test1 | grep 'format: 2'
    rbd rm test1 && exit 1 || true
    rbd migration execute test1
    test "$(get_migration_state test1)" = executed
    rbd migration commit test1
    get_migration_state test1 && exit 1 || true

    # Enable layering (and some other features)
    rbd info test1 | grep 'features: .*layering' && exit 1 || true
    rbd migration prepare test1 --image-feature \
        layering,exclusive-lock,object-map,fast-diff,deep-flatten
    rbd info test1 | grep 'features: .*layering'
    rbd migration execute test1
    rbd migration commit test1

    # Migration to other pool
    rbd migration prepare test1 rbd2/test1
    test "$(get_migration_state rbd2/test1)" = prepared
    rbd ls | wc -l | grep '^0$'
    rbd -p rbd2 ls | grep test1
    rbd migration execute test1
    test "$(get_migration_state rbd2/test1)" = executed
    rbd rm rbd2/test1 && exit 1 || true
    rbd migration commit test1

    # Migration to other namespace
    rbd namespace create rbd2/ns1
    rbd namespace create rbd2/ns2
    rbd migration prepare rbd2/test1 rbd2/ns1/test1
    test "$(get_migration_state rbd2/ns1/test1)" = prepared
    rbd migration execute rbd2/test1
    test "$(get_migration_state rbd2/ns1/test1)" = executed
    rbd migration commit rbd2/test1
    rbd migration prepare rbd2/ns1/test1 rbd2/ns2/test1
    rbd migration execute rbd2/ns2/test1
    rbd migration commit rbd2/ns2/test1

    # Enable data pool
    rbd create -s 128M test1
    rbd migration prepare test1 --data-pool rbd2
    rbd info test1 | grep 'data_pool: rbd2'
    rbd migration execute test1
    rbd migration commit test1

    # testing trash
    rbd migration prepare test1
    expect_fail rbd trash mv test1
    ID=`rbd trash ls -a | cut -d ' ' -f 1`
    expect_fail rbd trash rm $ID
    expect_fail rbd trash restore $ID
    rbd migration abort test1

    # Migrate parent
    rbd remove test1
    dd if=/dev/urandom bs=1M count=1 | rbd --image-format 2 import - test1
    md5sum=$(rbd export test1 - | md5sum)
    rbd snap create test1@snap1
    rbd snap protect test1@snap1
    rbd snap create test1@snap2
    rbd clone test1@snap1 clone_v1 --rbd_default_clone_format=1
    rbd clone test1@snap2 clone_v2 --rbd_default_clone_format=2
    rbd info clone_v1 | fgrep 'parent: rbd/test1@snap1'
    rbd info clone_v2 | fgrep 'parent: rbd/test1@snap2'
    rbd info clone_v2 |grep 'op_features: clone-child'
    test "$(rbd export clone_v1 - | md5sum)" = "${md5sum}"
    test "$(rbd export clone_v2 - | md5sum)" = "${md5sum}"
    test "$(rbd children test1@snap1)" = "rbd/clone_v1"
    test "$(rbd children test1@snap2)" = "rbd/clone_v2"
    rbd migration prepare test1 rbd2/test2
    rbd info clone_v1 | fgrep 'parent: rbd2/test2@snap1'
    rbd info clone_v2 | fgrep 'parent: rbd2/test2@snap2'
    rbd info clone_v2 | fgrep 'op_features: clone-child'
    test "$(rbd children rbd2/test2@snap1)" = "rbd/clone_v1"
    test "$(rbd children rbd2/test2@snap2)" = "rbd/clone_v2"
    rbd migration execute test1
    expect_fail rbd migration commit test1
    rbd migration commit test1 --force
    test "$(rbd export clone_v1 - | md5sum)" = "${md5sum}"
    test "$(rbd export clone_v2 - | md5sum)" = "${md5sum}"
    rbd migration prepare rbd2/test2 test1
    rbd info clone_v1 | fgrep 'parent: rbd/test1@snap1'
    rbd info clone_v2 | fgrep 'parent: rbd/test1@snap2'
    rbd info clone_v2 | fgrep 'op_features: clone-child'
    test "$(rbd children test1@snap1)" = "rbd/clone_v1"
    test "$(rbd children test1@snap2)" = "rbd/clone_v2"
    rbd migration execute test1
    expect_fail rbd migration commit test1
    rbd migration commit test1 --force
    test "$(rbd export clone_v1 - | md5sum)" = "${md5sum}"
    test "$(rbd export clone_v2 - | md5sum)" = "${md5sum}"
    rbd remove clone_v1
    rbd remove clone_v2
    rbd snap unprotect test1@snap1
    rbd snap purge test1
    rbd rm test1

    for format in 1 2; do
        # Abort migration after successful prepare
        rbd create -s 128M --image-format ${format} test2
        rbd migration prepare test2 --data-pool rbd2
        rbd bench --io-type write --io-size 1024 --io-total 1024 test2
        rbd migration abort test2
        rbd bench --io-type write --io-size 1024 --io-total 1024 test2
        rbd rm test2

        # Abort migration after successful execute
        rbd create -s 128M --image-format ${format} test2
        rbd migration prepare test2 --data-pool rbd2
        rbd bench --io-type write --io-size 1024 --io-total 1024 test2
        rbd migration execute test2
        rbd migration abort test2
        rbd bench --io-type write --io-size 1024 --io-total 1024 test2
        rbd rm test2

        # Migration is automatically aborted if prepare failed
        rbd create -s 128M --image-format ${format} test2
        rbd migration prepare test2 --data-pool INVALID_DATA_POOL && exit 1 || true
        rbd bench --io-type write --io-size 1024 --io-total 1024 test2
        rbd rm test2

        # Abort migration to other pool
        rbd create -s 128M --image-format ${format} test2
        rbd migration prepare test2 rbd2/test2
        rbd bench --io-type write --io-size 1024 --io-total 1024 rbd2/test2
        rbd migration abort test2
        rbd bench --io-type write --io-size 1024 --io-total 1024 test2
        rbd rm test2

        # The same but abort using destination image
        rbd create -s 128M --image-format ${format} test2
        rbd migration prepare test2 rbd2/test2
        rbd migration abort rbd2/test2
        rbd bench --io-type write --io-size 1024 --io-total 1024 test2
        rbd rm test2

        test $format = 1 && continue

        # Abort migration to other namespace
        rbd create -s 128M --image-format ${format} test2
        rbd migration prepare test2 rbd2/ns1/test3
        rbd bench --io-type write --io-size 1024 --io-total 1024 rbd2/ns1/test3
        rbd migration abort test2
        rbd bench --io-type write --io-size 1024 --io-total 1024 test2
        rbd rm test2
    done

    remove_images
    ceph osd pool rm rbd2 rbd2 --yes-i-really-really-mean-it
}

test_config() {
    echo "testing config..."
    remove_images

    expect_fail rbd config global set osd rbd_cache true
    expect_fail rbd config global set global debug_ms 10
    expect_fail rbd config global set global rbd_UNKNOWN false
    expect_fail rbd config global set global rbd_cache INVALID
    rbd config global set global rbd_cache false
    rbd config global set client rbd_cache true
    rbd config global set client.123 rbd_cache false
    rbd config global get global rbd_cache | grep '^false$'
    rbd config global get client rbd_cache | grep '^true$'
    rbd config global get client.123 rbd_cache | grep '^false$'
    expect_fail rbd config global get client.UNKNOWN rbd_cache
    rbd config global list global | grep '^rbd_cache * false * global *$'
    rbd config global list client | grep '^rbd_cache * true * client *$'
    rbd config global list client.123 | grep '^rbd_cache * false * client.123 *$'
    rbd config global list client.UNKNOWN | grep '^rbd_cache * true * client *$'
    rbd config global rm client rbd_cache
    expect_fail rbd config global get client rbd_cache
    rbd config global list client | grep '^rbd_cache * false * global *$'
    rbd config global rm client.123 rbd_cache
    rbd config global rm global rbd_cache

    rbd config pool set rbd rbd_cache true
    rbd config pool list rbd | grep '^rbd_cache * true * pool *$'
    rbd config pool get rbd rbd_cache | grep '^true$'

    rbd create $RBD_CREATE_ARGS -s 1 test1

    rbd config image list rbd/test1 | grep '^rbd_cache * true * pool *$'
    rbd config image set rbd/test1 rbd_cache false
    rbd config image list rbd/test1 | grep '^rbd_cache * false * image *$'
    rbd config image get rbd/test1 rbd_cache | grep '^false$'
    rbd config image remove rbd/test1 rbd_cache
    expect_fail rbd config image get rbd/test1 rbd_cache
    rbd config image list rbd/test1 | grep '^rbd_cache * true * pool *$'

    rbd config pool remove rbd rbd_cache
    expect_fail rbd config pool get rbd rbd_cache
    rbd config pool list rbd | grep '^rbd_cache * true * config *$'

    rbd rm test1
}

test_trash_purge_schedule() {
    echo "testing trash purge schedule..."
    remove_images
    ceph osd pool create rbd2 8
    rbd pool init rbd2
    rbd namespace create rbd2/ns1

    test "$(ceph rbd trash purge schedule list)" = "{}"
    ceph rbd trash purge schedule status | fgrep '"scheduled": []'

    expect_fail rbd trash purge schedule ls
    test "$(rbd trash purge schedule ls -R --format json)" = "[]"

    rbd trash purge schedule add -p rbd 1d 01:30

    rbd trash purge schedule ls -p rbd | grep 'every 1d starting at 01:30'
    expect_fail rbd trash purge schedule ls
    rbd trash purge schedule ls -R | grep 'every 1d starting at 01:30'
    rbd trash purge schedule ls -R -p rbd | grep 'every 1d starting at 01:30'
    expect_fail rbd trash purge schedule ls -p rbd2
    test "$(rbd trash purge schedule ls -p rbd2 -R --format json)" = "[]"

    rbd trash purge schedule add -p rbd2/ns1 2d
    test "$(rbd trash purge schedule ls -p rbd2 -R --format json)" != "[]"
    rbd trash purge schedule ls -p rbd2 -R | grep 'rbd2 *ns1 *every 2d'
    rbd trash purge schedule rm -p rbd2/ns1
    test "$(rbd trash purge schedule ls -p rbd2 -R --format json)" = "[]"

    for i in `seq 12`; do
        test "$(rbd trash purge schedule status --format xml |
            $XMLSTARLET sel -t -v '//scheduled/item/pool')" = 'rbd' && break
        sleep 10
    done
    rbd trash purge schedule status
    test "$(rbd trash purge schedule status --format xml |
        $XMLSTARLET sel -t -v '//scheduled/item/pool')" = 'rbd'
    test "$(rbd trash purge schedule status -p rbd --format xml |
        $XMLSTARLET sel -t -v '//scheduled/item/pool')" = 'rbd'

    rbd trash purge schedule add 2d 00:17
    rbd trash purge schedule ls | grep 'every 2d starting at 00:17'
    rbd trash purge schedule ls -R | grep 'every 2d starting at 00:17'
    expect_fail rbd trash purge schedule ls -p rbd2
    rbd trash purge schedule ls -p rbd2 -R | grep 'every 2d starting at 00:17'
    rbd trash purge schedule ls -p rbd2/ns1 -R | grep 'every 2d starting at 00:17'
    test "$(rbd trash purge schedule ls -R -p rbd2/ns1 --format xml |
        $XMLSTARLET sel -t -v '//schedules/schedule/pool')" = "-"
    test "$(rbd trash purge schedule ls -R -p rbd2/ns1 --format xml |
        $XMLSTARLET sel -t -v '//schedules/schedule/namespace')" = "-"
    test "$(rbd trash purge schedule ls -R -p rbd2/ns1 --format xml |
        $XMLSTARLET sel -t -v '//schedules/schedule/items/item/start_time')" = "00:17:00"

    for i in `seq 12`; do
        rbd trash purge schedule status --format xml |
            $XMLSTARLET sel -t -v '//scheduled/item/pool' | grep 'rbd2' && break
        sleep 10
    done
    rbd trash purge schedule status
    rbd trash purge schedule status --format xml |
        $XMLSTARLET sel -t -v '//scheduled/item/pool' | grep 'rbd2'
    echo $(rbd trash purge schedule status --format xml |
        $XMLSTARLET sel -t -v '//scheduled/item/pool') | grep 'rbd rbd2 rbd2'
    test "$(rbd trash purge schedule status -p rbd --format xml |
        $XMLSTARLET sel -t -v '//scheduled/item/pool')" = 'rbd'
    test "$(echo $(rbd trash purge schedule status -p rbd2 --format xml |
        $XMLSTARLET sel -t -v '//scheduled/item/pool'))" = 'rbd2 rbd2'

    test "$(echo $(rbd trash purge schedule ls -R --format xml |
        $XMLSTARLET sel -t -v '//schedules/schedule/items'))" = "2d00:17:00 1d01:30:00"

    rbd trash purge schedule add 1d
    rbd trash purge schedule ls | grep 'every 2d starting at 00:17'
    rbd trash purge schedule ls | grep 'every 1d'

    rbd trash purge schedule ls -R --format xml |
        $XMLSTARLET sel -t -v '//schedules/schedule/items' | grep '2d00:17'

    rbd trash purge schedule rm 1d
    rbd trash purge schedule ls | grep 'every 2d starting at 00:17'
    rbd trash purge schedule rm 2d 00:17
    expect_fail rbd trash purge schedule ls

    for p in rbd2 rbd2/ns1; do
        rbd create $RBD_CREATE_ARGS -s 1 rbd2/ns1/test1
        rbd trash mv rbd2/ns1/test1
        rbd trash ls rbd2/ns1 | wc -l | grep '^1$'

        rbd trash purge schedule add -p $p 1m
        rbd trash purge schedule list -p rbd2 -R | grep 'every 1m'
        rbd trash purge schedule list -p rbd2/ns1 -R | grep 'every 1m'

        for i in `seq 12`; do
            rbd trash ls rbd2/ns1 | wc -l | grep '^1$' || break
            sleep 10
        done
        rbd trash ls rbd2/ns1 | wc -l | grep '^0$'

        # repeat with kicked in schedule, see https://tracker.ceph.com/issues/53915
        rbd trash purge schedule list -p rbd2 -R | grep 'every 1m'
        rbd trash purge schedule list -p rbd2/ns1 -R | grep 'every 1m'

        rbd trash purge schedule status | grep 'rbd2  *ns1'
        rbd trash purge schedule status -p rbd2 | grep 'rbd2  *ns1'
        rbd trash purge schedule status -p rbd2/ns1 | grep 'rbd2  *ns1'

        rbd trash purge schedule rm -p $p 1m
    done

    # Negative tests
    rbd trash purge schedule add 2m
    expect_fail rbd trash purge schedule add -p rbd dummy
    expect_fail rbd trash purge schedule add dummy
    expect_fail rbd trash purge schedule remove -p rbd dummy
    expect_fail rbd trash purge schedule remove dummy
    rbd trash purge schedule ls -p rbd | grep 'every 1d starting at 01:30'
    rbd trash purge schedule ls | grep 'every 2m'
    rbd trash purge schedule remove -p rbd 1d 01:30
    rbd trash purge schedule remove 2m
    test "$(rbd trash purge schedule ls -R --format json)" = "[]"

    remove_images
    ceph osd pool rm rbd2 rbd2 --yes-i-really-really-mean-it
}

test_trash_purge_schedule_recovery() {
    echo "testing recovery of trash_purge_schedule handler after module's RADOS client is blocklisted..."
    remove_images
    ceph osd pool create rbd3 8
    rbd pool init rbd3
    rbd namespace create rbd3/ns1

    rbd trash purge schedule add -p rbd3/ns1 2d
    rbd trash purge schedule ls -p rbd3 -R | grep 'rbd3 *ns1 *every 2d'

    # Fetch and blocklist the rbd_support module's RADOS client
    CLIENT_ADDR=$(ceph mgr dump | jq .active_clients[] |
	jq 'select(.name == "rbd_support")' |
	jq -r '[.addrvec[0].addr, "/", .addrvec[0].nonce|tostring] | add')
    ceph osd blocklist add $CLIENT_ADDR

    # Check that you can add a trash purge schedule after a few retries
    expect_fail rbd trash purge schedule add -p rbd3 10m
    sleep 10
    for i in `seq 24`; do
        rbd trash purge schedule add -p rbd3 10m && break
	sleep 10
    done

    rbd trash purge schedule ls -p rbd3 -R | grep 'every 10m'
    # Verify that the schedule present before client blocklisting is preserved
    rbd trash purge schedule ls -p rbd3 -R | grep 'rbd3 *ns1 *every 2d'

    rbd trash purge schedule remove -p rbd3 10m
    rbd trash purge schedule remove -p rbd3/ns1 2d
    rbd trash purge schedule ls -p rbd3 -R | expect_fail grep 'every 10m'
    rbd trash purge schedule ls -p rbd3 -R | expect_fail grep 'rbd3 *ns1 *every 2d'

    ceph osd pool rm rbd3 rbd3 --yes-i-really-really-mean-it

}

test_mirror_snapshot_schedule() {
    echo "testing mirror snapshot schedule..."
    remove_images
    ceph osd pool create rbd2 8
    rbd pool init rbd2
    rbd namespace create rbd2/ns1

    rbd mirror pool enable rbd2 image
    rbd mirror pool enable rbd2/ns1 image
    rbd mirror pool peer add rbd2 cluster1

    test "$(ceph rbd mirror snapshot schedule list)" = "{}"
    ceph rbd mirror snapshot schedule status | fgrep '"scheduled_images": []'

    expect_fail rbd mirror snapshot schedule ls
    test "$(rbd mirror snapshot schedule ls -R --format json)" = "[]"

    rbd create $RBD_CREATE_ARGS -s 1 rbd2/ns1/test1

    test "$(rbd mirror image status rbd2/ns1/test1 |
        grep -c mirror.primary)" = '0'

    rbd mirror image enable rbd2/ns1/test1 snapshot

    test "$(rbd mirror image status rbd2/ns1/test1 |
        grep -c mirror.primary)" = '1'

    rbd mirror snapshot schedule add -p rbd2/ns1 --image test1 1m
    expect_fail rbd mirror snapshot schedule ls
    rbd mirror snapshot schedule ls -R | grep 'rbd2 *ns1 *test1 *every 1m'
    expect_fail rbd mirror snapshot schedule ls -p rbd2
    rbd mirror snapshot schedule ls -p rbd2 -R | grep 'rbd2 *ns1 *test1 *every 1m'
    expect_fail rbd mirror snapshot schedule ls -p rbd2/ns1
    rbd mirror snapshot schedule ls -p rbd2/ns1 -R | grep 'rbd2 *ns1 *test1 *every 1m'
    test "$(rbd mirror snapshot schedule ls -p rbd2/ns1 --image test1)" = 'every 1m'

    for i in `seq 12`; do
        test "$(rbd mirror image status rbd2/ns1/test1 |
            grep -c mirror.primary)" -gt '1' && break
        sleep 10
    done

    test "$(rbd mirror image status rbd2/ns1/test1 |
        grep -c mirror.primary)" -gt '1'

    # repeat with kicked in schedule, see https://tracker.ceph.com/issues/53915
    expect_fail rbd mirror snapshot schedule ls
    rbd mirror snapshot schedule ls -R | grep 'rbd2 *ns1 *test1 *every 1m'
    expect_fail rbd mirror snapshot schedule ls -p rbd2
    rbd mirror snapshot schedule ls -p rbd2 -R | grep 'rbd2 *ns1 *test1 *every 1m'
    expect_fail rbd mirror snapshot schedule ls -p rbd2/ns1
    rbd mirror snapshot schedule ls -p rbd2/ns1 -R | grep 'rbd2 *ns1 *test1 *every 1m'
    test "$(rbd mirror snapshot schedule ls -p rbd2/ns1 --image test1)" = 'every 1m'

    rbd mirror snapshot schedule status
    test "$(rbd mirror snapshot schedule status --format xml |
        $XMLSTARLET sel -t -v '//scheduled_images/image/image')" = 'rbd2/ns1/test1'
    test "$(rbd mirror snapshot schedule status -p rbd2 --format xml |
        $XMLSTARLET sel -t -v '//scheduled_images/image/image')" = 'rbd2/ns1/test1'
    test "$(rbd mirror snapshot schedule status -p rbd2/ns1 --format xml |
        $XMLSTARLET sel -t -v '//scheduled_images/image/image')" = 'rbd2/ns1/test1'
    test "$(rbd mirror snapshot schedule status -p rbd2/ns1 --image test1 --format xml |
        $XMLSTARLET sel -t -v '//scheduled_images/image/image')" = 'rbd2/ns1/test1'

    rbd mirror image demote rbd2/ns1/test1
    for i in `seq 12`; do
        rbd mirror snapshot schedule status | grep 'rbd2/ns1/test1' || break
        sleep 10
    done
    rbd mirror snapshot schedule status | expect_fail grep 'rbd2/ns1/test1'

    rbd mirror image promote rbd2/ns1/test1
    for i in `seq 12`; do
        rbd mirror snapshot schedule status | grep 'rbd2/ns1/test1' && break
        sleep 10
    done
    rbd mirror snapshot schedule status | grep 'rbd2/ns1/test1'

    rbd mirror snapshot schedule add 1h 00:15
    test "$(rbd mirror snapshot schedule ls)" = 'every 1h starting at 00:15:00'
    rbd mirror snapshot schedule ls -R | grep 'every 1h starting at 00:15:00'
    rbd mirror snapshot schedule ls -R | grep 'rbd2 *ns1 *test1 *every 1m'
    expect_fail rbd mirror snapshot schedule ls -p rbd2
    rbd mirror snapshot schedule ls -p rbd2 -R | grep 'every 1h starting at 00:15:00'
    rbd mirror snapshot schedule ls -p rbd2 -R | grep 'rbd2 *ns1 *test1 *every 1m'
    expect_fail rbd mirror snapshot schedule ls -p rbd2/ns1
    rbd mirror snapshot schedule ls -p rbd2/ns1 -R | grep 'every 1h starting at 00:15:00'
    rbd mirror snapshot schedule ls -p rbd2/ns1 -R | grep 'rbd2 *ns1 *test1 *every 1m'
    test "$(rbd mirror snapshot schedule ls -p rbd2/ns1 --image test1)" = 'every 1m'

    # Negative tests
    expect_fail rbd mirror snapshot schedule add dummy
    expect_fail rbd mirror snapshot schedule add -p rbd2/ns1 --image test1 dummy
    expect_fail rbd mirror snapshot schedule remove dummy
    expect_fail rbd mirror snapshot schedule remove -p rbd2/ns1 --image test1 dummy
    test "$(rbd mirror snapshot schedule ls)" = 'every 1h starting at 00:15:00'
    test "$(rbd mirror snapshot schedule ls -p rbd2/ns1 --image test1)" = 'every 1m'

    rbd rm rbd2/ns1/test1
    for i in `seq 12`; do
        rbd mirror snapshot schedule status | grep 'rbd2/ns1/test1' || break
        sleep 10
    done
    rbd mirror snapshot schedule status | expect_fail grep 'rbd2/ns1/test1'

    rbd mirror snapshot schedule remove
    test "$(rbd mirror snapshot schedule ls -R --format json)" = "[]"

    remove_images
    ceph osd pool rm rbd2 rbd2 --yes-i-really-really-mean-it
}

test_mirror_snapshot_schedule_recovery() {
    echo "testing recovery of mirror snapshot scheduler after module's RADOS client is blocklisted..."
    remove_images
    ceph osd pool create rbd3 8
    rbd pool init rbd3
    rbd namespace create rbd3/ns1

    rbd mirror pool enable rbd3 image
    rbd mirror pool enable rbd3/ns1 image
    rbd mirror pool peer add rbd3 cluster1

    rbd create $RBD_CREATE_ARGS -s 1 rbd3/ns1/test1
    rbd mirror image enable rbd3/ns1/test1 snapshot
    test "$(rbd mirror image status rbd3/ns1/test1 |
        grep -c mirror.primary)" = '1'

    rbd mirror snapshot schedule add -p rbd3/ns1 --image test1 1m
    test "$(rbd mirror snapshot schedule ls -p rbd3/ns1 --image test1)" = 'every 1m'

    # Fetch and blocklist rbd_support module's RADOS client
    CLIENT_ADDR=$(ceph mgr dump | jq .active_clients[] |
	jq 'select(.name == "rbd_support")' |
	jq -r '[.addrvec[0].addr, "/", .addrvec[0].nonce|tostring] | add')
    ceph osd blocklist add $CLIENT_ADDR

    # Check that you can add a mirror snapshot schedule after a few retries
    expect_fail rbd mirror snapshot schedule add -p rbd3/ns1 --image test1 2m
    sleep 10
    for i in `seq 24`; do
        rbd mirror snapshot schedule add -p rbd3/ns1 --image test1 2m && break
	sleep 10
    done

    rbd mirror snapshot schedule ls -p rbd3/ns1 --image test1 | grep 'every 2m'
    # Verify that the schedule present before client blocklisting is preserved
    rbd mirror snapshot schedule ls -p rbd3/ns1 --image test1 | grep 'every 1m'

    rbd mirror snapshot schedule rm -p rbd3/ns1 --image test1 2m
    rbd mirror snapshot schedule rm -p rbd3/ns1 --image test1 1m
    rbd mirror snapshot schedule ls -p rbd3/ns1 --image test1 | expect_fail grep 'every 2m'
    rbd mirror snapshot schedule ls -p rbd3/ns1 --image test1 | expect_fail grep 'every 1m'

    rbd snap purge rbd3/ns1/test1
    rbd rm rbd3/ns1/test1
    ceph osd pool rm rbd3 rbd3 --yes-i-really-really-mean-it
}

test_perf_image_iostat() {
    echo "testing perf image iostat..."
    remove_images

    ceph osd pool create rbd1 8
    rbd pool init rbd1
    rbd namespace create rbd1/ns
    ceph osd pool create rbd2 8
    rbd pool init rbd2
    rbd namespace create rbd2/ns

    IMAGE_SPECS=("test1" "rbd1/test2" "rbd1/ns/test3" "rbd2/test4" "rbd2/ns/test5")
    for spec in "${IMAGE_SPECS[@]}"; do
        # ensure all images are created without a separate data pool
        # as we filter iostat by specific pool specs below
        rbd create $RBD_CREATE_ARGS --size 10G --rbd-default-data-pool '' $spec
    done

    BENCH_PIDS=()
    for spec in "${IMAGE_SPECS[@]}"; do
        rbd bench --io-type write --io-pattern rand --io-total 10G --io-threads 1 \
            --rbd-cache false $spec >/dev/null 2>&1 &
        BENCH_PIDS+=($!)
    done

    # test specifying pool spec via spec syntax
    test "$(rbd perf image iostat --format json rbd1 |
        jq -r 'map(.image) | sort | join(" ")')" = 'test2'
    test "$(rbd perf image iostat --format json rbd1/ns |
        jq -r 'map(.image) | sort | join(" ")')" = 'test3'
    test "$(rbd perf image iostat --format json --rbd-default-pool rbd1 /ns |
        jq -r 'map(.image) | sort | join(" ")')" = 'test3'

    # test specifying pool spec via options
    test "$(rbd perf image iostat --format json --pool rbd2 |
        jq -r 'map(.image) | sort | join(" ")')" = 'test4'
    test "$(rbd perf image iostat --format json --pool rbd2 --namespace ns |
        jq -r 'map(.image) | sort | join(" ")')" = 'test5'
    test "$(rbd perf image iostat --format json --rbd-default-pool rbd2 --namespace ns |
        jq -r 'map(.image) | sort | join(" ")')" = 'test5'

    # test omitting pool spec (-> GLOBAL_POOL_KEY)
    test "$(rbd perf image iostat --format json |
        jq -r 'map(.image) | sort | join(" ")')" = 'test1 test2 test3 test4 test5'

    for pid in "${BENCH_PIDS[@]}"; do
        kill $pid
    done
    wait

    remove_images
    ceph osd pool rm rbd2 rbd2 --yes-i-really-really-mean-it
    ceph osd pool rm rbd1 rbd1 --yes-i-really-really-mean-it
}

test_perf_image_iostat_recovery() {
    echo "testing recovery of perf handler after module's RADOS client is blocklisted..."
    remove_images

    ceph osd pool create rbd3 8
    rbd pool init rbd3
    rbd namespace create rbd3/ns

    IMAGE_SPECS=("rbd3/test1" "rbd3/ns/test2")
    for spec in "${IMAGE_SPECS[@]}"; do
        # ensure all images are created without a separate data pool
        # as we filter iostat by specific pool specs below
        rbd create $RBD_CREATE_ARGS --size 10G --rbd-default-data-pool '' $spec
    done

    BENCH_PIDS=()
    for spec in "${IMAGE_SPECS[@]}"; do
        rbd bench --io-type write --io-pattern rand --io-total 10G --io-threads 1 \
            --rbd-cache false $spec >/dev/null 2>&1 &
        BENCH_PIDS+=($!)
    done

    test "$(rbd perf image iostat --format json rbd3 |
        jq -r 'map(.image) | sort | join(" ")')" = 'test1'

    # Fetch and blocklist the rbd_support module's RADOS client
    CLIENT_ADDR=$(ceph mgr dump | jq .active_clients[] |
	jq 'select(.name == "rbd_support")' |
	jq -r '[.addrvec[0].addr, "/", .addrvec[0].nonce|tostring] | add')
    ceph osd blocklist add $CLIENT_ADDR

    expect_fail rbd perf image iostat --format json rbd3/ns
    sleep 10
    for i in `seq 24`; do
        test "$(rbd perf image iostat --format json rbd3/ns |
            jq -r 'map(.image) | sort | join(" ")')" = 'test2' && break
	sleep 10
    done

    for pid in "${BENCH_PIDS[@]}"; do
        kill $pid
    done
    wait

    remove_images
    ceph osd pool rm rbd3 rbd3 --yes-i-really-really-mean-it
}

test_mirror_pool_peer_bootstrap_create() {
    echo "testing mirror pool peer bootstrap create..."
    remove_images

    ceph osd pool create rbd1 8
    rbd pool init rbd1
    rbd mirror pool enable rbd1 image
    ceph osd pool create rbd2 8
    rbd pool init rbd2
    rbd mirror pool enable rbd2 pool

    readarray -t MON_ADDRS < <(ceph mon dump |
        sed -n 's/^[0-9]: \(.*\) mon\.[a-z]$/\1/p')

    # check that all monitors make it to the token even if only one
    # valid monitor is specified
    BAD_MON_ADDR="1.2.3.4:6789"
    MON_HOST="${MON_ADDRS[0]},$BAD_MON_ADDR"
    TOKEN="$(rbd mirror pool peer bootstrap create \
        --mon-host "$MON_HOST" rbd1 | base64 -d)"
    TOKEN_FSID="$(jq -r '.fsid' <<< "$TOKEN")"
    TOKEN_CLIENT_ID="$(jq -r '.client_id' <<< "$TOKEN")"
    TOKEN_KEY="$(jq -r '.key' <<< "$TOKEN")"
    TOKEN_MON_HOST="$(jq -r '.mon_host' <<< "$TOKEN")"

    test "$TOKEN_FSID" = "$(ceph fsid)"
    test "$TOKEN_KEY" = "$(ceph auth get-key client.$TOKEN_CLIENT_ID)"
    for addr in "${MON_ADDRS[@]}"; do
        fgrep "$addr" <<< "$TOKEN_MON_HOST"
    done
    expect_fail fgrep "$BAD_MON_ADDR" <<< "$TOKEN_MON_HOST"

    # check that the token does not change, including across pools
    test "$(rbd mirror pool peer bootstrap create \
        --mon-host "$MON_HOST" rbd1 | base64 -d)" = "$TOKEN"
    test "$(rbd mirror pool peer bootstrap create \
        rbd1 | base64 -d)" = "$TOKEN"
    test "$(rbd mirror pool peer bootstrap create \
        --mon-host "$MON_HOST" rbd2 | base64 -d)" = "$TOKEN"
    test "$(rbd mirror pool peer bootstrap create \
        rbd2 | base64 -d)" = "$TOKEN"

    ceph osd pool rm rbd2 rbd2 --yes-i-really-really-mean-it
    ceph osd pool rm rbd1 rbd1 --yes-i-really-really-mean-it
}

test_tasks_removed_pool() {
    echo "testing removing pool under running tasks..."
    remove_images

    ceph osd pool create rbd2 8
    rbd pool init rbd2

    rbd create $RBD_CREATE_ARGS --size 1G foo
    rbd snap create foo@snap
    rbd snap protect foo@snap
    rbd clone foo@snap bar

    rbd create $RBD_CREATE_ARGS --size 1G rbd2/dummy
    rbd bench --io-type write --io-pattern seq --io-size 1M --io-total 1G rbd2/dummy
    rbd snap create rbd2/dummy@snap
    rbd snap protect rbd2/dummy@snap
    for i in {1..5}; do
        rbd clone rbd2/dummy@snap rbd2/dummy$i
    done

    # queue flattens on a few dummy images and remove that pool
    test "$(ceph rbd task list)" = "[]"
    for i in {1..5}; do
        ceph rbd task add flatten rbd2/dummy$i
    done
    ceph osd pool delete rbd2 rbd2 --yes-i-really-really-mean-it
    test "$(ceph rbd task list)" != "[]"

    # queue flatten on another image and check that it completes
    rbd info bar | grep 'parent: '
    expect_fail rbd snap unprotect foo@snap
    ceph rbd task add flatten bar
    for i in {1..12}; do
        rbd info bar | grep 'parent: ' || break
        sleep 10
    done
    rbd info bar | expect_fail grep 'parent: '
    rbd snap unprotect foo@snap

    # check that flattens disrupted by pool removal are cleaned up
    for i in {1..12}; do
        test "$(ceph rbd task list)" = "[]" && break
        sleep 10
    done
    test "$(ceph rbd task list)" = "[]"

    remove_images
}

test_tasks_recovery() {
    echo "testing task handler recovery after module's RADOS client is blocklisted..."
    remove_images

    ceph osd pool create rbd2 8
    rbd pool init rbd2

    rbd create $RBD_CREATE_ARGS --size 1G rbd2/img1
    rbd bench --io-type write --io-pattern seq --io-size 1M --io-total 1G rbd2/img1
    rbd snap create rbd2/img1@snap
    rbd snap protect rbd2/img1@snap
    rbd clone rbd2/img1@snap rbd2/clone1

    # Fetch and blocklist rbd_support module's RADOS client
    CLIENT_ADDR=$(ceph mgr dump | jq .active_clients[] |
	jq 'select(.name == "rbd_support")' |
	jq -r '[.addrvec[0].addr, "/", .addrvec[0].nonce|tostring] | add')
    ceph osd blocklist add $CLIENT_ADDR

    expect_fail ceph rbd task add flatten rbd2/clone1
    sleep 10
    for i in `seq 24`; do
       ceph rbd task add flatten rbd2/clone1 && break
       sleep 10
    done
    test "$(ceph rbd task list)" != "[]"

    for i in {1..12}; do
        rbd info rbd2/clone1 | grep 'parent: ' || break
        sleep 10
    done
    rbd info rbd2/clone1 | expect_fail grep 'parent: '
    rbd snap unprotect rbd2/img1@snap

    test "$(ceph rbd task list)" = "[]"
    ceph osd pool rm rbd2 rbd2 --yes-i-really-really-mean-it
}

test_pool_image_args
test_rename
test_ls
test_remove
test_migration
test_config
RBD_CREATE_ARGS=""
test_others
test_locking
test_thick_provision
RBD_CREATE_ARGS="--image-format 2"
test_others
test_locking
test_clone
test_trash
test_purge
test_deep_copy_clone
test_clone_v2
test_thick_provision
test_namespace
test_trash_purge_schedule
test_trash_purge_schedule_recovery
test_mirror_snapshot_schedule
test_mirror_snapshot_schedule_recovery
test_perf_image_iostat
test_perf_image_iostat_recovery
test_mirror_pool_peer_bootstrap_create
test_tasks_removed_pool
test_tasks_recovery

echo OK
