#!/bin/sh -ex

# make sure rbd pool is EMPTY.. this is a test script!!
rbd ls | wc -l | grep -v '^0$' && echo "nonempty rbd pool, aborting!  run this script on an empty test cluster only." && exit 1

IMGS="testimg1 testimg2 testimg3 foo foo2 bar bar2 test1 test2 test3 clone2"

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
    TMP_FILES="/tmp/img1 /tmp/img1.new /tmp/img2 /tmp/img2.new /tmp/img3 /tmp/img3.new /tmp/img1.snap1"

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
    rbd info testimg1 | grep 'size 128 MB'
    rbd info --snap=snap1 testimg1 | grep 'size 256 MB'

    # make copies
    rbd copy testimg1 --snap=snap1 testimg2
    rbd copy testimg1 testimg3

    # verify the result
    rbd info testimg2 | grep 'size 256 MB'
    rbd info testimg3 | grep 'size 128 MB'

    rbd export testimg1 /tmp/img1.new
    rbd export testimg2 /tmp/img2.new
    rbd export testimg3 /tmp/img3.new

    cmp /tmp/img2 /tmp/img2.new
    cmp /tmp/img3 /tmp/img3.new

    # rollback
    rbd snap rollback --snap=snap1 testimg1
    rbd info testimg1 | grep 'size 256 MB'
    rbd export testimg1 /tmp/img1.snap1
    cmp /tmp/img2 /tmp/img1.snap1

    # test create, copy of zero-length images
    rbd rm testimg2
    rbd rm testimg3
    rbd create testimg2 -s 0
    rbd cp testimg2 testimg3

    # remove snapshots
    rbd snap rm --snap=snap1 testimg1
    rbd info --snap=snap1 testimg1 2>&1 | grep 'error setting snapshot context: (2) No such file or directory'

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

    rados mkpool rbd2
    rbd create -p rbd2 -s 1 foo
    rbd rename rbd2/foo rbd2/bar
    rbd -p rbd2 ls | grep bar
    rbd rename rbd2/bar foo
    rbd rename --pool rbd2 foo bar
    ! rbd rename rbd2/bar --dest-pool rbd foo
    rbd rename --pool rbd2 bar --dest-pool rbd2 foo
    rbd -p rbd2 ls | grep foo
    rados rmpool rbd2 rbd2 --yes-i-really-really-mean-it

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
    rbd ls -l | grep 'test1.*1024k.*1'
    rbd ls -l | grep 'test2.*1024k.*1'

    rbd rm test1
    rbd rm test2

    rbd create --image-format 2 -s 1 test1
    rbd create --image-format 2 -s 1 test2
    rbd ls | grep test1
    rbd ls | grep test2
    rbd ls | wc -l | grep 2
    rbd ls -l | grep 'test1.*1024k.*2'
    rbd ls -l | grep 'test2.*1024k.*2'

    rbd rm test1
    rbd rm test2

    rbd create --image-format 2 -s 1 test1
    rbd create --image-format 1 -s 1 test2
    rbd ls | grep test1
    rbd ls | grep test2
    rbd ls | wc -l | grep 2
    rbd ls -l | grep 'test1.*1024k.*2'
    rbd ls -l | grep 'test2.*1024k.*1'
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
    rbd clone test2@snap clone

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

    rbd create -s 1 test1
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
    ceph osd pool create test 100
    truncate -s 1 /tmp/empty

    rbd ls | wc -l | grep 0
    rbd create -s 1 test1
    rbd ls | grep -q test1
    rbd import --image test2 /tmp/empty
    rbd ls | grep -q test2
    rbd --dest test3 import /tmp/empty
    rbd ls | grep -q test3
    rbd import /tmp/empty foo
    rbd ls | grep -q foo

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

    rm -f /tmp/empty
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

    rados mkpool rbd2
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
    rados rmpool rbd2 rbd2 --yes-i-really-really-mean-it
}

test_pool_image_args
test_rename
test_ls
test_remove
RBD_CREATE_ARGS=""
test_others
test_locking
RBD_CREATE_ARGS="--image-format 2"
test_others
test_locking
test_clone

echo OK
