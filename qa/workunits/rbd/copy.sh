#!/bin/sh -ex

IMGS="testimg1 testimg2 testimg3 foo foo2 bar bar2 test1 test2 test3"

remove_images() {
    for img in $IMGS
    do
        rbd snap purge $img || true
        rbd rm $img || true
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
    rbd resize testimg1 --size=256
    rbd export testimg1 /tmp/img2
    rbd snap create testimg1 --snap=snap1
    rbd resize testimg1 --size=128
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

    # remove snapshots
    rbd snap rm --snap=snap1 testimg1
    rbd info --snap=snap1 testimg1 2>&1 | grep 'error setting snapshot context: (2) No such file or directory'

    remove_images
    rm -f $TMP_FILES
}

test_rename() {
    echo "testing rename..."
    remove_images

    rbd create -s 1 foo
    rbd create --new-format -s 1 bar
    rbd rename foo foo2
    rbd rename foo2 bar 2>&1 | grep exists
    rbd rename bar bar2
    rbd rename bar2 foo2 2>&1 | grep exists

    remove_images
}

test_ls() {
    echo "testing ls..."
    remove_images

    rbd create -s 1 test1
    rbd create -s 1 test2
    rbd ls | grep test1
    rbd ls | grep test2
    rbd ls | wc -l | grep 2

    rbd rm test1
    rbd rm test2

    rbd create --new-format -s 1 test1
    rbd create --new-format -s 1 test2
    rbd ls | grep test1
    rbd ls | grep test2
    rbd ls | wc -l | grep 2

    rbd rm test1
    rbd rm test2

    rbd create --new-format -s 1 test1
    rbd create -s 1 test2
    rbd ls | grep test1
    rbd ls | grep test2
    rbd ls | wc -l | grep 2

    remove_images
}

test_remove() {
    echo "testing remove..."
    remove_images

    rbd create -s 1 test1
    rbd rm test1
    rbd ls | wc -l | grep "^0$"

    rbd create --new-format -s 1 test2
    rbd rm test2
    rbd ls | wc -l | grep "^0$"

    # check that remove succeeds even if it's
    # interrupted partway through. simulate this
    # by removing some objects manually.

    # remove with header missing (old format)
    rbd create -s 1 test1
    rados rm -p rbd test1.rbd
    rbd rm test1
    rbd ls | wc -l | grep "^0$"

    # remove with header missing
    rbd create --new-format -s 1 test2
    HEADER=$(rados -p rbd ls | grep '^rbd_header')
    rados -p rbd rm $HEADER
    rbd rm test2
    rbd ls | wc -l | grep "^0$"

    # remove with header and id missing
    rbd create --new-format -s 1 test2
    HEADER=$(rados -p rbd ls | grep '^rbd_header')
    rados -p rbd rm $HEADER
    rados -p rbd rm rbd_id.test2
    rbd rm test2
    rbd ls | wc -l | grep "^0$"
}

test_rename
test_ls
test_remove
RBD_CREATE_ARGS=""
test_others
# wait for watch to timeout so we can remove old images
# TODO: remove this once #2476 is fixed
sleep 30
RBD_CREATE_ARGS="--new-format"
test_others

echo OK
