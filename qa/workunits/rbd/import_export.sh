#!/bin/sh -ex

# returns data pool for a given image
get_image_data_pool () {
    image=$1
    data_pool=$(rbd info $image | grep "data_pool: " | awk -F':' '{ print $NF }')
    if [ -z $data_pool ]; then
       data_pool='rbd'
    fi

    echo $data_pool
}

# return list of object numbers populated in image
objects () {
   image=$1
   prefix=$(rbd info $image | grep block_name_prefix | awk '{print $NF;}')

   # strip off prefix and leading zeros from objects; sort, although
   # it doesn't necessarily make sense as they're hex, at least it makes
   # the list repeatable and comparable
   objects=$(rados ls -p $(get_image_data_pool $image) | grep $prefix | \
       sed -e 's/'$prefix'\.//' -e 's/^0*\([0-9a-f]\)/\1/' | sort -u)
   echo $objects
}

# return false if either files don't compare or their ondisk
# sizes don't compare

compare_files_and_ondisk_sizes () {
    cmp -l $1 $2 || return 1
    origsize=$(stat $1 --format %b)
    exportsize=$(stat $2 --format %b)
    difference=$(($exportsize - $origsize))
    difference=${difference#-} # absolute value
    test $difference -ge 0 -a $difference -lt 4096
}

TMPDIR=/tmp/rbd_import_export_$$
rm -rf $TMPDIR
mkdir $TMPDIR
trap "rm -rf $TMPDIR" INT TERM EXIT

# cannot import a dir
mkdir foo.$$
rbd import foo.$$ foo.dir && exit 1 || true   # should fail
rmdir foo.$$

# create a sparse file
dd if=/bin/sh of=${TMPDIR}/img bs=1k count=1 seek=10
dd if=/bin/dd of=${TMPDIR}/img bs=1k count=10 seek=100
dd if=/bin/rm of=${TMPDIR}/img bs=1k count=100 seek=1000
dd if=/bin/ls of=${TMPDIR}/img bs=1k seek=10000
dd if=/bin/ln of=${TMPDIR}/img bs=1k seek=100000
dd if=/bin/grep of=${TMPDIR}/img bs=1k seek=1000000

rbd rm testimg || true

rbd import $RBD_CREATE_ARGS ${TMPDIR}/img testimg
rbd export testimg ${TMPDIR}/img2
rbd export testimg - > ${TMPDIR}/img3
rbd rm testimg
cmp ${TMPDIR}/img ${TMPDIR}/img2
cmp ${TMPDIR}/img ${TMPDIR}/img3
rm ${TMPDIR}/img2 ${TMPDIR}/img3

# try again, importing from stdin
rbd import $RBD_CREATE_ARGS - testimg < ${TMPDIR}/img
rbd export testimg ${TMPDIR}/img2
rbd export testimg - > ${TMPDIR}/img3
rbd rm testimg
cmp ${TMPDIR}/img ${TMPDIR}/img2
cmp ${TMPDIR}/img ${TMPDIR}/img3

rm ${TMPDIR}/img ${TMPDIR}/img2 ${TMPDIR}/img3

if rbd help export | grep -q export-format; then
    # try with --export-format for snapshots
    dd if=/bin/dd of=${TMPDIR}/img bs=1k count=10 seek=100
    rbd import $RBD_CREATE_ARGS ${TMPDIR}/img testimg
    rbd snap create testimg@snap
    rbd export --export-format 2 testimg ${TMPDIR}/img_v2
    rbd import --export-format 2 ${TMPDIR}/img_v2 testimg_import
    rbd info testimg_import
    rbd info testimg_import@snap

    # compare the contents between testimg and testimg_import
    rbd export testimg_import ${TMPDIR}/img_import
    compare_files_and_ondisk_sizes ${TMPDIR}/img ${TMPDIR}/img_import

    rbd export testimg@snap ${TMPDIR}/img_snap
    rbd export testimg_import@snap ${TMPDIR}/img_snap_import
    compare_files_and_ondisk_sizes ${TMPDIR}/img_snap ${TMPDIR}/img_snap_import

    rm ${TMPDIR}/img_v2
    rm ${TMPDIR}/img_import
    rm ${TMPDIR}/img_snap
    rm ${TMPDIR}/img_snap_import

    rbd snap rm testimg_import@snap
    rbd remove testimg_import
    rbd snap rm testimg@snap
    rbd rm testimg

    # order
    rbd import --order 20 ${TMPDIR}/img testimg
    rbd export --export-format 2 testimg ${TMPDIR}/img_v2
    rbd import --export-format 2 ${TMPDIR}/img_v2 testimg_import
    rbd info testimg_import|grep order|awk '{print $2}'|grep 20
    
    rm ${TMPDIR}/img_v2

    rbd remove testimg_import
    rbd remove testimg

    # features
    rbd import --image-feature layering ${TMPDIR}/img testimg
    FEATURES_BEFORE=`rbd info testimg|grep features`
    rbd export --export-format 2 testimg ${TMPDIR}/img_v2
    rbd import --export-format 2 ${TMPDIR}/img_v2 testimg_import
    FEATURES_AFTER=`rbd info testimg_import|grep features`
    if [ "$FEATURES_BEFORE" != "$FEATURES_AFTER" ]; then
        false
    fi

    rm ${TMPDIR}/img_v2

    rbd remove testimg_import
    rbd remove testimg

    # stripe
    rbd import --stripe-count 1000 --stripe-unit 4096 ${TMPDIR}/img testimg
    rbd export --export-format 2 testimg ${TMPDIR}/img_v2
    rbd import --export-format 2 ${TMPDIR}/img_v2 testimg_import
    rbd info testimg_import|grep "stripe unit"|awk '{print $3}'|grep 4096
    rbd info testimg_import|grep "stripe count"|awk '{print $3}'|grep 1000

    rm ${TMPDIR}/img_v2

    rbd remove testimg_import
    rbd remove testimg
fi

tiered=0
if ceph osd dump | grep ^pool | grep "'rbd'" | grep tier; then
    tiered=1
fi

# create specifically sparse files
# 1 1M block of sparse, 1 1M block of random
dd if=/dev/urandom bs=1M seek=1 count=1 of=${TMPDIR}/sparse1

# 1 1M block of random, 1 1M block of sparse
dd if=/dev/urandom bs=1M count=1 of=${TMPDIR}/sparse2; truncate ${TMPDIR}/sparse2 -s 2M

# 1M-block images; validate resulting blocks

# 1M sparse, 1M data
rbd rm sparse1 || true
rbd import $RBD_CREATE_ARGS --order 20 ${TMPDIR}/sparse1
rbd ls -l | grep sparse1 | grep -i '2048k'
[ $tiered -eq 1 -o "$(objects sparse1)" = '1' ]

# export, compare contents and on-disk size
rbd export sparse1 ${TMPDIR}/sparse1.out
compare_files_and_ondisk_sizes ${TMPDIR}/sparse1 ${TMPDIR}/sparse1.out
rm ${TMPDIR}/sparse1.out
rbd rm sparse1

# 1M data, 1M sparse
rbd rm sparse2 || true
rbd import $RBD_CREATE_ARGS --order 20 ${TMPDIR}/sparse2
rbd ls -l | grep sparse2 | grep -i '2048k'
[ $tiered -eq 1 -o "$(objects sparse2)" = '0' ]
rbd export sparse2 ${TMPDIR}/sparse2.out
compare_files_and_ondisk_sizes ${TMPDIR}/sparse2 ${TMPDIR}/sparse2.out
rm ${TMPDIR}/sparse2.out
rbd rm sparse2

# extend sparse1 to 10 1M blocks, sparse at the end
truncate ${TMPDIR}/sparse1 -s 10M
# import from stdin just for fun, verify still sparse
rbd import $RBD_CREATE_ARGS --order 20 - sparse1 < ${TMPDIR}/sparse1
rbd ls -l | grep sparse1 | grep -i '10240k'
[ $tiered -eq 1 -o "$(objects sparse1)" = '1' ]
rbd export sparse1 ${TMPDIR}/sparse1.out
compare_files_and_ondisk_sizes ${TMPDIR}/sparse1 ${TMPDIR}/sparse1.out
rm ${TMPDIR}/sparse1.out
rbd rm sparse1

# extend sparse2 to 4M total with two more nonsparse megs
dd if=/dev/urandom bs=2M count=1 of=${TMPDIR}/sparse2 oflag=append conv=notrunc
# again from stding
rbd import $RBD_CREATE_ARGS --order 20 - sparse2 < ${TMPDIR}/sparse2
rbd ls -l | grep sparse2 | grep -i '4096k'
[ $tiered -eq 1 -o "$(objects sparse2)" = '0 2 3' ]
rbd export sparse2 ${TMPDIR}/sparse2.out
compare_files_and_ondisk_sizes ${TMPDIR}/sparse2 ${TMPDIR}/sparse2.out
rm ${TMPDIR}/sparse2.out
rbd rm sparse2

# zeros import to a sparse image.  Note: all zeros currently
# doesn't work right now due to the way we handle 'empty' fiemaps;
# the image ends up zero-filled.

echo "partially-sparse file imports to partially-sparse image"
rbd import $RBD_CREATE_ARGS --order 20 ${TMPDIR}/sparse1 sparse
[ $tiered -eq 1 -o "$(objects sparse)" = '1' ]
rbd rm sparse

echo "zeros import through stdin to sparse image"
# stdin
dd if=/dev/zero bs=1M count=4 | rbd import $RBD_CREATE_ARGS - sparse
[ $tiered -eq 1 -o "$(objects sparse)" = '' ]
rbd rm sparse

echo "zeros export to sparse file"
#  Must be tricky to make image "by hand" ; import won't create a zero image
rbd create $RBD_CREATE_ARGS sparse --size 4
prefix=$(rbd info sparse | grep block_name_prefix | awk '{print $NF;}')
# drop in 0 object directly
dd if=/dev/zero bs=4M count=1 | rados -p $(get_image_data_pool sparse) \
                                      put ${prefix}.000000000000 -
[ $tiered -eq 1 -o "$(objects sparse)" = '0' ]
# 1 object full of zeros; export should still create 0-disk-usage file
rm ${TMPDIR}/sparse || true
rbd export sparse ${TMPDIR}/sparse
[ $(stat ${TMPDIR}/sparse --format=%b) = '0' ]
rbd rm sparse

rm ${TMPDIR}/sparse ${TMPDIR}/sparse1 ${TMPDIR}/sparse2 ${TMPDIR}/sparse3 || true

echo OK
