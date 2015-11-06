#!/bin/bash -ex

. $(dirname $0)/../ceph-helpers.sh

function expect_false()
{
    set -x
    if "$@"; then return 1; else return 0; fi
}

TMPDIR=/tmp/rbd_journal$$
mkdir $TMPDIR
trap "rm -fr $TMPDIR" 0

wait_for_clean

image=testimg$$

rbd create --image-features exclusive-lock,journaling --size 128 ${image}
journal=$(rbd info ${image} --format=xml 2>/dev/null |
		 $XMLSTARLET sel -t -v "//image/journal")
test -n "${journal}"
rbd journal info ${journal}
rbd journal info --journal ${journal}
rbd journal info --image ${image}

rbd feature disable ${image} journaling

rbd info ${image} --format=xml 2>/dev/null |
    expect_false $XMLSTARLET sel -t -v "//image/journal"
expect_false rbd journal info ${journal}
expect_false rbd journal info --image ${image}

rbd feature enable ${image} journaling

journal1=$(rbd info ${image} --format=xml 2>/dev/null |
		  $XMLSTARLET sel -t -v "//image/journal")
test "${journal}" = "${journal1}"

rbd journal info ${journal}

rbd journal status ${journal}

count=10
rbd bench-write ${image} --io-size 4096 --io-threads 1 \
    --io-total $((4096 * count)) --io-pattern seq
count1=$(rbd journal inspect --verbose ${journal} |
		grep -c 'event_type.*AioWrite')
test "${count}" -eq "${count1}"

rbd journal export ${journal} $TMPDIR/journal.export
size=$(stat -c "%s" $TMPDIR/journal.export)
test "${size}" -gt 0

rbd export ${image} $TMPDIR/${image}.export

image1=${image}1
rbd create --image-features exclusive-lock,journaling --size 128 ${image1}
journal1=$(rbd info ${image1} --format=xml 2>/dev/null |
		  $XMLSTARLET sel -t -v "//image/journal")
test -n "${journal1}"

rbd journal import $TMPDIR/journal.export ${journal1}
rbd snap create ${image1}@test
rbd export ${image1}@test $TMPDIR/${image1}.export
cmp $TMPDIR/${image}.export $TMPDIR/${image1}.export

rbd journal inspect ${journal1}

rbd journal reset ${journal}

rbd journal inspect --verbose ${journal} | expect_false grep 'event_type'

rbd remove ${image}
