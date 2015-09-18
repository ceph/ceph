#!/bin/bash -e

. $(dirname $0)/../ceph-helpers.sh

function list_tests()
{
  echo "AVAILABLE TESTS"
  for i in $TESTS; do
    echo "  $i"
  done
}

function usage()
{
  echo "usage: $0 [-h|-l|-t <testname> [-t <testname>...] [--no-sanity-check] [--no-cleanup]]"
}

function expect_false()
{
    set -x
    if "$@"; then return 1; else return 0; fi
}

test_rbd_journal()
{
    local image=testrbdjournal$$

    rbd create --image-feature exclusive-lock --image-feature journaling \
	--size 128 ${image}
    local journal=$(rbd info ${image} --format=xml 2>/dev/null |
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

    local journal1=$(rbd info ${image} --format=xml 2>/dev/null |
			    $XMLSTARLET sel -t -v "//image/journal")
    test "${journal}" = "${journal1}"

    rbd journal info ${journal}

    rbd journal status ${journal}

    local count=10
    rbd bench-write ${image} --io-size 4096 --io-threads 1 \
	--io-total $((4096 * count)) --io-pattern seq
    local count1=$(rbd journal inspect --verbose ${journal} |
			  grep -c 'event_type.*AioWrite')
    test "${count}" -eq "${count1}"

    rbd journal export ${journal} $TMPDIR/journal.export
    local size=$(stat -c "%s" $TMPDIR/journal.export)
    test "${size}" -gt 0

    rbd export ${image} $TMPDIR/${image}.export

    local image1=${image}1
    rbd create --image-feature exclusive-lock --image-feature journaling \
	--size 128 ${image1}

    rbd journal import --dest ${image1} $TMPDIR/journal.export
    rbd snap create ${image1}@test
    # check that commit position is properly updated: the journal should contain
    # 12 entries (10 AioWrite + 1 SnapCreate + 1 OpFinish) and commit
    # position set to tid=11
    rbd journal inspect --image ${image1} --verbose | awk '
      /AioWrite/          {w++}         # match: "event_type": "AioWrite",
      /SnapCreate/        {s++}         # match: "event_type": "SnapCreate",
      /OpFinish/          {f++}         # match: "event_type": "OpFinish",
      /entries inspected/ {t=$1; e=$4}  # match: 12 entries inspected, 0 errors
                          {print}       # for diagnostic
      END                 {
        if (w != 10 || s != 1 || f != 1 || t != 12 || e != 0) exit(1)
      }
    '
    rbd journal status --image ${image1} | grep 'tid=11'

    rbd export ${image1}@test $TMPDIR/${image1}.export
    cmp $TMPDIR/${image}.export $TMPDIR/${image1}.export

    rbd journal inspect ${journal1}

    rbd journal reset ${journal}

    rbd journal inspect --verbose ${journal} | expect_false grep 'event_type'

    rbd snap purge ${image1}
    rbd remove ${image1}
    rbd remove ${image}
}

TESTS+=" rbd_journal"

#
# "main" follows
#

tests_to_run=()

sanity_check=true
cleanup=true

while [[ $# -gt 0 ]]; do
    opt=$1

    case "$opt" in
	"-l" )
	    do_list=1
	    ;;
	"--no-sanity-check" )
	    sanity_check=false
	    ;;
	"--no-cleanup" )
	    cleanup=false
	    ;;
	"-t" )
	    shift
	    if [[ -z "$1" ]]; then
		echo "missing argument to '-t'"
		usage ;
		exit 1
	    fi
	    tests_to_run+=" $1"
	    ;;
	"-h" )
	    usage ;
	    exit 0
	    ;;
    esac
    shift
done

if [[ $do_list -eq 1 ]]; then
    list_tests ;
    exit 0
fi

TMPDIR=/tmp/rbd_journal$$
mkdir $TMPDIR
if $cleanup; then
    trap "rm -fr $TMPDIR" 0
fi

if test -z "$tests_to_run" ; then
    tests_to_run="$TESTS"
fi

for i in $tests_to_run; do
    if $sanity_check ; then
	wait_for_clean
    fi
    set -x
    test_${i}
    set +x
done
if $sanity_check ; then
    wait_for_clean
fi

echo OK
