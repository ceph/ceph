#!/bin/bash
set -e

usage () {
	printf '%s: usage: %s -d WORKING_DIR -o OUT_BASENAME -t TEST_DIR\n' "$(basename "$0")" "$(basename "$0")" 1>&2
	echo <<EOF
WORKING_DIR should contain the source, .gcno, and initial lcov files (as created by cov-init.sh)
TEST_DIR should contain the data archived from a teuthology test.

Example:
    mkdir coverage
    ./cov-init.sh ~/teuthology_output/foo coverage
    $0 -t ~/teuthology_output/foo -d coverage -o foo
EOF
	exit 1
}

OUTPUT_BASENAME=
TEST_DIR=
COV_DIR=

while getopts  "d:o:t:" flag
do
	case $flag in
		d) COV_DIR=$OPTARG;;
		o) OUTPUT_BASENAME=$OPTARG;;
		t) TEST_DIR=$OPTARG;;
		*) usage;;
	esac
done

shift $(($OPTIND - 1))

echo $OUTPUT_BASENAME
if [ -z "$OUTPUT_BASENAME" ] || [ -z "$TEST_DIR" ] || [ -z "$COV_DIR" ]; then
	usage
fi

cp $COV_DIR/base.lcov "$COV_DIR/${OUTPUT_BASENAME}.lcov"

for remote in `ls $TEST_DIR/remote`; do
	echo "processing coverage for $remote..."
	cp $TEST_DIR/remote/$remote/coverage/*.gcda $COV_DIR/ceph/src
	cp $TEST_DIR/remote/$remote/coverage/_libs/*.gcda $COV_DIR/ceph/src/.libs
	lcov -d $COV_DIR/ceph/src -c -o "$COV_DIR/${remote}_full.lcov"
	lcov -r "$COV_DIR/${remote}_full.lcov" /usr/include\* -o "$COV_DIR/${remote}.lcov"
	lcov -a "$COV_DIR/${remote}.lcov" -a "$COV_DIR/${OUTPUT_BASENAME}.lcov" -o "$COV_DIR/${OUTPUT_BASENAME}_tmp.lcov"
	mv "$COV_DIR/${OUTPUT_BASENAME}_tmp.lcov" "$COV_DIR/${OUTPUT_BASENAME}.lcov"
	rm "$COV_DIR/${remote}_full.lcov"
	find $COV_DIR/ceph/src -name '*.gcda' -type f -delete
done
