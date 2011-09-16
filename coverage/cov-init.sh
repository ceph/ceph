#!/bin/bash
set -e

usage () {
	printf '%s: usage: %s TEST_DIR OUTPUT_DIR\n' "$(basename "$0")" "$(basename "$0")" 1>&2
	exit 1
}

TEST_DIR=$1
OUTPUT_DIR=$2

if [ -z "$TEST_DIR" ] || [ -z "$OUTPUT_DIR" ]; then
	usage
fi

SHA1=`cat $TEST_DIR/ceph-sha1`

mkdir -p $OUTPUT_DIR/ceph

echo "Retrieving source and .gcno files..."
wget -q -O- "https://github.com/NewDreamNetwork/ceph/tarball/$SHA1" | tar xzf - --strip-components=1 -C $OUTPUT_DIR/ceph
wget "http://gitbuilder-gcov-amd64.ceph.newdream.net/output/sha1/$SHA1/ceph.x86_64.tgz" -P $OUTPUT_DIR
tar zxf $OUTPUT_DIR/ceph.x86_64.tgz -C $OUTPUT_DIR
cp $OUTPUT_DIR/usr/local/lib/ceph/coverage/*.gcno $OUTPUT_DIR/ceph/src
mkdir $OUTPUT_DIR/ceph/src/.libs
cp $OUTPUT_DIR/usr/local/lib/ceph/coverage/.libs/*.gcno $OUTPUT_DIR/ceph/src/.libs
rm -rf $OUTPUT_DIR/usr
rm $OUTPUT_DIR/ceph.x86_64.tgz

echo "Initializing lcov files..."
lcov -d $OUTPUT_DIR/ceph/src -z
lcov -d $OUTPUT_DIR/ceph/src -c -i -o $OUTPUT_DIR/base_full.lcov
lcov -r $OUTPUT_DIR/base_full.lcov /usr/include\* -o $OUTPUT_DIR/base.lcov
rm $OUTPUT_DIR/base_full.lcov
echo "Done."
