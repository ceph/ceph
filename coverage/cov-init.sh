#!/bin/bash
set -e

usage () {
	printf '%s: usage: %s TEST_DIR OUTPUT_DIR CEPH_BUILD_TARBALL\n' "$(basename "$0")" "$(basename "$0")" 1>&2
	exit 1
}

TEST_DIR=$1
OUTPUT_DIR=$2
CEPH_TARBALL=$3

if [ -z "$TEST_DIR" ] || [ -z "$OUTPUT_DIR" ] || [ -z "$CEPH_TARBALL" ]; then
	usage
fi

SHA1=`cat $TEST_DIR/ceph-sha1`

mkdir -p $OUTPUT_DIR/ceph

echo "Retrieving source and .gcno files..."
wget -q -O- "https://github.com/ceph/ceph/tarball/$SHA1" | tar xzf - --strip-components=1 -C $OUTPUT_DIR/ceph
tar zxf $CEPH_TARBALL -C $OUTPUT_DIR
cp $OUTPUT_DIR/usr/local/lib/ceph/coverage/*.gcno $OUTPUT_DIR/ceph/src
mkdir $OUTPUT_DIR/ceph/src/.libs
cp $OUTPUT_DIR/usr/local/lib/ceph/coverage/.libs/*.gcno $OUTPUT_DIR/ceph/src/.libs
rm -rf $OUTPUT_DIR/usr
# leave ceph tarball around in case we need to inspect core files

echo "Initializing lcov files..."
lcov -d $OUTPUT_DIR/ceph/src -z
lcov -d $OUTPUT_DIR/ceph/src -c -i -o $OUTPUT_DIR/base_full.lcov
lcov -r $OUTPUT_DIR/base_full.lcov /usr/include\* -o $OUTPUT_DIR/base.lcov
rm $OUTPUT_DIR/base_full.lcov
echo "Done."
