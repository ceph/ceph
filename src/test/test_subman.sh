#!/bin/bash -e

source $(dirname $0)/detect-build-env-vars.sh

TMP=$(mktemp --tmpdir -d)
trap "rm -fr $TMP" EXIT

export PATH=$TMP:$PATH

cat > $TMP/ceph-disk <<EOF
echo '[{"partition":[{"type":"data","path":"/dev/foo/bar"}]}]'
EOF
chmod +x $TMP/ceph-disk

cat > $TMP/df <<EOF
echo Used
echo $((2 * 1024 * 1024 * 1024))
EOF
chmod +x $TMP/df

cat > $TMP/expected <<EOF
{
"band.storage.usage": 2
}
EOF
export CEPH_FACTS_FILE=$TMP/facts
$CEPH_ROOT/src/script/subman
diff -u $CEPH_FACTS_FILE $TMP/expected
