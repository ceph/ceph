# rados_write_test.sh
#
# Write a RADOS object and read it back
#
# NOTE: function assumes the pool "write_test" already exists. Pool can be
# created by calling e.g. "create_all_pools_at_once write_test" immediately
# before calling this function.
#
# args: None

set -ex

ceph osd pool application enable write_test deepsea_qa
echo "dummy_content" > verify.txt
rados -p write_test put test_object verify.txt
rados -p write_test get test_object verify_returned.txt
test "x$(cat verify.txt)" = "x$(cat verify_returned.txt)"

echo "OK" >/dev/null
