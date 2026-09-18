#!/bin/bash
# Regression test: EC2Engine must invalidate and retry the cached Keystone
# admin token when Keystone returns 401 (e.g. after a Fernet key rotation).
#
# Scenario:
#   1. Start a Keystone fake server + a vstart RGW cluster configured with
#      Keystone S3 auth (rgw keystone token cache ttl = 5 to keep the test fast).
#   2. Make an S3 request: RGW fetches admin-token-1, s3tokens succeeds.
#   3. Simulate a Fernet key rotation: the fake server invalidates
#      admin-token-1 and starts issuing admin-token-2.
#   4. Wait for the secret cache to expire, then make another S3 request.
#      RGW still has admin-token-1 cached and sends it to s3tokens.
#
# Expected:
#   - WITHOUT the fix: s3tokens returns 401, EC2Engine misreports it as
#     ERR_SIGNATURE_NO_MATCH and the request fails with SignatureDoesNotMatch.
#     The stale admin token is never refreshed (auth_post stays at 1).
#   - WITH the fix: EC2Engine detects the 401, invalidates the admin token
#     cache, fetches admin-token-2 (auth_post becomes 2) and retries;
#     the request succeeds.
#
# Usage (inside the ceph-build container, see ContainerBuild.md):
#   python3 src/script/build-with-container.py -d rocky10 --no-prereqs -e custom -- \
#       bash /ceph/qa/workunits/rgw/test-ec2-admin-token-retry.sh

set -x

# The build container lacks iproute, required by vstart.sh
dnf install -y iproute 2>/dev/null || true

CEPH_DIR=/ceph
BUILD_DIR=$CEPH_DIR/build
FAKE_SERVER=$CEPH_DIR/qa/workunits/rgw/keystone-fake-server-s3test.py
RGW_PORT=8000
KEYSTONE_PORT=5000

# EC2 credentials served by the fake Keystone server
ACCESS_KEY=04daaeb0960f46b9a2c9abbb25f1ad4a
SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

cleanup() {
    $CEPH_DIR/src/stop.sh 2>/dev/null || true
    kill $KEYSTONE_PID 2>/dev/null || true
}
trap cleanup EXIT

pip3 install boto3 2>/dev/null || true

s3_list_buckets() {
    python3 -c "
import boto3
s3 = boto3.client('s3',
    endpoint_url='http://localhost:$RGW_PORT',
    aws_access_key_id='$ACCESS_KEY',
    aws_secret_access_key='$SECRET_KEY',
    region_name='us-east-1')
try:
    resp = s3.list_buckets()
    print('SUCCESS: list_buckets returned', len(resp.get('Buckets', [])), 'buckets')
except Exception as e:
    print('FAILED:', e)
    exit(1)
"
}

# Start fake Keystone server
python3 $FAKE_SERVER > /tmp/keystone.log 2>&1 &
KEYSTONE_PID=$!
sleep 2
curl -s http://localhost:$KEYSTONE_PORT/stats && echo " <- Keystone fake server up"

# Start vstart cluster. Do NOT use CEPH_ARGS: it is injected into every
# Ceph binary, including ceph-authtool, which chokes on rgw_* options.
# Use vstart's -o option to inject the Keystone configuration instead.
cd $BUILD_DIR
rm -f keyring ceph.conf
rm -rf dev out

echo "Starting vstart cluster..."
RGW=1 OSD=1 MON=1 MGR=0 MDS=0 $CEPH_DIR/src/vstart.sh -n -l --bluestore --without-dashboard \
    -o "rgw s3 auth use keystone = true" \
    -o "rgw keystone url = http://localhost:$KEYSTONE_PORT" \
    -o "rgw keystone admin user = admin" \
    -o "rgw keystone admin password = ADMIN" \
    -o "rgw keystone admin project = admin" \
    -o "rgw keystone admin domain = default" \
    -o "rgw keystone api version = 3" \
    -o "rgw keystone accepted roles = admin, Member" \
    -o "rgw keystone token cache ttl = 5" \
    -o "rgw keystone verify ssl = false" \
    -o "debug rgw = 20" \
    2>&1 || true

echo "Waiting for RGW on port $RGW_PORT..."
for i in $(seq 1 30); do
    if curl -s http://localhost:$RGW_PORT/ >/dev/null 2>&1; then
        echo "RGW is listening"
        break
    fi
    sleep 2
done

echo "=== Keystone stats before any request ==="
curl -s http://localhost:$KEYSTONE_PORT/stats | python3 -m json.tool

# Request 1: admin token cache is cold, RGW fetches admin-token-1
echo "=== Request 1 (admin token valid, must succeed) ==="
s3_list_buckets
R1=$?

echo "=== Keystone stats after request 1 ==="
curl -s http://localhost:$KEYSTONE_PORT/stats | python3 -m json.tool

# Simulate Fernet key rotation: admin-token-1 is no longer accepted
echo "=== Simulating Fernet key rotation ==="
curl -s http://localhost:$KEYSTONE_PORT/rotate | python3 -m json.tool

# Wait for the secret cache (rgw_keystone_token_cache_ttl = 5s) to expire so
# that request 2 goes back to Keystone with the stale admin token
echo "Waiting 6s for the secret cache to expire..."
sleep 6

# Request 2: RGW sends the stale admin-token-1 to s3tokens and gets a 401.
# With the fix, EC2Engine invalidates the cache, fetches admin-token-2
# and retries; without the fix the request fails with SignatureDoesNotMatch.
echo "=== Request 2 (after rotation, must succeed with the fix) ==="
s3_list_buckets
R2=$?

echo "=== Keystone stats after request 2 ==="
curl -s http://localhost:$KEYSTONE_PORT/stats | python3 -m json.tool

echo "=== RGW log: admin token retry messages ==="
grep -i 'invalidating admin_token\|retrying with uncached' \
    $BUILD_DIR/out/radosgw.*.log 2>/dev/null | tail -20 || echo "(no retry messages found)"

echo ""
if [ $R1 -eq 0 ] && [ $R2 -eq 0 ]; then
    echo "PASS: request 1 and request 2 both succeeded"
    exit 0
else
    echo "FAIL: request 1 (rc=$R1), request 2 (rc=$R2)"
    echo "Without the fix, request 2 is expected to fail with SignatureDoesNotMatch."
    exit 1
fi
