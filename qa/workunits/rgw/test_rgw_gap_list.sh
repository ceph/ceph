#!/usr/bin/env bash
set -e

# Configuration
data_pool=default.rgw.buckets.data
gap_list_out=/tmp/gap_list.out.$$
rgw_host="$(hostname --fqdn)"
rgw_port=80

export RGW_ACCESS_KEY="0555b35654ad1656d804"
export RGW_SECRET_KEY="h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="
export RGW_HOST="${RGW_HOST:-$rgw_host}"

bucket="gap-list-bkt-$((RANDOM % 899999 + 100000))"
object_prefix="gap-test-obj"
object_count=20

# Create the RGW user if it does not exist
if ! radosgw-admin user info --access-key "$RGW_ACCESS_KEY" &>/dev/null; then
    radosgw-admin user create --uid testid \
        --access-key "$RGW_ACCESS_KEY" \
        --secret "$RGW_SECRET_KEY" \
        --display-name "Tester" --email tester@ceph.com
fi


s3cmd --access_key="$RGW_ACCESS_KEY" --secret_key="$RGW_SECRET_KEY" \
      --host="$RGW_HOST" --host-bucket="$RGW_HOST" \
      mb s3://"$bucket"

# Upload test objects (simulate missing objects to test for gaps)
for i in $(seq 1 "$object_count"); do
    if (( i % 5 == 0 )); then
        continue 
    fi
    echo "data-$i" > "/tmp/${object_prefix}-$i"
    s3cmd --access_key="$RGW_ACCESS_KEY" --secret_key="$RGW_SECRET_KEY" \
          --host="$RGW_HOST" --host-bucket="$RGW_HOST" \
          put "/tmp/${object_prefix}-$i" s3://"$bucket"/"${object_prefix}-$i"
done

# Allow time for metadata consistency
sleep 5

# Execute rgw-gap-list to identify missing objects
rgw-gap-list "$data_pool" > "$gap_list_out"

# Validate whether rgw-gap-list successfully identified gaps
if grep -q "$bucket" "$gap_list_out"; then
    echo "Gap(s) detected successfully in bucket: $bucket"
else
    echo "ERROR: No gaps detected. Verification failed."
    exit 1
fi

s3cmd --access_key="$RGW_ACCESS_KEY" --secret_key="$RGW_SECRET_KEY" \
      --host="$RGW_HOST" --host-bucket="$RGW_HOST" \
      rb --recursive s3://"$bucket"

rm -f /tmp/"$object_prefix"-* "$gap_list_out"
echo "Test test_rgw_gap_list.sh completed."
