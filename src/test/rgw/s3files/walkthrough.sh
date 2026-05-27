#!/usr/bin/env bash
#
# S3 Files API walkthrough — AWS CLI equivalents of the
# conformance test suite at src/test/rgw/s3files/.
#
# Walks the full lifecycle of every Smithy-modeled operation
# against a local vstart cluster, plus a handful of representative
# negative cases. Copy-paste section by section, or run the whole
# script end-to-end. Sections are independent enough that you can
# rerun any one in isolation as long as setup ran first.
#
# Usage (defaults match vstart's testid user):
#
#   ZONE_ID=$(./bin/radosgw-admin zonegroup get \
#     | python3 -c 'import json,sys; print(json.load(sys.stdin)["zones"][0]["id"].replace("-",""))') \
#   ./walkthrough.sh
#
# Override creds/endpoint by exporting AWS_ACCESS_KEY_ID,
# AWS_SECRET_ACCESS_KEY, ENDPOINT before invoking. Requires the
# `testid` user to carry IAM caps:
#
#   ./bin/radosgw-admin caps add --uid=testid \
#     --caps="users=*;roles=*;user-policy=*"

set -euo pipefail

# --- Setup ---------------------------------------------------------------
: "${AWS_ACCESS_KEY_ID:=0555b35654ad1656d804}"
: "${AWS_SECRET_ACCESS_KEY:=h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==}"
: "${AWS_DEFAULT_REGION:=default}"
: "${ENDPOINT:=http://localhost:8000}"
: "${ZONE_ID:?ZONE_ID must be set to the dash-stripped local zone-id}"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION

s3()      { aws --endpoint-url "$ENDPOINT" s3       "$@"; }
s3files() { aws --endpoint-url "$ENDPOINT" s3files  "$@"; }
iam()     { aws --endpoint-url "$ENDPOINT" iam      "$@"; }

# Lead with `\033[0m` so a stray escape code from a preceding
# command (notably the AWS CLI's own colored stderr) can't leave
# the terminal in a state that swallows our color.
banner() { printf "\n\033[0m\033[1;36m== %s ==\033[0m\n" "$1"; }
expect_fail() {
  # Run the rest of the line and expect a non-zero exit — the error
  # envelope IS the point of a negative test, so we print it. Mark
  # the success path explicitly so a reader doesn't mistake the
  # AWS CLI's red `[ERROR]` for a real test failure.
  if "$@" 2>&1; then
    echo "EXPECTED FAILURE BUT SUCCEEDED: $*" >&2
    return 1
  fi
  printf "\033[0m\033[1;32m✓ rejected as expected\033[0m\n"
  return 0
}

# Pretty-print a Smithy field from JSON on stdin.
jget() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)"; }

# --- Section 0: Pre-flight -----------------------------------------------
banner "Pre-flight: list-file-systems must be reachable"
s3files list-file-systems

# --- Section 1: Bucket + IAM role ----------------------------------------
# Both are prerequisites for CreateFileSystem.
banner "Setup: S3 bucket and IAM role"

BUCKET="s3files-walkthrough-$(date +%s)"
s3 mb "s3://$BUCKET"
BUCKET_ARN="arn:aws:s3:::$BUCKET"

ROLE_NAME="s3files-walkthrough-role-$(date +%s)"
ROLE_ARN=$(iam create-role \
  --role-name "$ROLE_NAME" \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"s3files.amazonaws.com"},"Action":"sts:AssumeRole"}]}' \
  | jget '["Role"]["Arn"]')

echo "BUCKET_ARN=$BUCKET_ARN"
echo "ROLE_ARN=$ROLE_ARN"

# --- Section 2: CreateFileSystem (positive) ------------------------------
# Smithy: CreateFileSystem. Required: bucket, roleArn.
# Output round-trips: fileSystemId, fileSystemArn, status, ownerId,
# creationTime.
banner "CreateFileSystem (minimum)"
FS=$(s3files create-file-system --bucket "$BUCKET_ARN" --role-arn "$ROLE_ARN")
echo "$FS" | python3 -m json.tool
FS_ID=$(echo "$FS" | jget '["fileSystemId"]')
echo "FS_ID=$FS_ID"

# --- Section 3: GetFileSystem + ListFileSystems --------------------------
banner "GetFileSystem (by id)"
s3files get-file-system --file-system-id "$FS_ID"

banner "GetFileSystem (by ARN — Smithy ResourceId accepts both forms)"
FS_ARN=$(echo "$FS" | jget '["fileSystemArn"]')
s3files get-file-system --file-system-id "$FS_ARN"

banner "ListFileSystems"
s3files list-file-systems

banner "ListFileSystems (filtered by bucket)"
s3files list-file-systems --bucket "$BUCKET_ARN"

# --- Section 4: CreateFileSystem (validation negatives) ------------------
# These are pulled from test_create_file_system.py. The CLI returns the
# typed exception name + the RGW-defined errorCode.
banner "Negative: invalid bucket ARN → ValidationException / INVALID_BUCKET_ARN"
expect_fail s3files create-file-system \
  --bucket "not-an-arn" --role-arn "$ROLE_ARN"

banner "Negative: invalid role ARN → ValidationException / INVALID_ROLE_ARN"
expect_fail s3files create-file-system \
  --bucket "$BUCKET_ARN" --role-arn "not-an-arn"

banner "Negative: prefix without trailing slash → INVALID_PREFIX"
expect_fail s3files create-file-system \
  --bucket "$BUCKET_ARN" --role-arn "$ROLE_ARN" --prefix "data"

banner "Negative: bucket doesn't exist → BUCKET_NOT_FOUND"
expect_fail s3files create-file-system \
  --bucket "arn:aws:s3:::no-such-bucket-9z9z9z" --role-arn "$ROLE_ARN"

banner "Negative: role doesn't exist → ROLE_NOT_FOUND"
expect_fail s3files create-file-system \
  --bucket "$BUCKET_ARN" \
  --role-arn "arn:aws:iam::000000000000:role/no-such-role"

banner "Negative: bucket already bound → ConflictException / BUCKET_ALREADY_IN_USE"
expect_fail s3files create-file-system \
  --bucket "$BUCKET_ARN" --role-arn "$ROLE_ARN"

# --- Section 5: TagResource + ListTagsForResource + UntagResource --------
banner "TagResource (FS)"
s3files tag-resource --resource-id "$FS_ID" \
  --tags 'key=Name,value=walkthrough-fs' \
         'key=env,value=dev' \
         'key=tier,value=gold'

banner "ListTagsForResource (FS)"
s3files list-tags-for-resource --resource-id "$FS_ID"

banner "UntagResource (remove env, tier)"
s3files untag-resource --resource-id "$FS_ID" \
  --tag-keys env tier

banner "ListTagsForResource (FS) — only Name remains"
s3files list-tags-for-resource --resource-id "$FS_ID"

# --- Section 6: PutFileSystemPolicy + Get + Delete -----------------------
banner "PutFileSystemPolicy"
POLICY='{"Version":"2012-10-17","Statement":[{"Sid":"Demo","Effect":"Allow","Principal":"*","Action":"s3files:GetFileSystem","Resource":"*"}]}'
s3files put-file-system-policy --file-system-id "$FS_ID" --policy "$POLICY"

banner "GetFileSystemPolicy"
s3files get-file-system-policy --file-system-id "$FS_ID"

banner "DeleteFileSystemPolicy"
s3files delete-file-system-policy --file-system-id "$FS_ID"

banner "GetFileSystemPolicy (after delete) → POLICY_NOT_FOUND"
expect_fail s3files get-file-system-policy --file-system-id "$FS_ID"

# --- Section 7: PutSynchronizationConfiguration + Get --------------------
# RGW v1 stores the configuration but does not act on it; the data path
# serves the bucket directly. See test_ceph_divergences.py.
banner "PutSynchronizationConfiguration"
s3files put-synchronization-configuration \
  --file-system-id "$FS_ID" \
  --import-data-rules '[{"prefix":"","trigger":"ON_FILE_ACCESS","sizeLessThan":1048576}]' \
  --expiration-data-rules '[{"daysAfterLastAccess":30}]'

banner "GetSynchronizationConfiguration"
s3files get-synchronization-configuration --file-system-id "$FS_ID"

banner "Negative: invalid trigger value → INVALID_SYNC_RULES"
expect_fail s3files put-synchronization-configuration \
  --file-system-id "$FS_ID" \
  --import-data-rules '[{"prefix":"","trigger":"NOT_AN_ENUM","sizeLessThan":1048576}]' \
  --expiration-data-rules '[{"daysAfterLastAccess":30}]'

banner "Negative: daysAfterLastAccess out of range → INVALID_SYNC_RULES"
expect_fail s3files put-synchronization-configuration \
  --file-system-id "$FS_ID" \
  --import-data-rules '[{"prefix":"","trigger":"ON_FILE_ACCESS","sizeLessThan":1048576}]' \
  --expiration-data-rules '[{"daysAfterLastAccess":1000}]'

# --- Section 8: AccessPoint lifecycle ------------------------------------
# Smithy: CreateAccessPoint. Required: fileSystemId. Optional: posixUser
# (uid+gid required when present), rootDirectory, tags, clientToken.
banner "CreateAccessPoint (minimum)"
AP=$(s3files create-access-point --file-system-id "$FS_ID")
AP_ID=$(echo "$AP" | jget '["accessPointId"]')
echo "AP_ID=$AP_ID"

banner "GetAccessPoint"
s3files get-access-point --access-point-id "$AP_ID"

banner "ListAccessPoints (filtered by FS)"
s3files list-access-points --file-system-id "$FS_ID"

banner "CreateAccessPoint with posixUser + rootDirectory"
AP2=$(s3files create-access-point \
  --file-system-id "$FS_ID" \
  --posix-user 'uid=1000,gid=1000,secondaryGids=1001,1002' \
  --root-directory 'path=/scoped,creationPermissions={ownerUid=1000,ownerGid=1000,permissions=0755}')
AP2_ID=$(echo "$AP2" | jget '["accessPointId"]')

banner "GetAccessPoint — posixUser and rootDirectory round-trip"
s3files get-access-point --access-point-id "$AP2_ID"

banner "Negative: AP on nonexistent FS → FILE_SYSTEM_NOT_FOUND"
expect_fail s3files create-access-point \
  --file-system-id "fs-00000000000000000000000000000000"

banner "DeleteAccessPoint (both)"
s3files delete-access-point --access-point-id "$AP_ID"
s3files delete-access-point --access-point-id "$AP2_ID"

banner "Negative: GetAccessPoint after delete → ACCESS_POINT_NOT_FOUND"
expect_fail s3files get-access-point --access-point-id "$AP_ID"

# --- Section 9: MountTarget lifecycle (Ceph divergence) ------------------
# Per the design doc, the AWS `subnetId` field carries a Ceph zone-id
# encoded as `subnet-{zone_hex}` (hyphens stripped from the UUID to
# match the Smithy `^subnet-[0-9a-f]{8,40}$` pattern). The response's
# `availabilityZoneId` echoes the bare zone-id.
SUBNET_ID="subnet-${ZONE_ID}"

banner "CreateMountTarget"
MT=$(s3files create-mount-target \
  --file-system-id "$FS_ID" \
  --subnet-id "$SUBNET_ID")
MT_ID=$(echo "$MT" | jget '["mountTargetId"]')
echo "MT_ID=$MT_ID"

banner "GetMountTarget"
s3files get-mount-target --mount-target-id "$MT_ID"

banner "ListMountTargets (filtered by FS)"
s3files list-mount-targets --file-system-id "$FS_ID"

banner "UpdateMountTarget (security groups — stored, not enforced in v1)"
s3files update-mount-target --mount-target-id "$MT_ID" \
  --security-groups sg-0000aaa1 sg-0000aaa2

banner "GetMountTarget — securityGroups round-trip"
s3files get-mount-target --mount-target-id "$MT_ID"

banner "Negative: second MT on same FS+zone → MOUNT_TARGET_ALREADY_EXISTS_IN_ZONE"
expect_fail s3files create-mount-target \
  --file-system-id "$FS_ID" --subnet-id "$SUBNET_ID"

banner "Negative: zone not in zonegroup → ZONE_NOT_IN_ZONEGROUP"
expect_fail s3files create-mount-target \
  --file-system-id "$FS_ID" \
  --subnet-id "subnet-ffffffffffffffffffffffffffffffff"

banner "DeleteMountTarget"
s3files delete-mount-target --mount-target-id "$MT_ID"

banner "Negative: GetMountTarget after delete → MOUNT_TARGET_NOT_FOUND"
expect_fail s3files get-mount-target --mount-target-id "$MT_ID"

# --- Section 10: Pagination ----------------------------------------------
# Demonstrate nextToken pagination. We have one FS already; create two
# more so there's something to page.
banner "Pagination demo: create two extra FSes"
B2="s3files-walkthrough-$(date +%s)-2"; s3 mb "s3://$B2"
B3="s3files-walkthrough-$(date +%s)-3"; s3 mb "s3://$B3"
F2=$(s3files create-file-system --bucket "arn:aws:s3:::$B2" --role-arn "$ROLE_ARN" | jget '["fileSystemId"]')
F3=$(s3files create-file-system --bucket "arn:aws:s3:::$B3" --role-arn "$ROLE_ARN" | jget '["fileSystemId"]')

banner "ListFileSystems with maxResults=1 — walk every page"
TOKEN=""
PAGE=0
while :; do
  PAGE=$((PAGE+1))
  if [[ -n "$TOKEN" ]]; then
    RESP=$(s3files list-file-systems --max-results 1 --next-token "$TOKEN")
  else
    RESP=$(s3files list-file-systems --max-results 1)
  fi
  echo "--- page $PAGE ---"
  echo "$RESP"
  TOKEN=$(echo "$RESP" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("nextToken",""))')
  [[ -z "$TOKEN" ]] && break
  (( PAGE > 20 )) && { echo "pagination did not terminate" >&2; break; }
done

# --- Section 11: Cleanup -------------------------------------------------
banner "Cleanup"
s3files delete-file-system --file-system-id "$F2" || true
s3files delete-file-system --file-system-id "$F3" || true
s3files delete-file-system --file-system-id "$FS_ID" || true
iam delete-role --role-name "$ROLE_NAME" || true
s3 rb "s3://$BUCKET" --force || true
s3 rb "s3://$B2" --force || true
s3 rb "s3://$B3" --force || true

banner "Walkthrough complete"
