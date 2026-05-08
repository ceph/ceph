#!/usr/bin/env bash
#
# S3 Files API end-to-end mount walkthrough — AWS CLI to drive the
# control plane, then the Linux NFS client + ls/cat/echo to round-
# trip through Ganesha + FSAL_RGW.
#
# This is a peer to walkthrough.sh (API-only). It adds the mount
# and data-path verification.
#
# What this script demonstrates today:
#
#   1. The full s3files control-plane lifecycle: bucket arn ->
#      file system -> access point -> mount target.
#   2. NFS mount through Ganesha + FSAL_RGW into a Ceph user's
#      bucket namespace.
#   3. Round-trip writes (NFS write visible via S3 GET, S3 PUT
#      visible via NFS read).
#
# What this script does NOT yet demonstrate (intentionally):
#
#   * Bucket-scoped EXPORT blocks. Upstream FSAL_RGW (V9.13 and
#     earlier) doesn't honor a `bucket = "..."` directive in the
#     FSAL block. Until that patch lands, an EXPORT exposes the
#     whole user namespace and the script navigates *into* the
#     bucket dir as a subpath. The s3files API is still modeling
#     "one bucket per file system"; the reconciler renders the
#     intent into the EXPORT block, but FSAL ignores the bucket
#     directive today.
#   * Prefix scoping. Same blocker — FSAL doesn't honor Path =
#     "/<prefix>/" within a bucket. The CreateFileSystem call
#     does accept a `--prefix` and stores it in the model;
#     enforcement at the data plane is a TODO once FSAL lands the
#     prefix-handling patch.
#
# Prerequisites:
#
#   * vstart cluster with rgw_enable_apis containing `s3files`,
#     reconciler enabled (rgw_s3files_reconciler_enabled=true),
#     FSAL creds set to an account-rooted user.
#   * nfs-ganesha (V9.13 from source recommended; distro packaging
#     in Ubuntu 24.04 has buffer-overflow bugs in libfsalrgw's
#     construct_handle for cluster/name args) running locally with
#     the system bus reachable to radosgw for D-Bus AddExport.
#   * The caller is account-rooted in IAM so s3files actions don't
#     trip the legacy-user implicit-deny path.
#
# Usage:
#
#   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
#   ZONE_ID=$(./bin/radosgw-admin zonegroup get \
#     | python3 -c 'import json,sys; print(json.load(sys.stdin)["zones"][0]["id"].replace("-",""))') \
#   FSAL_USER_ID=demoroot \
#   ./walkthrough_mount.sh

set -euo pipefail

: "${AWS_ACCESS_KEY_ID:?must be set (account-rooted user, drives s3files API)}"
: "${AWS_SECRET_ACCESS_KEY:?must be set}"
: "${AWS_DEFAULT_REGION:=default}"
: "${ENDPOINT:=http://localhost:8000}"
: "${ZONE_ID:?must be set to the dash-stripped local zone-id}"
: "${MOUNT_DIR:=/mnt/s3files-demo}"
: "${BUCKET:=s3files-demo-photos}"
: "${ROLE_NAME:=s3files-demo-role}"
: "${EXPORT_DIR:=/var/lib/ceph/rgw/s3files-exports}"

# FSAL credentials are separate from API credentials by design:
#
#   * The s3files control plane (CreateFileSystem etc.) requires
#     an account-rooted IAM user to bypass the policy engine's
#     implicit-deny on legacy users.
#   * FSAL_RGW V9.13's user-namespace mount only surfaces buckets
#     owned by `User_Id` directly. Account-owned buckets aren't
#     enumerated. Until FSAL learns `bucket = ...`, the bucket
#     and the FSAL credentials must point at the same legacy user.
: "${FSAL_USER_ID:?must be set to the FSAL user_id (matches rgw_s3files_fsal_rgw_user_id)}"
: "${FSAL_ACCESS_KEY_ID:?must be set}"
: "${FSAL_SECRET_ACCESS_KEY:?must be set}"

export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION

# API helpers run with the account-rooted creds (control plane).
s3files() { aws --endpoint-url "$ENDPOINT" s3files  "$@"; }
iam()     { aws --endpoint-url "$ENDPOINT" iam      "$@"; }

# Idempotent cleanup of leftover state from prior runs.
banner() { printf "\n\033[0m\033[1;36m== %s ==\033[0m\n" "$1"; }
banner "0. Reset"
sudo umount -lf "$MOUNT_DIR" 2>/dev/null || true
for fs in $(s3files list-file-systems --query "fileSystems[].fileSystemId" --output text 2>/dev/null); do
  for mt in $(s3files list-mount-targets --file-system-id "$fs" --query "mountTargets[].mountTargetId" --output text 2>/dev/null); do
    s3files delete-mount-target --mount-target-id "$mt" 2>/dev/null || true
  done
  for ap in $(s3files list-access-points --file-system-id "$fs" --query "accessPoints[].accessPointId" --output text 2>/dev/null); do
    s3files delete-access-point --access-point-id "$ap" 2>/dev/null || true
  done
  s3files delete-file-system --file-system-id "$fs" 2>/dev/null || true
done
iam delete-role --role-name "$ROLE_NAME" 2>/dev/null || true
sudo find "$EXPORT_DIR" -name '*.conf' -delete 2>/dev/null || true
echo "  state cleared"

# S3 helpers run with the FSAL-side user creds, so buckets and
# objects belong to that user — visible through the FSAL_RGW mount.
s3() {
  AWS_ACCESS_KEY_ID="$FSAL_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$FSAL_SECRET_ACCESS_KEY" \
  aws --endpoint-url "$ENDPOINT" s3 "$@"
}
s3api() {
  AWS_ACCESS_KEY_ID="$FSAL_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$FSAL_SECRET_ACCESS_KEY" \
  aws --endpoint-url "$ENDPOINT" s3api "$@"
}

# --- Section 1: Seed an S3 bucket with multiple prefixes ---------------
#
# Three prefixes, three objects. The mount in this script reaches
# into the whole bucket (FSAL constraint above); the prefixes are
# kept here so future runs against a prefix-aware FSAL can show
# scope filtering.

banner "1. Seed bucket"

s3 mb "s3://$BUCKET" 2>/dev/null || true
echo "host-jan-2026" | s3 cp - "s3://$BUCKET/2026/jan/host.txt"
echo "host-feb-2026" | s3 cp - "s3://$BUCKET/2026/feb/host.txt"
echo "issue-2025"    | s3 cp - "s3://$BUCKET/2025/issue.txt"
s3 ls --recursive "s3://$BUCKET/"

# --- Section 2: IAM role for the file system ---------------------------

banner "2. IAM role"

cat >/tmp/s3files-trust.json <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "s3files.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
JSON

iam create-role \
  --role-name "$ROLE_NAME" \
  --assume-role-policy-document file:///tmp/s3files-trust.json \
  >/dev/null 2>&1 || true

ROLE_ARN=$(iam get-role --role-name "$ROLE_NAME" --query 'Role.Arn' --output text)
echo "role_arn=$ROLE_ARN"

# --- Section 3: CreateFileSystem ---------------------------------------
#
# `--prefix 2026/jan/` is stored in the model; the reconciler
# renders it into Path = "/2026/jan/" in the EXPORT block. FSAL
# doesn't enforce it yet — see banner.

banner "3. CreateFileSystem"
# `--prefix 2026/jan/` is intentionally omitted: FSAL_RGW V9.13
# without the bucket-scoping patch interprets the rendered
# `Path = "/2026/jan/"` against the *user namespace*, not the
# bucket, so a 2026/jan dir doesn't exist. Once FSAL learns
# bucket+prefix, restore the --prefix arg here.

FS_ID=$(s3files create-file-system \
  --bucket "arn:aws:s3:::$BUCKET" \
  --role-arn "$ROLE_ARN" \
  --query fileSystemId --output text)
echo "fs_id=$FS_ID"

# --- Section 4: CreateAccessPoint --------------------------------------

banner "4. CreateAccessPoint"

AP_ID=$(s3files create-access-point \
  --file-system-id "$FS_ID" \
  --query accessPointId --output text)
echo "ap_id=$AP_ID"

# --- Section 5: CreateMountTarget --------------------------------------
#
# subnet-{zone_id} is RGW's reinterpretation of the AWS-mandated
# SubnetId field (see test_ceph_divergences.py).

banner "5. CreateMountTarget"

MT_ID=$(s3files create-mount-target \
  --file-system-id "$FS_ID" \
  --subnet-id "subnet-$ZONE_ID" \
  --query mountTargetId --output text)
echo "mt_id=$MT_ID"

# --- Section 6: Wire up the EXPORT (reconciler or manual) --------------
#
# The colocated reconciler should pick up the change feed and
# AddExport over D-Bus within `safety_net_interval_s`. If it
# doesn't (e.g. running in a partial setup), fall through to a
# manual D-Bus AddExport so the rest of the script remains a
# usable runbook.

banner "6. Wait for reconciler to load the export"

# The colocated reconciler subscribes to the in-process change
# feed, writes one EXPORT config file per (fs, ap, mt) triple
# under $EXPORT_DIR, regenerates index.conf, and triggers
# Ganesha's admin.reread_config DBus method. Wait for the
# export to show up in Ganesha's live export set. ShowExports
# returns export ids (uint16); a non-zero id beyond the root
# pseudo means the reconciler-rendered export is loaded.
ready=0
for _ in $(seq 1 15); do
  ids=$(sudo dbus-send --system --print-reply --type=method_call \
          --dest=org.ganesha.nfsd /org/ganesha/nfsd/ExportMgr \
          org.ganesha.nfsd.exportmgr.ShowExports 2>/dev/null \
        | awk '/^[[:space:]]*uint16 [1-9]/ {print $2}')
  if [ -n "$ids" ]; then
    ready=1
    EXPORT_ID="$ids"
    break
  fi
  sleep 2
done
if [ "$ready" -eq 0 ]; then
  echo "FAIL: reconciler did not register an export with Ganesha" >&2
  exit 1
fi
echo "  reconciler-driven export loaded: id=$EXPORT_ID"

# --- Section 7: NFS mount ----------------------------------------------

banner "7. NFS mount"

sudo mkdir -p "$MOUNT_DIR"
sudo umount "$MOUNT_DIR" 2>/dev/null || true
sudo mount -t nfs4 -o vers=4.1,actimeo=0 \
  "localhost:/$FS_ID/$AP_ID" "$MOUNT_DIR"
mount | grep "$MOUNT_DIR"

# --- Section 8: Linux ops on the mount ---------------------------------
#
# FSAL_RGW exposes the full user namespace as the export root, so
# buckets appear as top-level directories. We navigate *into* the
# seeded bucket; once the bucket+prefix patches land in FSAL_RGW
# the mount itself will be already-scoped.

banner "8. ls / cat through the mount"

ls -la "$MOUNT_DIR/"
ls -la "$MOUNT_DIR/$BUCKET/2026/jan/"
sudo cat "$MOUNT_DIR/$BUCKET/2026/jan/host.txt"

# --- Section 9: NFS write -> S3 read -----------------------------------

banner "9. NFS write -> S3 read"

echo "captured at $(date -Iseconds)" \
  | sudo tee "$MOUNT_DIR/$BUCKET/2026/jan/note-from-nfs.txt" >/dev/null
sudo cat "$MOUNT_DIR/$BUCKET/2026/jan/note-from-nfs.txt"

s3 ls "s3://$BUCKET/2026/jan/note-from-nfs.txt"
s3 cp "s3://$BUCKET/2026/jan/note-from-nfs.txt" -

# --- Section 10: S3 write -> NFS read ----------------------------------

banner "10. S3 write -> NFS read"

echo "captured via S3 PUT" | s3 cp - "s3://$BUCKET/2026/jan/note-from-s3.txt"
sudo cat "$MOUNT_DIR/$BUCKET/2026/jan/note-from-s3.txt"

# --- Section 11: Append + delete ---------------------------------------

banner "11. Append + delete"

echo "second line" \
  | sudo tee -a "$MOUNT_DIR/$BUCKET/2026/jan/note-from-nfs.txt" >/dev/null
s3 ls "s3://$BUCKET/2026/jan/note-from-nfs.txt"

sudo rm "$MOUNT_DIR/$BUCKET/2026/jan/note-from-nfs.txt"
if s3api head-object --bucket "$BUCKET" \
     --key "2026/jan/note-from-nfs.txt" 2>/dev/null; then
  echo "FAIL: object survived NFS rm"; exit 1
fi
echo "  rm propagated to S3"

# --- Section 12: Cleanup ----------------------------------------------

banner "12. Cleanup"

sudo umount "$MOUNT_DIR"
s3files delete-mount-target --mount-target-id "$MT_ID"
s3files delete-access-point --access-point-id "$AP_ID"
s3files delete-file-system --file-system-id "$FS_ID"
iam delete-role --role-name "$ROLE_NAME"

printf "\n\033[1;32mDone.\033[0m  Bucket %s remains; remove with 'aws s3 rb --force s3://%s'.\n" \
  "$BUCKET" "$BUCKET"
