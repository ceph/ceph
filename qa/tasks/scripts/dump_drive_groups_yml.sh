#!/bin/bash
echo "Dumping drive_groups.yml"
set -ex
DRIVE_GROUPS_DIR="/srv/salt/ceph/configuration/files/"
DRIVE_GROUPS_YML="${DRIVE_GROUPS_DIR}drive_groups.yml"
test -d "$DRIVE_GROUPS_DIR"
test -f "$DRIVE_GROUPS_YML"
cat "$DRIVE_GROUPS_YML"
