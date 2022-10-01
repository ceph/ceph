#!/bin/bash -ex
# Create some file-backed iSCSI targets and attach them locally.

# Exit if it's not CentOS
if ! grep -q rhel /etc/*-release; then
    echo "The script only supports CentOS."
    exit 1
fi

[ -z "$SUDO" ] && SUDO=sudo

# 15 GB
DISK_FILE_SIZE="16106127360"

$SUDO yum install -y targetcli iscsi-initiator-utils

TARGET_NAME="iqn.2003-01.org.linux-iscsi.$(hostname).x8664:sn.foobar"
$SUDO targetcli /iscsi create ${TARGET_NAME}
$SUDO targetcli /iscsi/${TARGET_NAME}/tpg1/portals delete 0.0.0.0 3260
$SUDO targetcli /iscsi/${TARGET_NAME}/tpg1/portals create 127.0.0.1 3260
$SUDO targetcli /iscsi/${TARGET_NAME}/tpg1 set attribute generate_node_acls=1
$SUDO targetcli /iscsi/${TARGET_NAME}/tpg1 set attribute demo_mode_write_protect=0

for i in $(seq 3); do
    # Create truncated files, and add them as luns
    DISK_FILE="/tmp/disk${i}"
    $SUDO truncate --size ${DISK_FILE_SIZE} ${DISK_FILE}

    $SUDO targetcli /backstores/fileio create "lun${i}" ${DISK_FILE}
    # Workaround for https://tracker.ceph.com/issues/47758
    $SUDO targetcli "/backstores/fileio/lun${i}" set attribute optimal_sectors=0
    $SUDO targetcli /iscsi/${TARGET_NAME}/tpg1/luns create "/backstores/fileio/lun${i}"
done

$SUDO iscsiadm -m discovery -t sendtargets -p 127.0.0.1
$SUDO iscsiadm -m node -p 127.0.0.1 -T ${TARGET_NAME} -l
