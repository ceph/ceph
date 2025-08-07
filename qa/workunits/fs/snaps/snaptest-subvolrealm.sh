# The test verifies subvolume snapshots when there are snaps between root and subvolume snapshot path.
# The snapshot data should be correct irrespective enabling/disabling mds_use_global_snaprealm_seq_for_subvol

#!/bin/sh -x

set -e

MOUNTPOINT="."
VOLUMES="$MOUNTPOINT/volumes"
GROUP="$VOLUMES/group"
SUBVOL1="$GROUP/subvol1"
SUBVOL2="$GROUP/subvol2"
SUBVOL1_DATA="$SUBVOL1/user_dir"
SUBVOL2_DATA="$SUBVOL2/user_dir"

# Create directory tree
mkdir -p "$SUBVOL1_DATA"
mkdir -p "$SUBVOL2_DATA"

# Mark subvolume
setfattr -n ceph.dir.subvolume -v 1 "$SUBVOL1"
setfattr -n ceph.dir.subvolume -v 1 "$SUBVOL2"

# Fill in some data
echo "file at root" > "$MOUNTPOINT/root_file.txt"
echo "volumes file" > "$VOLUMES/volumes_file.txt"
echo "group file" > "$GROUP/group_file.txt"
echo "subvol1 config file" > "$SUBVOL1/meta.txt"
echo "subvol2 config file" > "$SUBVOL2/meta.txt"
echo "subvol1 data file" > "$SUBVOL1_DATA/data1.txt"
echo "subvol1 data file" > "$SUBVOL2_DATA/data2.txt"

# Create CephFS snapshots
mkdir "$MOUNTPOINT/.snap/root_snap"
mkdir "$VOLUMES/.snap/volumes_snap"
mkdir "$GROUP/.snap/group_snap"
mkdir "$SUBVOL1/.snap/subvol1_snap"
mkdir "$SUBVOL2/.snap/subvol2_snap"

# Function to compute checksum of all files under a directory
checksum_dir() {
    dir=$1
    find "$dir" -type f -exec md5sum {} \; | awk '{print $1}' | sort | md5sum
}

# Compute checksums
csum_root=$(checksum_dir "$MOUNTPOINT")
csum_snap_root=$(checksum_dir "$MOUNTPOINT/.snap/root_snap")
csum_volumes=$(checksum_dir "$VOLUMES")
csum_snap_volumes=$(checksum_dir "$VOLUMES/.snap/volumes_snap")
csum_group=$(checksum_dir "$GROUP")
csum_snap_group=$(checksum_dir "$GROUP/.snap/group_snap")
csum_subvol1=$(checksum_dir "$SUBVOL1")
csum_snap_subvol1=$(checksum_dir "$SUBVOL1/.snap/subvol1_snap")
csum_subvol2=$(checksum_dir "$SUBVOL2")
csum_snap_subvol2=$(checksum_dir "$SUBVOL2/.snap/subvol2_snap")

# Verify checksums
verify_checksum() {
    original=$1
    snap=$2
    snap_path=$3
    label=$4
    label1=$5

    if [ "$original" = "$snap" ]; then
        echo "[OK] $label snapshot checksum matches original $label1"
        echo "-------------------------------------------------------------------"
        echo "$(find $snap_path -type f -print -exec cat {} \;)"
        echo "-------------------------------------------------------------------"
        echo ""
        true
    else
        echo "[FAIL] $label snapshot checksum does not match original $label1"
        echo "-------------------------------------------------------------------"
        echo "$(find $snap_path -type f -print -exec cat {} \;)"
        echo "-------------------------------------------------------------------"
        echo ""
        false
    fi
}

# Verify snapshot checksums before write
verify_checksum "$csum_root" "$csum_snap_root" "$MOUNTPOINT/.snap/root_snap" "root_snap" "before write"
verify_checksum "$csum_volumes" "$csum_snap_volumes" "$VOLUMES/.snap/volumes_snap" "volumes_snap" "before write"
verify_checksum "$csum_group" "$csum_snap_group" "$GROUP/.snap/group_snap" "group_snap" "before write"
verify_checksum "$csum_subvol1" "$csum_snap_subvol1" "$SUBVOL1/.snap/subvol1_snap" "subvol1_snap" "before write"
verify_checksum "$csum_subvol2" "$csum_snap_subvol2" "$SUBVOL2/.snap/subvol2_snap" "subvol2_snap" "before write"

# Write data
echo "append" >> "$MOUNTPOINT/root_file.txt"
echo "append" >> "$VOLUMES/volumes_file.txt"
ln "$VOLUMES/volumes_file.txt" "$MOUNTPOINT/ln_volumes_file.txt"
mv "$GROUP/group_file.txt" "$VOLUMES/mv_group_file.txt"
truncate -s 0 "$SUBVOL1/meta.txt"
echo "append" >> "$SUBVOL2/meta.txt"
echo "append" >> "$SUBVOL1_DATA/data1.txt"
echo "append" >> "$SUBVOL2_DATA/data2.txt"
mkdir "$SUBVOL1_DATA/dir1"
mkdir "$SUBVOL1_DATA/dir2"
mkdir "$SUBVOL2_DATA/dir1"
mkdir "$SUBVOL2_DATA/dir2"
ln "$SUBVOL1_DATA/data1.txt" "$SUBVOL1_DATA/dir2/hl_data1.txt"
mv "$SUBVOL1_DATA/data1.txt" "$SUBVOL1_DATA/dir1/renamed_data1.txt"
ln "$SUBVOL2_DATA/data2.txt" "$SUBVOL2_DATA/dir2/hl_data2.txt"
mv "$SUBVOL2_DATA/data2.txt" "$SUBVOL2_DATA/dir1/renamed_data2.txt"

# Verify snapshot checksums after write
verify_checksum "$csum_root" "$csum_snap_root" "$MOUNTPOINT/.snap/root_snap" "root_snap" "after write"
verify_checksum "$csum_volumes" "$csum_snap_volumes" "$VOLUMES/.snap/volumes_snap" "volumes_snap" "after write"
verify_checksum "$csum_group" "$csum_snap_group" "$GROUP/.snap/group_snap" "group_snap" "after write"
verify_checksum "$csum_subvol1" "$csum_snap_subvol1" "$SUBVOL1/.snap/subvol1_snap" "subvol1_snap" "after write"
verify_checksum "$csum_subvol2" "$csum_snap_subvol2" "$SUBVOL2/.snap/subvol2_snap" "subvol2_snap" "after write"

# Snap again, now it has hardlinks/renames
mkdir "$MOUNTPOINT/.snap/root_snap1"
mkdir "$VOLUMES/.snap/volumes_snap1"
mkdir "$GROUP/.snap/group_snap1"
mkdir "$SUBVOL1/.snap/subvol1_snap1"
mkdir "$SUBVOL2/.snap/subvol2_snap1"

# Compute checksums
csum_root1=$(checksum_dir "$MOUNTPOINT")
csum_snap_root1=$(checksum_dir "$MOUNTPOINT/.snap/root_snap1")
csum_volumes1=$(checksum_dir "$VOLUMES")
csum_snap_volumes1=$(checksum_dir "$VOLUMES/.snap/volumes_snap1")
csum_group1=$(checksum_dir "$GROUP")
csum_snap_group1=$(checksum_dir "$GROUP/.snap/group_snap1")
csum_subvol1_1=$(checksum_dir "$SUBVOL1")
csum_snap_subvol1_1=$(checksum_dir "$SUBVOL1/.snap/subvol1_snap1")
csum_subvol2_1=$(checksum_dir "$SUBVOL2")
csum_snap_subvol2_1=$(checksum_dir "$SUBVOL2/.snap/subvol2_snap1")

# Verify snapshot checksums before write
verify_checksum "$csum_root1" "$csum_snap_root1" "$MOUNTPOINT/.snap/root_snap1" "root_snap1" "before write, second snap with hardlink/rename"
verify_checksum "$csum_volumes1" "$csum_snap_volumes1" "$VOLUMES/.snap/volumes_snap1" "volumes_snap1" "before write, second snap with hardlink/rename"
verify_checksum "$csum_group1" "$csum_snap_group1" "$GROUP/.snap/group_snap1" "group_snap1" "before write, second snap with hardlink/rename"
verify_checksum "$csum_subvol1_1" "$csum_snap_subvol1_1" "$SUBVOL1/.snap/subvol1_snap1" "subvol1_snap1" "before write, second snap with hardlink/rename"
verify_checksum "$csum_subvol2_1" "$csum_snap_subvol2_1" "$SUBVOL2/.snap/subvol2_snap1" "subvol2_snap1" "before write, second snap with hardlink/renaem"

# cleanup
rmdir "$MOUNTPOINT/.snap/root_snap"
rmdir "$VOLUMES/.snap/volumes_snap"
rmdir "$GROUP/.snap/group_snap"
rmdir "$SUBVOL1/.snap/subvol1_snap"
rmdir "$SUBVOL2/.snap/subvol2_snap"
rmdir "$MOUNTPOINT/.snap/root_snap1"
rmdir "$VOLUMES/.snap/volumes_snap1"
rmdir "$GROUP/.snap/group_snap1"
rmdir "$SUBVOL1/.snap/subvol1_snap1"
rmdir "$SUBVOL2/.snap/subvol2_snap1"
rm -rf $MOUNTPOINT/ln_volumes_file.txt
rm -rf $MOUNTPOINT/root_file.txt
rm -rf $MOUNTPOINT/$VOLUMES

echo OK
