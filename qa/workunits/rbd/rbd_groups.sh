#!/bin/sh -ex

#
# rbd_consistency_groups.sh - test consistency groups cli commands
#

#
# Functions
#

create_group()
{
    local group_name=$1

    rbd group create $group_name
}

list_groups()
{
    rbd group list
}

check_group_exists()
{
    local group_name=$1
    list_groups | grep $group_name
}

remove_group()
{
    local group_name=$1

    rbd group remove $group_name
}

rename_group()
{
    local src_name=$1
    local dest_name=$2

    rbd group rename $src_name $dest_name
}

check_group_does_not_exist()
{
    local group_name=$1
    for v in $(list_groups); do
        if [ "$v" == "$group_name" ]; then
            return 1
        fi
    done
    return 0
}

create_image()
{
    local image_name=$1
    rbd create --size 10M $image_name
}

remove_image()
{
    local image_name=$1
    rbd remove $image_name
}

add_image_to_group()
{
    local image_name=$1
    local group_name=$2
    rbd group image add $group_name $image_name
}

remove_image_from_group()
{
    local image_name=$1
    local group_name=$2
    rbd group image remove $group_name $image_name
}

check_image_in_group()
{
    local image_name=$1
    local group_name=$2
    for v in $(rbd group image list $group_name); do
        local vtrimmed=${v#*/}
        if [ "$vtrimmed" = "$image_name" ]; then
            return 0
        fi
    done
    return 1
}

check_image_not_in_group()
{
    local image_name=$1
    local group_name=$2
    for v in $(rbd group image list $group_name); do
        local vtrimmed=${v#*/}
        if [ "$vtrimmed" = "$image_name" ]; then
            return 1
        fi
    done
    return 0
}

create_snapshot()
{
    local group_name=$1
    local snap_name=$2
    rbd group snap create $group_name@$snap_name
}

remove_snapshot()
{
    local group_name=$1
    local snap_name=$2
    rbd group snap remove $group_name@$snap_name
}

rename_snapshot()
{
    local group_name=$1
    local snap_name=$2
    local new_snap_name=$3
    rbd group snap rename $group_name@$snap_name $new_snap_name
}

list_snapshots()
{
    local group_name=$1
    rbd group snap list $group_name
}

rollback_snapshot()
{
    local group_name=$1
    local snap_name=$2
    rbd group snap rollback $group_name@$snap_name
}

check_snapshot_in_group()
{
    local group_name=$1
    local snap_name=$2
    list_snapshots $group_name | grep $snap_name
}

check_snapshot_not_in_group()
{
    local group_name=$1
    local snap_name=$2
    for v in $(list_snapshots $group_name | awk '{print $1}'); do
        if [ "$v" = "$snap_name" ]; then
            return 1
        fi
    done
    return 0
}

write_image()
{
    local image_name=$1
    rbd bench --io-total 10M --io-type write --io-pattern rand $image_name
}

export_to_file()
{
    local group_name=$1
    local file_path=$2
    rbd group export $group_name $file_path
}

export_diff_to_file()
{
    local group_name=$1
    local file_path=$2
    local group_from_snap=$3
    local group_end_snap=$4
    if [ "$group_from_snap" == "start" ] && [ "$group_end_snap" == "end" ];then
        rbd group export-diff $group_name $file_path
    elif [ "$group_from_snap" == "start" ];then
        rbd group export-diff $group_name@$group_end_snap $file_path
    elif [ "$group_end_snap" == "end" ];then
        rbd group export-diff $group_name --from-snap $group_from_snap $file_path
    else
        rbd group export-diff $group_name@$group_end_snap --from-snap $group_from_snap $file_path
    fi
}

import_from_file()
{
    local file_path=$1
    local group_name=$2
    rbd group import $file_path $group_name
}

import_diff_from_file()
{
    local file_path=$1
    local group_name=$2
    rbd group import-diff $file_path $group_name
}

cmp_file()
{
    local file1=$1
    local file2=$2
    cmp $file1 $file2
}

echo "TEST: create remove consistency group"
group="test_consistency_group"
new_group="test_new_consistency_group"
create_group $group
check_group_exists $group
rename_group $group $new_group
check_group_exists $new_group
remove_group $new_group
check_group_does_not_exist $new_group
echo "PASSED"

echo "TEST: add remove images to consistency group"
image="test_image"
group="test_consistency_group"
create_image $image
create_group $group
add_image_to_group $image $group
check_image_in_group $image $group
remove_image_from_group $image $group
check_image_not_in_group $image $group
remove_group $group
remove_image $image
echo "PASSED"

echo "TEST: create remove snapshots of consistency group"
image="test_image"
group="test_consistency_group"
snap="group_snap"
new_snap="new_group_snap"
sec_snap="group_snap2"
create_image $image
create_group $group
add_image_to_group $image $group
create_snapshot $group $snap
check_snapshot_in_group $group $snap
rename_snapshot $group $snap $new_snap
check_snapshot_not_in_group $group $snap
create_snapshot $group $sec_snap
check_snapshot_in_group $group $sec_snap
rollback_snapshot $group $new_snap
remove_snapshot $group $new_snap
check_snapshot_not_in_group $group $new_snap
remove_snapshot $group $sec_snap
check_snapshot_not_in_group $group $sec_snap
remove_group $group
remove_image $image
echo "PASSED"

echo "TEST: export/import of consistency group"
image="test_image"
group="test_consistency_group"
group_import="test_consistency_group_import"
snap1="group_snap_v1"
snap2="group_snap_v2"
export_file="/tmp/${group}_export"
export_start_to_v1_file="/tmp/${group}_export_v1"
export_v1_to_v2_file="/tmp/${group}_export_v1_to_v2"
export_v2_to_end_file="/tmp/${group}_export_v2_to_end"
group_import_export_file="/tmp/${group_import}_export"
group_import_export_start_to_v1_file="/tmp/${group_import}_export_v1"
group_import_export_v1_to_v2_file="/tmp/${group_import}_export_v1_to_v2"
group_import_export_v2_to_end_file="/tmp/${group_import}_export_v2_to_end"
create_image $image
create_group $group
add_image_to_group $image $group
write_image $image
create_snapshot $group $snap1
write_image $image
create_snapshot $group $snap2
write_image $image
export_to_file $group $export_file
export_diff_to_file $group $export_start_to_v1_file "start" $snap1
export_diff_to_file $group $export_v1_to_v2_file $snap1 $snap2
export_diff_to_file $group $export_v2_to_end_file $snap2 "end"
remove_snapshot $group $snap2
remove_snapshot $group $snap1
remove_image_from_group $image $group
remove_image $image
remove_group $group
import_from_file $export_file $group_import
export_to_file $group_import $group_import_export_file
export_diff_to_file $group_import $group_import_export_start_to_v1_file "start" $snap1
export_diff_to_file $group_import $group_import_export_v1_to_v2_file $snap1 $snap2
export_diff_to_file $group_import $group_import_export_v2_to_end_file $snap2 "end"
cmp_file $export_file $group_import_export_file
cmp_file $export_start_to_v1_file $group_import_export_start_to_v1_file
cmp_file $export_v1_to_v2_file $group_import_export_v1_to_v2_file
cmp_file $export_v2_to_end_file $group_import_export_v2_to_end_file
remove_snapshot $group_import $snap2
remove_snapshot $group_import $snap1
remove_image_from_group $image $group_import
remove_image $image
rm -f $group_import_export_file
rm -f $group_import_export_start_to_v1_file
rm -f $group_import_export_v1_to_v2_file
rm -f $group_import_export_v2_to_end_file
import_diff_from_file $export_start_to_v1_file $group_import
import_diff_from_file $export_v1_to_v2_file $group_import
import_diff_from_file $export_v2_to_end_file $group_import
export_to_file $group_import $group_import_export_file
export_diff_to_file $group_import $group_import_export_start_to_v1_file "start" $snap1
export_diff_to_file $group_import $group_import_export_v1_to_v2_file $snap1 $snap2
export_diff_to_file $group_import $group_import_export_v2_to_end_file $snap2 "end"
cmp_file $export_file $group_import_export_file
cmp_file $export_start_to_v1_file $group_import_export_start_to_v1_file
cmp_file $export_v1_to_v2_file $group_import_export_v1_to_v2_file
cmp_file $export_v2_to_end_file $group_import_export_v2_to_end_file
remove_snapshot $group_import $snap2
remove_snapshot $group_import $snap1
remove_image_from_group $image $group_import
remove_image $image
remove_group $group_import
rm -f $group_import_export_file
rm -f $group_import_export_start_to_v1_file
rm -f $group_import_export_v1_to_v2_file
rm -f $group_import_export_v2_to_end_file
rm -f $export_file
rm -f $export_start_to_v1_file
rm -f $export_v1_to_v2_file
rm -f $export_v2_to_end_file
echo "PASSED"

echo "OK"
