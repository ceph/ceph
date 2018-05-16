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
create_image $image
create_group $group
add_image_to_group $image $group
create_snapshot $group $snap
check_snapshot_in_group $group $snap
rename_snapshot $group $snap $new_snap
check_snapshot_not_in_group $group $snap
remove_snapshot $group $new_snap
check_snapshot_not_in_group $group $new_snap
remove_group $group
remove_image $image
echo "PASSED"

echo "OK"
