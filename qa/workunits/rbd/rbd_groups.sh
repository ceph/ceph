#!/usr/bin/env bash

set -ex

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
    list_groups | grep -w $group_name
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

create_snapshots()
{
    local group_name=$1
    local snap_name=$2
    local snap_count=$3
    for i in `seq 1 $snap_count`; do
        rbd group snap create $group_name@$snap_name$i
    done
}

remove_snapshot()
{
    local group_name=$1
    local snap_name=$2
    rbd group snap remove $group_name@$snap_name
}

remove_snapshots()
{
    local group_name=$1
    local snap_name=$2
    local snap_count=$3
    for i in `seq 1 $snap_count`; do
        rbd group snap remove $group_name@$snap_name$i
    done
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
    list_snapshots $group_name | grep -w $snap_name
}

check_snapshots_count_in_group()
{
    local group_name=$1
    local snap_name=$2
    local expected_count=$3
    local actual_count
    actual_count=$(list_snapshots $group_name | grep -c $snap_name)
    (( actual_count == expected_count ))
}

check_snapshot_not_in_group()
{
    local group_name=$1
    local snap_name=$2

    check_group_exists $group_name || return 1
    ! check_snapshot_in_group $group_name $snap_name
}

check_snap_id_in_list_snapshots()
{
    local group_name=$1
    local snap_name=$2

    local snap_id_in_info=$(
        rbd group snap info $group_name@$snap_name --format=json |
        jq -r '.id')
    [[ -n "$snap_id_in_info" ]] || return 1

    local snap_id_in_list=$(
        rbd group snap ls $group_name --format=json |
        jq --arg snap_name $snap_name -r '
            .[] | select(.snapshot == $snap_name) | .id')
    test "$snap_id_in_list" = "$snap_id_in_info"
}

check_snapshot_info()
{
    local group_name=$1
    local snap_name=$2
    local image_count=$3

    local snap_info_json=$(
        rbd group snap info $group_name@$snap_name --format=json)
    local actual_snap_name=$(jq -r ".name" <<< "$snap_info_json")
    test "$actual_snap_name" = "$snap_name" || return 1

    local snap_state=$(jq -r ".state" <<< "$snap_info_json")
    test "$snap_state" = "complete" || return 1

    local actual_image_count=$(jq '.images | length' <<< "$snap_info_json")
    test "$actual_image_count" = "$image_count" || return 1

    local image_snap_name=$(jq -r '.image_snap_name' <<< "$snap_info_json")
    local snap_info=$(rbd group snap info $group_name@$snap_name)
    local snap_state=$(grep -w 'state:' <<< "$snap_info" | tr -d '\t')
    test "$snap_state" = "state: complete" || return 1
    local image_snap_field=$(grep -w 'image snap:' <<< "$snap_info")
    local images_field=$(grep -w 'images:' <<< "$snap_info")
    if ((image_count != 0)); then
        test -n "$image_snap_name" || return 1
        test -n "$image_snap_field" || return 1
        test -n "$images_field" || return 1
    else
        test -z "$image_snap_name" || return 1
        test -z "$image_snap_field" || return 1
        test -z "$images_field" || return 1
    fi
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
snaps=("group_snap1" "group_snap2" "group_snap3" "group_snap4")
create_image $image
create_group $group
create_snapshot $group ${snaps[0]}
check_snapshot_info $group ${snaps[0]} 0
add_image_to_group $image $group
create_snapshot $group ${snaps[1]}
check_snapshot_info $group ${snaps[1]} 1
rename_snapshot $group ${snaps[1]} ${snaps[2]}
check_snapshot_info $group ${snaps[2]} 1
check_snapshot_not_in_group $group ${snaps[1]}
create_snapshot $group ${snaps[3]}
check_snapshot_in_group $group ${snaps[3]}
rollback_snapshot $group ${snaps[2]}
remove_snapshot $group ${snaps[2]}
check_snapshot_not_in_group $group ${snaps[2]}
remove_snapshot $group ${snaps[3]}
check_snapshot_not_in_group $group ${snaps[3]}
remove_group $group
remove_image $image
echo "PASSED"

echo "TEST: list snapshots of consistency group"
image="test_image"
group="test_consistency_group"
snap="group_snap"
create_image $image
create_group $group
add_image_to_group $image $group
create_snapshots $group $snap 10
check_snapshots_count_in_group $group $snap 10
check_snap_id_in_list_snapshots $group ${snap}1
remove_snapshots $group $snap 10
create_snapshots $group $snap 100
check_snapshots_count_in_group $group $snap 100
remove_snapshots $group $snap 100
remove_group $group
remove_image $image
echo "PASSED"

echo "OK"
