#!/usr/bin/env bash

set -ex

function get_block_name_prefix() {
    rbd info --format=json $1 | python -c "import sys, json; print json.load(sys.stdin)['block_name_prefix']"
}

function do_pwrite() {
    local spec=$1
    local old_byte=$2
    local new_byte=$3

    local dev
    dev=$(sudo rbd map $spec)
    cmp <(dd if=/dev/zero bs=1M count=10 | tr \\000 \\$old_byte) $dev
    xfs_io -c "pwrite -b 1M -S $new_byte 0 10M" $dev
    sudo rbd unmap $dev
}

function do_cmp() {
    local spec=$1
    local byte=$2

    local dev
    dev=$(sudo rbd map $spec)
    cmp <(dd if=/dev/zero bs=1M count=10 | tr \\000 \\$byte) $dev
    sudo rbd unmap $dev
}

function gen_child_specs() {
    local i=$1

    local child_specs="foo/img$i-clone1 foo/img$i-clone2 foo/ns1/img$i-clone1 foo/ns1/img$i-clone2"
    if [[ $i -ge 3 ]]; then
        child_specs="$child_specs foo/ns2/img$i-clone1 foo/ns2/img$i-clone2"
    fi
    echo $child_specs
}

ceph osd pool create foo 12
rbd pool init foo
ceph osd pool create bar 12
rbd pool init bar

ceph osd set-require-min-compat-client nautilus
rbd namespace create foo/ns1
rbd namespace create foo/ns2

SPECS=(foo/img1 foo/img2 foo/ns1/img3 foo/ns1/img4)

COUNT=1
for spec in "${SPECS[@]}"; do
    if [[ $spec =~ img1|img3 ]]; then
        rbd create --size 10 $spec
    else
        rbd create --size 10 --data-pool bar $spec
    fi
    do_pwrite $spec 000 $(printf %03d $COUNT)
    rbd snap create $spec@snap
    COUNT=$((COUNT + 1))
done
for i in {1..4}; do
    for child_spec in $(gen_child_specs $i); do
        if [[ $child_spec =~ clone1 ]]; then
            rbd clone ${SPECS[i - 1]}@snap $child_spec
        else
            rbd clone --data-pool bar ${SPECS[i - 1]}@snap $child_spec
        fi
        do_pwrite $child_spec $(printf %03d $i) $(printf %03d $COUNT)
        COUNT=$((COUNT + 1))
    done
done

[[ $(rados -p foo ls | grep -c $(get_block_name_prefix foo/img1)) -eq 3 ]]
[[ $(rados -p bar ls | grep -c $(get_block_name_prefix foo/img2)) -eq 3 ]]
[[ $(rados -p foo -N ns1 ls | grep -c $(get_block_name_prefix foo/ns1/img3)) -eq 3 ]]
[[ $(rados -p bar -N ns1 ls | grep -c $(get_block_name_prefix foo/ns1/img4)) -eq 3 ]]

[[ $(rados -p foo ls | grep -c $(get_block_name_prefix foo/img1-clone1)) -eq 3 ]]
[[ $(rados -p bar ls | grep -c $(get_block_name_prefix foo/img1-clone2)) -eq 3 ]]
[[ $(rados -p foo -N ns1 ls | grep -c $(get_block_name_prefix foo/ns1/img1-clone1)) -eq 3 ]]
[[ $(rados -p bar -N ns1 ls | grep -c $(get_block_name_prefix foo/ns1/img1-clone2)) -eq 3 ]]

[[ $(rados -p foo ls | grep -c $(get_block_name_prefix foo/img2-clone1)) -eq 3 ]]
[[ $(rados -p bar ls | grep -c $(get_block_name_prefix foo/img2-clone2)) -eq 3 ]]
[[ $(rados -p foo -N ns1 ls | grep -c $(get_block_name_prefix foo/ns1/img2-clone1)) -eq 3 ]]
[[ $(rados -p bar -N ns1 ls | grep -c $(get_block_name_prefix foo/ns1/img2-clone2)) -eq 3 ]]

[[ $(rados -p foo ls | grep -c $(get_block_name_prefix foo/img3-clone1)) -eq 3 ]]
[[ $(rados -p bar ls | grep -c $(get_block_name_prefix foo/img3-clone2)) -eq 3 ]]
[[ $(rados -p foo -N ns1 ls | grep -c $(get_block_name_prefix foo/ns1/img3-clone1)) -eq 3 ]]
[[ $(rados -p bar -N ns1 ls | grep -c $(get_block_name_prefix foo/ns1/img3-clone2)) -eq 3 ]]
[[ $(rados -p foo -N ns2 ls | grep -c $(get_block_name_prefix foo/ns2/img3-clone1)) -eq 3 ]]
[[ $(rados -p bar -N ns2 ls | grep -c $(get_block_name_prefix foo/ns2/img3-clone2)) -eq 3 ]]

[[ $(rados -p foo ls | grep -c $(get_block_name_prefix foo/img4-clone1)) -eq 3 ]]
[[ $(rados -p bar ls | grep -c $(get_block_name_prefix foo/img4-clone2)) -eq 3 ]]
[[ $(rados -p foo -N ns1 ls | grep -c $(get_block_name_prefix foo/ns1/img4-clone1)) -eq 3 ]]
[[ $(rados -p bar -N ns1 ls | grep -c $(get_block_name_prefix foo/ns1/img4-clone2)) -eq 3 ]]
[[ $(rados -p foo -N ns2 ls | grep -c $(get_block_name_prefix foo/ns2/img4-clone1)) -eq 3 ]]
[[ $(rados -p bar -N ns2 ls | grep -c $(get_block_name_prefix foo/ns2/img4-clone2)) -eq 3 ]]

COUNT=1
for spec in "${SPECS[@]}"; do
    do_cmp $spec $(printf %03d $COUNT)
    COUNT=$((COUNT + 1))
done
for i in {1..4}; do
    for child_spec in $(gen_child_specs $i); do
        do_cmp $child_spec $(printf %03d $COUNT)
        COUNT=$((COUNT + 1))
    done
done

echo OK
