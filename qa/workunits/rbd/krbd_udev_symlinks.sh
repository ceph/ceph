#!/usr/bin/env bash

set -ex

SPECS=(
rbd/img1
rbd/img2
rbd/img2@snap1
rbd/img3
rbd/img3@snap1
rbd/img3@snap2
rbd/ns1/img1
rbd/ns1/img2
rbd/ns1/img2@snap1
rbd/ns1/img3
rbd/ns1/img3@snap1
rbd/ns1/img3@snap2
rbd/ns2/img1
rbd/ns2/img2
rbd/ns2/img2@snap1
rbd/ns2/img3
rbd/ns2/img3@snap1
rbd/ns2/img3@snap2
custom/img1
custom/img1@snap1
custom/img2
custom/img2@snap1
custom/img2@snap2
custom/img3
custom/ns1/img1
custom/ns1/img1@snap1
custom/ns1/img2
custom/ns1/img2@snap1
custom/ns1/img2@snap2
custom/ns1/img3
custom/ns2/img1
custom/ns2/img1@snap1
custom/ns2/img2
custom/ns2/img2@snap1
custom/ns2/img2@snap2
custom/ns2/img3
)

ceph osd pool create custom 8
rbd pool init custom

ceph osd set-require-min-compat-client nautilus
rbd namespace create rbd/ns1
rbd namespace create rbd/ns2
rbd namespace create custom/ns1
rbd namespace create custom/ns2

# create in order, images before snapshots
for spec in "${SPECS[@]}"; do
    if [[ "$spec" =~ snap ]]; then
        rbd snap create "$spec"
    else
        rbd create --size 10 "$spec"
        DEV="$(sudo rbd map "$spec")"
        sudo sfdisk "$DEV" <<EOF
unit: sectors
${DEV}p1 : start=        2048, size=           2, type=83
${DEV}p2 : start=        4096, size=           2, type=83
EOF
        sudo rbd unmap "$DEV"
    fi
done

[[ ! -e /dev/rbd ]]

# map in random order
COUNT=${#SPECS[@]}
read -r -a INDEXES < <(python3 <<EOF
import random
l = list(range($COUNT))
random.shuffle(l)
print(*l)
EOF
)

DEVS=()
for idx in "${INDEXES[@]}"; do
    DEVS+=("$(sudo rbd map "${SPECS[idx]}")")
done

[[ $(rbd showmapped | wc -l) -eq $((COUNT + 1)) ]]

for ((i = 0; i < COUNT; i++)); do
    [[ "$(readlink -e "/dev/rbd/${SPECS[INDEXES[i]]}")" == "${DEVS[i]}" ]]
    [[ "$(readlink -e "/dev/rbd/${SPECS[INDEXES[i]]}-part1")" == "${DEVS[i]}p1" ]]
    [[ "$(readlink -e "/dev/rbd/${SPECS[INDEXES[i]]}-part2")" == "${DEVS[i]}p2" ]]
done

for idx in "${INDEXES[@]}"; do
    sudo rbd unmap "/dev/rbd/${SPECS[idx]}"
done

[[ ! -e /dev/rbd ]]

# remove in reverse order, snapshots before images
for ((i = COUNT - 1; i >= 0; i--)); do
    if [[ "${SPECS[i]}" =~ snap ]]; then
        rbd snap rm "${SPECS[i]}"
    else
        rbd rm "${SPECS[i]}"
    fi
done

rbd namespace rm custom/ns2
rbd namespace rm custom/ns1
rbd namespace rm rbd/ns2
rbd namespace rm rbd/ns1

ceph osd pool delete custom custom --yes-i-really-really-mean-it

echo OK
