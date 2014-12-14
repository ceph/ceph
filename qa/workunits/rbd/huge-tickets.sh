#!/bin/bash

# This is a test for http://tracker.ceph.com/issues/8979 and the fallout
# from triaging it.  #8979 itself was random crashes on corrupted memory
# due to a buffer overflow (for tickets larger than 256 bytes), further
# inspection showed that vmalloced tickets weren't handled correctly as
# well.
#
# What we are doing here is generating three huge keyrings and feeding
# them to libceph (through 'rbd map' on a scratch image).  Bad kernels
# will crash reliably either on corrupted memory somewhere or a bad page
# fault in scatterwalk_pagedone().

set -ex

function generate_keyring() {
    local user=$1
    local n=$2

    ceph-authtool -C -n client.$user --cap mon 'allow *' --gen-key /tmp/keyring-$user

    set +x # don't pollute trace with echos
    echo -en "\tcaps osd = \"allow rwx pool=rbd" >>/tmp/keyring-$user
    for i in $(seq 1 $n); do
        echo -n ", allow rwx pool=pool$i" >>/tmp/keyring-$user
    done
    echo "\"" >>/tmp/keyring-$user
    set -x
}

generate_keyring foo 1000 # ~25K, kmalloc
generate_keyring bar 20000 # ~500K, vmalloc
generate_keyring baz 300000 # ~8M, vmalloc + sg chaining

rbd create --size 1 test

for user in {foo,bar,baz}; do
    ceph auth import -i /tmp/keyring-$user
    DEV=$(sudo rbd map -n client.$user --keyring /tmp/keyring-$user test)
    sudo rbd unmap $DEV
done
