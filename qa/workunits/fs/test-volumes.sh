#!/bin/bash -ex

function expect_false()
{
    set -x
    if "$@"; then return 1; else return 0; fi
}

# make sure we can do multiple file systems
ceph fs flag set enable_multiple true --yes-i-really-mean-it

# create a volume if one doesn't exist
if `ceph fs volume ls | jq 'length'` -eq 0 ; then
    EXISTING='foo'
    ceph fs volume create $EXISTING
else
    EXISTING=`ceph fs volume ls | jq -r '.[0].name'`
fi



# create and remove volumes
if ceph fs volume ls | grep bar; then
    echo 'uh, volume bar already exists, bailing'
    exit 1
fi
ceph fs volume create bar
ceph fs volume ls | grep bar
ceph fs volume rm bar

# subvolumes on $EXISTING
ceph fs subvolume create $EXISTING sub1
ceph fs subvolume create $EXISTING sub2
ceph fs subvolume rm $EXISTING sub2
ceph fs subvolume rm $EXISTING sub1

echo OK
