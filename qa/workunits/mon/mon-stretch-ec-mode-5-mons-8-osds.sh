#!/bin/bash -ex

# A bash script for setting up stretch mode with 5 monitors and 8 OSDs.

NUM_OSDS_UP=$(ceph osd df | grep "up" | wc -l)

if [ $NUM_OSDS_UP -lt 8 ]; then
    echo "test requires at least 8 OSDs up and running"
    exit 1
fi

# ensure election strategy is set to "connectivity"
# See https://tracker.ceph.com/issues/69107
ceph mon set election_strategy connectivity

for dc in dc1 dc2
    do
      ceph osd crush add-bucket $dc datacenter
      ceph osd crush move $dc root=default
    done

ceph osd crush add-bucket host01 host
ceph osd crush add-bucket host02 host
ceph osd crush add-bucket host03 host
ceph osd crush add-bucket host04 host

ceph osd crush move host01 datacenter=dc1
ceph osd crush move host02 datacenter=dc1
ceph osd crush move host03 datacenter=dc2
ceph osd crush move host04 datacenter=dc2

ceph osd crush move osd.0 host=host01
ceph osd crush move osd.1 host=host01
ceph osd crush move osd.2 host=host02
ceph osd crush move osd.3 host=host02
ceph osd crush move osd.4 host=host03
ceph osd crush move osd.5 host=host03
ceph osd crush move osd.6 host=host04
ceph osd crush move osd.7 host=host04

# set location for monitors
ceph mon set_location a datacenter=dc1 host=host01
ceph mon set_location b datacenter=dc1 host=host02
ceph mon set_location c datacenter=dc2 host=host03
ceph mon set_location d datacenter=dc2 host=host04

# set location for tiebreaker monitor
ceph mon set_location e datacenter=dc3 host=host05

# remove the current host from crush map
hostname=$(hostname -s)
ceph osd crush remove $hostname
# create a new crush rule with stretch ec rule
ceph osd erasure-code-profile set stretch_ec_profile plugin=jerasure k=2 m=1
ceph osd crush rule create-erasure stretch_ec_rule stretch_ec_profile 2
