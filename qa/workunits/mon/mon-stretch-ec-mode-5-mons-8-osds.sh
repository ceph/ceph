#!/bin/bash -ex

# Lays out the cluster for tasks.stretch_ec_mode_disable_enable, which the
# rados/singleton stretch-ec-mode-5-mons-8-osds job runs next: datacenters
# dc1 and dc2, each of 2 hosts with 2 OSDs, monitors a and b in dc1, c and d
# in dc2, and e in dc3, which has no OSDs, as the tiebreaker. It also makes
# the erasure code profile stretch_ec_profile (k=2 m=1) and its rule over
# the two datacenters, stretch_ec_rule, for the pool that the task creates.

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
# a datacenter has 2 hosts for the 3 shards of each PG in it, so the
# profile places shards on OSDs; create-erasure with 2 zones makes a rule
# with 3 shards in each datacenter
ceph osd erasure-code-profile set stretch_ec_profile plugin=jerasure k=2 m=1 \
    crush-failure-domain=osd
ceph osd crush rule create-erasure stretch_ec_rule stretch_ec_profile 2
