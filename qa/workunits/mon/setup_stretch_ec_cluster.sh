#!/usr/bin/env bash

set -ex

NUM_OSDS_UP=$(ceph osd df | grep "up" | wc -l)

if [ $NUM_OSDS_UP -lt 8 ]; then
    echo "test requires at least 8 OSDs up and running"
    exit 1
fi

NUM_MONS=$(ceph mon dump --format=json | jq '.mons | length')

if [ $NUM_MONS -lt 5 ]; then
    echo "test requires at least 5 monitors (a, b, c, d, e)"
    exit 1
fi


ceph mon set election_strategy connectivity
ceph mon add disallowed_leader e

for dc in dc1 dc2
    do
      ceph osd crush add-bucket $dc datacenter
      ceph osd crush move $dc root=default
    done

ceph osd crush add-bucket node-2 host
ceph osd crush add-bucket node-3 host
ceph osd crush add-bucket node-4 host
ceph osd crush add-bucket node-5 host
ceph osd crush add-bucket node-6 host
ceph osd crush add-bucket node-7 host
ceph osd crush add-bucket node-8 host
ceph osd crush add-bucket node-9 host

ceph osd crush move node-2 datacenter=dc1
ceph osd crush move node-3 datacenter=dc1
ceph osd crush move node-4 datacenter=dc1
ceph osd crush move node-5 datacenter=dc1

ceph osd crush move node-6 datacenter=dc2
ceph osd crush move node-7 datacenter=dc2
ceph osd crush move node-8 datacenter=dc2
ceph osd crush move node-9 datacenter=dc2

ceph osd crush move osd.0 host=node-2
ceph osd crush move osd.1 host=node-3
ceph osd crush move osd.2 host=node-4
ceph osd crush move osd.3 host=node-5

ceph osd crush move osd.4 host=node-6
ceph osd crush move osd.5 host=node-7
ceph osd crush move osd.6 host=node-8
ceph osd crush move osd.7 host=node-9


ceph mon set_location a datacenter=dc1 host=node-2
ceph mon set_location b datacenter=dc1 host=node-3
ceph mon set_location c datacenter=dc2 host=node-6
ceph mon set_location d datacenter=dc2 host=node-7

hostname=$(hostname -s)
ceph osd crush remove $hostname ||  { echo 'command failed' ; exit 1; }

ceph osd erasure-code-profile set stretch_ec_profile plugin=jerasure k=2 m=1 crush-num-osd-failure-domains=2

ceph osd crush rule create-erasure stretch_rule stretch_ec_profile 2

# rule stretch_rule {
#         id 1
#         type erasure
#         step set_chooseleaf_tries 5
#         step set_choose_tries 100
#         step take default
#         step choose firstn 0 type datacenter
#         step chooseleaf indep 6 type host

# }
# end crush map
ceph osd pool rm .mgr .mgr --yes-i-really-really-mean-it

ceph mon set_location e datacenter=arbiter host=node-1 || { echo 'command failed' ; exit 1; }

stretched_poolname=stretch_pool
ceph osd pool create $stretched_poolname 32 32 erasure --rule=stretch_rule --num_zones=2 || { echo 'command failed' ; exit 1; }
ceph osd pool set $stretched_poolname allow_ec_optimizations true || { echo 'command failed' ; exit 1; }
ceph osd pool application enable $stretched_poolname rados || { echo 'command failed' ; exit 1; }