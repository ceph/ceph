#!/bin/bash -ex

# A CLI test for ceph osd pool stretch set and ceph osd pool stretch show.
# Sets up the cluster with 3 datacenters and 3 hosts in each datacenter

NUM_OSDS_UP=$(ceph osd df | grep "up" | wc -l)

if [ $NUM_OSDS_UP -lt 6 ]; then
    echo "test requires at least 6 OSDs up and running"
    exit 1
fi

function expect_false()
{
  # expect the command to return false
	if "$@"; then return 1; else return 0; fi
}

function expect_true()
{
    # expect the command to return true
    if "$@"; then return 0; else return 1; fi
}

function teardown()
{
    # cleanup
    for pool in `ceph osd pool ls`
    do
      ceph osd pool rm $pool $pool --yes-i-really-really-mean-it
    done
}

for dc in dc1 dc2
    do
      ceph osd crush add-bucket $dc datacenter
      ceph osd crush move $dc root=default
    done

ceph osd crush add-bucket node-1 host
ceph osd crush add-bucket node-2 host
ceph osd crush add-bucket node-3 host
ceph osd crush add-bucket node-4 host
ceph osd crush add-bucket node-5 host
ceph osd crush add-bucket node-6 host

ceph osd crush move node-1 datacenter=dc1
ceph osd crush move node-2 datacenter=dc1
ceph osd crush move node-3 datacenter=dc1
ceph osd crush move node-4 datacenter=dc2
ceph osd crush move node-5 datacenter=dc2
ceph osd crush move node-6 datacenter=dc2

ceph osd crush move osd.0 host=node-1
ceph osd crush move osd.1 host=node-2
ceph osd crush move osd.2 host=node-3
ceph osd crush move osd.3 host=node-4
ceph osd crush move osd.4 host=node-5
ceph osd crush move osd.5 host=node-6

ceph osd crush move dc1 root=default
ceph osd crush move dc2 root=default

ceph mon set_location a datacenter=dc1 host=node-1
ceph mon set_location b datacenter=dc1 host=node-2
ceph mon set_location c datacenter=dc1 host=node-3
ceph mon set_location d datacenter=dc2 host=node-4
ceph mon set_location e datacenter=dc2 host=node-5
ceph mon set_location f datacenter=dc2 host=node-6
ceph mon set_location g datacenter=arbiter

#stretch ec configuration
TEST_POOL_STRETCH_EC=stretch_ec_pool
TEST_STRETCH_EC_PROFILE=stretch_ec_profile
TEST_STRETCH_EC_RULE=stretch_ec_rule

expect_true ceph osd erasure-code-profile set $TEST_STRETCH_EC_PROFILE plugin=jerasure k=2 m=1

expect_true ceph osd crush rule create-erasure $TEST_STRETCH_EC_RULE $TEST_STRETCH_EC_PROFILE 2

expect_true ceph osd pool create $TEST_POOL_STRETCH_EC erasure --erasure_code_profile=$TEST_STRETCH_EC_PROFILE

# cleanup
teardown

