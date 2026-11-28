#!/bin/bash -ex

# osd pool stretch set and osd pool stretch unset are for replicated pools:
# an erasure coded pool goes over two zones with osd pool set <pool>
# num_zones 2 instead. This test creates the erasure coded pool
# stretch_ec_pool (k=2 m=1), and expects osd pool stretch show to fail
# before the pool exists and after, as it is not a stretch pool. osd pool
# stretch set is refused for a pool that does not exist and, for
# stretch_ec_pool, with an argument missing. Given valid arguments,
# stretch_ec_rule among them, a rule over the two datacenters, it is
# refused as not supported for erasure coded pools, before any other check;
# mon-stretch-pool.sh checks the barrier and the rule with a replicated
# pool. osd pool stretch unset is refused, as stretch_ec_pool is not a
# stretch pool.
# It also lays out the cluster for tasks.stretch_ec_cluster, which the
# rados/singleton mon-stretch-ec-pool job runs next: datacenters dc1 and
# dc2, each of 3 hosts with one OSD, monitors a to c in dc1 and d to f in
# dc2, and g in dc3, which has no OSDs, as the tiebreaker.

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

ceph mon set_location a datacenter=dc1 host=node-1
ceph mon set_location b datacenter=dc1 host=node-2
ceph mon set_location c datacenter=dc1 host=node-3
ceph mon set_location d datacenter=dc2 host=node-4
ceph mon set_location e datacenter=dc2 host=node-5
ceph mon set_location f datacenter=dc2 host=node-6
# set location for tiebreaker monitor: the pools over two zones that
# tasks.stretch_ec_cluster creates next turn on the cluster stretch flags,
# which need one
ceph mon set_location g datacenter=dc3 host=node-7

ceph mon set election_strategy connectivity

# the erasure coded pool, its profile and a rule over the two datacenters
TEST_POOL_STRETCH_EC=stretch_ec_pool
TEST_STRETCH_EC_PROFILE=stretch_ec_profile
TEST_STRETCH_EC_RULE=stretch_ec_rule

# A non-existent pool should return an error.
expect_false ceph osd pool stretch show $TEST_POOL_STRETCH_EC

expect_true ceph osd erasure-code-profile set $TEST_STRETCH_EC_PROFILE plugin=jerasure k=2 m=1
expect_true ceph osd crush rule create-erasure $TEST_STRETCH_EC_RULE $TEST_STRETCH_EC_PROFILE 2
expect_true ceph osd pool create $TEST_POOL_STRETCH_EC erasure --erasure_code_profile=$TEST_STRETCH_EC_PROFILE

# A freshly created pool is not a stretch pool yet.
expect_false ceph osd pool stretch show $TEST_POOL_STRETCH_EC

# Missing arguments must fail.
expect_false ceph osd pool stretch set $TEST_POOL_STRETCH_EC 2 2 datacenter $TEST_STRETCH_EC_RULE
# Non-existent pool must fail.
expect_false ceph osd pool stretch set non_exist_pool 2 2 datacenter $TEST_STRETCH_EC_RULE 6 2

# An erasure coded pool goes over two zones with osd pool set <pool>
# num_zones 2, not with osd pool stretch set or unset, which refuse it.
expect_false ceph osd pool stretch set $TEST_POOL_STRETCH_EC 2 2 datacenter $TEST_STRETCH_EC_RULE 6 2
ceph osd pool stretch set $TEST_POOL_STRETCH_EC 2 2 datacenter $TEST_STRETCH_EC_RULE 6 2 2>&1 |
    grep "osd pool stretch set is not supported for EC pools"
expect_false ceph osd pool stretch show $TEST_POOL_STRETCH_EC
expect_false ceph osd pool stretch unset $TEST_POOL_STRETCH_EC $TEST_STRETCH_EC_RULE 3 2
ceph osd pool stretch unset $TEST_POOL_STRETCH_EC $TEST_STRETCH_EC_RULE 3 2 2>&1 |
    grep "pool $TEST_POOL_STRETCH_EC is not a stretch pool"

# cleanup
teardown
ceph osd crush rule rm $TEST_STRETCH_EC_RULE
