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

for dc in dc1 dc2 dc3
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
ceph osd crush add-bucket node-7 host
ceph osd crush add-bucket node-8 host
ceph osd crush add-bucket node-9 host

ceph osd crush move node-1 datacenter=dc1
ceph osd crush move node-2 datacenter=dc1
ceph osd crush move node-3 datacenter=dc1
ceph osd crush move node-4 datacenter=dc2
ceph osd crush move node-5 datacenter=dc2
ceph osd crush move node-6 datacenter=dc2
ceph osd crush move node-7 datacenter=dc3
ceph osd crush move node-8 datacenter=dc3
ceph osd crush move node-9 datacenter=dc3

ceph osd crush move osd.0 host=node-1
ceph osd crush move osd.1 host=node-2
ceph osd crush move osd.2 host=node-3
ceph osd crush move osd.3 host=node-4
ceph osd crush move osd.4 host=node-5
ceph osd crush move osd.5 host=node-6
ceph osd crush move osd.6 host=node-7
ceph osd crush move osd.7 host=node-8
ceph osd crush move osd.8 host=node-9

ceph mon set_location a datacenter=dc1 host=node-1
ceph mon set_location b datacenter=dc1 host=node-2
ceph mon set_location c datacenter=dc1 host=node-3
ceph mon set_location d datacenter=dc2 host=node-4
ceph mon set_location e datacenter=dc2 host=node-5
ceph mon set_location f datacenter=dc2 host=node-6
ceph mon set_location g datacenter=dc3 host=node-7
ceph mon set_location h datacenter=dc3 host=node-8
ceph mon set_location i datacenter=dc3 host=node-9


TEST_POOL_STRETCH=pool_stretch
TEST_CRUSH_RULE=replicated_rule_custom

# Non existence pool should return error
expect_false ceph osd pool stretch show $TEST_POOL_STRETCH

ceph osd pool create $TEST_POOL_STRETCH 1

# pool must be a stretch pool for this command to show anything.
expect_false ceph osd pool stretch show $TEST_POOL_STRETCH

# All Argument must present
expect_false ceph osd pool stretch set $TEST_POOL_STRETCH 2 3 datacenter $TEST_CRUSH_RULE
# Non existence pool should return error
expect_false ceph osd pool stretch set non_exist_pool 2 3 datacenter $TEST_CRUSH_RULE 6 3
# Non existence barrier should return appropriate error
expect_false ceph osd pool stretch set $TEST_POOL_STRETCH 2 3 non_exist_barrier $TEST_CRUSH_RULE 6 3
# Non existence crush_rule should return appropriate error
expect_false ceph osd pool stretch set $TEST_POOL_STRETCH 2 3 datacenter $TEST_CRUSH_RULE 6 3
# Unsetting a pool with missing arguments
expect_false ceph osd pool stretch unset $TEST_POOL_STRETCH
# Unsetting a non existence pool should return error
expect_false ceph osd pool stretch unset non_exist_pool replicated_rule 6 3
# Unsetting a non-stretch pool should return error
expect_false ceph osd pool stretch unset $TEST_POOL_STRETCH replicated_rule 6 3

# Create a custom crush rule
ceph osd getcrushmap > crushmap
crushtool --decompile crushmap > crushmap.txt
sed 's/^# end crush map$//' crushmap.txt > crushmap_modified.txt
cat >> crushmap_modified.txt << EOF
rule replicated_rule_custom {
        id 1
        type replicated
        step take default
        step choose firstn 3 type datacenter
        step chooseleaf firstn 2 type host
        step emit
}
# end crush map
EOF

# compile the modified crushmap and set it
crushtool --compile crushmap_modified.txt -o crushmap.bin
ceph osd setcrushmap -i crushmap.bin

# Set the election strategy to connectivity
ceph mon set election_strategy connectivity

# peer_crush_bucket_count > 3 datacenters throws Error EPERM
expect_false ceph osd pool stretch set $TEST_POOL_STRETCH 4 3 datacenter $TEST_CRUSH_RULE 6 3

# peer_crush_bucket_target > 3 datacenters throws Error EPERM
expect_false ceph osd pool stretch set $TEST_POOL_STRETCH 2 4 datacenter $TEST_CRUSH_RULE 6 3

# peer_crush_bucket_target > 3 datacenters success when add --yes-i-really-mean-it flag
expect_true ceph osd pool stretch set $TEST_POOL_STRETCH 2 4 datacenter $TEST_CRUSH_RULE 6 3 --yes-i-really-mean-it

# pool must be a stretch pool for this command to show anything.
expect_true ceph osd pool stretch set $TEST_POOL_STRETCH 2 3 datacenter $TEST_CRUSH_RULE 6 3
expect_true ceph osd pool stretch show $TEST_POOL_STRETCH

# Unset the stretch pool and expects it to work
expect_true ceph osd pool stretch unset $TEST_POOL_STRETCH replicated_rule 6 3
# try to show the stretch pool values again, should return error since
# the pool is not a stretch pool anymore.
expect_false ceph osd pool stretch show $TEST_POOL_STRETCH

# cleanup
teardown