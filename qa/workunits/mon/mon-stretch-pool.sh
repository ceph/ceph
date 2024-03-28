#!/bin/bash -ex

NUM_OSDS_UP=$(ceph osd df | grep "up" | wc -l)

if [ $NUM_OSDS_UP -lt 9 ]; then
    echo "test requires at least 9 OSDs up and running"
    exit 1
fi

function expect_false()
{
	if "$@"; then return 1; else return 0; fi
}

function expect_true()
{
    if "$@"; then return 0; else return 1; fi
}

for dc in dc1 dc2 dc3
    do
      ceph osd crush add-bucket $dc datacenter
      ceph osd crush move $dc root=default
    done

ceph osd crush move node.a datacenter=dc1
ceph osd crush move node.b datacenter=dc2
ceph osd crush move node.c datacenter=dc3

ceph osd tree
ceph mon dump

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

crushtool --compile crushmap_modified.txt -o crushmap.bin
ceph osd setcrushmap -i crushmap.bin

expect_true ceph osd pool stretch set $TEST_POOL_STRETCH 2 3 datacenter $TEST_CRUSH_RULE 6 3
expect_true ceph osd pool stretch show $TEST_POOL_STRETCH

