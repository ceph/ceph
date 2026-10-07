#!/bin/bash -ex

NUM_OSDS=$(ceph osd ls | wc -l)
if [ $NUM_OSDS -lt 6 ]; then
    echo "test requires at least 6 OSDs"
    exit 1
fi

NUM_POOLS=$(ceph osd pool ls | wc -l)
if [ $NUM_POOLS -gt 0 ]; then
    echo "test requires no preexisting pools"
    exit 1
fi

function wait_for() {
    local sec=$1
    local cmd=$2

    while true ; do
        if bash -c "$cmd" ; then
            break
        fi
        sec=$(( $sec - 1 ))
        if [ $sec -eq 0 ]; then
            echo failed
            return 1
        fi
        sleep 1
    done
    return 0
}

function power2_floor() { echo "x=l($1)/l(2); scale=0; 2^(x/1)" | bc -l;}

function power2_ceil() { echo "x=l($1)/l(2); scale=0; 2^((x+0.999)/1)" | bc -l; }

function eval_actual_expected_val() {
    local actual_value=$1
    local expected_value=$2
    if [[ $actual_value = $expected_value ]]
    then
     echo "Success: " $actual_value "=" $expected_value
    else
      echo "Error: " $actual_value "!=" $expected_value
      exit 1
    fi
}

function test_autoscaler_basic() {
    # pg_num_min
    MON_TARGET_PG_PER_OSD=200
    ceph config set global mon_target_pg_per_osd $MON_TARGET_PG_PER_OSD

    ceph osd pool create meta0
    ceph osd pool create bulk0 --bulk
    ceph osd pool create bulk1 --bulk
    ceph osd pool create bulk2 --bulk
    ceph osd pool set meta0 pg_autoscale_mode on
    ceph osd pool set bulk0 pg_autoscale_mode on
    ceph osd pool set bulk1 pg_autoscale_mode on
    ceph osd pool set bulk2 pg_autoscale_mode on
    # set pool size
    ceph osd pool set meta0 size 2
    ceph osd pool set bulk0 size 2
    ceph osd pool set bulk1 size 2
    ceph osd pool set bulk2 size 2

    # get num pools again since we created more pools
    NUM_POOLS=$(ceph osd pool ls | wc -l)

    # get bulk flag of each pool through the command ceph osd pool autoscale-status
    BULK_FLAG_1=$(ceph osd pool autoscale-status | grep 'meta0' | grep -o -m 1 'True\|False' || true)
    BULK_FLAG_2=$(ceph osd pool autoscale-status | grep 'bulk0' | grep -o -m 1 'True\|False' || true)
    BULK_FLAG_3=$(ceph osd pool autoscale-status | grep 'bulk1' | grep -o -m 1 'True\|False' || true)
    BULK_FLAG_4=$(ceph osd pool autoscale-status | grep 'bulk2' | grep -o -m 1 'True\|False' || true)

    # evaluate the accuracy of ceph osd pool autoscale-status specifically the `BULK` column

    eval_actual_expected_val $BULK_FLAG_1 'False'
    eval_actual_expected_val $BULK_FLAG_2 'True'
    eval_actual_expected_val $BULK_FLAG_3 'True'
    eval_actual_expected_val $BULK_FLAG_4 'True'

    # This part of this code will now evaluate the accuracy of the autoscaler

    # get pool size
    POOL_SIZE_1=$(ceph osd pool get meta0 size| grep -Eo '[0-9]{1,4}')
    POOL_SIZE_2=$(ceph osd pool get bulk0 size| grep -Eo '[0-9]{1,4}')
    POOL_SIZE_3=$(ceph osd pool get bulk1 size| grep -Eo '[0-9]{1,4}')
    POOL_SIZE_4=$(ceph osd pool get bulk2 size| grep -Eo '[0-9]{1,4}')
    sleep 30

    # Calculate target pg of each pools


    TARGET_PG_1=$(ceph osd pool get meta0 pg_num| grep -Eo '[0-9]{1,4}')
    PG_LEFT=$(($NUM_OSDS*$MON_TARGET_PG_PER_OSD - TARGET_PG_1 * $POOL_SIZE_1))
    NUM_POOLS_LEFT=$NUM_POOLS-1
    # Rest of the pool is bulk and even pools so pretty straight forward
    # calculations.
    TARGET_PG_2=$(power2_floor $((($PG_LEFT)/($NUM_POOLS_LEFT)/($POOL_SIZE_2))))
    TARGET_PG_3=$(power2_floor $((($PG_LEFT)/($NUM_POOLS_LEFT)/($POOL_SIZE_3))))
    TARGET_PG_4=$(power2_floor $((($PG_LEFT)/($NUM_POOLS_LEFT)/($POOL_SIZE_4))))

    # evaluate target_pg against pg num of each pools
    wait_for 300 "ceph osd pool get meta0 pg_num | grep $TARGET_PG_1"
    wait_for 300 "ceph osd pool get bulk0 pg_num | grep $TARGET_PG_2"
    wait_for 300 "ceph osd pool get bulk1 pg_num | grep $TARGET_PG_3"
    wait_for 300 "ceph osd pool get bulk2 pg_num | grep $TARGET_PG_4"

    # target ratio
    ceph osd pool set meta0 target_size_ratio 5
    ceph osd pool set bulk0 target_size_ratio 2
    sleep 60
    APGS=$(ceph osd dump -f json-pretty | jq '.pools[0].pg_num_target')
    BPGS=$(ceph osd dump -f json-pretty | jq '.pools[1].pg_num_target')
    test $APGS -gt 200
    test $BPGS -gt 20

    # small ratio change does not change pg_num
    ceph osd pool set meta0 target_size_ratio 7
    ceph osd pool set bulk0 target_size_ratio 2
    sleep 60
    APGS2=$(ceph osd dump -f json-pretty | jq '.pools[0].pg_num_target')
    BPGS2=$(ceph osd dump -f json-pretty | jq '.pools[1].pg_num_target')
    test $APGS -eq $APGS2
    test $BPGS -eq $BPGS2

    # target_size
    ceph osd pool set meta0 target_size_bytes 1000000000000000
    ceph osd pool set bulk0 target_size_bytes 1000000000000000
    ceph osd pool set meta0 target_size_ratio 0
    ceph osd pool set bulk0 target_size_ratio 0
    wait_for 60 "ceph health detail | grep POOL_TARGET_SIZE_BYTES_OVERCOMMITTED"

    ceph osd pool set meta0 target_size_bytes 1000
    ceph osd pool set bulk0 target_size_bytes 1000
    ceph osd pool set meta0 target_size_ratio 1
    wait_for 60 "ceph health detail | grep POOL_HAS_TARGET_SIZE_BYTES_AND_RATIO"

    # test autoscale warn
    ceph osd pool create warn0 1 --autoscale-mode=warn
    wait_for 120 "ceph health detail | grep POOL_TOO_FEW_PGS"

    ceph osd pool create warn1 256 --autoscale-mode=warn
    wait_for 120 "ceph health detail | grep POOL_TOO_MANY_PGS"

    ceph osd pool rm meta0 meta0 --yes-i-really-really-mean-it
    ceph osd pool rm bulk0 bulk0 --yes-i-really-really-mean-it
    ceph osd pool rm bulk1 bulk1 --yes-i-really-really-mean-it
    ceph osd pool rm bulk2 bulk2 --yes-i-really-really-mean-it
    ceph osd pool rm warn0 warn0 --yes-i-really-really-mean-it
    ceph osd pool rm warn1 warn1 --yes-i-really-really-mean-it
}

function test_pool_starvation() {
    # test pool starvation where PGs not distributed evenly
    MON_TARGET_PG_PER_OSD=200
    ceph config set global mon_target_pg_per_osd $MON_TARGET_PG_PER_OSD

    ceph osd pool create data0
    ceph osd pool create data1
    ceph osd pool create data2
    ceph osd pool set data0 pg_autoscale_mode on
    ceph osd pool set data1 pg_autoscale_mode on
    ceph osd pool set data2 pg_autoscale_mode on
    ceph osd pool set data0 target_size_ratio 0.3333
    ceph osd pool set data1 target_size_ratio 0.3333
    ceph osd pool set data2  target_size_ratio 0.3333
    sleep 30

    PG_LEFT=$(($NUM_OSDS*$MON_TARGET_PG_PER_OSD))
    POOL_SIZE=$(ceph osd pool get data0 size| grep -Eo '[0-9]{1,4}')
    NUM_POOLS=$(ceph osd pool ls | wc -l)
    TARGET_PG=$(power2_floor $((($PG_LEFT)/($NUM_POOLS)/($POOL_SIZE))))
    wait_for 300 "ceph osd pool get data0 pg_num | grep $TARGET_PG"
    wait_for 300 "ceph osd pool get data1 pg_num | grep $TARGET_PG"
    wait_for 300 "ceph osd pool get data2 pg_num | grep $TARGET_PG"

    ceph osd pool rm data0 data0 --yes-i-really-really-mean-it
    ceph osd pool rm data1 data1 --yes-i-really-really-mean-it
    ceph osd pool rm data2 data2 --yes-i-really-really-mean-it
}

function test_exact_budget() {
    # test edge case for exact budget
    MON_TARGET_PG_PER_OSD=128
    ceph config set global mon_target_pg_per_osd $MON_TARGET_PG_PER_OSD

    ceph osd pool create data0
    ceph osd pool create data1
    ceph osd pool set data0 pg_autoscale_mode on
    ceph osd pool set data1 pg_autoscale_mode on
    ceph osd pool set data0 size 3
    ceph osd pool set data1 size 3
    ceph osd pool set data0 target_size_ratio 0.5
    ceph osd pool set data1 target_size_ratio 0.5
    sleep 60

    POOL_SIZE_1=$(ceph osd pool get data0 size| grep -Eo '[0-9]{1,4}')
    POOL_SIZE_2=$(ceph osd pool get data1 size| grep -Eo '[0-9]{1,4}')
    PG_LEFT=$(($NUM_OSDS * $MON_TARGET_PG_PER_OSD))
    NUM_POOLS=$(ceph osd pool ls | wc -l)
    TARGET_PG_1=$(power2_floor $((($PG_LEFT)/($NUM_POOLS)/($POOL_SIZE_1))))
    TARGET_PG_2=$(power2_floor $((($PG_LEFT)/($NUM_POOLS)/($POOL_SIZE_2))))

    wait_for 300 "ceph osd pool get data0 pg_num | grep $TARGET_PG_1"
    wait_for 300 "ceph osd pool get data1 pg_num | grep $TARGET_PG_2"

    ceph osd pool rm data0 data0 --yes-i-really-really-mean-it
    ceph osd pool rm data1 data1 --yes-i-really-really-mean-it
}

function test_overlapping_roots() {
    # Create custom CRUSH rules for zone-based placement
    MON_TARGET_PG_PER_OSD=100
    ceph config set global mon_target_pg_per_osd $MON_TARGET_PG_PER_OSD

    ceph osd crush add-bucket us-east region
    ceph osd crush move us-east root=default

    ceph osd crush add-bucket us-east-1 zone
    ceph osd crush add-bucket us-east-2 zone
    ceph osd crush add-bucket us-east-3 zone
    ceph osd crush move us-east-1 region=us-east
    ceph osd crush move us-east-2 region=us-east
    ceph osd crush move us-east-3 region=us-east

    ceph osd crush add-bucket host0 host
    ceph osd crush add-bucket host1 host
    ceph osd crush add-bucket host2 host
    ceph osd crush add-bucket host3 host
    ceph osd crush add-bucket host4 host
    ceph osd crush add-bucket host5 host

    ceph osd crush move host0 zone=us-east-1
    ceph osd crush move host1 zone=us-east-1
    ceph osd crush move host2 zone=us-east-2
    ceph osd crush move host3 zone=us-east-2
    ceph osd crush move host4 zone=us-east-3
    ceph osd crush move host5 zone=us-east-3

    ceph osd crush set osd.0 1.0 host=host0
    ceph osd crush set osd.1 1.0 host=host1
    ceph osd crush set osd.2 1.0 host=host2
    ceph osd crush set osd.3 1.0 host=host3
    ceph osd crush set osd.4 1.0 host=host4
    ceph osd crush set osd.5 1.0 host=host5

    ceph osd crush move us-east root=default

    # Add zone-based CRUSH rules
    hostname=$(hostname -s)
    ceph osd crush remove $hostname

    ceph osd getcrushmap > crushmap
    crushtool --decompile crushmap > crushmap.txt
    sed 's/^# end crush map$//' crushmap.txt > crushmap_modified.txt
    cat >> crushmap_modified.txt << EOF
rule zone-1-only {
        id 1
        type replicated
        step take us-east-1
        step chooseleaf firstn 3 type host
        step emit
}

rule zone-2-only {
        id 2
        type replicated
        step take us-east-2
        step chooseleaf firstn 3 type host
        step emit
}

rule zone-3-only {
        id 3
        type replicated
        step take us-east-3
        step chooseleaf firstn 3 type host
        step emit
}

rule default {
        id 4
        type replicated
        step take default
        step chooseleaf firstn 3 type host
        step emit
}
# end crush map
EOF

    # compile the modified crushmap and set it
    crushtool --compile crushmap_modified.txt -o crushmap.bin
    ceph osd setcrushmap -i crushmap.bin


    ceph osd pool create data0 --size=1
    ceph osd pool create data1 --size=1
    ceph osd pool create data2 --size=1
    ceph osd pool create data3 --size=3

    ceph osd pool set data0 pg_autoscale_mode on
    ceph osd pool set data1 pg_autoscale_mode on
    ceph osd pool set data2 pg_autoscale_mode on
    ceph osd pool set data3 pg_autoscale_mode on
    

    ceph osd pool set data0 crush_rule zone-1-only
    ceph osd pool set data1 crush_rule zone-2-only
    ceph osd pool set data2 crush_rule zone-3-only
    ceph osd pool set data3 crush_rule default

    ceph osd pool set data0 target_size_ratio 1.0
    ceph osd pool set data1 target_size_ratio 1.0
    ceph osd pool set data2 target_size_ratio 1.0
    ceph osd pool set data3 target_size_ratio 1.0

    # expect
    #  -1         6.00000  root default
    #  -8         6.00000      region us-east
    #  -5         2.00000          zone us-east-1
    # -13         1.00000              host host0
    #   0    ssd  1.00000                  osd.0
    # -14         1.00000              host host1
    #   1    ssd  1.00000                  osd.1
    #  -6         2.00000          zone us-east-2
    # -15         1.00000              host host2
    #   2    ssd  1.00000                  osd.2
    # -16         1.00000              host host3
    #   3    ssd  1.00000                  osd.3
    #  -7         2.00000          zone us-east-3
    # -17         1.00000              host host4
    #   4    ssd  1.00000                  osd.4
    # -18         1.00000              host host5
    #   5    ssd  1.00000                  osd.5

    # -1: pg_target = 300
    # -5: pg_target = 100
    # -6  pg_target = 100
    # -7: pg_target = 100

    TARGET_PG_1=$(power2_floor $MON_TARGET_PG_PER_OSD)
    TARGET_PG_2=$(power2_floor $(( ($NUM_OSDS - 3) * $MON_TARGET_PG_PER_OSD )))


    wait_for 300 "ceph osd pool get data0 pg_num | grep $TARGET_PG_1"
    wait_for 300 "ceph osd pool get data1 pg_num | grep $TARGET_PG_1"
    wait_for 300 "ceph osd pool get data2 pg_num | grep $TARGET_PG_1"
    wait_for 300 "ceph osd pool get data3 pg_num | grep $TARGET_PG_2"

    ceph osd pool rm data0 data0 --yes-i-really-really-mean-it
    ceph osd pool rm data1 data1 --yes-i-really-really-mean-it
    ceph osd pool rm data2 data2 --yes-i-really-really-mean-it
    ceph osd pool rm data3 data3 --yes-i-really-really-mean-it
}

# enable
ceph config set mgr mgr/pg_autoscaler/sleep_interval 60
ceph mgr module enable pg_autoscaler
ceph osd pool set threshold 1.0


test_autoscaler_basic || return 1
test_pool_starvation || return 1
test_exact_budget || return 1
test_overlapping_roots || exit 1

echo OK

