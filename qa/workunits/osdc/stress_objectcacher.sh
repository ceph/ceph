#!/bin/sh -ex

for i in $(seq 1 10)
do
    for DELAY in 0 1000
    do
        for OPS in 1000 10000
        do
            for OBJECTS in 10 50 100
            do
                for READS in 0.90 0.50 0.10
                do
                    for OP_SIZE in 4096 131072 1048576
                    do
                        for MAX_DIRTY in 0 25165824
                        do
                            ceph_test_objectcacher_stress --ops $OPS --percent-read $READS --delay-ns $DELAY --objects $OBJECTS --max-op-size $OP_SIZE --client-oc-max-dirty $MAX_DIRTY --stress-test > /dev/null 2>&1
                        done
                    done
                done
            done
        done
    done
done

ceph_test_objectcacher_stress --correctness-test > /dev/null 2>&1

echo OK
