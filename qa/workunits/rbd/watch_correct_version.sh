#!/bin/bash -ex

resize_image() {
    for i in $(seq 1 100)
    do
        rbd resize --size $i test
    done
}

rm -f test.exported || true
rbd rm test || true
rbd create -s 1 --order 25 test
resize_image &

for i in $(seq 1 100)
do
    rbd export test test.exported --debug-rbd 20 2>export.log
    rm -f test.exported
    MATCHED=`cat export.log | tr -d '\n' | (grep -c 'watching header object returned -34.*watching header object returned 0' || true)`
    rm -f export.log
    if [ "$MATCHED" == "1" ]
    then
        echo OK
        exit 0
    fi
done
rbd rm test

echo "No race detected"
exit 1
