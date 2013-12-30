#!/bin/sh -ex

OBJ=test_rados_obj
POOL=rbd

cleanup() {
    rados -p $POOL rm $OBJ || true
}

test_omap() {
    cleanup
    for i in $(seq 1 1 600)
    do
        rados -p $POOL setomapval $OBJ $i $i
        rados -p $POOL getomapval $OBJ $i | grep -q "\\: $i\$"
    done
    rados -p $POOL listomapvals $OBJ | grep -c value | grep 600
    cleanup
}

test_omap

echo OK
exit 0
