#!/bin/bash -ex

OBJ=test_rados_obj
POOL=rbd

expect_false()
{
	set -x
	if "$@"; then return 1; else return 0; fi
}

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

test_xattr() {
    cleanup
    rados -p $POOL put $OBJ /etc/pass
    V1=`mktemp fooattrXXXXXXX`
    V2=`mktemp fooattrXXXXXXX`
    echo -n fooval > $V1
    expect_false rados -p $POOL setxattr $OBJ 2>/dev/null
    expect_false rados -p $POOL setxattr $OBJ foo fooval extraarg 2>/dev/null
    rados -p $POOL setxattr $OBJ foo fooval
    rados -p $POOL getxattr $OBJ foo > $V2
    cmp $V1 $V2
    cat $V1 | rados -p $POOL setxattr $OBJ bar
    rados -p $POOL getxattr $OBJ bar > $V2
    cmp $V1 $V2
    rados -p $POOL listxattr $OBJ > $V1
    grep -q foo $V1
    grep -q bar $V1
    wc -l $V1 | grep -q "^2 "
    rm $V1 $V2
    cleanup
}

test_xattr
test_omap

echo OK
exit 0
