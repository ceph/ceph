#!/bin/bash -x

delay_mon() {
    MSGTYPE=$1
    shift
    $@ --rados-mon-op-timeout 1 --ms-inject-delay-type mon --ms-inject-delay-max 10000000 --ms-inject-delay-probability 1 --ms-inject-delay-msg-type $MSGTYPE
    if [ $? -eq 0 ]; then
        exit 1
    fi
}

delay_osd() {
    MSGTYPE=$1
    shift
    $@ --rados-osd-op-timeout 1 --ms-inject-delay-type osd --ms-inject-delay-max 10000000 --ms-inject-delay-probability 1 --ms-inject-delay-msg-type $MSGTYPE
    if [ $? -eq 0 ]; then
        exit 2
    fi
}

# pool ops
delay_mon omap rados lspools
delay_mon poolopreply rados mkpool test
delay_mon poolopreply rados mksnap -p test snap
delay_mon poolopreply rados rmpool test test --yes-i-really-really-mean-it

# other mon ops
delay_mon getpoolstats rados df
delay_mon mon_command ceph df
delay_mon omap ceph osd dump
delay_mon omap ceph -s

# osd ops
delay_osd osd_op_reply rados -p data put ls /bin/ls
delay_osd osd_op_reply rados -p data get ls - >/dev/null
delay_osd osd_op_reply rados -p data ls
delay_osd command_reply ceph tell osd.0 bench 1 1

# rbd commands, using more kinds of osd ops
rbd create -s 1 test
delay_osd osd_op_reply rbd watch test
delay_osd osd_op_reply rbd info test
delay_osd osd_op_reply rbd snap create test@snap
delay_osd osd_op_reply rbd import /bin/ls ls
rbd rm test

echo OK
