#!/bin/sh -x

set -e

ceph osd pool create foo 123 123
ceph osd pool create fooo 123

ceph osd pool create foo 123 # idempotent

ceph osd pool delete foo && exit 1 || true    # should fail due to safety interlock
ceph osd pool delete foo foo && exit 1 || true    # should fail due to safety interlock
ceph osd pool delete foo foo --force && exit 1 || true    # should fail due to safety interlock

ceph osd pool delete foo foo --yes-i-really-really-mean-it
ceph osd pool delete foo foo --yes-i-really-really-mean-it
ceph osd pool delete fuggg fugg --yes-i-really-really-mean-it

ceph osd pool delete fooo

echo OK


