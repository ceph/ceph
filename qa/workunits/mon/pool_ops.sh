#!/bin/sh -x

set -e

ceph osd pool create foo 123 123
ceph osd pool create fooo 123

ceph osd pool create foo 123 # idempotent

# should fail due to safety interlock
! ceph osd pool delete foo
! ceph osd pool delete foo foo
! ceph osd pool delete foo foo --force
! ceph osd pool delete foo fooo --yes-i-really-mean-it
! ceph osd pool delete foo --yes-i-really-mean-it foo
! ceph osd pool delete --yes-i-really-mean-it foo foo


ceph osd pool delete fooo fooo --yes-i-really-really-mean-it
ceph osd pool delete foo foo --yes-i-really-really-mean-it

# idempotent
ceph osd pool delete foo foo --yes-i-really-really-mean-it
ceph osd pool delete fooo fooo --yes-i-really-really-mean-it
ceph osd pool delete fooo fooo --yes-i-really-really-mean-it

# non-existent pool
! ceph osd pool delete fuggg fuggg --yes-i-really-really-mean-it

echo OK


