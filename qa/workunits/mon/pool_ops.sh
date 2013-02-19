#!/bin/sh -x

set -e

ceph osd pool create foo 123 123
ceph osd pool create fooo 123

ceph osd pool create foo 123 # idempotent

ceph osd pool set foo size 1
ceph osd pool set foo size 4
ceph osd pool set foo size 10
ceph osd pool set foo size 0                           && exit 1 || true
ceph osd pool set foo size 20                          && exit 1 || true

# should fail due to safety interlock
ceph osd pool delete foo                               && exit 1 || true
ceph osd pool delete foo foo                           && exit 1 || true
ceph osd pool delete foo foo --force                   && exit 1 || true
ceph osd pool delete foo fooo --yes-i-really-mean-it   && exit 1 || true
ceph osd pool delete foo --yes-i-really-mean-it foo    && exit 1 || true

ceph osd pool delete fooo fooo --yes-i-really-really-mean-it
ceph osd pool delete foo foo --yes-i-really-really-mean-it

# idempotent
ceph osd pool delete foo foo --yes-i-really-really-mean-it
ceph osd pool delete fooo fooo --yes-i-really-really-mean-it
ceph osd pool delete fooo fooo --yes-i-really-really-mean-it

# non-existent pool
ceph osd pool delete fuggg fuggg --yes-i-really-really-mean-it     && exit 1 || true

echo OK


