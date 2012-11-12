#!/bin/sh -x

set -e

ceph osd pool create foo 123 123
ceph osd pool create fooo 123

ceph osd pool create foo 123 # idempotent

ceph osd pool delete foo
ceph osd pool delete foo
ceph osd pool delete fuggg

ceph osd pool delete fooo

echo OK


