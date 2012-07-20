#!/bin/sh -x

set -e

ceph osd pool create foo 123 123
ceph osd pool create fooo 123
ceph osd pool create foooo

ceph osd pool create foo  # idempotent

ceph osd pool delete foo
ceph osd pool delete foo
ceph osd pool delete fuggg

ceph osd pool delete fooo
ceph osd pool delete foooo

echo OK


