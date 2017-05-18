#!/bin/bash -ex

set -u

# number of osds = 10
crushtool -o crushmap --build --num_osds 10 host straw 2 rack straw 2 row straw 2 root straw 0
ceph osd setcrushmap -i crushmap
ceph osd tree

test_mark_two_osds_same_host_down() {
  ceph osd down osd.0 osd.1
  ceph health detail
  ceph health | grep "host"
  ceph health detail | grep "osd.0"
  ceph health detail | grep "osd.1"
}

test_mark_two_osds_same_rack_down() {
  ceph osd down osd.8 osd.9
  ceph health detail
  ceph health | grep "rack"
  ceph health detail | grep "osd.8"
  ceph health detail | grep "osd.9"
}

test_mark_all_osds_down() {
  ceph osd down `ceph osd ls`
  ceph health detail
  ceph health | grep "row"
}

test_mark_two_osds_same_host_down
test_mark_two_osds_same_rack_down
test_mark_all_osds_down

exit 0
