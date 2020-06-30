#!/usr/bin/env bash

set -uex

# number of osds = 10
crushtool -o crushmap --build --num_osds 10 host straw 2 rack straw 2 row straw 2 root straw 0
ceph osd setcrushmap -i crushmap
ceph osd tree
ceph tell osd.* injectargs --osd_max_markdown_count 1024 --osd_max_markdown_period 1
ceph osd set noout

wait_for_healthy() {
  while ceph health | grep down
  do
    sleep 1
  done
}

test_mark_two_osds_same_host_down() {
  ceph osd set noup
  ceph osd down osd.0 osd.1
  ceph health detail
  ceph health | grep "1 host"
  ceph health | grep "2 osds"
  ceph health detail | grep "osd.0"
  ceph health detail | grep "osd.1"
  ceph osd unset noup
  wait_for_healthy
}

test_mark_two_osds_same_rack_down() {
  ceph osd set noup
  ceph osd down osd.8 osd.9
  ceph health detail
  ceph health | grep "1 host"
  ceph health | grep "1 rack"
  ceph health | grep "1 row"
  ceph health | grep "2 osds"
  ceph health detail | grep "osd.8"
  ceph health detail | grep "osd.9"
  ceph osd unset noup
  wait_for_healthy
}

test_mark_all_but_last_osds_down() {
  ceph osd set noup
  ceph osd down $(ceph osd ls | sed \$d)
  ceph health detail
  ceph health | grep "1 row"
  ceph health | grep "2 racks"
  ceph health | grep "4 hosts"
  ceph health | grep "9 osds"
  ceph osd unset noup
  wait_for_healthy
}

test_mark_two_osds_same_host_down_with_classes() {
    ceph osd set noup
    ceph osd crush set-device-class ssd osd.0 osd.2 osd.4 osd.6 osd.8
    ceph osd crush set-device-class hdd osd.1 osd.3 osd.5 osd.7 osd.9
    ceph osd down osd.0 osd.1
    ceph health detail
    ceph health | grep "1 host"
    ceph health | grep "2 osds"
    ceph health detail | grep "osd.0"
    ceph health detail | grep "osd.1"
    ceph osd unset noup
    wait_for_healthy
}

test_mark_two_osds_same_host_down
test_mark_two_osds_same_rack_down
test_mark_all_but_last_osds_down
test_mark_two_osds_same_host_down_with_classes

exit 0
