#!/usr/bin/env bash

set -uex

# number of osds = 1
crushtool -o crushmap --build --num_osds 1 host straw 2 rack straw 2 row straw 2 root straw 0
ceph osd setcrushmap -i crushmap
ceph osd tree
ceph tell osd.* injectargs --osd_max_markdown_count 1024 --osd_max_markdown_period 1
ceph osd set noout

wait_for_healthy() {
  while ceph health detail | grep "DISPATCH_QUEUE_THROTTLE"
  do
    sleep 1
  done
}

test_dispatch_queue_throttle() {
    ceph config set global ms_dispatch_throttle_bytes 10
    ceph config set global ms_dispatch_throttle_log_interval 1
    ceph health detail
    ceph health | grep "Dispatch Queue Throttling"
    ceph health detail | grep "DISPATCH_QUEUE_THROTTLE"
    ceph config set global ms_dispatch_throttle_bytes 104857600 # default: 100_M
    ceph config set global ms_dispatch_throttle_log_interval 30
    wait_for_healthy
}

test_dispatch_queue_throttle

exit 0
