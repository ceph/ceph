#!/bin/bash
#
# Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
code=0
echo "Run unit tests that need a cluster, using vstart.sh"
while read line ; do
  echo "================ START ================"
  echo "$line"
  if ! test/vstart_wrapper.sh $line ; then
      code=1
  fi
  echo "================ STOP ================="  
done <<EOF
../qa/workunits/cephtool/test.sh
./ceph_test_librbd
./ceph_test_cls_rbd
./ceph_test_cls_refcount
./ceph_test_cls_version
./ceph_test_cls_log
./ceph_test_cls_statelog
./ceph_test_cls_replica_log
./ceph_test_cls_lock
./ceph_test_cls_hello
./ceph_test_cls_rgw
./ceph_test_rados_api_cmd
./ceph_test_rados_api_io
./ceph_test_rados_api_c_write_operations
./ceph_test_rados_api_c_read_operations
./ceph_test_rados_api_list
./ceph_test_rados_api_pool
./ceph_test_rados_api_stat
./ceph_test_rados_api_watch_notify
./ceph_test_rados_api_snapshots
./ceph_test_rados_api_cls
./ceph_test_rados_api_misc
EOF
exit $code
