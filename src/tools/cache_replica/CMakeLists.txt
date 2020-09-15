#
# SPDX-License-Identifier: LGPL-3.0-or-later
# Copyright 2020, Intel Corporation
#
# Author: Changcheng Liu <changcheng.liu@aliyun.com>
#

set(cache_replica_srcs
    ReplicaContext.cc
    ReplicaDaemon.cc)
add_library(ceph_cache_replica_lib STATIC ${cache_replica_srcs})

add_executable(ceph-replica ceph_replica.cc)

target_link_libraries(ceph-replica
   ceph_cache_replica_lib
   ceph-common
   global
   Boost::thread)
install(TARGETS ceph-replica DESTINATION bin)
