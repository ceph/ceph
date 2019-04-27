// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/perf_counters.h"
#include "include/rados/librados.hpp"
#include "global/global_context.h"
#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"
#include <iostream>
#include <string>

PerfCounters *g_perf_counters = nullptr;

extern void register_test_cluster_watcher();
extern void register_test_image_policy();
extern void register_test_image_sync();
extern void register_test_instance_watcher();
extern void register_test_instances();
extern void register_test_leader_watcher();
extern void register_test_pool_watcher();
extern void register_test_rbd_mirror();
extern void register_test_rbd_mirror_image_deleter();

int main(int argc, char **argv)
{
  register_test_cluster_watcher();
  register_test_image_policy();
  register_test_image_sync();
  register_test_instance_watcher();
  register_test_instances();
  register_test_leader_watcher();
  register_test_pool_watcher();
  register_test_rbd_mirror();
  register_test_rbd_mirror_image_deleter();

  ::testing::InitGoogleTest(&argc, argv);

  librados::Rados rados;
  std::string result = connect_cluster_pp(rados);
  if (result != "" ) {
    std::cerr << result << std::endl;
    return 1;
  }

  g_ceph_context = reinterpret_cast<CephContext*>(rados.cct());

  int r = rados.conf_set("lockdep", "true");
  if (r < 0) {
    std::cerr << "failed to enable lockdep" << std::endl;
    return -r;
  }
  return RUN_ALL_TESTS();
}
