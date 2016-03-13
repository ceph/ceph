// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.hpp"
#include "global/global_context.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"
#include <iostream>
#include <string>

extern void register_test_cluster_watcher();
extern void register_test_pool_watcher();
extern void register_test_rbd_mirror();
extern void register_test_image_sync();

int main(int argc, char **argv)
{
  register_test_cluster_watcher();
  register_test_pool_watcher();
  register_test_rbd_mirror();
  register_test_image_sync();

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
