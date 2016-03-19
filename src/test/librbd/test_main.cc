// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.hpp"
#include "global/global_context.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"
#include <iostream>
#include <string>

extern void register_test_librbd();
#ifdef TEST_LIBRBD_INTERNALS
extern void register_test_image_watcher();
extern void register_test_internal();
extern void register_test_journal_entries();
extern void register_test_journal_replay();
extern void register_test_object_map();
extern void register_test_mirroring();
#endif // TEST_LIBRBD_INTERNALS

int main(int argc, char **argv)
{
  register_test_librbd();
#ifdef TEST_LIBRBD_INTERNALS
  register_test_image_watcher();
  register_test_internal();
  register_test_journal_entries();
  register_test_journal_replay();
  register_test_object_map();
  register_test_mirroring();
#endif // TEST_LIBRBD_INTERNALS

  ::testing::InitGoogleTest(&argc, argv);

  librados::Rados rados;
  std::string result = connect_cluster_pp(rados);
  if (result != "" ) {
    std::cerr << result << std::endl;
    return 1;
  }

#ifdef TEST_LIBRBD_INTERNALS
  g_ceph_context = reinterpret_cast<CephContext*>(rados.cct());
#endif // TEST_LIBRBD_INTERNALS

  int r = rados.conf_set("lockdep", "true");
  if (r < 0) {
    std::cerr << "failed to enable lockdep" << std::endl;
    return -r;
  }
  return RUN_ALL_TESTS();
}
