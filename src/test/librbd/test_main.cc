// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/ceph_crypto.h"
#include "common/config.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include <vector>

extern void register_test_librbd();
#ifdef TEST_LIBRBD_INTERNALS
extern void register_test_image_watcher();
extern void register_test_internal();
extern void register_test_object_map();
#endif // TEST_LIBRBD_INTERNALS

int main(int argc, char **argv)
{
  register_test_librbd();
#ifdef TEST_LIBRBD_INTERNALS
  register_test_image_watcher();
  register_test_internal();
  register_test_object_map();
#endif // TEST_LIBRBD_INTERNALS

  ::testing::InitGoogleTest(&argc, argv);

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  g_conf->set_val("lockdep", "true");
  common_init_finish(g_ceph_context);

  int r = RUN_ALL_TESTS();
  g_ceph_context->put();
  ceph::crypto::shutdown();
  return r;
}
