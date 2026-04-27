// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/ceph_crypto.h"
#include "common/config_proxy.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include <vector>

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_OSD,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_MON_CONFIG);
  g_conf().set_val("lockdep", "true");
  common_init_finish(g_ceph_context);

  int r = RUN_ALL_TESTS();
  return r;
}
