// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_CEPH_TEST_H
#define CEPH_TEST_CEPH_TEST_H

#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "include/stringify.h"
#include "common/errno.h"
#include <gtest/gtest.h>

// generic main() to allow environment and command line arguments to
// be parsed, run tests, and then cleanly shut down g_ceph_context.

#define WRITE_CEPH_UNITTEST_MAIN(x)					\
  int main(int argc, char **argv) {					\
    vector<const char*> args;						\
    argv_to_vec(argc, (const char **)argv, args);			\
    env_to_vec(args);							\
    global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,			\
		CODE_ENVIRONMENT_UTILITY, 0);				\
    common_init_finish(g_ceph_context);					\
    x									\
    ::testing::InitGoogleTest(&argc, argv);				\
    int r = RUN_ALL_TESTS();						\
    g_ceph_context->put();						\
    return r;								\
  }

#endif
