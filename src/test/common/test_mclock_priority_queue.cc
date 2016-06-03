// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include "gtest/gtest.h"

#include "common/mClockPriorityQueue.h"
#if 0
#include "global/global_context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/common_init.h"
#endif


struct Request {
    char data[17];
};

struct Client {
    int client_num;
}


TEST(mClockPriorityQueue, Create)
{
    ceph::mClockQueue<Request,Client> q;
}


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

#if 0 // REMOVE?
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
#endif

  return RUN_ALL_TESTS();
}
