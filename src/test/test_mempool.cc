// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2016 Western Digital Corporation
 *
 * Author: Allen Samuels <allen.samuels@sandisk.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <stdio.h>

#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"
#include "include/mempool.h"


TEST(test_mempool, mempool_context) {
   mempool::map<mempool::unittest_1,int,int,3> m1;
   m1[1] = 2;
   EXPECT_EQ(m1.size(),size_t(1));

   multimap<size_t,mempool::StatsBySlots_t> Slots;
   mempool::pool_t::StatsBySlots("",Slots);
   for (auto& p : Slots) {
      std::cout << "Slots:" << p.first << " : Slabs:" << p.second.slabs << " Bytes:" << p.second.bytes << " Type:" << p.second.typeID << "\n";
   }
   EXPECT_EQ(Slots.size(),1u);
   EXPECT_NE(Slots.find(1),Slots.end());
   EXPECT_EQ(Slots.find(1)->second.slabs,0u);
   EXPECT_EQ(Slots.find(1)->second.bytes,0u);

   m1[2] = 2;
   m1[3] = 3;
   m1[4] = 4;

   Slots.clear();
   mempool::pool_t::StatsBySlots("",Slots);
   for (auto& p : Slots) {
      std::cout << "Slots:" << p.first << " : Slabs:" << p.second.slabs << " Bytes:" << p.second.bytes << " Type:" << p.second.typeID << "\n";
   }
   EXPECT_EQ(Slots.size(),1u);
   EXPECT_NE(Slots.find(4),Slots.end());
   EXPECT_EQ(Slots.find(4)->second.slabs,1u);
   EXPECT_NE(Slots.find(4)->second.bytes,0u);
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


/*
 * Local Variables:
 * compile-command: "cd ../../build ; make -j4 &&
 *   make unittest_mempool &&
 *   valgrind --tool=memcheck ./unittest_mempool --gtest_filter=*.*"
 * End:
 */
