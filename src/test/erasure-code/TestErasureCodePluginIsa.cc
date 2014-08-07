/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CERN (Switzerland)
 *
 * Author: Andreas-Joachim Peters <Andreas.Joachim.Peters@cern.ch>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <errno.h>
#include "arch/probe.h"
#include "arch/intel.h"
#include "global/global_init.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "erasure-code/isa/ErasureCodeIsaTableCache.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

TEST(ErasureCodePlugin, factory)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  map<std::string,std::string> parameters;
  parameters["directory"] = ".libs";
  {
    ErasureCodeInterfaceRef erasure_code;
    EXPECT_FALSE(erasure_code);
    EXPECT_EQ(-EIO, instance.factory("no-isa", parameters,
                                        &erasure_code, cerr));
    EXPECT_FALSE(erasure_code);
  }
  const char *techniques[] = {
    "reed_sol_van",
    "cauchy",
    0
  };
  for(const char **technique = techniques; *technique; technique++) {
    ErasureCodeInterfaceRef erasure_code;
    parameters["technique"] = *technique;
    EXPECT_FALSE(erasure_code);
    EXPECT_EQ(0, instance.factory("isa", parameters,
                                  &erasure_code, cerr));
    EXPECT_TRUE(erasure_code);
  }
}

TEST(ErasureCodePlugin, cache)
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  map<std::string,std::string> parameters;
  parameters["directory"] = ".libs";

  const char *techniques[] = {
    "reed_sol_van",
    "cauchy",
    0
  };
  for(const char **technique = techniques; *technique; technique++) {
    ErasureCodeInterfaceRef erasure_code;
    parameters["technique"] = *technique;
    EXPECT_FALSE(erasure_code);
    EXPECT_EQ(0, instance.factory("isa", parameters,
                                  &erasure_code, cerr));
    EXPECT_TRUE(erasure_code);
  }

  ErasureCodeIsaTableCache table_cache;
  
  // there should be no cache yet - size -1
  for (int i=0; i<=1; i++) {
    EXPECT_EQ(-1,table_cache.getDecodingCacheSize(i));
  }

  // don't share the decoding table cache map between matrix types
  ErasureCodeIsaTableCache::lru_map_t* a[2];
  ErasureCodeIsaTableCache::lru_list_t* b[2];
  ErasureCodeIsaTableCache::lru_map_t* c[2];
  ErasureCodeIsaTableCache::lru_list_t* d[2];
  for (int i=0; i<=1; i++) {
    a[i] = table_cache.getDecodingTables(i);
    b[i] = table_cache.getDecodingTablesLru(i);
    c[i] = table_cache.getDecodingTables(i);
    d[i] = table_cache.getDecodingTablesLru(i);
  }

  EXPECT_TRUE(a[0] != a[1]);
  EXPECT_TRUE(b[0] != b[1]);
  EXPECT_TRUE(a[0] == c[0]);
  EXPECT_TRUE(b[0] == d[0]);

  // there should be a decoding cache with size 0
  for (int i=0; i<=1; i++) {
    EXPECT_EQ(0,table_cache.getDecodingCacheSize(i));
  }

  {
    // share encoding tables for same (k,m), but not for different matrices
    unsigned char** t1 = table_cache.getEncodingTable(0,10,3);
    unsigned char** t2 = table_cache.getEncodingTable(1,10,3);
    unsigned char** t3 = table_cache.getEncodingTable(0,10,4);
    unsigned char** t4 = table_cache.getEncodingTable(1,11,3);
    unsigned char** t5 = table_cache.getEncodingTable(0,10,3);
    unsigned char** t6 = table_cache.getEncodingTable(1,10,3);
    
    EXPECT_TRUE(t1==t5);
    EXPECT_TRUE(t2==t6);
    EXPECT_FALSE(t1==t2);
    EXPECT_FALSE(t3==t4);
    EXPECT_FALSE(t1==t3);
    EXPECT_FALSE(t2==t4);

    // attach some memory to one encoding table to see if it get's freed on exit
    *t1 = (unsigned char*) malloc(1024*1024);
  }

  {
    // share encoding coefficients for same (k,m), but not for different matrices
    unsigned char** t1 = table_cache.getEncodingCoefficient(0,10,3);
    unsigned char** t2 = table_cache.getEncodingCoefficient(1,10,3);
    unsigned char** t3 = table_cache.getEncodingCoefficient(0,10,4);
    unsigned char** t4 = table_cache.getEncodingCoefficient(1,11,3);
    unsigned char** t5 = table_cache.getEncodingCoefficient(0,10,3);
    unsigned char** t6 = table_cache.getEncodingCoefficient(1,10,3);
    
    EXPECT_TRUE(t1==t5);
    EXPECT_TRUE(t2==t6);
    EXPECT_FALSE(t1==t2);
    EXPECT_FALSE(t3==t4);
    EXPECT_FALSE(t1==t3);
    EXPECT_FALSE(t2==t4);

    // attach some memory to one encoding coefficient table to see if it get's freed on exit
    *t1 = (unsigned char*) malloc(1024*1024);
  }

  EXPECT_TRUE(table_cache.getLock()!=0);
  Mutex::Locker lock(*table_cache.getLock());
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
 * compile-command: "cd ../.. ; make -j4 &&
 *   make unittest_erasure_code_plugin_isa &&
 *   valgrind --tool=memcheck ./unittest_erasure_code_plugin_isa \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
