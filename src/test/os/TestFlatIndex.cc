// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#include <stdio.h>
#include <signal.h>
#include "os/FlatIndex.h"
#include "os/CollectionIndex.h"
#include "os/chain_xattr.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include <gtest/gtest.h>

TEST(FlatIndex, FlatIndex) {
  coll_t collection("ABC");
  const std::string base_path("PATH");
  FlatIndex index(collection, base_path);
  EXPECT_EQ(collection, index.coll());
  EXPECT_EQ((unsigned)0, index.collection_version());
  //
  // checking placeholders
  //
  EXPECT_EQ(0, index.init());
  EXPECT_EQ(0, index.cleanup());
}

#ifdef GTEST_HAS_DEATH_TEST
TEST(FlatIndex, collection) {
  coll_t collection("ABC");
  const std::string base_path("PATH");
  FlatIndex index(collection, base_path);
  const std::string key("KEY");
  uint64_t hash = 111;
  uint64_t pool = 222;
  const std::string object_name(10, 'A');
  ghobject_t hoid(hobject_t(object_t(object_name), key, CEPH_NOSNAP, hash, pool, ""));
  vector<ghobject_t> ls;
  ASSERT_DEATH(index.collection_list_partial(hoid, 0, 0, 0, &ls, &hoid), "0");
}
#endif //GTEST_HAS_DEATH_TEST

TEST(FlatIndex, created_unlink) {
  coll_t collection("ABC");
  const std::string base_path("PATH");
  EXPECT_EQ(0, ::system("rm -fr PATH"));
  EXPECT_EQ(0, ::mkdir("PATH", 0700));
  ceph::shared_ptr<CollectionIndex> index(new FlatIndex(collection, base_path));
  const std::string key("KEY");
  uint64_t hash = 111;
  uint64_t pool = 222;
  //
  // short object name
  //
  {
    CollectionIndex::IndexedPath indexed_path;
    index->set_ref(index);
    const std::string object_name(10, 'A');
    ghobject_t hoid(hobject_t(object_t(object_name), key, CEPH_NOSNAP, hash, pool, ""));
    int exists;
    EXPECT_EQ(0, index->lookup(hoid, &indexed_path, &exists));
    EXPECT_EQ(0, exists);
    EXPECT_EQ(0, ::close(::creat(indexed_path->path(), 0600)));
    EXPECT_EQ(0, index->lookup(hoid, &indexed_path, &exists));
    EXPECT_EQ(1, exists);
    EXPECT_EQ(0, index->unlink(hoid));
    EXPECT_EQ(0, index->lookup(hoid, &indexed_path, &exists));
    EXPECT_EQ(0, exists);
  }
  //
  // long object name
  //
  {
    CollectionIndex::IndexedPath indexed_path;
    index->set_ref(index);
    const std::string object_name(1024, 'A');
    ghobject_t hoid(hobject_t(object_t(object_name), key, CEPH_NOSNAP, hash, pool, ""));
    int exists;
    EXPECT_EQ(0, index->lookup(hoid, &indexed_path, &exists));
    EXPECT_EQ(0, exists);
    EXPECT_EQ(0, ::close(::creat(indexed_path->path(), 0600)));
    EXPECT_EQ(0, index->created(hoid, indexed_path->path()));
    EXPECT_EQ(0, index->unlink(hoid));
    EXPECT_EQ(0, index->lookup(hoid, &indexed_path, &exists));
    EXPECT_EQ(0, exists);
  }
  EXPECT_EQ(0, ::system("rm -fr PATH"));
}

TEST(FlatIndex, collection_list) {
  coll_t collection("ABC");
  const std::string base_path("PATH");
  EXPECT_EQ(0, ::system("rm -fr PATH"));
  EXPECT_EQ(0, ::mkdir("PATH", 0700));
  const std::string object_name("ABC");
  const std::string filename("PATH/" + object_name + "_head");
  EXPECT_EQ(0, ::close(::creat(filename.c_str(), 0600)));
  ceph::shared_ptr<CollectionIndex> index(new FlatIndex(collection, base_path));
  vector<ghobject_t> ls;
  index->collection_list(&ls);
  EXPECT_EQ((unsigned)1, ls.size());
  EXPECT_EQ(object_name, ls[0].hobj.oid.name);
  EXPECT_EQ(0, ::system("rm -fr PATH"));
}

int main(int argc, char **argv) {
  int fd = ::creat("detect", 0600);
  int ret = chain_fsetxattr(fd, "user.test", "A", 1);
  ::close(fd);
  ::unlink("detect");
  if (ret < 0) {
    cerr << "SKIP FlatIndex because unable to test for xattr" << std::endl;
  } else {
    vector<const char*> args;
    argv_to_vec(argc, (const char **)argv, args);

    global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
    common_init_finish(g_ceph_context);

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
  }
}

// Local Variables:
// compile-command: "cd ../.. ; make unittest_flatindex ; ./unittest_flatindex # --gtest_filter=FlatIndexTest.FlatIndex --log-to-stderr=true --debug-filestore=20"
// End:
