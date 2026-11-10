// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <set>
#include <string>
#include <errno.h>
#include "TestClient.h"
#include "client/Dentry.h"
#include "client/Dir.h"
#include "client/Inode.h"
#include "include/scope_guard.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "gtest/gtest-spi.h"
#include "gmock/gmock-matchers.h"
#include "gmock/gmock-more-matchers.h"

TEST_F(TestClient, CheckDummyOP) {
  ASSERT_EQ(client->check_dummy_op(myperm), -EOPNOTSUPP);
}

TEST_F(TestClient, CheckUnknownSessionOp) {
  ASSERT_EQ(client->send_unknown_session_op(-1), 0);
  sleep(5);
  ASSERT_EQ(client->check_client_blocklisted(), true);
}

TEST_F(TestClient, CheckZeroReclaimFlag) {
  ASSERT_EQ(client->check_unknown_reclaim_flag(0), true);
}
TEST_F(TestClient, CheckUnknownReclaimFlag) {
  ASSERT_EQ(client->check_unknown_reclaim_flag(2), true);
}
TEST_F(TestClient, CheckNegativeReclaimFlagUnmasked) {
  ASSERT_EQ(client->check_unknown_reclaim_flag(-1 & ~MClientReclaim::FLAG_FINISH), true);
}
TEST_F(TestClient, CheckNegativeReclaimFlag) {
  ASSERT_EQ(client->check_unknown_reclaim_flag(-1), true);
}

static std::set<std::string> list_dir(ClientScaffold *client,
                                      dir_result_t *dirp)
{
  std::set<std::string> names;
  struct dirent de;
  int r;
  while ((r = client->readdir_r(dirp, &de)) > 0) {
    EXPECT_TRUE(names.insert(de.d_name).second)
      << "duplicate directory entry: " << de.d_name;
  }
  EXPECT_EQ(0, r) << "readdir_r must reach the end of the directory";
  names.erase(".");
  names.erase("..");
  return names;
}

static bool readdir_cache_has(Inode *diri, uintptr_t dn)
{
  for (Dentry *cached : diri->dir->readdir_cache) {
    if (reinterpret_cast<uintptr_t>(cached) == dn)
      return true;
  }
  return false;
}

// readdir_cache holds no references. _try_to_trim_inode() nulls the cached
// dentries of an inode (a rename target that has other links, say) and
// trim_cache() then frees the null dentry, so the cache must let it go.
TEST_F(TestClient, ReaddirCacheDropsFreedDentry) {
  const std::string path = "/readdir_cache_freed_" + std::to_string(getpid());
  std::set<std::string> names;
  ASSERT_EQ(0, client->mkdir(path.c_str(), 0755, myperm));
  for (int i = 0; i < 8; ++i) {
    names.insert("f" + std::to_string(i));
    ASSERT_EQ(0, client->mknod((path + "/f" + std::to_string(i)).c_str(),
                               S_IFREG | 0644, myperm));
  }
  auto cleanup = make_scope_guard([&] {
    for (auto& name : names)
      client->unlink((path + "/" + name).c_str(), myperm);
    client->rmdir(path.c_str(), myperm);
  });

  dir_result_t *dirp;
  ASSERT_EQ(0, client->opendir(path.c_str(), &dirp, myperm));
  InodeRef diri = dirp->inode;
  ASSERT_EQ(names, list_dir(client, dirp));
  client->closedir(dirp);

  {
    std::scoped_lock lock(client->client_lock);
    ASSERT_TRUE(diri->is_complete_and_ordered());
    ASSERT_EQ(names.size(), diri->dir->readdir_cache.size());
    Dentry *dn = diri->dir->dentries.at("f3");
    const auto freed = reinterpret_cast<uintptr_t>(dn);
    InodeRef in = dn->inode;
    client->unlink(dn, true, true);  // keep dir, keep dentry
    client->trim_dentry(dn);         // frees dn
    EXPECT_FALSE(readdir_cache_has(diri.get(), freed));
    EXPECT_FALSE(diri->flags & I_COMPLETE);
  }

  ASSERT_EQ(0, client->opendir(path.c_str(), &dirp, myperm));
  EXPECT_EQ(names, list_dir(client, dirp));
  client->closedir(dirp);
}

// The same while a listing is still filling readdir_cache of a directory
// that is not complete: nothing else drops that cache.
TEST_F(TestClient, ReaddirCacheDropsFreedDentryWhileFilling) {
  const std::string path = "/readdir_cache_filling_" + std::to_string(getpid());
  std::set<std::string> names;
  ASSERT_EQ(0, client->mkdir(path.c_str(), 0755, myperm));
  for (int i = 0; i < 8; ++i) {
    names.insert("f" + std::to_string(i));
    ASSERT_EQ(0, client->mknod((path + "/f" + std::to_string(i)).c_str(),
                               S_IFREG | 0644, myperm));
  }
  auto cleanup = make_scope_guard([&] {
    for (auto& name : names)
      client->unlink((path + "/" + name).c_str(), myperm);
    client->rmdir(path.c_str(), myperm);
  });

  dir_result_t *dirp;
  ASSERT_EQ(0, client->opendir(path.c_str(), &dirp, myperm));
  InodeRef diri = dirp->inode;
  {
    std::scoped_lock lock(client->client_lock);
    client->clear_dir_complete_and_ordered(diri.get(), true);
  }
  // '.', '..' and the first entry: the reply fills readdir_cache
  struct dirent de;
  for (int i = 0; i < 3; ++i)
    ASSERT_EQ(1, client->readdir_r(dirp, &de));
  {
    std::scoped_lock lock(client->client_lock);
    ASSERT_FALSE(diri->flags & I_COMPLETE);
    ASSERT_EQ(names.size(), diri->dir->readdir_cache.size());
    Dentry *dn = diri->dir->readdir_cache.back();
    const auto freed = reinterpret_cast<uintptr_t>(dn);
    InodeRef in = dn->inode;
    client->unlink(dn, true, true);
    client->trim_dentry(dn);
    EXPECT_FALSE(readdir_cache_has(diri.get(), freed));
  }
  std::set<std::string> rest = list_dir(client, dirp);
  rest.insert(de.d_name);
  EXPECT_EQ(names, rest);
  client->closedir(dirp);
  // the listing that was filling the cache cannot complete the directory
  std::scoped_lock lock(client->client_lock);
  EXPECT_FALSE(diri->flags & I_COMPLETE);
}
