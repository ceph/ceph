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
#include "client/Dir.h"
#include "client/Inode.h"
#include "client/MetaSession.h"
#include "include/scope_guard.h"
#include "messages/MClientQuota.h"
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

// names gets each subdirectory that was made, for remove_subdirs()
static void make_subdirs(ClientScaffold *client, const std::string& path,
                         int n, const UserPerm& perms,
                         std::set<std::string> *names)
{
  ASSERT_EQ(0, client->mkdir(path.c_str(), 0755, perms));
  for (int i = 0; i < n; ++i) {
    std::string name = "d" + std::to_string(i);
    ASSERT_EQ(0, client->mkdir((path + "/" + name).c_str(), 0755, perms));
    names->insert(name);
  }
}

static void remove_subdirs(ClientScaffold *client, const std::string& path,
                           const std::set<std::string>& names,
                           const UserPerm& perms)
{
  for (auto& name : names)
    client->rmdir((path + "/" + name).c_str(), perms);
  client->rmdir(path.c_str(), perms);
}

// The cached subdirectories of a listed directory. Dir::dentries cannot be
// looked up by name here: under fscrypt its keys are the encrypted names.
// Call with client_lock held.
static std::vector<InodeRef> cached_subdirs(const InodeRef& diri)
{
  std::vector<InodeRef> subdirs;
  for (auto& [name, dn] : diri->dir->dentries) {
    if (dn->inode && dn->inode->is_dir())
      subdirs.push_back(dn->inode);
  }
  return subdirs;
}

// With client_dirsize_rbytes, a listing takes the rstat of subdirectories
// from its READDIR replies instead of sending a GETATTR for each of them,
// also when the directory is complete and cached.
TEST_F(TestClient, ReaddirRefreshesRstatInReplies) {
  if (!g_ceph_context->_conf->client_dirsize_rbytes) {
    GTEST_SKIP() << "requires client_dirsize_rbytes=true";
  }
  const std::string path = "/readdir_rstat_" + std::to_string(getpid());
  std::set<std::string> names;
  auto cleanup = make_scope_guard([&] {
    remove_subdirs(client, path, names, myperm);
  });
  ASSERT_NO_FATAL_FAILURE(make_subdirs(client, path, 16, myperm, &names));
  for (int pass = 0; pass < 2; ++pass) {
    SCOPED_TRACE(pass);
    dir_result_t *dirp;
    ASSERT_EQ(0, client->opendir(path.c_str(), &dirp, myperm));
    auto close = make_scope_guard([&] { client->closedir(dirp); });
    ceph_tid_t before;
    {
      std::scoped_lock lock(client->client_lock);
      if (pass > 0) {  // start with a complete readdir_cache
        ASSERT_TRUE(dirp->inode->is_complete_and_ordered());
      }
      before = client->last_tid;
    }
    EXPECT_EQ(names, list_dir(client, dirp));
    std::scoped_lock lock(client->client_lock);
    // one READDIR, and a GETATTR each for '.' and '..'
    EXPECT_EQ(3u, client->last_tid - before);
    EXPECT_TRUE(dirp->inode->is_complete_and_ordered());
    for (auto& [name, dn] : dirp->inode->dir->dentries)
      EXPECT_GE(dn->inode->rstat_seq, dirp->listing_seq) << name;
  }
}

// Where fewer than one in 32 entries is a subdirectory, a page of entries
// is not worth it: the cached listing sends a GETATTR for each instead.
TEST_F(TestClient, ReaddirRefreshesSparseSubdirAlone) {
  if (!g_ceph_context->_conf->client_dirsize_rbytes) {
    GTEST_SKIP() << "requires client_dirsize_rbytes=true";
  }
  const std::string path = "/readdir_sparse_" + std::to_string(getpid());
  std::set<std::string> names;
  std::set<std::string> files;
  auto cleanup = make_scope_guard([&] {
    for (auto& name : files)
      client->unlink((path + "/" + name).c_str(), myperm);
    remove_subdirs(client, path, names, myperm);
  });
  ASSERT_NO_FATAL_FAILURE(make_subdirs(client, path, 1, myperm, &names));
  for (int i = 0; i < 63; ++i) {
    files.insert("f" + std::to_string(i));
    ASSERT_EQ(0, client->mknod((path + "/f" + std::to_string(i)).c_str(),
                               S_IFREG | 0644, myperm));
  }
  std::set<std::string> all = names;
  all.insert(files.begin(), files.end());
  dir_result_t *dirp;
  ASSERT_EQ(0, client->opendir(path.c_str(), &dirp, myperm));
  {
    auto close = make_scope_guard([&] { client->closedir(dirp); });
    ASSERT_EQ(all, list_dir(client, dirp));
  }

  ASSERT_EQ(0, client->opendir(path.c_str(), &dirp, myperm));
  auto close = make_scope_guard([&] { client->closedir(dirp); });
  ceph_tid_t before;
  {
    std::scoped_lock lock(client->client_lock);
    ASSERT_TRUE(dirp->inode->is_complete_and_ordered());
    before = client->last_tid;
  }
  EXPECT_EQ(all, list_dir(client, dirp));
  std::scoped_lock lock(client->client_lock);
  // a GETATTR each for '.', '..' and d0, and no READDIR into the buffer
  EXPECT_EQ(3u, client->last_tid - before);
  EXPECT_TRUE(dirp->buffer.empty());
  EXPECT_TRUE(dirp->inode->is_complete_and_ordered());
  auto subdirs = cached_subdirs(dirp->inode);
  ASSERT_EQ(names.size(), subdirs.size());
  EXPECT_GE(subdirs[0]->rstat_seq, dirp->listing_seq);
}

// Check batch and individual refresh above, at and below the 1/32 boundary.
TEST_F(TestClient, ReaddirRefreshesMixedSubdirsAtBatchThreshold) {
  if (!g_ceph_context->_conf->client_dirsize_rbytes) {
    GTEST_SKIP() << "requires client_dirsize_rbytes=true";
  }
  struct Scenario {
    int nfiles;
    ceph_tid_t requests;
    bool buffer_empty;
  };
  const Scenario scenarios[] = {
    {30, 3, false},  // 2/32: two GETATTRs for '.' and '..', plus one READDIR
    {62, 3, false},  // 2/64: exactly 1/32, still one READDIR
    {63, 4, true},   // 2/65: GETATTR for '.', '..' and both subdirectories
  };
  for (const auto& scenario : scenarios) {
    SCOPED_TRACE(scenario.nfiles);
    const std::string path = "/readdir_mixed_" + std::to_string(getpid()) +
      "_" + std::to_string(scenario.nfiles);
    std::set<std::string> names;
    std::set<std::string> files;
    auto cleanup = make_scope_guard([&] {
      for (auto& name : files)
        client->unlink((path + "/" + name).c_str(), myperm);
      remove_subdirs(client, path, names, myperm);
    });
    ASSERT_NO_FATAL_FAILURE(make_subdirs(client, path, 2, myperm, &names));
    for (int i = 0; i < scenario.nfiles; ++i) {
      files.insert("f" + std::to_string(i));
      ASSERT_EQ(0, client->mknod((path + "/f" + std::to_string(i)).c_str(),
                                 S_IFREG | 0644, myperm));
    }
    std::set<std::string> all = names;
    all.insert(files.begin(), files.end());
    dir_result_t *dirp;
    ASSERT_EQ(0, client->opendir(path.c_str(), &dirp, myperm));
    {
      auto close = make_scope_guard([&] { client->closedir(dirp); });
      ASSERT_EQ(all, list_dir(client, dirp));
    }

    ASSERT_EQ(0, client->opendir(path.c_str(), &dirp, myperm));
    auto close = make_scope_guard([&] { client->closedir(dirp); });
    ceph_tid_t before;
    {
      std::scoped_lock lock(client->client_lock);
      ASSERT_TRUE(dirp->inode->is_complete_and_ordered());
      before = client->last_tid;
    }
    EXPECT_EQ(all, list_dir(client, dirp));
    std::scoped_lock lock(client->client_lock);
    EXPECT_EQ(scenario.requests, client->last_tid - before);
    EXPECT_EQ(scenario.buffer_empty, dirp->buffer.empty());
    EXPECT_TRUE(dirp->inode->is_complete_and_ordered());
    auto subdirs = cached_subdirs(dirp->inode);
    EXPECT_EQ(names.size(), subdirs.size());
    for (auto& in : subdirs)
      EXPECT_GE(in->rstat_seq, dirp->listing_seq) << in->ino;
  }
}

// An MClientQuota carries rstat that no listing asked for; the next
// listing refreshes it in the same READDIR as the other subdirectories.
TEST_F(TestClient, ReaddirQuotaPushLeavesRstatStale) {
  if (!g_ceph_context->_conf->client_dirsize_rbytes) {
    GTEST_SKIP() << "requires client_dirsize_rbytes=true";
  }
  const std::string path = "/readdir_quota_" + std::to_string(getpid());
  std::set<std::string> names;
  auto cleanup = make_scope_guard([&] {
    remove_subdirs(client, path, names, myperm);
  });
  ASSERT_NO_FATAL_FAILURE(make_subdirs(client, path, 4, myperm, &names));
  dir_result_t *dirp;
  ASSERT_EQ(0, client->opendir(path.c_str(), &dirp, myperm));
  InodeRef diri = dirp->inode;
  {
    auto close = make_scope_guard([&] { client->closedir(dirp); });
    ASSERT_EQ(names, list_dir(client, dirp));
  }

  InodeRef child;
  MetaSession *session;
  auto quota = ceph::make_message<MClientQuota>();
  {
    std::scoped_lock lock(client->client_lock);
    auto subdirs = cached_subdirs(diri);
    ASSERT_EQ(names.size(), subdirs.size());
    child = subdirs[0];
    ASSERT_GT(child->rstat_seq, 0u);
    ASSERT_TRUE(child->auth_cap);
    session = child->auth_cap->session;  // the MDS that sends MClientQuota
    quota->ino = child->ino;
    quota->rstat = child->rstat;
    quota->quota = child->quota;
    quota->set_src(entity_name_t::MDS(session->mds_num));
    quota->set_connection(session->con);
  }
  client->handle_quota(quota);
  {
    // the MDS did not send it: undo its count in the session's push seq
    std::scoped_lock lock(client->client_lock);
    --session->seq;
  }

  ASSERT_EQ(0, client->opendir(path.c_str(), &dirp, myperm));
  auto close = make_scope_guard([&] { client->closedir(dirp); });
  ceph_tid_t before;
  {
    std::scoped_lock lock(client->client_lock);
    ASSERT_TRUE(diri->is_complete_and_ordered());
    ASSERT_EQ(0u, child->rstat_seq);
    before = client->last_tid;
  }
  EXPECT_EQ(names, list_dir(client, dirp));
  std::scoped_lock lock(client->client_lock);
  EXPECT_EQ(3u, client->last_tid - before);
  EXPECT_GE(child->rstat_seq, dirp->listing_seq);
}

// Such a listing starts from the beginning of a complete directory, and its
// reply must agree with readdir_cache; if not, the cache goes, not the client.
TEST_F(TestClient, ReaddirRelistDropsMismatchedCache) {
  if (!g_ceph_context->_conf->client_dirsize_rbytes) {
    GTEST_SKIP() << "requires client_dirsize_rbytes=true";
  }
  const std::string path = "/readdir_mismatch_" + std::to_string(getpid());
  std::set<std::string> names;
  auto cleanup = make_scope_guard([&] {
    remove_subdirs(client, path, names, myperm);
  });
  ASSERT_NO_FATAL_FAILURE(make_subdirs(client, path, 4, myperm, &names));
  dir_result_t *dirp;
  ASSERT_EQ(0, client->opendir(path.c_str(), &dirp, myperm));
  InodeRef diri = dirp->inode;
  {
    auto close = make_scope_guard([&] { client->closedir(dirp); });
    ASSERT_EQ(names, list_dir(client, dirp));
  }
  {
    std::scoped_lock lock(client->client_lock);
    ASSERT_TRUE(diri->is_complete_and_ordered());
    ASSERT_NE(nullptr, diri->dir);
    auto& cache = diri->dir->readdir_cache;
    ASSERT_EQ(names.size(), cache.size());
    cache.erase(cache.begin());  // a cache that lost an entry
  }
  ASSERT_EQ(0, client->opendir(path.c_str(), &dirp, myperm));
  auto close = make_scope_guard([&] { client->closedir(dirp); });
  EXPECT_EQ(names, list_dir(client, dirp));
  std::scoped_lock lock(client->client_lock);
  EXPECT_EQ(0u, diri->flags & (I_COMPLETE | I_DIR_ORDERED));
  ASSERT_NE(nullptr, diri->dir);
  EXPECT_TRUE(diri->dir->readdir_cache.empty());
}
