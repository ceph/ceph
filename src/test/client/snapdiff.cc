// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <algorithm>
#include <array>
#include <set>

#include "include/scope_guard.h"
#include "test/client/TestClient.h"

TEST_F(TestClient, SnapDiffEntryCountOnRollback) {
  const auto dir = std::string("/snapdiff_rollback_") + std::to_string(getpid());
  ASSERT_EQ(0, client->mkdir(dir.c_str(), 0777, myperm));
  dir_result_t* before = nullptr;
  dir_result_t* after = nullptr;
  auto cleanup = make_scope_guard([&] {
    if (before)
      client->closedir(before);
    if (after)
      client->closedir(after);
    client->rmsnap(dir.c_str(), "before", myperm);
    client->rmsnap(dir.c_str(), "after", myperm);
    for (const auto* name : {"a", "b"})
      client->unlink((dir + "/" + name).c_str(), myperm);
    client->rmdir(dir.c_str(), myperm);
  });

  // One entry fits in an 8 KiB reply, but the inode stat of the next
  // differently named entry does not. Its name and lease still fit.
  const std::string xattr(6000, 'x');
  for (const auto* name : {"a", "b"}) {
    const auto path = dir + "/" + name;
    int fd = client->open(path.c_str(), O_CREAT | O_WRONLY, myperm, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(0, client->close(fd));
    ASSERT_EQ(0, client->setxattr(path.c_str(), "user.big", xattr.data(),
                                xattr.size(), 0, myperm));
  }
  ASSERT_EQ(0, client->mksnap(dir.c_str(), "before", myperm));
  for (const auto* name : {"a", "b"})
    ASSERT_EQ(0, client->unlink((dir + "/" + name).c_str(), myperm));
  ASSERT_EQ(0, client->mksnap(dir.c_str(), "after", myperm));
  ASSERT_EQ(0, client->opendir((dir + "/.snap/before").c_str(), &before, myperm));
  ASSERT_EQ(0, client->opendir((dir + "/.snap/after").c_str(), &after, myperm));

  // Inspect a single reply: automatic pagination could otherwise hide an
  // under-reported count by fetching the omitted entry again.
  ASSERT_EQ(0, client->read_snapdiff_page(before, after->inode->snapid, 8192));
  ASSERT_EQ(1u, before->buffer.size());
  EXPECT_TRUE(before->buffer.front().name == "a" ||
              before->buffer.front().name == "b");
  EXPECT_EQ(before->inode->snapid, before->buffer.front().inode->snapid);
}

using snapdiff_entries = std::multiset<std::pair<std::string, uint64_t>>;

// Sort names the way readdir_snapdiff returns them: by name hash.
template <size_t N>
static void sort_by_hash(Inode* diri, std::array<std::string, N>& names)
{
  std::sort(names.begin(), names.end(), [&](const auto& lhs, const auto& rhs) {
    return std::make_pair(ceph_frag_value(diri->hash_dentry_name(lhs)), lhs) <
           std::make_pair(ceph_frag_value(diri->hash_dentry_name(rhs)), rhs);
  });
}

TEST_F(TestClient, SnapDiffSameNameRollbackResume) {
  const auto dir = std::string("/snapdiff_group_resume_") +
                   std::to_string(getpid());
  ASSERT_EQ(0, client->mkdir(dir.c_str(), 0777, myperm));
  dir_result_t* before = nullptr;
  dir_result_t* after = nullptr;
  dir_result_t* head = nullptr;
  std::array<std::string, 4> names = {"a", "b", "c", "d"};
  auto cleanup = make_scope_guard([&] {
    for (auto* dirp : {before, after, head}) {
      if (dirp)
        client->closedir(dirp);
    }
    client->rmsnap(dir.c_str(), "before", myperm);
    client->rmsnap(dir.c_str(), "after", myperm);
    for (const auto& name : names)
      client->unlink((dir + "/" + name).c_str(), myperm);
    client->rmdir(dir.c_str(), myperm);
  });
  ASSERT_EQ(0, client->opendir(dir.c_str(), &head, myperm));
  // In hash order: names[0] is deleted, names[1] replaced, names[2]
  // created and names[3] unchanged.
  sort_by_hash(head->inode.get(), names);
  ASSERT_EQ(0, client->closedir(head));
  head = nullptr;
  const auto replacement = dir + "/" + names[1];
  const std::string xattr(6000, 'x');
  auto create = [&](const std::string& name) {
    int fd = client->open((dir + "/" + name).c_str(),
                          O_CREAT | O_EXCL | O_WRONLY, myperm, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(0, client->close(fd));
  };
  for (const auto i : {0, 1, 3})
    ASSERT_NO_FATAL_FAILURE(create(names[i]));
  ASSERT_EQ(0, client->setxattr(replacement.c_str(), "user.big", xattr.data(),
                              xattr.size(), 0, myperm));
  ASSERT_EQ(0, client->mksnap(dir.c_str(), "before", myperm));
  for (const auto i : {0, 1})
    ASSERT_EQ(0, client->unlink((dir + "/" + names[i]).c_str(), myperm));
  ASSERT_NO_FATAL_FAILURE(create(names[1]));
  ASSERT_EQ(0, client->setxattr(replacement.c_str(), "user.big", xattr.data(),
                              xattr.size(), 0, myperm));
  ASSERT_NO_FATAL_FAILURE(create(names[2]));
  ASSERT_EQ(0, client->mksnap(dir.c_str(), "after", myperm));
  // Remount so that the replies carry the xattrs.
  TearDown();
  SetUp();
  ASSERT_TRUE(client->is_mounted());
  ASSERT_EQ(0, client->opendir((dir + "/.snap/before").c_str(), &before, myperm));
  ASSERT_EQ(0, client->opendir((dir + "/.snap/after").c_str(), &after, myperm));
  const auto snap_before = before->inode->snapid;
  const auto snap_after = after->inode->snapid;

  // The pair does not fit after names[0] and is rolled back.
  ASSERT_EQ(0, client->read_snapdiff_page(before, snap_after, 8192));
  ASSERT_EQ(1u, before->buffer.size());
  EXPECT_EQ(names[0], before->buffer.front().name);
  EXPECT_EQ(snap_before, before->buffer.front().inode->snapid);
  ASSERT_EQ(names[0], before->last_name);
  ASSERT_GT(before->next_offset, 2u);

  // Resuming, the pair is the first group and is sent whole.
  ASSERT_EQ(0, client->read_snapdiff_page(before, snap_after, 8192));
  snapdiff_entries entries;
  for (const auto& entry : before->buffer)
    entries.emplace(entry.name, entry.inode->snapid);
  snapdiff_entries expected = {{names[1], snap_before}, {names[1], snap_after}};
  EXPECT_EQ(expected, entries);
  ASSERT_EQ(names[1], before->last_name);
  ASSERT_GT(before->next_offset, 2u);

  // Pagination continues after it.
  ASSERT_EQ(0, client->read_snapdiff_page(before, snap_after, 8192));
  entries.clear();
  for (const auto& entry : before->buffer)
    entries.emplace(entry.name, entry.inode->snapid);
  expected = {{names[2], snap_after}};
  EXPECT_EQ(expected, entries);
  EXPECT_EQ(names[2], before->last_name);
  EXPECT_EQ(2u, before->next_offset);
}
