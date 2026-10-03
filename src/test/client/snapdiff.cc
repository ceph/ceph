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
