// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>

#include <iostream>
#include <string>

#include <fmt/format.h>

#include "test/client/TestClient.h"

TEST_F(TestClient, AlternateNameRemount) {
  auto altname = std::string("foo");
  auto dir = fmt::format("{}_{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), getpid());
  ASSERT_EQ(0, client->mkdirat(CEPHFS_AT_FDCWD, dir.c_str(), 0777, myperm, altname));

  client->unmount();
  TearDown();
  SetUp();
  client->mount("/", myperm, true);

  {
    ClientScaffold::walk_dentry_result wdr;
    ASSERT_EQ(0, client->walk(dir.c_str(), &wdr, myperm));
    ASSERT_EQ(wdr.alternate_name, altname);
  }

  ASSERT_EQ(0, client->rmdir(dir.c_str(), myperm));
}


TEST_F(TestClient, AlternateNameMkdir) {
  auto dir = fmt::format("{}_{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), getpid());
  ASSERT_EQ(0, client->mkdirat(CEPHFS_AT_FDCWD, dir.c_str(), 0777, myperm, "foo"));

  {
    ClientScaffold::walk_dentry_result wdr;
    ASSERT_EQ(0, client->walk(dir.c_str(), &wdr, myperm));
    ASSERT_EQ(wdr.alternate_name, "foo");
  }

  ASSERT_EQ(0, client->rmdir(dir.c_str(), myperm));
}

TEST_F(TestClient, AlternateNameLong) {
  auto altname = std::string(4096+1024, '-');
  auto dir = fmt::format("{}_{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), getpid());
  ASSERT_EQ(0, client->mkdirat(CEPHFS_AT_FDCWD, dir.c_str(), 0777, myperm, altname));

  {
    ClientScaffold::walk_dentry_result wdr;
    ASSERT_EQ(0, client->walk(dir.c_str(), &wdr, myperm));
    ASSERT_EQ(wdr.alternate_name, altname);
  }

  ASSERT_EQ(0, client->rmdir(dir.c_str(), myperm));
}

TEST_F(TestClient, AlternateNameCreat) {
  auto altname = std::string("foo");
  auto file = fmt::format("{}_{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), getpid());
  int fd = client->openat(CEPHFS_AT_FDCWD, file.c_str(), O_CREAT|O_WRONLY, myperm, 0777, 0, 0, 0, nullptr, altname);
  ASSERT_LE(0, fd);
  ASSERT_EQ(3, client->write(fd, "baz", 3, 0));
  ASSERT_EQ(0, client->close(fd));

  {
    ClientScaffold::walk_dentry_result wdr;
    ASSERT_EQ(0, client->walk(file, &wdr, myperm));
    ASSERT_EQ(wdr.alternate_name, altname);
  }
}

TEST_F(TestClient, AlternateNameSymlink) {
  auto altname = std::string("foo");
  auto file = fmt::format("{}_{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), getpid());
  int fd = client->openat(CEPHFS_AT_FDCWD, file.c_str(), O_CREAT|O_WRONLY, myperm, 0777, 0, 0, 0, nullptr, altname);
  ASSERT_LE(0, fd);
  ASSERT_EQ(3, client->write(fd, "baz", 3, 0));
  ASSERT_EQ(0, client->close(fd));

  auto file2 = file+"2";
  auto altname2 = altname+"2";
  ASSERT_EQ(0, client->symlinkat(file.c_str(), CEPHFS_AT_FDCWD, file2.c_str(), myperm, altname2));

  {
    ClientScaffold::walk_dentry_result wdr;
    ASSERT_EQ(0, client->walk(file2, &wdr, myperm, false));
    ASSERT_EQ(wdr.alternate_name, altname2);
    ASSERT_EQ(0, client->walk(file, &wdr, myperm));
    ASSERT_EQ(wdr.alternate_name, altname);
  }
}

TEST_F(TestClient, AlternateNameRename) {
  auto altname = std::string("foo");
  auto file = fmt::format("{}_{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), getpid());
  int fd = client->openat(CEPHFS_AT_FDCWD, file.c_str(), O_CREAT|O_WRONLY, myperm, 0777, 0, 0, 0, nullptr, altname);
  ASSERT_LE(0, fd);
  ASSERT_EQ(3, client->write(fd, "baz", 3, 0));
  ASSERT_EQ(0, client->close(fd));

  auto file2 = file+"2";
  auto altname2 = altname+"2";

  ASSERT_EQ(0, client->rename(file.c_str(), file2.c_str(), myperm, altname2));

  {
    ClientScaffold::walk_dentry_result wdr;
    ASSERT_EQ(0, client->walk(file2, &wdr, myperm));
    ASSERT_EQ(wdr.alternate_name, altname2);
  }
}

TEST_F(TestClient, AlternateNameRenameExistMatch) {
  auto altname = std::string("foo");
  auto file = fmt::format("{}_{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), getpid());
  int fd = client->openat(CEPHFS_AT_FDCWD, file.c_str(), O_CREAT|O_WRONLY, myperm, 0777, 0, 0, 0, nullptr, altname);
  ASSERT_LE(0, fd);
  ASSERT_EQ(3, client->write(fd, "baz", 3, 0));
  ASSERT_EQ(0, client->close(fd));

  auto file2 = file+"2";
  auto altname2 = altname+"2";

  fd = client->openat(CEPHFS_AT_FDCWD, file2.c_str(), O_CREAT|O_WRONLY, myperm, 0777, 0, 0, 0, nullptr, altname2);
  ASSERT_LE(0, fd);
  ASSERT_EQ(3, client->write(fd, "baz", 3, 0));
  ASSERT_EQ(0, client->close(fd));

  ASSERT_EQ(0, client->rename(file.c_str(), file2.c_str(), myperm, altname2));

  {
    ClientScaffold::walk_dentry_result wdr;
    ASSERT_EQ(0, client->walk(file2, &wdr, myperm));
    ASSERT_EQ(wdr.alternate_name, altname2);
  }
}

TEST_F(TestClient, AlternateNameRenameExistMisMatch) {
  auto altname = std::string("foo");
  auto file = fmt::format("{}_{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), getpid());
  int fd = client->openat(CEPHFS_AT_FDCWD, file.c_str(), O_CREAT|O_WRONLY, myperm, 0777, 0, 0, 0, nullptr, altname);
  ASSERT_LE(0, fd);
  ASSERT_EQ(3, client->write(fd, "baz", 3, 0));
  ASSERT_EQ(0, client->close(fd));

  auto file2 = file+"2";
  auto altname2 = altname+"2";

  fd = client->openat(CEPHFS_AT_FDCWD, file2.c_str(), O_CREAT|O_WRONLY, myperm, 0777, 0, 0, 0, nullptr, altname+"mismatch");
  ASSERT_LE(0, fd);
  ASSERT_EQ(3, client->write(fd, "baz", 3, 0));
  ASSERT_EQ(0, client->close(fd));

  ASSERT_EQ(-EINVAL, client->rename(file.c_str(), file2.c_str(), myperm, altname2));

  {
    ClientScaffold::walk_dentry_result wdr;
    ASSERT_EQ(0, client->walk(file2, &wdr, myperm));
    ASSERT_EQ(wdr.alternate_name, altname+"mismatch");
  }
}

TEST_F(TestClient, AlternateNameLink) {
  auto altname = std::string("foo");
  auto file = fmt::format("{}_{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), getpid());
  int fd = client->openat(CEPHFS_AT_FDCWD, file.c_str(), O_CREAT|O_WRONLY, myperm, 0777, 0, 0, 0, nullptr, altname);
  ASSERT_LE(0, fd);
  ASSERT_EQ(3, client->write(fd, "baz", 3, 0));
  ASSERT_EQ(0, client->close(fd));

  auto file2 = file+"2";
  auto altname2 = altname+"2";

  ASSERT_EQ(0, client->link(file.c_str(), file2.c_str(), myperm, altname2));

  {
    ClientScaffold::walk_dentry_result wdr;
    ASSERT_EQ(0, client->walk(file2, &wdr, myperm));
    ASSERT_EQ(wdr.alternate_name, altname2);
    ASSERT_EQ(0, client->walk(file, &wdr, myperm));
    ASSERT_EQ(wdr.alternate_name, altname);
  }
}
