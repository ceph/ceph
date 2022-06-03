// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "cls/user/cls_user_client.h"
#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"

#include <optional>
#include <system_error>

// create/destroy a pool that's shared by all tests in the process
struct RadosEnv : public ::testing::Environment {
  static std::optional<std::string> pool_name;
 public:
  static librados::Rados rados;
  static librados::IoCtx ioctx;

  void SetUp() override {
    // create pool
    std::string name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(name, rados));
    pool_name = name;
    ASSERT_EQ(rados.ioctx_create(name.c_str(), ioctx), 0);
  }
  void TearDown() override {
    ioctx.close();
    if (pool_name) {
      ASSERT_EQ(destroy_one_pool_pp(*pool_name, rados), 0);
    }
  }
};
std::optional<std::string> RadosEnv::pool_name;
librados::Rados RadosEnv::rados;
librados::IoCtx RadosEnv::ioctx;

auto *const rados_env = ::testing::AddGlobalTestEnvironment(new RadosEnv);

// test fixture with helper functions
class ClsAccount : public ::testing::Test {
 protected:
  librados::IoCtx& ioctx = RadosEnv::ioctx;

  int users_add(const std::string& oid, const std::string& user,
                uint32_t max_users)
  {
    librados::ObjectWriteOperation op;
    cls_account_users_add(op, user, max_users);
    return ioctx.operate(oid, &op);
  }

  int users_rm(const std::string& oid, const std::string& user)
  {
    librados::ObjectWriteOperation op;
    cls_account_users_rm(op, user);
    return ioctx.operate(oid, &op);
  }

  int users_list(const std::string& oid, const std::string& marker,
                 uint32_t max_entries, std::vector<std::string>& entries,
                 bool *truncated, int *pret)
  {
    librados::ObjectReadOperation op;
    cls_account_users_list(op, marker, max_entries, entries, truncated, pret);
    return ioctx.operate(oid, &op, nullptr);
  }

  std::vector<std::string> all_users(const std::string& oid)
  {
    constexpr uint32_t max_entries = 1000;
    std::vector<std::string> entries;
    std::string marker;
    bool truncated = true;

    while (truncated) {
      int r2 = 0;
      int r1 = users_list(oid, marker, max_entries, entries, &truncated, &r2);
      if (r1 < 0) throw std::system_error(r1, std::system_category());
      if (r2 < 0) throw std::system_error(r2, std::system_category());
      if (truncated) {
        marker = entries.back();
      }
    }
    return entries;
  }
};

template <typename ...Args>
std::vector<std::string> make_list(Args&& ...args)
{
  return {std::forward<Args>(args)...};
}

TEST_F(ClsAccount, users_add)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const std::string u1 = "user1";
  const std::string u2 = "user2";
  EXPECT_EQ(0, users_add(oid, u1, 1));
  EXPECT_EQ(-EUSERS, users_add(oid, u2, 1));
  EXPECT_EQ(-EEXIST, users_add(oid, u1, 1));
  EXPECT_EQ(-EEXIST, users_add(oid, u1, 2));
}

TEST_F(ClsAccount, users_rm)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const std::string u1 = "user1";
  EXPECT_EQ(-ENOENT, users_rm(oid, u1));
  ASSERT_EQ(0, users_add(oid, u1, 1));
  ASSERT_EQ(0, users_rm(oid, u1));
  EXPECT_EQ(-ENOENT, users_rm(oid, u1));
  ASSERT_EQ(0, users_add(oid, u1, 1));
}

TEST_F(ClsAccount, users_list)
{
  const std::string oid = __PRETTY_FUNCTION__;
  const std::string u1 = "user1";
  const std::string u2 = "user2";
  const std::string u3 = "user3";
  const std::string u4 = "user4";
  constexpr uint32_t max_users = 1024;

  ASSERT_EQ(0, ioctx.create(oid, true));
  ASSERT_EQ(make_list(), all_users(oid));
  ASSERT_EQ(0, users_add(oid, u1, max_users));
  EXPECT_EQ(make_list(u1), all_users(oid));
  ASSERT_EQ(0, users_add(oid, u2, max_users));
  ASSERT_EQ(0, users_add(oid, u3, max_users));
  ASSERT_EQ(0, users_add(oid, u4, max_users));
  EXPECT_EQ(make_list(u1, u2, u3, u4), all_users(oid));
  ASSERT_EQ(0, users_rm(oid, u2));
  EXPECT_EQ(make_list(u1, u3, u4), all_users(oid));
}
