// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "common/fault_injector.h"
#include "common/common_init.h"
#include "common/ceph_argparse.h"
#include <gtest/gtest.h>

TEST(FaultInjectorDeathTest, InjectAbort)
{
  constexpr FaultInjector f{false, InjectAbort{}};
  EXPECT_EQ(f.check(true), 0);
  EXPECT_DEATH([[maybe_unused]] int r = f.check(false), "FaultInjector");
}

TEST(FaultInjectorDeathTest, AssignAbort)
{
  FaultInjector<bool> f;
  ASSERT_EQ(f.check(false), 0);
  f.inject(false, InjectAbort{});
  EXPECT_DEATH([[maybe_unused]] int r = f.check(false), "FaultInjector");
}

// death tests have to run in single-threaded mode, so we can't initialize a
// CephContext until after those have run (gtest automatically runs them first)
class Fixture : public testing::Test {
  boost::intrusive_ptr<CephContext> cct;
  std::optional<NoDoutPrefix> prefix;
 protected:
  void SetUp() override {
    CephInitParameters params(CEPH_ENTITY_TYPE_CLIENT);
    cct.reset(common_preinit(params, CODE_ENVIRONMENT_UTILITY,
			     CINIT_FLAG_NO_DEFAULT_CONFIG_FILE),
	      false);
    prefix.emplace(cct.get(), ceph_subsys_context);
  }
  void TearDown() override {
    prefix.reset();
    cct.reset();
  }
  const DoutPrefixProvider* dpp() { return &*prefix; }
};

// test int as a Key type
using FaultInjectorInt = Fixture;

TEST_F(FaultInjectorInt, Default)
{
  constexpr FaultInjector<int> f;
  EXPECT_EQ(f.check(0), 0);
  EXPECT_EQ(f.check(1), 0);
  EXPECT_EQ(f.check(2), 0);
  EXPECT_EQ(f.check(3), 0);
}

TEST_F(FaultInjectorInt, InjectError)
{
  constexpr FaultInjector f{2, InjectError{-EINVAL}};
  EXPECT_EQ(f.check(0), 0);
  EXPECT_EQ(f.check(1), 0);
  EXPECT_EQ(f.check(2), -EINVAL);
  EXPECT_EQ(f.check(3), 0);
}

TEST_F(FaultInjectorInt, InjectErrorMessage)
{
  FaultInjector f{2, InjectError{-EINVAL, dpp()}};
  EXPECT_EQ(f.check(0), 0);
  EXPECT_EQ(f.check(1), 0);
  EXPECT_EQ(f.check(2), -EINVAL);
  EXPECT_EQ(f.check(3), 0);
}

TEST_F(FaultInjectorInt, AssignError)
{
  FaultInjector<int> f;
  ASSERT_EQ(f.check(0), 0);
  f.inject(0, InjectError{-EINVAL});
  EXPECT_EQ(f.check(0), -EINVAL);
}

TEST_F(FaultInjectorInt, AssignErrorMessage)
{
  FaultInjector<int> f;
  ASSERT_EQ(f.check(0), 0);
  f.inject(0, InjectError{-EINVAL, dpp()});
  EXPECT_EQ(f.check(0), -EINVAL);
}

// test std::string_view as a Key type
using FaultInjectorString = Fixture;

TEST_F(FaultInjectorString, Default)
{
  constexpr FaultInjector<std::string_view> f;
  EXPECT_EQ(f.check("Red"), 0);
  EXPECT_EQ(f.check("Green"), 0);
  EXPECT_EQ(f.check("Blue"), 0);
}

TEST_F(FaultInjectorString, InjectError)
{
  FaultInjector<std::string_view> f{"Red", InjectError{-EIO}};
  EXPECT_EQ(f.check("Red"), -EIO);
  EXPECT_EQ(f.check("Green"), 0);
  EXPECT_EQ(f.check("Blue"), 0);
}

TEST_F(FaultInjectorString, InjectErrorMessage)
{
  FaultInjector<std::string_view> f{"Red", InjectError{-EIO, dpp()}};
  EXPECT_EQ(f.check("Red"), -EIO);
  EXPECT_EQ(f.check("Green"), 0);
  EXPECT_EQ(f.check("Blue"), 0);
}

TEST_F(FaultInjectorString, AssignError)
{
  FaultInjector<std::string_view> f;
  ASSERT_EQ(f.check("Red"), 0);
  f.inject("Red", InjectError{-EINVAL});
  EXPECT_EQ(f.check("Red"), -EINVAL);
}

TEST_F(FaultInjectorString, AssignErrorMessage)
{
  FaultInjector<std::string_view> f;
  ASSERT_EQ(f.check("Red"), 0);
  f.inject("Red", InjectError{-EINVAL, dpp()});
  EXPECT_EQ(f.check("Red"), -EINVAL);
}

// test enum class as a Key type
using FaultInjectorEnum = Fixture;

enum class Color { Red, Green, Blue };

static std::ostream& operator<<(std::ostream& out, const Color& c) {
  switch (c) {
    case Color::Red: return out << "Red";
    case Color::Green: return out << "Green";
    case Color::Blue: return out << "Blue";
  }
  return out;
}

TEST_F(FaultInjectorEnum, Default)
{
  constexpr FaultInjector<Color> f;
  EXPECT_EQ(f.check(Color::Red), 0);
  EXPECT_EQ(f.check(Color::Green), 0);
  EXPECT_EQ(f.check(Color::Blue), 0);
}

TEST_F(FaultInjectorEnum, InjectError)
{
  FaultInjector f{Color::Red, InjectError{-EIO}};
  EXPECT_EQ(f.check(Color::Red), -EIO);
  EXPECT_EQ(f.check(Color::Green), 0);
  EXPECT_EQ(f.check(Color::Blue), 0);
}

TEST_F(FaultInjectorEnum, InjectErrorMessage)
{
  FaultInjector f{Color::Red, InjectError{-EIO, dpp()}};
  EXPECT_EQ(f.check(Color::Red), -EIO);
  EXPECT_EQ(f.check(Color::Green), 0);
  EXPECT_EQ(f.check(Color::Blue), 0);
}

TEST_F(FaultInjectorEnum, AssignError)
{
  FaultInjector<Color> f;
  ASSERT_EQ(f.check(Color::Red), 0);
  f.inject(Color::Red, InjectError{-EINVAL});
  EXPECT_EQ(f.check(Color::Red), -EINVAL);
}

TEST_F(FaultInjectorEnum, AssignErrorMessage)
{
  FaultInjector<Color> f;
  ASSERT_EQ(f.check(Color::Red), 0);
  f.inject(Color::Red, InjectError{-EINVAL, dpp()});
  EXPECT_EQ(f.check(Color::Red), -EINVAL);
}

// test custom move-only Key type
using FaultInjectorMoveOnly = Fixture;

struct MoveOnlyKey {
  MoveOnlyKey() = default;
  MoveOnlyKey(const MoveOnlyKey&) = delete;
  MoveOnlyKey& operator=(const MoveOnlyKey&) = delete;
  MoveOnlyKey(MoveOnlyKey&&) = default;
  MoveOnlyKey& operator=(MoveOnlyKey&&) = default;
  ~MoveOnlyKey() = default;
};

static bool operator==(const MoveOnlyKey&, const MoveOnlyKey&) {
  return true; // all keys are equal
}
static std::ostream& operator<<(std::ostream& out, const MoveOnlyKey&) {
  return out;
}

TEST_F(FaultInjectorMoveOnly, Default)
{
  constexpr FaultInjector<MoveOnlyKey> f;
  EXPECT_EQ(f.check(MoveOnlyKey{}), 0);
}

TEST_F(FaultInjectorMoveOnly, InjectError)
{
  FaultInjector f{MoveOnlyKey{}, InjectError{-EIO}};
  EXPECT_EQ(f.check(MoveOnlyKey{}), -EIO);
}

TEST_F(FaultInjectorMoveOnly, InjectErrorMessage)
{
  FaultInjector f{MoveOnlyKey{}, InjectError{-EIO, dpp()}};
  EXPECT_EQ(f.check(MoveOnlyKey{}), -EIO);
}

TEST_F(FaultInjectorMoveOnly, AssignError)
{
  FaultInjector<MoveOnlyKey> f;
  ASSERT_EQ(f.check({}), 0);
  f.inject({}, InjectError{-EINVAL});
  EXPECT_EQ(f.check({}), -EINVAL);
}

TEST_F(FaultInjectorMoveOnly, AssignErrorMessage)
{
  FaultInjector<MoveOnlyKey> f;
  ASSERT_EQ(f.check({}), 0);
  f.inject({}, InjectError{-EINVAL, dpp()});
  EXPECT_EQ(f.check({}), -EINVAL);
}
