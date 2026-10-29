// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <memory>
#include <type_traits>
#include <utility>

#include <gtest/gtest.h>

#include "include/scope_guard.h"

TEST(ScopeGuard, RunsOnScopeExit)
{
  int n = 0;
  {
    auto guard = make_scope_guard([&n] { ++n; });
    EXPECT_EQ(0, n);
  }
  EXPECT_EQ(1, n);
}

TEST(ScopeGuard, CopiesLvalueCallable)
{
  int n = 0;
  auto f = [&n] { ++n; };
  {
    auto guard = make_scope_guard(f);
    static_assert(std::is_same_v<decltype(guard), scope_guard<decltype(f)>>);
  }
  EXPECT_EQ(1, n);
}

TEST(ScopeGuard, MoveOnlyCallable)
{
  int n = 0;
  {
    auto guard = make_scope_guard([p = std::make_unique<int>(42), &n] {
      n = *p;
    });
  }
  EXPECT_EQ(42, n);
}

TEST(ScopeGuard, DeducesCallableType)
{
  int n = 0;
  {
    scope_guard guard{[&n] { ++n; }};
  }
  EXPECT_EQ(1, n);
}

TEST(ScopeGuard, ConstructsInPlace)
{
  struct Incr {
    int& n;
    explicit Incr(int& n) : n(n) {}
    Incr(Incr&&) = delete;
    // rvalue-qualified, so the guard has to call it as an rvalue
    void operator()() && { ++n; }
  };
  int n = 0;
  {
    scope_guard<Incr> guard(std::in_place, n);
  }
  EXPECT_EQ(1, n);
}

TEST(ScopeGuard, Dismiss)
{
  int n = 0;
  {
    auto guard = make_scope_guard([&n] { ++n; });
    guard.dismiss();
  }
  EXPECT_EQ(0, n);
}

TEST(ScopeGuard, NotCopyableOrMovable)
{
  using guard_t = decltype(make_scope_guard([] {}));
  static_assert(!std::is_copy_constructible_v<guard_t>);
  static_assert(!std::is_move_constructible_v<guard_t>);
  static_assert(!std::is_copy_assignable_v<guard_t>);
  static_assert(!std::is_move_assignable_v<guard_t>);
}
