// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <initializer_list>
#include <optional>

#include "gtest/gtest.h"

#include "include/any.h"

using std::optional;
using std::bad_any_cast;

using ceph::immobile_any;
using ceph::unique_any;
using ceph::shared_any;

using ceph::make_immobile_any;
using ceph::make_unique_any;
using ceph::make_shared_any;

using ceph::any_cast;
using std::swap;

template<typename A>
static void test_empty() {
  A a;
  EXPECT_FALSE(a.has_value());
  EXPECT_EQ(typeid(void), a.type());
  a.reset();
  EXPECT_FALSE(a.has_value());
  EXPECT_EQ(typeid(void), a.type());

}

TEST(Empty, Immobile) {
  static_assert(std::is_nothrow_default_constructible_v<immobile_any<1024>>);
  test_empty<immobile_any<1024>>();
}

TEST(Empty, Unique) {
  static_assert(std::is_nothrow_default_constructible_v<unique_any>);
  test_empty<unique_any>();
}

TEST(Empty, Shared) {
  static_assert(std::is_nothrow_default_constructible_v<shared_any>);
  test_empty<shared_any>();
}

struct cmd_tattler {
  static thread_local bool copied;
  static thread_local bool moved;
  static thread_local bool destructed;

  static void reset() {
    copied = false;
    moved = false;
    destructed = false;
  }

  cmd_tattler() noexcept = default;
  ~cmd_tattler() noexcept {
    if (destructed) {
      std::terminate();
    }
    destructed = true;
  }
  cmd_tattler(const cmd_tattler&) noexcept {
    if (copied) {
      std::terminate();
    }
    copied = true;
  }
  cmd_tattler& operator =(const cmd_tattler&) noexcept {
    if (copied) {
      std::terminate();
    }
    copied = true;
    return *this;
  }

  cmd_tattler(cmd_tattler&&) noexcept {
    if (moved) {
      std::terminate();
    }
    moved = true;
  }
  cmd_tattler& operator =(cmd_tattler&&) noexcept {
    if (moved) {
      std::terminate();
    }
    moved = true;
    return *this;
  }
};

thread_local bool cmd_tattler::copied = false;
thread_local bool cmd_tattler::moved = false;
thread_local bool cmd_tattler::destructed = false;

struct not_noexcept {
  not_noexcept() = default;

  not_noexcept(const not_noexcept&) noexcept(false) {
  }
  not_noexcept& operator =(const not_noexcept&) noexcept(false) {
    return *this;
  }

  not_noexcept(not_noexcept&&) noexcept(false) {
  }
  not_noexcept& operator =(not_noexcept&&) noexcept(false) {
    return *this;
  }

  template<typename ...Args>
  not_noexcept(Args&& ...) noexcept(false) {
  }

  template<typename U, typename ...Args>
  not_noexcept(std::initializer_list<U>, Args&& ...) noexcept(false) {
  }
};

template<typename A>
static void test_value_CMD() {
  {
    cmd_tattler::reset();
    cmd_tattler c;
    A a(c);
    EXPECT_TRUE(cmd_tattler::copied);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(cmd_tattler), a.type());
    a.reset();
    EXPECT_TRUE(cmd_tattler::destructed);
    EXPECT_FALSE(a.has_value());
    EXPECT_EQ(typeid(void), a.type());

    cmd_tattler::reset();
    a = c;
    EXPECT_TRUE(cmd_tattler::copied);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(cmd_tattler), a.type());

    cmd_tattler::reset();
    a = c;
    EXPECT_TRUE(cmd_tattler::copied);
    EXPECT_TRUE(cmd_tattler::destructed);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(cmd_tattler), a.type());
    cmd_tattler::reset();
    a.reset();
    EXPECT_TRUE(cmd_tattler::destructed);
    cmd_tattler::reset();
  }
  {
    cmd_tattler::reset();
    cmd_tattler c;
    A a(std::move(c));
    EXPECT_TRUE(cmd_tattler::moved);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(cmd_tattler), a.type());
    a.reset();
    EXPECT_TRUE(cmd_tattler::destructed);
    EXPECT_FALSE(a.has_value());
    EXPECT_EQ(typeid(void), a.type());

    cmd_tattler::reset();
    a = std::move(c);
    EXPECT_TRUE(cmd_tattler::moved);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(cmd_tattler), a.type());

    cmd_tattler::reset();
    a = std::move(c);
    EXPECT_TRUE(cmd_tattler::moved);
    EXPECT_TRUE(cmd_tattler::destructed);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(cmd_tattler), a.type());
    cmd_tattler::reset();
    a.reset();
    EXPECT_TRUE(cmd_tattler::destructed);
    cmd_tattler::reset();
  }
  {
    cmd_tattler::reset();
    A a(cmd_tattler{});
    EXPECT_TRUE(cmd_tattler::moved);
    EXPECT_TRUE(cmd_tattler::destructed);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(cmd_tattler), a.type());

    cmd_tattler::reset();
    a.reset();
    EXPECT_TRUE(cmd_tattler::destructed);
    EXPECT_FALSE(a.has_value());
    EXPECT_EQ(typeid(void), a.type());

    cmd_tattler::reset();
    a = cmd_tattler{};
    EXPECT_TRUE(cmd_tattler::moved);
    EXPECT_TRUE(cmd_tattler::destructed);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(cmd_tattler), a.type());
    cmd_tattler::reset();
    a.reset();
    EXPECT_TRUE(cmd_tattler::destructed);
    cmd_tattler::reset();
  }
}



TEST(Value_CMD, Immobile) {
  static_assert(std::is_nothrow_constructible_v<
		immobile_any<1024>, const cmd_tattler&>);
  static_assert(std::is_nothrow_assignable_v<
		immobile_any<1024>, const cmd_tattler&>);
  static_assert(std::is_nothrow_constructible_v<
		immobile_any<1024>, cmd_tattler&&>);
  static_assert(std::is_nothrow_assignable_v<
		immobile_any<1024>, cmd_tattler&&>);

  static_assert(!std::is_nothrow_constructible_v<
		immobile_any<1024>, const not_noexcept&>);
  static_assert(!std::is_nothrow_assignable_v<
		immobile_any<1024>, const not_noexcept&>);
  static_assert(!std::is_nothrow_constructible_v<
		immobile_any<1024>, not_noexcept&&>);
  static_assert(!std::is_nothrow_assignable_v<
		immobile_any<1024>, not_noexcept&&>);

  test_value_CMD<immobile_any<1024>>();
}

TEST(Value_CMD, Unique) {
  static_assert(!std::is_nothrow_constructible_v<
		unique_any, const cmd_tattler&>);
  static_assert(!std::is_nothrow_assignable_v<
		unique_any, const cmd_tattler&>);
  static_assert(!std::is_nothrow_constructible_v<
		unique_any, cmd_tattler&&>);
  static_assert(!std::is_nothrow_assignable_v<
		unique_any, cmd_tattler&&>);

  static_assert(!std::is_nothrow_constructible_v<
		unique_any, const not_noexcept&>);
  static_assert(!std::is_nothrow_assignable_v<
		unique_any, const not_noexcept&>);
  static_assert(!std::is_nothrow_constructible_v<
		unique_any, not_noexcept&&>);
  static_assert(!std::is_nothrow_assignable_v<
		unique_any, not_noexcept&&>);

  test_value_CMD<unique_any>();
}

TEST(Value_CMD, Shared) {
  static_assert(!std::is_nothrow_constructible_v<
		shared_any, const cmd_tattler&>);
  static_assert(!std::is_nothrow_assignable_v<
		shared_any, const cmd_tattler&>);
  static_assert(!std::is_nothrow_constructible_v<
		shared_any, cmd_tattler&&>);
  static_assert(!std::is_nothrow_assignable_v<
		shared_any, cmd_tattler&&>);

  static_assert(!std::is_nothrow_constructible_v<
		shared_any, const not_noexcept&>);
  static_assert(!std::is_nothrow_assignable_v<
		shared_any, const not_noexcept&>);
  static_assert(!std::is_nothrow_constructible_v<
		shared_any, not_noexcept&&>);
  static_assert(!std::is_nothrow_assignable_v<
		shared_any, not_noexcept&&>);

  test_value_CMD<shared_any>();
}

template<typename A>
static void test_move() {
  {
    A a(5);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(int), a.type());

    A b(std::move(a));
    EXPECT_TRUE(b.has_value());
    EXPECT_EQ(typeid(int), b.type());

    EXPECT_FALSE(a.has_value());
    EXPECT_EQ(typeid(void), a.type());
  }
  {
    cmd_tattler::reset();
    A a(cmd_tattler{});

    A b(5);
    EXPECT_TRUE(b.has_value());
    EXPECT_EQ(typeid(int), b.type());
    cmd_tattler::reset();

    a = std::move(b);
    EXPECT_TRUE(cmd_tattler::destructed);

    EXPECT_FALSE(b.has_value());
    EXPECT_EQ(typeid(void), b.type());

    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(int), a.type());
  }
}

static_assert(!std::is_move_constructible_v<immobile_any<1024>>);
static_assert(!std::is_move_assignable_v<immobile_any<1024>>);

TEST(Move, Unique) {
  static_assert(std::is_nothrow_move_constructible_v<unique_any>);
  static_assert(std::is_nothrow_move_assignable_v<unique_any>);

  test_move<unique_any>();
}

TEST(Move, Shared) {
  static_assert(std::is_nothrow_move_constructible_v<shared_any>);
  static_assert(std::is_nothrow_move_assignable_v<shared_any>);

  test_move<shared_any>();
}

template<typename A>
static void test_copy() {
  {
    const A a(5);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(int), a.type());

    A b(a);
    EXPECT_TRUE(b.has_value());
    EXPECT_EQ(typeid(int), b.type());

    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(int), a.type());

    EXPECT_EQ(any_cast<int>(a), any_cast<int>(b));
  }
  {
    cmd_tattler::reset();
    A a(cmd_tattler{});

    const A b(5);
    EXPECT_TRUE(b.has_value());
    EXPECT_EQ(typeid(int), b.type());
    cmd_tattler::reset();

    a = b;
    EXPECT_TRUE(cmd_tattler::destructed);

    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(int), a.type());

    EXPECT_TRUE(b.has_value());
    EXPECT_EQ(typeid(int), b.type());

    EXPECT_EQ(any_cast<int>(a), any_cast<int>(b));
  }
}

static_assert(!std::is_copy_constructible_v<immobile_any<1024>>);
static_assert(!std::is_copy_assignable_v<immobile_any<1024>>);

static_assert(!std::is_copy_constructible_v<unique_any>);
static_assert(!std::is_copy_assignable_v<unique_any>);

TEST(Copy, Shared) {
  static_assert(std::is_nothrow_copy_constructible_v<shared_any>);
  test_copy<shared_any>();
}

struct unmoving {
  optional<int> a;

  unmoving() noexcept {}

  template<typename... Args>
  unmoving(Args&& ...args) noexcept
    : a(sizeof...(Args)) {}

  template<typename U, typename... Args>
  unmoving(std::initializer_list<U> l) noexcept
    : a(-l.size()) {}

  template<typename U, typename... Args>
  unmoving(std::initializer_list<U> l, Args&& ...args) noexcept
    : a(-l.size() * sizeof...(Args)) {}

  unmoving(const unmoving&) = delete;
  unmoving& operator =(const unmoving&) = delete;

  unmoving(unmoving&&) = delete;
  unmoving& operator =(unmoving&&) = delete;
};

template<typename A>
static void test_unmoving_pack_il() {
  // Nothing!
  {
    const A a(std::in_place_type<unmoving>);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_FALSE(any_cast<const unmoving&>(a).a);
  }
  {
    cmd_tattler::reset();
    A a(cmd_tattler{});

    cmd_tattler::reset();
    a.template emplace<unmoving>();
    EXPECT_TRUE(cmd_tattler::destructed);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_FALSE(any_cast<unmoving&>(a).a);
  }

  // Pack!
  {
    const A a(std::in_place_type<unmoving>, nullptr, 5, 3.1);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_TRUE(any_cast<const unmoving&>(a).a);
    EXPECT_EQ(3, *any_cast<const unmoving&>(a).a);
  }
  {
    cmd_tattler::reset();
    A a(cmd_tattler{});

    cmd_tattler::reset();
    a.template emplace<unmoving>(nullptr, 5, 3.1);
    EXPECT_TRUE(cmd_tattler::destructed);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_EQ(3, *any_cast<unmoving&>(a).a);
  }

  // List!
  {
    const A a(std::in_place_type<unmoving>, {true, true, true, true});
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_TRUE(any_cast<const unmoving&>(a).a);
    EXPECT_EQ(-4, *any_cast<const unmoving&>(a).a);
  }
  {
    cmd_tattler::reset();
    A a(cmd_tattler{});

    cmd_tattler::reset();
    a.template emplace<unmoving>({true, true, true, true});
    EXPECT_TRUE(cmd_tattler::destructed);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_EQ(-4, *any_cast<unmoving&>(a).a);
  }

  // List + pack!!
  {
    const A a(std::in_place_type<unmoving>, {true, true, true, true},
	      nullptr, 5, 3.1);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_TRUE(any_cast<const unmoving&>(a).a);
    EXPECT_EQ(-12, *any_cast<const unmoving&>(a).a);
  }
  {
    cmd_tattler::reset();
    A a(cmd_tattler{});

    cmd_tattler::reset();
    a.template emplace<unmoving>({true, true, true, true}, nullptr, 5, 3.1);
    EXPECT_TRUE(cmd_tattler::destructed);
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_EQ(-12, *any_cast<unmoving&>(a).a);
  }
}

TEST(UmovingPackIl, Immobile) {
  static_assert(std::is_nothrow_constructible_v<immobile_any<1024>,
		std::in_place_type_t<unmoving>>);
  static_assert(noexcept(immobile_any<1024>{}.emplace<unmoving>()));

  static_assert(std::is_nothrow_constructible_v<immobile_any<1024>,
		std::in_place_type_t<unmoving>, std::nullptr_t, int, double>);
  static_assert(noexcept(immobile_any<1024>{}.emplace<unmoving>(
			   nullptr, 5, 3.1)));

  static_assert(std::is_nothrow_constructible_v<immobile_any<1024>,
		std::in_place_type_t<unmoving>, std::initializer_list<int>>);
  static_assert(noexcept(immobile_any<1024>{}.emplace<unmoving>(
			   {true, true, true, true})));

  static_assert(std::is_nothrow_constructible_v<immobile_any<1024>,
		std::in_place_type_t<unmoving>, std::initializer_list<int>,
		std::nullptr_t, int, double>);
  static_assert(noexcept(immobile_any<1024>{}.emplace<unmoving>(
			   {true, true, true, true}, nullptr, 5, 3.1)));

  test_unmoving_pack_il<immobile_any<1024>>();
}

TEST(UmovingPackIl, Unique) {
  static_assert(!std::is_nothrow_constructible_v<unique_any,
		std::in_place_type_t<unmoving>>);
  static_assert(!noexcept(unique_any{}.emplace<unmoving>()));

  static_assert(!std::is_nothrow_constructible_v<unique_any,
		std::in_place_type_t<unmoving>, std::nullptr_t, int, double>);
  static_assert(!noexcept(unique_any{}.emplace<unmoving>(
			    nullptr, 5, 3.1)));

  static_assert(!std::is_nothrow_constructible_v<unique_any,
		std::in_place_type_t<unmoving>, std::initializer_list<int>>);
  static_assert(!noexcept(unique_any{}.emplace<unmoving>(
			   {true, true, true, true})));

  static_assert(!std::is_nothrow_constructible_v<unique_any,
		std::in_place_type_t<unmoving>, std::initializer_list<int>,
		std::nullptr_t, int, double>);
  static_assert(!noexcept(unique_any{}.emplace<unmoving>(
			   {true, true, true, true}, nullptr, 5, 3.1)));

  test_unmoving_pack_il<unique_any>();
}

TEST(UmovingPackIl, Shared) {
  static_assert(!std::is_nothrow_constructible_v<shared_any,
		std::in_place_type_t<unmoving>>);
  static_assert(!noexcept(shared_any{}.emplace<unmoving>()));

  static_assert(!std::is_nothrow_constructible_v<shared_any,
		std::in_place_type_t<unmoving>, std::nullptr_t, int, double>);
  static_assert(!noexcept(shared_any{}.emplace<unmoving>(
			    nullptr, 5, 3.1)));

  static_assert(!std::is_nothrow_constructible_v<shared_any,
		std::in_place_type_t<unmoving>, std::initializer_list<int>>);
  static_assert(!noexcept(shared_any{}.emplace<unmoving>(
			   {true, true, true, true})));

  static_assert(!std::is_nothrow_constructible_v<shared_any,
		std::in_place_type_t<unmoving>, std::initializer_list<int>,
		std::nullptr_t, int, double>);
  static_assert(!noexcept(shared_any{}.emplace<unmoving>(
			   {true, true, true, true}, nullptr, 5, 3.1)));

  test_unmoving_pack_il<shared_any>();
}

template<typename A>
static void test_swap() {
  A a(true);
  ASSERT_TRUE(a.has_value());
  ASSERT_EQ(typeid(bool), a.type());
  ASSERT_EQ(true, any_cast<bool>(a));

  A b(5);
  ASSERT_TRUE(b.has_value());
  ASSERT_EQ(typeid(int), b.type());
  ASSERT_EQ(5, any_cast<int>(b));

  a.swap(b);
  EXPECT_TRUE(a.has_value());
  EXPECT_EQ(typeid(int), a.type());
  EXPECT_EQ(5, any_cast<int>(a));

  EXPECT_TRUE(b.has_value());
  ASSERT_EQ(typeid(bool), b.type());
  ASSERT_EQ(true, any_cast<bool>(b));

  swap(a,b);
  EXPECT_TRUE(a.has_value());
  EXPECT_EQ(typeid(bool), a.type());
  EXPECT_EQ(true, any_cast<bool>(a));

  EXPECT_TRUE(b.has_value());
  EXPECT_EQ(typeid(int), b.type());
  EXPECT_EQ(5, any_cast<int>(b));
}

static_assert(!std::is_swappable_v<immobile_any<1024>>);

TEST(Swap, Unique) {
  static_assert(std::is_nothrow_swappable_v<unique_any>);
  test_swap<unique_any>();
}

TEST(Swap, Shared) {
  static_assert(std::is_nothrow_swappable_v<shared_any>);
  test_swap<shared_any>();
}

template<typename A>
static void test_cast() {
  // Empty
  {
    A a;
    EXPECT_EQ(nullptr, any_cast<int>(&a));
    EXPECT_THROW({any_cast<int>(a);}, bad_any_cast);
    EXPECT_THROW({any_cast<int&>(a);}, bad_any_cast);
    EXPECT_THROW({any_cast<int>(std::move(a));}, bad_any_cast);
    EXPECT_THROW({any_cast<int&&>(std::move(a));}, bad_any_cast);
  }

  // Constant Empty
  {
    const A a{};
    EXPECT_EQ(nullptr, any_cast<int>(const_cast<const A*>(&a)));
    EXPECT_THROW({any_cast<int>(a);}, bad_any_cast);
    EXPECT_THROW({any_cast<const int&>(a);}, bad_any_cast);
  }

  // Filled!
  {
    A a(true);
    EXPECT_TRUE(*any_cast<bool>(&a));
    EXPECT_EQ(nullptr, any_cast<int>(&a));

    EXPECT_TRUE(any_cast<bool>(a));
    EXPECT_THROW({any_cast<int>(a);}, bad_any_cast);

    EXPECT_TRUE(any_cast<bool&>(a));
    EXPECT_THROW({any_cast<int&>(a);}, bad_any_cast);

    EXPECT_TRUE(any_cast<bool>(std::move(a)));
    EXPECT_THROW({any_cast<int>(std::move(a));}, bad_any_cast);

    EXPECT_TRUE(any_cast<bool&&>(std::move(a)));
    EXPECT_THROW({any_cast<int&&>(std::move(a));}, bad_any_cast);
  }

  // Constant filled
  {
    const A a(true);
    EXPECT_TRUE(*any_cast<const bool>(&a));
    EXPECT_EQ(nullptr, any_cast<const int>(&a));

    EXPECT_TRUE(any_cast<bool>(a));
    EXPECT_THROW({any_cast<int>(a);}, bad_any_cast);

    EXPECT_TRUE(any_cast<const bool&>(a));
    EXPECT_THROW({any_cast<const int&>(a);}, bad_any_cast);
  }

  // Move!
  {
    cmd_tattler::reset();
    A a(cmd_tattler{});
    cmd_tattler::reset();

    auto q = any_cast<cmd_tattler>(std::move(a));
    EXPECT_TRUE(cmd_tattler::moved);
    cmd_tattler::reset();
    a.reset();
    cmd_tattler::reset();
  }

  // Move! Again!
  {
    cmd_tattler::reset();
    auto q = any_cast<cmd_tattler>(A(std::in_place_type<cmd_tattler>));
    EXPECT_TRUE(cmd_tattler::moved);
    cmd_tattler::reset();
  }
}

TEST(Cast, Immobile) {
  test_cast<immobile_any<1024>>();
}

TEST(Cast, Unique) {
  test_cast<unique_any>();
}

TEST(Cast, Shared) {
  test_cast<shared_any>();
}

TEST(Make, Immobile) {
  // Nothing!
  {
    auto a{make_immobile_any<unmoving, 1024>()};
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_FALSE(any_cast<const unmoving&>(a).a);
  }

  // Pack!
  {
    auto a(make_immobile_any<unmoving, 1024>(nullptr, 5, 3.1));
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_TRUE(any_cast<const unmoving&>(a).a);
    EXPECT_EQ(3, *any_cast<const unmoving&>(a).a);
  }

  // List!
  {
    auto a(make_immobile_any<unmoving, 1024>({true, true, true, true}));
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_TRUE(any_cast<const unmoving&>(a).a);
    EXPECT_EQ(-4, *any_cast<const unmoving&>(a).a);
  }

  // List + pack!!
  {
    auto a{make_immobile_any<unmoving, 1024>({true, true, true, true},
					     nullptr, 5, 3.1)};
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_TRUE(any_cast<const unmoving&>(a).a);
    EXPECT_EQ(-12, *any_cast<const unmoving&>(a).a);
  }
}

TEST(Make, Unique) {
  // Nothing!
  {
    auto a{make_unique_any<unmoving>()};
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_FALSE(any_cast<const unmoving&>(a).a);
  }

  // Pack!
  {
    auto a(make_unique_any<unmoving>(nullptr, 5, 3.1));
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_TRUE(any_cast<const unmoving&>(a).a);
    EXPECT_EQ(3, *any_cast<const unmoving&>(a).a);
  }

  // List!
  {
    auto a(make_unique_any<unmoving>({true, true, true, true}));
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_TRUE(any_cast<const unmoving&>(a).a);
    EXPECT_EQ(-4, *any_cast<const unmoving&>(a).a);
  }

  // List + pack!!
  {
    auto a{make_unique_any<unmoving>({true, true, true, true},
				     nullptr, 5, 3.1)};
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_TRUE(any_cast<const unmoving&>(a).a);
    EXPECT_EQ(-12, *any_cast<const unmoving&>(a).a);
  }
}

TEST(Make, Shared) {
  // Nothing!
  {
    auto a{make_shared_any<unmoving>()};
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_FALSE(any_cast<const unmoving&>(a).a);
  }

  // Pack!
  {
    auto a(make_shared_any<unmoving>(nullptr, 5, 3.1));
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_TRUE(any_cast<const unmoving&>(a).a);
    EXPECT_EQ(3, *any_cast<const unmoving&>(a).a);
  }

  // List!
  {
    auto a(make_shared_any<unmoving>({true, true, true, true}));
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_TRUE(any_cast<const unmoving&>(a).a);
    EXPECT_EQ(-4, *any_cast<const unmoving&>(a).a);
  }

  // List + pack!!
  {
    auto a{make_shared_any<unmoving>({true, true, true, true},
				     nullptr, 5, 3.1)};
    EXPECT_TRUE(a.has_value());
    EXPECT_EQ(typeid(unmoving), a.type());
    EXPECT_TRUE(any_cast<const unmoving&>(a).a);
    EXPECT_EQ(-12, *any_cast<const unmoving&>(a).a);
  }
}
