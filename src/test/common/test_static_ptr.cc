// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/static_ptr.h"
#include <gtest/gtest.h>

using ceph::static_ptr;
using ceph::make_static;

class base {
public:
  virtual int func() = 0;
  virtual ~base() = default;
};

class sibling1 : public base {
public:
  int func() override { return 0; }
};

class sibling2 : public base {
public:
  int func() override { return 9; }
  virtual int call(int) = 0;
};

class grandchild : public sibling2 {
protected:
  int val;
public:
  explicit grandchild(int val) : val(val) {}
  virtual int call(int n) { return n * val; }
};

class great_grandchild : public grandchild {
public:
  great_grandchild(int val) : grandchild(val) {}
  int call(int n) override { return n + val; }
};

TEST(StaticPtr, EmptyCreation) {
  static_ptr<base, sizeof(grandchild)> p;
  EXPECT_FALSE(p);
  EXPECT_EQ(p, nullptr);
  EXPECT_EQ(nullptr, p);
  EXPECT_TRUE(p.get() == nullptr);
}

TEST(StaticPtr, CreationCall) {
  {
    static_ptr<base, sizeof(grandchild)> p(std::in_place_type_t<sibling1>{});
    EXPECT_TRUE(p);
    EXPECT_FALSE(p == nullptr);
    EXPECT_FALSE(nullptr == p);
    EXPECT_FALSE(p.get() == nullptr);
    EXPECT_EQ(p->func(), 0);
    EXPECT_EQ((*p).func(), 0);
    EXPECT_EQ((p.get())->func(), 0);
  }
  {
    auto p = make_static<base, sibling1>();
    EXPECT_TRUE(p);
    EXPECT_FALSE(p == nullptr);
    EXPECT_FALSE(nullptr == p);
    EXPECT_FALSE(p.get() == nullptr);
    EXPECT_EQ(p->func(), 0);
    EXPECT_EQ((*p).func(), 0);
    EXPECT_EQ((p.get())->func(), 0);
  }
}

TEST(StaticPtr, CreateReset) {
  {
    static_ptr<base, sizeof(grandchild)> p(std::in_place_type_t<sibling1>{});
    EXPECT_EQ((p.get())->func(), 0);
    p.reset();
    EXPECT_FALSE(p);
    EXPECT_EQ(p, nullptr);
    EXPECT_EQ(nullptr, p);
    EXPECT_TRUE(p.get() == nullptr);
  }
  {
    static_ptr<base, sizeof(grandchild)> p(std::in_place_type_t<sibling1>{});
    EXPECT_EQ((p.get())->func(), 0);
    p = nullptr;
    EXPECT_FALSE(p);
    EXPECT_EQ(p, nullptr);
    EXPECT_EQ(nullptr, p);
    EXPECT_TRUE(p.get() == nullptr);
  }
}

TEST(StaticPtr, CreateEmplace) {
  static_ptr<base, sizeof(grandchild)> p(std::in_place_type_t<sibling1>{});
  EXPECT_EQ((p.get())->func(), 0);
  p.emplace<grandchild>(30);
  EXPECT_EQ(p->func(), 9);
}

TEST(StaticPtr, CopyMove) {
  // Won't compile. Good.
  // static_ptr<base, sizeof(base)> p1(std::in_place_type_t<grandchild>{}, 3);

  static_ptr<base, sizeof(base)> p1(std::in_place_type_t<sibling1>{});
  static_ptr<base, sizeof(grandchild)> p2(std::in_place_type_t<grandchild>{},
                                          3);

  // This also does not compile. Good.
  // p1 = p2;
  p2 = p1;
  EXPECT_EQ(p1->func(), 0);

  p2 = std::move(p1);
  EXPECT_EQ(p1->func(), 0);
}

TEST(StaticPtr, ImplicitUpcast) {
  static_ptr<base, sizeof(grandchild)> p1;
  static_ptr<sibling2, sizeof(grandchild)> p2(std::in_place_type_t<grandchild>{}, 3);

  p1 = p2;
  EXPECT_EQ(p1->func(), 9);

  p1 = std::move(p2);
  EXPECT_EQ(p1->func(), 9);

  p2.reset();

  // Doesn't compile. Good.
  // p2 = p1;
}

TEST(StaticPtr, StaticCast) {
  static_ptr<base, sizeof(grandchild)> p1(std::in_place_type_t<grandchild>{}, 3);
  static_ptr<sibling2, sizeof(grandchild)> p2;

  p2 = ceph::static_pointer_cast<sibling2, sizeof(grandchild)>(p1);
  EXPECT_EQ(p2->func(), 9);
  EXPECT_EQ(p2->call(10), 30);

  p2 = ceph::static_pointer_cast<sibling2, sizeof(grandchild)>(std::move(p1));
  EXPECT_EQ(p2->func(), 9);
  EXPECT_EQ(p2->call(10), 30);
}

TEST(StaticPtr, DynamicCast) {
  static constexpr auto sz = sizeof(great_grandchild);
  {
    static_ptr<base, sz> p1(std::in_place_type_t<grandchild>{}, 3);
    auto p2 = ceph::dynamic_pointer_cast<great_grandchild, sz>(p1);
    EXPECT_FALSE(p2);
  }
  {
    static_ptr<base, sz> p1(std::in_place_type_t<grandchild>{}, 3);
    auto p2 = ceph::dynamic_pointer_cast<great_grandchild, sz>(std::move(p1));
    EXPECT_FALSE(p2);
  }

  {
    static_ptr<base, sz> p1(std::in_place_type_t<grandchild>{}, 3);
    auto p2 = ceph::dynamic_pointer_cast<grandchild, sz>(p1);
    EXPECT_TRUE(p2);
    EXPECT_EQ(p2->func(), 9);
    EXPECT_EQ(p2->call(10), 30);
  }
  {
    static_ptr<base, sz> p1(std::in_place_type_t<grandchild>{}, 3);
    auto p2 = ceph::dynamic_pointer_cast<grandchild, sz>(std::move(p1));
    EXPECT_TRUE(p2);
    EXPECT_EQ(p2->func(), 9);
    EXPECT_EQ(p2->call(10), 30);
  }
}

class constable {
public:
  int foo() {
    return 2;
  }
  int foo() const {
    return 5;
  }
};

TEST(StaticPtr, ConstCast) {
  static constexpr auto sz = sizeof(constable);
  {
    auto p1 = make_static<const constable>();
    static_assert(std::is_const<decltype(p1)::element_type>{},
                  "Things are not as const as they ought to be.");
    EXPECT_EQ(p1->foo(), 5);
    auto p2 = ceph::const_pointer_cast<constable, sz>(p1);
    static_assert(!std::is_const<decltype(p2)::element_type>{},
                  "Things are more const than they ought to be.");
    EXPECT_TRUE(p2);
    EXPECT_EQ(p2->foo(), 2);
  }
  {
    auto p1 = make_static<const constable>();
    EXPECT_EQ(p1->foo(), 5);
    auto p2 = ceph::const_pointer_cast<constable, sz>(std::move(p1));
    static_assert(!std::is_const<decltype(p2)::element_type>{},
                  "Things are more const than they ought to be.");
    EXPECT_TRUE(p2);
    EXPECT_EQ(p2->foo(), 2);
  }
}

TEST(StaticPtr, ReinterpretCast) {
  static constexpr auto sz = sizeof(grandchild);
  {
    auto p1 = make_static<grandchild>(3);
    auto p2 = ceph::reinterpret_pointer_cast<constable, sz>(p1);
    static_assert(std::is_same<decltype(p2)::element_type, constable>{},
                  "Reinterpret is screwy.");
    auto p3 = ceph::reinterpret_pointer_cast<grandchild, sz>(p2);
    static_assert(std::is_same<decltype(p3)::element_type, grandchild>{},
                  "Reinterpret is screwy.");
    EXPECT_EQ(p3->func(), 9);
    EXPECT_EQ(p3->call(10), 30);
  }
  {
    auto p1 = make_static<grandchild>(3);
    auto p2 = ceph::reinterpret_pointer_cast<constable, sz>(std::move(p1));
    static_assert(std::is_same<decltype(p2)::element_type, constable>{},
                  "Reinterpret is screwy.");
    auto p3 = ceph::reinterpret_pointer_cast<grandchild, sz>(std::move(p2));
    static_assert(std::is_same<decltype(p3)::element_type, grandchild>{},
                  "Reinterpret is screwy.");
    EXPECT_EQ(p3->func(), 9);
    EXPECT_EQ(p3->call(10), 30);
  }
}

struct exceptional {
  exceptional() = default;
  exceptional(const exceptional& e) {
    throw std::exception();
  }
  exceptional(exceptional&& e) {
    throw std::exception();
  }
};

TEST(StaticPtr, Exceptional) {
  static_ptr<exceptional> p1(std::in_place_type_t<exceptional>{});
  EXPECT_ANY_THROW(static_ptr<exceptional> p2(p1));
  EXPECT_ANY_THROW(static_ptr<exceptional> p2(std::move(p1)));
}
