// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "common/async/completion.h"
#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <optional>
#include <boost/intrusive/list.hpp>
#include <gtest/gtest.h>

namespace ceph::async {

using boost::system::error_code;

struct move_only {
  move_only() = default;
  move_only(move_only&&) = default;
  move_only& operator=(move_only&&) = default;
  move_only(const move_only&) = delete;
  move_only& operator=(const move_only&) = delete;
};

TEST(AsyncCompletion, BindHandler)
{
  auto h1 = [] (int i, char c) {};
  auto b1 = bind_handler(std::move(h1), 5, 'a');
  b1();
  const auto& c1 = b1;
  c1();
  std::move(b1)();

  // move-only types can be forwarded with 'operator() &&'
  auto h2 = [] (move_only&& m) {};
  auto b2 = bind_handler(std::move(h2), move_only{});
  std::move(b2)();

  // references bound with std::ref() can be passed to all operator() overloads
  auto h3 = [] (int& c) { c++; };
  int count = 0;
  auto b3 = bind_handler(std::move(h3), std::ref(count));
  EXPECT_EQ(0, count);
  b3();
  EXPECT_EQ(1, count);
  const auto& c3 = b3;
  c3();
  EXPECT_EQ(2, count);
  std::move(b3)();
  EXPECT_EQ(3, count);
}

TEST(AsyncCompletion, ForwardHandler)
{
  // move-only types can be forwarded with 'operator() &'
  auto h = [] (move_only&& m) {};
  auto b = bind_handler(std::move(h), move_only{});
  auto f = forward_handler(std::move(b));
  f();
}

TEST(AsyncCompletion, MoveOnly)
{
  boost::asio::io_context context;
  auto ex1 = context.get_executor();

  std::optional<error_code> ec1, ec2, ec3;
  std::optional<move_only> arg3;
  {
    // move-only user data
    using Completion = Completion<void(error_code), move_only>;
    auto c = Completion::create(ex1, [&ec1] (error_code ec) { ec1 = ec; });
    Completion::post(std::move(c), boost::asio::error::operation_aborted);
    EXPECT_FALSE(ec1);
  }
  {
    // move-only handler
    using Completion = Completion<void(error_code)>;
    auto c = Completion::create(ex1, [&ec2, m=move_only{}] (error_code ec) {
				       static_cast<void>(m);
				       ec2 = ec; });
    Completion::post(std::move(c), boost::asio::error::operation_aborted);
    EXPECT_FALSE(ec2);
  }
  {
    // move-only arg in signature
    using Completion = Completion<void(error_code, move_only)>;
    auto c = Completion::create(ex1, [&] (error_code ec, move_only m) {
        ec3 = ec;
        arg3 = std::move(m);
      });
    Completion::post(std::move(c), boost::asio::error::operation_aborted, move_only{});
    EXPECT_FALSE(ec3);
  }

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec1);
  ASSERT_TRUE(ec2);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec2);
  ASSERT_TRUE(ec3);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec3);
  ASSERT_TRUE(arg3);
}

TEST(AsyncCompletion, VoidCompletion)
{
  boost::asio::io_context context;
  auto ex1 = context.get_executor();

  using Completion = Completion<void(error_code)>;
  std::optional<error_code> ec1;

  auto c = Completion::create(ex1, [&ec1] (error_code ec) { ec1 = ec; });
  Completion::post(std::move(c), boost::asio::error::operation_aborted);

  EXPECT_FALSE(ec1);

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(ec1);
  EXPECT_EQ(boost::asio::error::operation_aborted, *ec1);
}

TEST(AsyncCompletion, CompletionList)
{
  boost::asio::io_context context;
  auto ex1 = context.get_executor();

  using T = AsBase<boost::intrusive::list_base_hook<>>;
  using Completion = Completion<void(), T>;
  boost::intrusive::list<Completion> completions;
  int completed = 0;
  for (int i = 0; i < 3; i++) {
    auto c = Completion::create(ex1, [&] { completed++; });
    completions.push_back(*c.release());
  }
  completions.clear_and_dispose([] (Completion *c) {
      Completion::post(std::unique_ptr<Completion>{c});
    });

  EXPECT_EQ(0, completed);

  context.poll();
  EXPECT_TRUE(context.stopped());

  EXPECT_EQ(3, completed);
}

TEST(AsyncCompletion, CompletionPair)
{
  boost::asio::io_context context;
  auto ex1 = context.get_executor();

  using T = std::pair<int, std::string>;
  using Completion = Completion<void(int, std::string), T>;

  std::optional<T> t;
  auto c = Completion::create(ex1, [&] (int first, std::string second) {
      t = T{first, std::move(second)};
    }, 2, "hello");

  auto data = std::move(c->user_data);
  Completion::post(std::move(c), data.first, std::move(data.second));

  EXPECT_FALSE(t);

  context.poll();
  EXPECT_TRUE(context.stopped());

  ASSERT_TRUE(t);
  EXPECT_EQ(2, t->first);
  EXPECT_EQ("hello", t->second);
}

TEST(AsyncCompletion, CompletionReference)
{
  boost::asio::io_context context;
  auto ex1 = context.get_executor();

  using Completion = Completion<void(int&)>;

  auto c = Completion::create(ex1, [] (int& i) { ++i; });

  int i = 42;
  Completion::post(std::move(c), std::ref(i));

  EXPECT_EQ(42, i);

  context.poll();
  EXPECT_TRUE(context.stopped());

  EXPECT_EQ(43, i);
}

struct throws_on_move {
  throws_on_move() = default;
  throws_on_move(throws_on_move&&) {
    throw std::runtime_error("oops");
  }
  throws_on_move& operator=(throws_on_move&&) {
    throw std::runtime_error("oops");
  }
  throws_on_move(const throws_on_move&) = default;
  throws_on_move& operator=(const throws_on_move&) = default;
};

TEST(AsyncCompletion, ThrowOnCtor)
{
  boost::asio::io_context context;
  auto ex1 = context.get_executor();
  {
    using Completion = Completion<void(int&)>;

    // throw on Handler move construction
    EXPECT_THROW(Completion::create(ex1, [t=throws_on_move{}] (int& i) {
					   static_cast<void>(t);
					   ++i; }),
                 std::runtime_error);
  }
  {
    using T = throws_on_move;
    using Completion = Completion<void(int&), T>;

    // throw on UserData construction
    EXPECT_THROW(Completion::create(ex1, [] (int& i) { ++i; }, throws_on_move{}),
                 std::runtime_error);
  }
}

TEST(AsyncCompletion, FreeFunctions)
{
  boost::asio::io_context context;
  auto ex1 = context.get_executor();

  auto c1 = create_completion<void(), void>(ex1, [] {});
  post(std::move(c1));

  auto c2 = create_completion<void(int), int>(ex1, [] (int) {}, 5);
  defer(std::move(c2), c2->user_data);

  context.poll();
  EXPECT_TRUE(context.stopped());
}

} // namespace ceph::async
