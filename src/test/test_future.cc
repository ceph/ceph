// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (c) 2017 Casey Bodley <cbodley@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "common/use_future.hpp"
#include <boost/asio.hpp>
#include <boost/pool/pool_alloc.hpp>
#include <gtest/gtest.h>

using namespace ceph;

// dummy type that implements move and copy
struct move_copy {
  int value;
  move_copy(int value) : value(value) {}
};
bool operator==(const move_copy& lhs, const move_copy& rhs) {
  return lhs.value == rhs.value;
}

// dummy type that implements move without copy
struct move_no_copy {
  int value;
  move_no_copy(int value) : value(value) {}
  move_no_copy(const move_no_copy&) = delete;
  move_no_copy& operator=(const move_no_copy&) = delete;
  move_no_copy(move_no_copy&&) = default;
  move_no_copy& operator=(move_no_copy&&) = default;
};
bool operator==(const move_no_copy& lhs, const move_no_copy& rhs) {
  return lhs.value == rhs.value;
}

// TYPED_TEST cases will test each of these types
using FutureTypes = ::testing::Types<void, int, move_copy, move_no_copy>;


// promise test fixture
template <typename T>
struct Promise : public ::testing::Test {
  promise<T> p;
  void set_value() { p.set_value(0); }
  void expect_get(future<T>& f) { EXPECT_EQ(0, f.get()); }
};
// void specialization
template<>
struct Promise<void> : public ::testing::Test {
  promise<void> p;
  void set_value() { p.set_value(); }
  void expect_get(future<void>& f) { EXPECT_NO_THROW(f.get()); }
};
TYPED_TEST_CASE(Promise, FutureTypes);

TYPED_TEST(Promise, SetValue)
{
  auto f = this->p.get_future();
  EXPECT_FALSE(f.is_ready());
  EXPECT_FALSE(f.has_value());
  EXPECT_FALSE(f.has_exception());
  EXPECT_NO_THROW(this->set_value());
  EXPECT_TRUE(f.is_ready());
  EXPECT_TRUE(f.has_value());
  EXPECT_FALSE(f.has_exception());
  EXPECT_NO_THROW(this->expect_get(f));
  EXPECT_THROW(this->set_value(), boost::promise_already_satisfied);
}

TYPED_TEST(Promise, SetValueNoFuture)
{
  EXPECT_NO_THROW(this->set_value());
  EXPECT_THROW(this->set_value(), boost::promise_already_satisfied);
}

TYPED_TEST(Promise, SetValueDestroyedFuture)
{
  {
    auto f = this->p.get_future();
    EXPECT_FALSE(f.is_ready());
    EXPECT_FALSE(f.has_value());
    EXPECT_FALSE(f.has_exception());
  }
  EXPECT_NO_THROW(this->set_value());
  EXPECT_THROW(this->set_value(), boost::promise_already_satisfied);
}

TYPED_TEST(Promise, SetException)
{
  auto f = this->p.get_future();
  this->p.set_exception(std::exception());
  ASSERT_TRUE(f.is_ready());
  EXPECT_FALSE(f.has_value());
  EXPECT_TRUE(f.has_exception());
  EXPECT_THROW(f.get(), std::exception);
}

TYPED_TEST(Promise, SetExceptionPtr)
{
  auto f = this->p.get_future();
  try {
    throw std::exception();
  } catch (...) {
    this->p.set_exception(boost::current_exception());
  }
  ASSERT_TRUE(f.is_ready());
  EXPECT_FALSE(f.has_value());
  EXPECT_TRUE(f.has_exception());
  EXPECT_THROW(f.get(), std::exception);
}

// future test fixture
template <typename T>
struct Future : public ::testing::Test {
  static future<T> make_ready() { return make_ready_future<T>(0); }
  static future<T> make(launch policy) {
    return boost::async(policy, [] { return T(0); });
  }
  static void expect_get(future<T>& f) { EXPECT_EQ(0, f.get()); }
};
// void specialization
template<>
struct Future<void> : public ::testing::Test {
  static future<void> make_ready() { return make_ready_future(); }
  static future<void> make(launch policy) {
    return boost::async(policy, [] {});
  }
  static void expect_get(future<void>& f) { EXPECT_NO_THROW(f.get()); }
};
TYPED_TEST_CASE(Future, FutureTypes);

TYPED_TEST(Future, MakeReady)
{
  auto f = this->make_ready();
  EXPECT_TRUE(f.valid());
  EXPECT_TRUE(f.is_ready());
  EXPECT_TRUE(f.has_value());
  EXPECT_FALSE(f.has_exception());
  EXPECT_NO_THROW(this->expect_get(f));
}

TYPED_TEST(Future, MakeExceptional)
{
  auto f = make_exceptional_future<TypeParam>(std::exception());
  EXPECT_TRUE(f.valid());
  EXPECT_TRUE(f.is_ready());
  EXPECT_FALSE(f.has_value());
  EXPECT_TRUE(f.has_exception());
  EXPECT_THROW(f.get(), std::exception);
}

TYPED_TEST(Future, MakeExceptionalPtr)
{
  boost::exception_ptr eptr;
  try {
    throw std::exception();
  } catch (...) {
    eptr = boost::current_exception();
  }
  auto f = make_exceptional_future<TypeParam>(eptr);
  EXPECT_TRUE(f.valid());
  EXPECT_TRUE(f.is_ready());
  EXPECT_FALSE(f.has_value());
  EXPECT_TRUE(f.has_exception());
  EXPECT_THROW(f.get(), std::exception);
}

TYPED_TEST(Future, MakeAsync)
{
  auto f = this->make(launch::async);
  EXPECT_TRUE(f.valid());
  EXPECT_NO_THROW(this->expect_get(f));
}

TYPED_TEST(Future, MakeDeferred)
{
  auto f = this->make(launch::deferred);
  EXPECT_TRUE(f.valid());
  EXPECT_FALSE(f.is_ready());
  EXPECT_FALSE(f.has_value());
  EXPECT_FALSE(f.has_exception());
  EXPECT_NO_THROW(this->expect_get(f));
}

using tid = boost::thread::id;
constexpr auto &this_tid = boost::this_thread::get_id;

// continuation that returns the id of its thread of execution
tid get_thread_id(future<void> f) noexcept { return this_tid(); }

TEST(Future, ReadyThenValue)
{
  auto f1 = make_ready_future();
  auto f2 = f1.then(&get_thread_id);
  EXPECT_FALSE(f1.valid());
  EXPECT_TRUE(f2.valid());
  // async continuation runs in different thread
  EXPECT_NE(this_tid(), f2.get());
}

TEST(Future, ReadyThenAsyncValue)
{
  auto f1 = make_ready_future();
  auto f2 = f1.then(launch::async, &get_thread_id);
  EXPECT_FALSE(f1.valid());
  EXPECT_TRUE(f2.valid());
  // async continuation runs in different thread
  EXPECT_NE(this_tid(), f2.get());
}

TEST(Future, ReadyThenDeferredValue)
{
  auto f1 = make_ready_future();
  auto f2 = f1.then(launch::deferred, &get_thread_id);
  EXPECT_FALSE(f1.valid());
  EXPECT_TRUE(f2.valid());
  EXPECT_FALSE(f2.is_ready());
  EXPECT_FALSE(f2.has_value());
  boost::thread thread([&f2] {
    EXPECT_NO_THROW({
      // deferred continuation runs in same thread as get()
      EXPECT_EQ(this_tid(), f2.get());
    });
  });
  thread.join();
}

// returns a tuple of the given thread id and current thread id
std::tuple<tid, tid> make_tid_tuple(future<tid> f)
{
  return std::make_tuple(f.get(), this_tid());
}

TEST(Future, DeferredThenValue)
{
  auto f1 = boost::async(launch::deferred, [] { return this_tid(); });
  auto f2 = f1.then(&make_tid_tuple);
  EXPECT_FALSE(f1.valid());
  EXPECT_TRUE(f2.valid());
  EXPECT_FALSE(f2.is_ready());
  EXPECT_FALSE(f2.has_value());
  boost::thread thread([&f2] {
    EXPECT_NO_THROW({
      const auto& tids = f2.get();
      // deferred continuations run in same thread as get()
      EXPECT_EQ(this_tid(), std::get<0>(tids));
      EXPECT_EQ(this_tid(), std::get<1>(tids));
    });
  });
  thread.join();
}

TEST(Future, DeferredThenDeferredValue)
{
  auto f1 = boost::async(launch::deferred, [] { return this_tid(); });
  auto f2 = f1.then(launch::deferred, &make_tid_tuple);
  EXPECT_FALSE(f1.valid());
  EXPECT_TRUE(f2.valid());
  EXPECT_FALSE(f2.is_ready());
  EXPECT_FALSE(f2.has_value());
  boost::thread thread([&f2] {
    EXPECT_NO_THROW({
      const auto& tids = f2.get();
      // deferred continuations run in same thread as get()
      EXPECT_EQ(this_tid(), std::get<0>(tids));
      EXPECT_EQ(this_tid(), std::get<1>(tids));
    });
  });
  thread.join();
}

TEST(Future, AsyncThenValue)
{
  auto f1 = boost::async(launch::async, [] { return this_tid(); });
  auto f2 = f1.then(&make_tid_tuple);
  EXPECT_FALSE(f1.valid());
  EXPECT_TRUE(f2.valid());
  EXPECT_NO_THROW({
    const auto& tids = f2.get();
    EXPECT_NE(this_tid(), std::get<0>(tids));
    EXPECT_NE(this_tid(), std::get<1>(tids));
    // tid0 and tid1 may be the same
  });
}

TEST(Future, AsyncThenAsyncValue)
{
  auto f1 = boost::async(launch::async, [] { return this_tid(); });
  auto f2 = f1.then(launch::async, &make_tid_tuple);
  EXPECT_FALSE(f1.valid());
  EXPECT_TRUE(f2.valid());
  EXPECT_NO_THROW({
    const auto& tids = f2.get();
    EXPECT_NE(this_tid(), std::get<0>(tids));
    EXPECT_NE(this_tid(), std::get<1>(tids));
    // tid0 and tid1 may be the same
  });
}

TEST(Future, AsyncThenDeferredValue)
{
  auto f1 = boost::async(launch::async, [] { return this_tid(); });
  auto f2 = f1.then(launch::deferred, &make_tid_tuple);
  EXPECT_FALSE(f1.valid());
  EXPECT_TRUE(f2.valid());
  EXPECT_FALSE(f2.is_ready());
  EXPECT_FALSE(f2.has_value());
  const auto& main_tid = this_tid();
  boost::thread thread([&] {
    EXPECT_NO_THROW({
      const auto& tids = f2.get();
      EXPECT_NE(main_tid, std::get<0>(tids));
      EXPECT_EQ(this_tid(), std::get<1>(tids));
      // tid0 and tid1 may be the same
    });
  });
  thread.join();
}

TEST(Future, PromiseThenValue)
{
  promise<void> p;
  auto f1 = p.get_future();
  auto f2 = f1.then(&get_thread_id);
  EXPECT_FALSE(f1.valid());
  EXPECT_TRUE(f2.valid());
  EXPECT_FALSE(f2.is_ready());
  EXPECT_FALSE(f2.has_value());
  EXPECT_NO_THROW({
    p.set_value();
    EXPECT_NE(this_tid(), f2.get());
  });
}

TEST(Future, PromiseThenAsyncValue)
{
  promise<void> p;
  auto f1 = p.get_future();
  auto f2 = f1.then(launch::async, &get_thread_id);
  EXPECT_FALSE(f1.valid());
  EXPECT_TRUE(f2.valid());
  EXPECT_FALSE(f2.is_ready());
  EXPECT_FALSE(f2.has_value());
  EXPECT_NO_THROW({
    p.set_value();
    EXPECT_NE(this_tid(), f2.get());
  });
}

TEST(Future, PromiseThenDeferredValue)
{
  promise<void> p;
  auto f1 = p.get_future();
  auto f2 = f1.then(launch::deferred, &get_thread_id);
  EXPECT_FALSE(f1.valid());
  EXPECT_TRUE(f2.valid());
  EXPECT_FALSE(f2.is_ready());
  EXPECT_FALSE(f2.has_value());
  boost::thread thread([&] {
    EXPECT_NO_THROW({
      p.set_value();
      EXPECT_EQ(this_tid(), f2.get());
    });
  });
  thread.join();
}

/// a synchronous, inline executor
class inline_executor final {
 public:
  template <typename Closure>
  static void submit(Closure&& c) { std::forward<Closure>(c)(); }
  bool try_executing_one() { return false; }
  void close() {}
  bool closed() { return false; }
};

TEST(Future, ThenExecutor)
{
  inline_executor executor;
  auto f1 = make_ready_future().then(executor, &get_thread_id);
  auto f2 = f1.then(executor, &make_tid_tuple);
  EXPECT_NO_THROW({
    const auto& tids = f2.get();
    EXPECT_EQ(this_tid(), std::get<0>(tids));
    EXPECT_EQ(this_tid(), std::get<1>(tids));
  });
}

TYPED_TEST(Future, ThenThrow)
{
  auto f1 = this->make(launch::deferred);
  auto f2 = f1.then([] (future<TypeParam> f) -> TypeParam {
    throw std::exception();
  });
  EXPECT_THROW(f2.get(), std::exception);
}

TYPED_TEST(Future, ThenFuture)
{
  auto f1 = this->make_ready();
  auto f2 = f1.then([&] (future<TypeParam> f) {
    return this->make_ready();
  }).unwrap(); // unwrap future<future<T>>
  EXPECT_NO_THROW(this->expect_get(f2));
}

TYPED_TEST(Future, ThenAsyncFuture)
{
  auto f1 = this->make_ready();
  auto f2 = f1.then(launch::async, [&] (future<TypeParam> f) {
    return this->make_ready();
  }).unwrap();
  EXPECT_NO_THROW(this->expect_get(f2));
}

TYPED_TEST(Future, ThenDeferredFuture)
{
  auto f1 = this->make(launch::deferred);
  auto f2 = f1.then([&] (future<TypeParam> f) {
    return this->make_ready();
  });
  f2.wait(); // XXX: this is required before unwrap()
  auto f3 = f2.unwrap();
  EXPECT_NO_THROW(this->expect_get(f3));
}

TYPED_TEST(Future, ThenExceptionalFuture)
{
  auto f1 = this->make_ready();
  auto f2 = f1.then([] (future<TypeParam> f) {
    return make_exceptional_future<TypeParam>(std::exception());
  }).unwrap();
  EXPECT_THROW(f2.get(), std::exception);
}

TYPED_TEST(Future, ThenAsyncExceptionalFuture)
{
  auto f1 = this->make_ready();
  auto f2 = f1.then(launch::async, [] (future<TypeParam> f) {
    return make_exceptional_future<TypeParam>(std::exception());
  }).unwrap();
  EXPECT_THROW(f2.get(), std::exception);
}

TYPED_TEST(Future, ThenDeferredExceptionalFuture)
{
  auto f1 = this->make_ready();
  auto f2 = f1.then(launch::deferred, [] (future<TypeParam> f) {
    return make_exceptional_future<TypeParam>(std::exception());
  });
  f2.wait(); // XXX: this is required before unwrap()
  auto f3 = f2.unwrap();
  EXPECT_THROW(f3.get(), std::exception);
}

TEST(shared_future, then)
{
  promise<int> p;
  shared_future<int> s1 = p.get_future();
  shared_future<int> s2 = s1;
  future<int> f1 = s1.then([] (shared_future<int> f) {
    return f.get() + 1;
  });
  ASSERT_TRUE(s1.valid());
  ASSERT_TRUE(f1.valid());
  future<int> f2 = s2.then([] (shared_future<int> f) {
    return make_ready_future(f.get() + 1);
  }).unwrap();
  ASSERT_TRUE(s2.valid());
  ASSERT_TRUE(f2.valid());
  EXPECT_FALSE(f1.is_ready());
  EXPECT_FALSE(f2.is_ready());
  p.set_value(1);
  f1.wait();
  f2.wait();
  ASSERT_TRUE(s1.is_ready());
  ASSERT_TRUE(s2.is_ready());
  ASSERT_TRUE(f1.is_ready());
  ASSERT_TRUE(f2.is_ready());
  EXPECT_EQ(1, s1.get());
  EXPECT_EQ(1, s2.get());
  EXPECT_EQ(2, f1.get());
  EXPECT_EQ(2, f2.get());
}

TEST(shared_future, then_void)
{
  promise<void> p;
  shared_future<void> s1 = p.get_future();
  shared_future<void> s2 = s1;
  future<void> f1 = s1.then([] (shared_future<void> f) {});
  ASSERT_TRUE(s1.valid());
  ASSERT_TRUE(f1.valid());
  future<void> f2 = s2.then([] (shared_future<void> f) {
    return make_ready_future();
  }).unwrap();
  ASSERT_TRUE(s2.valid());
  ASSERT_TRUE(f2.valid());
  EXPECT_FALSE(f1.is_ready());
  EXPECT_FALSE(f2.is_ready());
  p.set_value();
  f1.wait();
  f2.wait();
  ASSERT_TRUE(s1.is_ready());
  ASSERT_TRUE(s2.is_ready());
  ASSERT_TRUE(f1.is_ready());
  ASSERT_TRUE(f2.is_ready());
  EXPECT_NO_THROW(s1.get());
  EXPECT_NO_THROW(s2.get());
  EXPECT_NO_THROW(f1.get());
  EXPECT_NO_THROW(f2.get());
}

TEST(future, when_all)
{
  promise<int> p1, p2;
  promise<void> p3;
  auto all = when_all(p1.get_future(), p2.get_future(), p3.get_future());
  ASSERT_TRUE(all.valid());
  EXPECT_FALSE(all.is_ready());
  p1.set_value(0);
  p2.set_value(1);
  p3.set_value();
  all.wait();
  ASSERT_TRUE(all.is_ready());
  auto futures = all.get();
  auto& f1 = std::get<0>(futures);
  auto& f2 = std::get<1>(futures);
  auto& f3 = std::get<2>(futures);
  ASSERT_TRUE(f1.is_ready());
  ASSERT_TRUE(f2.is_ready());
  ASSERT_TRUE(f3.is_ready());
  EXPECT_EQ(0, f1.get());
  EXPECT_EQ(1, f2.get());
  EXPECT_NO_THROW(f3.get());
}

TEST(future, when_any)
{
  promise<int> p1, p2;
  promise<void> p3;
  auto any = when_any(p1.get_future(), p2.get_future(), p3.get_future());
  ASSERT_TRUE(any.valid());
  EXPECT_FALSE(any.is_ready());
  p1.set_value(0);
  any.wait();
  ASSERT_TRUE(any.is_ready());
  auto futures = any.get();
  auto& f1 = std::get<0>(futures);
  auto& f2 = std::get<1>(futures);
  auto& f3 = std::get<2>(futures);
  ASSERT_TRUE(f1.is_ready());
  EXPECT_EQ(0, f1.get());
  EXPECT_FALSE(f2.is_ready());
  EXPECT_FALSE(f3.is_ready());
}

TEST(use_future, token)
{
  boost::asio::io_service io;
  // timer that completes immediately. doesn't start until io.run()
  boost::asio::deadline_timer timer(io, boost::posix_time::time_duration());
  // test that use_future works as a completion token to produce a future
  future<void> f = timer.async_wait(use_future);
  ASSERT_TRUE(f.valid());
  EXPECT_FALSE(f.is_ready());
  io.run();
  f.wait();
  ASSERT_TRUE(f.is_ready());
  EXPECT_NO_THROW(f.get());
}

TEST(use_future, allocator)
{
  boost::asio::io_service io;
  boost::asio::deadline_timer timer(io, boost::posix_time::time_duration());
  // test use_future with a custom allocator
  future<void> f = timer.async_wait(use_future[boost::pool_allocator<int>()]);
  ASSERT_TRUE(f.valid());
  EXPECT_FALSE(f.is_ready());
  io.run();
  f.wait();
  ASSERT_TRUE(f.is_ready());
  EXPECT_NO_THROW(f.get());
}
