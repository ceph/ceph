// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

//#define BOOST_ASIO_ENABLE_HANDLER_TRACKING
#include "sync/lease.h"

#include <utility>
#include <vector>
#include <gtest/gtest.h>
#include "common/async/co_waiter.h"

namespace rgw::sync {

using namespace std::chrono_literals;

// injects delays and exceptions into MockClient
using MockWaiter = ceph::async::co_waiter<void, default_executor>;

enum class event { acquire, renew, release };

// a LockClient that logs events and injects delays and exceptions
class MockClient : public LockClient {
  MockWaiter waiter;
 public:
  std::vector<event> events; // tracks the sequence of LockClient calls

  awaitable<void> acquire(ceph::timespan) override {
    events.push_back(event::acquire);
    co_await waiter.get();
  }
  awaitable<void> renew(ceph::timespan) override {
    events.push_back(event::renew);
    co_await waiter.get();
  }
  awaitable<void> release() override {
    events.push_back(event::release);
    co_await waiter.get();
  }

  void complete(std::exception_ptr eptr) {
    waiter.complete(eptr);
  }
};

template <typename T>
auto capture(std::optional<T>& opt)
{
  return [&opt] (T value) {
    opt = std::move(value);
  };
}

template <typename T>
auto capture(asio::cancellation_signal& signal, std::optional<T>& opt)
{
  return asio::bind_cancellation_slot(signal.slot(), capture(opt));
}

template <typename ...Args>
auto capture(std::optional<std::tuple<Args...>>& opt)
{
  return [&opt] (Args ...args) {
    opt = std::forward_as_tuple(std::forward<Args>(args)...);
  };
}


TEST(with_lease, return_void)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto cr = [] () -> awaitable<void> { co_return; };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(nullptr); // unblock acquire

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  locker.complete(nullptr); // unblock release

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

TEST(with_lease, return_value)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  using ptr = std::unique_ptr<int>; // test a move-only return type
  auto cr = [] () -> awaitable<ptr> { co_return std::make_unique<int>(42); };

  using result_type = std::tuple<std::exception_ptr, ptr>;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(nullptr); // unblock acquire

  ctx.poll(); // run until release blocks
  EXPECT_FALSE(result);
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  locker.complete(nullptr); // unblock release

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(std::get<0>(*result));
  ASSERT_TRUE(std::get<1>(*result));
  EXPECT_EQ(42, *std::get<1>(*result));
}

TEST(with_lease, spawned_exception)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto cr = [] () -> awaitable<void> {
    throw std::runtime_error{"oops"};
    co_return;
  };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(nullptr); // unblock acquire

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  locker.complete(nullptr); // unblock release

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  EXPECT_THROW(std::rethrow_exception(*result), std::runtime_error);
}

TEST(with_lease, acquire_exception)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto cr = [] () -> awaitable<void> { co_return; };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  EXPECT_THROW(std::rethrow_exception(*result), std::runtime_error);
}

TEST(with_lease, acquire_shutdown)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto cr = [] () -> awaitable<void> { co_return; };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());
  // shut down before acquire completes
}

TEST(with_lease, acquire_cancel)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto cr = [] () -> awaitable<void> { co_return; };

  asio::cancellation_signal signal;
  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(signal, result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  // cancel before acquire completes
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result); // throws on cancellation
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception& e) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(with_lease, acquired_shutdown)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto waiter = MockWaiter{};
  auto cr = waiter.get(); // cr is a wait that never completes

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 10ms, std::move(cr)), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(nullptr); // unblock acquire

  ctx.poll(); // start cr and renewal timer
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  // shut down before renewal timer
}

TEST(with_lease, acquired_cancel)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto waiter = MockWaiter{};
  auto cr = waiter.get(); // cr is a wait that never completes

  asio::cancellation_signal signal;
  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 10ms, std::move(cr)),
                 capture(signal, result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(nullptr); // unblock acquire

  ctx.poll(); // wait for acquire to finish
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  // cancel before renewal timer
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result); // throws on cancellation
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception& e) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(with_lease, renew_exception)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto waiter = MockWaiter{};
  auto cr = waiter.get(); // cr is a wait that never completes

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 10ms, std::move(cr)), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(nullptr); // unblock acquire

  ctx.poll(); // run until renew timer blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  ctx.run_one(); // wait ~5ms for renew timer
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::renew, locker.events.back());

  // inject an exception on renew
  locker.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  EXPECT_THROW(std::rethrow_exception(*result), std::runtime_error);
}

TEST(with_lease, renew_after_timeout)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto waiter = MockWaiter{};
  auto cr = waiter.get(); // cr is a wait that never completes

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 10ms, std::move(cr)), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(nullptr); // unblock acquire

  ctx.poll(); // run until renew timer blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  ctx.run_one(); // wait ~5ms for renew timer
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::renew, locker.events.back());

  ctx.run_one(); // wait for renew timeout
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  locker.complete(nullptr); // unblock renew

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(3, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  locker.complete(nullptr); // unblock release

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), boost::system::errc::timed_out);
  } catch (const std::exception& e) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(with_lease, renew_exception_after_timeout)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto waiter = MockWaiter{};
  auto cr = waiter.get(); // cr is a wait that never completes

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 10ms, std::move(cr)), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(nullptr); // unblock acquire

  ctx.poll(); // run until renew timer blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  ctx.run_one(); // wait ~5ms for renew timer
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::renew, locker.events.back());

  ctx.run_one(); // wait for renew timeout
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  // inject an exception on renew
  locker.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), boost::system::errc::timed_out);
  } catch (const std::exception& e) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(with_lease, renew_cancel_after_timeout)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto waiter = MockWaiter{};
  auto cr = waiter.get(); // cr is a wait that never completes

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::cancellation_signal signal;
  asio::co_spawn(ex, with_lease(locker, 10ms, std::move(cr)),
                 capture(signal, result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(nullptr); // unblock acquire

  ctx.poll(); // run until renew timer blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  ctx.run_one(); // wait ~5ms for renew timer
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::renew, locker.events.back());

  ctx.run_one(); // wait for renew timeout
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  // cancel before renew completes
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_EQ(2, locker.events.size()); // no release
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), boost::system::errc::timed_out);
  } catch (const std::exception& e) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(with_lease, renew_shutdown)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto waiter = MockWaiter{};
  auto cr = waiter.get(); // cr is a wait that never completes

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 10ms, std::move(cr)), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(nullptr); // unblock acquire

  ctx.poll(); // run until renew timer blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  ctx.run_one(); // wait ~5ms for renew timer
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::renew, locker.events.back());
  // shut down before renew completes
}

TEST(with_lease, renew_cancel)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto waiter = MockWaiter{};
  auto cr = waiter.get(); // cr is a wait that never completes

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::cancellation_signal signal;
  asio::co_spawn(ex, with_lease(locker, 10ms, std::move(cr)),
                 capture(signal, result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(nullptr); // unblock acquire

  ctx.poll(); // run until renew timer blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  ctx.run_one(); // wait ~5ms for renew timer
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::renew, locker.events.back());

  // cancel before renew completes
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result); // throws on cancellation
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception& e) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(with_lease, release_exception)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto cr = [] () -> awaitable<void> { co_return; };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(nullptr); // unblock acquire

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  // inject an exception on release
  locker.complete(std::make_exception_ptr(std::runtime_error{"oops"}));

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result); // release exceptions are ignored
}

TEST(with_lease, release_shutdown)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto cr = [] () -> awaitable<void> { co_return; };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(nullptr); // unblock acquire

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());
  // shut down before release completes
}

TEST(with_lease, release_cancel)
{
  boost::asio::io_context ctx;
  auto ex = default_executor{ctx.get_executor()};
  auto locker = MockClient{};

  auto cr = [] () -> awaitable<void> { co_return; };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::cancellation_signal signal;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(signal, result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(nullptr); // unblock acquire

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  // cancel before release completes
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_FALSE(*result); // exception is ignored
}

} // namespace rgw::sync
