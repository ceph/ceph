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
#include "common/async/lease.h"

#include <optional>
#include <utility>
#include <vector>
#include <boost/asio/any_completion_executor.hpp>
#include <boost/asio/append.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <gtest/gtest.h>
#include "common/async/co_waiter.h"
#include "common/async/yield_waiter.h"

namespace ceph::async {

namespace asio = boost::asio;
using executor_type = asio::any_io_executor;

using namespace std::chrono_literals;

enum class event { acquire, renew, release };

// a LockClient that logs events and injects delays and exceptions
class MockClient : public LockClient {
  Handler waiter;
  using Work = asio::executor_work_guard<asio::any_completion_executor>;
  std::optional<Work> work;
 public:
  std::vector<event> events; // tracks the sequence of LockClient calls

  void acquire(ceph::timespan, Handler handler) override {
    events.push_back(event::acquire);
    work.emplace(asio::make_work_guard(handler));
    waiter = std::move(handler);
  }
  void renew(ceph::timespan, Handler handler) override {
    events.push_back(event::renew);
    work.emplace(asio::make_work_guard(handler));
    waiter = std::move(handler);
  }
  void release(Handler handler) override {
    events.push_back(event::release);
    work.emplace(asio::make_work_guard(handler));
    waiter = std::move(handler);
  }

  void complete(boost::system::error_code ec = {}) {
    auto w = std::move(*work);
    work = std::nullopt;
    asio::dispatch(asio::append(std::move(waiter), ec));
  }
};

// a MockClient that supports per-op cancellation
class CancelableMockClient : public MockClient {
  struct op_cancellation {
    MockClient* self;
    op_cancellation(MockClient* self) : self(self) {}
    void operator()(asio::cancellation_type_t type) {
      if (type != asio::cancellation_type::none) {
        self->complete(make_error_code(asio::error::operation_aborted));
      }
    }
  };
 public:
  void acquire(ceph::timespan dur, Handler handler) override {
    auto slot = get_associated_cancellation_slot(handler);
    if (slot.is_connected()) {
      slot.template emplace<op_cancellation>(this);
    }
    MockClient::acquire(dur, std::move(handler));
  }
  void renew(ceph::timespan dur, Handler handler) override {
    auto slot = get_associated_cancellation_slot(handler);
    if (slot.is_connected()) {
      slot.template emplace<op_cancellation>(this);
    }
    MockClient::renew(dur, std::move(handler));
  }
  void release(Handler handler) override {
    auto slot = get_associated_cancellation_slot(handler);
    if (slot.is_connected()) {
      slot.template emplace<op_cancellation>(this);
    }
    MockClient::release(std::move(handler));
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


TEST(co_spawn, return_void)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  auto cr = [] () -> asio::awaitable<void> { co_return; };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  locker.complete(); // unblock release

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

TEST(spawn, return_void)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::spawn(ex, [&locker] (asio::yield_context yield) {
        with_lease(locker, 30s, yield, [] (asio::yield_context) {});
      }, capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  locker.complete(); // unblock release

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result);
}

TEST(co_spawn, return_value)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  using ptr = std::unique_ptr<int>; // test a move-only return type
  auto cr = [] () -> asio::awaitable<ptr> { co_return std::make_unique<int>(42); };

  using result_type = std::tuple<std::exception_ptr, ptr>;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until release blocks
  EXPECT_FALSE(result);
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  locker.complete(); // unblock release

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(std::get<0>(*result));
  ASSERT_TRUE(std::get<1>(*result));
  EXPECT_EQ(42, *std::get<1>(*result));
}

TEST(spawn, return_value)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  /// using ptr = std::unique_ptr<int>;
  // XXX: spawn() doesn't support move-only return values, see
  // https://github.com/chriskohlhoff/asio/issues/1543
  using ptr = std::shared_ptr<int>;
  auto cr = [] (asio::yield_context) { return std::make_shared<int>(42); };

  using result_type = std::tuple<std::exception_ptr, ptr>;
  std::optional<result_type> result;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        return with_lease(locker, 30s, yield, cr);
      }, capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until release blocks
  EXPECT_FALSE(result);
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  locker.complete(); // unblock release

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(std::get<0>(*result));
  ASSERT_TRUE(std::get<1>(*result));
  EXPECT_EQ(42, *std::get<1>(*result));
}

TEST(co_spawn, spawned_exception)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  auto cr = [] () -> asio::awaitable<void> {
    throw std::domain_error{"oops"};
    co_return;
  };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  locker.complete(); // unblock release

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  EXPECT_THROW(std::rethrow_exception(*result), std::domain_error);
}

TEST(spawn, spawned_exception)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  auto cr = [] (asio::yield_context) { throw std::domain_error{"oops"}; };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 30s, yield, cr);
      }, capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  locker.complete(); // unblock release

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  EXPECT_THROW(std::rethrow_exception(*result), std::domain_error);
}

TEST(co_spawn, acquire_exception)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  auto cr = [] () -> asio::awaitable<void> { co_return; };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(make_error_code(std::errc::invalid_argument));

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), std::errc::invalid_argument);
  } catch (const std::exception& e) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(spawn, acquire_exception)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  auto cr = [] (asio::yield_context) {};

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 30s, yield, cr);
      }, capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(make_error_code(std::errc::invalid_argument));

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), std::errc::invalid_argument);
  } catch (const std::exception& e) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(co_spawn, acquire_shutdown)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  auto cr = [] () -> asio::awaitable<void> { co_return; };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());
  // shut down before acquire completes
}

TEST(spawn, acquire_shutdown)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  auto cr = [] (asio::yield_context) {};

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 30s, yield, cr);
      }, capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());
  // shut down before acquire completes
}

TEST(co_spawn, acquire_cancel_supported)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = CancelableMockClient{};

  auto cr = [] () -> asio::awaitable<void> { co_return; };

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

TEST(spawn, acquire_cancel_supported)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = CancelableMockClient{};

  auto cr = [] (asio::yield_context) {};

  asio::cancellation_signal signal;
  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 30s, yield, cr);
      }, capture(signal, result));

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

TEST(co_spawn, acquire_cancel)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  auto cr = [] () -> asio::awaitable<void> { co_return; };

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

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  locker.complete();

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception& e) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(spawn, acquire_cancel)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  auto cr = [] (asio::yield_context) {};

  asio::cancellation_signal signal;
  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 30s, yield, cr);
      }, capture(signal, result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  // cancel before acquire completes
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll();
  EXPECT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  locker.complete();

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

TEST(co_spawn, acquired_shutdown)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::co_waiter<void, executor_type> waiter;
  auto cr = waiter.get(); // cr is a wait that never completes

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 10ms, std::move(cr)), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // start cr and renewal timer
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  // shut down before renewal timer
}

TEST(spawn, acquired_shutdown)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::yield_waiter<void> waiter;
  auto cr = [&waiter] (asio::yield_context yield) { waiter.async_wait(yield); };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 10ms, yield, cr);
      }, capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // start cr and renewal timer
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  // shut down before renewal timer
}

TEST(co_spawn, acquired_cancel)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::co_waiter<void, executor_type> waiter;
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

  locker.complete(); // unblock acquire

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

TEST(spawn, acquired_cancel)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::yield_waiter<void> waiter;
  auto cr = [&waiter] (asio::yield_context yield) { waiter.async_wait(yield); };

  asio::cancellation_signal signal;
  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 10ms, yield, cr);
      }, capture(signal, result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

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

TEST(co_spawn, renew_exception)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::co_waiter<void, executor_type> waiter;
  auto cr = waiter.get(); // cr is a wait that never completes

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 10ms, std::move(cr)), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until renew timer blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  ctx.run_one(); // wait ~5ms for renew timer
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::renew, locker.events.back());

  // inject an exception on renew
  locker.complete(make_error_code(std::errc::invalid_argument));

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const lease_aborted& e) {
    EXPECT_EQ(e.code(), std::errc::invalid_argument);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, lease_aborted);
  }
}

TEST(spawn, renew_exception)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::yield_waiter<void> waiter;
  auto cr = [&waiter] (asio::yield_context yield) { waiter.async_wait(yield); };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 10ms, yield, cr);
      }, capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until renew timer blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  ctx.run_one(); // wait ~5ms for renew timer
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::renew, locker.events.back());

  // inject an exception on renew
  locker.complete(make_error_code(std::errc::invalid_argument));

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const lease_aborted& e) {
    EXPECT_EQ(e.code(), std::errc::invalid_argument);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, lease_aborted);
  }
}

TEST(co_spawn, renew_after_timeout)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::co_waiter<void, executor_type> waiter;
  auto cr = waiter.get(); // cr is a wait that never completes

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 10ms, std::move(cr)), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

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

  locker.complete(); // unblock renew

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(3, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  locker.complete(); // unblock release

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const lease_aborted& e) {
    EXPECT_EQ(e.code(), boost::system::errc::timed_out);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, lease_aborted);
  }
}

TEST(spawn, renew_after_timeout)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::yield_waiter<void> waiter;
  auto cr = [&waiter] (asio::yield_context yield) { waiter.async_wait(yield); };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 10ms, yield, cr);
      }, capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

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

  locker.complete(); // unblock renew

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(3, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  locker.complete(); // unblock release

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const lease_aborted& e) {
    EXPECT_EQ(e.code(), boost::system::errc::timed_out);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, lease_aborted);
  }
}

TEST(co_spawn, renew_exception_after_timeout)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::co_waiter<void, executor_type> waiter;
  auto cr = waiter.get(); // cr is a wait that never completes

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 10ms, std::move(cr)), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

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
  locker.complete(make_error_code(std::errc::invalid_argument));

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const lease_aborted& e) {
    EXPECT_EQ(e.code(), boost::system::errc::timed_out);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, lease_aborted);
  }
}

TEST(spawn, renew_exception_after_timeout)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::yield_waiter<void> waiter;
  auto cr = [&waiter] (asio::yield_context yield) { waiter.async_wait(yield); };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 10ms, yield, cr);
      }, capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

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
  locker.complete(make_error_code(std::errc::invalid_argument));

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const lease_aborted& e) {
    EXPECT_EQ(e.code(), boost::system::errc::timed_out);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, lease_aborted);
  }
}

TEST(co_spawn, renew_cancel_after_timeout_supported)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = CancelableMockClient{};

  ceph::async::co_waiter<void, executor_type> waiter;
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

  locker.complete(); // unblock acquire

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
  } catch (const lease_aborted& e) {
    EXPECT_EQ(e.code(), boost::system::errc::timed_out);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, lease_aborted);
  }
}

TEST(spawn, renew_cancel_after_timeout_supported)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = CancelableMockClient{};

  ceph::async::yield_waiter<void> waiter;
  auto cr = [&waiter] (asio::yield_context yield) { waiter.async_wait(yield); };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::cancellation_signal signal;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 10ms, yield, cr);
      }, capture(signal, result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

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
  } catch (const lease_aborted& e) {
    EXPECT_EQ(e.code(), boost::system::errc::timed_out);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, lease_aborted);
  }
}

TEST(co_spawn, renew_cancel_after_timeout)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::co_waiter<void, executor_type> waiter;
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

  locker.complete(); // unblock acquire

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

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  locker.complete();

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_EQ(2, locker.events.size()); // no release
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const lease_aborted& e) {
    EXPECT_EQ(e.code(), boost::system::errc::timed_out);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, lease_aborted);
  }
}

TEST(spawn, renew_cancel_after_timeout)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::yield_waiter<void> waiter;
  auto cr = [&waiter] (asio::yield_context yield) { waiter.async_wait(yield); };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::cancellation_signal signal;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 10ms, yield, cr);
      }, capture(signal, result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

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

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  locker.complete();

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_EQ(2, locker.events.size()); // no release
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const lease_aborted& e) {
    EXPECT_EQ(e.code(), boost::system::errc::timed_out);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, lease_aborted);
  }
}

TEST(co_spawn, renew_shutdown)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::co_waiter<void, executor_type> waiter;
  auto cr = waiter.get(); // cr is a wait that never completes

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 10ms, std::move(cr)), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

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

TEST(spawn, renew_shutdown)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::yield_waiter<void> waiter;
  auto cr = [&waiter] (asio::yield_context yield) { waiter.async_wait(yield); };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 10ms, yield, cr);
      }, capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

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

TEST(co_spawn, renew_cancel)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::co_waiter<void, executor_type> waiter;
  auto cr = waiter.get(); // cr is a wait that never completes

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::cancellation_signal signal;
  asio::co_spawn(ex, with_lease(locker, 50ms, std::move(cr)),
                 capture(signal, result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until renew timer blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  ctx.run_one(); // wait ~25ms for renew timer
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::renew, locker.events.back());

  // cancel before renew completes
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  locker.complete();

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result); // throws on cancellation
  try {
    std::rethrow_exception(*result);
  } catch (const lease_aborted& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, lease_aborted);
  }
}

TEST(spawn, renew_cancel)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  ceph::async::yield_waiter<void> waiter;
  auto cr = [&waiter] (asio::yield_context yield) { waiter.async_wait(yield); };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::cancellation_signal signal;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 50ms, yield, cr);
      }, capture(signal, result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until renew timer blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  ctx.run_one(); // wait ~25ms for renew timer
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::renew, locker.events.back());

  // cancel before renew completes
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  locker.complete();

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result); // throws on cancellation
  try {
    std::rethrow_exception(*result);
  } catch (const lease_aborted& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, lease_aborted);
  }
}

TEST(co_spawn, release_exception)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  auto cr = [] () -> asio::awaitable<void> { co_return; };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  // inject an exception on release
  locker.complete(make_error_code(std::errc::invalid_argument));

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result); // release exceptions are ignored
}

TEST(spawn, release_exception)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  auto cr = [] (asio::yield_context) {};

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 30s, yield, cr);
      }, capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  // inject an exception on release
  locker.complete(make_error_code(std::errc::invalid_argument));

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  EXPECT_FALSE(*result); // release exceptions are ignored
}

TEST(co_spawn, release_shutdown)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  auto cr = [] () -> asio::awaitable<void> { co_return; };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());
  // shut down before release completes
}

TEST(spawn, release_shutdown)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  auto cr = [] (asio::yield_context) {};

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 30s, yield, cr);
      }, capture(result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());
  // shut down before release completes
}

TEST(co_spawn, release_cancel_supported)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = CancelableMockClient{};

  auto cr = [] () -> asio::awaitable<void> { co_return; };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::cancellation_signal signal;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(signal, result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

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

TEST(spawn, release_cancel_supported)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = CancelableMockClient{};

  auto cr = [] (asio::yield_context) {};

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::cancellation_signal signal;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 30s, yield, cr);
      }, capture(signal, result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

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

TEST(co_spawn, release_cancel)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  auto cr = [] () -> asio::awaitable<void> { co_return; };

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::cancellation_signal signal;
  asio::co_spawn(ex, with_lease(locker, 30s, cr()), capture(signal, result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  // cancel before release completes
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  locker.complete();

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_FALSE(*result); // exception is ignored
}

TEST(spawn, release_cancel)
{
  asio::io_context ctx;
  executor_type ex = ctx.get_executor();
  auto locker = MockClient{};

  auto cr = [] (asio::yield_context) {};

  using result_type = std::exception_ptr;
  std::optional<result_type> result;
  asio::cancellation_signal signal;
  asio::spawn(ex, [&] (asio::yield_context yield) {
        with_lease(locker, 30s, yield, cr);
      }, capture(signal, result));

  ctx.poll(); // run until acquire blocks
  ASSERT_FALSE(ctx.stopped());
  ASSERT_EQ(1, locker.events.size());
  EXPECT_EQ(event::acquire, locker.events.back());

  locker.complete(); // unblock acquire

  ctx.poll(); // run until release blocks
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);
  ASSERT_EQ(2, locker.events.size());
  EXPECT_EQ(event::release, locker.events.back());

  // cancel before release completes
  signal.emit(asio::cancellation_type::terminal);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_FALSE(result);

  locker.complete();

  ctx.poll(); // run to completion
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_FALSE(*result); // exception is ignored
}

} // namespace ceph::async
