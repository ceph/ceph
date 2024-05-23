// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "common/async/spawn_throttle.h"

#include <optional>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/steady_timer.hpp>
#include <gtest/gtest.h>
#include "common/async/yield_waiter.h"

namespace ceph::async {

namespace asio = boost::asio;
using error_code = boost::system::error_code;

void rethrow(std::exception_ptr eptr)
{
  if (eptr) std::rethrow_exception(eptr);
}

auto capture(std::optional<std::exception_ptr>& eptr)
{
  return [&eptr] (std::exception_ptr e) { eptr = e; };
}

auto capture(asio::cancellation_signal& signal,
             std::optional<std::exception_ptr>& eptr)
{
  return asio::bind_cancellation_slot(signal.slot(), capture(eptr));
}

using namespace std::chrono_literals;

void wait_for(std::chrono::milliseconds dur, asio::yield_context yield)
{
  auto timer = asio::steady_timer{yield.get_executor(), dur};
  timer.async_wait(yield);
}

auto wait_for(std::chrono::milliseconds dur)
{
  return [dur] (asio::yield_context yield) { wait_for(dur, yield); };
}

auto wait_on(yield_waiter<void>& handler)
{
  return [&handler] (asio::yield_context yield) {
    handler.async_wait(yield);
  };
}


TEST(YieldGroupSync, wait_empty)
{
  auto throttle = spawn_throttle{null_yield, 2};
  throttle.wait();
}

TEST(YieldGroupSync, spawn_wait)
{
  int completed = 0;
  auto cr = [&] (asio::yield_context yield) {
    wait_for(1ms, yield);
    ++completed;
  };

  auto throttle = spawn_throttle{null_yield, 2};
  asio::spawn(throttle.get_executor(), cr, throttle);
  throttle.wait();

  EXPECT_EQ(1, completed);
}

TEST(YieldGroupSync, spawn_shutdown)
{
  auto throttle = spawn_throttle{null_yield, 2};
  asio::spawn(throttle.get_executor(), wait_for(1s), throttle);
}

TEST(YieldGroupSync, spawn_cancel_wait)
{
  int completed = 0;

  auto cr = [&] (asio::yield_context yield) {
    wait_for(1s, yield);
    ++completed;
  };

  auto throttle = spawn_throttle{null_yield, 2};
  asio::spawn(throttle.get_executor(), cr, throttle);
  throttle.cancel();
  EXPECT_THROW(throttle.wait(), boost::system::system_error);

  EXPECT_EQ(0, completed);
}

TEST(YieldGroupSync, spawn_cancel_wait_spawn_wait)
{
  int completed = 0;

  auto cr = [&] (asio::yield_context yield) {
    wait_for(1ms, yield);
    ++completed;
  };

  auto throttle = spawn_throttle{null_yield, 2};
  asio::spawn(throttle.get_executor(), cr, throttle);
  throttle.cancel();
  EXPECT_THROW(throttle.wait(), boost::system::system_error);
  asio::spawn(throttle.get_executor(), cr, throttle);
  throttle.wait();

  EXPECT_EQ(1, completed);
}

TEST(YieldGroupSync, spawn_over_limit)
{
  int concurrent = 0;
  int max_concurrent = 0;
  int completed = 0;

  auto cr = [&] (asio::yield_context yield) {
    ++concurrent;
    if (max_concurrent < concurrent) {
      max_concurrent = concurrent;
    }

    wait_for(1ms, yield);

    --concurrent;
    ++completed;
  };

  auto throttle = spawn_throttle{null_yield, 2};
  asio::spawn(throttle.get_executor(), cr, throttle);
  asio::spawn(throttle.get_executor(), cr, throttle);
  asio::spawn(throttle.get_executor(), cr, throttle); // blocks
  asio::spawn(throttle.get_executor(), cr, throttle); // blocks
  throttle.wait(); // blocks

  EXPECT_EQ(0, concurrent);
  EXPECT_EQ(2, max_concurrent);
  EXPECT_EQ(4, completed);
}

TEST(YieldGroupSync, spawn_cancel_on_error_none)
{
  int completed = 0;

  auto cr = [&] (asio::yield_context yield) {
    wait_for(10ms, yield);
    ++completed;
  };
  auto err = [] (asio::yield_context yield) {
    wait_for(0ms, yield);
    throw std::logic_error{"err"};
  };

  auto throttle = spawn_throttle{null_yield, 4, cancel_on_error::none};
  asio::spawn(throttle.get_executor(), cr, throttle);
  asio::spawn(throttle.get_executor(), cr, throttle);
  asio::spawn(throttle.get_executor(), err, throttle);
  asio::spawn(throttle.get_executor(), cr, throttle);
  EXPECT_THROW(throttle.wait(), std::logic_error);

  EXPECT_EQ(3, completed);
}

TEST(YieldGroupSync, spawn_cancel_on_error_after)
{
  int completed = 0;

  auto cr = [&] (asio::yield_context yield) {
    wait_for(10ms, yield);
    ++completed;
  };
  auto err = [] (asio::yield_context yield) {
    wait_for(0ms, yield);
    throw std::logic_error{"err"};
  };

  auto throttle = spawn_throttle{null_yield, 4, cancel_on_error::after};
  asio::spawn(throttle.get_executor(), cr, throttle);
  asio::spawn(throttle.get_executor(), cr, throttle);
  asio::spawn(throttle.get_executor(), err, throttle);
  asio::spawn(throttle.get_executor(), cr, throttle);
  EXPECT_THROW(throttle.wait(), std::logic_error);

  EXPECT_EQ(2, completed);
}

TEST(YieldGroupSync, spawn_cancel_on_error_all)
{
  int completed = 0;

  auto cr = [&] (asio::yield_context yield) {
    wait_for(1s, yield);
    ++completed;
  };
  auto err = [] (asio::yield_context yield) {
    wait_for(0ms, yield);
    throw std::logic_error{"err"};
  };

  auto throttle = spawn_throttle{null_yield, 4, cancel_on_error::all};
  asio::spawn(throttle.get_executor(), cr, throttle);
  asio::spawn(throttle.get_executor(), cr, throttle);
  asio::spawn(throttle.get_executor(), err, throttle);
  asio::spawn(throttle.get_executor(), cr, throttle);
  EXPECT_THROW(throttle.wait(), std::logic_error);

  EXPECT_EQ(0, completed);
}


TEST(YieldGroupAsync, wait_empty)
{
  asio::io_context ctx;
  asio::spawn(ctx, [] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 2};
      throttle.wait();
    }, rethrow);

  ctx.run();
}

TEST(YieldGroupAsync, spawn_wait)
{
  asio::io_context ctx;
  yield_waiter<void> waiter;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 2};
      asio::spawn(yield, wait_on(waiter), throttle);
      throttle.wait(); // blocks
    }, rethrow);

  ASSERT_FALSE(waiter);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter);

  waiter.complete(error_code{});

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
}

TEST(YieldGroupAsync, spawn_over_limit)
{
  asio::io_context ctx;
  yield_waiter<void> waiter1;
  yield_waiter<void> waiter2;
  yield_waiter<void> waiter3;
  yield_waiter<void> waiter4;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 2};
      asio::spawn(yield, wait_on(waiter1), throttle);
      asio::spawn(yield, wait_on(waiter2), throttle);
      asio::spawn(yield, wait_on(waiter3), throttle); // blocks
      asio::spawn(yield, wait_on(waiter4), throttle); // blocks
      throttle.wait(); // blocks
    }, rethrow);

  ASSERT_FALSE(waiter1);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter1);
  ASSERT_TRUE(waiter2);
  ASSERT_FALSE(waiter3);

  waiter1.complete(error_code{});

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter3);
  ASSERT_FALSE(waiter4);

  waiter3.complete(error_code{});

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter4);

  waiter2.complete(error_code{});

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  waiter4.complete(error_code{});

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());
}

TEST(YieldGroupAsync, spawn_shutdown)
{
  asio::io_context ctx;
  yield_waiter<void> waiter1;
  yield_waiter<void> waiter2;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 2};
      asio::spawn(yield, wait_on(waiter1), throttle);
      waiter2.async_wait(yield); // blocks
      // shut down while there's an outstanding child but throttle is not
      // waiting on spawn() or wait()
    }, rethrow);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(waiter1);
  EXPECT_TRUE(waiter2);
}

TEST(YieldGroupAsync, spawn_throttled_shutdown)
{
  asio::io_context ctx;
  yield_waiter<void> waiter1;
  yield_waiter<void> waiter2;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 1};
      asio::spawn(yield, wait_on(waiter1), throttle);
      asio::spawn(yield, wait_on(waiter2), throttle); // blocks
      // shut down while we're throttled on the second spawn
    }, rethrow);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(waiter1);
  EXPECT_FALSE(waiter2);
}

TEST(YieldGroupAsync, spawn_wait_shutdown)
{
  asio::io_context ctx;
  yield_waiter<void> waiter;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 1};
      asio::spawn(yield, wait_on(waiter), throttle);
      throttle.wait(); // blocks
      // shut down while we're wait()ing
    }, rethrow);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  EXPECT_TRUE(waiter);
}

TEST(YieldGroupAsync, spawn_throttled_error)
{
  asio::io_context ctx;
  yield_waiter<void> waiter1;
  yield_waiter<void> waiter2;

  std::optional<std::exception_ptr> result;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 1};
      asio::spawn(yield, wait_on(waiter1), throttle);
      asio::spawn(yield, wait_on(waiter2), throttle); // blocks
    }, capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter1);
  ASSERT_FALSE(waiter2);

  waiter1.complete(make_error_code(std::errc::no_such_file_or_directory));

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(YieldGroupAsync, spawn_throttled_signal)
{
  asio::io_context ctx;
  yield_waiter<void> waiter1;
  yield_waiter<void> waiter2;

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> result;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 1};
      asio::spawn(yield, wait_on(waiter1), throttle);
      asio::spawn(yield, wait_on(waiter2), throttle); // blocks
    }, capture(signal, result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter1);
  ASSERT_FALSE(waiter2);

  signal.emit(boost::asio::cancellation_type::terminal);

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(YieldGroupAsync, spawn_wait_error)
{
  asio::io_context ctx;
  yield_waiter<void> waiter;

  std::optional<std::exception_ptr> result;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 1};
      asio::spawn(yield, wait_on(waiter), throttle);
      throttle.wait(); // blocks
    }, capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter);

  waiter.complete(make_error_code(std::errc::no_such_file_or_directory));

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(YieldGroupAsync, spawn_wait_signal)
{
  asio::io_context ctx;
  yield_waiter<void> waiter;

  asio::cancellation_signal signal;
  std::optional<std::exception_ptr> result;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 1};
      asio::spawn(yield, wait_on(waiter), throttle);
      throttle.wait(); // blocks
    }, capture(signal, result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter);
  ASSERT_FALSE(result);

  signal.emit(boost::asio::cancellation_type::terminal);

  ctx.poll();
  EXPECT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(YieldGroupAsync, spawn_cancel_wait)
{
  asio::io_context ctx;
  yield_waiter<void> waiter;
  std::optional<std::exception_ptr> result;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 2};
      asio::spawn(yield, wait_on(waiter), throttle);
      throttle.cancel();
      throttle.wait();
    }, capture(result));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), asio::error::operation_aborted);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(YieldGroupAsync, spawn_cancel_on_error_none)
{
  asio::io_context ctx;
  yield_waiter<void> waiter1;
  yield_waiter<void> waiter2;
  yield_waiter<void> waiter3;
  std::optional<std::exception_ptr> result;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 4, cancel_on_error::none};
      asio::spawn(yield, wait_on(waiter1), throttle);
      asio::spawn(yield, wait_on(waiter2), throttle);
      asio::spawn(yield, wait_on(waiter3), throttle);
      throttle.wait(); // blocks
    }, capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter1);
  ASSERT_TRUE(waiter2);
  ASSERT_TRUE(waiter3);

  waiter2.complete(make_error_code(std::errc::no_such_file_or_directory));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  waiter1.complete(error_code{});

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  waiter3.complete(error_code{});

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(YieldGroupAsync, spawn_cancel_on_error_after)
{
  asio::io_context ctx;
  yield_waiter<void> waiter1;
  yield_waiter<void> waiter2;
  yield_waiter<void> waiter3;
  std::optional<std::exception_ptr> result;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 4, cancel_on_error::after};
      asio::spawn(yield, wait_on(waiter1), throttle);
      asio::spawn(yield, wait_on(waiter2), throttle);
      asio::spawn(yield, wait_on(waiter3), throttle);
      throttle.wait(); // blocks
    }, capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter1);
  ASSERT_TRUE(waiter2);
  ASSERT_TRUE(waiter3);

  waiter2.complete(make_error_code(std::errc::no_such_file_or_directory));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());

  // if the waiter3 cr was canceled, completing waiter1 should unblock wait()
  waiter1.complete(error_code{});

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(YieldGroupAsync, spawn_cancel_on_error_all)
{
  asio::io_context ctx;
  yield_waiter<void> waiter1;
  yield_waiter<void> waiter2;
  yield_waiter<void> waiter3;
  std::optional<std::exception_ptr> result;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 4, cancel_on_error::all};
      asio::spawn(yield, wait_on(waiter1), throttle);
      asio::spawn(yield, wait_on(waiter2), throttle);
      asio::spawn(yield, wait_on(waiter3), throttle);
      throttle.wait(); // blocks
    }, capture(result));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter1);
  ASSERT_TRUE(waiter2);
  ASSERT_TRUE(waiter3);

  // should cancel the other crs and unblock throttle.wait()
  waiter2.complete(make_error_code(std::errc::no_such_file_or_directory));

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
  ASSERT_TRUE(result);
  ASSERT_TRUE(*result);
  try {
    std::rethrow_exception(*result);
  } catch (const boost::system::system_error& e) {
    EXPECT_EQ(e.code(), std::errc::no_such_file_or_directory);
  } catch (const std::exception&) {
    EXPECT_THROW(throw, boost::system::system_error);
  }
}

TEST(YieldGroupAsync, spawn_wait_spawn_wait)
{
  asio::io_context ctx;
  yield_waiter<void> waiter1;
  yield_waiter<void> waiter2;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 1};
      asio::spawn(yield, wait_on(waiter1), throttle);
      throttle.wait(); // blocks
      asio::spawn(yield, wait_on(waiter2), throttle);
      throttle.wait(); // blocks
    }, rethrow);

  ASSERT_FALSE(waiter1);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter1);
  ASSERT_FALSE(waiter2);

  waiter1.complete(error_code{});

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter2);

  waiter2.complete(error_code{});

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
}

TEST(YieldGroupAsync, spawn_cancel_wait_spawn_wait)
{
  asio::io_context ctx;
  yield_waiter<void> waiter1;
  yield_waiter<void> waiter2;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 1};
      asio::spawn(yield, wait_on(waiter1), throttle);
      throttle.cancel();
      EXPECT_THROW(throttle.wait(), boost::system::system_error);
      asio::spawn(yield, wait_on(waiter2), throttle);
      throttle.wait(); // blocks
    }, rethrow);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter2);

  waiter2.complete(error_code{});

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
}

TEST(YieldGroupAsync, spawn_error_wait_spawn_wait)
{
  asio::io_context ctx;
  yield_waiter<void> waiter1;
  yield_waiter<void> waiter2;

  asio::spawn(ctx, [&] (asio::yield_context yield) {
      auto throttle = spawn_throttle{yield, 1};
      asio::spawn(yield, wait_on(waiter1), throttle);
      EXPECT_THROW(throttle.wait(), boost::system::system_error);
      asio::spawn(yield, wait_on(waiter2), throttle);
      throttle.wait(); // blocks
    }, rethrow);

  ASSERT_FALSE(waiter1);

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter1);
  ASSERT_FALSE(waiter2);

  waiter1.complete(make_error_code(std::errc::no_such_file_or_directory));

  ctx.poll();
  ASSERT_FALSE(ctx.stopped());
  ASSERT_TRUE(waiter2);

  waiter2.complete(error_code{});

  ctx.poll();
  ASSERT_TRUE(ctx.stopped());
}

} // namespace ceph::async
