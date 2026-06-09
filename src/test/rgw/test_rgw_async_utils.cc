// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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

#include "rgw/async_utils.h"

#include <cerrno>
#include <memory>
#include <string>
#include <tuple>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/spawn.hpp>

#include <boost/system/generic_category.hpp>
#include <boost/system/system_error.hpp>

#include <gtest/gtest.h>

#include "include/common_fwd.h"

#include "common/async/context_pool.h"
#include "common/async/yield_context.h"
#include "common/dout.h"

#include "test/neorados/common_tests.h"

namespace asio = boost::asio;
namespace async = ceph::async;
namespace sys = boost::system;

using namespace std::literals;

static auto cct = std::make_unique<CephContext>(CEPH_ENTITY_TYPE_ANY);

static NoDoutPrefix dp(cct.get(), ceph_subsys_rgw);

static asio::awaitable<void>
maybethrow(int code)
{
  if (code != 0) {
    throw sys::system_error{code, sys::generic_category()};
  }
  co_return;
}

TEST(CoSpawn, CoSpawn) {
  async::io_context_pool pool{3};

  int r = 0;
  r = rgw::run_coro(&dp, pool, maybethrow(ENOENT), nullptr);
  ASSERT_EQ(-ENOENT, r);

  r = rgw::run_coro(&dp, pool, maybethrow(0), nullptr);
  ASSERT_EQ(0, r);

  asio::spawn(pool,
              [&](asio::yield_context y) -> void {
                r = rgw::run_coro(&dp, pool, maybethrow(ENOENT), "yielding",
                                  y);
              },
              async::use_blocked);
  ASSERT_EQ(-ENOENT, r);

  asio::spawn(pool,
              [&](asio::yield_context y) -> void {
                r = rgw::run_coro(&dp, pool, maybethrow(0), "yielding",
                                  y);
              },
              async::use_blocked);
  ASSERT_EQ(0, r);

  r = rgw::run_coro(&dp, pool, maybethrow(ENOENT), "blocking",
                    null_yield);
  ASSERT_EQ(-ENOENT, r);

  r = rgw::run_coro(&dp, pool, maybethrow(0), "blocking",
                    null_yield);
  ASSERT_EQ(0, r);

  auto maybethrowv = []<typename V>(int code, V v) -> asio::awaitable<V> {
    if (code != 0) {
      throw sys::system_error{code, sys::generic_category()};
    }
    co_return std::move(v);
  };

  const std::string instr("foo");
  std::string s;

  r = rgw::run_coro(&dp, pool, maybethrowv(ENOENT, instr),
                    s, nullptr);
  ASSERT_EQ(-ENOENT, r);
  ASSERT_TRUE(s.empty());

  r = rgw::run_coro(&dp, pool, maybethrowv(0, instr), s, nullptr);
  ASSERT_EQ(0, r);
  ASSERT_EQ(instr, s);

  s.clear();

  asio::spawn(pool,
              [&](asio::yield_context y) -> void {
                r = rgw::run_coro(&dp, pool, maybethrowv(ENOENT, instr),
                                  s, "yielding", y);
              },
              async::use_blocked);
  ASSERT_EQ(-ENOENT, r);
  ASSERT_TRUE(s.empty());

  asio::spawn(pool,
              [&](asio::yield_context y) -> void {
                r = rgw::run_coro(&dp, pool, maybethrowv(0, instr),
                                  s, "yielding", y);
              },
              async::use_blocked);
  ASSERT_EQ(0, r);
  ASSERT_EQ(instr, s);

  s.clear();
  r = rgw::run_coro(&dp, pool, maybethrowv(ENOENT, instr), s,
                    "blocking", null_yield);
  ASSERT_EQ(-ENOENT, r);
  ASSERT_TRUE(s.empty());

  r = rgw::run_coro(&dp, pool, maybethrowv(0, instr), s,
                    "blocking", null_yield);
  ASSERT_EQ(0, r);
  ASSERT_EQ(instr, s);

  auto maybethrowvs = []<typename ...Vs>(int code, Vs ...vs)
    -> asio::awaitable<std::tuple<Vs...>> {
    if (code != 0) {
      throw sys::system_error{code, sys::generic_category()};
    }
    co_return std::make_tuple(std::move(vs)...);
  };

  s.clear();
  std::unique_ptr<int> p;

  r = rgw::run_coro(&dp, pool, maybethrowvs(ENOENT, instr,
					    std::make_unique<int>(5)),
                    std::tie(s, p), nullptr);
  ASSERT_EQ(-ENOENT, r);
  ASSERT_TRUE(s.empty());
  ASSERT_FALSE(p);

  r = rgw::run_coro(&dp, pool, maybethrowvs(0, instr,
					    std::make_unique<int>(5)),
                    std::tie(s, p), nullptr);
  ASSERT_EQ(0, r);
  ASSERT_EQ(instr, s);
  ASSERT_TRUE(p);
  ASSERT_EQ(5, *p);

  s.clear();
  p.reset();

  asio::spawn(pool,
              [&](asio::yield_context y) -> void {
                r = rgw::run_coro(&dp, pool,
				  maybethrowvs(ENOENT, instr,
					       std::make_unique<int>(5)),
                                  std::tie(s, p), "yielding", y);
              },
              async::use_blocked);
  ASSERT_EQ(-ENOENT, r);
  ASSERT_TRUE(s.empty());
  ASSERT_FALSE(p);

  asio::spawn(pool,
              [&](asio::yield_context y) -> void {
                r = rgw::run_coro(&dp, pool,
				  maybethrowvs(0, instr,
					       std::make_unique<int>(5)),
                                  std::tie(s, p), "yielding", y);
              },
              async::use_blocked);
  ASSERT_EQ(0, r);
  ASSERT_EQ(instr, s);
  ASSERT_TRUE(p);
  ASSERT_EQ(5, *p);

  s.clear();
  p.reset();
  r = rgw::run_coro(&dp, pool, maybethrowvs(ENOENT, instr,
					    std::make_unique<int>(5)),
		    std::tie(s, p), "blocking", null_yield);
  ASSERT_EQ(-ENOENT, r);
  ASSERT_TRUE(s.empty());
  ASSERT_FALSE(p);

  r = rgw::run_coro(&dp, pool, maybethrowvs(0, instr,
					    std::make_unique<int>(5)),
		    std::tie(s, p), "blocking", null_yield);
  ASSERT_EQ(0, r);
  ASSERT_EQ(instr, s);
  ASSERT_TRUE(p);
  ASSERT_EQ(5, *p);
}

TEST(HybridToken, HybridToken)
{
  ceph::async::io_context_pool io_context{3, [] { is_asio_thread = true; }};
  bool ran = false;
  asio::spawn(
      io_context,
      [&](asio::yield_context yc) {
        optional_yield y{yc};

        ASSERT_NO_THROW(
            co_spawn(yc.get_executor(), maybethrow(0), rgw::oyc(&dp, y)));
        ASSERT_NO_THROW(co_spawn(
            yc.get_executor(), maybethrow(0), rgw::oyc(&dp, null_yield)));

        ASSERT_THROW(
            co_spawn(yc.get_executor(), maybethrow(ENOENT), rgw::oyc(&dp, y)),
            sys::system_error);
        ASSERT_THROW(
            co_spawn(
                yc.get_executor(), maybethrow(ENOENT),
                rgw::oyc(&dp, null_yield)),
            sys::system_error);

        {
          std::exception_ptr eptr;
          ASSERT_NO_THROW(co_spawn(
              yc.get_executor(), maybethrow(ENOENT), rgw::oyc(&dp, y, eptr)));
          ASSERT_EQ(-ENOENT, ceph::from_exception(eptr));
        }
        {
          std::exception_ptr eptr;
          ASSERT_NO_THROW(co_spawn(
              yc.get_executor(), maybethrow(ENOENT), rgw::oyc(&dp, null_yield, eptr)));
          ASSERT_EQ(-ENOENT, ceph::from_exception(eptr));
        }

        ran = true;
      },
      rgw::oyc(&dp, null_yield));
  ASSERT_TRUE(ran);
}

class Task : public CoroTest {
protected:
  int value = 0;

public:
  asio::awaitable<void>
  increment()
  {
    ++value;
    co_return;
  }

  asio::awaitable<void>
  waitcrement()
  {
    co_await rgw::wait_for(3s);
    ++value;
    co_return;
  }
};

CORO_TEST_F(Task, Run, Task)
{
  rgw::task task{
      asio::make_strand(co_await asio::this_coro::executor), increment()};

  EXPECT_EQ(rgw::task_state::ready, task.state());
  task.run();
  EXPECT_EQ(rgw::task_state::running, task.state());
  co_await task.join(asio::use_awaitable);
  EXPECT_EQ(rgw::task_state::dead, task.state());
  EXPECT_EQ(1, value);
  co_return;
}

CORO_TEST_F(Task, RunCoroFun, Task)
{
  rgw::task task{
      asio::make_strand(co_await asio::this_coro::executor),
      rgw::membind(*this, &Task::increment)};

  EXPECT_EQ(rgw::task_state::ready, task.state());
  task.run();
  EXPECT_EQ(rgw::task_state::running, task.state());
  co_await task.join(asio::use_awaitable);
  EXPECT_EQ(rgw::task_state::dead, task.state());
  EXPECT_EQ(1, value);
  co_return;
}

CORO_TEST_F(Task, Running, Task)
{
  rgw::task task{
      asio::make_strand(co_await asio::this_coro::executor), increment(),
      rgw::running};

  EXPECT_EQ(rgw::task_state::running, task.state());
  co_await task.join(asio::use_awaitable);
  EXPECT_EQ(rgw::task_state::dead, task.state());
  EXPECT_EQ(1, value);
  co_return;
}

CORO_TEST_F(Task, RunningCoroFun, Task)
{
  rgw::task task{
      asio::make_strand(co_await asio::this_coro::executor),
      rgw::membind(*this, &Task::increment), rgw::running};

  EXPECT_EQ(rgw::task_state::running, task.state());
  co_await task.join(asio::use_awaitable);
  EXPECT_EQ(rgw::task_state::dead, task.state());
  EXPECT_EQ(1, value);
  co_return;
}

CORO_TEST_F(Task, Cancel, Task)
{
  rgw::task task{
      asio::make_strand(co_await asio::this_coro::executor), waitcrement(),
      rgw::running};

  EXPECT_EQ(rgw::task_state::running, task.state());
  task.cancel();
  EXPECT_EQ(rgw::task_state::running, task.state());
  co_await expect_error_code(task.join(asio::use_awaitable), asio::error::operation_aborted);
  EXPECT_EQ(rgw::task_state::dead, task.state());
  EXPECT_EQ(0, value);
  co_return;
}

CORO_TEST_F(Task, Shutdown, Task)
{
  auto strand = asio::make_strand(co_await asio::this_coro::executor);
  static const std::string label = "Test!";
  rgw::task task{strand, increment(), rgw::running};
  EXPECT_EQ(strand, task.get_executor());

  EXPECT_EQ(rgw::task_state::running, task.state());
  rgw::shutdown_vector v;
  task.shutdown(label, v);
  EXPECT_EQ(rgw::task_state::dead, task.state());
  EXPECT_EQ(1u, v.size());
  auto& [l, p] = v.front();
  EXPECT_EQ(label, l);
  co_await p(asio::use_awaitable);
  EXPECT_EQ(1, value);
  co_return;
}

class Loop : public CoroTest {
protected:
  int value = 0;
  ceph::timespan delay = 0s;

public:
  asio::awaitable<ceph::timespan>
  increment()
  {
    ++value;
    co_return delay;
  }
};

CORO_TEST_F(Loop, Looping, Loop)
{
  delay = 250ms;
  rgw::run_loop loop{
      asio::make_strand(co_await asio::this_coro::executor),
      rgw::membind(*this, &Loop::increment)};

  EXPECT_EQ(rgw::task_state::ready, loop.state());
  loop.run();
  EXPECT_EQ(rgw::task_state::running, loop.state());
  co_await rgw::wait_for(300ms);
  loop.cancel();
  co_await loop.join(asio::use_awaitable);
  EXPECT_EQ(rgw::task_state::dead, loop.state());
  EXPECT_EQ(2, value);
  co_return;
}

CORO_TEST_F(Loop, Running, Loop)
{
  delay = 250ms;
  rgw::run_loop loop{
      asio::make_strand(co_await asio::this_coro::executor),
      rgw::membind(*this, &Loop::increment), rgw::running};

  EXPECT_EQ(rgw::task_state::running, loop.state());
  co_await rgw::wait_for(300ms);
  loop.cancel();
  co_await loop.join(asio::use_awaitable);
  EXPECT_EQ(rgw::task_state::dead, loop.state());
  EXPECT_EQ(2, value);
  co_return;
}

CORO_TEST_F(Loop, Wake, Loop)
{
  delay = 5s;
  rgw::run_loop loop{
      asio::make_strand(co_await asio::this_coro::executor),
      rgw::membind(*this, &Loop::increment), rgw::running};

  EXPECT_EQ(rgw::task_state::running, loop.state());
  loop.wake();
  co_await rgw::wait_for(100ms);
  loop.cancel();
  co_await loop.join(asio::use_awaitable);
  EXPECT_EQ(rgw::task_state::dead, loop.state());
  EXPECT_EQ(2, value);
  co_return;
}

namespace {
void
rethrower(std::exception_ptr e)
{
  if (e) {
    std::rethrow_exception(e);
  }
}
}

// The rest of the base class functionality is already covered in the Task tests.

class YTask : public ::testing::Test {
public:
  asio::io_context io_context;
  int value = 0;

  void
  increment(asio::yield_context)
  {
    ++value;
  }

  void
  waitcrement(asio::yield_context y)
  {
    rgw::wait_for(3s, y);
    ++value;
  }
};

TEST_F(YTask, Run)
{
  asio::spawn(
      io_context,
      [this](asio::yield_context y) {
        rgw::ytask task{
            asio::make_strand(y.get_executor()),
            rgw::membind(*this, &YTask::increment)};

        ASSERT_EQ(rgw::task_state::ready, task.state());
        task.run();
        ASSERT_EQ(rgw::task_state::running, task.state());
        task.join(y);
        ASSERT_EQ(rgw::task_state::dead, task.state());
        ASSERT_EQ(1, value);
      },
      &rethrower);
  io_context.run();
}

TEST_F(YTask, Running)
{
  asio::spawn(
      io_context,
      [this](asio::yield_context y) {
        rgw::ytask task{
            asio::make_strand(y.get_executor()),
            rgw::membind(*this, &YTask::increment), rgw::running};

        ASSERT_EQ(rgw::task_state::running, task.state());
        task.join(y);
        ASSERT_EQ(rgw::task_state::dead, task.state());
        ASSERT_EQ(1, value);
      },
      &rethrower);
  io_context.run();
}

TEST_F(YTask, Cancel)
{
  asio::spawn(
      io_context,
      [this](asio::yield_context y) {
        rgw::ytask task{
            asio::make_strand(y.get_executor()),
            rgw::membind(*this, &YTask::waitcrement), rgw::running};

        ASSERT_EQ(rgw::task_state::running, task.state());
        task.cancel();
        ASSERT_EQ(rgw::task_state::running, task.state());
        ASSERT_THROW(task.join(y), sys::system_error);
        ASSERT_EQ(rgw::task_state::dead, task.state());
        ASSERT_EQ(0, value);
      },
      &rethrower);
  io_context.run();
}

TEST_F(YTask, Shutdown)
{
  asio::spawn(
      io_context,
      [this](asio::yield_context y) {
        auto strand = asio::make_strand(y.get_executor());
        rgw::ytask task{
            strand, rgw::membind(*this, &YTask::increment), rgw::running};
        static const std::string label = "Test!";
        EXPECT_EQ(strand, task.get_executor());

        ASSERT_EQ(rgw::task_state::running, task.state());
        rgw::shutdown_vector v;
        task.shutdown(label, v);
        ASSERT_EQ(rgw::task_state::dead, task.state());
        ASSERT_EQ(1u, v.size());
        auto& [l, p] = v.front();
        ASSERT_EQ(label, l);
        p(y);
        ASSERT_EQ(1, value);
      },
      &rethrower);
  io_context.run();
}

class YLoop : public ::testing::Test {
public:
  asio::io_context io_context;
  int value = 0;
  ceph::timespan delay = 0s;

  ceph::timespan
  increment(asio::yield_context)
  {
    ++value;
    return delay;
  }
};

TEST_F(YLoop, Looping)
{
  asio::spawn(
      io_context,
      [this](asio::yield_context y) {
        delay = 250ms;
        rgw::yrun_loop loop{asio::make_strand(y.get_executor()),
                            rgw::membind(*this, &YLoop::increment)};

        ASSERT_EQ(rgw::task_state::ready, loop.state());
        loop.run();
        ASSERT_EQ(rgw::task_state::running, loop.state());
        rgw::wait_for(300ms, y);
        loop.cancel();
        loop.join(y);
        ASSERT_EQ(rgw::task_state::dead, loop.state());
        ASSERT_EQ(2, value);
      },
      &rethrower);
  io_context.run();
}

TEST_F(YLoop, Running)
{
  asio::spawn(
      io_context,
      [this](asio::yield_context y) {
        delay = 250ms;
        rgw::yrun_loop loop{
            asio::make_strand(y.get_executor()),
            rgw::membind(*this, &YLoop::increment), rgw::running};

        ASSERT_EQ(rgw::task_state::running, loop.state());
        rgw::wait_for(300ms, y);
        loop.cancel();
        loop.join(y);
        ASSERT_EQ(rgw::task_state::dead, loop.state());
        ASSERT_EQ(2, value);
      },
      &rethrower);
  io_context.run();
}

TEST_F(YLoop, Wake)
{
  asio::spawn(
      io_context,
      [this](asio::yield_context y) {
        delay = 5s;
        rgw::yrun_loop loop{
            asio::make_strand(y.get_executor()),
            rgw::membind(*this, &YLoop::increment), rgw::running};

        ASSERT_EQ(rgw::task_state::running, loop.state());
        loop.wake();
        rgw::wait_for(100ms, y);
        loop.cancel();
        loop.join(y);
        EXPECT_EQ(rgw::task_state::dead, loop.state());
        EXPECT_EQ(2, value);
      },
      &rethrower);
  io_context.run();
}

// The rest of the base class functionality is already covered in the YTask tests.

namespace {
rgw::task<>
thrower(ceph::timespan delay, auto executor, auto exception, bool& executed)
{
  return rgw::task{
      asio::make_strand(executor),
      [](ceph::timespan delay, auto e, bool& executed) -> asio::awaitable<void> {
        co_await rgw::wait_for(delay);
        executed = true;
        throw e;
      }(delay, std::forward<decltype(exception)>(exception), executed)};
}
} // namespace

CORO_TEST(AwaitShutdowns, AwaitShutdowns)
{
  bool ran1 = false;
  auto task1 = thrower(
      10us, co_await asio::this_coro::executor, std::logic_error{"Oh no!"},
      ran1);
  bool ran2 = false;
  auto task2 = thrower(
      500ms, co_await asio::this_coro::executor,
      std::runtime_error{"O me miserum!"}, ran2);
  task1.run();
  task2.run();

  rgw::shutdown_vector to_wait;
  task1.shutdown("task1", to_wait);
  task2.shutdown("task2", to_wait);

  EXPECT_THROW(
      co_await rgw::await_shutdowns(
          nullptr, co_await asio::this_coro::executor, std::move(to_wait),
          asio::use_awaitable),
      std::logic_error);
  EXPECT_TRUE(ran1);
  EXPECT_TRUE(ran2);
}

CORO_TEST(AwaitShutdowns, AwaitShutdownsRev)
{
  bool ran1 = false;
  auto task1 = thrower(
      10us, co_await asio::this_coro::executor, std::logic_error{"Oh no!"},
      ran1);
  bool ran2 = false;
  auto task2 = thrower(
      500ms, co_await asio::this_coro::executor,
      std::runtime_error{"O me miserum!"}, ran2);
  task2.run();
  task1.run();

  rgw::shutdown_vector to_wait;
  task2.shutdown("task2", to_wait);
  task1.shutdown("task1", to_wait);

  EXPECT_THROW(
      co_await rgw::await_shutdowns(
          nullptr, co_await asio::this_coro::executor, std::move(to_wait),
          asio::use_awaitable),
      std::logic_error);
  EXPECT_TRUE(ran1);
  EXPECT_TRUE(ran2);
}

TEST(YAwaitShutdowns, AwaitShutdowns)
{
  asio::io_context io_context;
  asio::spawn(
      io_context,
      [](asio::yield_context y) {
        bool ran1 = false;
        auto task1 =
            thrower(10us, y.get_executor(), std::logic_error{"Oh no!"}, ran1);
        bool ran2 = false;
        auto task2 = thrower(
            500ms, y.get_executor(), std::runtime_error{"O me miserum!"}, ran2);
        task1.run();
        task2.run();

        rgw::shutdown_vector to_wait;
        task1.shutdown("task1", to_wait);
        task2.shutdown("task2", to_wait);

        EXPECT_THROW(
            rgw::await_shutdowns(nullptr, y.get_executor(), std::move(to_wait), y), std::logic_error);
        EXPECT_TRUE(ran1);
        EXPECT_TRUE(ran2);
      },
      rethrower);
  io_context.run();
}

TEST(YAwaitShutdowns, AwaitShutdownsRev)
{
  asio::io_context io_context;
  asio::spawn(
      io_context,
      [](asio::yield_context y) {
        bool ran1 = false;
        auto task1 =
            thrower(10us, y.get_executor(), std::logic_error{"Oh no!"}, ran1);
        bool ran2 = false;
        auto task2 = thrower(
            500ms, y.get_executor(), std::runtime_error{"O me miserum!"}, ran2);
        task2.run();
        task1.run();

        rgw::shutdown_vector to_wait;
        task2.shutdown("task2", to_wait);
        task1.shutdown("task1", to_wait);

        EXPECT_THROW(
            rgw::await_shutdowns(nullptr, y.get_executor(), std::move(to_wait), y), std::logic_error);
        EXPECT_TRUE(ran1);
        EXPECT_TRUE(ran2);
      },
      rethrower);
  io_context.run();
}
