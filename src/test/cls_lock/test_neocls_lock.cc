// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * See file COPYING for license information.
 *
 */

#include "neorados/cls/lock.h"

#include <boost/system/detail/errc.hpp>
#include <coroutine>
#include <chrono>
#include <map>
#include <string>
#include <string_view>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/system/errc.hpp>
#include <boost/system/system_error.hpp>

#include <fmt/format.h>

#include "include/neorados/RADOS.hpp"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

using namespace std::literals;

namespace asio = boost::asio;
namespace sys = boost::system;
namespace nr = neorados;
namespace lock = neorados::cls::lock;
using enum lock::type;

static asio::awaitable<bool> is_expired(neorados::RADOS& r, nr::Object oid,
					nr::IOContext pool, std::string name)
{
  auto [ec, lockers, type, tag] =
    co_await lock::info(r, std::move(oid), std::move(pool), std::move(name),
			asio::as_tuple(asio::use_awaitable));
  if (sys::errc::no_such_file_or_directory == ec) {
    co_return true;
  }
  EXPECT_FALSE(ec);
  auto now = ceph::real_clock::now();
  for (const auto& [id, info] : lockers) {
    if (info.expiration > now) {
      co_return false;
    }
  }

  co_return true;
}

template<typename Rep, typename Period>
asio::awaitable<void> wait_for(std::chrono::duration<Rep, Period> dur) {
  asio::steady_timer timer{co_await asio::this_coro::executor, dur};
  co_await timer.async_wait(asio::use_awaitable);
  co_return;
}

CORO_TEST_F(neocls_lock, test_log_add_same_time, NeoRadosTest)
{
  static constexpr auto oid = "foo"sv;
  static constexpr auto lock_name = "mylock"s;

  auto rados2 = co_await nr::RADOS::Builder{}.build(asio_context,
                                                    asio::use_awaitable);

  lock::Lock l{rados, oid, pool, lock_name};

  // Test lock object
  co_await l.lock(exclusive, asio::use_awaitable);

  // Test exclusivity
  co_await expect_error_code(l.lock(exclusive, asio::use_awaitable),
			     sys::errc::file_exists);
  // Test idempotence
  co_await l.lock(exclusive, asio::use_awaitable, lock::may_renew);

  // Test second client
  lock::Lock l2{rados2, oid, pool, lock_name};
  co_await expect_error_code(l2.lock(exclusive, asio::use_awaitable),
			     sys::errc::device_or_resource_busy);

  co_await expect_error_code(l2.lock(shared, asio::use_awaitable),
			     sys::errc::device_or_resource_busy);

  auto locks = co_await lock::list(rados, oid, pool, asio::use_awaitable);
  EXPECT_EQ(1, std::ssize(locks));
  EXPECT_EQ(lock_name, locks.front());

  auto [lockers, type, tag] = co_await l2.info(asio::use_awaitable);
  EXPECT_EQ(type, exclusive);
  EXPECT_EQ(""s, tag);
  EXPECT_EQ(1, std::ssize(lockers));

  // Test unlock
  co_await l.unlock(asio::use_awaitable);

  locks = co_await lock::list(rados, oid, pool, asio::use_awaitable);
  EXPECT_TRUE(locks.empty());

  // Test shared
  co_await l2.lock(shared, asio::use_awaitable);
  co_await l.lock(shared, asio::use_awaitable);

  locks = co_await lock::list(rados, oid, pool, asio::use_awaitable);
  EXPECT_EQ(1, std::ssize(locks));
  EXPECT_EQ(lock_name, locks.front());

  std::tie(lockers, type, tag) = co_await l2.info(asio::use_awaitable);
  EXPECT_EQ(type, shared);
  EXPECT_EQ(""s, tag);
  EXPECT_EQ(2, std::ssize(lockers));

  /* test break locks */
  auto name = entity_name_t::CLIENT(rados.instance_id());
  auto name2 = entity_name_t::CLIENT(rados2.instance_id());

  co_await l2.break_lock("", name, asio::use_awaitable);
  std::tie(lockers, type, tag) = co_await l2.info(asio::use_awaitable);
  EXPECT_EQ(type, shared);
  EXPECT_EQ(""s, tag);
  EXPECT_EQ(1, std::ssize(lockers));
  EXPECT_EQ(name2, lockers.begin()->first.locker);

  // Test lock tag

  auto l_tag{lock::Lock(rados, oid, pool, lock_name)
	     .with_tag("non-default tag"s)};

  co_await expect_error_code(l_tag.lock(shared, asio::use_awaitable),
			     sys::errc::device_or_resource_busy);

  // Test modify description
  l.with_description("New description"s);
  co_await l.lock(shared, asio::use_awaitable);
}

CORO_TEST_F(neocls_lock, test_meta, NeoRadosTest)
{
  static constexpr auto oid = "foo"sv;
  static constexpr auto lock_name = "mylock"s;

  lock::Lock l{rados, oid, pool, lock_name};
  co_await l.lock(shared, asio::use_awaitable);


  // Test tag
  {
    auto l_tag{lock::Lock{rados, oid, pool, lock_name}
	       .with_tag("non-default tag")};
    co_await expect_error_code(l_tag.lock(shared, asio::use_awaitable),
			       sys::errc::device_or_resource_busy);
    co_await l.unlock(asio::use_awaitable);
  }

  // Test Description
  {
    static constexpr auto descr{"new description"s};
    auto l2{lock::Lock{rados, oid, pool, lock_name}
	    .with_description(descr)};
    co_await l2.lock(shared, asio::use_awaitable);

    auto [lockers, type, tag] = co_await l2.info(asio::use_awaitable);
    EXPECT_EQ(1, std::ssize(lockers));
    EXPECT_EQ(descr, lockers.cbegin()->second.description);
    co_await l2.unlock(asio::use_awaitable);
  }

  // Test new tag

  static constexpr auto new_tag{"new_tag"s};
  l.with_tag(new_tag);
  co_await l.lock(exclusive, asio::use_awaitable, lock::may_renew);
  auto [lockers, type, tag] = co_await l.info(asio::use_awaitable);
  EXPECT_EQ(1, std::ssize(lockers));

  l.with_tag(""s);
  co_await expect_error_code(l.lock(exclusive, asio::use_awaitable,
				    lock::may_renew),
			     sys::errc::device_or_resource_busy);

  l.with_tag(new_tag);
  co_await l.lock(exclusive, asio::use_awaitable, lock::may_renew);
}

CORO_TEST_F(neocls_lock, test_cookie, NeoRadosTest)
{
  static constexpr auto oid = "foo"sv;
  static constexpr auto lock_name = "mylock"s;

  lock::Lock l{rados, oid, pool, lock_name};
  co_await l.lock(exclusive, asio::use_awaitable);

  static constexpr auto cookie("new_cookie");
  l.with_cookie(cookie);

  co_await expect_error_code(l.lock(exclusive, asio::use_awaitable),
			     sys::errc::device_or_resource_busy);

  co_await expect_error_code(l.unlock(asio::use_awaitable),
			     sys::errc::no_such_file_or_directory);

  l.with_cookie("");
  co_await l.unlock(asio::use_awaitable);

  auto [lockers, type, tag] = co_await l.info(asio::use_awaitable);
  EXPECT_TRUE(lockers.empty());

  l.with_cookie(cookie);
  co_await l.lock(shared, asio::use_awaitable);
  l.with_cookie("");
  co_await l.lock(shared, asio::use_awaitable);

  std::tie(lockers, type, tag) = co_await l.info(asio::use_awaitable);
  EXPECT_EQ(2, std::ssize(lockers));
}

CORO_TEST_F(neocls_lock, test_multiple_locks, NeoRadosTest)
{
  static constexpr auto oid = "foo"sv;

  lock::Lock l(rados, oid, pool, "lock1"s);
  co_await l.lock(exclusive, asio::use_awaitable);

  lock::Lock l2(rados, oid, pool, "lock2"s);
  co_await l2.lock(exclusive, asio::use_awaitable);

  auto locks = co_await lock::list(rados, oid, pool, asio::use_awaitable);
  EXPECT_EQ(2, std::ssize(locks));
}

CORO_TEST_F(neocls_lock, test_lock_duration, NeoRadosTest)
{
  static constexpr auto oid = "foo"sv;

  static constexpr auto dur = 5s;
  auto l{lock::Lock{rados, oid, pool, "lock"}.with_duration(dur)};
  auto start = ceph::mono_clock::now();
  co_await l.lock(exclusive, asio::use_awaitable);

  bool error = false;
  try {
    co_await l.lock(exclusive, asio::use_awaitable);
  } catch (const sys::system_error& e) {
    error = true;
    EXPECT_EQ(sys::errc::file_exists, e.code());
  }
  if (!error) {
    EXPECT_LE(start + dur, ceph::mono_clock::now());
  }

  co_await wait_for(dur);

  co_await l.lock(exclusive, asio::use_awaitable);
}

CORO_TEST_F(neocls_lock, test_assert_locked, NeoRadosTest)
{
  static constexpr auto oid = "foo"sv;

  lock::Lock l{rados, oid, pool, "lock1"s};

  co_await l.lock(exclusive, asio::use_awaitable);

  co_await rados.execute(oid, pool,
			 l.assert_locked(nr::WriteOp{}, exclusive),
			 asio::use_awaitable);

  co_await expect_error_code(rados.execute(oid, pool, l.assert_locked(nr::WriteOp{}, shared),
					   asio::use_awaitable),
			     sys::errc::device_or_resource_busy);

  l.with_tag("tag"s);
  co_await expect_error_code(rados.execute(oid, pool,
					   l.assert_locked(nr::WriteOp{},
							   exclusive),
					   asio::use_awaitable),
			     sys::errc::device_or_resource_busy);
  l.with_tag(""s);
  l.with_cookie("cookie"s);
  co_await expect_error_code(rados.execute(oid, pool,
					   l.assert_locked(nr::WriteOp{},
							   exclusive),
					   asio::use_awaitable),
			     sys::errc::device_or_resource_busy);
  l.with_cookie(""s);

  co_await l.unlock(asio::use_awaitable);

  co_await expect_error_code(rados.execute(oid, pool,
					   l.assert_locked(nr::WriteOp{},
							   exclusive),
					   asio::use_awaitable),
			     sys::errc::device_or_resource_busy);
}

CORO_TEST_F(neocls_lock, test_set_cookie, NeoRadosTest)
{
  static constexpr auto oid = "foo"sv;

  auto name = "name"s;
  auto tag = "tag"s;
  auto cookie = "cookie"s;
  auto new_cookie = "new cookie"s;

  co_await expect_error_code(
    rados.execute(oid, pool,
		  lock::set_cookie({}, name, shared, cookie, tag, new_cookie),
		  asio::use_awaitable),
    sys::errc::no_such_file_or_directory);

  co_await rados.execute(oid, pool,
			 lock::lock({}, name, shared, cookie, tag, {}, {}, {}),
			 asio::use_awaitable);

  co_await rados.execute(oid, pool,
			 lock::lock({}, name, shared, "cookie 2"s, tag, {}, {}, {}),
			 asio::use_awaitable);

  co_await expect_error_code(
    rados.execute(oid, pool,
		  lock::set_cookie({}, name, shared, cookie, tag, cookie),
		  asio::use_awaitable),
    sys::errc::device_or_resource_busy);

  co_await expect_error_code(
    rados.execute(oid, pool,
		  lock::set_cookie({}, name, shared, cookie, "wrong tag"s,
				   new_cookie),
		  asio::use_awaitable),
    sys::errc::device_or_resource_busy);

  co_await expect_error_code(
    rados.execute(oid, pool,
		  lock::set_cookie({}, name, shared, "wrong cookie"s, tag,
				   new_cookie),
		  asio::use_awaitable),
    sys::errc::device_or_resource_busy);

  co_await expect_error_code(
    rados.execute(oid, pool,
		  lock::set_cookie({}, name, exclusive, cookie, tag,
				   new_cookie),
		  asio::use_awaitable),
    sys::errc::device_or_resource_busy);

  co_await expect_error_code(
    rados.execute(oid, pool,
		  lock::set_cookie({}, name, shared, cookie, tag,
				   "cookie 2"),
		  asio::use_awaitable),
    sys::errc::device_or_resource_busy);

  co_await rados.execute(oid, pool,
			 lock::set_cookie({}, name, shared, cookie, tag,
					  new_cookie), asio::use_awaitable);
}

CORO_TEST_F(neocls_lock, test_renew, NeoRadosTest)
{
  auto l1{lock::Lock{rados, "foo1"s, pool, "mylock1"s}
	  .with_duration(5s)};

  co_await l1.lock(exclusive, asio::use_awaitable);
  co_await wait_for(2s);
  co_await l1.lock(exclusive, asio::use_awaitable, lock::may_renew);
  // Lock operations that are may_renew should work after expiration
  co_await wait_for(7s);
  co_await l1.lock(exclusive, asio::use_awaitable, lock::may_renew);
  co_await l1.unlock(asio::use_awaitable);

  // -------------------------------------------------------------------

  auto l2{lock::Lock{rados, "foo2"s, pool, "mylock2"s}
	  .with_duration(5s)};
  co_await l2.lock(exclusive, asio::use_awaitable);
  co_await wait_for(2s);
  co_await l2.lock(exclusive, asio::use_awaitable, lock::must_renew);
  co_await wait_for(7s);
  // A must_renew relock after expiration should fail
  co_await expect_error_code(l2.lock(exclusive, asio::use_awaitable,
				     lock::must_renew),
			     sys::errc::no_such_file_or_directory);

  // -------------------------------------------------------------------

  auto l3{lock::Lock{rados, "foo3"s, pool, "mylock3"s}
	  .with_duration(5s)};
  // You can't create a new lock with must_renew.
  co_await expect_error_code(l3.lock(exclusive, asio::use_awaitable,
				     lock::must_renew),
			     sys::errc::no_such_file_or_directory);
}

CORO_TEST_F(neocls_lock, test_ephemeral_basic, NeoRadosTest)
{
  static constexpr auto oid1{"foo1"s};
  static constexpr auto oid2{"foo2"s};
  static constexpr auto name1{"mylock1"s};
  static constexpr auto name2{"mylock2"s};

  auto l1{lock::Lock(rados, oid1, pool, name1)
	  .with_duration(5s)};
  co_await l1.lock(ephemeral, asio::use_awaitable, lock::may_renew);
  co_await rados.execute(oid1, pool, nr::ReadOp{}.stat(nullptr, nullptr),
			 nullptr, asio::use_awaitable);
  co_await wait_for(2s);
  sys::error_code ec;
  co_await l1.unlock(asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_TRUE(!ec ||((sys::errc::no_such_file_or_directory == ec) &&
		      co_await is_expired(rados, oid1, pool, name1)));
  ec.clear();
  co_await expect_error_code(
    rados.execute(oid1, pool, nr::ReadOp{}.stat(nullptr, nullptr),
		  nullptr, asio::use_awaitable),
    sys::errc::no_such_file_or_directory);

  // -------------------------------------------------------------------

  auto l2{lock::Lock(rados, oid2, pool, name2)
	  .with_duration(5s)};
  co_await l2.lock(exclusive, asio::use_awaitable);
  co_await rados.execute(oid2, pool, nr::ReadOp{}.stat(nullptr, nullptr),
			 nullptr, asio::use_awaitable);
  co_await wait_for(2s);
  co_await l2.unlock(asio::redirect_error(asio::use_awaitable, ec));
  EXPECT_TRUE(!ec ||((sys::errc::no_such_file_or_directory == ec) &&
		      co_await is_expired(rados, oid2, pool, name2)));
  ec.clear();
  co_await rados.execute(oid2, pool, nr::ReadOp{}.stat(nullptr, nullptr),
			 nullptr, asio::use_awaitable);
}

CORO_TEST_F(neocls_lock, test_ephemeral_steal_ephemeral, NeoRadosTest)
{
  static constexpr auto oid1{"foo1"s};
  static constexpr auto name1{"mylock1"s};

  auto l1{lock::Lock(rados, oid1, pool, name1)
              .with_duration(3s)
	      .with_random_cookie()};
  co_await l1.lock(ephemeral, asio::use_awaitable);
  co_await wait_for(4s);

  // l1 is expired, l2 can now take an ephemeral lock
  auto l2{lock::Lock(rados, oid1, pool, name1)
              .with_duration(3s)
	      .with_random_cookie()};
  co_await l2.lock(ephemeral, asio::use_awaitable);
  co_await wait_for(1s);
  co_await l2.unlock(asio::use_awaitable);

  // l1 cannot unlock its expired lock
  co_await expect_error_code(l1.unlock(asio::use_awaitable),
			     sys::errc::no_such_file_or_directory);
}

CORO_TEST_F(neocls_lock, test_ephemeral_steal_exclusive, NeoRadosTest)
{
  static constexpr auto oid1{"foo1"s};
  static constexpr auto name1{"mylock1"s};

  auto l1{lock::Lock(rados, oid1, pool, name1)
              .with_duration(3s)
	      .with_random_cookie()};
  co_await l1.lock(ephemeral, asio::use_awaitable);
  co_await wait_for(4s);

  // l1 is expired, l2 can now take an exclusive (but not ephemeral) lock
  auto l2{lock::Lock(rados, oid1, pool, name1)
              .with_duration(3s)
	      .with_random_cookie()};
  co_await l2.lock(exclusive, asio::use_awaitable);
  co_await wait_for(1s);
  co_await l2.unlock(asio::use_awaitable);

  // l1 cannot unlock its expired lock
  co_await expect_error_code(l1.unlock(asio::use_awaitable),
			     sys::errc::no_such_file_or_directory);
}
