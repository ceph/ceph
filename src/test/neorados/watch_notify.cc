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

#include <boost/system/detail/errc.hpp>
#include <coroutine>
#include <cstdint>
#include <iostream>
#include <utility>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <boost/asio/experimental/awaitable_operators.hpp>

#include <boost/container/flat_set.hpp>

#include <boost/system/errc.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

#include "include/neorados/RADOS.hpp"
#include "include/buffer.h"

#include "test/neorados/common_tests.h"

#include "gtest/gtest.h"

using std::uint64_t;

namespace asio = boost::asio;
namespace buffer = ceph::buffer;
namespace container = boost::container;
namespace sys = boost::system;

using namespace std::literals;

using neorados::ReadOp;
using neorados::WriteOp;

using std::uint64_t;

using namespace boost::asio::experimental::awaitable_operators;

class NeoRadosWatchNotifyTest : public NeoRadosTest {
protected:
  buffer::list notify_bl;
  container::flat_set<uint64_t> notify_cookies;
  const std::string notify_oid = "foo"s;
  sys::error_code notify_err;
  ceph::timespan notify_sleep = 0s;
  bool should_reconnet = true;

  asio::awaitable<void> handle_notify(uint64_t notify_id, uint64_t cookie,
                                      uint64_t notifier_gid, buffer::list&& bl) {
    std::cout << __func__ << " cookie " << cookie << " notify_id " << notify_id
	      << " notifier_gid " << notifier_gid << std::endl;
    notify_bl = std::move(bl);
    notify_cookies.insert(cookie);
    if (notify_sleep > 0s) {
      std::cout << "Waiting for " << notify_sleep << std::endl;
      co_await wait_for(notify_sleep);
    }
    co_await rados().notify_ack(notify_oid, pool(), notify_id, cookie,
                                to_buffer_list("reply"sv), asio::use_awaitable);
  }

  asio::awaitable<void> handle_error(sys::error_code ec, uint64_t cookie) {
    std::cout << __func__ << " cookie " << cookie
              << " err " << ec.message() << std::endl;
    ceph_assert(cookie > 1000);
    if (!should_reconnet) {
      co_return;
    }
    co_await rados().unwatch(cookie, pool(), asio::use_awaitable);
    notify_cookies.erase(cookie);
    notify_err = ec;
    try {
      auto watchcookie
        = co_await rados().watch(notify_oid, pool(), std::nullopt,
                                 std::ref(*this), asio::use_awaitable);
      notify_cookies.insert(watchcookie);
    } catch (const sys::system_error& e) {
      std::cout << "reconnect error: " << e.what() << std::endl;
    }
  }

public:
  void operator ()(sys::error_code ec, uint64_t notify_id, uint64_t cookie,
                   uint64_t notifier_id, buffer::list&& bl) {
    asio::co_spawn(
      asio_context,
      [](NeoRadosWatchNotifyTest* t, sys::error_code ec, uint64_t notify_id,
         uint64_t cookie, uint64_t notifier_id, buffer::list bl)
      -> asio::awaitable<void> {
        if (ec) {
          co_await t->handle_error(ec, cookie);
        } else {
          co_await t->handle_notify(notify_id, cookie, notifier_id,
                                    std::move(bl));
        }
	co_return;
      }(this, ec, notify_id, cookie, notifier_id, std::move(bl)),
      [](std::exception_ptr e) {
	if (e) std::rethrow_exception(e);
      });
  }
};

CORO_TEST_F(NeoRadosWatchNotify, WatchNotify, NeoRadosWatchNotifyTest) {
  co_await create_obj(notify_oid);
  auto handle = co_await rados().watch(notify_oid, pool(), std::nullopt,
                                       std::ref(*this),
                                       asio::use_awaitable);
  EXPECT_TRUE(rados().check_watch(handle));
  std::vector<neorados::ObjWatcher> watchers;
  co_await execute(notify_oid, ReadOp{}.list_watchers(&watchers));
  EXPECT_EQ(1u, watchers.size());
  auto [reply_map, missed_set]
    = co_await rados().notify(notify_oid, pool(), {}, {},
			      asio::use_awaitable);
  EXPECT_EQ(1u, notify_cookies.size());
  EXPECT_EQ(1u, notify_cookies.count(handle));
  EXPECT_EQ(1u, reply_map.size());
  EXPECT_EQ(5u, reply_map.begin()->second.length());
  EXPECT_EQ(0, strncmp("reply", reply_map.begin()->second.c_str(), 5));
  EXPECT_EQ(0u, missed_set.size());
  EXPECT_TRUE(rados().check_watch(handle));
  co_await rados().unwatch(handle, pool(), asio::use_awaitable);

  co_return;
}

CORO_TEST_F(NeoRadosWatchNotify, WatchNotifyTimeout, NeoRadosWatchNotifyTest) {
  co_await create_obj(notify_oid);
  auto handle = co_await rados().watch(notify_oid, pool(), std::nullopt,
                                       std::ref(*this),
                                       asio::use_awaitable);
  EXPECT_TRUE(rados().check_watch(handle));
  std::vector<neorados::ObjWatcher> watchers;
  co_await execute(notify_oid, ReadOp{}.list_watchers(&watchers));
  EXPECT_EQ(1u, watchers.size());

  notify_sleep = 3s;

  std::cout << "Trying..." << std::endl;
  co_await expect_error_code(rados().notify(notify_oid, pool(), {}, 1s,
					    asio::use_awaitable),
			     sys::errc::timed_out);
  std::cout << "Timed out." << std::endl;

  should_reconnet = false; // Don't reconnect, we know we will timeout
  EXPECT_TRUE(rados().check_watch(handle));
  co_await rados().unwatch(handle, pool(), asio::use_awaitable);

  std::cout << "Flushing..." << std::endl;
  co_await rados().flush_watch(asio::use_awaitable);
  std::cout << "Flushed..." << std::endl;

  // Give time for notify_ack to fire before pool gets deleted.
  co_await wait_for(notify_sleep);

  co_return;
}

CORO_TEST_F(NeoRadosWatchNotifyPoll, WatchNotify, NeoRadosTest) {
  static constexpr auto oid = "obj"sv;
  co_await create_obj(oid);
  auto handle = co_await rados().watch(oid, pool(), asio::use_awaitable);
  EXPECT_TRUE(rados().check_watch(handle));
  std::vector<neorados::ObjWatcher> watchers;
  co_await execute(oid, ReadOp{}.list_watchers(&watchers));
  EXPECT_EQ(1u, watchers.size());
  auto notify = [](neorados::RADOS& r, neorados::IOContext ioc)
    -> asio::awaitable<void> {
    auto [reply_map, missed_set]
      = co_await r.notify(oid, ioc, {}, {}, asio::use_awaitable);

    EXPECT_EQ(1u, reply_map.size());
    EXPECT_EQ(5u, reply_map.begin()->second.length());
    EXPECT_EQ(0, strncmp("reply", reply_map.begin()->second.c_str(), 5));
    EXPECT_EQ(0u, missed_set.size());

    co_return;
  }(rados(), pool());
  auto poll = [](neorados::RADOS& r, neorados::IOContext ioc,
		 uint64_t handle) -> asio::awaitable<void> {
    auto notification = co_await r.next_notification(handle,
						     asio::use_awaitable);
    co_await r.notify_ack(oid, ioc, notification.notify_id, handle,
			  to_buffer_list("reply"sv), asio::use_awaitable);
    EXPECT_EQ(handle, notification.cookie);
  }(rados(), pool(), handle);

  co_await (std::move(notify) && std::move(poll));

  EXPECT_TRUE(rados().check_watch(handle));
  co_await rados().unwatch(handle, pool(), asio::use_awaitable);

  co_return;
}

CORO_TEST_F(NeoRadosWatchNotifyPoll, WatchNotifyTimeout, NeoRadosTest) {
  static constexpr auto oid = "obj"sv;
  static constexpr auto timeout = 1s;
  static constexpr auto delay = 3s;

  co_await create_obj(oid);
  auto handle = co_await rados().watch(oid, pool(), asio::use_awaitable);
  EXPECT_TRUE(rados().check_watch(handle));
  std::vector<neorados::ObjWatcher> watchers;
  co_await execute(oid, ReadOp{}.list_watchers(&watchers));
  EXPECT_EQ(1u, watchers.size());

  auto notify = [](neorados::RADOS& r, neorados::IOContext ioc)
    -> asio::awaitable<void> {
    co_await expect_error_code(r.notify(oid, ioc, {}, timeout,
					asio::use_awaitable),
			       sys::errc::timed_out);
  }(rados(), pool());

  auto ack_slowly = [](neorados::RADOS& r, neorados::IOContext ioc,
		       uint64_t handle) -> asio::awaitable<void> {
    auto notification = co_await r.next_notification(handle,
						     asio::use_awaitable);
    EXPECT_EQ(handle, notification.cookie);
    co_await wait_for(delay);
    co_await r.notify_ack(oid, ioc, notification.notify_id, handle,
			  to_buffer_list("reply"sv), asio::use_awaitable);
  }(rados(), pool(), handle);


  co_await (std::move(notify) || std::move(ack_slowly));

  EXPECT_TRUE(rados().check_watch(handle));
  co_await rados().unwatch(handle, pool(), asio::use_awaitable);

  co_await rados().flush_watch(asio::use_awaitable);

  co_return;
}

CORO_TEST_F(NeoRadosWatchNotifyPoll, WrongWatchType, NeoRadosTest) {
  static constexpr auto oid = "obj"sv;

  co_await create_obj(oid);
  auto handle = co_await rados().watch(oid, pool(), std::nullopt,
                                       [](auto&&...) { std::terminate(); },
                                       asio::use_awaitable);
  co_await expect_error_code(
    rados().next_notification(handle, asio::use_awaitable),
    neorados::errc::polled_callback_watch);
  co_await expect_error_code(
    rados().next_notification(handle, asio::use_awaitable),
    sys::errc::bad_file_descriptor);
  co_await expect_error_code(
    rados().next_notification(handle, asio::use_awaitable),
    sys::errc::invalid_argument);
}

CORO_TEST_F(NeoRadosWatchNotifyPoll, WatchNotifyCancel, NeoRadosTest) {
  static constexpr auto oid = "obj"sv;

  co_await create_obj(oid);
  auto handle = co_await rados().watch(oid, pool(), asio::use_awaitable);
  EXPECT_TRUE(rados().check_watch(handle));
  std::vector<neorados::ObjWatcher> watchers;
  co_await execute(oid, ReadOp{}.list_watchers(&watchers));
  EXPECT_EQ(1u, watchers.size());

  co_await (rados().next_notification(handle, asio::use_awaitable) ||
	    wait_for(50us));
  EXPECT_TRUE(rados().check_watch(handle));
  co_await rados().unwatch(handle, pool(), asio::use_awaitable);

  co_await rados().flush_watch(asio::use_awaitable);

  co_return;
}
