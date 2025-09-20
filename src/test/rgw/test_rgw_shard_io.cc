// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "driver/rados/shard_io.h"

#include <optional>
#include <limits>

#include <boost/asio/append.hpp>
#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/system/errc.hpp>

#include <gtest/gtest.h>

#include "global/global_context.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

using boost::system::error_code;
using boost::system::errc::io_error;
using boost::system::errc::no_such_file_or_directory;
using boost::system::errc::operation_canceled;
using boost::system::errc::resource_unavailable_try_again;

constexpr size_t infinite_aio = std::numeric_limits<size_t>::max();

template <typename T>
auto capture(std::optional<T>& opt)
{
  return [&opt] (T value) { opt = std::move(value); };
}

template <typename T>
auto capture(boost::asio::cancellation_signal& signal, std::optional<T>& opt)
{
  return boost::asio::bind_cancellation_slot(signal.slot(), capture(opt));
}

// handler wrapper that removes itself from a list on cancellation
template <typename Handler>
struct MockHandler :
    boost::intrusive::list_base_hook<
        boost::intrusive::link_mode<
            boost::intrusive::auto_unlink>> {
  Handler handler;

  struct Cancel {
    MockHandler* self;
    explicit Cancel(MockHandler* self) : self(self) {}

    void operator()(boost::asio::cancellation_type type) {
      if (!!(type & boost::asio::cancellation_type::terminal)) {
        auto tmp = std::move(self->handler);
        delete self; // auto unlink
        auto ec = make_error_code(operation_canceled);
        boost::asio::dispatch(boost::asio::append(std::move(tmp), ec));
      }
    }
  };

  MockHandler(Handler&& h) : handler(std::move(h)) {
    auto slot = boost::asio::get_associated_cancellation_slot(handler);
    if (slot.is_connected()) {
      slot.template emplace<Cancel>(this);
    }
  }

  void operator()(error_code ec) {
    auto slot = boost::asio::get_associated_cancellation_slot(handler);
    slot.clear();
    std::move(handler)(ec);
  }

  const Handler* operator->() const { return &handler; }
};

namespace boost::asio {

// forward wrapped handler's associations
template <template <typename, typename> class Associator,
    typename Handler, typename DefaultCandidate>
struct associator<Associator, MockHandler<Handler>, DefaultCandidate>
  : Associator<Handler, DefaultCandidate>
{
  static auto get(const MockHandler<Handler>& h) noexcept {
    return Associator<Handler, DefaultCandidate>::get(h.handler);
  }
  static auto get(const MockHandler<Handler>& h,
                  const DefaultCandidate& c) noexcept {
    return Associator<Handler, DefaultCandidate>::get(h.handler, c);
  }
};

} // namespace boost::asio

template <typename Handler>
using MockHandlerList = boost::intrusive::list<MockHandler<Handler>,
      boost::intrusive::constant_time_size<false>>; // required by auto_unlink

template <typename Handler>
static void complete_at(MockHandlerList<Handler>& handlers,
                        size_t index, error_code ec)
{
  auto i = std::next(handlers.begin(), index);
  auto h = std::move(*i);
  handlers.erase_and_dispose(i, std::default_delete<MockHandler<Handler>>{});
  boost::asio::dispatch(boost::asio::append(std::move(h), ec));
}

namespace rgwrados::shard_io {

struct MockRevertibleWriter : RevertibleWriter {
  MockHandlerList<detail::RevertibleWriteHandler> writes;
  MockHandlerList<detail::RevertHandler> reverts;

  using RevertibleWriter::RevertibleWriter;

  ~MockRevertibleWriter() {
    writes.clear_and_dispose(std::default_delete<
        MockHandler<detail::RevertibleWriteHandler>>{});
    reverts.clear_and_dispose(std::default_delete<
        MockHandler<detail::RevertHandler>>{});
  }

  void write(int shard, const std::string& object,
             detail::RevertibleWriteHandler&& handler) override {
    auto h = new MockHandler<detail::RevertibleWriteHandler>(std::move(handler));
    writes.push_back(*h);
  }
  void revert(int shard, const std::string& object,
              detail::RevertHandler&& handler) override {
    auto h = new MockHandler<detail::RevertHandler>(std::move(handler));
    reverts.push_back(*h);
  }
  Result on_complete(int, error_code ec) override {
    if (ec == resource_unavailable_try_again) {
      return Result::Retry;
    } else if (ec) {
      return Result::Error;
    } else {
      return Result::Success;
    }
  }
};

TEST(RevertibleWriter, empty)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{}; // no objects

  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, writer.writes.size());
  EXPECT_EQ(0, writer.reverts.size());
}

TEST(RevertibleWriter, co_spawn)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{}; // no objects

  std::optional<std::exception_ptr> eptr;
  boost::asio::co_spawn(ex,
                        async_writes(writer, objects, infinite_aio,
                                     boost::asio::use_awaitable),
                        capture(eptr));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(eptr);
  EXPECT_FALSE(*eptr);
  EXPECT_EQ(0, writer.writes.size());
  EXPECT_EQ(0, writer.reverts.size());
}

TEST(RevertibleWriter, spawn)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{}; // no objects

  std::optional<std::exception_ptr> eptr;
  boost::asio::spawn(ex, [&] (boost::asio::yield_context yield) {
        async_writes(writer, objects, infinite_aio, yield);
      }, capture(eptr));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(eptr);
  EXPECT_FALSE(*eptr);
  EXPECT_EQ(0, writer.writes.size());
  EXPECT_EQ(0, writer.reverts.size());
}

TEST(RevertibleWriter, throttle)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  std::optional<error_code> ec;
  async_writes(writer, objects, max_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  EXPECT_EQ(1, writer.writes.back()->shard.id());

  complete_at(writer.writes, 1, error_code{}); // complete shard 1

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  EXPECT_EQ(2, writer.writes.back()->shard.id());

  complete_at(writer.writes, 1, error_code{}); // complete shard 2

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  EXPECT_EQ(3, writer.writes.back()->shard.id());

  complete_at(writer.writes, 1, error_code{}); // complete shard 3

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());

  complete_at(writer.writes, 0, error_code{}); // complete shard 0

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, writer.writes.size());
  EXPECT_EQ(0, writer.reverts.size());
}

TEST(RevertibleWriter, retry_success)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{{0, "obj0"}, {1, "obj1"}};

  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  EXPECT_EQ(1, writer.writes.back()->shard.id());

  complete_at(writer.writes, 0, make_error_code(resource_unavailable_try_again));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(1, writer.writes.front()->shard.id());
  EXPECT_EQ(0, writer.writes.back()->shard.id());

  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());

  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(RevertibleWriter, retry_error)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{{0, "obj0"}, {1, "obj1"}};

  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  EXPECT_EQ(1, writer.writes.back()->shard.id());
  ASSERT_EQ(0, writer.reverts.size());

  complete_at(writer.writes, 0, make_error_code(resource_unavailable_try_again));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(1, writer.writes.front()->shard.id());
  EXPECT_EQ(0, writer.writes.back()->shard.id());
  ASSERT_EQ(0, writer.reverts.size());

  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  ASSERT_EQ(0, writer.reverts.size());

  // retry error on shard 0 triggers revert of shard 1
  complete_at(writer.writes, 0, make_error_code(no_such_file_or_directory));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(0, writer.writes.size());
  ASSERT_EQ(1, writer.reverts.size());
  EXPECT_EQ(1, writer.reverts.front()->shard.id());

  complete_at(writer.reverts, 0, error_code{});

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(no_such_file_or_directory, *ec);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(RevertibleWriter, retry_success_other_error)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{{0, "obj0"}, {1, "obj1"}};

  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  EXPECT_EQ(1, writer.writes.back()->shard.id());

  complete_at(writer.writes, 0, make_error_code(resource_unavailable_try_again));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(1, writer.writes.front()->shard.id());
  EXPECT_EQ(0, writer.writes.back()->shard.id());
  ASSERT_EQ(0, writer.reverts.size());

  complete_at(writer.writes, 0, make_error_code(no_such_file_or_directory));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  ASSERT_EQ(0, writer.reverts.size());

  // successful retry of shard 0 triggers revert due to error from shard 1
  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(0, writer.writes.size());
  ASSERT_EQ(1, writer.reverts.size());
  EXPECT_EQ(0, writer.reverts.front()->shard.id());

  complete_at(writer.reverts, 0, error_code{});

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(no_such_file_or_directory, *ec);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(RevertibleWriter, retry_throttle)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  std::optional<error_code> ec;
  async_writes(writer, objects, max_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  EXPECT_EQ(1, writer.writes.back()->shard.id());
  ASSERT_EQ(0, writer.reverts.size());

  complete_at(writer.writes, 0, error_code{});
  complete_at(writer.writes, 0, make_error_code(resource_unavailable_try_again));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(2, writer.writes.front()->shard.id());
  EXPECT_EQ(3, writer.writes.back()->shard.id());
  ASSERT_EQ(0, writer.reverts.size());

  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(3, writer.writes.front()->shard.id());
  EXPECT_EQ(1, writer.writes.back()->shard.id());
  ASSERT_EQ(0, writer.reverts.size());

  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, writer.writes.size());
  EXPECT_EQ(1, writer.writes.front()->shard.id());
  ASSERT_EQ(0, writer.reverts.size());

  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, writer.writes.size());
  ASSERT_EQ(0, writer.reverts.size());
}

TEST(RevertibleWriter, revert)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};

  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(4, writer.writes.size());

  // complete shards 2 and 0
  complete_at(writer.writes, 2, error_code{});
  complete_at(writer.writes, 0, error_code{});
  // fail shard 1
  complete_at(writer.writes, 0, make_error_code(no_such_file_or_directory));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, writer.writes.size());
  EXPECT_EQ(3, writer.writes.front()->shard.id());
  ASSERT_EQ(2, writer.reverts.size());
  EXPECT_EQ(2, writer.reverts.front()->shard.id());
  EXPECT_EQ(0, writer.reverts.back()->shard.id());

  complete_at(writer.writes, 0, error_code{}); // complete shard 3

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(0, writer.writes.size());
  ASSERT_EQ(3, writer.reverts.size());
  EXPECT_EQ(2, writer.reverts.front()->shard.id());
  EXPECT_EQ(3, writer.reverts.back()->shard.id());

  complete_at(writer.reverts, 2, error_code{});
  complete_at(writer.reverts, 1, make_error_code(io_error));
  complete_at(writer.reverts, 0, error_code{});

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(no_such_file_or_directory, *ec);
  EXPECT_EQ(0, writer.writes.size());
  EXPECT_EQ(0, writer.reverts.size());
}

TEST(RevertibleWriter, throttle_all_fail)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  std::optional<error_code> ec;
  async_writes(writer, objects, max_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());

  complete_at(writer.writes, 0, make_error_code(no_such_file_or_directory));
  complete_at(writer.writes, 0, make_error_code(io_error));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(no_such_file_or_directory, *ec);
  EXPECT_EQ(0, writer.writes.size());
  EXPECT_EQ(0, writer.reverts.size());
}

TEST(RevertibleWriter, drain_all_fail)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};

  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(4, writer.writes.size());

  complete_at(writer.writes, 0, make_error_code(no_such_file_or_directory));
  complete_at(writer.writes, 0, make_error_code(io_error));
  complete_at(writer.writes, 0, make_error_code(io_error));
  complete_at(writer.writes, 0, make_error_code(io_error));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(no_such_file_or_directory, *ec);
  EXPECT_EQ(0, writer.writes.size());
  EXPECT_EQ(0, writer.reverts.size());
}

TEST(RevertibleWriter, throttle_cancel_total)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  boost::asio::cancellation_signal signal;
  std::optional<error_code> ec;
  async_writes(writer, objects, max_aio, capture(signal, ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());

  signal.emit(boost::asio::cancellation_type::total);

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());

  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, writer.writes.size());
  ASSERT_EQ(1, writer.reverts.size());

  // write completions after cancellation still get reverted
  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(0, writer.writes.size());
  ASSERT_EQ(2, writer.reverts.size());

  complete_at(writer.reverts, 0, error_code{});
  complete_at(writer.reverts, 0, error_code{});

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(operation_canceled, *ec);
  EXPECT_EQ(0, writer.writes.size());
  EXPECT_EQ(0, writer.reverts.size());
}

TEST(RevertibleWriter, throttle_cancel_total_then_terminal)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  boost::asio::cancellation_signal signal;
  std::optional<error_code> ec;
  async_writes(writer, objects, max_aio, capture(signal, ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());

  // total cancellation triggers reverts
  signal.emit(boost::asio::cancellation_type::total);

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());

  complete_at(writer.writes, 0, error_code{});
  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(0, writer.writes.size());
  ASSERT_EQ(2, writer.reverts.size());

  // terminal cancellation cancels reverts
  signal.emit(boost::asio::cancellation_type::terminal);

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(operation_canceled, *ec);
  EXPECT_EQ(0, writer.writes.size());
  EXPECT_EQ(0, writer.reverts.size());
}

TEST(RevertibleWriter, throttle_cancel_terminal)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  boost::asio::cancellation_signal signal;
  std::optional<error_code> ec;
  async_writes(writer, objects, max_aio, capture(signal, ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());

  signal.emit(boost::asio::cancellation_type::terminal);

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(operation_canceled, *ec);
  EXPECT_EQ(0, writer.writes.size());
  EXPECT_EQ(0, writer.reverts.size());
}

TEST(RevertibleWriter, drain_cancel_total)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};

  boost::asio::cancellation_signal signal;
  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(signal, ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(4, writer.writes.size());

  signal.emit(boost::asio::cancellation_type::total);

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(4, writer.writes.size());

  complete_at(writer.writes, 0, error_code{});
  complete_at(writer.writes, 0, error_code{});
  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, writer.writes.size());
  ASSERT_EQ(3, writer.reverts.size());

  // write completions after cancellation still get reverted
  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(0, writer.writes.size());
  ASSERT_EQ(4, writer.reverts.size());

  complete_at(writer.reverts, 0, error_code{});
  complete_at(writer.reverts, 0, error_code{});
  complete_at(writer.reverts, 0, error_code{});
  complete_at(writer.reverts, 0, error_code{});

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(operation_canceled, *ec);
  EXPECT_EQ(0, writer.writes.size());
  EXPECT_EQ(0, writer.reverts.size());
}

TEST(RevertibleWriter, drain_cancel_total_then_terminal)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};

  boost::asio::cancellation_signal signal;
  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(signal, ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(4, writer.writes.size());

  // total cancellation triggers reverts
  signal.emit(boost::asio::cancellation_type::total);

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(4, writer.writes.size());

  complete_at(writer.writes, 0, error_code{});
  complete_at(writer.writes, 0, error_code{});
  complete_at(writer.writes, 0, error_code{});
  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(0, writer.writes.size());
  ASSERT_EQ(4, writer.reverts.size());

  // terminal cancellation cancels reverts
  signal.emit(boost::asio::cancellation_type::terminal);

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(operation_canceled, *ec);
  EXPECT_EQ(0, writer.writes.size());
  EXPECT_EQ(0, writer.reverts.size());
}

TEST(RevertibleWriter, drain_cancel_terminal)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockRevertibleWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};

  boost::asio::cancellation_signal signal;
  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(signal, ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(4, writer.writes.size());

  signal.emit(boost::asio::cancellation_type::terminal);

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(operation_canceled, *ec);
  EXPECT_EQ(0, writer.writes.size());
  EXPECT_EQ(0, writer.reverts.size());
}


struct MockWriter : Writer {
  MockHandlerList<detail::WriteHandler> writes;

  using Writer::Writer;

  ~MockWriter() {
    writes.clear_and_dispose(std::default_delete<
        MockHandler<detail::WriteHandler>>{});
  }

  void write(int shard, const std::string& object,
             detail::WriteHandler&& handler) override {
    auto h = new MockHandler<detail::WriteHandler>(std::move(handler));
    writes.push_back(*h);
  }
  Result on_complete(int, error_code ec) override {
    if (ec == resource_unavailable_try_again) {
      return Result::Retry;
    } else if (ec) {
      return Result::Error;
    } else {
      return Result::Success;
    }
  }
};

TEST(Writer, empty)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{}; // no objects

  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(Writer, co_spawn)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{}; // no objects

  std::optional<std::exception_ptr> eptr;
  boost::asio::co_spawn(ex,
                        async_writes(writer, objects, infinite_aio,
                                     boost::asio::use_awaitable),
                        capture(eptr));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(eptr);
  EXPECT_FALSE(*eptr);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(Writer, spawn)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{}; // no objects

  std::optional<std::exception_ptr> eptr;
  boost::asio::spawn(ex, [&] (boost::asio::yield_context yield) {
        async_writes(writer, objects, infinite_aio, yield);
      }, capture(eptr));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(eptr);
  EXPECT_FALSE(*eptr);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(Writer, throttle)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  std::optional<error_code> ec;
  async_writes(writer, objects, max_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  EXPECT_EQ(1, writer.writes.back()->shard.id());

  complete_at(writer.writes, 1, error_code{}); // complete shard 1

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  EXPECT_EQ(2, writer.writes.back()->shard.id());

  complete_at(writer.writes, 1, error_code{}); // complete shard 2

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  EXPECT_EQ(3, writer.writes.back()->shard.id());

  complete_at(writer.writes, 1, error_code{}); // complete shard 3

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());

  complete_at(writer.writes, 0, error_code{}); // complete shard 0

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(Writer, retry_success)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{{0, "obj0"}, {1, "obj1"}};

  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  EXPECT_EQ(1, writer.writes.back()->shard.id());

  complete_at(writer.writes, 0, make_error_code(resource_unavailable_try_again));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(1, writer.writes.front()->shard.id());
  EXPECT_EQ(0, writer.writes.back()->shard.id());

  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());

  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(Writer, retry_error)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{{0, "obj0"}, {1, "obj1"}};

  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  EXPECT_EQ(1, writer.writes.back()->shard.id());

  complete_at(writer.writes, 0, make_error_code(resource_unavailable_try_again));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(1, writer.writes.front()->shard.id());
  EXPECT_EQ(0, writer.writes.back()->shard.id());

  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());

  complete_at(writer.writes, 0, make_error_code(no_such_file_or_directory));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(no_such_file_or_directory, *ec);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(Writer, retry_success_other_error)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{{0, "obj0"}, {1, "obj1"}};

  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  EXPECT_EQ(1, writer.writes.back()->shard.id());

  complete_at(writer.writes, 0, make_error_code(resource_unavailable_try_again));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(1, writer.writes.front()->shard.id());
  EXPECT_EQ(0, writer.writes.back()->shard.id());

  complete_at(writer.writes, 0, make_error_code(no_such_file_or_directory));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());

  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(no_such_file_or_directory, *ec);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(Writer, retry_throttle)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  std::optional<error_code> ec;
  async_writes(writer, objects, max_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(0, writer.writes.front()->shard.id());
  EXPECT_EQ(1, writer.writes.back()->shard.id());

  complete_at(writer.writes, 0, error_code{});
  complete_at(writer.writes, 0, make_error_code(resource_unavailable_try_again));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(2, writer.writes.front()->shard.id());
  EXPECT_EQ(3, writer.writes.back()->shard.id());

  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());
  EXPECT_EQ(3, writer.writes.front()->shard.id());
  EXPECT_EQ(1, writer.writes.back()->shard.id());

  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, writer.writes.size());
  EXPECT_EQ(1, writer.writes.front()->shard.id());

  complete_at(writer.writes, 0, error_code{});

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(Writer, throttle_errors)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  std::optional<error_code> ec;
  async_writes(writer, objects, max_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());

  complete_at(writer.writes, 0, error_code{});
  complete_at(writer.writes, 0, make_error_code(no_such_file_or_directory));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());

  complete_at(writer.writes, 0, error_code{});
  complete_at(writer.writes, 0, make_error_code(io_error));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(no_such_file_or_directory, *ec);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(Writer, drain_errors)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};

  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(4, writer.writes.size());

  complete_at(writer.writes, 0, error_code{});
  complete_at(writer.writes, 0, make_error_code(no_such_file_or_directory));
  complete_at(writer.writes, 0, error_code{});
  complete_at(writer.writes, 0, make_error_code(io_error));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(no_such_file_or_directory, *ec);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(Writer, throttle_cancel_total)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  boost::asio::cancellation_signal signal;
  std::optional<error_code> ec;
  async_writes(writer, objects, max_aio, capture(signal, ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());

  signal.emit(boost::asio::cancellation_type::total); // noop

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());

  complete_at(writer.writes, 0, error_code{}); // shard 0
  complete_at(writer.writes, 0, error_code{}); // shard 1

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());

  complete_at(writer.writes, 0, error_code{}); // shard 2
  complete_at(writer.writes, 0, error_code{}); // shard 3

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(Writer, throttle_cancel_terminal)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  boost::asio::cancellation_signal signal;
  std::optional<error_code> ec;
  async_writes(writer, objects, max_aio, capture(signal, ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());

  signal.emit(boost::asio::cancellation_type::terminal);

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(operation_canceled, *ec);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(Writer, drain_cancel_total)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};

  boost::asio::cancellation_signal signal;
  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(signal, ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(4, writer.writes.size());

  signal.emit(boost::asio::cancellation_type::total); // noop

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(4, writer.writes.size());

  complete_at(writer.writes, 0, error_code{}); // shard 0
  complete_at(writer.writes, 0, error_code{}); // shard 1

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, writer.writes.size());

  complete_at(writer.writes, 0, error_code{}); // shard 2
  complete_at(writer.writes, 0, error_code{}); // shard 3

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, writer.writes.size());
}

TEST(Writer, drain_cancel_terminal)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto writer = MockWriter{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};

  boost::asio::cancellation_signal signal;
  std::optional<error_code> ec;
  async_writes(writer, objects, infinite_aio, capture(signal, ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(4, writer.writes.size());

  signal.emit(boost::asio::cancellation_type::terminal);

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(operation_canceled, *ec);
  EXPECT_EQ(0, writer.writes.size());
}


struct MockReader : Reader {
  MockHandlerList<detail::ReadHandler> reads;

  using Reader::Reader;

  ~MockReader() {
    reads.clear_and_dispose(std::default_delete<
        MockHandler<detail::ReadHandler>>{});
  }

  void read(int shard, const std::string& object,
            detail::ReadHandler&& handler) override {
    auto h = new MockHandler<detail::ReadHandler>(std::move(handler));
    reads.push_back(*h);
  }
  Result on_complete(int, error_code ec) override {
    if (ec == resource_unavailable_try_again) {
      return Result::Retry;
    } else if (ec) {
      return Result::Error;
    } else {
      return Result::Success;
    }
  }
};

TEST(Reader, empty)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto reader = MockReader{dpp, ex};
  const auto objects = std::map<int, std::string>{}; // no objects

  std::optional<error_code> ec;
  async_reads(reader, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, reader.reads.size());
}

TEST(Reader, co_spawn)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto reader = MockReader{dpp, ex};
  const auto objects = std::map<int, std::string>{}; // no objects

  std::optional<std::exception_ptr> eptr;
  boost::asio::co_spawn(ex,
                        async_reads(reader, objects, infinite_aio,
                                    boost::asio::use_awaitable),
                        capture(eptr));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(eptr);
  EXPECT_FALSE(*eptr);
  EXPECT_EQ(0, reader.reads.size());
}

TEST(Reader, spawn)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto reader = MockReader{dpp, ex};
  const auto objects = std::map<int, std::string>{}; // no objects

  std::optional<std::exception_ptr> eptr;
  boost::asio::spawn(ex, [&] (boost::asio::yield_context yield) {
        async_reads(reader, objects, infinite_aio, yield);
      }, capture(eptr));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(eptr);
  EXPECT_FALSE(*eptr);
  EXPECT_EQ(0, reader.reads.size());
}

TEST(Reader, throttle)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto reader = MockReader{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  std::optional<error_code> ec;
  async_reads(reader, objects, max_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());
  EXPECT_EQ(0, reader.reads.front()->shard.id());
  EXPECT_EQ(1, reader.reads.back()->shard.id());

  complete_at(reader.reads, 1, error_code{}); // complete shard 1

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());
  EXPECT_EQ(0, reader.reads.front()->shard.id());
  EXPECT_EQ(2, reader.reads.back()->shard.id());

  complete_at(reader.reads, 1, error_code{}); // complete shard 2

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());
  EXPECT_EQ(0, reader.reads.front()->shard.id());
  EXPECT_EQ(3, reader.reads.back()->shard.id());

  complete_at(reader.reads, 1, error_code{}); // complete shard 3

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, reader.reads.size());
  EXPECT_EQ(0, reader.reads.front()->shard.id());

  complete_at(reader.reads, 0, error_code{}); // complete shard 0

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, reader.reads.size());
}

TEST(Reader, retry_success)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto reader = MockReader{dpp, ex};
  const auto objects = std::map<int, std::string>{{0, "obj0"}, {1, "obj1"}};

  std::optional<error_code> ec;
  async_reads(reader, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());
  EXPECT_EQ(0, reader.reads.front()->shard.id());
  EXPECT_EQ(1, reader.reads.back()->shard.id());

  complete_at(reader.reads, 0, make_error_code(resource_unavailable_try_again));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());
  EXPECT_EQ(1, reader.reads.front()->shard.id());
  EXPECT_EQ(0, reader.reads.back()->shard.id());

  complete_at(reader.reads, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, reader.reads.size());
  EXPECT_EQ(0, reader.reads.front()->shard.id());

  complete_at(reader.reads, 0, error_code{});

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, reader.reads.size());
}

TEST(Reader, retry_error)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto reader = MockReader{dpp, ex};
  const auto objects = std::map<int, std::string>{{0, "obj0"}, {1, "obj1"}};

  std::optional<error_code> ec;
  async_reads(reader, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());
  EXPECT_EQ(0, reader.reads.front()->shard.id());
  EXPECT_EQ(1, reader.reads.back()->shard.id());

  complete_at(reader.reads, 0, make_error_code(resource_unavailable_try_again));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());
  EXPECT_EQ(1, reader.reads.front()->shard.id());
  EXPECT_EQ(0, reader.reads.back()->shard.id());

  complete_at(reader.reads, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, reader.reads.size());
  EXPECT_EQ(0, reader.reads.front()->shard.id());

  complete_at(reader.reads, 0, make_error_code(no_such_file_or_directory));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(no_such_file_or_directory, *ec);
  EXPECT_EQ(0, reader.reads.size());
}

TEST(Reader, retry_then_error)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto reader = MockReader{dpp, ex};
  const auto objects = std::map<int, std::string>{{0, "obj0"}, {1, "obj1"}};

  std::optional<error_code> ec;
  async_reads(reader, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());
  EXPECT_EQ(0, reader.reads.front()->shard.id());
  EXPECT_EQ(1, reader.reads.back()->shard.id());

  complete_at(reader.reads, 0, make_error_code(resource_unavailable_try_again));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());
  EXPECT_EQ(1, reader.reads.front()->shard.id());
  EXPECT_EQ(0, reader.reads.back()->shard.id());

  complete_at(reader.reads, 0, make_error_code(no_such_file_or_directory));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(no_such_file_or_directory, *ec);
  EXPECT_EQ(0, reader.reads.size());
}

TEST(Reader, retry_throttle)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto reader = MockReader{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  std::optional<error_code> ec;
  async_reads(reader, objects, max_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());
  EXPECT_EQ(0, reader.reads.front()->shard.id());
  EXPECT_EQ(1, reader.reads.back()->shard.id());

  complete_at(reader.reads, 0, error_code{});
  complete_at(reader.reads, 0, make_error_code(resource_unavailable_try_again));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());
  EXPECT_EQ(2, reader.reads.front()->shard.id());
  EXPECT_EQ(3, reader.reads.back()->shard.id());

  complete_at(reader.reads, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());
  EXPECT_EQ(3, reader.reads.front()->shard.id());
  EXPECT_EQ(1, reader.reads.back()->shard.id());

  complete_at(reader.reads, 0, error_code{});

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(1, reader.reads.size());
  EXPECT_EQ(1, reader.reads.front()->shard.id());

  complete_at(reader.reads, 0, error_code{});

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, reader.reads.size());
}

TEST(Reader, throttle_errors)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto reader = MockReader{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  std::optional<error_code> ec;
  async_reads(reader, objects, max_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());

  complete_at(reader.reads, 0, error_code{});
  complete_at(reader.reads, 0, make_error_code(no_such_file_or_directory));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(no_such_file_or_directory, *ec);
  EXPECT_EQ(0, reader.reads.size());
}

TEST(Reader, drain_errors)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto reader = MockReader{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};

  std::optional<error_code> ec;
  async_reads(reader, objects, infinite_aio, capture(ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(4, reader.reads.size());

  complete_at(reader.reads, 0, error_code{});
  complete_at(reader.reads, 0, make_error_code(no_such_file_or_directory));

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(no_such_file_or_directory, *ec);
  EXPECT_EQ(0, reader.reads.size());
}

TEST(Reader, throttle_cancel_total)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto reader = MockReader{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  boost::asio::cancellation_signal signal;
  std::optional<error_code> ec;
  async_reads(reader, objects, max_aio, capture(signal, ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());

  signal.emit(boost::asio::cancellation_type::total); // noop

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());

  complete_at(reader.reads, 0, error_code{}); // shard 0
  complete_at(reader.reads, 0, error_code{}); // shard 1

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());

  complete_at(reader.reads, 0, error_code{}); // shard 2
  complete_at(reader.reads, 0, error_code{}); // shard 3

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, reader.reads.size());
}

TEST(Reader, throttle_cancel_terminal)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto reader = MockReader{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};
  constexpr size_t max_aio = 2;

  boost::asio::cancellation_signal signal;
  std::optional<error_code> ec;
  async_reads(reader, objects, max_aio, capture(signal, ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());

  signal.emit(boost::asio::cancellation_type::terminal);

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(operation_canceled, *ec);
  EXPECT_EQ(0, reader.reads.size());
}

TEST(Reader, drain_cancel_total)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto reader = MockReader{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};

  boost::asio::cancellation_signal signal;
  std::optional<error_code> ec;
  async_reads(reader, objects, infinite_aio, capture(signal, ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(4, reader.reads.size());

  signal.emit(boost::asio::cancellation_type::total); // noop

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(4, reader.reads.size());

  complete_at(reader.reads, 0, error_code{}); // shard 0
  complete_at(reader.reads, 0, error_code{}); // shard 1

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(2, reader.reads.size());

  complete_at(reader.reads, 0, error_code{}); // shard 2
  complete_at(reader.reads, 0, error_code{}); // shard 3

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_FALSE(*ec);
  EXPECT_EQ(0, reader.reads.size());
}

TEST(Reader, drain_cancel_terminal)
{
  const auto dpp = NoDoutPrefix{dout_context, dout_subsys};
  boost::asio::io_context service;
  auto ex = service.get_executor();

  auto reader = MockReader{dpp, ex};
  const auto objects = std::map<int, std::string>{
    {0, "obj0"}, {1, "obj1"}, {2, "obj2"}, {3, "obj3"}};

  boost::asio::cancellation_signal signal;
  std::optional<error_code> ec;
  async_reads(reader, objects, infinite_aio, capture(signal, ec));

  service.poll();
  ASSERT_FALSE(service.stopped());
  ASSERT_FALSE(ec);
  ASSERT_EQ(4, reader.reads.size());

  signal.emit(boost::asio::cancellation_type::terminal);

  service.poll();
  ASSERT_TRUE(service.stopped());
  ASSERT_TRUE(ec);
  EXPECT_EQ(operation_canceled, *ec);
  EXPECT_EQ(0, reader.reads.size());
}

} // namespace rgwrados::shard_io
