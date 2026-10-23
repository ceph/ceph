// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef TEST_RGW_FDB_CONCURRENCY_H
 #define TEST_RGW_FDB_CONCURRENCY_H

#include "test_fdb_common.h"
#include "rgw/ceph_fdb_execution.h"

#include <catch2/generators/catch_generators.hpp>
#include <catch2/generators/catch_generators_range.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <concepts>
#include <cstring>
#include <exception>
#include <memory>
#include <new>
#include <ranges>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include <boost/asio/bind_allocator.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>

namespace lfdbx = ceph::libfdb::experimental;

// Supply a native successful operation for backend-independent contract tests.
template <typename ValueT>
auto native_value(ValueT value)
{
#if defined(CEPH_LIBFDB_EXPERIMENTAL_NVIDIA_STDEXEC)
 return lfdbx::operation {stdexec::just(std::move(value))};
#else
 auto token = boost::asio::deferred;

 return lfdbx::operation {boost::asio::async_initiate<decltype(token),
  void(std::exception_ptr, std::optional<ValueT>)>(
  [](auto handler, ValueT value) {
   std::move(handler)(std::exception_ptr {}, std::optional<ValueT> {std::move(value)});
  }, token, std::move(value))};
#endif
}

inline std::string_view as_string_view(const std::span<const std::uint8_t> bytes)
{
 return {reinterpret_cast<const char *>(bytes.data()), std::size(bytes)};
}

inline void write_bytes(lfdb::transaction_handle txn, const std::string_view key, const std::string_view value)
{
 lfdb::detail::transaction_set_kv_bytes(txn, lfdb::detail::as_byte_view(key),
                                       lfdb::detail::as_byte_view(value));
}

inline void check_operation_aborted(const std::exception_ptr& error)
{
 REQUIRE(error);

 try {
  std::rethrow_exception(error);
 } catch (const boost::system::system_error& failure) {
  CHECK(boost::asio::error::operation_aborted == failure.code());
 }
}

inline void delay_reads(lfdb::database_handle dbh, const lfdb::transaction_handle& txn)
{
 auto version_txn = lfdb::make_transaction(std::move(dbh));
 lfdb::set_read_version(txn, lfdb::read_version(version_txn) + 100'000);
}

void check_active_cancellation(auto operation,
                               const boost::asio::cancellation_type type =
                                boost::asio::cancellation_type::partial)
{
 boost::asio::io_context context;
 boost::asio::cancellation_signal cancel;
 std::exception_ptr error;
 bool completed = false;

 lfdbx::async_wait(std::move(operation), boost::asio::bind_cancellation_slot(cancel.slot(),
   boost::asio::bind_executor(context.get_executor(),
     [&](std::exception_ptr failure, auto result) {
      completed = true;
      error = std::move(failure);
      CHECK_FALSE(result);
     })));
 cancel.emit(type);
 REQUIRE(context.run());
 REQUIRE(completed);
 check_operation_aborted(error);
}

TEST_CASE("Scan metadata handles unaligned native records without copying bytes",
          "[fdb][concurrency][alignment]")
{
 const std::string key = "key";
 const std::string value = "value";
 const FDBKeyValue record {
  reinterpret_cast<const std::uint8_t *>(key.data()), static_cast<int>(key.size()),
  reinterpret_cast<const std::uint8_t *>(value.data()), static_cast<int>(value.size())
 };
 alignas(FDBKeyValue) std::array<std::byte, sizeof(FDBKeyValue) + 1> bytes;
 std::memcpy(bytes.data() + 1, &record, sizeof(record));
 std::unique_ptr<FDBKeyValue[]> storage;
 auto rows = lfdb::detail::aligned_result_pairs(
   reinterpret_cast<const FDBKeyValue *>(bytes.data() + 1), 1, storage);
 REQUIRE(1 == rows.size());
 REQUIRE(storage);
 // Packed pointer fields must be loaded before Catch binds operand references:
 CHECK(static_cast<const void *>(record.key) == static_cast<const void *>(rows[0].key));
 CHECK(static_cast<const void *>(record.value) == static_cast<const void *>(rows[0].value));
 auto owner = std::move(storage);
 CHECK(static_cast<const void *>(record.key) == static_cast<const void *>(rows[0].key));
 CHECK(static_cast<const void *>(record.value) == static_cast<const void *>(rows[0].value));

 std::unique_ptr<FDBKeyValue[]> unused;
 auto aligned = lfdb::detail::aligned_result_pairs(&record, 1, unused);
 CHECK(&record == aligned.data());
 CHECK_FALSE(unused);
}

TEST_CASE("Experimental transactions replay with isolated staged output", "[fdb][concurrency]")
{
 janitor dbh;
 const auto key = test_key("concurrency/transact/replay");
 int published = 5;
 int attempts = 0;
 lfdb::transaction_handle first_transaction;
 lfdb::transaction_options options {{FDB_TR_OPTION_MAX_RETRY_DELAY, std::int64_t {1}}};
 auto work = lfdbx::transact(dbh, options,
   [&](lfdb::transaction_handle& txn, auto& output) {
    CHECK(5 == published);
    output += ++attempts;

    if (1 == attempts) {
     first_transaction = txn;
     throw lfdb::libfdb_exception {lfdb::fdb_error_code::not_committed};
    }

    CHECK(first_transaction == txn);
    write_bytes(txn, key, "replayed");

    return lfdbx::get(lfdb::raw, txn, key);
   }, lfdb::staged(published));
 CHECK(0 == attempts);
 auto result = lfdbx::wait(std::move(work), null_yield);
 CHECK(2 == attempts);
 CHECK(7 == published);
 REQUIRE(result.bytes());
 CHECK("replayed" == as_string_view(*result.bytes()));
 CHECK(lfdb::key_exists(dbh, key));
}

TEST_CASE("Experimental transactions own ordered options across initiation and replay",
          "[fdb][concurrency][options]")
{
 janitor dbh;

 for (const bool transfer : {false, true}) {
  CAPTURE(transfer);
  const auto key = test_key(transfer ? "options/moved" : "options/copied");
  lfdb::transaction_options options {
   {FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag},
   {FDB_TR_OPTION_TAG, "a"},
   {FDB_TR_OPTION_MAX_RETRY_DELAY, std::int64_t {1}},
   {FDB_TR_OPTION_TAG, "b"},
   {FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag}
  };
  int attempts = 0;
  lfdb::transaction_handle first_transaction;
  const auto body = [&](lfdb::transaction_handle& txn) {
   ++attempts;

   if (1 == attempts) {
    first_transaction = txn;
    throw lfdb::libfdb_exception {lfdb::fdb_error_code::not_committed};
   }

   CHECK(first_transaction == txn);
   write_bytes(txn, key, "pending");

   return lfdbx::get(lfdb::raw, txn, key);
  };
  auto work = transfer ? lfdbx::transact(dbh, std::move(options), body)
                       : lfdbx::transact(dbh, options, body);
  options.clear();
  CHECK(0 == attempts);
  auto result = lfdbx::wait(std::move(work), null_yield);

  CHECK(2 == attempts);
  CHECK_FALSE(result.bytes());
  CHECK(lfdb::key_exists(dbh, key));
 }
}

TEST_CASE("Experimental invalid options fail after initiation with the FoundationDB error",
          "[fdb][concurrency][options]")
{
 janitor dbh;
 const lfdb::transaction_options invalid {{FDB_TR_OPTION_TIMEOUT, lfdb::option_flag}};
 bool invoked = false;
 auto work = lfdbx::transact(dbh, invalid, [&](lfdb::transaction_handle& txn) {
  invoked = true;

  return lfdbx::read_version(txn);
 });
 CHECK_FALSE(invoked);

 // FoundationDB may defer validation until the body's first request.
 try {
  lfdbx::wait(std::move(work), null_yield);
  FAIL("expected invalid option value to fail");
 } catch (const lfdb::libfdb_exception& error) {
  CHECK(error.code());
  CHECK(lfdb::fdb_error_category() == error.code().category());
  CHECK_FALSE(error.retryable());
 }

}


TEST_CASE("async transactions do not replay non-native error codes", "[fdb][errors]")
{
 janitor dbh;
 const auto value = GENERATE(1020, 1021);
 const auto category = GENERATE(0, 1, 2);
 const std::array categories {&std::generic_category(), &std::system_category(),
                              &lfdb::libfdb_error_category()};
 const std::error_code expected(value, *categories[category]);
 const lfdb::transaction_options limits {
  {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {2}},
  {FDB_TR_OPTION_MAX_RETRY_DELAY, std::int64_t {1}}
 };
 int calls = 0;

 auto work = lfdbx::transact(dbh, limits, [&](lfdb::transaction_handle& txn) {
  ++calls;
  throw lfdb::libfdb_exception(expected, "non-native body failure");

  return lfdbx::read_version(txn);
 });

 try {
  lfdbx::wait(std::move(work), null_yield);
  FAIL("expected non-native body failure");
 } catch (const lfdb::libfdb_exception& error) {
  CHECK(expected == error.code());
 }
 CHECK(1 == calls);
}

namespace concurrency_test {

// Emulate the documented throwing boundary without replacing global allocation.
struct failing_slot final
{
 bool is_connected() const noexcept { return true; }

 template <typename HandlerT, typename ...ArgTs>
 HandlerT& emplace(ArgTs&&...) const
 {
  throw std::bad_alloc {};
 }
};

// Request stop in the gap between two futures, when no FDB stop callback is
// installed. The next pending read must still observe the remembered request.
struct transition_stop_request final : lfdbx::detail::future_request
{
 using value_type = int;
 lfdb::transaction_handle txn;
 lfdb::transaction_handle delayed;
 boost::asio::cancellation_signal *stop_signal;
 bool advanced = false;

 void start(state_type& state)
 {
  state.emplace(fdb_transaction_get_read_version(txn->raw_handle()));
 }

 std::optional<value_type> advance(state_type& state)
 {
  if (const auto error = fdb_future_get_error(state->raw_ptr_or_throw()); 0 != error) {
   throw lfdb::libfdb_exception {std::error_code(error, lfdb::fdb_error_category())};
  }

  if (advanced) {
   return 1;
  }

  advanced = true;
  const std::uint8_t key = 'x';
  state.emplace(fdb_transaction_get(delayed->raw_handle(), &key, 1, false));
  stop_signal->emit(boost::asio::cancellation_type::partial);
  return std::nullopt;
 }
};

struct allocation_state final
{
 std::atomic_size_t calls = 0;
 std::atomic_size_t outstanding = 0;
 std::size_t fail_at;
};

template <typename T>
struct failing_allocator
{
 using value_type = T;
 allocation_state *state;

 explicit failing_allocator(allocation_state& state) : state(&state) {}

 template <typename U>
 failing_allocator(const failing_allocator<U>& other) : state(other.state) {}

 T *allocate(const std::size_t count)
 {
  if (++state->calls == state->fail_at) {
   throw std::bad_alloc {};
  }

  auto result = std::allocator<T> {}.allocate(count);
  ++state->outstanding;
  return result;
 }

 void deallocate(T *ptr, const std::size_t count)
 {
  --state->outstanding;
  std::allocator<T> {}.deallocate(ptr, count);
 }

 template <typename U>
 bool operator==(const failing_allocator<U>& other) const noexcept
 {
  return state == other.state;
 }
};

} // namespace concurrency_test

TEST_CASE("Completion releases associated storage before its callback", "[fdb][concurrency][allocation]")
{
 const auto fail_at = GENERATE(0u, 2u);
 const bool cancellable = GENERATE(false, true);
 boost::asio::io_context context;
 boost::asio::cancellation_signal cancel;
 concurrency_test::allocation_state state {.fail_at = fail_at};
 unsigned calls = 0;
 lfdbx::async_wait(lfdbx::get(lfdb::raw, {}, "invalid"),
   boost::asio::bind_allocator(concurrency_test::failing_allocator<std::byte> {state},
    boost::asio::bind_cancellation_slot(cancellable ? cancel.slot() : boost::asio::cancellation_slot {},
     boost::asio::bind_executor(context.get_executor(), [&](std::exception_ptr error, auto result) {
      ++calls;
      CHECK(error);
      CHECK_FALSE(result);
      CHECK(0 == state.outstanding);
     }))));
 context.run();
 cancel.emit(boost::asio::cancellation_type::partial);
 CHECK(1 == calls);
 CHECK(0 == state.outstanding);
}

TEST_CASE("Concurrent posting releases associated storage before delivery",
          "[fdb][concurrency][allocation]")
{
 constexpr std::size_t count = 128;
 std::array<concurrency_test::allocation_state, count> states {};
 std::array<std::size_t, count> observed {};
 std::atomic_size_t completed = 0;
 boost::asio::io_context context;
 auto work = boost::asio::make_work_guard(context);
 std::jthread runner {[&] { context.run(); }};
 for (std::size_t i = 0; i < count; ++i) {
  lfdbx::async_wait(lfdbx::get(lfdb::raw, {}, "invalid"),
    boost::asio::bind_allocator(concurrency_test::failing_allocator<std::byte> {states[i]},
     boost::asio::bind_executor(context.get_executor(), [&, i](std::exception_ptr, auto) {
      observed[i] = states[i].outstanding.load();
      ++completed;
     })));
 }
 work.reset();
 runner.join();
 CHECK(count == completed);
 for (const auto value : observed) {
  CHECK(0 == value);
 }
}

TEST_CASE("Experimental future transitions preserve cancellation between stages",
          "[fdb][concurrency][metadata]")
{
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 auto delayed = lfdb::make_transaction(dbh);
 delay_reads(dbh, delayed);
 boost::asio::io_context context;
 boost::asio::cancellation_signal cancel;
 bool completed = false;
 auto operation = lfdbx::detail::make_future_operation(concurrency_test::transition_stop_request {
  {}, txn, delayed, &cancel
 });
 lfdbx::async_wait(std::move(operation), boost::asio::bind_cancellation_slot(cancel.slot(),
   boost::asio::bind_executor(context.get_executor(),
     [&](std::exception_ptr error, auto result) {
      completed = true;
      CHECK_FALSE(result);
      check_operation_aborted(error);
     })));
 REQUIRE(context.run());
 CHECK(completed);
}

TEST_CASE("Experimental cancellation setup releases state when slot installation throws",
          "[fdb][concurrency][allocation]")
{
 boost::asio::io_context context;
 auto marker = std::make_shared<int>(42);
 const std::weak_ptr<int> weak = marker;
 auto token = boost::asio::bind_cancellation_slot(concurrency_test::failing_slot {},
   boost::asio::bind_executor(context.get_executor(),
     [marker](std::exception_ptr, auto) { FAIL("uninitiated operation completed"); }));
 REQUIRE_THROWS_AS(lfdbx::async_wait(lfdbx::get(lfdb::raw, {}, "invalid"), std::move(token)),
                   std::bad_alloc);
 marker.reset();
 CHECK(weak.expired());
 CHECK(0 == context.poll());
}

TEST_CASE("Experimental leaf delivery survives a failing associated allocator",
          "[fdb][concurrency][allocation]")
{
 boost::asio::io_context context;
 concurrency_test::allocation_state state {.fail_at = 2};
 int completions = 0;
 lfdbx::async_wait(lfdbx::get(lfdb::raw, {}, "invalid"), boost::asio::bind_allocator(
   concurrency_test::failing_allocator<std::byte> {state},
   boost::asio::bind_executor(context.get_executor(),
     [&](std::exception_ptr error, auto result) {
      ++completions;
      CHECK(0 == state.outstanding);
      CHECK_FALSE(result);
      REQUIRE(error);
      CHECK_THROWS_AS(std::rethrow_exception(error), std::bad_alloc);
     })));
 CHECK_NOTHROW(context.run());
 CHECK(1 == completions);
 CHECK(2 == state.calls);
 CHECK(0 == state.outstanding);
}

TEST_CASE("Experimental transaction delivery survives a failing associated allocator",
          "[fdb][concurrency][allocation]")
{
 boost::asio::io_context context;
#if defined(CEPH_LIBFDB_EXPERIMENTAL_ASIO)
 // The parent allocation, initial post, and terminal post all use the allocator.
 concurrency_test::allocation_state state {.fail_at = 3};
#else
 concurrency_test::allocation_state state {.fail_at = 2};
#endif
 int completions = 0;
 auto work = lfdbx::transact(lfdb::database_handle {}, [](lfdb::transaction_handle& txn) {
  return lfdbx::get(lfdb::raw, txn, "unused");
 });
 lfdbx::async_wait(std::move(work), boost::asio::bind_allocator(
   concurrency_test::failing_allocator<std::byte> {state},
   boost::asio::bind_executor(context.get_executor(),
     [&](std::exception_ptr error, auto result) {
      ++completions;
      CHECK(0 == state.outstanding);
      CHECK_FALSE(result);
      REQUIRE(error);
      CHECK_THROWS_AS(std::rethrow_exception(error), std::bad_alloc);
     })));
 CHECK_NOTHROW(context.run());
 CHECK(1 == completions);
 CHECK(state.fail_at == state.calls);
 CHECK(0 == state.outstanding);
}

TEST_CASE("Native point and window reads honor snapshot conflict semantics", "[fdb][concurrency][api]")
{
 janitor dbh;
 const auto mode = GENERATE(lfdb::read_mode::serializable, lfdb::read_mode::snapshot);
 const auto key = test_key("native/read-mode");
 lfdb::set(dbh, key, "original");
 const auto txn = lfdb::make_transaction(dbh);

 SECTION("point read") {
  const auto result = lfdbx::wait(lfdbx::get(lfdb::raw, txn, std::string_view(key), mode), null_yield);
  REQUIRE(result.bytes());
 }
 SECTION("window read") {
  const auto result = lfdbx::wait(lfdbx::read_window(
    lfdb::raw, txn, lfdbx::scan_cursor {lfdb::query::singleton(key)}, mode), null_yield);
  REQUIRE(result.rows().size() == 1);
 }

 lfdb::set(dbh, key, "changed");
 lfdb::set(txn, test_key("native/read-mode-effect"), "written");
 const auto committed = lfdb::commit(lfdb::with_result, txn);
 if (mode == lfdb::read_mode::snapshot) {
  CHECK(committed);
 } else {
  REQUIRE_FALSE(committed);
  CHECK(committed.error() == lfdb::fdb_error_code::not_committed);
 }
}

TEST_CASE("Native replay preparation is lazy and preserves typed errors", "[fdb][concurrency][api]")
{
 janitor dbh;
 const auto key = test_key("native/manual-replay");
 const auto txn = lfdb::make_transaction(dbh);
 lfdb::versionstamp stamp;
 lfdb::set(txn, key, "pending");
 lfdb::set(txn, test_key("native/manual-replay-stamp"), lfdb::versioned("", stamp));

 SECTION("preparation starts on initiation") {
  auto prepare = lfdbx::reset_for_replay(txn, lfdb::fdb_error_code::not_committed);
  CHECK_FALSE(stamp.is_ready());
  REQUIRE(lfdb::key_exists(txn, key));
  CHECK_NOTHROW(lfdbx::wait(std::move(prepare), null_yield));
  REQUIRE(stamp.is_ready());
  CHECK(stamp.resolved_bytes(lfdb::with_result).error() == lfdb::fdb_error_code::not_committed);
  CHECK_FALSE(lfdb::key_exists(txn, key));
  lfdb::set(txn, key, "replayed");
  CHECK_NOTHROW(lfdbx::wait(lfdbx::commit(txn), null_yield));
  CHECK(lfdb::key_exists(dbh, key));
 }
 SECTION("non-native categories cannot reset transaction state") {
  auto prepare = lfdbx::reset_for_replay(txn, std::error_code(1020, std::generic_category()));
  CHECK_THROWS_AS(lfdbx::wait(std::move(prepare), null_yield), lfdb::libfdb_exception);
  CHECK_FALSE(stamp.is_ready());
  CHECK(lfdb::key_exists(txn, key));
 }
 SECTION("terminal native errors preserve their category and value") {
  try {
   lfdbx::wait(lfdbx::reset_for_replay(txn, lfdb::fdb_error_code::operation_cancelled), null_yield);
   FAIL("terminal native error was unexpectedly prepared for replay");
  } catch (const lfdb::libfdb_exception& failure) {
   CHECK(failure.code() == lfdb::fdb_error_code::operation_cancelled);
  }
 }
}

TEST_CASE("Experimental point operations own borrowed and transferred keys",
          "[fdb][concurrency]")
{
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 const auto original = test_key("concurrency/owned-key/" + std::string(128, 'x'));
 write_bytes(txn, original, "owned");
 auto borrowed_key = original;
 auto borrowed = lfdbx::get(lfdb::raw, txn, std::string_view {borrowed_key});
 borrowed_key.assign(borrowed_key.size(), 'z');
 auto owned_key = original;
 auto owned = lfdbx::get(lfdb::raw, txn, std::move(owned_key));
 owned_key.assign(original.size(), 'z');
 auto lvalue_key = original;
 auto lvalue = lfdbx::get(lfdb::raw, txn, lvalue_key);
 CHECK(original == lvalue_key);
 lvalue_key.assign(lvalue_key.size(), 'z');

 for (auto *operation : {&borrowed, &owned, &lvalue}) {
  auto result = lfdbx::wait(std::move(*operation), null_yield);
  REQUIRE(result.bytes());
  CHECK("owned" == as_string_view(*result.bytes()));
 }
}

TEST_CASE("Experimental metadata operations start lazily and return scalar values",
          "[fdb][concurrency][metadata]")
{
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 const auto version = lfdb::read_version(lfdb::make_transaction(dbh));
 auto version_request = lfdbx::read_version(txn);
 lfdb::set_read_version(txn, version - 1);
 CHECK(version - 1 == lfdbx::wait(std::move(version_request), null_yield));

 const auto empty_size = lfdb::approximate_commit_bytes(txn);
 auto commit_size_request = lfdbx::approximate_commit_bytes(txn);
 write_bytes(txn, test_key("concurrency/metadata/bytes"), "pending mutation");
 const auto written_size = lfdbx::wait(std::move(commit_size_request), null_yield);
 CHECK(empty_size < written_size);
 CHECK(lfdb::approximate_commit_bytes(txn) == written_size);

 auto managed = lfdbx::transact(dbh, [](lfdb::transaction_handle& current) {
  return lfdbx::read_version(current);
 });
 CHECK(0 < lfdbx::wait(std::move(managed), null_yield));
}

TEST_CASE("Experimental range estimates traverse empty and batched selections",
          "[fdb][concurrency][metadata]")
{
 namespace lq = lfdb::query;
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 const auto prefix = "concurrency-estimates";

 for (int i = 0; i < 10; ++i) {
  write_bytes(txn, make_key(i, prefix), std::string(1024, 'x'));
 }

 REQUIRE_NOTHROW(lfdb::commit(txn));
 auto read_txn = lfdb::make_transaction(dbh);
 const auto selection = lq::difference(
   lq::between(make_key(0, prefix), make_key(10, prefix)),
   lq::set_union(lq::singleton(make_key(1, prefix)),
    lq::set_union(lq::singleton(make_key(3, prefix)),
     lq::set_union(lq::singleton(make_key(5, prefix)), lq::singleton(make_key(7, prefix))))));
 std::size_t intervals = 0;
 lq::for_each_interval(selection, [&](const auto&) { ++intervals; });
 REQUIRE(5 == intervals);

 for (const bool reverse : {false, true}) {
  auto ordered = lq::with_options(selection, lq::query_options {.reverse_order = reverse});
  auto estimate = lfdbx::approximate_range_size(read_txn, ordered);
  CHECK(0 <= lfdbx::wait(std::move(estimate), null_yield));
 }

 CHECK(0 == lfdbx::wait(lfdbx::approximate_range_size(read_txn,
   lq::difference(selection, selection)), null_yield));
 CHECK(0 == lfdbx::wait(lfdbx::approximate_range_size(read_txn,
   lfdb::select {make_key(0, prefix), make_key(0, prefix)}), null_yield));
}

TEST_CASE("Experimental partitions own algebra-compatible ranges in either order",
          "[fdb][concurrency][metadata]")
{
 namespace lq = lfdb::query;
 janitor dbh;
 const auto prefix = "concurrency-partitions";
 auto txn = lfdb::make_transaction(dbh);

 for (int i = 0; i < 32; ++i) {
  lfdb::set(txn, make_key(i, prefix), std::string(512, 'x'));
 }

 REQUIRE_NOTHROW(lfdb::commit(txn));
 auto bounds = lfdb::select {make_key(0, prefix), make_key(31, prefix)};
 bounds.begin_inclusive = false;
 bounds.end_inclusive = true;
 const auto selection = lq::difference(bounds,
   lq::set_union(lq::between(make_key(9, prefix), make_key(19, prefix)),
    lq::set_union(lq::singleton(make_key(3, prefix)),
     lq::set_union(lq::singleton(make_key(5, prefix)), lq::singleton(make_key(23, prefix))))));

 for (const bool reverse : {false, true}) {
  const auto ordered = lq::with_options(selection,
    lq::query_options {.result_limit = 3, .reverse_order = reverse});
  auto read_txn = lfdb::make_transaction(dbh);
  const auto expected = lfdb::scan(read_txn, ordered) | std::views::keys
                      | std::ranges::to<std::vector<std::string>>();
  auto work = lfdbx::partitions(read_txn, ordered, 256);
  auto ranges = lfdbx::wait(std::move(work), null_yield);
  read_txn.reset();
  REQUIRE(5 <= ranges.size());
  std::vector<std::string> actual;

  for (const auto& range : ranges) {
   CHECK(ordered.options == range.options);
   auto independent = lfdb::make_transaction(dbh);

   for (const auto& [key, value] : lfdb::scan(independent, range)) {
    actual.push_back(key);
   }
  }

  CHECK(expected == actual);
 }

 CHECK(lfdbx::wait(lfdbx::partitions(txn, lq::empty(), 256), null_yield).empty());
}

TEST_CASE("Experimental metadata refills each consumed slot within its bound",
          "[fdb][concurrency][metadata]")
{
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 lfdbx::detail::selection_intervals selections;
 for (int i = 0; i < 10; ++i) {
  selections.push_back(lfdb::select {make_key(2 * i, "metadata-pipeline"),
                                    make_key(1 + 2 * i, "metadata-pipeline")});
  write_bytes(txn, selections.back().begin_key, "value");
 }
 lfdb::commit(txn);
 auto read_txn = lfdb::make_transaction(dbh);
 auto check = [](auto request) {
  using request_t = decltype(request);
  typename request_t::state_type state;
  request.start(state);
  CHECK(request_t::pipeline_width == state.count);
  std::size_t completed = 0;
  while (auto future = request_t::pending_future(state)) {
   REQUIRE(0 == fdb_future_block_until_ready(future));
   REQUIRE(0 == fdb_future_get_error(future));
   auto result = request.advance(state);
   ++completed;
   CHECK(std::min(request_t::pipeline_width, 10 - completed) == state.count);
   CHECK(std::min(std::size_t {10}, request_t::pipeline_width + completed) == request.next);
   CHECK((10 == completed) == result.has_value());
  }
  CHECK(10 == completed);
 };
 check(lfdbx::detail::range_size_request {{}, read_txn, selections});
 check(lfdbx::detail::partition_request {{}, read_txn, selections, 1024});
}

TEST_CASE("Experimental metadata propagates cancellation with several futures pending",
          "[fdb][concurrency][metadata]")
{
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 delay_reads(dbh, txn);
 const auto selection = lfdb::query::set_union(lfdb::query::prefix("pipeline-a"),
   lfdb::query::set_union(lfdb::query::prefix("pipeline-b"),
    lfdb::query::set_union(lfdb::query::prefix("pipeline-c"),
     lfdb::query::set_union(lfdb::query::prefix("pipeline-d"),
                             lfdb::query::prefix("pipeline-e")))));
 SECTION("estimate") {
  check_active_cancellation(lfdbx::approximate_range_size(txn, selection));
 }
 SECTION("split points") {
  check_active_cancellation(lfdbx::partitions(txn, selection, 1024));
 }
}

TEST_CASE("Experimental metadata validates on initiation and preserves errors",
          "[fdb][concurrency][metadata]")
{
 namespace lq = lfdb::query;
 const auto invalid = [](auto operation) {
  CHECK_THROWS_AS(lfdbx::wait(std::move(operation), null_yield), lfdb::libfdb_exception);
 };
 invalid(lfdbx::read_version({}));
 invalid(lfdbx::approximate_commit_bytes({}));
 invalid(lfdbx::approximate_range_size({}, lq::prefix("invalid")));
 invalid(lfdbx::partitions({}, lq::prefix("invalid"), 256));

 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 invalid(lfdbx::partitions(txn, lq::empty(), 0));
 invalid(lfdbx::partitions(txn, lq::prefix("invalid"), -1));
 auto failed = lfdbx::read_version(txn);
 fdb_transaction_cancel(txn->raw_handle());

 try {
  lfdbx::wait(std::move(failed), null_yield);
  FAIL("cancelled transaction produced a read version");
 } catch (const lfdb::libfdb_exception& error) {
  CHECK(std::error_code(1025, lfdb::fdb_error_category()) == error.code());
 }
}

TEST_CASE("Experimental scans traverse bounded pages in both interval orders",
          "[fdb][concurrency]")
{
 janitor dbh;
 const auto prefix = "concurrency-scan";
 auto txn = lfdb::make_transaction(dbh);

 for (int i = 0; i < 10; ++i) {
  write_bytes(txn, make_key(i, prefix), std::to_string(i));
 }

 REQUIRE_NOTHROW(lfdb::commit(txn));
 const auto selection = lfdb::query::difference(
   lfdb::query::between(make_key(0, prefix), make_key(10, prefix)),
   lfdb::query::between(make_key(3, prefix), make_key(7, prefix)));
 const std::vector expected {make_key(0, prefix), make_key(1, prefix), make_key(2, prefix),
                             make_key(7, prefix), make_key(8, prefix), make_key(9, prefix)};

 for (const bool reverse : {false, true}) {
  auto cursor = lfdbx::scan_cursor {lfdb::query::with_options(selection,
    lfdb::query::query_options {.result_limit = 2, .reverse_order = reverse})};
  auto read_txn = lfdb::make_transaction(dbh);
  std::vector<std::string> keys;
  int windows = 0;

  while (cursor) {
   auto window = lfdbx::wait(lfdbx::read_window(lfdb::raw, read_txn, std::move(cursor)), null_yield);
   ++windows;
   CHECK(std::ranges::distance(window.rows()) <= 2);

   for (const auto row : window.rows()) {
    keys.emplace_back(as_string_view(row.key));
   }

   cursor = std::move(window).next();
  }

  CHECK(windows >= 4);

  if (reverse) {
   std::ranges::reverse(keys);
  }

  CHECK(expected == keys);
 }
}

namespace concurrency_test {

struct traversal_rows final
{
 int active = 0;
 int peak = 0;
 std::vector<std::string> keys;
 std::thread::id caller = std::this_thread::get_id();
 boost::asio::io_context *context = nullptr;
 boost::asio::cancellation_signal *cancel = nullptr;
};

struct traversal_row_lifetime final
{
 traversal_rows& rows;

 explicit traversal_row_lifetime(traversal_rows& rows) : rows(rows)
 {
  rows.peak = std::max(rows.peak, ++rows.active);
 }

 ~traversal_row_lifetime() { --rows.active; }
};

struct traversal_row_request final : lfdbx::detail::future_request
{
 using value_type = std::tuple<>;
 lfdb::transaction_handle txn;
 std::span<const std::uint8_t> key;
 std::span<const std::uint8_t> value;
 std::string expected_key;
 std::string expected_value;
 std::shared_ptr<traversal_row_lifetime> lifetime;

 void start(state_type& state)
 {
  state.emplace(fdb_transaction_get(txn->raw_handle(), key.data(), static_cast<int>(key.size()), true));
  auto& rows = lifetime->rows;
  if (rows.cancel) {
   boost::asio::post(*rows.context, [cancel = rows.cancel] {
    cancel->emit(boost::asio::cancellation_type::partial);
   });
  }
 }

 std::optional<value_type> advance(state_type& state)
 {
  if (const auto error = fdb_future_get_error(state->raw_handle()); 0 != error) {
   throw lfdb::libfdb_exception {std::error_code(error, lfdb::fdb_error_category())};
  }
  CHECK(expected_key == as_string_view(key));
  CHECK(expected_value == as_string_view(value));
  lifetime->rows.keys.emplace_back(as_string_view(key));
  return value_type {};
 }
};

} // namespace concurrency_test

TEST_CASE("Async operations accept values that are only move constructible",
          "[fdb][concurrency][traversal][api]")
{
 struct result_value final
 {
  std::unique_ptr<int> payload;

  explicit result_value(const int value)
   : payload(std::make_unique<int>(value))
  {}

  result_value(result_value&&) noexcept = default;
  result_value(const result_value&) = delete;

  result_value& operator=(result_value&&) = delete;
  result_value& operator=(const result_value&) = delete;

 };
 static_assert(std::is_nothrow_move_constructible_v<result_value>);
 static_assert(not std::default_initializable<result_value>);
 static_assert(not std::copy_constructible<result_value>);
 static_assert(not std::assignable_from<result_value&, result_value>);

 struct ready_request final : lfdbx::detail::future_request
 {
  using value_type = result_value;

  void start(state_type&) {}
  std::optional<value_type> advance(state_type&) { return value_type {42}; }

 };
 auto ready = lfdbx::wait(lfdbx::detail::make_future_operation(ready_request {}), null_yield);
 CHECK(42 == *ready.payload);

 const bool prefetch = GENERATE(false, true);
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 const auto prefix = test_key("construction-only/");
 for (const auto i : std::views::iota(0, 5)) {
  write_bytes(txn, prefix + std::to_string(i), "value");
 }
 unsigned calls = 0;
 auto work = lfdbx::for_each(lfdb::raw, txn, lfdb::query::with_options(
   lfdb::query::prefix(prefix), lfdb::query::query_options {.result_limit = 2}),
   lfdbx::scan_options {.prefetch = prefetch}, [&calls](auto key, auto value) {
    CHECK_FALSE(key.empty());
    CHECK_FALSE(value.empty());
    ++calls;
    return native_value(result_value {42});
   });
 CHECK(5 == lfdbx::wait(std::move(work), null_yield));
 CHECK(5 == calls);
}

TEST_CASE("Async traversal preserves bounded interval order with optional prefetch",
          "[fdb][concurrency][traversal]")
{
 janitor dbh;
 const bool reverse = GENERATE(false, true);
 const bool prefetch = GENERATE(false, true);
 const bool managed = GENERATE(false, true);
 auto txn = lfdb::make_transaction(dbh);
 const auto prefix = "async-traversal";
 for (int i = 0; i < 10; ++i) {
  write_bytes(txn, make_key(i, prefix), std::to_string(i));
 }
 lfdb::commit(txn);
 auto selection = lfdb::query::with_options(lfdb::query::difference(
   lfdb::query::between(make_key(0, prefix), make_key(10, prefix)),
   lfdb::query::between(make_key(3, prefix), make_key(7, prefix))),
   lfdb::query::query_options {.result_limit = 2, .reverse_order = reverse});
 std::vector<std::string> keys;
 auto consume = [&keys, ownership = std::make_unique<int>(42)](auto key, auto value) {
  CHECK(42 == *ownership);
  CHECK(1 == std::size(value));
  keys.emplace_back(as_string_view(key));
 };
 const lfdbx::scan_options options {.prefetch = prefetch};
 const auto count = managed
  ? lfdbx::wait(lfdbx::for_each(lfdb::raw, dbh.dbh(), selection, options, std::move(consume)),
                null_yield)
  : lfdbx::wait(lfdbx::for_each(lfdb::raw, lfdb::make_transaction(dbh), selection, options,
                                std::move(consume)), null_yield);
 CHECK(6 == count);
 if (reverse) {
  std::ranges::reverse(keys);
 }
 const std::vector expected {make_key(0, prefix), make_key(1, prefix), make_key(2, prefix),
                             make_key(7, prefix), make_key(8, prefix), make_key(9, prefix)};
 CHECK(expected == keys);
}

TEST_CASE("Async traversal accepts native row operations and empty selections",
          "[fdb][concurrency][traversal]")
{
 janitor dbh;
 const bool prefetch = GENERATE(false, true);
 auto txn = lfdb::make_transaction(dbh);
 const auto prefix = "async-native-traversal";
 for (int i = 0; i < 7; ++i) {
  write_bytes(txn, make_key(i, prefix), std::to_string(i));
 }
 const auto selection = lfdb::query::with_options(lfdb::query::prefix(make_key_prefix(prefix)),
   lfdb::query::query_options {.result_limit = 2});
 int called = 0;
 auto work = lfdbx::for_each(lfdb::raw, txn, selection, lfdbx::scan_options {.prefetch = prefetch},
   [&called, txn](auto key, auto value) {
    ++called;
    CHECK(1 == std::size(value));
    return lfdbx::get(lfdb::raw, txn, as_string_view(key));
   });
 CHECK(0 == called);
 CHECK(7 == lfdbx::wait(std::move(work), null_yield));
 CHECK(7 == called);
 CHECK(0 == lfdbx::wait(lfdbx::for_each(lfdb::raw, txn, lfdb::query::empty(),
   [&called](auto, auto) { ++called; }), null_yield));
 CHECK(7 == called);
}

TEST_CASE("Async managed traversal never retries a delivered callback",
          "[fdb][concurrency][traversal]")
{
 janitor dbh;
 const bool prefetch = GENERATE(false, true);
 const auto key = test_key("async-traversal/error");
 auto txn = lfdb::make_transaction(dbh);
 write_bytes(txn, key, "bytes");
 lfdb::commit(txn);
 int called = 0;
 auto work = lfdbx::for_each(lfdb::raw, dbh.dbh(), lfdb::query::singleton(key),
   lfdbx::scan_options {.prefetch = prefetch}, [&called](auto, auto) {
    ++called;
    throw lfdb::libfdb_exception {lfdb::fdb_error_code::not_committed};
   });
 try {
  std::ignore = lfdbx::wait(std::move(work), null_yield);
  FAIL("callback failure was discarded");
 } catch (const lfdb::libfdb_exception& error) {
  CHECK(lfdb::fdb_error_code::not_committed == error.code());
 }
 CHECK(1 == called);
}

TEST_CASE("Async traversal cancels an outstanding range read", "[fdb][concurrency][traversal]")
{
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 delay_reads(dbh, txn);
 check_active_cancellation(lfdbx::for_each(lfdb::raw, txn, lfdb::query::prefix("delayed-traversal"),
   [](auto, auto) { FAIL("cancelled traversal delivered a row"); }));
}

TEST_CASE("Async traversal retains row storage until native consumers complete",
          "[fdb][concurrency][traversal]")
{
 janitor dbh;
 const bool prefetch = GENERATE(false, true);
 const bool cancel_consumer = GENERATE(false, true);
 auto txn = lfdb::make_transaction(dbh);
 const auto prefix = "async-row-lifetime";
 for (int i = 0; i < 7; ++i) {
  write_bytes(txn, make_key(i, prefix), std::to_string(i));
 }
 lfdb::commit(txn);
 auto reader = lfdb::make_transaction(dbh);
 if (cancel_consumer) {
  delay_reads(dbh, reader);
 }
 boost::asio::io_context context;
 boost::asio::cancellation_signal cancel;
 concurrency_test::traversal_rows rows;
 if (cancel_consumer) {
  rows.context = &context;
  rows.cancel = &cancel;
 }
 auto consume = [&rows, reader](auto key, auto value) {
#if defined(CEPH_LIBFDB_EXPERIMENTAL_ASIO)
  CHECK(rows.caller == std::this_thread::get_id());
#endif
  return lfdbx::detail::make_future_operation(concurrency_test::traversal_row_request {
   {}, reader, key, value, std::string {as_string_view(key)}, std::string {as_string_view(value)},
   std::make_shared<concurrency_test::traversal_row_lifetime>(rows)
  });
 };
 auto work = lfdbx::for_each(lfdb::raw, dbh.dbh(), lfdb::query::with_options(
   lfdb::query::prefix(make_key_prefix(prefix)), lfdb::query::query_options {.result_limit = 2}),
   lfdbx::scan_options {.prefetch = prefetch}, std::move(consume));
 bool completed = false;
 lfdbx::async_wait(std::move(work), boost::asio::bind_executor(context.get_executor(),
   boost::asio::bind_cancellation_slot(cancel.slot(),
    [&](std::exception_ptr error, auto result) {
     completed = true;
     if (cancel_consumer) {
      CHECK_FALSE(result);
      check_operation_aborted(error);
      return;
     }
     if (error) {
      try {
       std::rethrow_exception(error);
      } catch (const std::exception& failure) {
       FAIL_CHECK(failure.what());
      }
      return;
     }
     REQUIRE(result.has_value());
     CHECK(7 == *result);
    })));
 REQUIRE(context.run());
 REQUIRE(completed);
 CHECK(0 == rows.active);
 CHECK(1 == rows.peak);
 CHECK((cancel_consumer ? 0 : 7) == std::size(rows.keys));
}

TEST_CASE("Async traversal retries a failed window without losing its cursor",
          "[fdb][concurrency][traversal]")
{
 struct source final
 {
  lfdb::transaction_handle txn;
  lfdb::transaction_handle make() const { return txn; }
 };
 janitor dbh;
 auto writer = lfdb::make_transaction(dbh);
 const auto prefix = "async-window-replay";
 for (int i = 0; i < 5; ++i) {
  write_bytes(writer, make_key(i, prefix), std::to_string(i));
 }
 lfdb::commit(writer);
 auto reader = lfdb::make_transaction(dbh);
 lfdb::set_read_version(reader, 1);
 std::vector<std::string> keys;
 auto work = lfdbx::detail::make_traversal_operation(source {reader}, lfdb::query::with_options(
   lfdb::query::prefix(make_key_prefix(prefix)), lfdb::query::query_options {.result_limit = 2}),
   lfdbx::scan_options {.prefetch = true}, [&keys](auto key, auto) {
    keys.emplace_back(as_string_view(key));
   });
 CHECK(5 == lfdbx::wait(std::move(work), null_yield));
 CHECK(1 < lfdb::read_version(reader));
 REQUIRE(5 == keys.size());
 for (int i = 0; i < 5; ++i) {
  CHECK(make_key(i, prefix) == keys[i]);
 }
}

TEST_CASE("Async traversal owns transaction options until lazy initiation",
          "[fdb][concurrency][traversal]")
{
 janitor dbh;
 lfdb::transaction_options options {{FDB_TR_OPTION_TIMEOUT, lfdb::option_flag}};
 int called = 0;
 auto work = lfdbx::for_each(lfdb::raw, dbh.dbh(), options, lfdb::query::prefix("invalid-options"),
   [&called](auto, auto) { ++called; });
 options.clear();
 CHECK(0 == called);
 try {
  std::ignore = lfdbx::wait(std::move(work), null_yield);
  FAIL("invalid captured options were discarded");
 } catch (const lfdb::libfdb_exception& error) {
  CHECK(lfdb::fdb_error_category() == error.code().category());
  CHECK_FALSE(error.retryable());
 }
 CHECK(0 == called);
}

TEST_CASE("Async traversal completes when associated allocation fails",
          "[fdb][concurrency][traversal]")
{
 janitor dbh;
 const auto fail_at = GENERATE(Catch::Generators::range(1u, 81u));
 CAPTURE(fail_at);
 auto txn = lfdb::make_transaction(dbh);
 const auto prefix = "async-traversal-allocation";
 for (int i = 0; i < 5; ++i) {
  write_bytes(txn, make_key(i, prefix), std::to_string(i));
 }
 auto marker = std::make_shared<int>(42);
 std::weak_ptr<int> released = marker;
 boost::asio::io_context context;
 concurrency_test::allocation_state allocation {0, 0, fail_at};
 bool completed = false;
 bool threw = false;
 try {
  auto work = lfdbx::for_each(lfdb::raw, txn, lfdb::query::with_options(
    lfdb::query::prefix(make_key_prefix(prefix)), lfdb::query::query_options {.result_limit = 2}),
    lfdbx::scan_options {.prefetch = true}, [marker = std::move(marker)](auto, auto) {
     CHECK(42 == *marker);
     return native_value(std::tuple<> {});
    });
  lfdbx::async_wait(std::move(work), boost::asio::bind_allocator(
    concurrency_test::failing_allocator<std::byte> {allocation},
    boost::asio::bind_executor(context.get_executor(), [&](std::exception_ptr error, auto result) {
     completed = true;
     if (error) {
      CHECK_THROWS_AS(std::rethrow_exception(error), std::bad_alloc);
      CHECK_FALSE(result);
      return;
     }
     REQUIRE(result.has_value());
     CHECK(5 == *result);
    })));
 } catch (const std::bad_alloc&) {
  threw = true;
 }
 context.run();
 CHECK((completed or threw));
 CHECK(released.expired());
 CHECK(0 == allocation.outstanding);
}

#if defined(CEPH_LIBFDB_EXPERIMENTAL_ASIO)
TEST_CASE("Managed completion waits for a reentrant successful initiation",
          "[fdb][concurrency][allocation]")
{
 janitor dbh;
 boost::asio::io_context context;
 int published = 0;
 unsigned calls = 0;
 concurrency_test::allocation_state allocation {.fail_at = 0};
 auto work = lfdbx::transact(dbh, [&context, &published, &calls](auto&, auto& staged) {
  staged = 42;
  auto token = boost::asio::deferred;
  return lfdbx::operation {boost::asio::async_initiate<decltype(token),
    void(std::exception_ptr, std::optional<std::tuple<>>)>(
    [&context, &published, &calls](auto handler) {
     std::move(handler)(std::exception_ptr {}, std::optional {std::tuple<> {}});
     const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds {5};
     while (42 != published and std::chrono::steady_clock::now() < deadline) {
      context.run_one_until(deadline);
     }
     REQUIRE(42 == published);
     context.poll();
     CHECK(0 == calls);
    }, token)};
 }, lfdb::staged(published));
 lfdbx::async_wait(std::move(work), boost::asio::bind_allocator(
   concurrency_test::failing_allocator<std::byte> {allocation},
   boost::asio::bind_executor(context.get_executor(), [&](std::exception_ptr error, auto result) {
    ++calls;
    CHECK_FALSE(error);
    CHECK(result);
    CHECK(0 == allocation.outstanding);
   })));
 context.run();
 CHECK(1 == calls);
 CHECK(42 == published);
}

TEST_CASE("Asio traversal joins children after late group setup failure",
          "[fdb][concurrency][traversal]")
{
 const bool queued = GENERATE(false, true);
 boost::asio::io_context context;
 auto marker = std::make_shared<int>(42);
 std::weak_ptr<int> released = marker;
 int children = 0;
 int completions = 0;
 auto child = [&] {
  auto token = boost::asio::deferred;
  return lfdbx::operation {boost::asio::async_initiate<decltype(token),
    lfdbx::detail::traversal_step_signature>(
     [queued, marker, &context, &children](auto handler) {
      auto finish = [marker, &children, handler = std::move(handler)]() mutable {
       CHECK(42 == *marker);
       ++children;
       std::move(handler)(std::exception_ptr {}, std::optional {std::tuple<> {}});
      };
      if (queued) {
       boost::asio::post(context, std::move(finish));
       return;
      }
      finish();
     }, token)};
 };
 auto work = lfdbx::detail::join_traversal_steps(child(), child());
 marker.reset();
 CHECK_NOTHROW(lfdbx::async_wait(std::move(work), boost::asio::bind_cancellation_slot(
   concurrency_test::failing_slot {}, boost::asio::bind_executor(context.get_executor(),
    [&](std::exception_ptr error, auto result) {
     ++completions;
     CHECK(2 == children);
     CHECK_FALSE(result);
     REQUIRE(error);
     CHECK_THROWS_AS(std::rethrow_exception(error), std::bad_alloc);
    }))));
 context.run();
 CHECK(1 == completions);
 CHECK(2 == children);
 CHECK(released.expired());
}
#endif

TEST_CASE("Managed async cancellation abandons registered stamps immediately", "[fdb][stamps]")
{
 janitor dbh;
 lfdb::transaction_handle retained;
 lfdb::versionstamp stamp;
 boost::asio::io_context context;
 boost::asio::cancellation_signal cancel;
 std::exception_ptr error;
 auto work = lfdbx::transact(dbh, [&](auto& txn) {
  retained = txn;
  delay_reads(dbh.dbh(), txn);
  lfdb::set(txn, test_key("async-cancelled-stamp"), lfdb::versioned("", stamp));
  return lfdbx::get(lfdb::raw, txn, test_key("async-delayed-stamp"));
 });
 lfdbx::async_wait(std::move(work), boost::asio::bind_cancellation_slot(cancel.slot(),
  boost::asio::bind_executor(context.get_executor(), [&](std::exception_ptr failure, auto value) {
   error = std::move(failure);
   CHECK_FALSE(value);
  })));

 while (not retained) {
  REQUIRE(context.run_one());
 }
 cancel.emit(boost::asio::cancellation_type::partial);
 REQUIRE(context.run());
 check_operation_aborted(error);
 REQUIRE(stamp.is_ready());
 CHECK(std::make_error_code(std::errc::operation_canceled) ==
       stamp.resolved_bytes(lfdb::with_result).error());
}

TEST_CASE("Managed async application errors abandon registered stamps immediately", "[fdb][stamps]")
{
 janitor dbh;
 lfdb::transaction_handle retained;
 lfdb::versionstamp stamp;
 auto work = lfdbx::transact(dbh, [&](auto& txn) -> decltype(native_value(42)) {
  retained = txn;
  lfdb::set(txn, test_key("async-abandoned-stamp"), lfdb::versioned("", stamp));
  throw std::runtime_error("application rejected update");
 });

 CHECK_THROWS_AS(lfdbx::wait(std::move(work), null_yield), std::runtime_error);
 REQUIRE(retained);
 REQUIRE(stamp.is_ready());
 CHECK(std::make_error_code(std::errc::operation_canceled) ==
       stamp.resolved_bytes(lfdb::with_result).error());
}

TEST_CASE("Native async retry policy preserves decisions and original failures", "[fdb][policy]")
{
 janitor dbh;
 const auto key = test_key("async-retry-policy");
 int attempts = 0;
 int decisions = 0;

 SECTION("false preserves unknown body error without preparation") {
  lfdb::transaction_handle retained;
  auto policy = [&](const lfdb::transaction_failure& failure) {
   ++decisions;
   CHECK(lfdb::fdb_error_code::commit_unknown_result == failure.code);
   CHECK(lfdb::transaction_phase::body == failure.phase);
   return false;
  };
  auto work = lfdbx::transact(lfdb::retry_if(std::ref(policy)), dbh,
   [&](auto& txn) -> decltype(native_value(42)) {
    ++attempts;
    retained = txn;
    lfdb::set(txn, key, "uncommitted");
    throw lfdb::libfdb_exception(lfdb::fdb_error_code::commit_unknown_result);
   });
  CHECK_THROWS_AS(lfdbx::wait(std::move(work), null_yield), lfdb::libfdb_exception);
  CHECK(1 == attempts);
  CHECK(1 == decisions);
  std::string value;
  REQUIRE(lfdb::get(retained, key, value));
  CHECK("uncommitted" == value);
  CHECK_FALSE(lfdb::key_exists(dbh, key));
 }

 SECTION("one mutable move-only policy survives replay and staged output") {
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {3}},
   {FDB_TR_OPTION_MAX_RETRY_DELAY, std::int64_t {1}}
  };
  int published = 0;
  lfdb::versionstamp stamp;
  auto work = lfdbx::transact(lfdb::retry_if(
   [state = std::make_unique<int>(0), &decisions](const lfdb::transaction_failure& failure) mutable {
    CHECK(lfdb::transaction_phase::body == failure.phase);
    decisions = ++*state;
    return true;
   }), dbh, options, [&](auto& txn, auto& output) {
    output += 1;
    lfdb::set(txn, key, lfdb::versioned("", stamp));
    if (++attempts < 3) {
     throw lfdb::libfdb_exception(lfdb::fdb_error_code::not_committed);
    }
    return native_value(42);
   }, lfdb::staged(published));
  CHECK(42 == lfdbx::wait(std::move(work), null_yield));
  CHECK(3 == attempts);
  CHECK(2 == decisions);
  CHECK(1 == published);
  CHECK(stamp.has_value());
  CHECK(lfdb::key_exists(dbh, key));
 }

 SECTION("terminal commit failure is observed and cannot be forced to replay") {
  auto work = lfdbx::transact(lfdb::retry_if([&](const lfdb::transaction_failure& failure) {
   ++decisions;
   CHECK(lfdb::transaction_phase::commit == failure.phase);
   CHECK(lfdb::fdb_error_code::transaction_cancelled == failure.code);
   return true;
  }), dbh, [&](auto& txn) {
   ++attempts;
   fdb_transaction_cancel(txn->raw_handle());
   return native_value(42);
  });
  CHECK_THROWS_AS(lfdbx::wait(std::move(work), null_yield), lfdb::libfdb_exception);
  CHECK(1 == attempts);
  CHECK(1 == decisions);
 }

 SECTION("native retry exhaustion is terminal without another policy call") {
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {0}}
  };
  auto work = lfdbx::transact(lfdb::retry_if([&](const lfdb::transaction_failure& failure) {
   ++decisions;
   CHECK(lfdb::fdb_error_code::commit_unknown_result == failure.code);
   return true;
  }), dbh, options, [&](auto&) -> decltype(native_value(42)) {
   ++attempts;
   throw lfdb::libfdb_exception(lfdb::fdb_error_code::commit_unknown_result);
  });
  CHECK_THROWS_AS(lfdbx::wait(std::move(work), null_yield), lfdb::libfdb_exception);
  CHECK(1 == attempts);
  CHECK(1 == decisions);
 }

 SECTION("policy exceptions retain identity and never re-enter the policy") {
  const auto phase = GENERATE(lfdb::transaction_phase::body, lfdb::transaction_phase::commit);
  const auto error = std::make_exception_ptr(
   lfdb::libfdb_exception(lfdb::fdb_error_code::not_committed, "policy failure"));
  auto work = lfdbx::transact(lfdb::retry_if([&](const lfdb::transaction_failure& failure) -> bool {
   ++decisions;
   CHECK(phase == failure.phase);
   std::rethrow_exception(error);
  }), dbh, [&](auto& txn) {
   ++attempts;
   if (lfdb::transaction_phase::body == phase) {
    throw lfdb::libfdb_exception(lfdb::fdb_error_code::not_committed);
   }
   fdb_transaction_cancel(txn->raw_handle());
   return native_value(42);
  });
  try {
   std::ignore = lfdbx::wait(std::move(work), null_yield);
   FAIL("policy exception was suppressed");
  } catch (...) {
   CHECK(error == std::current_exception());
  }
  CHECK(1 == attempts);
  CHECK(1 == decisions);
 }

 SECTION("local errors bypass the policy") {
  const auto error = std::make_exception_ptr(
   lfdb::libfdb_exception(std::error_code(1021, std::generic_category())));
  auto work = lfdbx::transact(lfdb::retry_if([&](const lfdb::transaction_failure&) {
   ++decisions;
   return true;
  }), dbh, [&](auto&) -> decltype(native_value(42)) {
   ++attempts;
   std::rethrow_exception(error);
  });
  try {
   std::ignore = lfdbx::wait(std::move(work), null_yield);
   FAIL("local exception was suppressed");
  } catch (...) {
   CHECK(error == std::current_exception());
  }
  CHECK(1 == attempts);
  CHECK(0 == decisions);
 }
}

#endif
