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

#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <exec/single_thread_context.hpp>
#include <exec/variant_sender.hpp>

#include <stdexec/execution.hpp>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/system/system_error.hpp>

#include "test/rgw/test_fdb_concurrency.h"
#include "rgw/ceph_fdb_execution.h"
#include "rgw/fdb/execution.h"
#include "rgw/fdb/experimental/concurrency.h"

#include <array>
#include <concepts>
#include <cstdint>
#include <exception>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

namespace lfdb = ceph::libfdb;

using namespace std::literals;

namespace concurrency_test {

template <typename ErrorT>
struct reference_error_sender final
{
 using sender_concept = stdexec::sender_tag;
 using completion_signatures =
  stdexec::completion_signatures<stdexec::set_error_t(const ErrorT&)>;

 const ErrorT& error;

 explicit reference_error_sender(const ErrorT& error)
  : error(error)
 {}

 reference_error_sender(reference_error_sender&&) = default;
 reference_error_sender(const reference_error_sender&) = delete;

 template <typename ReceiverT>
 struct state final
 {
  using operation_state_concept = stdexec::operation_state_tag;

  const ErrorT& error;
  ReceiverT receiver;

  void start() & noexcept { stdexec::set_error(std::move(receiver), error); }

 };

 template <typename ReceiverT>
 auto connect(ReceiverT receiver) const
 {
  return state<ReceiverT> {error, std::move(receiver)};
 }

};

} // namespace concurrency_test

using point_read_sender = decltype(lfdbx::get(
  lfdb::raw, lfdb::transaction_handle {}, std::string_view {}));
using point_read_completions = stdexec::completion_signatures<
  stdexec::set_value_t(lfdbx::point_read_result),
  stdexec::set_error_t(std::exception_ptr),
  stdexec::set_stopped_t()>;

static_assert(stdexec::sender<point_read_sender>);
static_assert(stdexec::sender_in<point_read_sender>);
static_assert(std::same_as<
  stdexec::completion_signatures_of_t<point_read_sender>,
  point_read_completions>);
static_assert(std::is_nothrow_move_constructible_v<lfdbx::point_read_result>);
static_assert(std::is_nothrow_move_assignable_v<lfdbx::point_read_result>);
static_assert(not std::copy_constructible<lfdbx::point_read_result>);

using scan_read_sender = decltype(lfdbx::read_window(
  lfdb::raw, lfdb::transaction_handle {},
  lfdbx::scan_cursor {lfdb::select {"a", "b"}}));
using scan_read_completions = stdexec::completion_signatures<
  stdexec::set_value_t(lfdbx::scan_window),
  stdexec::set_error_t(std::exception_ptr),
  stdexec::set_stopped_t()>;

static_assert(stdexec::sender<scan_read_sender>);
static_assert(stdexec::sender_in<scan_read_sender>);
static_assert(std::same_as<
  stdexec::completion_signatures_of_t<scan_read_sender>,
  scan_read_completions>);
static_assert(std::is_nothrow_move_constructible_v<lfdbx::scan_cursor>);
static_assert(std::is_nothrow_move_assignable_v<lfdbx::scan_cursor>);
static_assert(std::is_nothrow_move_constructible_v<lfdbx::scan_window>);
static_assert(std::is_nothrow_move_assignable_v<lfdbx::scan_window>);
static_assert(not std::copy_constructible<lfdbx::scan_cursor>);
static_assert(not std::copy_constructible<lfdbx::scan_window>);

template <typename T>
concept can_borrow_bytes_from_temporary = requires(T&& value) {
 std::move(value).bytes();
};

static_assert(not can_borrow_bytes_from_temporary<lfdbx::point_read_result>);

using watch_sender = decltype(lfdbx::when_changed(std::declval<lfdb::watch_handle>()));
using watch_completions = stdexec::completion_signatures<
  stdexec::set_value_t(),
  stdexec::set_error_t(std::exception_ptr),
  stdexec::set_stopped_t()>;

static_assert(stdexec::sender<watch_sender>);
static_assert(stdexec::sender_in<watch_sender>);
static_assert(std::same_as<
  stdexec::completion_signatures_of_t<watch_sender>,
  watch_completions>);

using commit_sender = decltype(lfdbx::commit(lfdb::transaction_handle {}));
using commit_completions = stdexec::completion_signatures<
  stdexec::set_value_t(),
  stdexec::set_error_t(std::exception_ptr),
  stdexec::set_stopped_t()>;

static_assert(stdexec::sender<commit_sender>);
static_assert(stdexec::sender_in<commit_sender>);
static_assert(std::same_as<
  stdexec::completion_signatures_of_t<commit_sender>,
  commit_completions>);

static_assert(std::same_as<stdexec::completion_signatures_of_t<
 decltype(lfdbx::commit(lfdb::with_result, lfdb::transaction_handle {}))>,
 stdexec::completion_signatures<
  stdexec::set_value_t(std::expected<void, std::error_code>),
  stdexec::set_error_t(std::exception_ptr),
  stdexec::set_stopped_t()>>);

using transaction_sender = decltype(lfdbx::transact(
  lfdb::database_handle {},
  [](lfdb::transaction_handle&) { return stdexec::just(42); }));
using transaction_completions = stdexec::completion_signatures<
  stdexec::set_value_t(int),
  stdexec::set_error_t(std::exception_ptr),
  stdexec::set_stopped_t()>;

static_assert(stdexec::sender<transaction_sender>);
static_assert(stdexec::sender_in<transaction_sender>);
static_assert(std::same_as<
  stdexec::completion_signatures_of_t<transaction_sender>,
  transaction_completions>);

template <typename T>
concept can_borrow_rows_from_temporary = requires(T&& value) {
 std::move(value).rows();
};

template <typename T>
concept can_advance_lvalue_window = requires(T& value) {
 value.next();
};

static_assert(not can_borrow_rows_from_temporary<lfdbx::scan_window>);
static_assert(not can_advance_lvalue_window<lfdbx::scan_window>);

using native_point_sender = point_read_sender::native_handle_type;
static_assert(stdexec::sender<native_point_sender>);
static_assert(std::same_as<decltype(std::declval<point_read_sender&>().native_handle()),
                           native_point_sender&>);
static_assert(std::same_as<decltype(std::declval<const point_read_sender&>().native_handle()),
                           const native_point_sender&>);
static_assert(std::same_as<decltype(std::declval<point_read_sender>().native_handle()),
                           native_point_sender>);

using read_version_sender = decltype(lfdbx::read_version(lfdb::transaction_handle {}));
using commit_size_sender = decltype(lfdbx::approximate_commit_bytes(lfdb::transaction_handle {}));
using range_size_sender = decltype(lfdbx::approximate_range_size(
  lfdb::transaction_handle {}, lfdb::query::empty()));
using int64_completions = stdexec::completion_signatures<
  stdexec::set_value_t(std::int64_t),
  stdexec::set_error_t(std::exception_ptr),
  stdexec::set_stopped_t()>;

static_assert(std::same_as<stdexec::completion_signatures_of_t<read_version_sender>,
                           int64_completions>);
static_assert(std::same_as<stdexec::completion_signatures_of_t<commit_size_sender>,
                           int64_completions>);
static_assert(std::same_as<stdexec::completion_signatures_of_t<range_size_sender>,
                           int64_completions>);

using partition_sender = decltype(lfdbx::partitions(
  lfdb::transaction_handle {}, lfdb::query::empty(), 256));
static_assert(std::same_as<stdexec::completion_signatures_of_t<partition_sender>,
  stdexec::completion_signatures<
   stdexec::set_value_t(std::vector<lfdb::select>),
   stdexec::set_error_t(std::exception_ptr),
   stdexec::set_stopped_t()>>);

TEST_CASE("Experimental metadata composes directly with native stdexec algorithms",
          "[fdb][execution][metadata]")
{
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 auto work = stdexec::when_all(lfdbx::read_version(txn),
   lfdbx::approximate_commit_bytes(txn),
   lfdbx::approximate_range_size(txn, lfdb::query::empty()),
   lfdbx::partitions(txn, lfdb::query::empty(), 256)) |
   stdexec::then([](const std::int64_t version, const std::int64_t size,
                    const std::int64_t estimate, std::vector<lfdb::select> ranges) {
    CHECK(0 <= size);
    CHECK(0 == estimate);
    CHECK(ranges.empty());
    return version;
   });
 auto result = stdexec::sync_wait(std::move(work));
 REQUIRE(result.has_value());
 CHECK(0 < std::get<0>(*result));
}

TEST_CASE("Experimental metadata honors receiver stop before initiation",
          "[fdb][execution][metadata]")
{
 stdexec::inplace_stop_source stop;
 stop.request_stop();
 const auto stopped = [&](auto operation) {
  auto work = std::move(operation) | stdexec::write_env(stdexec::prop {
    stdexec::get_stop_token, stop.get_token()
  });
  CHECK_FALSE(stdexec::sync_wait(std::move(work)).has_value());
 };
 stopped(lfdbx::read_version({}));
 stopped(lfdbx::approximate_commit_bytes({}));
 stopped(lfdbx::approximate_range_size({}, lfdb::query::empty()));
 stopped(lfdbx::partitions({}, lfdb::query::empty(), 0));
}

TEST_CASE("Experimental operation preserves native sender composition and error types",
          "[fdb][execution][native]")
{
 auto work = lfdbx::operation {stdexec::just(40, std::make_unique<int>(2))} |
             stdexec::then([](const int value, auto increment) { return value + *increment; });
 auto completed = stdexec::sync_wait(std::move(work));
 REQUIRE(completed.has_value());
 CHECK(42 == std::get<0>(*completed));

 auto failure = lfdbx::operation {stdexec::just_error(lfdb::libfdb_exception {lfdb::fdb_error_code::client_invalid_operation})};
 using native_failure = decltype(failure)::native_handle_type;
 static_assert(std::same_as<stdexec::completion_signatures_of_t<decltype(failure)>,
                            stdexec::completion_signatures_of_t<native_failure>>);
 auto recovered = stdexec::sync_wait(std::move(failure) |
   stdexec::upon_error([](const lfdb::libfdb_exception& error) { return error.code().value(); }));
 REQUIRE(recovered.has_value());
 CHECK(2000 == std::get<0>(*recovered));
}

TEST_CASE("Experimental operation forwards native completion scheduler queries",
          "[fdb][execution][native]")
{
 stdexec::run_loop loop;
 auto work = lfdbx::operation {stdexec::schedule(loop.get_scheduler())};
 CHECK(loop.get_scheduler() == stdexec::get_completion_scheduler<stdexec::set_value_t>(
   stdexec::get_env(work)));
 auto native = std::move(work).native_handle();
 CHECK(loop.get_scheduler() == stdexec::get_completion_scheduler<stdexec::set_value_t>(
   stdexec::get_env(native)));
}

void write_bytes(lfdb::database_handle dbh, const std::string_view key,
                 const std::string_view value)
{
 auto txn = lfdb::make_transaction(std::move(dbh));
 write_bytes(txn, key, value);

 REQUIRE_NOTHROW(lfdb::commit(txn));
}

std::vector<std::string> collect_scan_keys(lfdb::transaction_handle txn,
                                           lfdbx::scan_cursor cursor)
{
 std::vector<std::string> keys;

 while (cursor) {
  auto completed = stdexec::sync_wait(
    lfdbx::read_window(lfdb::raw, txn, std::move(cursor)));

  REQUIRE(completed.has_value());

  auto window = std::move(std::get<0>(*completed));

  for (const auto row : window.rows()) {
   keys.emplace_back(as_string_view(row.key));
  }

  cursor = std::move(window).next();
 }

 return keys;
}

TEST_CASE("FDB transaction senders commit before publishing values",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/transaction/value");

 auto completed = stdexec::sync_wait(lfdbx::transact(
   dbh,
   [key](lfdb::transaction_handle& txn) {
     lfdb::set(txn, key, "committed");

     return stdexec::just(42);
   }));

 REQUIRE(completed.has_value());
 CHECK(42 == std::get<0>(*completed));

 std::string value;

 REQUIRE(lfdb::get(dbh.dbh(), key, value));
 CHECK("committed" == value);
}

TEST_CASE("FDB transaction senders compose multiple reads",
          "[fdb][execution]")
{
 janitor dbh;
 const auto first_key = test_key("execution/transaction/first");
 const auto second_key = test_key("execution/transaction/second");
 write_bytes(dbh, first_key, "one");
 write_bytes(dbh, second_key, "two");

 auto completed = stdexec::sync_wait(lfdbx::transact(
   dbh,
   [first_key, second_key](lfdb::transaction_handle& txn) {
     return stdexec::when_all(
       lfdbx::get(lfdb::raw, txn, first_key),
       lfdbx::get(lfdb::raw, txn, second_key));
   }));

 REQUIRE(completed.has_value());

 auto& [first, second] = *completed;

 REQUIRE(first);
 REQUIRE(second);
 CHECK("one" == as_string_view(*first.bytes()));
 CHECK("two" == as_string_view(*second.bytes()));
}

TEST_CASE("FDB transaction senders preserve scheduler-dependent bodies",
          "[fdb][execution][coroutine]")
{
 janitor dbh;
 exec::single_thread_context application;
 const auto scheduler = application.get_scheduler();
 const auto key = test_key("execution/transaction/environment");
 auto work = lfdbx::transact(dbh, [key, scheduler](lfdb::transaction_handle txn) {
  return stdexec::read_env(stdexec::get_start_scheduler) |
         stdexec::then([key, scheduler, txn](const auto actual) {
          CHECK(scheduler == actual);
          lfdb::set(txn, key, "scheduled");
          return 42;
         });
 });
 static_assert(stdexec::dependent_sender<decltype(work)>);
 auto completed = stdexec::sync_wait(stdexec::starts_on(scheduler, std::move(work)));
 REQUIRE(completed.has_value());
 CHECK(42 == std::get<0>(*completed));
 std::string value;
 REQUIRE(lfdb::get(dbh.dbh(), key, value));
 CHECK("scheduled" == value);
}

TEST_CASE("FDB coroutine transaction bodies retain their scheduler across reads",
          "[fdb][execution][coroutine]")
{
 janitor dbh;
 exec::single_thread_context application;
 const auto first_key = test_key("execution/coroutine/first");
 const auto second_key = test_key("execution/coroutine/second");
 const auto output_key = test_key("execution/coroutine/output");
 write_bytes(dbh, first_key, "one");
 write_bytes(dbh, second_key, "two");
 auto body = [first_key, second_key, output_key](lfdb::transaction_handle txn)
  -> stdexec::task<std::string> {
  const auto thread = std::this_thread::get_id();
  auto [first, second] = co_await stdexec::when_all(
    lfdbx::get(lfdb::raw, txn, first_key), lfdbx::get(lfdb::raw, txn, second_key));
  CHECK(thread == std::this_thread::get_id());
  REQUIRE(first.has_value());
  REQUIRE(second.has_value());
  auto result = std::string {as_string_view(*first.bytes())} +
                std::string {as_string_view(*second.bytes())};
  lfdb::set(txn, output_key, result);
  co_return result;
 };
 auto completed = stdexec::sync_wait(stdexec::starts_on(application.get_scheduler(),
   lfdbx::transact(dbh, std::move(body))));
 REQUIRE(completed.has_value());
 CHECK("onetwo" == std::get<0>(*completed));
 std::string value;
 REQUIRE(lfdb::get(dbh.dbh(), output_key, value));
 CHECK("onetwo" == value);
}

TEST_CASE("FDB coroutine transaction bodies replay with isolated staged output",
          "[fdb][execution][coroutine]")
{
 janitor dbh;
 exec::single_thread_context application;
 const auto key = test_key("execution/coroutine/replay");
 std::vector<int> published;
 int attempts = 0;
 auto body = [&attempts, key](lfdb::transaction_handle txn, auto& output)
  -> stdexec::task<int> {
  ++attempts;
  std::ignore = co_await lfdbx::get(lfdb::raw, txn, key);
  output.push_back(attempts);
  if (1 == attempts) {
   throw lfdb::libfdb_exception {lfdb::fdb_error_code::not_committed};
  }
  lfdb::set(txn, key, "committed");
  co_return attempts;
 };
 auto completed = stdexec::sync_wait(stdexec::starts_on(application.get_scheduler(),
   lfdbx::transact(dbh, std::move(body), lfdb::staged(published, std::in_place))));
 REQUIRE(completed.has_value());
 CHECK(2 == std::get<0>(*completed));
 CHECK(std::vector {2} == published);
}

TEST_CASE("FDB coroutine transaction bodies preserve application errors and stop",
          "[fdb][execution][coroutine]")
{
 janitor dbh;
 exec::single_thread_context application;
 const auto key = test_key("execution/coroutine/terminal");
 const bool stop = GENERATE(false, true);
 std::vector<int> published;
 auto body = [key, stop](lfdb::transaction_handle txn, auto& output) -> stdexec::task<> {
  lfdb::set(txn, key, "uncommitted");
  output.push_back(1);
  if (stop) {
   co_await stdexec::just_stopped();
   co_return;
  }
  throw std::invalid_argument {"coroutine application failure"};
 };
 auto work = stdexec::starts_on(application.get_scheduler(),
   lfdbx::transact(dbh, std::move(body), lfdb::staged(published, std::in_place)));
 if (stop) {
  CHECK_FALSE(stdexec::sync_wait(std::move(work)).has_value());
 }
 if (not stop) {
  REQUIRE_THROWS_AS(stdexec::sync_wait(std::move(work)), std::invalid_argument);
 }
 CHECK(published.empty());
 CHECK_FALSE(lfdb::key_exists(dbh.dbh(), key));
}

TEST_CASE("Async traversal accepts scheduled coroutine consumers", "[fdb][execution][traversal]")
{
 janitor dbh;
 exec::single_thread_context application;
 const auto prefix = "execution-coroutine-traversal";
 auto txn = lfdb::make_transaction(dbh);
 for (int i = 0; i < 5; ++i) {
  write_bytes(txn, make_key(i, prefix), std::to_string(i));
 }
 lfdb::commit(txn);
 auto reader = lfdb::make_transaction(dbh);
 const auto thread = std::get<0>(*stdexec::sync_wait(stdexec::schedule(application.get_scheduler()) |
   stdexec::then([] { return std::this_thread::get_id(); })));
 std::vector<std::string> keys;
 auto consume = [reader, &keys, thread](auto key, auto value) -> stdexec::task<> {
  auto result = co_await lfdbx::get(lfdb::raw, reader, as_string_view(key));
  CHECK(thread == std::this_thread::get_id());
  REQUIRE(result.has_value());
  CHECK(as_string_view(value) == as_string_view(*result.bytes()));
  keys.emplace_back(as_string_view(key));
 };
 auto work = lfdbx::for_each(lfdb::raw, dbh.dbh(), lfdb::query::with_options(
   lfdb::query::prefix(make_key_prefix(prefix)), lfdb::query::query_options {.result_limit = 2}),
   lfdbx::scan_options {.prefetch = true}, std::move(consume));
 auto completed = stdexec::sync_wait(stdexec::starts_on(application.get_scheduler(), std::move(work)));
 REQUIRE(completed.has_value());
 CHECK(5 == std::get<0>(*completed));
 CHECK(5 == keys.size());
}

TEST_CASE("Async traversal preserves native consumer error channels", "[fdb][execution][traversal]")
{
 struct application_error final { std::unique_ptr<int> value; };
 janitor dbh;
 const bool empty = GENERATE(false, true);
 auto txn = lfdb::make_transaction(dbh);
 const auto prefix = "execution-traversal/typed-error";
 if (not empty) {
  write_bytes(txn, make_key(0, prefix), "value");
  write_bytes(txn, make_key(1, prefix), "value");
 }
 int called = 0;
 auto work = lfdbx::for_each(lfdb::raw, txn, lfdb::query::with_options(
   lfdb::query::prefix(make_key_prefix(prefix)), lfdb::query::query_options {.result_limit = 1}),
   lfdbx::scan_options {.prefetch = true}, [&called](auto, auto) {
    ++called;
    return stdexec::just_error(application_error {std::make_unique<int>(42)});
   }) | stdexec::upon_error([](auto error) -> std::size_t {
  if constexpr (std::same_as<decltype(error), application_error>) {
   return *error.value;
  } else {
   std::rethrow_exception(error);
  }
 });
 auto completed = stdexec::sync_wait(std::move(work));
 REQUIRE(completed.has_value());
 CHECK((empty ? 0 : 42) == std::get<0>(*completed));
 CHECK((empty ? 0 : 1) == called);
}

TEST_CASE("Async traversal preserves stopped-only consumers and empty batches",
          "[fdb][execution][traversal]")
{
 janitor dbh;
 const bool empty = GENERATE(false, true);
 auto txn = lfdb::make_transaction(dbh);
 const auto prefix = "execution-traversal/stopped";
 if (not empty) {
  write_bytes(txn, make_key(0, prefix), "value");
  write_bytes(txn, make_key(1, prefix), "value");
 }
 int called = 0;
 auto work = lfdbx::for_each(lfdb::raw, txn, lfdb::query::with_options(
   lfdb::query::prefix(make_key_prefix(prefix)), lfdb::query::query_options {.result_limit = 1}),
   lfdbx::scan_options {.prefetch = true}, [&called](auto, auto) {
    ++called;
    return stdexec::just_stopped();
   });
 auto completed = stdexec::sync_wait(std::move(work));
 CHECK(empty == completed.has_value());
 if (completed) {
  CHECK(0 == std::get<0>(*completed));
 }
 CHECK((empty ? 0 : 1) == called);
}

TEST_CASE("Async traversal owns move-only consumers and normalizes reference errors",
          "[fdb][execution][traversal]")
{
 janitor dbh;
 const bool prefetch = GENERATE(false, true);
 auto txn = lfdb::make_transaction(dbh);
 const auto prefix = "execution-traversal/reference-error";
 write_bytes(txn, make_key(0, prefix), "value");
 write_bytes(txn, make_key(1, prefix), "value");
 const auto original = std::make_exception_ptr(std::invalid_argument {"consumer failure"});
 auto work = lfdbx::for_each(lfdb::raw, txn, lfdb::query::with_options(
   lfdb::query::prefix(make_key_prefix(prefix)), lfdb::query::query_options {.result_limit = 1}),
   lfdbx::scan_options {.prefetch = prefetch}, [&original](auto, auto) {
    return concurrency_test::reference_error_sender {original};
   });
 static_assert(lfdbx::detail::asio_waitable_sender<decltype(work)>);
 std::exception_ptr observed;
 try {
  std::ignore = lfdbx::wait(std::move(work), null_yield);
 } catch (...) {
  observed = std::current_exception();
 }
 CHECK(original == observed);
}

TEST_CASE("Async traversal contains throwing error copies",
          "[fdb][execution][traversal]")
{
 struct error_value final
 {
  error_value() = default;
  error_value(error_value&&) noexcept = default;
  error_value(const error_value&) { throw std::runtime_error {"error copy"}; }

 };

 janitor dbh;
 const bool prefetch = GENERATE(false, true);
 auto txn = lfdb::make_transaction(dbh);
 const auto prefix = "execution-traversal/throwing-error-copy";
 write_bytes(txn, make_key(0, prefix), "value");
 write_bytes(txn, make_key(1, prefix), "value");
 const error_value original;
 auto work = lfdbx::for_each(lfdb::raw, txn, lfdb::query::with_options(
   lfdb::query::prefix(make_key_prefix(prefix)), lfdb::query::query_options {.result_limit = 1}),
   lfdbx::scan_options {.prefetch = prefetch}, [&original](auto, auto) {
    return concurrency_test::reference_error_sender {original};
   }) | stdexec::upon_error([](auto&& error) {
    if constexpr (std::same_as<std::decay_t<decltype(error)>, std::exception_ptr>) {
     std::rethrow_exception(error);
    }
    return std::size_t {0};
   });
 REQUIRE_THROWS_AS(stdexec::sync_wait(std::move(work)), std::runtime_error);
}

TEST_CASE("FDB transaction senders publish staged output after replay",
          "[fdb][execution]")
{
 struct count_delete final
 {
  std::reference_wrapper<std::size_t> destructions;

  void operator()(int *value) const noexcept
  {
   ++destructions.get();
   delete value;
  }
 };

 using counted_attempt = std::unique_ptr<int, count_delete>;

 janitor dbh;
 const auto conflict_key = test_key("execution/transaction/conflict");
 const auto output_key = test_key("execution/transaction/output");
 write_bytes(dbh, conflict_key, "before");

 std::vector<int> published;
 std::size_t attempts = 0;
 std::size_t discarded_results = 0;
 int decisions = 0;

 auto completed = stdexec::sync_wait(lfdbx::transact(
   lfdb::retry_if([&](const lfdb::transaction_failure& failure) {
    ++decisions;
    CHECK(lfdb::transaction_phase::commit == failure.phase);
    CHECK(lfdb::fdb_error_code::not_committed == failure.code);
    return true;
   }),
   dbh,
   [&, conflict_key, output_key](lfdb::transaction_handle& txn,
                                 auto& staged_output) {
     ++attempts;
     lfdb::set(txn, output_key, "committed");

     return lfdbx::get(lfdb::raw, txn, conflict_key) |
            stdexec::let_value(
              [&, this_attempt = attempts](auto& result) {
                if (not result) {
                 throw std::runtime_error("missing conflict value");
                }

                return lfdbx::transact(
                  dbh,
                  [conflict_key, this_attempt](
                    lfdb::transaction_handle& conflict_txn) {
                    if (1 == this_attempt) {
                     lfdb::set(conflict_txn, conflict_key, "changed");
                    }

                    return stdexec::just();
                  }) |
                  stdexec::then([&staged_output, &discarded_results,
                                 this_attempt] {
                    staged_output.push_back(
                      static_cast<int>(this_attempt));

                    return counted_attempt {
                     new int(static_cast<int>(this_attempt)),
                     count_delete {discarded_results}
                    };
                   });
              });
   },
   lfdb::staged(published, std::in_place)));

 REQUIRE(completed.has_value());
 CHECK(2 == attempts);
 CHECK(2 == *std::get<0>(*completed));
 CHECK(std::vector {2} == published);
 CHECK(1 == discarded_results);
 CHECK(1 == decisions);

 std::string value;

 REQUIRE(lfdb::get(dbh.dbh(), output_key, value));
 CHECK("committed" == value);
}

TEST_CASE("FDB transaction senders replay retryable body errors",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/transaction/body-replay");
 std::size_t attempts = 0;

 auto completed = stdexec::sync_wait(lfdbx::transact(
   dbh,
   [&, key](lfdb::transaction_handle& txn) {
     ++attempts;
     lfdb::set(txn, key, "committed");

     return stdexec::just() | stdexec::then([this_attempt = attempts] {
       if (1 == this_attempt) {
        throw lfdb::libfdb_exception {lfdb::fdb_error_code::not_committed};
       }

       return this_attempt;
     });
   }));

 REQUIRE(completed.has_value());
 CHECK(2 == attempts);
 CHECK(2 == std::get<0>(*completed));
}

TEST_CASE("FDB transaction senders discard failed-attempt version stamps",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/transaction/replay-versionstamp");
 lfdb::versionstamp abandoned_stamp;
 std::size_t attempts = 0;

 auto completed = stdexec::sync_wait(lfdbx::transact(
   dbh,
   [&](lfdb::transaction_handle& txn) {
     ++attempts;

     if (1 == attempts) {
      lfdb::set(txn, key, lfdb::versioned("", abandoned_stamp));
     }

     if (1 < attempts) {
      lfdb::set(txn, key, "committed");
     }

     return stdexec::just() | stdexec::then([this_attempt = attempts] {
       if (1 == this_attempt) {
        throw lfdb::libfdb_exception {lfdb::fdb_error_code::not_committed};
       }
     });
   }));

 REQUIRE(completed.has_value());
 CHECK(2 == attempts);
 CHECK_FALSE(abandoned_stamp.has_value());
}

TEST_CASE("FDB transaction senders replay direct retryable errors",
          "[fdb][execution]")
{
 janitor dbh;
 std::size_t attempts = 0;

 auto work = lfdbx::transact(
   dbh,
   [&](lfdb::transaction_handle&) {
     ++attempts;

     using success_sender_t = decltype(stdexec::just());
     using error_sender_t = decltype(
       stdexec::just_error(lfdb::libfdb_exception {lfdb::fdb_error_code::not_committed}));
     using body_sender_t = exec::variant_sender<success_sender_t,
                                                error_sender_t>;

     if (1 == attempts) {
      return body_sender_t {
       stdexec::just_error(lfdb::libfdb_exception {lfdb::fdb_error_code::not_committed})
      };
     }

     return body_sender_t {stdexec::just()};
   });

 CHECK(stdexec::sync_wait(std::move(work)).has_value());
 CHECK(2 == attempts);
}

TEST_CASE("FDB transaction senders preserve direct non-retryable errors",
          "[fdb][execution]")
{
 janitor dbh;
 std::size_t attempts = 0;

 auto work = lfdbx::transact(
   dbh,
   [&](lfdb::transaction_handle&) {
     ++attempts;

     using success_sender_t = decltype(stdexec::just());
     using error_sender_t = decltype(
       stdexec::just_error(lfdb::libfdb_exception {lfdb::fdb_error_code::client_invalid_operation}));

     return exec::variant_sender<success_sender_t, error_sender_t> {
      stdexec::just_error(lfdb::libfdb_exception {lfdb::fdb_error_code::client_invalid_operation})
     };
   });

 try {
  std::ignore = stdexec::sync_wait(std::move(work));
  FAIL("non-retryable body error unexpectedly succeeded");
 } catch (const lfdb::libfdb_exception& e) {
  CHECK(std::error_code(2000, lfdb::fdb_error_category()) == e.code());
 }

 CHECK(1 == attempts);
}

TEST_CASE("FDB transaction senders own move-only callables and arguments",
          "[fdb][execution]")
{
 janitor dbh;
 auto value = std::make_unique<int>(40);
 auto caller_database = dbh.dbh();

 auto work = lfdbx::transact(
   caller_database,
   [increment = std::make_unique<int>(2)](
     lfdb::transaction_handle&, auto& input) {
     return stdexec::just(*input + *increment);
   },
   std::move(value));

 caller_database.reset();

 auto completed = stdexec::sync_wait(std::move(work));

 REQUIRE(completed.has_value());
 CHECK_FALSE(value);
 CHECK(42 == std::get<0>(*completed));
}

TEST_CASE("FDB transaction senders publish ordinary staged output",
          "[fdb][execution]")
{
 janitor dbh;
 int published = 35;

 auto completed = stdexec::sync_wait(lfdbx::transact(
   dbh,
   [](lfdb::transaction_handle&, auto& staged_output) {
     staged_output += 7;

     return stdexec::just();
   },
   lfdb::staged(published)));

 REQUIRE(completed.has_value());
 CHECK(42 == published);
}

TEST_CASE("FDB transaction senders replay retryable invocation errors",
          "[fdb][execution]")
{
 const auto retries = GENERATE(1, 256);
 janitor dbh;
 const auto key = test_key("execution/transaction/invocation-replay");
 std::size_t attempts = 0;
 const lfdb::transaction_options options {
  {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {retries}},
  {FDB_TR_OPTION_MAX_RETRY_DELAY, std::int64_t {0}}
 };

 auto completed = stdexec::sync_wait(lfdbx::transact(
   dbh, options,
   [&, key](lfdb::transaction_handle& txn)
     -> decltype(stdexec::just(42)) {
     ++attempts;

     if (attempts <= static_cast<std::size_t>(retries)) {
      throw lfdb::libfdb_exception {lfdb::fdb_error_code::not_committed};
     }

     lfdb::set(txn, key, "committed");

     return stdexec::just(42);
   }));

 REQUIRE(completed.has_value());
 CHECK(1 + retries == attempts);
 CHECK(42 == std::get<0>(*completed));
 CHECK(lfdb::key_exists(dbh, key));
}

TEST_CASE("FDB transaction senders preserve non-retryable body errors",
          "[fdb][execution]")
{
 janitor dbh;
 int published = 7;

 auto work = lfdbx::transact(
   dbh,
   [](lfdb::transaction_handle&, auto& staged_output) {
     staged_output += 5;

     return stdexec::just() | stdexec::then([]() -> int {
       throw std::runtime_error("body failed");
     });
   },
   lfdb::staged(published));

 CHECK_THROWS_AS(stdexec::sync_wait(std::move(work)), std::runtime_error);
 CHECK(7 == published);
}

TEST_CASE("FDB transaction senders release completed attempts during replay",
          "[fdb][execution][lifetime]")
{
 const auto retries = GENERATE(0, 1, 256);
 const auto error = GENERATE(1020, 1021);
 janitor dbh;
 const auto key = test_key("execution/transaction/bounded-replay");
 const auto lifetime = std::make_shared<int>(42);
 long max_live = 0;
 std::size_t attempts = 0;
 int published = 7;
 lfdb::versionstamp stamp;
 const lfdb::transaction_options options {
  {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {retries}},
  {FDB_TR_OPTION_MAX_RETRY_DELAY, std::int64_t {0}}
 };

 auto completed = stdexec::sync_wait(lfdbx::transact(dbh, options,
   [&](lfdb::transaction_handle& txn, auto& output) {
    ++attempts;
    output += 1;
    lfdb::set(txn, key, lfdb::versioned("", stamp));

    return stdexec::just(lifetime) |
     stdexec::then([&](const std::shared_ptr<int>& value) {
      max_live = std::max(max_live, lifetime.use_count());

      if (attempts <= static_cast<std::size_t>(retries)) {
       throw lfdb::libfdb_exception {
        std::error_code(error, lfdb::fdb_error_category())};
      }

      return std::make_unique<int>(*value);
     });
   }, lfdb::staged(published)));

 REQUIRE(completed.has_value());
 CHECK(42 == *std::get<0>(*completed));
 CHECK(1 + retries == attempts);
 CHECK(2 == max_live);
 CHECK(1 == lifetime.use_count());
 CHECK(8 == published);
 CHECK(stamp.has_value());
}

TEST_CASE("FDB transaction senders preserve the final replay error",
          "[fdb][execution]")
{
 const auto retry_limit = GENERATE(0, 2, 12);
 const auto error = GENERATE(1020, 1021);
 janitor dbh;
 std::size_t attempts = 0;
 const lfdb::transaction_options options {
  {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {retry_limit}},
  {FDB_TR_OPTION_MAX_RETRY_DELAY, std::int64_t {1}}
 };

 auto work = lfdbx::transact(
   dbh, options,
   [&](lfdb::transaction_handle&) {
     ++attempts;

     return stdexec::just() | stdexec::then([error]() -> int {
       throw lfdb::libfdb_exception {std::error_code(error, lfdb::fdb_error_category())};
     });
   });

 try {
  std::ignore = stdexec::sync_wait(std::move(work));
  FAIL("exhausted transaction sender unexpectedly succeeded");
 } catch (const lfdb::libfdb_exception& e) {
  CHECK(std::error_code(error, lfdb::fdb_error_category()) == e.code());
 }

 CHECK(1 + retry_limit == attempts);
}

TEST_CASE("FDB transaction senders accept transaction options",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/transaction/options");
 const lfdb::transaction_options options {
  {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {3}}
 };

 auto completed = stdexec::sync_wait(lfdbx::transact(
   dbh, options,
   [key](lfdb::transaction_handle& txn) {
     lfdb::set(txn, key, "committed");

     return stdexec::just();
   }));

 REQUIRE(completed.has_value());
 CHECK(lfdb::key_exists(dbh, key));
}

TEST_CASE("FDB transaction senders publish version stamps",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/transaction/versionstamp");
 lfdb::versionstamp stamp;

 auto completed = stdexec::sync_wait(lfdbx::transact(
   dbh,
   [&, key](lfdb::transaction_handle& txn) {
     lfdb::set(txn, key, lfdb::versioned("", stamp));

     return stdexec::just();
   }));

 REQUIRE(completed.has_value());
 REQUIRE(stamp.has_value());
 CHECK(10 == std::size(stamp.resolved_bytes()));

 lfdb::versionstamp stored;

 REQUIRE(lfdb::get(dbh, key, stored));
 CHECK(stamp.resolved_bytes() == stored.resolved_bytes());
}

TEST_CASE("FDB transaction senders report invalid databases",
          "[fdb][execution]")
{
 auto work = lfdbx::transact(
   lfdb::database_handle {},
   [](lfdb::transaction_handle&) { return stdexec::just(); });

 CHECK_THROWS_AS(stdexec::sync_wait(std::move(work)), lfdb::libfdb_exception);
}

TEST_CASE("FDB transaction senders honor stop before start",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/transaction/stopped");
 std::size_t invocations = 0;
 stdexec::inplace_stop_source stop;
 stop.request_stop();

 auto work = lfdbx::transact(
   dbh,
   [&, key](lfdb::transaction_handle& txn) {
     ++invocations;
     lfdb::set(txn, key, "not committed");

     return stdexec::just(42);
   }) |
   stdexec::write_env(stdexec::prop {
     stdexec::get_stop_token, stop.get_token()
   });

 CHECK_FALSE(stdexec::sync_wait(std::move(work)).has_value());
 CHECK(0 == invocations);
 CHECK_FALSE(lfdb::key_exists(dbh, key));
}

TEST_CASE("FDB transaction senders honor cancellation during the body",
          "[fdb][execution][asio]")
{
 janitor dbh;
 int published = 7;

 auto work = lfdbx::transact(
   dbh,
   [&](lfdb::transaction_handle& txn, auto& staged_output) {
     staged_output += 5;
     delay_reads(dbh.dbh(), txn);

     return lfdbx::get(
       lfdb::raw, txn, test_key("execution/transaction/cancel-body")) |
       stdexec::then([](auto) { return 42; });
   },
   lfdb::staged(published));

 check_active_cancellation(std::move(work), boost::asio::cancellation_type::all);
 CHECK(7 == published);
}

TEST_CASE("FDB transaction senders honor cancellation between retries",
          "[fdb][execution]")
{
 janitor dbh;
 std::size_t attempts = 0;
 stdexec::inplace_stop_source stop;

 auto work = lfdbx::transact(
   dbh,
   [&](lfdb::transaction_handle&) {
     ++attempts;

     return stdexec::just() | stdexec::then([&]() -> int {
       stop.request_stop();
       throw lfdb::libfdb_exception {lfdb::fdb_error_code::not_committed};
     });
   }) |
   stdexec::write_env(stdexec::prop {
     stdexec::get_stop_token, stop.get_token()
   });

 CHECK_FALSE(stdexec::sync_wait(std::move(work)).has_value());
 CHECK(1 == attempts);
}

TEST_CASE("FDB transaction senders do not hide maybe-committed errors",
          "[fdb][execution]")
{
 janitor dbh;
 std::size_t attempts = 0;
 stdexec::inplace_stop_source stop;

 auto work = lfdbx::transact(
   dbh,
   [&](lfdb::transaction_handle&) {
     ++attempts;

     return stdexec::just() | stdexec::then([&]() -> int {
       stop.request_stop();
       throw lfdb::libfdb_exception {lfdb::fdb_error_code::commit_unknown_result};
     });
   }) |
   stdexec::write_env(stdexec::prop {
     stdexec::get_stop_token, stop.get_token()
   });

 try {
  std::ignore = stdexec::sync_wait(std::move(work));
  FAIL("maybe-committed error was replaced with stopped completion");
 } catch (const lfdb::libfdb_exception& e) {
  CHECK(std::error_code(1021, lfdb::fdb_error_category()) == e.code());
 }

 CHECK(1 == attempts);
}

TEST_CASE("FDB transaction senders ignore cancellation after commit submission",
          "[fdb][execution][asio]")
{
 janitor dbh;
 const auto key = test_key("execution/transaction/late-stop");
 boost::asio::io_context context;
 boost::asio::cancellation_signal cancel;
 std::exception_ptr error;
 std::optional<int> result;

 lfdbx::async_wait(
   lfdbx::transact(
     dbh,
     [key](lfdb::transaction_handle& txn) {
       lfdb::set(txn, key, "committed");

       return stdexec::just(42);
     }),
   boost::asio::bind_cancellation_slot(
     cancel.slot(),
     boost::asio::bind_executor(
       context.get_executor(),
       [&](std::exception_ptr completion_error,
           std::optional<int> completion_result) {
        error = std::move(completion_error);
        result = completion_result;
       })));

 // async_wait() has started the inline body and submitted the commit:
 cancel.emit(boost::asio::cancellation_type::all);

 REQUIRE(context.run());
 CHECK_FALSE(error);
 REQUIRE(result);
 CHECK(42 == *result);
 CHECK(lfdb::key_exists(dbh, key));
}

TEST_CASE("FDB transaction senders cross the Asio yield bridge",
          "[fdb][execution][asio]")
{
 janitor dbh;
 const auto key = test_key("execution/transaction/asio-yield");
 boost::asio::io_context context;
 std::optional<int> result;

 boost::asio::spawn(
   context,
   [&](boost::asio::yield_context yield) {
    result.emplace(lfdbx::wait(
      lfdbx::transact(
        dbh,
        [key](lfdb::transaction_handle& txn) {
          lfdb::set(txn, key, "committed");

          return stdexec::just(42);
        }),
      optional_yield {yield}));
   }, boost::asio::detached);

 REQUIRE(context.run());
 REQUIRE(result);
 CHECK(42 == *result);
 CHECK(lfdb::key_exists(dbh, key));
}

TEST_CASE("FDB execution point reads own their result", "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/owned-result");
 const auto expected = "a zero-copy value"s;
 write_bytes(dbh, key, expected);

 auto txn = lfdb::make_transaction(dbh);
 auto completed = stdexec::sync_wait(lfdbx::get(lfdb::raw, txn, key));

 REQUIRE(completed.has_value());

 auto result = std::move(std::get<0>(*completed));
 txn.reset();

 REQUIRE(result.has_value());
 CHECK(static_cast<bool>(result));
 REQUIRE(expected == as_string_view(*result.bytes()));
}

TEST_CASE("FDB execution point-read senders own their key",
          "[fdb][execution]")
{
 janitor dbh;
 auto key = test_key("execution/owned-key");
 const auto expected = "the sender outlives its argument"s;
 write_bytes(dbh, key, expected);

 auto point_read = lfdbx::get(
   lfdb::raw, lfdb::make_transaction(dbh), std::string_view {key});
 key.assign("no longer the selected key");

 auto completed = stdexec::sync_wait(std::move(point_read));

 REQUIRE(completed.has_value());

 const auto& result = std::get<0>(*completed);

 REQUIRE(result);
 CHECK(expected == as_string_view(*result.bytes()));
}

TEST_CASE("FDB execution point results transfer ownership on assignment",
          "[fdb][execution]")
{
 janitor dbh;
 const auto first_key = test_key("execution/move-result/first");
 const auto second_key = test_key("execution/move-result/second");
 write_bytes(dbh, first_key, "first");
 write_bytes(dbh, second_key, "second");

 auto first_completed = stdexec::sync_wait(lfdbx::get(
   lfdb::raw, lfdb::make_transaction(dbh), first_key));
 auto second_completed = stdexec::sync_wait(lfdbx::get(
   lfdb::raw, lfdb::make_transaction(dbh), second_key));

 REQUIRE(first_completed.has_value());
 REQUIRE(second_completed.has_value());

 auto first = std::move(std::get<0>(*first_completed));
 auto second = std::move(std::get<0>(*second_completed));
 first = std::move(second);

 REQUIRE(first.bytes());
 CHECK("second"sv == as_string_view(*first.bytes()));
 CHECK_FALSE(second.bytes());
}

TEST_CASE("FDB execution point reads represent a missing key as a value",
          "[fdb][execution]")
{
 janitor dbh;
 auto completed = stdexec::sync_wait(
   lfdbx::get(
     lfdb::raw, lfdb::make_transaction(dbh), test_key("execution/missing")));

 REQUIRE(completed.has_value());

 const auto& result = std::get<0>(*completed);

 CHECK_FALSE(result.has_value());
 CHECK_FALSE(static_cast<bool>(result));
 CHECK_FALSE(result.bytes());
}

TEST_CASE("FDB execution point results support explicit value decoding",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/decoded-result");
 const auto expected = "decode only when requested"s;
 lfdb::set(lfdb::make_transaction(dbh), key, expected,
           lfdb::commit_after_op::commit);

 auto completed = stdexec::sync_wait(
   lfdbx::get(lfdb::raw, lfdb::make_transaction(dbh), key));

 REQUIRE(completed.has_value());

 const auto& result = std::get<0>(*completed);
 const auto bytes = result.bytes();

 REQUIRE(bytes);

 std::string decoded;
 lfdb::from::convert(*bytes, decoded);

 CHECK(expected == decoded);
}

TEST_CASE("FDB execution point reads compose with when_all", "[fdb][execution]")
{
 janitor dbh;
 const auto first_key = test_key("execution/first");
 const auto second_key = test_key("execution/second");
 write_bytes(dbh, first_key, "first");
 write_bytes(dbh, second_key, "second");

 auto txn = lfdb::make_transaction(dbh);
 auto completed = stdexec::sync_wait(stdexec::when_all(
   lfdbx::get(lfdb::raw, txn, first_key),
   lfdbx::get(lfdb::raw, txn, second_key)));

 REQUIRE(completed.has_value());

 const auto& [first, second] = *completed;

 REQUIRE("first"sv == as_string_view(*first.bytes()));
 REQUIRE("second"sv == as_string_view(*second.bytes()));
}

TEST_CASE("FDB execution point reads honor stop before start", "[fdb][execution]")
{
 janitor dbh;
 stdexec::inplace_stop_source stop;
 stop.request_stop();

 auto point_read = lfdbx::get(
   lfdb::raw, lfdb::make_transaction(dbh), test_key("execution/stopped")) |
   stdexec::write_env(stdexec::prop {
     stdexec::get_stop_token, stop.get_token()
   });

 CHECK_FALSE(stdexec::sync_wait(std::move(point_read)).has_value());
}

TEST_CASE("FDB execution point reads report invalid transactions",
          "[fdb][execution]")
{
 CHECK_THROWS_AS(
   stdexec::sync_wait(
     lfdbx::get(lfdb::raw, {}, test_key("execution/invalid"))),
   lfdb::libfdb_exception);
}

TEST_CASE("FDB execution point reads preserve FoundationDB errors",
          "[fdb][execution]")
{
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 fdb_transaction_cancel(txn->raw_handle());

 CHECK_THROWS_AS(
   stdexec::sync_wait(lfdbx::get(
     lfdb::raw, std::move(txn), test_key("execution/error"))),
   lfdb::libfdb_exception);
}

TEST_CASE("FDB execution watches complete after committed changes",
          "[fdb][execution][asio]")
{
 janitor dbh;
 const auto first_key = test_key("execution/watch/first");
 const auto second_key = test_key("execution/watch/second");
 auto first = lfdb::make_watch(dbh, first_key);
 auto second = lfdb::make_watch(dbh, second_key);
 boost::asio::io_context context;
 std::exception_ptr error;
 bool changed = false;

 lfdbx::async_wait(
   stdexec::when_all(
     lfdbx::when_changed(std::move(first)),
     lfdbx::when_changed(std::move(second))),
   boost::asio::bind_executor(
     context.get_executor(),
     [&](std::exception_ptr completion_error, auto result) {
      error = std::move(completion_error);
      changed = result.has_value();
     }));

 lfdb::set(dbh, first_key, "changed");
 lfdb::set(dbh, second_key, "changed");

 REQUIRE(context.run());
 CHECK_FALSE(error);
 CHECK(changed);
}

TEST_CASE("FDB execution watches handle ready and moved-from handles",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/watch/ready");
 auto watch = lfdb::make_watch(dbh, key);
 auto changed = lfdbx::when_changed(std::move(watch));

 CHECK_FALSE(watch.ready());
 CHECK_NOTHROW(watch.cancel());

 auto invalid = lfdbx::when_changed(std::move(watch));

 CHECK_THROWS_AS(stdexec::sync_wait(std::move(invalid)),
                 lfdb::libfdb_exception);

 lfdb::set(dbh, key, "changed before start");

 CHECK(stdexec::sync_wait(std::move(changed)).has_value());
}

TEST_CASE("FDB execution watches honor cancellation before start",
          "[fdb][execution]")
{
 janitor dbh;

 SECTION("receiver stop") {
  stdexec::inplace_stop_source stop;
  stop.request_stop();

  auto changed = lfdbx::when_changed(
    lfdb::make_watch(dbh, test_key("execution/watch/pre-stopped"))) |
    stdexec::write_env(stdexec::prop {
      stdexec::get_stop_token, stop.get_token()
    });

  CHECK_FALSE(stdexec::sync_wait(std::move(changed)).has_value());
 }

 SECTION("cancelled watch") {
  auto watch = lfdb::make_watch(dbh, test_key("execution/watch/pre-cancelled"));
  watch.cancel();

  CHECK_THROWS_AS(
    stdexec::sync_wait(lfdbx::when_changed(std::move(watch))),
    lfdb::libfdb_exception);
 }
}

TEST_CASE("FDB execution watches honor active cancellation",
          "[fdb][execution][asio]")
{
 janitor dbh;

 check_active_cancellation(lfdbx::when_changed(
   lfdb::make_watch(dbh, test_key("execution/watch/cancel-active"))));
}

TEST_CASE("FDB execution watches preserve FoundationDB errors",
          "[fdb][execution]")
{
 janitor dbh;
 lfdb::transaction_options options {
  {FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag}
 };
 auto txn = lfdb::make_transaction(dbh, options);

 CHECK_THROWS_AS(
   stdexec::sync_wait(lfdbx::when_changed(
     lfdb::make_watch(txn, test_key("execution/watch/error")))),
   lfdb::libfdb_exception);
}

TEST_CASE("FDB execution watches cross the Asio yield bridge",
          "[fdb][execution][asio]")
{
 janitor dbh;
 const auto key = test_key("execution/watch/asio-yield");
 auto changed = lfdbx::when_changed(lfdb::make_watch(dbh, key));
 boost::asio::io_context context;
 bool observed = false;

 boost::asio::spawn(
   context,
   [changed = std::move(changed), &observed](
     boost::asio::yield_context yield) mutable {
    lfdbx::wait(std::move(changed), optional_yield {yield});
    observed = true;
   }, boost::asio::detached);

 lfdb::set(dbh, key, "changed");

 REQUIRE(context.run());
 CHECK(observed);
}

TEST_CASE("FDB execution commits are lazy and own their transaction",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/commit-owned");
 auto txn = lfdb::make_transaction(dbh);
 lfdb::set(txn, key, "committed by sender");

 auto commit = lfdbx::commit(txn);

 CHECK_FALSE(lfdb::key_exists(dbh, key));

 txn.reset();
 auto completed = stdexec::sync_wait(std::move(commit));

 REQUIRE(completed.has_value());

 std::string value;

 REQUIRE(lfdb::get(dbh, key, value));
 CHECK("committed by sender" == value);
}

TEST_CASE("FDB execution commits honor stop before start",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/commit-stopped");
 auto txn = lfdb::make_transaction(dbh);
 lfdb::set(txn, key, "value");

 stdexec::inplace_stop_source stop;
 stop.request_stop();

 auto commit = lfdbx::commit(txn) |
               stdexec::write_env(stdexec::prop {
                 stdexec::get_stop_token, stop.get_token()
               });

 CHECK_FALSE(stdexec::sync_wait(std::move(commit)).has_value());
 CHECK_FALSE(lfdb::key_exists(dbh, key));

 REQUIRE_NOTHROW(lfdb::commit(txn));
 CHECK(lfdb::key_exists(dbh, key));
}

TEST_CASE("FDB execution commits support read-only transactions",
          "[fdb][execution]")
{
 janitor dbh;
 auto completed = stdexec::sync_wait(
   lfdbx::commit(lfdb::make_transaction(dbh)));

 REQUIRE(completed.has_value());
}

TEST_CASE("FDB execution commits leave replay preparation explicit after conflicts",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/commit-conflict");
 lfdb::set(dbh, key, "initial");

 auto txn = lfdb::make_transaction(dbh);
 std::string value;

 REQUIRE(lfdb::get(txn, key, value));
 REQUIRE("initial" == value);

 lfdb::set(dbh, key, "conflict");
 lfdb::set(txn, key, "first attempt");

 auto completed = stdexec::sync_wait(lfdbx::commit(lfdb::with_result, txn));

 REQUIRE(completed.has_value());

 const auto& result = std::get<0>(*completed);

 REQUIRE_FALSE(result);
 CHECK(lfdb::retryable_not_committed(result.error()));

 lfdb::reset_for_replay(txn, result.error());
 REQUIRE(lfdb::get(txn, key, value));
 CHECK("conflict" == value);

 lfdb::set(txn, key, "second attempt");

 completed = stdexec::sync_wait(lfdbx::commit(lfdb::with_result, txn));

 REQUIRE(completed.has_value());
 CHECK(std::get<0>(*completed).has_value());

 REQUIRE(lfdb::get(dbh, key, value));
 CHECK("second attempt" == value);
}

TEST_CASE("FDB execution commits publish version stamps",
          "[fdb][execution]")
{
 janitor dbh;

 SECTION("version-stamped value") {
  const auto key = test_key("execution/commit-versionstamp-value");
  auto txn = lfdb::make_transaction(dbh);
  lfdb::versionstamp stamp;

  lfdb::set(txn, key, lfdb::versioned("", stamp));

  auto completed = stdexec::sync_wait(lfdbx::commit(txn));

  REQUIRE(completed.has_value());
  REQUIRE(stamp.has_value());
  CHECK(10 == std::size(stamp.resolved_bytes()));

  lfdb::versionstamp stored;

  REQUIRE(lfdb::get(dbh, key, stored));
  CHECK(stamp.resolved_bytes() == stored.resolved_bytes());
 }

 SECTION("version-stamped key") {
  const auto prefix = test_key("execution/commit-versionstamp-key/");
  auto txn = lfdb::make_transaction(dbh);
  lfdb::versionstamp stamp;

  lfdb::set(txn, lfdb::versioned(prefix, "/entry", stamp), "value"s);

  auto completed = stdexec::sync_wait(lfdbx::commit(txn));

  REQUIRE(completed.has_value());
  REQUIRE(stamp.has_value());

  std::map<std::string, std::string> values;
  lfdb::get(dbh, lfdb::select {prefix},
            std::inserter(values, std::end(values)));

  REQUIRE(1 == std::size(values));
  CHECK("value" == std::begin(values)->second);
 }
}

TEST_CASE("FDB execution commits report invalid transactions",
          "[fdb][execution]")
{
 CHECK_THROWS_AS(
   stdexec::sync_wait(lfdbx::commit({})),
   lfdb::libfdb_exception);
}

TEST_CASE("FDB execution commits preserve non-retryable errors",
          "[fdb][execution]")
{
 // FoundationDB's public error table assigns 1025 to transaction_cancelled:
 constexpr fdb_error_t transaction_cancelled = 1025;

 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 fdb_transaction_cancel(txn->raw_handle());

 try {
  std::ignore = stdexec::sync_wait(lfdbx::commit(txn));
  FAIL("cancelled transaction commit unexpectedly succeeded");
 } catch (const lfdb::libfdb_exception& e) {
  CHECK(std::error_code(transaction_cancelled, lfdb::fdb_error_category()) == e.code());
  CHECK_FALSE(e.retryable());
 }

 CHECK(static_cast<bool>(*txn));
}

TEST_CASE("FDB execution terminal bodies complete stamps before transaction destruction", "[fdb][stamps]")
{
 janitor dbh;
 lfdb::versionstamp stamp;
 lfdb::transaction_handle retained;
 auto work = lfdbx::transact(dbh, [&](auto& txn) {
  retained = txn;
  lfdb::set(txn, test_key("terminal-body-stamp"), lfdb::versioned("", stamp));
  fdb_transaction_cancel(txn->raw_handle());
  return lfdbx::get(lfdb::raw, txn, "cancelled");
 });

 CHECK_THROWS_AS(stdexec::sync_wait(std::move(work)), lfdb::libfdb_exception);
 REQUIRE(retained);
 REQUIRE(stamp.is_ready());
 CHECK(lfdb::fdb_error_code::transaction_cancelled ==
       stamp.resolved_bytes(lfdb::with_result).error());
}

TEST_CASE("FDB execution tagged commits return terminal native errors as values",
          "[fdb][execution]")
{
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 lfdb::versionstamp stamp;
 lfdb::set(txn, test_key("execution/terminal-stamp"), lfdb::versioned("", stamp));
 fdb_transaction_cancel(txn->raw_handle());

 const auto completed = stdexec::sync_wait(lfdbx::commit(lfdb::with_result, txn));
 REQUIRE(completed.has_value());
 const auto& result = std::get<0>(*completed);
 REQUIRE_FALSE(result);
 CHECK(lfdb::fdb_error_code::transaction_cancelled == result.error());
 REQUIRE(stamp.is_ready());
 CHECK(result.error() == stamp.resolved_bytes(lfdb::with_result).error());
}

TEST_CASE("FDB execution commits ignore cancellation after submission",
          "[fdb][execution][asio]")
{
 janitor dbh;
 const auto key = test_key("execution/commit-late-stop");
 auto txn = lfdb::make_transaction(dbh);
 lfdb::set(txn, key, "value");

 boost::asio::io_context context;
 boost::asio::cancellation_signal cancel;
 std::exception_ptr error;
 std::optional<std::tuple<>> result;

 lfdbx::async_wait(
   lfdbx::commit(txn),
   boost::asio::bind_cancellation_slot(
     cancel.slot(),
     boost::asio::bind_executor(
       context.get_executor(),
       [&](std::exception_ptr completion_error,
           std::optional<std::tuple<>> completion_result) {
        error = std::move(completion_error);
        result = std::move(completion_result);
       })));

 // async_wait() starts the sender before it returns. A later stop request must
 // not erase the commit disposition:
 cancel.emit(boost::asio::cancellation_type::all);

 REQUIRE(context.run());
 CHECK_FALSE(error);
 REQUIRE(result);
 CHECK(lfdb::key_exists(dbh, key));
}

TEST_CASE("FDB execution scan windows own zero-copy rows", "[fdb][execution]")
{
 janitor dbh;
 const auto first_key = test_key("execution/scan-owned/0");
 const auto second_key = test_key("execution/scan-owned/1");
 const auto end_key = test_key("execution/scan-owned/2");
 write_bytes(dbh, first_key, "first");
 write_bytes(dbh, second_key, "second");

 auto txn = lfdb::make_transaction(dbh);
 auto cursor = lfdbx::scan_cursor {lfdb::select {first_key, end_key}};
 auto completed = stdexec::sync_wait(
   lfdbx::read_window(lfdb::raw, txn, std::move(cursor)));

 REQUIRE(completed.has_value());

 auto window = std::move(std::get<0>(*completed));
 txn.reset();
 const auto rows = window.rows();

 REQUIRE(2 == std::ranges::distance(rows));

 auto row = std::begin(rows);
 auto [first_key_bytes, first_value_bytes] = *row;

 CHECK(first_key == as_string_view(first_key_bytes));
 CHECK("first"sv == as_string_view(first_value_bytes));

 auto [second_key_bytes, second_value_bytes] = *++row;

 CHECK(second_key == as_string_view(second_key_bytes));
 CHECK("second"sv == as_string_view(second_value_bytes));

 CHECK_FALSE(std::move(window).next());
}

TEST_CASE("FDB execution scan windows transfer ownership on assignment",
          "[fdb][execution]")
{
 janitor dbh;
 const auto first_key = test_key("execution/move-window/first");
 const auto second_key = test_key("execution/move-window/second");
 const auto end_key = test_key("execution/move-window/third");
 write_bytes(dbh, first_key, "first");
 write_bytes(dbh, second_key, "second");

 auto first_completed = stdexec::sync_wait(lfdbx::read_window(
   lfdb::raw, lfdb::make_transaction(dbh),
   lfdbx::scan_cursor {lfdb::select {first_key, second_key}}));
 auto second_completed = stdexec::sync_wait(lfdbx::read_window(
   lfdb::raw, lfdb::make_transaction(dbh),
   lfdbx::scan_cursor {lfdb::select {second_key, end_key}}));

 REQUIRE(first_completed.has_value());
 REQUIRE(second_completed.has_value());

 auto first = std::move(std::get<0>(*first_completed));
 auto second = std::move(std::get<0>(*second_completed));
 first = std::move(second);

 const auto rows = first.rows();

 REQUIRE(1 == std::ranges::distance(rows));

 const auto row = *std::begin(rows);

 CHECK(second_key == as_string_view(row.key));
 CHECK("second"sv == as_string_view(row.value));
 CHECK_FALSE(std::move(second).next());
}

TEST_CASE("FDB execution scan cursors transfer progress on assignment",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/move-cursor/0");
 const auto end_key = test_key("execution/move-cursor/1");
 write_bytes(dbh, key, "value");

 auto source = lfdbx::scan_cursor {lfdb::select {key, end_key}};
 auto destination = lfdbx::scan_cursor {lfdb::query::empty()};
 destination = std::move(source);

 CHECK_FALSE(source);
 REQUIRE(destination);

 const std::array expected {key};

 CHECK(std::ranges::equal(
   collect_scan_keys(lfdb::make_transaction(dbh), std::move(destination)),
   expected));
}

TEST_CASE("FDB execution scan cursors traverse pages and disjoint intervals",
          "[fdb][execution]")
{
 janitor dbh;
 constexpr auto key_count = 10;
 constexpr auto prefix = "execution-scan";

 for (const auto i : std::views::iota(0, key_count)) {
  write_bytes(dbh, make_key(i, prefix), std::to_string(i));
 }

 auto make_selection = [=] {
  return lfdb::query::difference(
    lfdb::query::between(make_key(0, prefix), make_key(key_count, prefix)),
    lfdb::query::between(make_key(3, prefix), make_key(7, prefix)));
 };
 const std::array expected_forward {
  make_key(0, prefix), make_key(1, prefix), make_key(2, prefix),
  make_key(7, prefix), make_key(8, prefix), make_key(9, prefix)
 };
 const std::array expected_reverse {
  make_key(9, prefix), make_key(8, prefix), make_key(7, prefix),
  make_key(2, prefix), make_key(1, prefix), make_key(0, prefix)
 };

 const auto forward = lfdb::query::with_options(
   make_selection(), lfdb::query::query_options {.result_limit = 2});
 const auto reverse = lfdb::query::with_options(
   make_selection(), lfdb::query::query_options {
    .result_limit = 2,
    .reverse_order = true
   });

 CHECK(std::ranges::equal(
   collect_scan_keys(lfdb::make_transaction(dbh),
                     lfdbx::scan_cursor {forward}),
   expected_forward));
 CHECK(std::ranges::equal(
   collect_scan_keys(lfdb::make_transaction(dbh),
                     lfdbx::scan_cursor {reverse}),
   expected_reverse));
}

TEST_CASE("FDB execution scan cursors reject exhausted scans",
          "[fdb][execution]")
{
 janitor dbh;
 auto cursor = lfdbx::scan_cursor {lfdb::query::empty()};

 REQUIRE_FALSE(cursor);
 CHECK_THROWS_AS(
   stdexec::sync_wait(lfdbx::read_window(
     lfdb::raw, lfdb::make_transaction(dbh), std::move(cursor))),
   lfdb::libfdb_exception);
}

TEST_CASE("FDB execution scan reads reject invalid transactions",
          "[fdb][execution]")
{
 CHECK_THROWS_AS(
   stdexec::sync_wait(lfdbx::read_window(
     lfdb::raw, {}, lfdbx::scan_cursor {lfdb::select {"a", "b"}})),
   lfdb::libfdb_exception);
}

TEST_CASE("FDB execution scan windows represent empty results",
          "[fdb][execution]")
{
 janitor dbh;
 auto completed = stdexec::sync_wait(lfdbx::read_window(
   lfdb::raw, lfdb::make_transaction(dbh),
   lfdbx::scan_cursor {
    lfdb::query::prefix(test_key("execution/empty-window/"))
   }));

 REQUIRE(completed.has_value());

 auto window = std::move(std::get<0>(*completed));

 CHECK(std::ranges::empty(window.rows()));
 CHECK_FALSE(std::move(window).next());
}

TEST_CASE("FDB execution scan reads honor stop before start",
          "[fdb][execution]")
{
 janitor dbh;
 stdexec::inplace_stop_source stop;
 stop.request_stop();

 auto window_read = lfdbx::read_window(
   lfdb::raw, lfdb::make_transaction(dbh),
   lfdbx::scan_cursor {lfdb::select {"a", "b"}}) |
   stdexec::write_env(stdexec::prop {
     stdexec::get_stop_token, stop.get_token()
   });

 CHECK_FALSE(stdexec::sync_wait(std::move(window_read)).has_value());
}

TEST_CASE("FDB execution scan reads preserve FoundationDB errors",
          "[fdb][execution]")
{
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 fdb_transaction_cancel(txn->raw_handle());

 CHECK_THROWS_AS(
   stdexec::sync_wait(lfdbx::read_window(
     lfdb::raw, std::move(txn),
     lfdbx::scan_cursor {lfdb::select {"a", "b"}})),
   lfdb::libfdb_exception);
}

TEST_CASE("FDB execution cancels active FoundationDB reads",
          "[fdb][execution]")
{
 janitor dbh;

 SECTION("point read") {
  auto txn = lfdb::make_transaction(dbh);
  delay_reads(dbh, txn);

  check_active_cancellation(lfdbx::get(
    lfdb::raw, std::move(txn), test_key("execution/cancel-point")));
 }

 SECTION("range read") {
  auto txn = lfdb::make_transaction(dbh);
  delay_reads(dbh, txn);

  check_active_cancellation(lfdbx::read_window(
    lfdb::raw, std::move(txn),
    lfdbx::scan_cursor {lfdb::select {"a", "b"}}));
 }
}

TEST_CASE("FDB execution scan windows cross the Asio bridge",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/asio-scan/0");
 const auto end_key = test_key("execution/asio-scan/1");
 write_bytes(dbh, key, "value");

 boost::asio::io_context context;
 std::optional<lfdbx::scan_window> result;

 boost::asio::spawn(
   context,
   [&](boost::asio::yield_context yield) {
    result.emplace(lfdbx::wait(
      lfdbx::read_window(
        lfdb::raw, lfdb::make_transaction(dbh),
        lfdbx::scan_cursor {lfdb::select {key, end_key}}),
      optional_yield {yield}));
   }, boost::asio::detached);

 REQUIRE(context.run());
 REQUIRE(result);

 const auto rows = result->rows();
 REQUIRE(1 == std::ranges::distance(rows));

 const auto row = *std::begin(rows);
 CHECK(key == as_string_view(row.key));
 CHECK("value"sv == as_string_view(row.value));
}

TEST_CASE("FDB execution Asio bridge preserves executor affinity",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/asio-affinity");
 const auto expected = "completion returns through Asio"s;
 write_bytes(dbh, key, expected);

 boost::asio::io_context context;
 std::optional<lfdbx::point_read_result> result;
 std::exception_ptr error;
 std::thread::id completion_thread;
 const auto context_thread = std::this_thread::get_id();

 lfdbx::async_wait(
   lfdbx::get(lfdb::raw, lfdb::make_transaction(dbh), key),
   boost::asio::bind_executor(
     context.get_executor(),
     [&](std::exception_ptr completion_error,
         std::optional<lfdbx::point_read_result> completion_result) {
      error = std::move(completion_error);
      result = std::move(completion_result);
      completion_thread = std::this_thread::get_id();
     }));

 REQUIRE(context.run());
 REQUIRE_FALSE(error);
 REQUIRE(result);
 REQUIRE(expected == as_string_view(*result->bytes()));
 CHECK(context_thread == completion_thread);
}

TEST_CASE("FDB execution Asio bridge accepts an empty tuple value",
          "[fdb][execution]")
{
 boost::asio::io_context context;
 std::exception_ptr error;
 std::optional<std::tuple<>> result;
 auto empty_tuple = stdexec::just(std::tuple {}) |
                    stdexec::then([](auto value) { return value; });

 lfdbx::async_wait(
   std::move(empty_tuple),
   boost::asio::bind_executor(
     context.get_executor(),
     [&](std::exception_ptr completion_error,
         std::optional<std::tuple<>> completion_result) {
      error = std::move(completion_error);
      result = std::move(completion_result);
     }));

 REQUIRE(context.run());
 CHECK_FALSE(error);
 CHECK(result.has_value());
}

TEST_CASE("FDB execution Asio bridge collects several values",
          "[fdb][execution]")
{
 using results_t = std::tuple<lfdbx::point_read_result,
                              lfdbx::point_read_result>;

 janitor dbh;
 const auto first_key = test_key("execution/asio-several/first");
 const auto second_key = test_key("execution/asio-several/second");
 const auto first_value = "first value"s;
 const auto second_value = "second value"s;

 write_bytes(dbh, first_key, first_value);
 write_bytes(dbh, second_key, second_value);

 auto reads = [&] {
  auto txn = lfdb::make_transaction(dbh);

  return stdexec::when_all(
    lfdbx::get(lfdb::raw, txn, first_key),
    lfdbx::get(lfdb::raw, txn, second_key));
 };

 auto check_results = [&](const results_t& results) {
  const auto& [first, second] = results;
  const auto first_bytes = first.bytes();
  const auto second_bytes = second.bytes();

  REQUIRE(first_bytes);
  REQUIRE(second_bytes);
  CHECK(first_value == as_string_view(*first_bytes));
  CHECK(second_value == as_string_view(*second_bytes));
 };

 SECTION("completion token") {
  boost::asio::io_context context;
  std::exception_ptr error;
  std::optional<results_t> results;

  lfdbx::async_wait(
    reads(),
    boost::asio::bind_executor(
      context.get_executor(),
      [&](std::exception_ptr completion_error,
          std::optional<results_t> completion_results) {
       error = std::move(completion_error);
       results = std::move(completion_results);
      }));

  REQUIRE(context.run());
  REQUIRE_FALSE(error);
  REQUIRE(results.has_value());
  check_results(*results);
 }

 SECTION("yield context") {
  boost::asio::io_context context;
  std::optional<results_t> results;

  boost::asio::spawn(
    context,
    [&](boost::asio::yield_context yield) {
     results.emplace(lfdbx::wait(reads(), optional_yield {yield}));
    }, boost::asio::detached);

  REQUIRE(context.run());
  REQUIRE(results.has_value());
  check_results(*results);
 }

 SECTION("blocking wait") {
  const auto results = lfdbx::wait(reads(), null_yield);

  check_results(results);
 }
}

TEST_CASE("FDB execution composes with application senders", "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/application-composition");
 const auto expected = "database value"s;
 write_bytes(dbh, key, expected);

 auto txn = lfdb::make_transaction(dbh);
 auto [result, application_value] = lfdbx::wait(stdexec::when_all(
   lfdbx::get(lfdb::raw, txn, key), stdexec::just(42)), null_yield);
 const auto bytes = result.bytes();

 REQUIRE(bytes);
 CHECK(expected == as_string_view(*bytes));
 CHECK(42 == application_value);
}

TEST_CASE("FDB execution Asio bridge suspends yield contexts",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/asio-yield");
 const auto expected = "a suspended coroutine"s;
 write_bytes(dbh, key, expected);

 boost::asio::io_context context;
 std::optional<lfdbx::point_read_result> result;

 boost::asio::spawn(
   context,
   [&](boost::asio::yield_context yield) {
    result.emplace(lfdbx::wait(
      lfdbx::get(lfdb::raw, lfdb::make_transaction(dbh), key),
      optional_yield {yield}));
   }, boost::asio::detached);

 REQUIRE(context.run());
 REQUIRE(result);
 CHECK(expected == as_string_view(*result->bytes()));
}

TEST_CASE("FDB execution wait blocks without a yield context",
          "[fdb][execution]")
{
 janitor dbh;
 const auto key = test_key("execution/blocking-wait");
 const auto expected = "a blocking boundary"s;
 write_bytes(dbh, key, expected);

 auto result = lfdbx::wait(
   lfdbx::get(lfdb::raw, lfdb::make_transaction(dbh), key), null_yield);

 REQUIRE(result.bytes());
 CHECK(expected == as_string_view(*result.bytes()));

 CHECK_THROWS_AS(
   lfdbx::wait(
     lfdbx::get(lfdb::raw, {}, test_key("execution/blocking-error")),
     null_yield),
   lfdb::libfdb_exception);
}

TEST_CASE("FDB execution Asio bridge preserves library exception types",
          "[fdb][execution]")
{
 boost::asio::io_context context;
 bool caught = false;

 boost::asio::spawn(
   context,
   [&](boost::asio::yield_context yield) {
    try {
     std::ignore = lfdbx::wait(
       lfdbx::get(lfdb::raw, {}, test_key("execution/asio-error")),
       optional_yield {yield});
    } catch (const lfdb::libfdb_exception&) {
     caught = true;
    }
   }, boost::asio::detached);

 REQUIRE(context.run());
 CHECK(caught);
}

TEST_CASE("FDB execution Asio bridge preserves application invalid arguments", "[fdb][execution]")
{
 boost::asio::io_context context;
 const auto original = std::make_exception_ptr(std::invalid_argument("application validation"));
 std::exception_ptr observed;
 boost::asio::spawn(context, [&](boost::asio::yield_context yield) {
  try {
   auto failure = stdexec::just() | stdexec::then([&]() -> int { std::rethrow_exception(original); });
   std::ignore = lfdbx::wait(lfdbx::operation {std::move(failure)}, optional_yield {yield});
  } catch (...) {
   observed = std::current_exception();
  }
 }, boost::asio::detached);
 REQUIRE(context.run());
 CHECK(observed == original);
}

TEST_CASE("FDB execution Asio bridge forwards cancellation",
          "[fdb][execution]")
{
 stdexec::run_loop source_loop;
 boost::asio::io_context context;
 boost::asio::cancellation_signal cancel;
 std::exception_ptr error;
 std::optional<int> result;

 auto delayed = stdexec::schedule(source_loop.get_scheduler()) |
                stdexec::then([] { return 42; });

 lfdbx::async_wait(
   std::move(delayed),
   boost::asio::bind_cancellation_slot(
     cancel.slot(),
     boost::asio::bind_executor(
       context.get_executor(),
       [&](std::exception_ptr completion_error,
           std::optional<int> completion_result) {
        error = std::move(completion_error);
        result = std::move(completion_result);
       })));

 cancel.emit(boost::asio::cancellation_type::all);
 source_loop.finish();
 source_loop.run();

 REQUIRE(context.run());
 CHECK_FALSE(result);
 check_operation_aborted(error);
}

TEST_CASE("FDB execution Asio bridge releases abandoned completions",
          "[fdb][execution]")
{
 std::weak_ptr<int> marker;

 {
  boost::asio::io_context context;
  auto owner = std::make_shared<int>(42);
  marker = owner;

  lfdbx::async_wait(
    stdexec::just(42) | stdexec::then([](const int value) { return value; }),
    boost::asio::bind_executor(
      context.get_executor(),
      [owner](std::exception_ptr, std::optional<int>) {}));

  owner.reset();
  REQUIRE_FALSE(marker.expired());
 }

 CHECK(marker.expired());
}

int main(int argc, char **argv)
{
 const auto result = Catch::Session().run(argc, argv);

 lfdb::shutdown_libfdb();

 return result;
}
