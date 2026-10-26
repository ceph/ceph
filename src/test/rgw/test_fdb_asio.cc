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

#include "test/rgw/test_fdb_concurrency.h"
#include "rgw/ceph_fdb_execution.h"
#include "rgw/fdb/experimental/concurrency.h"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>

#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <tuple>
#include <type_traits>

using point_operation = decltype(lfdbx::get(lfdb::raw, {}, std::string_view {}));
using native_point = point_operation::native_handle_type;

static_assert(boost::asio::async_operation<native_point>);
static_assert(std::same_as<decltype(std::declval<point_operation&>().native_handle()), native_point&>);
static_assert(std::same_as<decltype(std::declval<const point_operation&>().native_handle()),
                           const native_point&>);
static_assert(std::same_as<decltype(std::declval<point_operation>().native_handle()), native_point>);
static_assert(not std::copy_constructible<lfdbx::scan_window>);
static_assert(std::is_nothrow_move_constructible_v<lfdbx::point_read_result>);

TEST_CASE("Experimental ASIO metadata supports native deferred composition",
          "[fdb][asio][metadata]")
{
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 auto native = lfdbx::read_version(txn).native_handle();
 auto work = std::move(native) | boost::asio::deferred(
   [](std::exception_ptr error, std::optional<std::int64_t> version) {
    std::optional<std::string> text;

    if (version) {
     text.emplace(std::to_string(*version));
    }

    return boost::asio::deferred.values(std::move(error), std::move(text));
   });
 const auto result = lfdbx::wait(lfdbx::operation {std::move(work)}, null_yield);
 CHECK(std::to_string(lfdb::read_version(txn)) == result);
}

TEST_CASE("Experimental ASIO native extraction preserves lazy reads and ownership", "[fdb][asio]")
{
 janitor dbh;
 const auto key = test_key("asio/native");
 auto txn = lfdb::make_transaction(dbh);
 auto operation = lfdbx::get(lfdb::raw, txn, key);
 auto native = std::move(operation).native_handle();
 write_bytes(txn, key, "visible only after construction");

 auto result = lfdbx::wait(lfdbx::operation {std::move(native)}, null_yield);
 txn.reset();
 REQUIRE(result.bytes());
 CHECK("visible only after construction" == as_string_view(*result.bytes()));
}

TEST_CASE("Experimental ASIO preserves missing reads and original errors", "[fdb][asio]")
{
 janitor dbh;
 auto result = lfdbx::wait(lfdbx::get(lfdb::raw, lfdb::make_transaction(dbh),
                                   test_key("asio/missing")), null_yield);
 CHECK_FALSE(result.bytes());
 CHECK_THROWS_AS(lfdbx::wait(lfdbx::get(lfdb::raw, {}, "invalid"), null_yield),
                 lfdb::libfdb_exception);

 auto txn = lfdb::make_transaction(dbh);
 fdb_transaction_cancel(txn->raw_handle());
 CHECK_THROWS_AS(lfdbx::wait(lfdbx::get(lfdb::raw, txn, "cancelled"), null_yield),
                 lfdb::libfdb_exception);
}

TEST_CASE("Experimental ASIO scan windows own rows and preserve cursor progress", "[fdb][asio]")
{
 janitor dbh;
 const auto first = test_key("asio/scan/0");
 const auto second = test_key("asio/scan/1");
 const auto end_key = test_key("asio/scan/2");
 auto txn = lfdb::make_transaction(dbh);
 write_bytes(txn, first, "first");
 write_bytes(txn, second, "second");
 auto window = lfdbx::wait(lfdbx::read_window(lfdb::raw, txn,
   lfdbx::scan_cursor {lfdb::select {first, end_key}}), null_yield);
 std::weak_ptr<lfdb::transaction> transaction = txn;
 txn.reset();
 REQUIRE_FALSE(transaction.expired());
 auto scan_txn = transaction.lock();
 std::vector<std::pair<std::string, std::string>> rows;
 auto cursor = lfdbx::scan_cursor {lfdb::select {first, end_key}};

 for (;;) {
  for (const auto row : window.rows()) {
   rows.emplace_back(as_string_view(row.key), as_string_view(row.value));
  }

  cursor = std::move(window).next();
  CHECK(std::ranges::empty(window.rows()));

  if (not cursor) {
   break;
  }

  window = lfdbx::wait(lfdbx::read_window(lfdb::raw, scan_txn, std::move(cursor)), null_yield);
 }

 REQUIRE(2 == rows.size());
 CHECK(std::pair {first, std::string {"first"}} == rows[0]);
 CHECK(std::pair {second, std::string {"second"}} == rows[1]);
 CHECK_THROWS_AS(lfdbx::wait(lfdbx::read_window(lfdb::raw, lfdb::make_transaction(dbh),
                                          std::move(cursor)), null_yield),
                 lfdb::libfdb_exception);
}

TEST_CASE("Experimental ASIO commit is lazy and ignores cancellation after submission", "[fdb][asio]")
{
 janitor dbh;
 const auto key = test_key("asio/commit");
 auto txn = lfdb::make_transaction(dbh);
 auto commit = lfdbx::commit(txn);
 write_bytes(txn, key, "committed");
 boost::asio::io_context context;
 boost::asio::cancellation_signal cancel;
 std::exception_ptr error;
 std::optional<std::tuple<>> result;

 lfdbx::async_wait(std::move(commit), boost::asio::bind_cancellation_slot(cancel.slot(),
   boost::asio::bind_executor(context.get_executor(),
     [&](std::exception_ptr failure, auto value) {
      error = std::move(failure);
      result = std::move(value);
     })));
 cancel.emit(boost::asio::cancellation_type::all);
 REQUIRE(context.run());
 CHECK_FALSE(error);
 REQUIRE(result);
 CHECK(lfdb::key_exists(dbh, key));
}

TEST_CASE("Experimental ASIO commits leave replay preparation explicit", "[fdb][asio]")
{
 janitor dbh;
 const auto key = test_key("asio/conflict");
 lfdb::set(dbh, key, "initial");
 auto txn = lfdb::make_transaction(dbh);
 std::string value;
 REQUIRE(lfdb::get(txn, key, value));
 lfdb::set(dbh, key, "conflict");
 lfdb::set(txn, key, "first attempt");

 const auto result = lfdbx::wait(lfdbx::commit(lfdb::with_result, txn), null_yield);
 REQUIRE_FALSE(result);
 CHECK(lfdb::retryable_not_committed(result.error()));
 lfdb::reset_for_replay(txn, result.error());
 REQUIRE(lfdb::get(txn, key, value));
 CHECK("conflict" == value);
 lfdb::set(txn, key, "second attempt");
 CHECK_NOTHROW(lfdbx::wait(lfdbx::commit(txn), null_yield));
 REQUIRE(lfdb::get(dbh, key, value));
 CHECK("second attempt" == value);
}

TEST_CASE("Experimental ASIO commits resolve version stamps", "[fdb][asio]")
{
 janitor dbh;
 const auto key = test_key("asio/versionstamp");
 auto txn = lfdb::make_transaction(dbh);
 lfdb::versionstamp stamp;
 lfdb::set(txn, key, lfdb::versioned("", stamp));
 REQUIRE_NOTHROW(lfdbx::wait(lfdbx::commit(txn), null_yield));
 REQUIRE(stamp.has_value());
 CHECK(10 == stamp.resolved_bytes().size());
}

TEST_CASE("Experimental ASIO terminal bodies complete stamps before transaction destruction", "[fdb][stamps]")
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

 CHECK_THROWS_AS(lfdbx::wait(std::move(work), null_yield), lfdb::libfdb_exception);
 REQUIRE(retained);
 REQUIRE(stamp.is_ready());
 CHECK(lfdb::fdb_error_code::transaction_cancelled ==
       stamp.resolved_bytes(lfdb::with_result).error());
}

TEST_CASE("Experimental ASIO tagged commits return terminal errors as values", "[fdb][asio]")
{
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 lfdb::versionstamp stamp;
 lfdb::set(txn, test_key("asio/terminal-stamp"), lfdb::versioned("", stamp));
 fdb_transaction_cancel(txn->raw_handle());

 const auto result = lfdbx::wait(lfdbx::commit(lfdb::with_result, txn), null_yield);
 REQUIRE_FALSE(result);
 CHECK(lfdb::fdb_error_code::transaction_cancelled == result.error());
 REQUIRE(stamp.is_ready());
 CHECK(result.error() == stamp.resolved_bytes(lfdb::with_result).error());

 auto ordinary = lfdb::make_transaction(dbh);
 fdb_transaction_cancel(ordinary->raw_handle());
 CHECK_THROWS_AS(lfdbx::wait(lfdbx::commit(ordinary), null_yield),
                  lfdb::libfdb_exception);
}

TEST_CASE("Experimental ASIO cancels outstanding reads scans and watches", "[fdb][asio]")
{
 janitor dbh;

 SECTION("point read") {
  auto txn = lfdb::make_transaction(dbh);
  delay_reads(dbh.dbh(), txn);
  check_active_cancellation(lfdbx::get(lfdb::raw, txn, test_key("asio/cancel-point")));
 }

 SECTION("scan") {
  auto txn = lfdb::make_transaction(dbh);
  delay_reads(dbh.dbh(), txn);
  check_active_cancellation(lfdbx::read_window(lfdb::raw, txn,
    lfdbx::scan_cursor {lfdb::select {test_key("asio/cancel-scan")}}));
 }

 SECTION("watch") {
  check_active_cancellation(lfdbx::when_changed(lfdb::make_watch(dbh, test_key("asio/cancel-watch"))));
 }
}

TEST_CASE("Experimental ASIO watch delivery uses the associated executor", "[fdb][asio]")
{
 janitor dbh;
 const auto key = test_key("asio/watch");
 boost::asio::io_context context;
 bool completed = false;

 lfdbx::async_wait(lfdbx::when_changed(lfdb::make_watch(dbh, key)),
   boost::asio::bind_executor(context.get_executor(), [&](std::exception_ptr error, auto result) {
    CHECK(context.get_executor().running_in_this_thread());
    CHECK_FALSE(error);
    CHECK(result);
    completed = true;
   }));
 lfdb::set(dbh, key, "changed");
 CHECK_FALSE(completed);
 REQUIRE(context.run());
 CHECK(completed);
}

TEST_CASE("Experimental ASIO transactions accept native bodies and move-only arguments", "[fdb][asio]")
{
 janitor dbh;
 auto work = lfdbx::transact(dbh,
   [increment = std::make_unique<int>(2)](lfdb::transaction_handle&, auto& input) {
    return native_value(std::tuple {*input + *increment, std::make_unique<int>(9)});
   }, std::make_unique<int>(40));
 auto [value, owned] = lfdbx::wait(std::move(work), null_yield);
 CHECK(42 == value);
 REQUIRE(owned);
 CHECK(9 == *owned);
}

TEST_CASE("Experimental ASIO transactions preserve application errors without publication", "[fdb][asio]")
{
 janitor dbh;
 const auto key = test_key("asio/transact/error");
 int published = 5;
 auto work = lfdbx::transact(dbh,
   [&](lfdb::transaction_handle& txn, auto& output) -> point_operation {
    output += 10;
    write_bytes(txn, key, "uncommitted");
    throw std::domain_error("application failure");
   }, lfdb::staged(published));
 CHECK_THROWS_AS(lfdbx::wait(std::move(work), null_yield), std::domain_error);
 CHECK(5 == published);
 CHECK_FALSE(lfdb::key_exists(dbh, key));
}

TEST_CASE("Experimental ASIO cancellation before transaction body skips invocation", "[fdb][asio]")
{
 janitor dbh;
 boost::asio::io_context context;
 boost::asio::cancellation_signal cancel;
 int invocations = 0;
 std::exception_ptr error;
 auto work = lfdbx::transact(dbh, [&](lfdb::transaction_handle&) {
  ++invocations;
  return native_value(42);
 });
 lfdbx::async_wait(std::move(work), boost::asio::bind_cancellation_slot(cancel.slot(),
   boost::asio::bind_executor(context.get_executor(),
     [&](std::exception_ptr failure, auto result) {
      error = std::move(failure);
      CHECK_FALSE(result);
     })));
 cancel.emit(boost::asio::cancellation_type::partial);
 REQUIRE(context.run());
 CHECK(0 == invocations);
 check_operation_aborted(error);
}

TEST_CASE("Experimental ASIO transactions retain uncertain errors when cancelled", "[fdb][asio]")
{
 janitor dbh;
 boost::asio::io_context context;
 boost::asio::cancellation_signal cancel;
 std::exception_ptr error;
 auto work = lfdbx::transact(dbh, [&](lfdb::transaction_handle&) -> point_operation {
  cancel.emit(boost::asio::cancellation_type::partial);
  throw lfdb::libfdb_exception {lfdb::fdb_error_code::commit_unknown_result};
 });
 lfdbx::async_wait(std::move(work), boost::asio::bind_cancellation_slot(cancel.slot(),
   boost::asio::bind_executor(context.get_executor(),
     [&](std::exception_ptr failure, auto result) {
      error = std::move(failure);
      CHECK_FALSE(result);
     })));
 REQUIRE(context.run());
 REQUIRE(error);

 try {
  std::rethrow_exception(error);
 } catch (const lfdb::libfdb_exception& failure) {
  CHECK(std::error_code(1021, lfdb::fdb_error_category()) == failure.code());
 }
}

TEST_CASE("Experimental ASIO operations cross the yield boundary", "[fdb][asio]")
{
 janitor dbh;
 boost::asio::io_context context;
 bool completed = false;
 boost::asio::spawn(context, [&](boost::asio::yield_context yield) {
  CHECK(42 == lfdbx::wait(lfdbx::transact(dbh, [](lfdb::transaction_handle&) {
    return native_value(42);
   }), optional_yield {yield}));
  completed = true;
 }, boost::asio::detached);
 REQUIRE(context.run());
 CHECK(completed);
}

TEST_CASE("Experimental ASIO native deferred composition preserves owning results", "[fdb][asio]")
{
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);
 const auto key = test_key("asio/native-composition");
 write_bytes(txn, key, "bytes");
 auto native = lfdbx::get(lfdb::raw, txn, key).native_handle();
 auto count = std::move(native) | boost::asio::deferred(
   [](std::exception_ptr error, std::optional<lfdbx::point_read_result> result) {
    std::optional<std::size_t> size;

    if (result) {
     const auto bytes = result->bytes();
     size.emplace(bytes ? bytes->size() : 0);
    }

    return boost::asio::deferred.values(std::move(error), std::move(size));
   });
 CHECK(5 == lfdbx::wait(lfdbx::operation {std::move(count)}, null_yield));
}

TEST_CASE("Experimental ASIO transactions cancel an active body without publication", "[fdb][asio]")
{
 janitor dbh;
 boost::asio::io_context context;
 boost::asio::cancellation_signal cancel;
 std::exception_ptr error;
 const auto key = test_key("asio/cancel-body");
 int published = 5;
 bool invoked = false;
 auto work = lfdbx::transact(dbh, [&](lfdb::transaction_handle& txn, auto& output) {
  invoked = true;
  output += 10;
  delay_reads(dbh.dbh(), txn);
  write_bytes(txn, key, "uncommitted");
  return lfdbx::get(lfdb::raw, txn, test_key("asio/delayed"));
 }, lfdb::staged(published));
 lfdbx::async_wait(std::move(work), boost::asio::bind_cancellation_slot(cancel.slot(),
   boost::asio::bind_executor(context.get_executor(), [&](std::exception_ptr failure, auto result) {
    error = std::move(failure);
    CHECK_FALSE(result);
   })));
 REQUIRE(1 == context.run_one());
 REQUIRE(invoked);
 cancel.emit(boost::asio::cancellation_type::partial);
 REQUIRE(context.run());
 check_operation_aborted(error);
 CHECK(5 == published);
 CHECK_FALSE(lfdb::key_exists(dbh, key));
}

TEST_CASE("Experimental ASIO transactions publish after a submitted commit despite cancellation",
          "[fdb][asio]")
{
 janitor dbh;
 boost::asio::io_context context;
 boost::asio::cancellation_signal cancel;
 std::exception_ptr error;
 bool completed = false;
 int published = 5;
 const auto key = test_key("asio/late-transaction-stop");
 auto work = lfdbx::transact(dbh, [&](lfdb::transaction_handle& txn, auto& output) {
  output += 10;
  write_bytes(txn, key, "committed");
  return lfdbx::get(lfdb::raw, txn, key);
 }, lfdb::staged(published));
 lfdbx::async_wait(std::move(work), boost::asio::bind_cancellation_slot(cancel.slot(),
   boost::asio::bind_executor(context.get_executor(), [&](std::exception_ptr failure, auto result) {
    error = std::move(failure);
    CHECK(result);
    completed = true;
   })));
 REQUIRE(1 == context.run_one()); // Starts the body.
 REQUIRE(1 == context.run_one()); // Delivers its read and submits commit.
 cancel.emit(boost::asio::cancellation_type::partial);
 REQUIRE(context.run());
 CHECK(completed);
 CHECK_FALSE(error);
 CHECK(15 == published);
 CHECK(lfdb::key_exists(dbh, key));
}

TEST_CASE("Experimental ASIO transactions preserve the exhausted replay error", "[fdb][asio]")
{
 const auto retry_limit = GENERATE(0, 2, 12);
 const auto error = GENERATE(1020, 1021);
 janitor dbh;
 int attempts = 0;
 const lfdb::transaction_options options {
  {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {retry_limit}},
  {FDB_TR_OPTION_MAX_RETRY_DELAY, std::int64_t {1}}
 };
 auto work = lfdbx::transact(dbh, options, [&](lfdb::transaction_handle&) -> point_operation {
  ++attempts;
  throw lfdb::libfdb_exception {std::error_code(error, lfdb::fdb_error_category())};
 });

 try {
  std::ignore = lfdbx::wait(std::move(work), null_yield);
  FAIL("exhausted transaction unexpectedly succeeded");
 } catch (const lfdb::libfdb_exception& failure) {
  CHECK(std::error_code(error, lfdb::fdb_error_category()) == failure.code());
 }

 CHECK(1 + retry_limit == attempts);
}

TEST_CASE("Experimental ASIO releases abandoned queued completions", "[fdb][asio]")
{
 std::weak_ptr<int> marker;

 {
  boost::asio::io_context context;
  auto owned = std::make_shared<int>(42);
  marker = owned;
  lfdbx::async_wait(lfdbx::get(lfdb::raw, {}, "invalid"),
    boost::asio::bind_executor(context.get_executor(),
      [owned](std::exception_ptr, auto) {}));
  owned.reset();
  CHECK_FALSE(marker.expired());
 }

 CHECK(marker.expired());
}

TEST_CASE("Experimental ASIO initial transaction post honors the associated allocator",
          "[fdb][asio][allocation]")
{
 boost::asio::io_context context;
 concurrency_test::allocation_state state {.fail_at = 2};
 auto marker = std::make_shared<int>(42);
 const std::weak_ptr<int> weak = marker;
 auto work = lfdbx::transact(lfdb::database_handle {}, [](lfdb::transaction_handle& txn) {
  return lfdbx::get(lfdb::raw, txn, "unused");
 });
 auto token = boost::asio::bind_allocator(concurrency_test::failing_allocator<std::byte> {state},
   boost::asio::bind_executor(context.get_executor(),
     [marker](std::exception_ptr, auto) { FAIL("uninitiated transaction completed"); }));
 REQUIRE_THROWS_AS(lfdbx::async_wait(std::move(work), std::move(token)), std::bad_alloc);
 marker.reset();
 CHECK(weak.expired());
 CHECK(2 == state.calls);
 CHECK(0 == state.outstanding);
 CHECK(0 == context.poll());
}

TEST_CASE("Experimental ASIO releases abandoned transaction terminal posts", "[fdb][asio]")
{
 std::weak_ptr<int> weak;

 {
  boost::asio::io_context context;
  auto marker = std::make_shared<int>(42);
  weak = marker;
  auto work = lfdbx::transact(lfdb::database_handle {}, [](lfdb::transaction_handle& txn) {
   return lfdbx::get(lfdb::raw, txn, "unused");
  });
  lfdbx::async_wait(std::move(work), boost::asio::bind_executor(context.get_executor(),
    [marker](std::exception_ptr, auto) { FAIL("discarded completion ran"); }));
  marker.reset();
  REQUIRE(1 == context.run_one());
  CHECK_FALSE(weak.expired());
 }

 CHECK(weak.expired());
}

int main(int argc, char **argv)
{
 const auto result = Catch::Session().run(argc, argv);
 lfdb::shutdown_libfdb();

 return result;
}
