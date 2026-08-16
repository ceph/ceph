// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- // vim: ts=8 sw=2 smarttab ft=cpp 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025-2026 International Business Machines Corp. (IBM)
 *      
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#include <catch2/catch_config.hpp>

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>

#include <catch2/generators/catch_generators.hpp>
#include <catch2/generators/catch_generators_adapters.hpp>

#include <catch2/matchers/catch_matchers_all.hpp>

#include "test/rgw/test_fdb_common.h"

#include <fmt/format.h>
#include <fmt/chrono.h>
#include <fmt/ranges.h>

#include "include/random.h"

#include <boost/container/flat_map.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <compare>
#include <concepts>
#include <cstdint>
#include <exception>
#include <iterator>
#include <limits>
#include <list>
#include <map>
#include <ranges>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

using Catch::Matchers::AllMatch;

using fmt::format;
using fmt::println;

using std::end;
using std::begin;

using std::string;
using std::string_view;

using std::to_string;

using std::vector;

using namespace std::literals;

namespace content = ceph::libfdb::layer::content;

// Be nice to Catch2's template-test macros:
using string_pair = std::pair<std::string, std::string>;

template <typename ...Ts>
concept can_lfdb_set = requires(Ts&& ...xs) {
 lfdb::set(std::forward<Ts>(xs)...);
};

template <typename ValueT>
concept can_atomic_add =
 requires(lfdb::database_handle dbh, std::string key, ValueT value) {
  lfdb::atomic::add(dbh, key, value);
 };

template <typename ValueT>
concept can_atomic_min =
 requires(lfdb::database_handle dbh, std::string key, ValueT value) {
  lfdb::atomic::min(dbh, key, value);
 };

struct pair_identity final
{
 const string_pair& operator()(const string_pair& kv) const noexcept
 {
  return kv;
 }
};

[[nodiscard]] bool contains_key(const std::vector<string_pair>& rows,
                                const std::string& key)
{
 const auto found = std::ranges::find(rows, key, &string_pair::first);

 return std::end(rows) != found;
}

TEST_CASE("libfdb concepts describe supported API shapes", "[fdb][concepts]")
{
 using string_pair_vector = std::vector<string_pair>;
 using bad_key_vector = std::vector<std::pair<int, std::string>>;
 using transformed_pair_range = decltype(std::declval<string_pair_vector&>() |
                                         std::views::transform(pair_identity {}));

 STATIC_REQUIRE(lfdb::concepts::key_value_iterator<string_pair_vector::iterator>);
 STATIC_REQUIRE(lfdb::concepts::key_value_range<string_pair_vector>);
 STATIC_REQUIRE(lfdb::concepts::key_value_forward_range<string_pair_vector>);
 STATIC_REQUIRE(lfdb::concepts::key_value_forward_range<transformed_pair_range>);
 STATIC_REQUIRE_FALSE(lfdb::concepts::key_value_iterator<bad_key_vector::iterator>);

 STATIC_REQUIRE(lfdb::concepts::string_pair_output_range<string_pair_vector>);
 STATIC_REQUIRE(lfdb::concepts::string_pair_output_range<std::map<std::string, std::string>>);

 STATIC_REQUIRE(lfdb::concepts::decoded_value_sink<std::string&>);
 STATIC_REQUIRE(lfdb::concepts::decoded_value_sink<char(&)[9]>);
 STATIC_REQUIRE_FALSE(lfdb::concepts::decoded_value_sink<const std::string&>);
 STATIC_REQUIRE_FALSE(lfdb::concepts::decoded_value_sink<std::string>);

 STATIC_REQUIRE(can_atomic_add<std::uint64_t>);
 STATIC_REQUIRE_FALSE(can_atomic_add<bool>);
 STATIC_REQUIRE_FALSE(can_atomic_min<std::int64_t>);
}

TEST_CASE("query prefix handles byte-string keyspace edges", "[fdb][query]")
{
 namespace lq = ceph::libfdb::query;

 const auto max_keyspace_prefix = std::string(1, static_cast<char>(0xFF));
 const auto before_max_keyspace_prefix = std::string(1, static_cast<char>(0xFE));

 CHECK_FALSE(lq::byte_string_domain::successor(max_keyspace_prefix));
 CHECK(max_keyspace_prefix == lq::successor(before_max_keyspace_prefix));

 CHECK(lq::is_universal(lq::prefix("")));
 CHECK(lq::is_empty(lq::prefix(max_keyspace_prefix)));
 CHECK(lq::is_empty(lq::prefix(max_keyspace_prefix + "metadata")));
}

struct position_insert_iterator
{
 using difference_type = std::ptrdiff_t;

 std::vector<string_pair> *out;
 std::size_t pos = 0;

 position_insert_iterator& operator*() noexcept { return *this; }
 position_insert_iterator& operator++() noexcept { return *this; }
 position_insert_iterator& operator++(int) noexcept { return *this; }

 position_insert_iterator& operator=(string_pair kv) {
  out->insert(std::next(std::begin(*out), pos), std::move(kv));
  ++pos;

  return *this;
 }
};

// Collect values in selection to out_values:
auto key_counter(auto txn, const auto& selector, auto& out_values) -> auto {
 out_values.clear();

 return lfdb::get(txn, selector,
                  std::inserter(out_values, std::end(out_values)));
};

auto key_count(auto& dbh, const auto& selector) {
 std::map<std::string, std::string> _;
 return key_counter(lfdb::make_transaction(dbh), selector, _);
}

inline auto write_monotonic_kvs(lfdb::database_handle dbh, const int N, std::string_view prefix = "key")
{
 auto kvs = make_monotonic_kvs(N, prefix);

 for (const auto& [k, v] : kvs)
  lfdb::set(lfdb::make_transaction(dbh), k, v, lfdb::commit_after_op::commit);

 return kvs;
}

inline void write_raw_fdb_value(lfdb::database_handle dbh,
                                std::string_view key,
                                std::span<const std::uint8_t> value)
{
 auto txn = lfdb::make_transaction(dbh);

 fdb_transaction_set(txn->raw_handle(),
                     reinterpret_cast<const std::uint8_t *>(key.data()),
                     static_cast<int>(std::size(key)),
                     value.data(),
                     static_cast<int>(std::size(value)));

 REQUIRE(lfdb::commit(txn));
}

inline std::vector<std::uint8_t> read_raw_fdb_value(lfdb::database_handle dbh,
                                                    std::string_view key)
{
 std::vector<std::uint8_t> out;

 REQUIRE(lfdb::get(dbh, key, [&out](std::span<const std::uint8_t> value) {
  out.assign(std::begin(value), std::end(value));
 }));

 return out;
}

inline std::span<const std::uint8_t> raw_bytes(std::string_view value)
{
 return {
  reinterpret_cast<const std::uint8_t *>(value.data()),
  std::size(value)
 };
}

template <std::unsigned_integral ValueT>
constexpr ValueT decode_little_endian(std::span<const std::uint8_t> bytes)
{
 ValueT out = 0;

 for (auto byte : bytes | std::views::reverse) {
  out <<= 8;
  out |= static_cast<ValueT>(byte);
 }

 return out;
}

inline auto decode_raw_fdb_pairs(std::span<const FDBKeyValue> pairs)
{
 return pairs
      | std::views::transform(ceph::libfdb::detail::to_decoded_kv_pair<std::string>)
      | std::ranges::to<std::vector<string_pair>>();
}

inline auto first_keys(const auto& kvs, const std::size_t n)
{
 return kvs
      | std::views::take(n)
      | std::ranges::to<std::vector<string_pair>>();
}

inline auto last_keys_reversed(const auto& kvs, const std::size_t n)
{
 return kvs
      | std::views::reverse
      | std::views::take(n)
      | std::ranges::to<std::vector<string_pair>>();
}

// Basically, make sure we're actually linking with the library:
TEST_CASE()
{
 REQUIRE_THROWS_AS([] { throw ceph::libfdb::libfdb_exception(0); }(),
                   ceph::libfdb::libfdb_exception);
}

TEST_CASE("fdb simple", "[rgw][fdb]") {
 janitor j;

 const string k = test_key("key");
 const string v = fmt::format("value-{:%c}", std::chrono::system_clock::now());

 SECTION("read missing key") {
    const string missing_key = test_key("missing_key");

    SECTION("with transaction") {
        std::string out_value;

        auto txn_handle = lfdb::make_transaction(j);
        REQUIRE(nullptr != txn_handle);
  
        CAPTURE(missing_key); 
        CAPTURE(out_value); 
        REQUIRE_FALSE(lfdb::get(txn_handle, missing_key, out_value, lfdb::commit_after_op::no_commit));
        CHECK(v != out_value);
    }
 }

 SECTION("CRD single-key") {
    std::string out_value;

    // The key initially either exists, or we'll write it anew, either is fine:
    CHECK_NOTHROW(lfdb::set(lfdb::make_transaction(j), k, v, lfdb::commit_after_op::commit));

    // Make sure that it DOES exist:
    CHECK(lfdb::get(lfdb::make_transaction(j), k, out_value, lfdb::commit_after_op::no_commit));
    CHECK(v == out_value); 

    // "erase()" is known as "clear" in FDB parlance, deleting a record:
    REQUIRE_NOTHROW(lfdb::erase(lfdb::make_transaction(j), k, lfdb::commit_after_op::commit));

    // ...as this shouldn't be updated again, make sure there isn't an accidental match:
    out_value.clear();

    // ...and, POOF!-- it should be gone:
    CHECK_FALSE(lfdb::get(lfdb::make_transaction(j), k, out_value, lfdb::commit_after_op::no_commit));
    CHECK(v != out_value);
 }

 SECTION("read/write single key") {
    REQUIRE(nullptr != j.dbh());

    // First, be sure we have a valid value written to the database:
    REQUIRE_NOTHROW(lfdb::set(lfdb::make_transaction(j), k, v, lfdb::commit_after_op::commit));

    SECTION("read transaction") {
      std::string out_value;
     
      CHECK(lfdb::get(lfdb::make_transaction(j), k, out_value, lfdb::commit_after_op::no_commit));
      CHECK(v == out_value); 
    }
 }

 SECTION("check for existence of key") {
    REQUIRE(nullptr != j.dbh());

    // Erase the key if it's already there:
    lfdb::erase(lfdb::make_transaction(j), k, lfdb::commit_after_op::commit);

    // Now, we shouldn't find anything:
    CHECK_FALSE(lfdb::key_exists(lfdb::make_transaction(j), k, lfdb::commit_after_op::no_commit));

    // Write the key:
    lfdb::set(lfdb::make_transaction(j), k, v, lfdb::commit_after_op::commit);

    // ...it should magically be there!
    CHECK(lfdb::key_exists(lfdb::make_transaction(j), k, lfdb::commit_after_op::no_commit));

    // ...and now it should be gone again:
    lfdb::erase(lfdb::make_transaction(j), k, lfdb::commit_after_op::commit);
    CHECK_FALSE(lfdb::key_exists(lfdb::make_transaction(j), k, lfdb::commit_after_op::no_commit));
 }
}

// Version-stamped keys and values are deliberately mutually exclusive:
static_assert(can_lfdb_set<lfdb::database_handle,
                           lfdb::versioned_bytes,
                           std::string>);

static_assert(can_lfdb_set<lfdb::database_handle,
                           std::string_view,
                           lfdb::versioned_bytes>);

static_assert(not can_lfdb_set<lfdb::database_handle,
                               lfdb::versioned_bytes,
                               lfdb::versioned_bytes>);

TEST_CASE("version stamps", "[fdb]") {
 janitor dbh;

 constexpr auto stamp_data = [](const std::uint8_t x) {
  lfdb::versionstamp::versionstamp_data_t out {};
  out.back() = x;
  return out;
 };

 const auto resolved_stamp = [](const auto& versionstamp_data) {
  lfdb::versionstamp stamp;
  lfdb::from::convert(std::span(versionstamp_data), stamp);
  return stamp;
 };

 SECTION("accessing unresolved versionstamp throws") {
  lfdb::versionstamp stamp;

  CHECK_FALSE(stamp.is_resolved());
  CHECK_THROWS_MATCHES(stamp.resolved_bytes(),
                       std::invalid_argument,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring("attempt to access unresolved version stamp")));
 }

 SECTION("invalid versionstamp bytes are an API error") {
  lfdb::versionstamp stamp;
  const std::array<std::uint8_t, 1> invalid_stamp_bytes {};

  CHECK_THROWS_AS(lfdb::from::convert(std::span(invalid_stamp_bytes), stamp),
                  std::invalid_argument);
 }

 SECTION("commit resolves explicit version stamp") {
  const auto key = test_key("versionstamp/explicit");
  auto txn = lfdb::make_transaction(dbh);
  lfdb::versionstamp stamp;

  lfdb::set(txn, key, "value");
  REQUIRE(lfdb::commit(txn, stamp));

  CHECK(stamp.is_resolved());
  CHECK(10 == stamp.resolved_bytes().size());
 }

 SECTION("one versionstamp can resolve multiple operations in one transaction") {
  const auto first_key = test_key("versionstamp/multi/first");
  const auto second_key = test_key("versionstamp/multi/second");
  lfdb::versionstamp stamp;

  auto txn = lfdb::make_transaction(dbh);
  lfdb::set(txn, first_key, lfdb::versioned("", stamp));
  lfdb::set(txn, second_key, lfdb::versioned("", stamp));
  REQUIRE(lfdb::commit(txn));

  lfdb::versionstamp first;
  lfdb::versionstamp second;
  REQUIRE(lfdb::get(dbh, first_key, first));
  REQUIRE(lfdb::get(dbh, second_key, second));

  REQUIRE(stamp.is_resolved());
  CHECK(first.resolved_bytes() == stamp.resolved_bytes());
  CHECK(second.resolved_bytes() == stamp.resolved_bytes());
 }

 SECTION("retryable commit failure does not resolve versionstamp") {
  const auto conflict_key = test_key("versionstamp/conflict/read");
  const auto output_key = test_key("versionstamp/conflict/output");
  lfdb::set(dbh, conflict_key, "old");

  auto txn = lfdb::make_transaction(dbh);
  std::string value;
  REQUIRE(lfdb::get(txn, conflict_key, value));

  lfdb::versionstamp stamp;
  lfdb::set(txn, output_key, lfdb::versioned("", stamp));

  lfdb::set(dbh, conflict_key, "new");

  CHECK_FALSE(lfdb::commit(txn));
  CHECK_FALSE(stamp.is_resolved());
 }

 SECTION("resolved versionstamp cannot be reused for commit") {
  const auto first_key = test_key("versionstamp/reuse/first");
  const auto second_key = test_key("versionstamp/reuse/second");
  lfdb::versionstamp stamp;

  auto txn = lfdb::make_transaction(dbh);
  lfdb::set(txn, first_key, "value");
  REQUIRE(lfdb::commit(txn, stamp));
  REQUIRE(stamp.is_resolved());

  auto reuse_txn = lfdb::make_transaction(dbh);
  lfdb::set(reuse_txn, second_key, "value");

  CHECK_THROWS_MATCHES(lfdb::commit(reuse_txn, stamp),
                       std::invalid_argument,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring("attempt to reuse resolved version stamp")));
 }

 SECTION("versionstamp comparison requires resolved values") {
  lfdb::versionstamp unresolved;
  const auto resolved = resolved_stamp(stamp_data(1));

  CHECK_THROWS_MATCHES((void)(unresolved == resolved),
                       std::invalid_argument,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring("attempt to access unresolved version stamp")));
  CHECK_THROWS_MATCHES((void)(resolved == unresolved),
                       std::invalid_argument,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring("attempt to access unresolved version stamp")));
  CHECK_THROWS_MATCHES((void)(unresolved < resolved),
                       std::invalid_argument,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring("attempt to access unresolved version stamp")));
  CHECK_THROWS_MATCHES((void)(resolved < unresolved),
                       std::invalid_argument,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring("attempt to access unresolved version stamp")));
 }

 SECTION("versionstamp comparison algebra") {
  auto [lhs_version, rhs_version, expected_result] =
   GENERATE(Catch::Generators::table<std::uint8_t,
                                     std::uint8_t,
                                     std::strong_ordering>({
    { 1, 1, std::strong_ordering::equal },
    { 1, 2, std::strong_ordering::less },
    { 2, 1, std::strong_ordering::greater },
   }));

  const auto lhs = resolved_stamp(stamp_data(lhs_version));
  const auto rhs = resolved_stamp(stamp_data(rhs_version));

  CHECK((lhs <=> rhs) == expected_result);
  CHECK((lhs == rhs) == (expected_result == std::strong_ordering::equal));
  CHECK((lhs != rhs) == (expected_result != std::strong_ordering::equal));
  CHECK((lhs < rhs) == (expected_result == std::strong_ordering::less));
  CHECK((lhs <= rhs) == (expected_result != std::strong_ordering::greater));
  CHECK((lhs > rhs) == (expected_result == std::strong_ordering::greater));
  CHECK((lhs >= rhs) == (expected_result != std::strong_ordering::less));
 }

 SECTION("versionstamp key") {
  const auto prefix = test_key("versionstamp/key/");
  lfdb::erase(dbh, lfdb::select { prefix });

  lfdb::versionstamp stamp;

  lfdb::set(dbh,
            lfdb::versioned(prefix, "/entry", stamp),
            "value"s);

  REQUIRE(stamp.is_resolved());

  std::map<std::string, std::string> out;
  lfdb::get(dbh, lfdb::select { prefix }, std::inserter(out, out.end()));

  REQUIRE(1 == out.size());
  CHECK("value" == out.begin()->second);
 }

 SECTION("versionstamp value") {
  const auto key = test_key("versionstamp/value");
  lfdb::versionstamp stamp;

  lfdb::set(dbh,
            key,
            lfdb::versioned("", stamp));

  REQUIRE(stamp.is_resolved());

  lfdb::versionstamp out;
  REQUIRE(lfdb::get(dbh, key, out));

  REQUIRE(out.is_resolved());
  CHECK(stamp.resolved_bytes() == out.resolved_bytes());
 }

 SECTION("versionstamp overloads compile and commit") {
  // As these overloads share call paths, just check that they compile and briefly check output:
  {
    const auto key_prefix = test_key("versionstamp/overload/key/");
    auto txn = lfdb::make_transaction(dbh);
    lfdb::versionstamp stamp;

    lfdb::set(txn,
              lfdb::versioned(key_prefix, stamp),
              "value",
              lfdb::commit_after_op::commit);

    CHECK(stamp.is_resolved());
  }

  {
    const auto key = test_key("versionstamp/overload/value");
    auto txn = lfdb::make_transaction(dbh);
    lfdb::versionstamp stamp;

    lfdb::set(txn,
              key,
              lfdb::versioned("value:", stamp),
              lfdb::commit_after_op::commit);

    CHECK(stamp.is_resolved());
  }
 }
}

TEST_CASE("transaction versions", "[fdb]") {
 janitor dbh;

 SECTION("read version is available in an active transaction") {
  auto txn = lfdb::make_transaction(dbh);

  CHECK(0 < lfdb::read_version(txn));
 }

 SECTION("committed version is only available after commit") {
  auto txn = lfdb::make_transaction(dbh);

  CHECK_THROWS_MATCHES(lfdb::committed_version(txn),
                       std::invalid_argument,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring(
                         "committed_version() requires committed transaction")));
 }

 SECTION("committed version is available after commit") {
  const auto key = test_key("transaction-version/committed");
  auto txn = lfdb::make_transaction(dbh);

  lfdb::set(txn, key, "value");
  REQUIRE(lfdb::commit(txn));

  CHECK(0 < lfdb::committed_version(txn));
 }

 SECTION("explicit read version can read an earlier committed value") {
  const auto key = test_key("transaction-version/read-at");

  auto first_txn = lfdb::make_transaction(dbh);
  lfdb::set(first_txn, key, "first");
  REQUIRE(lfdb::commit(first_txn));
  const auto first_version = lfdb::committed_version(first_txn);

  auto second_txn = lfdb::make_transaction(dbh);
  lfdb::set(second_txn, key, "second");
  REQUIRE(lfdb::commit(second_txn));

  auto read_txn = lfdb::make_transaction(dbh);
  lfdb::set_read_version(read_txn, first_version);

  std::string value;
  REQUIRE(lfdb::get(read_txn, key, value));

  CHECK(first_version == lfdb::read_version(read_txn));
  CHECK("first" == value);
 }
}

static_assert(not std::default_initializable<lfdb::watch_handle>);
static_assert(not std::copy_constructible<lfdb::watch_handle>);
static_assert(std::move_constructible<lfdb::watch_handle>);

static constexpr std::string_view watch_key = "watch/key";

bool wait_until(std::predicate auto&& predicate,
                const std::chrono::milliseconds timeout = 5s)
{
 const auto expiration_time = std::chrono::steady_clock::now() + timeout;
 while (!std::invoke(predicate) &&
        std::chrono::steady_clock::now() < expiration_time) {
  std::this_thread::sleep_for(10ms);
 }

 return std::invoke(predicate);
}

lfdb::watch_event wait_for_watch_event(lfdb::watch_handle& watch,
                                       const std::chrono::milliseconds timeout = 5s)
{
 std::atomic_bool done = false;
 std::exception_ptr thrown;
 lfdb::watch_event result = lfdb::watch_event::cancelled;

 std::jthread wait_thread {
  [&done, &result, &thrown, &watch](std::stop_token stop_token) {
   try
   {
    result = watch.wait_for_event(stop_token);
   }
   catch (...)
   {
    thrown = std::current_exception();
   }

   done.store(true, std::memory_order_release);
  }
 };

 const auto completed = wait_until([&done] {
  return done.load(std::memory_order_acquire);
 }, timeout);

 if (not completed) {
  wait_thread.request_stop();
 }

 wait_thread.join();

 if (not completed) {
  FAIL("watch did not report an event before timeout; check local FoundationDB watch support");
 }

 if (thrown) {
  std::rethrow_exception(thrown);
 }

 return result;
}

bool trigger_watch_until(lfdb::database_handle dbh,
                         std::string_view key,
                         std::atomic_int& callbacks,
                         const int target,
                         const std::chrono::milliseconds timeout = 5s)
{
 const auto expiration_time = std::chrono::steady_clock::now() + timeout;
 auto enough_callbacks = [&] {
  return target <= callbacks.load(std::memory_order_acquire);
 };

 for (const auto n : std::views::iota(0) |
                     std::views::take_while([&](auto) {
                      return not enough_callbacks() &&
                             std::chrono::steady_clock::now() < expiration_time;
                     })) {
  lfdb::set(dbh, key, fmt::format("value-{}", n));
  wait_until(enough_callbacks, 50ms);
 }

 return enough_callbacks();
}

TEST_CASE("transaction watches", "[rgw][fdb]") {
 janitor dbh;

 SECTION("watch fires after committed key change") {
  auto txn = lfdb::make_transaction(dbh);
  auto watch = lfdb::make_watch(txn, watch_key);

  REQUIRE(lfdb::commit(txn));
  REQUIRE_FALSE(watch.ready());

  lfdb::set(dbh, watch_key, "value");

  CHECK(lfdb::watch_event::changed == wait_for_watch_event(watch));
  CHECK(watch.ready());
 }

 SECTION("watch fires after committed key erase") {
  lfdb::set(dbh, watch_key, "value");

  auto txn = lfdb::make_transaction(dbh);
  auto watch = lfdb::make_watch(txn, watch_key);

  REQUIRE(lfdb::commit(txn));
  REQUIRE_FALSE(watch.ready());

  lfdb::erase(dbh, watch_key);

  CHECK(lfdb::watch_event::changed == wait_for_watch_event(watch));
  CHECK(watch.ready());
 }

 SECTION("database watch commits its watch transaction") {
  auto watch = lfdb::make_watch(dbh, watch_key);

  REQUIRE_FALSE(watch.ready());

  lfdb::set(dbh, watch_key, "value");

  CHECK(lfdb::watch_event::changed == wait_for_watch_event(watch));
  CHECK(watch.ready());
 }

 SECTION("watch cancellation returns a wait result") {
  auto watch = lfdb::make_watch(dbh, watch_key);

  REQUIRE_FALSE(watch.ready());

  watch.cancel();

  CHECK(lfdb::watch_event::cancelled == watch.wait_for_event());
  CHECK(watch.ready());
 }

 SECTION("moved-from watch can be probed and cancelled") {
  auto watch = lfdb::make_watch(dbh, watch_key);
  auto moved_watch = std::move(watch);

  CHECK_FALSE(watch.ready());
  CHECK_NOTHROW(watch.cancel());
  CHECK_THROWS_AS(watch.wait_for_event(), std::invalid_argument);

  moved_watch.cancel();
  CHECK(lfdb::watch_event::cancelled == moved_watch.wait_for_event());
 }

 SECTION("throwing wait preserves cancellation as an exception") {
  auto watch = lfdb::make_watch(dbh, watch_key);

  REQUIRE_FALSE(watch.ready());

  watch.cancel();

  CHECK_THROWS_AS(watch.wait(), lfdb::libfdb_exception);
  CHECK(watch.ready());
 }

 SECTION("throwing stop-token wait preserves cancellation as an exception") {
  auto watch = lfdb::make_watch(dbh, watch_key);
  std::stop_source stop;

  REQUIRE_FALSE(watch.ready());

  stop.request_stop();

  CHECK_THROWS_AS(watch.wait(stop.get_token()), lfdb::libfdb_exception);
  CHECK(watch.ready());
 }

 SECTION("watch can be rearmed after each key change") {
  auto watch = lfdb::make_watch(dbh, watch_key);

  lfdb::set(dbh, watch_key, "first-value");

  REQUIRE(lfdb::watch_event::changed == wait_for_watch_event(watch));

  watch = lfdb::make_watch(dbh, watch_key);

  lfdb::set(dbh, watch_key, "second-value");

  CHECK(lfdb::watch_event::changed == wait_for_watch_event(watch));
  CHECK(watch.ready());
 }

 SECTION("read-your-writes disabled transactions reject watches") {
  lfdb::transaction_options opts{
   { FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag },
  };

  auto txn = lfdb::make_transaction(dbh, opts);
  auto watch = lfdb::make_watch(txn, watch_key);

  CHECK_THROWS_AS(watch.wait_for_event(), lfdb::libfdb_exception);
  CHECK(watch.ready());
 }

 SECTION("watch wait can be cancelled by stop token") {
  lfdb::watch_event result = lfdb::watch_event::changed;
  std::jthread wait_thread {
   [&dbh, &result](std::stop_token stop_token) {
    auto watch = lfdb::make_watch(dbh, watch_key);
    result = watch.wait_for_event(stop_token);
   }
  };

  wait_thread.request_stop();
  wait_thread.join();

  CHECK(lfdb::watch_event::cancelled == result);
 }

 SECTION("watch loop re-arms until stopped") {
  std::atomic_int callbacks = 0;
  std::atomic_bool wrong_key = false;

  std::jthread watch_thread {
   [&dbh, &callbacks, &wrong_key](std::stop_token stop_token) {
    lfdb::watched_loop(dbh, watch_key, stop_token,
     [&callbacks, &wrong_key](std::string_view key) {
      if (watch_key != key) {
       wrong_key.store(true, std::memory_order_release);
      }

      callbacks.fetch_add(1, std::memory_order_release);
     });
   }
  };

  REQUIRE(trigger_watch_until(dbh, watch_key, callbacks, 1));
  REQUIRE(trigger_watch_until(dbh, watch_key, callbacks, 2));

  watch_thread.request_stop();
  watch_thread.join();

  CHECK_FALSE(wrong_key.load(std::memory_order_acquire));
 }
}

TEST_CASE("delete keys in range", "[rgw][fdb]") {
 janitor dbh;

 // Exactly 20 keys, 0-19:
 const auto selector = lfdb::select { make_key(0), make_key(20) };

 // Make sure we're "empty":
 REQUIRE(0 == key_count(dbh, selector));

 // Make sure we have a matching number of keys in our selector range:
 const auto kvs = write_monotonic_kvs(dbh, 20);
 REQUIRE(20 == key_count(dbh, selector));

 // Erase a single value:
 lfdb::erase(dbh, make_key(5));
 CHECK(19 == key_count(dbh, selector));

 // Erase an edge of the range:
 lfdb::erase(dbh, lfdb::select { make_key(0), make_key(1) });
 CHECK(18 == key_count(dbh, selector));

 // ...the other edge: 
 lfdb::erase(dbh, lfdb::select { make_key(19), make_key(20) });
 CHECK(17 == key_count(dbh, selector));

 // Erase the entire range:
 lfdb::erase(dbh, selector);
 CHECK(0 == key_count(dbh, selector));

 const auto bounded_selector = lfdb::select { make_key(0, "bounded"), make_key(20, "bounded") };
 write_monotonic_kvs(dbh, 20, "bounded");
 lfdb::erase(dbh, lfdb::select { lfdb::exclusive(make_key(5, "bounded")), lfdb::inclusive(make_key(7, "bounded")) });

 CHECK(18 == key_count(dbh, bounded_selector));
 CHECK(lfdb::key_exists(dbh, make_key(5, "bounded")));
 CHECK_FALSE(lfdb::key_exists(dbh, make_key(6, "bounded")));
 CHECK_FALSE(lfdb::key_exists(dbh, make_key(7, "bounded")));
 CHECK(lfdb::key_exists(dbh, make_key(8, "bounded")));
}

TEST_CASE("content keys work with libfdb operations", "[rgw][fdb][content]")
{
 janitor dbh;

 const auto object =
  content::keyspace(test_namespace_prefix())
  / "content"
  / "object";
 const auto blocks = object / "blocks";
 const auto block_range = content::prefix(blocks);

 lfdb::set(dbh, object, "metadata");
 lfdb::set(dbh, blocks / "0000000000", "block-0");
 lfdb::set(dbh, blocks / "0000000001", "block-1");

 std::string metadata;
 CHECK(lfdb::get(dbh, object, metadata));
 CHECK("metadata" == metadata);
 CHECK(lfdb::key_exists(dbh, object));

 std::vector<std::pair<std::string, std::string>> block_entries;
 lfdb::get(dbh, block_range, std::back_inserter(block_entries));

 REQUIRE(2 == block_entries.size());
 CHECK("block-0" == block_entries[0].second);
 CHECK("block-1" == block_entries[1].second);

 lfdb::erase(dbh, object);

 metadata.clear();
 CHECK_FALSE(lfdb::get(dbh, object, metadata));
 CHECK_FALSE(lfdb::key_exists(dbh, object));

 lfdb::erase(dbh, block_range);

 block_entries.clear();
 lfdb::get(dbh, block_range, std::back_inserter(block_entries));
 CHECK(block_entries.empty());
}

TEMPLATE_PRODUCT_TEST_CASE("multi-key ops", "[rgw][fdb]", 
(std::vector, std::list), (string_pair)) 
{
 janitor j;

 // Write a sequence of keys so we have some data to work with:
 const auto kvs = write_monotonic_kvs(j, 100);

 SECTION("check multiple key write", "[fdb]") {
  auto txn = lfdb::make_transaction(j);

  std::string out_value;
 
  CHECK((*(kvs.find(make_key(0)))).second == make_value(0));
  CHECK(lfdb::get(txn, make_key(0), out_value, lfdb::commit_after_op::no_commit));
  CHECK(make_value(0) == out_value);

  out_value.clear();
  CHECK((*(kvs.find(make_key(99)))).second == make_value(99));
  CHECK(lfdb::get(txn, make_key(99), out_value, lfdb::commit_after_op::no_commit));
  CHECK(make_value(99) == out_value);
 }

 SECTION("check multiple key selection", "[fdb]") {
  TestType out_values;

  auto txn = lfdb::make_transaction(j);

  const auto nread =
   lfdb::get(txn,
             lfdb::select { make_key(0), make_key(100) },
             std::back_inserter(out_values),
             lfdb::commit_after_op::no_commit);

  CHECK(100 == nread);
  CHECK(100 == out_values.size());

  // Maybe not the world's most creative test, but the idea is to try getting some random keys:
  for (auto i = ceph::util::generate_random_number(out_values.size() - 1); i; --i) {
    CHECK(std::end(out_values) != std::ranges::find(out_values, string_pair { make_key(i), make_value(i) }));
  }
 }

 SECTION("check multiple key selection into container", "[fdb]") {
  TestType out_values;

  auto txn = lfdb::make_transaction(j);

  CHECK(100 == lfdb::get(txn,
                         lfdb::select { make_key(0), make_key(100) },
                         out_values,
                         lfdb::commit_after_op::no_commit));

  CHECK(100 == out_values.size());
 }

 SECTION("range overloads", "[fdb]") {
  j.drop_test_namespace();

  const TestType in_values {
   string_pair { make_key(0), make_value(0) },
   string_pair { make_key(1), make_value(1) },
   string_pair { make_key(2), make_value(2) }
  };

  lfdb::set(j, in_values);

  TestType out_values;
  CHECK(3 == lfdb::get(j, { make_key_prefix() }, out_values));
  CHECK_THAT(out_values, Catch::Matchers::RangeEquals(in_values));

  auto txn = lfdb::make_transaction(j);
  const TestType txn_values {
   string_pair { make_key(3), make_value(3) },
   string_pair { make_key(4), make_value(4) },
   string_pair { make_key(5), make_value(5) }
  };

  lfdb::set(txn, txn_values, lfdb::commit_after_op::commit);

  TestType query_values;
  CHECK(6 == lfdb::get(j, lfdb::query::prefix(make_key_prefix()), query_values));
  CHECK_THAT(query_values | std::views::take(3),
             Catch::Matchers::RangeEquals(in_values));
  CHECK_THAT(query_values | std::views::drop(3),
             Catch::Matchers::RangeEquals(txn_values));
 }
}

TEST_CASE("check selectors", "[fdb][rgw]") {
 janitor dbh;

 const int nentries = 10;

 const auto select_all = lfdb::select { make_key(0), make_key(nentries) };

 CHECK("" == lfdb::select { "" }.begin_key);
 CHECK("\xFF" == lfdb::select { "" }.end_key);
 CHECK("abc" == lfdb::select { "abc" }.begin_key);
 CHECK("abd" == lfdb::select { "abc" }.end_key);
 CHECK("abd" == lfdb::select { std::string("abc\xFF", 4) }.end_key);
 CHECK_THROWS_AS([] {
   const auto invalid_prefix = std::string("\xFF", 1);
   const auto selector = lfdb::select { invalid_prefix };
   (void)selector;
 }(), lfdb::libfdb_exception);

 // Make sure that there's nothing in our test range:
 dbh.drop_test_namespace();
 REQUIRE(0 == key_count(dbh, select_all));

 const auto kvs = write_monotonic_kvs(dbh, nentries);

 // Make sure there's exactly as many entries as we added:
 REQUIRE(nentries == key_count(dbh, select_all));

 std::vector<std::pair<std::string, std::string>> out;
 const auto nread = lfdb::get(dbh, select_all, std::back_inserter(out));

 // These /are/ the droids you're looking for:
 CHECK(nentries == nread);
 CHECK(nentries == out.size());
 CHECK(make_key(0) == out.front().first);
 CHECK(make_key(nentries - 1) == out.back().first);

 auto keys_in = [&dbh](lfdb::select selector) {
  std::vector<std::pair<std::string, std::string>> entries;
  const auto nread = lfdb::get(dbh, selector, std::back_inserter(entries));

  CHECK(nread == std::size(entries));

  return entries
       | std::views::transform([](const auto& kv) { return kv.first; })
       | std::ranges::to<std::vector<std::string>>();
 };

 CHECK_THAT(keys_in(lfdb::select { make_key(3), make_key(6) }),
            Catch::Matchers::RangeEquals(std::vector { make_key(3), make_key(4), make_key(5) }));
 CHECK_THAT(keys_in(lfdb::select { lfdb::exclusive(make_key(3)), make_key(6) }),
            Catch::Matchers::RangeEquals(std::vector { make_key(4), make_key(5) }));
 CHECK_THAT(keys_in(lfdb::select { make_key(3), lfdb::inclusive(make_key(6)) }),
            Catch::Matchers::RangeEquals(std::vector { make_key(3), make_key(4), make_key(5), make_key(6) }));
 CHECK_THAT(keys_in(lfdb::select { lfdb::exclusive(make_key(3)), lfdb::inclusive(make_key(6)) }),
            Catch::Matchers::RangeEquals(std::vector { make_key(4), make_key(5), make_key(6) }));

 auto reverse_open_closed = lfdb::select { lfdb::exclusive(make_key(3)), lfdb::inclusive(make_key(6)) };
 reverse_open_closed.options.reverse_order = true;
 reverse_open_closed.options.result_limit = 2;
 CHECK_THAT(keys_in(reverse_open_closed),
            Catch::Matchers::RangeEquals(std::vector { make_key(6), make_key(5), make_key(4) }));

 SECTION("reverse order") {
  auto reverse_all = select_all;
  reverse_all.options.reverse_order = true;

  out.clear();
  lfdb::get(dbh, reverse_all, std::back_inserter(out));

  REQUIRE(nentries == out.size());
  CHECK(make_key(nentries - 1) == out.front().first);
  CHECK(make_key(0) == out.back().first);
  CHECK(std::ranges::is_sorted(out, std::ranges::greater {},
                               &std::pair<std::string, std::string>::first));
 }

 std::map<std::string, std::string> out_map;

 CHECK(nentries == lfdb::get(dbh, select_all, out_map));
 CHECK(nentries == out_map.size());
 CHECK(make_value(0) == out_map.at(make_key(0)));

 lfdb::set(dbh, test_key("keyx"), "outside");
 out.clear();
 CHECK(nentries == lfdb::get(dbh,
                             lfdb::select { make_key_prefix() },
                             std::back_inserter(out)));
 CHECK(nentries == out.size());

 // Get exactly no entries:
 out.clear();
 CHECK(0 == lfdb::get(dbh,
                      lfdb::select { make_key(0), make_key(0) },
                      std::back_inserter(out)));
 CHECK(0 == out.size());

 // Get exactly one entry: 
 out.clear();
 CHECK(1 == lfdb::get(dbh,
                      lfdb::select { make_key(1), make_key(2) },
                      std::back_inserter(out)));
 REQUIRE(1 == out.size());
 CHECK(make_key(1) == out.front().first);
}

TEST_CASE("query algebra expressions execute through fdb operations", "[fdb][query]")
{
 namespace lq = ceph::libfdb::query;

 janitor dbh;

 const int nentries = 10;
 const auto select_all = lfdb::select { make_key(0), make_key(nentries) };

 dbh.drop_test_namespace();
 REQUIRE(0 == key_count(dbh, select_all));

 write_monotonic_kvs(dbh, nentries);
 REQUIRE(nentries == key_count(dbh, select_all));

 const auto without_middle =
  lq::difference(lq::between(make_key(0), make_key(nentries)),
                 lq::between(make_key(3), make_key(7)));

 std::vector<std::pair<std::string, std::string>> out;
 CHECK(6 == lfdb::get(dbh, without_middle, std::back_inserter(out)));

 CHECK_THAT(out | std::views::keys | std::ranges::to<std::vector<std::string>>(),
            Catch::Matchers::RangeEquals(std::vector {
             make_key(0), make_key(1), make_key(2),
             make_key(7), make_key(8), make_key(9)
            }));

 lfdb::erase(dbh, without_middle);
 CHECK(4 == key_count(dbh, select_all));
}

TEST_CASE("managed range get publishes output only after a successful read", "[fdb][query]")
{
 janitor dbh;

 const auto prefix = make_key_prefix("publish");

 const auto good_key = make_key(0, "publish");
 const auto bad_key = make_key(1, "publish");

 const std::vector<string_pair> original {
  { "already", "present" }
 };

 lfdb::set(dbh, good_key, "good");

 const std::array<std::uint8_t, 1> invalid_serialized_value { 0xFF };
 write_raw_fdb_value(dbh, bad_key, invalid_serialized_value);

 SECTION("container output")
 {
  auto out = original;

  CHECK_THROWS_AS(lfdb::get(dbh, lfdb::select { prefix }, out),
                  lfdb::libfdb_exception);
  CHECK_THAT(out, Catch::Matchers::RangeEquals(original));
 }

 SECTION("output iterator")
 {
  auto out = original;

  CHECK_THROWS_AS(lfdb::get(dbh, lfdb::select { prefix }, std::back_inserter(out)),
                  lfdb::libfdb_exception);
  CHECK_THAT(out, Catch::Matchers::RangeEquals(original));
 }
}

TEST_CASE("managed range get appends materialized results after success", "[fdb][query]")
{
 janitor dbh;

 const auto prefix = make_key_prefix("publish-success");

 const auto first_key = make_key(0, "publish-success");
 const auto second_key = make_key(1, "publish-success");

 std::map<std::string, std::string> out {
  { "already", "present" }
 };

 lfdb::set(dbh, first_key, "zero");
 lfdb::set(dbh, second_key, "one");

 CHECK(2 == lfdb::get(dbh, lfdb::select { prefix }, out));

 CHECK("present" == out.at("already"));
 CHECK("zero" == out.at(first_key));
 CHECK("one" == out.at(second_key));
}

TEST_CASE("query algebra examples execute against fdb", "[fdb][query][example]")
{
 namespace lq = ceph::libfdb::query;

 janitor dbh;

 auto keys_in = [&dbh](const auto& query) {
  std::vector<std::pair<std::string, std::string>> entries;
  const auto nread = lfdb::get(dbh, query, std::back_inserter(entries));

  CHECK(nread == std::size(entries));

  return entries
       | std::views::keys
       | std::ranges::to<std::vector<std::string>>();
 };
 auto keys_from_transaction_scan = [&dbh](auto query) {
  auto txn = lfdb::make_transaction(dbh);

  return lfdb::scan(txn, std::move(query))
       | std::views::keys
       | std::ranges::to<std::vector<std::string>>();
 };
 auto keys_from_managed_scan = [&dbh](auto query) {
  return lfdb::scan(dbh, std::move(query))
       | std::views::keys
       | std::ranges::to<std::vector<std::string>>();
 };
 auto keys_from_blocks = [&dbh](auto query) {
  std::vector<std::string> keys;

  for (const auto& block : lfdb::blocks(dbh, std::move(query))) {
   std::ranges::copy(block | std::views::keys, std::back_inserter(keys));
  }

  return keys;
 };

 SECTION("prefix and cursor queries read the intended record keys")
 {
  const auto base = test_key("collection-a/records/");
  const auto record_key = [&base](std::string_view record_name) {
   return base + std::string(record_name);
  };
  const auto record_query = [&base](std::string_view prefix,
                                    std::string_view cursor = {},
                                    const bool cursor_inclusive = true) {
   const auto prefix_key = base + std::string(prefix);
   auto query = lq::prefix(prefix_key);

   if (cursor.empty()) {
    return query;
   }

   const auto cursor_key = base + std::string(cursor);
   const auto lower = cursor_inclusive ? lq::closed(cursor_key)
                                       : lq::open(cursor_key);

   return lq::intersection(query,
                           lq::between(lower, lq::open(lq::successor(prefix_key))));
  };

  for (const auto record_name : { "abandoned/9999"sv,
                                  "active/0001"sv,
                                  "active/0007"sv,
                                  "active/0010"sv,
                                  "expired/0001"sv }) {
   lfdb::set(dbh, record_key(record_name), record_name);
  }

  CHECK_THAT(keys_in(record_query("active/")),
             Catch::Matchers::RangeEquals(std::vector {
              record_key("active/0001"),
              record_key("active/0007"),
              record_key("active/0010")
             }));
  CHECK_THAT(keys_in(record_query("active/", "active/0007", false)),
             Catch::Matchers::RangeEquals(std::vector {
              record_key("active/0010")
             }));
  CHECK_THAT(keys_in(record_query("active/", "abandoned/9999")),
             Catch::Matchers::RangeEquals(std::vector {
              record_key("active/0001"),
              record_key("active/0007"),
              record_key("active/0010")
             }));
  CHECK(lq::is_empty_expression(record_query("active/", "expired/0001")));
 }

 SECTION("set operations read and erase multiple real intervals")
 {
  for (const auto key : { "cache/hot/a"sv,
                          "cache/hot/b"sv,
                          "cache/warm/a"sv,
                          "cache/cold/a"sv,
                          "cache/hot/private/a"sv }) {
   lfdb::set(dbh, test_key(key), key);
  }

  const auto active_cache =
   lq::set_union(lq::prefix(test_key("cache/hot/")),
                 lq::prefix(test_key("cache/warm/")));
  const auto active_cache_keys = std::vector {
   test_key("cache/hot/a"),
   test_key("cache/hot/b"),
   test_key("cache/hot/private/a"),
   test_key("cache/warm/a")
  };

  CHECK_THAT(keys_in(active_cache),
             Catch::Matchers::RangeEquals(active_cache_keys));

  std::vector<string_pair> positioned_entries;
  CHECK(std::size(active_cache_keys) ==
        lfdb::get(dbh, active_cache, position_insert_iterator { &positioned_entries }));
  CHECK_THAT(positioned_entries | std::views::keys | std::ranges::to<std::vector<std::string>>(),
             Catch::Matchers::RangeEquals(active_cache_keys));

  CHECK_THAT(keys_from_transaction_scan(active_cache),
             Catch::Matchers::RangeEquals(active_cache_keys));
  CHECK_THAT(keys_from_managed_scan(active_cache),
             Catch::Matchers::RangeEquals(active_cache_keys));
  CHECK_THAT(keys_from_blocks(active_cache),
             Catch::Matchers::RangeEquals(active_cache_keys));

  const auto visible_hot =
   lq::difference(lq::prefix(test_key("cache/hot/")),
                  lq::prefix(test_key("cache/hot/private/")));
  CHECK_THAT(keys_in(visible_hot),
             Catch::Matchers::RangeEquals(std::vector {
              test_key("cache/hot/a"),
              test_key("cache/hot/b")
             }));

  const auto visible_window =
   lq::intersection(visible_hot,
                    lq::between(test_key("cache/hot/b"),
                                 test_key("cache/hot/z")));
  CHECK_THAT(keys_in(visible_window),
             Catch::Matchers::RangeEquals(std::vector {
              test_key("cache/hot/b")
             }));

  lfdb::erase(dbh, active_cache);
  CHECK_THAT(keys_in(lfdb::select { test_key("cache/") }),
             Catch::Matchers::RangeEquals(std::vector {
              test_key("cache/cold/a")
             }));
 }

 SECTION("query intervals feed managed scans end-to-end")
 {
  const auto base = test_key("versions/");
  const auto version_key = [&base](std::string_view score, std::string_view version) {
   return base + std::string(score) + "/" + std::string(version);
  };

  lfdb::set(dbh, version_key("0000000000001.000000", "v1"), "v1");
  lfdb::set(dbh, version_key("0000000000002.000000", "v2"), "v2");
  lfdb::set(dbh, version_key("0000000000003.000000", "v3"), "v3");
  lfdb::set(dbh, version_key("0000000000004.000000", "v4"), "v4");

  const auto begin_prefix = base + "0000000000002.000000/";
  const auto end_prefix = base + "0000000000003.000000/";
  const auto score_range = lq::between(begin_prefix, lq::successor(end_prefix));

  std::vector<std::string> keys;
  for (const auto& block : lfdb::blocks(dbh, score_range)) {
   std::ranges::copy(block | std::views::keys, std::back_inserter(keys));
  }

  CHECK_THAT(keys,
             Catch::Matchers::RangeEquals(std::vector {
              version_key("0000000000002.000000", "v2"),
              version_key("0000000000003.000000", "v3")
             }));
 }
}

TEST_CASE("compatibility generator names delegate to scan vocabulary", "[fdb]")
{
 janitor dbh;

 const auto kvs_in = write_monotonic_kvs(dbh, 10, "compat-generator");
 const auto selector = lfdb::select { make_key(0, "compat-generator"),
                                      make_key(10, "compat-generator") };

 auto txn = lfdb::make_transaction(dbh);

 const auto scan_keys = lfdb::scan(txn, selector)
                      | std::views::keys
                      | std::ranges::to<std::vector<std::string>>();
 const auto pair_generator_keys = lfdb::pair_generator(txn, selector)
                                | std::views::keys
                                | std::ranges::to<std::vector<std::string>>();

 std::vector<std::string> block_generator_keys;
 for (const auto& block : lfdb::block_generator(dbh, selector)) {
  std::ranges::copy(block | std::views::keys,
                    std::back_inserter(block_generator_keys));
 }

 CAPTURE(kvs_in);
 CHECK_THAT(pair_generator_keys, Catch::Matchers::RangeEquals(scan_keys));
 CHECK_THAT(block_generator_keys, Catch::Matchers::RangeEquals(scan_keys));
}

TEST_CASE("fdb conversions (built-in)", "[fdb][rgw]") {
 // Manual tests of conversions to and from supported FDB built-in types.

 SECTION("spanlike") {
  // span<uint8_t> -> vector<uint8_t> -> vector<uint8_t>
  const std::span<const std::uint8_t> n((const std::uint8_t *)msg, sizeof(msg));

  std::vector<std::uint8_t> x;
  x = ceph::libfdb::to::convert(n);

  std::vector<std::uint8_t> o;
  ceph::libfdb::from::convert(x, o); 

  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 } 

 SECTION("NULL-as-data") {
  // with NULL data-- const char* -> vector<uint8_t> -> vector<uint8_t>
  const std::span<const std::uint8_t> n((const std::uint8_t *)msg_with_null, sizeof(msg_with_null));

  std::vector<std::uint8_t> x;
  x = ceph::libfdb::to::convert(n);

  std::vector<std::uint8_t> o;
  ceph::libfdb::from::convert(x, o); 

  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
  REQUIRE_THAT(msg_with_null, Catch::Matchers::RangeEquals(o));
 }
}

TEST_CASE("fdb conversions (round-trip)", "[fdb][rgw]") {
 janitor j;

 // string_view -> string
 {
 const std::string_view n = "Hello, World!";
 std::string o;

 const auto key = test_key("key");
 lfdb::set(lfdb::make_transaction(j), key, n, lfdb::commit_after_op::commit);
 lfdb::get(lfdb::make_transaction(j), key, o, lfdb::commit_after_op::no_commit);

 REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }

 // vector<uint8_t> -> vector<uint8_t>
 {
 const std::vector<uint8_t> n = { 1, 2, 3, 4, 5 };
 std::vector<uint8_t> o;

 const auto key = test_key("key");
 lfdb::set(lfdb::make_transaction(j), key, n, lfdb::commit_after_op::commit);
 lfdb::get(lfdb::make_transaction(j), key, o, lfdb::commit_after_op::no_commit);

 REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }
} 

TEST_CASE("fdb conversions (functions)", "[fdb][rgw]")
{
 SECTION("convert with a lambda function")
 {
  std::string_view n { pearl_msg };
  std::string o;

  std::vector<std::uint8_t> x = ceph::libfdb::to::convert(n);

  auto fn = [&o](const char *data, std::size_t sz) -> void { 
    // Because we did /conversion/ on the inbound data, we're still obliged to
    // reverse this (otherwise we'll see whatever artefacts the conversion produced)--
    // the complication is a consequence of dealing with the underlying buffer directly:
    std::span<const std::uint8_t> in_span((const std::uint8_t *)data, sz);
 
    ceph::libfdb::from::convert(in_span, o);
  };

  ceph::libfdb::from::convert(x, fn); 

  CAPTURE(n);
  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }

 SECTION("get with a raw value callback")
 {
  janitor j;

  std::string_view n { pearl_msg };
  std::string o;

  const auto key = test_key("key");
  lfdb::set(j, key, n);

  REQUIRE(lfdb::get(j, key, [&o](std::span<const std::uint8_t> in) {
    ceph::libfdb::from::convert(in, o);
  }));

  CAPTURE(n);
  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }
}

TEST_CASE("read_query_window", "[fdb]")
{
 janitor j;

 const std::size_t result_limit = 5;
 const std::size_t nkeys = 12;

 const auto kvs_in = write_monotonic_kvs(j, nkeys);

 SECTION("reads one bounded forward window") {
  auto selector = lfdb::select { make_key(0), make_key(nkeys) };
  selector.options.result_limit = result_limit;

  auto txn = lfdb::make_transaction(j);
  auto window = ceph::libfdb::detail::read_query_window(*txn, selector, 1);
  const auto out = decode_raw_fdb_pairs(window.result_pairs);

  REQUIRE_FALSE(out.empty());
  CHECK(out.size() <= result_limit);
  CHECK(window.more_available);
  CHECK_THAT(out, Catch::Matchers::RangeEquals(first_keys(kvs_in, out.size())));
 }

 SECTION("reads one bounded reverse window") {
  auto selector = lfdb::select { make_key(0), make_key(nkeys) };
  selector.options.result_limit = result_limit;
  selector.options.reverse_order = true;

  auto txn = lfdb::make_transaction(j);
  auto window = ceph::libfdb::detail::read_query_window(*txn, selector, 1);
  const auto out = decode_raw_fdb_pairs(window.result_pairs);

  REQUIRE_FALSE(out.empty());
  CHECK(out.size() <= result_limit);
  CHECK(window.more_available);
  CHECK_THAT(out, Catch::Matchers::RangeEquals(last_keys_reversed(kvs_in, out.size())));
 }

 SECTION("reports terminal windows") {
  auto selector = lfdb::select { make_key(0), make_key(1) };
  selector.options.result_limit = result_limit;

  auto txn = lfdb::make_transaction(j);
  auto window = ceph::libfdb::detail::read_query_window(*txn, selector, 1);
  const auto out = decode_raw_fdb_pairs(window.result_pairs);

  REQUIRE(1 == out.size());
  CHECK_FALSE(window.more_available);
  CHECK(make_key(0) == out.front().first);
 }
}

TEST_CASE("generate_FDB_pairs", "[fdb]")
{
 janitor j;

 const std::size_t result_limit = 5;
 const std::size_t nkeys = 12;

 const auto kvs_in = write_monotonic_kvs(j, nkeys);

 auto collect_pages = [&j](auto selector) {
  auto txn = lfdb::make_transaction(j);

  return ceph::libfdb::detail::generate_FDB_pairs(*txn, selector)
       | std::views::transform(decode_raw_fdb_pairs)
       | std::ranges::to<std::vector<std::vector<string_pair>>>();
 };

 SECTION("drains paged forward windows") {
  auto selector = lfdb::select { make_key(0), make_key(nkeys) };
  selector.options.result_limit = result_limit;

  const auto pages = collect_pages(selector);
  const auto out = pages | std::views::join | std::ranges::to<std::vector<string_pair>>();

  CAPTURE(pages.size());
  CAPTURE(out.size());
  CHECK(1 < pages.size());
  CHECK(std::ranges::all_of(pages, [result_limit](const auto& page) {
   return page.size() <= result_limit;
  }));
  CHECK_THAT(out, Catch::Matchers::RangeEquals(first_keys(kvs_in, nkeys)));
 }

 SECTION("drains paged reverse windows") {
  auto selector = lfdb::select { make_key(0), make_key(nkeys) };
  selector.options.result_limit = result_limit;
  selector.options.reverse_order = true;

  const auto pages = collect_pages(selector);
  const auto out = pages | std::views::join | std::ranges::to<std::vector<string_pair>>();

  CAPTURE(pages.size());
  CAPTURE(out.size());
  CHECK(1 < pages.size());
  CHECK(std::ranges::all_of(pages, [result_limit](const auto& page) {
   return page.size() <= result_limit;
  }));
  CHECK_THAT(out, Catch::Matchers::RangeEquals(last_keys_reversed(kvs_in, nkeys)));
 }
}

TEST_CASE("basic generators", "[fdb]") {
 janitor j;

 const unsigned nkeys = GENERATE(0, 1, 2, 3, 10, 100, 1'000);

 const auto kvs_in = write_monotonic_kvs(j, nkeys);
 REQUIRE(nkeys == kvs_in.size());

 SECTION("pair_generator forward") {
    auto txn = lfdb::make_transaction(j);

    const auto out = lfdb::pair_generator(txn, lfdb::select { make_key(0), make_key(nkeys) })
                   | std::ranges::to<std::vector<string_pair>>();

    CAPTURE(nkeys);
    CAPTURE(out.size());
    REQUIRE(nkeys == out.size());

    // Be sure we captured the head and the tail:
    if (0 < nkeys) {
      CAPTURE(out.front().first);
      CAPTURE(out.back().first);
      CHECK(make_key(0) == out.front().first);
      CHECK(make_key(nkeys - 1) == out.back().first);
      CHECK(std::ranges::is_sorted(out, std::ranges::less {},
                                   &std::pair<std::string, std::string>::first));
    }
 }

 SECTION("pair_generator reverse") {
    auto selector = lfdb::select { make_key(0), make_key(nkeys) };
    selector.options.reverse_order = true;

    auto txn = lfdb::make_transaction(j);
    const auto out = lfdb::pair_generator(txn, selector)
                   | std::ranges::to<std::vector<string_pair>>();

    CAPTURE(nkeys);
    CAPTURE(out.size());
    REQUIRE(nkeys == out.size());

    if (0 < nkeys) {
      CAPTURE(out.front().first);
      CAPTURE(out.back().first);
      CHECK(make_key(nkeys - 1) == out.front().first);
      CHECK(make_key(0) == out.back().first);
      CHECK(std::ranges::is_sorted(out, std::ranges::greater {},
                                   &std::pair<std::string, std::string>::first));
    }
 }

 SECTION("pair_generator forward, paged") {
    auto selector = lfdb::select { make_key(0), make_key(nkeys) };
    selector.options.result_limit = 5; // one of the most prime of prime numbers

    auto txn = lfdb::make_transaction(j);
    const auto out = lfdb::pair_generator(txn, selector)
                   | std::ranges::to<std::vector<string_pair>>();

    CAPTURE(nkeys);
    CAPTURE(out.size());
    REQUIRE(nkeys == out.size());

    if (0 < nkeys) {
      CAPTURE(out.front().first);
      CAPTURE(out.back().first);
      CHECK(make_key(0) == out.front().first);
      CHECK(make_key(nkeys - 1) == out.back().first);
      CHECK(std::ranges::is_sorted(out, std::ranges::less {},
                                   &std::pair<std::string, std::string>::first));
    }
 }

 SECTION("pair_generator reverse, paged") {
    auto selector = lfdb::select { make_key(0), make_key(nkeys) };
    selector.options.reverse_order = true;
    selector.options.result_limit = 5; // one of the most prime of prime numbers

    auto txn = lfdb::make_transaction(j);
    const auto out = lfdb::pair_generator(txn, selector)
                   | std::ranges::to<std::vector<string_pair>>();

    CAPTURE(nkeys);
    CAPTURE(out.size());
    REQUIRE(nkeys == out.size());

    if (0 < nkeys) {
      CAPTURE(out.front().first);
      CAPTURE(out.back().first);
      CHECK(make_key(nkeys - 1) == out.front().first);
      CHECK(make_key(0) == out.back().first);
      CHECK(std::ranges::is_sorted(out, std::ranges::greater {},
                                   &std::pair<std::string, std::string>::first));
    }
 }

 SECTION("pair_generator owns its coroutine transaction") {
    std::map<std::string, std::string> out;
    const auto selector = lfdb::select { make_key(0), make_key(nkeys) };

    // The transaction handle local to this lambda must remain alive inside the
    // generator's coroutine frame after the lambda returns:
    auto gen = [&j, selector] {
      auto txn = lfdb::make_transaction(j);
      return lfdb::pair_generator(txn, selector);
    }();

    std::ranges::copy(gen, std::inserter(out, std::end(out)));

    CAPTURE(nkeys);
    CAPTURE(out.size());
    REQUIRE(nkeys == out.size());

    if (0 < nkeys) {
      CHECK(out.contains(make_key(0)));
      CHECK(out.contains(make_key(nkeys - 1)));
    }
 }

 SECTION("for_each forward") {
    std::vector<string_pair> out;
    auto txn = lfdb::make_transaction(j);

    lfdb::for_each(txn, lfdb::select { make_key(0), make_key(nkeys) },
                   [&out](string_pair&& row) {
                     out.emplace_back(std::move(row));
                   });

    CAPTURE(nkeys);
    CAPTURE(out.size());
    REQUIRE(nkeys == out.size());

    if (0 < nkeys) {
      CHECK(make_key(0) == out.front().first);
      CHECK(make_key(nkeys - 1) == out.back().first);
      CHECK(std::ranges::is_sorted(out, std::ranges::less {},
                                   &std::pair<std::string, std::string>::first));
    }
 }

 SECTION("for_each reverse") {
    std::vector<string_pair> out;
    auto selector = lfdb::select { make_key(0), make_key(nkeys) };
    selector.options.reverse_order = true;

    auto txn = lfdb::make_transaction(j);
    lfdb::for_each(txn, selector, [&out](string_pair&& row) {
      out.emplace_back(std::move(row));
    });

    CAPTURE(nkeys);
    CAPTURE(out.size());
    REQUIRE(nkeys == out.size());

    if (0 < nkeys) {
      CHECK(make_key(nkeys - 1) == out.front().first);
      CHECK(make_key(0) == out.back().first);
      CHECK(std::ranges::is_sorted(out, std::ranges::greater {},
                                   &std::pair<std::string, std::string>::first));
    }
 }

 SECTION("for_each with database handle") {
    std::vector<string_pair> out;

    lfdb::for_each(j, lfdb::select { make_key(0), make_key(nkeys) },
                   [&out](string_pair&& row) {
                    out.emplace_back(std::move(row));
                   });

    CAPTURE(nkeys);
    CAPTURE(out.size());
    REQUIRE(nkeys == out.size());

    if (0 < nkeys) {
      CHECK(make_key(0) == out.front().first);
      CHECK(make_key(nkeys - 1) == out.back().first);
    }
 }

 SECTION("transform to output iterator") {
    std::vector<std::string> keys;
    auto txn = lfdb::make_transaction(j);

    lfdb::transform(txn, lfdb::select { make_key(0), make_key(nkeys) },
                    [](string_pair&& row) {
                     return std::move(row.first);
                    },
                    std::back_inserter(keys));

    CAPTURE(nkeys);
    CAPTURE(keys.size());
    REQUIRE(nkeys == keys.size());

    if (0 < nkeys) {
      CHECK(make_key(0) == keys.front());
      CHECK(make_key(nkeys - 1) == keys.back());
      CHECK(std::ranges::is_sorted(keys));
    }
 }

 SECTION("transform returns vector") {
    auto selector = lfdb::select { make_key(0), make_key(nkeys) };
    selector.options.reverse_order = true;

    auto txn = lfdb::make_transaction(j);
    const auto values = lfdb::transform(txn, selector, [](string_pair&& row) {
      return std::move(row.second);
    });

    CAPTURE(nkeys);
    CAPTURE(values.size());
    REQUIRE(nkeys == values.size());

    if (0 < nkeys) {
      CHECK(make_value(nkeys - 1) == values.front());
      CHECK(make_value(0) == values.back());
    }
 }

 SECTION("transform with database handle returns vector") {
    const auto values = lfdb::transform(j,
                                        lfdb::select { make_key(0), make_key(nkeys) },
                                        [](string_pair&& row) {
                                         return std::move(row.second);
                                        });

    CAPTURE(nkeys);
    CAPTURE(values.size());
    REQUIRE(nkeys == values.size());

    if (0 < nkeys) {
      CHECK(make_value(0) == values.front());
      CHECK(make_value(nkeys - 1) == values.back());
    }
 }

 SECTION("transform with database handle writes to output iterator") {
    std::vector<std::string> keys;

    lfdb::transform(j, lfdb::select { make_key(0), make_key(nkeys) },
                    [](string_pair&& row) {
                     return std::move(row.first);
                    },
                    std::back_inserter(keys));

    CAPTURE(nkeys);
    CAPTURE(keys.size());
    REQUIRE(nkeys == keys.size());

    if (0 < nkeys) {
      CHECK(make_key(0) == keys.front());
      CHECK(make_key(nkeys - 1) == keys.back());
    }
 }

 SECTION("erase_if in explicit transaction") {
    const auto erase_first = 0 < nkeys;
    const auto erase_third = 2 < nkeys;

    auto txn = lfdb::make_transaction(j);
    const auto removed = lfdb::erase_if(txn,
                                        lfdb::select { make_key(0), make_key(nkeys) },
                                        [](const string_pair& row) {
                                         return make_key(0) == row.first ||
                                                make_key(2) == row.first;
                                        });
    REQUIRE(lfdb::commit(txn));

    CHECK(static_cast<std::size_t>(erase_first) +
          static_cast<std::size_t>(erase_third) == removed);

    const auto remaining = lfdb::collect(j, lfdb::select { make_key(0), make_key(nkeys) });

    if (erase_first) {
      CHECK_FALSE(contains_key(remaining, make_key(0)));
    }
    if (erase_third) {
      CHECK_FALSE(contains_key(remaining, make_key(2)));
    }
    if (1 < nkeys) {
      CHECK(contains_key(remaining, make_key(1)));
    }
 }

 SECTION("erase_if with database handle") {
    const auto erase_second = 1 < nkeys;
    const auto erase_fourth = 3 < nkeys;

    const auto removed = lfdb::erase_if(j,
                                        lfdb::select { make_key(0), make_key(nkeys) },
                                        [](const string_pair& row) {
                                         return make_key(1) == row.first ||
                                                make_key(3) == row.first;
                                        });

    CHECK(static_cast<std::size_t>(erase_second) +
          static_cast<std::size_t>(erase_fourth) == removed);

    const auto remaining = lfdb::collect(j, lfdb::select { make_key(0), make_key(nkeys) });

    if (erase_second) {
      CHECK_FALSE(contains_key(remaining, make_key(1)));
    }
    if (erase_fourth) {
      CHECK_FALSE(contains_key(remaining, make_key(3)));
    }
    if (0 < nkeys) {
      CHECK(contains_key(remaining, make_key(0)));
    }
 }
}

TEST_CASE("paged scan", "[fdb]") {
 janitor j;

 const unsigned nkeys = 10;
 const auto kvs_in = write_monotonic_kvs(j, nkeys);

 SECTION("collects one generic range page") {
  auto source = std::views::iota(0, 6)
              | std::views::transform([](const auto n) { return n * 2; });

  const auto page = lfdb::collect(source, lfdb::page{3});

  CHECK(page.has_more);
  CHECK_THAT(page.rows, Catch::Matchers::RangeEquals(std::vector{0, 2, 4}));
 }

 SECTION("collects an unlimited generic range page") {
  const auto page = lfdb::collect(std::views::iota(0, 3), lfdb::page{});

  CHECK_FALSE(page.has_more);
  CHECK_THAT(page.rows, Catch::Matchers::RangeEquals(std::vector{0, 1, 2}));
 }

 SECTION("collects one page from a scan range") {
  auto txn = lfdb::make_transaction(j);
  auto rows = lfdb::scan(txn, lfdb::select { make_key(0), make_key(nkeys) });

  const auto page = lfdb::collect(std::move(rows), lfdb::page{4});

  CHECK(page.has_more);
  CHECK_THAT(page.rows, Catch::Matchers::RangeEquals(first_keys(kvs_in, 4)));
 }

 SECTION("scans one page in an explicit transaction") {
  auto txn = lfdb::make_transaction(j);
  const auto page = lfdb::scan(txn, lfdb::select { make_key(0), make_key(nkeys) },
                               lfdb::page{3});

  CHECK(page.has_more);
  CHECK_THAT(page.rows, Catch::Matchers::RangeEquals(first_keys(kvs_in, 3)));
 }

 SECTION("scans one page in an implicit transaction") {
  const auto page = lfdb::scan(j, lfdb::select { make_key(0), make_key(nkeys) },
                               lfdb::page{3});

  CHECK(page.has_more);
  CHECK_THAT(page.rows, Catch::Matchers::RangeEquals(first_keys(kvs_in, 3)));
 }

 SECTION("scans an unlimited page") {
  const auto page = lfdb::scan(j, lfdb::select { make_key(0), make_key(nkeys) },
                               lfdb::page{});

  CHECK_FALSE(page.has_more);
  CHECK_THAT(page.rows, Catch::Matchers::RangeEquals(kvs_in));
 }

 SECTION("scans a terminal page") {
  const auto page = lfdb::scan(j, lfdb::select { make_key(0), make_key(2) },
                               lfdb::page{3});

  CHECK_FALSE(page.has_more);
  CHECK_THAT(page.rows, Catch::Matchers::RangeEquals(first_keys(kvs_in, 2)));
 }

 SECTION("scans one reverse page") {
  auto selector = lfdb::select { make_key(0), make_key(nkeys) };
  selector.options.reverse_order = true;

  const auto page = lfdb::scan(j, selector, lfdb::page{3});

  CHECK(page.has_more);
  CHECK_THAT(page.rows, Catch::Matchers::RangeEquals(last_keys_reversed(kvs_in, 3)));
 }

 SECTION("does not decode the forward continuation sentinel") {
  constexpr auto prefix = "paged-scan-forward-sentinel";

  lfdb::make_transactor(j)([prefix](auto& txn) {
   for (const auto n : std::views::iota(0, 3)) {
    lfdb::set(txn, make_key(n, prefix), n);
   }

   std::span<const std::uint8_t> invalid_value;
   ceph::libfdb::detail::transaction_set_kv_bytes(
    txn,
    ceph::libfdb::detail::as_fdb_span(make_key(3, prefix)),
    invalid_value);
  });

  const auto page = lfdb::scan<int>(j, lfdb::select { make_key(0, prefix),
                                                      make_key(4, prefix) },
                                    lfdb::page{3});

  CHECK(page.has_more);
  CHECK(3 == std::size(page));
 }

 SECTION("does not decode the reverse continuation sentinel") {
  constexpr auto prefix = "paged-scan-reverse-sentinel";

  lfdb::make_transactor(j)([prefix](auto& txn) {
   std::span<const std::uint8_t> invalid_value;
   ceph::libfdb::detail::transaction_set_kv_bytes(
    txn,
    ceph::libfdb::detail::as_fdb_span(make_key(0, prefix)),
    invalid_value);

   for (const auto n : std::views::iota(1, 4)) {
    lfdb::set(txn, make_key(n, prefix), n);
   }
  });

  auto selector = lfdb::select { make_key(0, prefix), make_key(4, prefix) };
  selector.options.reverse_order = true;

  const auto page = lfdb::scan<int>(j, selector, lfdb::page{3});

  CHECK(page.has_more);
  CHECK(3 == std::size(page));
 }

 SECTION("scans an empty page") {
  const auto page = lfdb::scan(j, lfdb::select { make_key(nkeys), make_key(nkeys + 1) },
                               lfdb::page{3});

  CHECK(page.empty());
  CHECK_FALSE(page.has_more);
 }

 SECTION("rejects a page too large for an FDB range limit") {
  CHECK_THROWS_AS([] {
                   (void)lfdb::page{static_cast<uint64_t>(
                     std::numeric_limits<int>::max())};
                  }(),
                  lfdb::libfdb_exception);
 }
}

TEST_CASE("snapshot reads do not add read conflicts", "[fdb]") {
 janitor j;

 SECTION("single-key get") {
  const auto read_key = make_key(0, "snapshot-single");
  const auto write_key = make_key(1, "snapshot-single");

  lfdb::set(j, read_key, "old");

  auto serializable_txn = lfdb::make_transaction(j);
  std::string out;
  REQUIRE(lfdb::get(serializable_txn, read_key, out));
  lfdb::set(j, read_key, "new");
  lfdb::set(serializable_txn, write_key, "side-effect");
  CHECK_FALSE(lfdb::commit(serializable_txn));
  CHECK_FALSE(lfdb::key_exists(j, write_key));

  auto snapshot_txn = lfdb::make_transaction(j);
  REQUIRE(lfdb::get(snapshot_txn, read_key, out, lfdb::read_mode::snapshot));
  lfdb::set(j, read_key, "newer");
  lfdb::set(snapshot_txn, write_key, "snapshot-side-effect");
  CHECK(lfdb::commit(snapshot_txn));
  CHECK(lfdb::key_exists(j, write_key));
 }

 SECTION("range scan") {
  constexpr auto prefix = "snapshot-range";
  const auto conflict_key = make_key(0, prefix);
  const auto write_key = make_key(99, prefix);
  const auto selector = lfdb::select { make_key(0, prefix), make_key(2, prefix) };

  lfdb::set(j, conflict_key, "old");

  auto serializable_txn = lfdb::make_transaction(j);
  REQUIRE_FALSE(lfdb::collect(serializable_txn, selector).empty());
  lfdb::set(j, conflict_key, "new");
  lfdb::set(serializable_txn, write_key, "side-effect");
  CHECK_FALSE(lfdb::commit(serializable_txn));
  CHECK_FALSE(lfdb::key_exists(j, write_key));

  auto snapshot_txn = lfdb::make_transaction(j);
  REQUIRE_FALSE(lfdb::collect(snapshot_txn, selector, lfdb::read_mode::snapshot).empty());
  lfdb::set(j, conflict_key, "newer");
  lfdb::set(snapshot_txn, write_key, "snapshot-side-effect");
  CHECK(lfdb::commit(snapshot_txn));
  CHECK(lfdb::key_exists(j, write_key));
 }

 SECTION("selection get") {
  constexpr auto prefix = "snapshot-get-selection";
  const auto conflict_key = make_key(0, prefix);
  const auto write_key = make_key(99, prefix);
  const auto selector = lfdb::select { make_key(0, prefix), make_key(2, prefix) };

  lfdb::set(j, conflict_key, "old");

  auto snapshot_txn = lfdb::make_transaction(j);
  std::vector<string_pair> out;
  REQUIRE(1 == lfdb::get(snapshot_txn, selector, out, lfdb::read_mode::snapshot));

  lfdb::set(j, conflict_key, "new");
  lfdb::set(snapshot_txn, write_key, "snapshot-side-effect");

  CHECK(lfdb::commit(snapshot_txn));
  CHECK(lfdb::key_exists(j, write_key));
 }

 SECTION("functional helpers") {
  constexpr auto prefix = "snapshot-functional";
  const auto conflict_key = make_key(0, prefix);
  const auto write_key = make_key(99, prefix);
  const auto selector = lfdb::select { make_key(0, prefix), make_key(2, prefix) };

  lfdb::set(j, conflict_key, "old");

  auto snapshot_txn = lfdb::make_transaction(j);
  int visited = 0;
  lfdb::for_each(snapshot_txn, selector,
                 [&visited](string_pair&&) {
                  ++visited;
                 },
                 lfdb::read_mode::snapshot);
  REQUIRE(1 == visited);

  const auto values = lfdb::transform(snapshot_txn, selector,
                                      [](string_pair&& row) {
                                       return std::move(row.second);
                                      },
                                      lfdb::read_mode::snapshot);
  REQUIRE_THAT(values, Catch::Matchers::Equals(std::vector<std::string>{ "old" }));

  lfdb::set(j, conflict_key, "new");
  lfdb::set(snapshot_txn, write_key, "snapshot-side-effect");

  CHECK(lfdb::commit(snapshot_txn));
  CHECK(lfdb::key_exists(j, write_key));
 }

 SECTION("paginated scan") {
  constexpr auto prefix = "snapshot-page";
  const auto conflict_key = make_key(0, prefix);
  const auto write_key = make_key(99, prefix);
  const auto selector = lfdb::select { make_key(0, prefix), make_key(2, prefix) };

  lfdb::set(j, conflict_key, "old");

  auto snapshot_txn = lfdb::make_transaction(j);
  const auto page = lfdb::scan(snapshot_txn, selector, lfdb::page{1},
                              lfdb::read_mode::snapshot);
  REQUIRE(1 == page.size());

  lfdb::set(j, conflict_key, "new");
  lfdb::set(snapshot_txn, write_key, "snapshot-side-effect");

  CHECK(lfdb::commit(snapshot_txn));
  CHECK(lfdb::key_exists(j, write_key));
 }

 SECTION("managed block scan accepts snapshot mode") {
  constexpr auto prefix = "snapshot-blocks";
  const auto selector = lfdb::select { make_key(0, prefix), make_key(3, prefix) };

  lfdb::set(j, make_key(0, prefix), "zero");
  lfdb::set(j, make_key(1, prefix), "one");

  std::vector<string_pair> rows;
  for (const auto& block : lfdb::blocks(j, selector, lfdb::read_mode::snapshot)) {
   std::ranges::copy(block, std::back_inserter(rows));
  }

  CHECK(2 == std::size(rows));
 }

 SECTION("key existence") {
  const auto read_key = make_key(0, "snapshot-exists");
  const auto write_key = make_key(1, "snapshot-exists");

  lfdb::set(j, read_key, "old");

  auto snapshot_txn = lfdb::make_transaction(j);
  REQUIRE(lfdb::key_exists(snapshot_txn, read_key, lfdb::read_mode::snapshot));
  lfdb::set(j, read_key, "new");
  lfdb::set(snapshot_txn, write_key, "snapshot-side-effect");
  CHECK(lfdb::commit(snapshot_txn));
  CHECK(lfdb::key_exists(j, write_key));
 }
}

TEST_CASE("explicit conflict ranges", "[fdb]") {
 janitor j;

 SECTION("marked read conflict fails after concurrent write") {
  const auto guard_key = make_key(0, "conflict-read-key");
  const auto side_effect_key = make_key(1, "conflict-read-key");

  lfdb::set(j, guard_key, "old");

  auto txn = lfdb::make_transaction(j);

  lfdb::mark_conflict_read(txn, guard_key);

  lfdb::set(j, guard_key, "new");
  lfdb::set(txn, side_effect_key, "side-effect");

  CHECK_FALSE(lfdb::commit(txn));
  CHECK_FALSE(lfdb::key_exists(j, side_effect_key));
 }

 SECTION("marked write conflict invalidates overlapping read transaction") {
  const auto guard_key = make_key(0, "conflict-write-key");
  const auto side_effect_key = make_key(1, "conflict-write-key");

  lfdb::set(j, guard_key, "old");

  auto read_txn = lfdb::make_transaction(j);
  std::string value;

  REQUIRE(lfdb::get(read_txn, guard_key, value));

  auto write_txn = lfdb::make_transaction(j);

  lfdb::mark_conflict_write(write_txn, guard_key);
  REQUIRE(lfdb::commit(write_txn));

  lfdb::set(read_txn, side_effect_key, "side-effect");

  CHECK_FALSE(lfdb::commit(read_txn));
  CHECK_FALSE(lfdb::key_exists(j, side_effect_key));
 }

 SECTION("selector and query overloads normalize to conflict ranges") {
  constexpr auto prefix = "conflict-selector";
  const auto begin = make_key(0, prefix);
  const auto middle = make_key(1, prefix);
  const auto end = make_key(2, prefix);
  const auto side_effect_key = make_key(99, prefix);

  lfdb::set(j, middle, "old");

  auto selector_txn = lfdb::make_transaction(j);

  lfdb::mark_conflict_read(selector_txn, lfdb::select { begin, end });
  lfdb::set(j, middle, "new");
  lfdb::set(selector_txn, side_effect_key, "side-effect");

  CHECK_FALSE(lfdb::commit(selector_txn));
  CHECK_FALSE(lfdb::key_exists(j, side_effect_key));

  auto query_txn = lfdb::make_transaction(j);

  lfdb::mark_conflict_read(query_txn, lfdb::query::prefix(make_key_prefix(prefix)));
  lfdb::set(j, middle, "newer");
  lfdb::set(query_txn, side_effect_key, "query-side-effect");

  CHECK_FALSE(lfdb::commit(query_txn));
  CHECK_FALSE(lfdb::key_exists(j, side_effect_key));
 }

 SECTION("content keys can mark exact-key conflicts") {
  const auto object = content::keyspace(test_namespace_prefix()) / "conflict" / "object";
  const auto side_effect_key = make_key(0, "conflict-content");

  lfdb::set(j, object, "old");

  auto txn = lfdb::make_transaction(j);

  lfdb::mark_conflict_read(txn, object);

  lfdb::set(j, object, "new");
  lfdb::set(txn, side_effect_key, "side-effect");

  CHECK_FALSE(lfdb::commit(txn));
  CHECK_FALSE(lfdb::key_exists(j, side_effect_key));
 }

 SECTION("empty conflict ranges are rejected") {
  const auto key = make_key(0, "conflict-empty");

  auto txn = lfdb::make_transaction(j);

  CHECK_THROWS_MATCHES(lfdb::mark_conflict_read(txn, lfdb::select { key, key }),
                       std::invalid_argument,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring(
                         "conflict expression must contain a non-empty range")));

  CHECK_THROWS_MATCHES(lfdb::mark_conflict_write(txn, lfdb::query::prefix(std::string("\xFF", 1))),
                       std::invalid_argument,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring(
                         "conflict expression must contain a non-empty range")));
 }
}

TEST_CASE("atomic mutations", "[fdb]") {
 janitor j;

 SECTION("add mutates little-endian integer values") {
  const auto key = test_key("atomic/add");

  lfdb::atomic::add(j, key, std::uint64_t{5});

  auto txn = lfdb::make_transaction(j);

  lfdb::atomic::add(txn, key, std::int64_t{-2});
  REQUIRE(lfdb::commit(txn));

  const auto value = read_raw_fdb_value(j, key);

  REQUIRE(sizeof(std::uint64_t) == std::size(value));
  CHECK(3 == decode_little_endian<std::uint64_t>(value));
 }

 SECTION("add does not require a read conflict") {
  const auto key = test_key("atomic/add/concurrent");

  auto first_txn = lfdb::make_transaction(j);
  auto second_txn = lfdb::make_transaction(j);

  lfdb::atomic::add(first_txn, key, std::uint64_t{1});
  lfdb::atomic::add(second_txn, key, std::uint64_t{1});

  CHECK(lfdb::commit(first_txn));
  CHECK(lfdb::commit(second_txn));

  const auto value = read_raw_fdb_value(j, key);

  REQUIRE(sizeof(std::uint64_t) == std::size(value));
  CHECK(2 == decode_little_endian<std::uint64_t>(value));
 }

 SECTION("min and max use unsigned little-endian parameters") {
  const auto key = test_key("atomic/min-max");

  lfdb::atomic::max(j, key, std::uint64_t{10});
  lfdb::atomic::max(j, key, std::uint64_t{7});
  lfdb::atomic::max(j, key, std::uint64_t{12});
  lfdb::atomic::min(j, key, std::uint64_t{20});
  lfdb::atomic::min(j, key, std::uint64_t{4});

  const auto value = read_raw_fdb_value(j, key);

  REQUIRE(sizeof(std::uint64_t) == std::size(value));
  CHECK(4 == decode_little_endian<std::uint64_t>(value));
 }

 SECTION("bit operations mutate integer bytes") {
  const auto key = test_key("atomic/bit");

  lfdb::atomic::bit_or(j, key, std::uint64_t{0x0F});
  lfdb::atomic::bit_xor(j, key, std::uint64_t{0x03});
  lfdb::atomic::bit_and(j, key, std::uint64_t{0x0A});

  const auto value = read_raw_fdb_value(j, key);

  REQUIRE(sizeof(std::uint64_t) == std::size(value));
  CHECK(0x08 == decode_little_endian<std::uint64_t>(value));
 }

 SECTION("byte operations use lexicographic raw values") {
  const auto key = test_key("atomic/byte-min-max");

  const auto minimum = std::array<std::uint8_t, 1>{'b'};
  const auto maximum = std::vector<std::uint8_t>{'c'};

  write_raw_fdb_value(j, key, raw_bytes("m"));

  lfdb::atomic::byte_min(j, key, minimum);
  CHECK(std::vector<std::uint8_t>{'b'} == read_raw_fdb_value(j, key));

  lfdb::atomic::byte_max(j, key, maximum);
  CHECK(std::vector<std::uint8_t>{'c'} == read_raw_fdb_value(j, key));
 }

 SECTION("append_if_fits appends raw bytes") {
  const auto key = test_key("atomic/append-if-fits");

  write_raw_fdb_value(j, key, raw_bytes("cache"));
  lfdb::atomic::append_if_fits(j, key, "-block");

  CHECK(std::vector<std::uint8_t>{'c', 'a', 'c', 'h', 'e', '-', 'b', 'l', 'o', 'c', 'k'} ==
        read_raw_fdb_value(j, key));
 }

 SECTION("compare_and_clear clears only matching raw bytes") {
  const auto key = test_key("atomic/compare-and-clear");

  write_raw_fdb_value(j, key, raw_bytes("present"));
  lfdb::atomic::compare_and_clear(j, key, "missing");
  CHECK(lfdb::key_exists(j, key));

  lfdb::atomic::compare_and_clear(j, key, "present");
  CHECK_FALSE(lfdb::key_exists(j, key));
 }
}

TEST_CASE("generators honor selector endpoints", "[fdb]") {
 janitor j;

 constexpr auto prefix = "generator-selector";
 write_monotonic_kvs(j, 10, prefix);

 auto collect_pair_keys = [&j](lfdb::select selector) {
  auto txn = lfdb::make_transaction(j);
  return lfdb::pair_generator(txn, selector)
       | std::views::transform([](const auto& kv) { return kv.first; })
       | std::ranges::to<std::vector<std::string>>();
 };

 auto collect_block_keys = [&j](lfdb::select selector) {
  return lfdb::block_generator(j, selector)
       | std::views::join
       | std::views::transform([](const auto& kv) { return kv.first; })
       | std::ranges::to<std::vector<std::string>>();
 };

 auto selector = lfdb::select { lfdb::exclusive(make_key(3, prefix)), lfdb::inclusive(make_key(6, prefix)) };
 selector.options.result_limit = 2;

 const auto forward_keys = std::vector { make_key(4, prefix), make_key(5, prefix), make_key(6, prefix) };
 CHECK_THAT(collect_pair_keys(selector), Catch::Matchers::RangeEquals(forward_keys));
 CHECK_THAT(collect_block_keys(selector), Catch::Matchers::RangeEquals(forward_keys));

 selector.options.reverse_order = true;

 const auto reverse_keys = std::vector { make_key(6, prefix), make_key(5, prefix), make_key(4, prefix) };
 CHECK_THAT(collect_pair_keys(selector), Catch::Matchers::RangeEquals(reverse_keys));
 CHECK_THAT(collect_block_keys(selector), Catch::Matchers::RangeEquals(reverse_keys));
}

TEMPLATE_PRODUCT_TEST_CASE("associative data", "[fdb][rgw]",
(std::map, std::unordered_map, boost::container::flat_map), ((std::string, std::string)))
{
 janitor j;

 TestType kvs{
      { "hello", "world" },
      { "lorem", "ipsum" },
      { "perl", "camel" },
      { "pearl", pearl_msg }
    };

 // From the "database" point of view, the structure is now that we have a single 
 // key pointing (p) to an associative array, e.g. map<p, map<k, v>>:
 const auto key = test_key("key");
 lfdb::set(lfdb::make_transaction(j), key, kvs, lfdb::commit_after_op::commit);

 TestType out_kvs;

 lfdb::get(lfdb::make_transaction(j), key, out_kvs, lfdb::commit_after_op::no_commit);

 CHECK(pearl_msg == out_kvs["pearl"]);
}

SCENARIO("implicit transactions", "[fdb][rgw]")
{
 janitor j;

 const auto k = test_key("hi");
 std::string_view v = "there";

 CAPTURE(k);   
 CAPTURE(v);   

 SECTION("implicitly create and complete transactions") {

  REQUIRE_FALSE(lfdb::key_exists(j, k));
  CHECK_NOTHROW(lfdb::set(j, k, v));
  CHECK(lfdb::key_exists(j, k));

  std::string ov;
  CHECK(lfdb::get(j, k, ov));

  CAPTURE(ov);   

  REQUIRE(v == ov);

  CHECK_NOTHROW(lfdb::erase(j, k));
  REQUIRE_FALSE(lfdb::key_exists(j, k));

  REQUIRE_FALSE(lfdb::get(j, k, ov));
 }

 SECTION("implicitly create and complete transactions-- selection operations") {
  // With an implicit transaction, mutating transactions should commit by default:
  const auto selector = lfdb::select { make_key(0), make_key(20) };

  const auto kvs = write_monotonic_kvs(j, 20);

  lfdb::erase(j, lfdb::select { make_key(1), make_key(6) });

  CHECK(15 == key_count(j, selector));

  // Let's look around the edge cases of the selection:   
  CHECK_FALSE(lfdb::key_exists(j, make_key(1)));
  CHECK_FALSE(lfdb::key_exists(j, make_key(5)));

  CHECK(lfdb::key_exists(j, make_key(0)));
  CHECK(lfdb::key_exists(j, make_key(6)));
 }

 SECTION("test behavior with shared transaction") {
    SECTION("write in uncommitted transaction") {
      using lfdb::commit_after_op;
    
      auto txn = lfdb::make_transaction(j);
    
      const auto herman = test_key("Herman");
      const auto john = test_key("John");

      lfdb::set(txn, herman, "Hollerith", commit_after_op::no_commit);
     
      // Key exists with respect to this transaction: 
      CHECK(lfdb::key_exists(txn, herman));
      
      lfdb::set(txn, john, "Backus", commit_after_op::no_commit);
    
      // Key exists with respect to this transaction: 
      CHECK(lfdb::key_exists(txn, john, commit_after_op::no_commit));
    
      // transaction is abandoned
    }

  // These were only set in the abandoned transaction:
  CHECK_FALSE(lfdb::key_exists(j, test_key("Herman")));
  CHECK_FALSE(lfdb::key_exists(j, test_key("John")));
 }

 SECTION("round trip") {
  janitor scoped_j(j);

  using namespace ceph::libfdb;
  
  const auto key = test_key("key_0000");
  set(j, key, "value");
  std::string out;
  get(j, key, out);
  
  CHECK("value" == out);
 }

 SECTION("round trip with raw string") {
  // The underlying serializer can produce some surprising behavior; libfdb
  // works around this so that the "right" thing to do is what gets done, with
  // performance-maximzation left as an available, but explicit operation.

  janitor scoped_j(j);

  using namespace ceph::libfdb;
 
  // Notice the raw literal going in here: 
  const auto key = test_key("key_0000");
  set(j, key, "value");

  std::string out;
  CHECK_NOTHROW(get(j, key, out));

  CHECK(std::string_view("value") == std::string_view(out));

  // Explicit raw buffers:
  char out_buffer[9] = {}; 
  CHECK_NOTHROW(get(j, key, out_buffer));
  
  CHECK(std::string_view("value") == std::string_view(out));
 }
}

SCENARIO("transactor", "[fdb]")
{
 janitor j;

 SECTION("transaction function returns nothing") {
  auto txr = lfdb::make_transactor(j);
  const auto key = test_key("key");

  txr([&key](auto txn) {
    lfdb::set(txn, key, "value");
  });

  std::string out;
  CHECK(lfdb::get(j, key, out));
  CHECK("value" == out);
 }

 SECTION("transaction function returns value") {
  auto txr = lfdb::make_transactor(j);
  const auto key = test_key("key");

  auto [found, out] = txr([&key](auto txn) {
    lfdb::set(txn, key, "value");

    std::string out;
    auto found = lfdb::get(txn, key, out);

    return std::pair(found, out);
  });

  CHECK(found);
  CHECK("value" == out);
 }

 SECTION("construct with transaction options") {
  lfdb::transaction_options opts {
    { FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag }
  };

  auto txr = lfdb::make_transactor(j, opts);
  const auto key = test_key("key");

  txr([&key](auto txn) {
    lfdb::set(txn, key, "value");
  });

  std::string out;
  CHECK(lfdb::get(j, key, out));
  CHECK("value" == out);
 }

 SECTION("transactor replays after conflict") {
  auto txr = lfdb::make_transactor(j);
  const auto key = test_key("key");

  lfdb::set(j, key, "initial");

  txr([&j, &key](auto txn) {
    std::string out;
    if (not lfdb::get(txn, key, out)) {
     throw std::runtime_error("expected key does not exist");
    }

    // Force a conflict, making the transactor replay the body:
    if ("initial" == out) {
     lfdb::set(j, key, "conflict");
    }

    lfdb::set(txn, key, "final");
  });

  std::string out;
  CHECK(lfdb::get(j, key, out));
  CHECK("final" == out);
 }

 SECTION("transactor propagates transaction body exceptions") {
  auto txr = lfdb::make_transactor(j);

  CHECK_THROWS_WITH(txr([](auto) {
    throw std::runtime_error("transaction body failed");
  }), "transaction body failed");
 }
}

SCENARIO("options", "[fdb]")
{
 // For information about options, consult the FoundationDB's source tree's
 // documentation: fdbclient/vexillographer/fdb.options
 SECTION("option types") {

  // check that the types supported for FDB options are supported by
  // the library:
  lfdb::option_value ov;
  ov = lfdb::option_flag;                 // flag
  ov = 42;                                // integer
  ov = std::string("hi");                 // string
  ov = std::vector<std::uint8_t>(         // data
        (const std::uint8_t *)pearl_msg, 
        (const std::uint8_t *)(pearl_msg + sizeof(pearl_msg)));
 }

  auto dbh0 = lfdb::create_database(
                { { FDB_DB_OPTION_LOCATION_CACHE_SIZE, 200'000 } },  
                { { FDB_NET_OPTION_TRACE_ENABLE, lfdb::option_flag } });         

  auto dbh1 = lfdb::create_database("fishing for databass!",             // name
                { { FDB_DB_OPTION_LOCATION_CACHE_SIZE, 200'000 } },      // database options
                { { FDB_NET_OPTION_TRACE_ENABLE, lfdb::option_flag } }); // network options
 
  auto txn = lfdb::make_transaction(dbh0, 
               { { FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag } });

 SECTION("create_database()") {
  lfdb::create_database();
  lfdb::create_database("");
  lfdb::create_database("", {}, {});
  lfdb::create_database(lfdb::database_options {}, lfdb::network_options {});
 }

 SECTION("piecemeal construction") {
  lfdb::network_options netopts;

  // Note that, according to FDB's documentation, this setting's actually deprecated:
  netopts[FDB_NET_OPTION_LOCAL_ADDRESS] = "127.0.0.1:2323"; 

  // The cluster file is in "/etc/foundationdb.fdb.cluster" normally, but we'll point to 
  // nowhere just for fun. The cluster file is the "approved" way to establish a list of
  // addressess, AFAIK, rather than setting the option:
  lfdb::create_database("/dev/null", {}, netopts);
 }
}

TEST_CASE("mini-demo", "[fdb]") {
 janitor j;

 using std::map;
 using std::string;

 map<string, string> bucket_entries = {
    { "objName", "obj" },
    { "bucketName", "bucket" },
    { "creationTime", "2025-11-12T10:00:00" },
    { "dirty", "0" },
    { "hosts", "192.168.1.1:8000_192.168.1.2:8000" },
    { "etag", "abc123def" },
    { "objSize", "1048576" },
    { "userId", "user123" },
    { "displayName", "John Doe" }
  };
 
 // This write will make and commit its own transaction:
 const auto key = test_key("bucket_obj");
 lfdb::set(j, key, bucket_entries);

 map<string, string> out;
 lfdb::get(j, key, out);

 // For "demo" purposes, you can ignore everything below here:
 CAPTURE(out["userId"]);
 REQUIRE(bucket_entries == out);

 j.drop_test_namespace();
}

TEST_CASE("block_generator should correctly handle value types") {

 SECTION("wrangle the unwrangled!") {
  janitor dbh;

  using person = std::map<std::string, std::string>; // person => things about a person

  auto check_generators = [&dbh]<typename ValueT>(std::string_view prefix,
                                                  const std::map<std::string, ValueT>& values) {
    auto key_for = [prefix](std::string_view name) {
      return test_key(fmt::format("{}/{}", prefix, name));
    };

    lfdb::make_transactor(dbh)([&values, &key_for](auto txn) {
      std::ranges::for_each(values, [&txn, &key_for](const auto& individual) {
        lfdb::set(txn, key_for(individual.first), individual.second);
      });
    });

    const std::map<std::string, ValueT> expected {
      { key_for("Alice"), values.at("Alice") },
      { key_for("Bob"), values.at("Bob") }
    };

    auto selector = lfdb::select { key_for("A"), key_for("C") };

    std::map<std::string, ValueT> from_pairs;
    auto txn = lfdb::make_transaction(dbh);
    std::ranges::copy(lfdb::pair_generator<ValueT>(txn, selector),
                      std::inserter(from_pairs, std::end(from_pairs)));

    REQUIRE(from_pairs == expected);

    const auto from_transaction_collect =
     lfdb::collect<ValueT, std::map<std::string, ValueT>>(txn, selector);

    REQUIRE(from_transaction_collect == expected);

    std::map<std::string, ValueT> from_blocks;
    for (auto&& block : lfdb::blocks<ValueT>(dbh, selector)) {
      from_blocks.insert(std::begin(block), std::end(block));
    }

    REQUIRE(from_blocks == expected);
    REQUIRE(from_blocks == from_pairs);

    const auto from_database_collect =
     lfdb::collect<ValueT, std::map<std::string, ValueT>>(dbh, selector);

    REQUIRE(from_database_collect == expected);
  };

  check_generators("generator-values/string",
                   std::map<std::string, std::string> {
                     { "Alice", "boysenberry" },
                     { "Bob", "coconut" },
                     { "X", "coconut" },
                     { "Y", "coconut" }
                   });

  check_generators("generator-values/bytes",
                   std::map<std::string, std::vector<std::uint8_t>> {
                     { "Alice", { 1, 2, 3 } },
                     { "Bob", { 4, 5, 6 } },
                     { "X", { 7, 8, 9 } },
                     { "Y", { 10, 11, 12 } }
                   });

  check_generators("generator-values/person",
                   std::map<std::string, person> {
                     { "Alice", {
                       { "name", "Alice" },
                       { "ice_cream", "boysenberry" }
                     } },
                     { "Bob", {
                       { "name", "Bob" },
                       { "ice_cream", "coconut" }
                     } },
                     { "X", {
                       { "name", "X" },
                       { "ice_cream", "coconut" }
                     } },
                     { "Y", {
                       { "name", "Y" },
                       { "ice_cream", "coconut" }
                     } }
                   });
 }
}


// Adapted from Catch2 documentation:
#include <catch2/catch_session.hpp>

int main(int argc, char **argv) 
{
  int result = Catch::Session().run(argc, argv);

  // Make sure that FoundationDB is shut down once and only once:
  ceph::libfdb::shutdown_libfdb(); 

  return result;
}
