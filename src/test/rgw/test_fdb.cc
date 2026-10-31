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

#include <catch2/benchmark/catch_benchmark.hpp>

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

#include <map>
#include <list>
#include <array>
#include <deque>
#include <vector>
#include <unordered_map>

#include <ranges>
#include <iterator>
#include <algorithm>

#include <atomic>
#include <chrono>
#include <thread>

#include <limits>
#include <memory>
#include <cstdint>
#include <sstream>
#include <utility>
#include <compare>
#include <concepts>
#include <exception>
#include <filesystem>
#include <limits>
#include <stdexcept>

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

template <typename SourceT>
concept can_create_database_from = requires(SourceT source) {
 lfdb::create_database(source);
};

static_assert(std::constructible_from<lfdb::connection_source, std::filesystem::path>);
static_assert(std::constructible_from<lfdb::connection_source, std::string_view>);
static_assert(not std::is_convertible_v<std::filesystem::path, lfdb::connection_source>);
static_assert(not std::is_convertible_v<std::string_view, lfdb::connection_source>);
static_assert(not std::constructible_from<lfdb::connection_source, std::nullptr_t>);
static_assert(not can_create_database_from<std::filesystem::path>);
static_assert(not can_create_database_from<std::string_view>);
static_assert(not can_create_database_from<std::string>);
static_assert(not can_create_database_from<const char *>);

template <typename ValueT>
concept can_atomic_add =
 requires(lfdb::database_handle dbh, std::string key, ValueT value) {
  lfdb::atomic::add(dbh, key, value);
 };

template <typename ValueT>
concept can_get_as_fdb_integer =
 requires(lfdb::database_handle dbh, std::string key, ValueT& value) {
  { lfdb::get(dbh, key, lfdb::as_fdb_integer(value)) } -> std::same_as<bool>;
 };

template <typename ValueT>
concept can_atomic_min =
 requires(lfdb::database_handle dbh, std::string key, ValueT value) {
  lfdb::atomic::min(dbh, key, value);
 };

template <typename FnT>
concept can_lfdb_for_each =
 requires(lfdb::transaction_handle txn, lfdb::select selection, FnT fn) {
  lfdb::for_each(txn, selection, fn);
 };

template <typename FnT>
concept can_lfdb_get_raw_value =
 requires(lfdb::database_handle dbh, std::string key, FnT fn) {
  lfdb::get(dbh, key, fn);
 };

template <typename FnT>
concept can_lfdb_from_convert_output =
 requires(std::span<const std::uint8_t> bytes, FnT fn) {
  lfdb::from::convert(bytes, fn);
 };

template <typename FnT>
concept can_lfdb_watched_loop =
 requires(lfdb::database_handle dbh, std::string key, FnT fn) {
  lfdb::watched_loop(dbh, key, fn);
 };

struct pair_identity final
{
 const string_pair& operator()(const string_pair& kv) const noexcept
 {
  return kv;
 }
};

struct rvalue_string_view_only final
{
 operator std::string_view() && { return {}; }
};

struct move_only_invocation_result final
{
 move_only_invocation_result() = default;
 move_only_invocation_result(move_only_invocation_result&&) = default;
 move_only_invocation_result(const move_only_invocation_result&) = delete;
};

struct immovable_invocation_result final
{
 immovable_invocation_result() = default;
 immovable_invocation_result(immovable_invocation_result&&) = delete;
 immovable_invocation_result(const immovable_invocation_result&) = delete;
};

struct move_only_transaction_argument final
{
 move_only_transaction_argument() = default;
 move_only_transaction_argument(move_only_transaction_argument&&) = default;
 move_only_transaction_argument(const move_only_transaction_argument&) = delete;
};

struct transaction_with_move_only_argument final
{
 void operator()(lfdb::transaction_handle&, move_only_transaction_argument&) const {}
};

struct reference_returning_transaction final
{
 int& operator()(lfdb::transaction_handle&) const;
};

struct immovable_string_pair_output final
{
 std::vector<string_pair> values;

 immovable_string_pair_output() = default;
 immovable_string_pair_output(immovable_string_pair_output&&) = delete;
 immovable_string_pair_output(const immovable_string_pair_output&) = delete;

 auto begin() { return std::begin(values); }
 auto end() { return std::end(values); }

 void push_back(string_pair value) { values.push_back(std::move(value)); }
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

 STATIC_REQUIRE(std::movable<lfdb::detail::future_value>);
 STATIC_REQUIRE_FALSE(std::copyable<lfdb::detail::future_value>);

 STATIC_REQUIRE(lfdb::concepts::key_value_iterator<string_pair_vector::iterator>);
 STATIC_REQUIRE(lfdb::concepts::key_value_range<string_pair_vector>);
 STATIC_REQUIRE(lfdb::concepts::key_value_forward_range<string_pair_vector>);
 STATIC_REQUIRE(lfdb::concepts::key_value_forward_range<transformed_pair_range>);
 STATIC_REQUIRE_FALSE(lfdb::concepts::key_value_iterator<bad_key_vector::iterator>);

 STATIC_REQUIRE(lfdb::concepts::string_pair_output_range<string_pair_vector>);
 STATIC_REQUIRE(lfdb::concepts::string_pair_output_range<std::map<std::string, std::string>>);
 STATIC_REQUIRE(lfdb::concepts::string_pair_output_range<immovable_string_pair_output>);
 STATIC_REQUIRE_FALSE(
  lfdb::concepts::materializable_string_pair_output_range<immovable_string_pair_output>);

 STATIC_REQUIRE(lfdb::concepts::stringview_convertible<std::string>);
 STATIC_REQUIRE(lfdb::concepts::stringview_convertible<const char[4]>);
 STATIC_REQUIRE_FALSE(lfdb::concepts::stringview_convertible<rvalue_string_view_only>);

 STATIC_REQUIRE(lfdb::concepts::decoded_value_sink<std::string&>);
 STATIC_REQUIRE(lfdb::concepts::decoded_value_sink<char(&)[9]>);
 STATIC_REQUIRE_FALSE(lfdb::concepts::decoded_value_sink<const std::string&>);
 STATIC_REQUIRE_FALSE(lfdb::concepts::decoded_value_sink<std::string>);
 STATIC_REQUIRE(lfdb::concepts::value_callback<decltype([](std::span<const std::uint8_t>) {})>);
 STATIC_REQUIRE_FALSE(lfdb::concepts::value_callback<decltype([](std::span<const std::uint8_t>) {
  return true;
 })>);
 STATIC_REQUIRE(lfdb::concepts::raw_key_value_callback<
  decltype([](std::span<const std::uint8_t>, std::span<const std::uint8_t>) {})>);
 STATIC_REQUIRE_FALSE(lfdb::concepts::raw_key_value_callback<
  decltype([](std::span<const std::uint8_t>, std::span<const std::uint8_t>) {
   return true;
  })>);
 STATIC_REQUIRE_FALSE(lfdb::concepts::decoded_value_sink<decltype([](std::span<const std::uint8_t>) {
  return true;
 })&>);

 using void_transaction = decltype([](lfdb::transaction_handle&) {});
 using value_transaction = decltype([](lfdb::transaction_handle&) {
  return move_only_invocation_result {};
 });
 using immovable_transaction = decltype([](lfdb::transaction_handle&) {
  return immovable_invocation_result {};
 });

 STATIC_REQUIRE(lfdb::detail::transaction_op<void_transaction>);
 STATIC_REQUIRE(lfdb::detail::transaction_op<value_transaction>);
 STATIC_REQUIRE_FALSE(lfdb::detail::transaction_op<immovable_transaction>);
 STATIC_REQUIRE_FALSE(lfdb::detail::transaction_op<reference_returning_transaction>);
 STATIC_REQUIRE((lfdb::detail::bound_transaction_op<
                 transaction_with_move_only_argument,
                 move_only_transaction_argument>));
 STATIC_REQUIRE_FALSE((lfdb::detail::bound_transaction_op<
                       transaction_with_move_only_argument,
                       move_only_transaction_argument&>));

 STATIC_REQUIRE(can_atomic_add<std::uint64_t>);
 STATIC_REQUIRE_FALSE(can_atomic_add<bool>);
 STATIC_REQUIRE(can_get_as_fdb_integer<std::uint64_t>);
 STATIC_REQUIRE_FALSE(can_get_as_fdb_integer<bool>);
 STATIC_REQUIRE_FALSE(can_atomic_min<std::int64_t>);

 STATIC_REQUIRE(can_lfdb_for_each<decltype([](string_pair&&) {})>);
 STATIC_REQUIRE_FALSE(can_lfdb_for_each<decltype([](string_pair&&) { return 1; })>);

 STATIC_REQUIRE(lfdb::concepts::value_callback<decltype([](std::span<const std::uint8_t>) {})>);
 STATIC_REQUIRE_FALSE(lfdb::concepts::value_callback<decltype([](std::span<const std::uint8_t>) {
  return 1;
 })>);
 STATIC_REQUIRE(can_lfdb_get_raw_value<decltype([](std::span<const std::uint8_t>) {})>);
 STATIC_REQUIRE_FALSE(can_lfdb_get_raw_value<decltype([](std::span<const std::uint8_t>) {
  return 1;
 })>);
 STATIC_REQUIRE(can_lfdb_from_convert_output<decltype([](const char *, std::size_t) {})>);
 STATIC_REQUIRE_FALSE(can_lfdb_from_convert_output<decltype([](const char *, std::size_t) {
  return 1;
 })>);

 STATIC_REQUIRE(can_lfdb_watched_loop<decltype([](std::string_view) {})>);
 STATIC_REQUIRE_FALSE(can_lfdb_watched_loop<decltype([](std::string_view) { return true; })>);
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

TEST_CASE("key selectors navigate stored keys", "[fdb][query]")
{
 janitor j;
 const auto prefix = make_key_prefix("selector");

 const auto key0 = prefix + "0";
 const auto key1 = prefix + "1";
 const auto key2 = prefix + "2";
 const auto key3 = prefix + "3";
 const auto key4 = prefix + "4";
 const auto key2_5 = prefix + "2.5";

 lfdb::set(j, key0, "zero");
 lfdb::set(j, key1, "one");
 lfdb::set(j, key2, "two");
 lfdb::set(j, key3, "three");
 lfdb::set(j, key4, "four");

 CHECK(key1 == lfdb::get_key(j, lfdb::lower(key2)));
 CHECK(key2 == lfdb::get_key(j, lfdb::floor(key2)));
 CHECK(key2 == lfdb::get_key(j, lfdb::ceiling(key2)));
 CHECK(key3 == lfdb::get_key(j, lfdb::higher(key2)));
 CHECK(key2 == lfdb::get_key(j, lfdb::ceiling(key2), lfdb::read_mode::snapshot));

 CHECK(key2 == lfdb::get_key(j, lfdb::lower(key2_5)));
 CHECK(key2 == lfdb::get_key(j, lfdb::floor(key2_5)));
 CHECK(key3 == lfdb::get_key(j, lfdb::ceiling(key2_5)));
 CHECK(key3 == lfdb::get_key(j, lfdb::higher(key2_5)));
}

TEST_CASE("key selectors compose with content keys and query algebra", "[fdb][query][content]")
{
 namespace fdbc = ceph::libfdb::layer::content;
 namespace lq = ceph::libfdb::query;

 janitor j;
 const auto root = test_key("content-selector");
 const auto objects = fdbc::keyspace(root) / "objects";
 const auto object_a = objects / "a";
 const auto object_c = objects / "c";
 const auto object_b = objects / "b";

 lfdb::set(j, object_a, "alpha");
 lfdb::set(j, object_c, "charlie");

 const auto object_range = fdbc::prefix(objects);
 const auto next_object = lfdb::get_key(j, lfdb::ceiling(object_b));

 CHECK(lq::contains(object_range, next_object));
 CHECK(std::string(libfdb_key_view(object_c)) == next_object);
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
 const auto key_bytes = lfdb::detail::as_fdb_bytes(key);
 const auto value_bytes = lfdb::detail::as_fdb_bytes(value);

 fdb_transaction_set(txn->raw_handle(),
                     key_bytes.data, key_bytes.length,
                     value_bytes.data, value_bytes.length);

 REQUIRE_NOTHROW(lfdb::commit(txn));
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

TEST_CASE("error codes retain categories, context, and native values", "[fdb][errors]")
{
 STATIC_REQUIRE(std::derived_from<lfdb::libfdb_exception, std::system_error>);
 STATIC_REQUIRE(not std::constructible_from<lfdb::libfdb_exception, int>);
 STATIC_REQUIRE(std::convertible_to<lfdb::fdb_error_code, std::error_code>);
 STATIC_REQUIRE(std::convertible_to<lfdb::libfdb_error_code, std::error_code>);

 CHECK("FoundationDB"sv == lfdb::fdb_error_category().name());
 CHECK("libfdb"sv == lfdb::libfdb_error_category().name());
 CHECK(lfdb::fdb_error_category() != lfdb::libfdb_error_category());

 for (const int value : {0, -1, 1020, 1021, 123456789,
                         std::numeric_limits<int>::min(),
                         std::numeric_limits<int>::max()}) {
  const std::error_code code = static_cast<lfdb::fdb_error_code>(value);
  CHECK(value == code.value());
  CHECK(lfdb::fdb_error_category() == code.category());
 }

 const std::error_code code = lfdb::fdb_error_code::not_committed;
 const lfdb::libfdb_exception native {code, "loading test data"};
 const auto copied = native;
 CHECK(code == copied.code());
 CHECK(native.retryable());
 CHECK(lfdb::retryable(native));
 CHECK_THAT(native.what(), Catch::Matchers::ContainsSubstring("1020"));
 CHECK_THAT(native.what(), Catch::Matchers::ContainsSubstring(fdb_get_error(1020)));
 CHECK_THAT(native.what(), Catch::Matchers::ContainsSubstring("loading test data"));

 for (const auto& local : {lfdb::libfdb_exception {"local context"},
                           lfdb::libfdb_exception {std::string("local context")},
                           lfdb::libfdb_exception {"local context"sv}}) {
  CHECK(local.code());
  CHECK(lfdb::libfdb_error_code::operation_failed == local.code());
  CHECK_FALSE(local.retryable());
  CHECK_THAT(local.what(), Catch::Matchers::ContainsSubstring("local context"));
 }

 const lfdb::libfdb_exception malformed {lfdb::libfdb_error_code::invalid_result};
 CHECK(lfdb::libfdb_error_code::invalid_result == malformed.code());
 const lfdb::libfdb_exception shutdown {lfdb::libfdb_error_code::client_shutdown};
 CHECK(lfdb::libfdb_error_code::client_shutdown == shutdown.code());

 try {
  throw native;
 } catch (const std::system_error& error) {
  CHECK(code == error.code());
 }
}

TEST_CASE("library generated failures share an exception type with useful codes", "[fdb][errors]")
{
 const auto verify = [](auto&& operation, const std::errc code, const std::string_view context) {
  try {
   std::invoke(operation);
   FAIL("expected a library-generated error");
  } catch (const lfdb::libfdb_exception& error) {
   CHECK(error.code() == code);
   CHECK(error.code().category() == std::generic_category());
   CHECK_FALSE(error.retryable());
   CHECK_THAT(error.what(), Catch::Matchers::ContainsSubstring(std::string(context)));
  }
 };
 verify([] { std::ignore = lfdb::commit(lfdb::with_result, {}); },
        std::errc::invalid_argument, "requires a transaction");
 verify([] { std::ignore = lfdb::connection_source {std::filesystem::path {}}; },
        std::errc::invalid_argument, "non-empty cluster file path");
 verify([] { lfdb::versionstamp stamp; std::ignore = stamp.resolved_bytes(); },
        std::errc::invalid_argument, "unresolved version stamp");
 verify([] { std::ignore = content::key(""); },
        std::errc::invalid_argument, "keyspace root");
 verify([] { std::ignore = lfdb::query::compile_intervals<0>(lfdb::query::singleton("a")); },
        std::errc::value_too_large, "capacity exceeded");
 const lfdb::libfdb_exception local {std::errc::invalid_argument, "local context"sv};
 CHECK(local.code() == std::errc::invalid_argument);
 CHECK_THAT(local.what(), Catch::Matchers::ContainsSubstring("local context"));
}

TEST_CASE("error predicates classify only native category codes", "[fdb][errors]")
{
 const auto value = GENERATE(0, 1020, 1021, 1101, 2000, 9999);
 const std::error_code native(value, lfdb::fdb_error_category());

 CHECK(static_cast<bool>(fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, value)) ==
       lfdb::retryable(native));
 CHECK(static_cast<bool>(fdb_error_predicate(FDB_ERROR_PREDICATE_MAYBE_COMMITTED, value)) ==
       lfdb::maybe_committed(native));
 CHECK(static_cast<bool>(fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED, value)) ==
       lfdb::retryable_not_committed(native));

 for (const auto *category : {&std::generic_category(), &std::system_category(),
                              &lfdb::libfdb_error_category()}) {
  const std::error_code foreign(value, *category);
  CHECK_FALSE(lfdb::retryable(foreign));
  CHECK_FALSE(lfdb::maybe_committed(foreign));
  CHECK_FALSE(lfdb::retryable_not_committed(foreign));
 }
}

namespace {

struct returned_serialization_status final
{
 inline static std::errc status = std::errc::permission_denied;

 static zpp::bits::errc serialize(auto&, auto&)
 {
  return status;
 }

};

struct thrown_serialization_failure final
{
 inline static std::exception_ptr cause;
 inline static int calls = 0;

 static zpp::bits::errc serialize(auto&, auto&)
 {
  ++calls;
  std::rethrow_exception(cause);
 }

};

} // anonymous namespace

TEST_CASE("returned serializer failures retain their standard codes", "[fdb][errors][conversion]")
{
 returned_serialization_status::status =
  GENERATE(std::errc::permission_denied, static_cast<std::errc>(1020));
 const auto expected = std::make_error_code(returned_serialization_status::status);
 const std::array<std::uint8_t, 1> bytes {0};
 returned_serialization_status value;

 const auto check_failure = [&expected](auto&& operation) {
  try {
   std::invoke(operation);
   FAIL("expected returned serialization failure");
  } catch (const lfdb::libfdb_exception& error) {
   CHECK(expected == error.code());
   CHECK_FALSE(error.retryable());
   CHECK_THAT(error.what(), Catch::Matchers::ContainsSubstring("unable to"));
  }
 };

 SECTION("direct encoding") {
  check_failure([&] { std::ignore = lfdb::to::convert(value); });
 }

 SECTION("direct decoding") {
  check_failure([&] { lfdb::from::convert(bytes, value); });
 }

 SECTION("managed point read") {
  janitor dbh;
  const auto key = test_key("serializer-returned-point");
  write_raw_fdb_value(dbh, key, bytes);
  check_failure([&] { std::ignore = lfdb::get(dbh, key, value); });
 }

 SECTION("managed point write") {
  janitor dbh;
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {0}}
  };
  check_failure([&] { lfdb::set(dbh, options, test_key("serializer-returned-write"), value); });
 }

 SECTION("managed bulk write") {
  janitor dbh;
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {0}}
  };
  const std::array records {std::pair {test_key("serializer-returned-bulk-write"), value}};
  check_failure([&] { lfdb::set(dbh, options, records); });
 }

 SECTION("typed scan") {
  janitor dbh;
  const auto key = test_key("serializer-returned-scan");
  write_raw_fdb_value(dbh, key, bytes);
  check_failure([&] {
   std::ignore = lfdb::collect<returned_serialization_status>(dbh, lfdb::select {key});
  });
 }

 SECTION("managed write") {
  janitor dbh;
  check_failure([&] { lfdb::set(dbh, test_key("serializer-returned-write"), value); });
 }
}

TEST_CASE("thrown serializer exceptions retain their identity", "[fdb][errors][conversion]")
{
 const auto source = GENERATE(0, 1, 2, 3, 4);
 if (0 == source) {
  thrown_serialization_failure::cause =
   std::make_exception_ptr(std::runtime_error("serializer sentinel"));
 }
 if (1 == source) {
  thrown_serialization_failure::cause = std::make_exception_ptr(
   std::system_error(std::error_code(1020, std::generic_category()), "serializer sentinel"));
 }
 if (2 == source) {
  thrown_serialization_failure::cause = std::make_exception_ptr(
   lfdb::libfdb_exception(lfdb::fdb_error_code::not_committed, "serializer sentinel"));
 }
 if (3 == source) {
  thrown_serialization_failure::cause =
   std::make_exception_ptr(lfdb::libfdb_exception("local serializer sentinel"));
 }
 if (4 == source) {
  thrown_serialization_failure::cause =
   std::make_exception_ptr(std::invalid_argument("application validation sentinel"));
 }
 thrown_serialization_failure::calls = 0;
 const std::array<std::uint8_t, 1> bytes {0};
 thrown_serialization_failure value;

 const auto check_failure = [](auto&& operation) {
  try {
   std::invoke(operation);
   FAIL("expected thrown serialization failure");
  } catch (...) {
   CHECK(thrown_serialization_failure::cause == std::current_exception());
  }
  CHECK(1 == thrown_serialization_failure::calls);
 };

 SECTION("direct encoding") {
  check_failure([&] { std::ignore = lfdb::to::convert(value); });
 }

 SECTION("direct decoding") {
  check_failure([&] { lfdb::from::convert(bytes, value); });
 }

 SECTION("managed point read") {
  janitor dbh;
  const auto key = test_key("serializer-thrown-point");
  write_raw_fdb_value(dbh, key, bytes);
  check_failure([&] { std::ignore = lfdb::get(dbh, key, value); });
 }

 SECTION("managed point write") {
  janitor dbh;
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {0}}
  };
  const auto key = test_key("serializer-thrown-write");
  check_failure([&] { lfdb::set(dbh, options, key, value); });
  CHECK_FALSE(lfdb::key_exists(dbh, key));
 }

 SECTION("managed bulk write") {
  janitor dbh;
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {0}}
  };
  const auto key = test_key("serializer-thrown-bulk-write");
  const std::array records {std::pair {key, value}};
  check_failure([&] { lfdb::set(dbh, options, records); });
  CHECK_FALSE(lfdb::key_exists(dbh, key));
 }

 SECTION("managed iterator write") {
  janitor dbh;
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {0}}
  };
  const auto key = test_key("serializer-thrown-iterator-write");
  const std::array records {std::pair {key, value}};
  check_failure([&] { lfdb::set(dbh, options, records.begin(), records.end()); });
  CHECK_FALSE(lfdb::key_exists(dbh, key));
 }

 SECTION("managed versioned-key write") {
  janitor dbh;
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {0}}
  };
  lfdb::versionstamp stamp;
  const auto key = lfdb::versioned(test_key("serializer-thrown-versioned/"), stamp);
  check_failure([&] { lfdb::set(dbh, options, key, value); });
  CHECK_FALSE(stamp.is_ready());
 }

 SECTION("managed conditional erase protects decoder failures") {
  const lfdb::database_options defaults {
   {FDB_DB_OPTION_TRANSACTION_TIMEOUT, std::int64_t {5000}},
   {FDB_DB_OPTION_TRANSACTION_RETRY_LIMIT, std::int64_t {0}}
  };
  janitor dbh(lfdb::create_database(defaults));
  const auto key = test_key("serializer-thrown-erase");
  write_raw_fdb_value(dbh, key, bytes);
  check_failure([&] {
   std::ignore = lfdb::erase_if<thrown_serialization_failure>(
     dbh, lfdb::select {key}, [](const auto&) { return true; });
  });
  CHECK(lfdb::key_exists(dbh, key));
 }

 SECTION("typed scan") {
  janitor dbh;
  const auto key = test_key("serializer-thrown-scan");
  write_raw_fdb_value(dbh, key, bytes);
  check_failure([&] {
   std::ignore = lfdb::collect<thrown_serialization_failure>(dbh, lfdb::select {key});
  });
 }
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

static_assert(not can_lfdb_set<lfdb::database_handle,
                               lfdb::transaction_options,
                               lfdb::versioned_bytes,
                               lfdb::versioned_bytes>);

static_assert(std::same_as<void,
 decltype(lfdb::commit(std::declval<lfdb::transaction_handle&>()))>);
static_assert(not std::is_move_constructible_v<lfdb::transaction>);
static_assert(std::same_as<std::expected<void, std::error_code>,
 decltype(lfdb::commit(lfdb::with_result, std::declval<lfdb::transaction_handle&>()))>);

template <typename T>
concept can_borrow_stamp_from_temporary = requires(T&& stamp) {
 std::move(stamp).resolved_bytes();
};

template <typename T>
concept can_borrow_stamp_result_from_temporary = requires(T&& stamp) {
 std::move(stamp).resolved_bytes(lfdb::with_result);
};

static_assert(not can_borrow_stamp_from_temporary<lfdb::versionstamp>);
static_assert(not can_borrow_stamp_result_from_temporary<lfdb::versionstamp>);

TEST_CASE("version stamps", "[fdb]") {
 janitor dbh;

 const lfdb::transaction_options options {
  {FDB_TR_OPTION_PRIORITY_BATCH, lfdb::option_flag}
 };

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

  CHECK_FALSE(stamp.has_value());
  CHECK_FALSE(stamp.is_ready());
  CHECK_THROWS_AS(stamp.resolved_bytes(lfdb::with_result), lfdb::libfdb_exception);
  CHECK_THROWS_MATCHES(stamp.resolved_bytes(),
                       lfdb::libfdb_exception,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring("attempt to access unresolved version stamp")));
 }

 SECTION("invalid versionstamp bytes are an API error") {
  lfdb::versionstamp stamp;
  const std::array<std::uint8_t, 1> invalid_stamp_bytes {};

  CHECK_THROWS_AS(lfdb::from::convert(std::span(invalid_stamp_bytes), stamp),
                  lfdb::libfdb_exception);
 }

 SECTION("moved-from stamps remain safely queryable") {
  lfdb::versionstamp stamp;
  const auto moved = std::move(stamp);
  CHECK_FALSE(stamp.is_ready());
  CHECK_FALSE(stamp.has_value());
  CHECK_FALSE(moved.is_ready());
  CHECK_THROWS_AS(stamp.resolved_bytes(), lfdb::libfdb_exception);
  auto txn = lfdb::make_transaction(dbh);
  CHECK_THROWS_AS(lfdb::set(txn, test_key("versionstamp/moved"), lfdb::versioned("", stamp)),
                  lfdb::libfdb_exception);
 }

 SECTION("parsing identical bytes preserves borrowed storage") {
  const auto bytes = stamp_data(1);
  auto stamp = resolved_stamp(bytes);
  const auto *original = &stamp.resolved_bytes();
  lfdb::from::convert(std::span(bytes), stamp);
  CHECK(original == &stamp.resolved_bytes());
  const auto other = stamp_data(2);
  CHECK_THROWS_AS(lfdb::from::convert(std::span(other), stamp), lfdb::libfdb_exception);
  CHECK(bytes == stamp.resolved_bytes());
 }

 SECTION("commit resolves explicit version stamp") {
  const auto key = test_key("versionstamp/explicit");
  auto txn = lfdb::make_transaction(dbh);
  lfdb::versionstamp stamp;

  lfdb::set(txn, key, "value");
  REQUIRE_NOTHROW(lfdb::commit(txn, stamp));

  CHECK(stamp.has_value());
  CHECK(10 == stamp.resolved_bytes().size());
 }

 SECTION("one versionstamp can resolve multiple operations in one transaction") {
  const auto first_key = test_key("versionstamp/multi/first");
  const auto second_key = test_key("versionstamp/multi/second");
  lfdb::versionstamp stamp;

  auto txn = lfdb::make_transaction(dbh);
  lfdb::set(txn, first_key, lfdb::versioned("", stamp));
  lfdb::set(txn, second_key, lfdb::versioned("", stamp));
  REQUIRE_NOTHROW(lfdb::commit(txn));

  lfdb::versionstamp first;
  lfdb::versionstamp second;
  REQUIRE(lfdb::get(dbh, first_key, first));
  REQUIRE(lfdb::get(dbh, second_key, second));

  REQUIRE(stamp.has_value());
  CHECK(first.resolved_bytes() == stamp.resolved_bytes());
  CHECK(second.resolved_bytes() == stamp.resolved_bytes());
 }

 SECTION("failed commit records a shared stamp failure until explicit reuse") {
  const auto conflict_key = test_key("versionstamp/conflict/read");
  const auto output_key = test_key("versionstamp/conflict/output");
  lfdb::set(dbh, conflict_key, "old");

  auto txn = lfdb::make_transaction(dbh);
  std::string value;
  REQUIRE(lfdb::get(txn, conflict_key, value));

  lfdb::versionstamp stamp;
  lfdb::set(txn, output_key, lfdb::versioned("", stamp));

  lfdb::set(dbh, conflict_key, "new");

  const auto copy = stamp;
  const auto commit = lfdb::commit(lfdb::with_result, txn);
  REQUIRE_FALSE(commit);
  REQUIRE(stamp.is_ready());
  CHECK_FALSE(stamp.has_value());
  const auto failed_stamp = stamp.resolved_bytes(lfdb::with_result);
  REQUIRE_FALSE(failed_stamp);
  CHECK(commit.error() == failed_stamp.error());
  CHECK(copy.is_ready());
  const auto replacement = stamp_data(1);
  CHECK_THROWS_AS(lfdb::from::convert(std::span(replacement), stamp),
                  lfdb::libfdb_exception);

  try {
   std::ignore = copy.resolved_bytes();
   FAIL("failed stamp unexpectedly supplied bytes");
  } catch (const lfdb::libfdb_exception& error) {
   CHECK(commit.error() == error.code());
  }

  lfdb::reset_for_replay(txn, commit.error());
  CHECK(stamp.is_ready());
  lfdb::set(txn, output_key, lfdb::versioned("", stamp));
  CHECK_FALSE(copy.is_ready());
  CHECK(commit.error() == failed_stamp.error());
  REQUIRE_NOTHROW(lfdb::commit(txn));
  REQUIRE(copy.has_value());
  const auto bytes = stamp.resolved_bytes(lfdb::with_result);
  REQUIRE(bytes);
  CHECK(bytes->data() == copy.resolved_bytes().data());
 }

 SECTION("replay reset discards abandoned versionstamps") {
  constexpr fdb_error_t not_committed = 1020;
  REQUIRE(0 != fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, not_committed));

  const auto abandoned_key = test_key("versionstamp/replay/abandoned");
  const auto committed_key = test_key("versionstamp/replay/committed");
  auto txn = lfdb::make_transaction(dbh);

  lfdb::versionstamp abandoned_stamp;
  lfdb::set(txn, abandoned_key, lfdb::versioned("", abandoned_stamp));

  lfdb::reset_for_replay(txn, lfdb::fdb_error_code::not_committed);

  lfdb::versionstamp committed_stamp;
  lfdb::set(txn, committed_key, lfdb::versioned("", committed_stamp));
  REQUIRE_NOTHROW(lfdb::commit(txn));

  CHECK_FALSE(abandoned_stamp.has_value());
  REQUIRE(abandoned_stamp.is_ready());
  CHECK(lfdb::fdb_error_code::not_committed ==
        abandoned_stamp.resolved_bytes(lfdb::with_result).error());
  CHECK(committed_stamp.has_value());
  CHECK_FALSE(lfdb::key_exists(dbh, abandoned_key));
  CHECK(lfdb::key_exists(dbh, committed_key));
 }

 SECTION("read-only commit succeeds while its stamp reports no commit version") {
  auto txn = lfdb::make_transaction(dbh);
  lfdb::versionstamp stamp;
  const auto result = lfdb::commit(lfdb::with_result, txn, stamp);
  REQUIRE(result);
  REQUIRE(stamp.is_ready());
  CHECK_FALSE(stamp.has_value());
  CHECK(lfdb::fdb_error_code::no_commit_version ==
        stamp.resolved_bytes(lfdb::with_result).error());
  CHECK_THROWS_AS(stamp.resolved_bytes(), lfdb::libfdb_exception);
 }

 SECTION("manual serialization failure leaves other outputs registered") {
  auto txn = lfdb::make_transaction(dbh);
  lfdb::versionstamp stamp;
  const auto key = test_key("versionstamp/manual-recovery");
  lfdb::set(txn, key, lfdb::versioned("", stamp));
  returned_serialization_status::status = std::errc::permission_denied;
  CHECK_THROWS_AS(lfdb::set(txn, test_key("versionstamp/rejected-record"),
                           returned_serialization_status {}, lfdb::commit_after_op::commit),
                  lfdb::libfdb_exception);
  CHECK_FALSE(stamp.is_ready());
  REQUIRE_NOTHROW(lfdb::commit(txn));
  CHECK(stamp.has_value());
  CHECK(lfdb::key_exists(dbh, key));
 }

 SECTION("managed application failure abandons outputs before reporting failure") {
  lfdb::versionstamp stamp;
  lfdb::transaction_handle retained;
  auto run = lfdb::make_transactor(dbh);
  CHECK_THROWS_AS(run([&](auto txn) {
   retained = txn;
   lfdb::set(txn, test_key("versionstamp/application-failure"), lfdb::versioned("", stamp));
   throw std::runtime_error("application rejected update");
  }), std::runtime_error);
  REQUIRE(retained);
  REQUIRE(stamp.is_ready());
  CHECK(std::make_error_code(std::errc::operation_canceled) ==
        stamp.resolved_bytes(lfdb::with_result).error());
 }

 SECTION("terminal body failure completes stamps even when the transaction is retained") {
  lfdb::versionstamp stamp;
  lfdb::transaction_handle retained;
  auto run = lfdb::make_transactor(dbh);

  CHECK_THROWS_AS(run([&](auto txn) {
   retained = txn;
   lfdb::set(txn, test_key("versionstamp/terminal-body"), lfdb::versioned("", stamp));
   throw lfdb::libfdb_exception(lfdb::fdb_error_code::operation_cancelled);
  }), lfdb::libfdb_exception);

  REQUIRE(retained);
  REQUIRE(stamp.is_ready());
  CHECK(lfdb::fdb_error_code::operation_cancelled ==
        stamp.resolved_bytes(lfdb::with_result).error());
 }

 SECTION("abandoned registered output records cancellation in every copy") {
  lfdb::versionstamp stamp;
  const auto copy = stamp;

  {
   auto txn = lfdb::make_transaction(dbh);
   lfdb::set(txn, test_key("versionstamp/abandoned"), lfdb::versioned("", stamp));
   CHECK_FALSE(copy.is_ready());
  }

  REQUIRE(copy.is_ready());
  CHECK_FALSE(copy.has_value());
  CHECK(std::make_error_code(std::errc::operation_canceled) ==
        copy.resolved_bytes(lfdb::with_result).error());
 }

 SECTION("cross-transaction registration is rejected before mutation") {
  lfdb::versionstamp stamp;
  auto first = lfdb::make_transaction(dbh);
  auto second = lfdb::make_transaction(dbh);
  const auto rejected_key = test_key("versionstamp/rejected-registration");
  lfdb::set(first, test_key("versionstamp/owner"), lfdb::versioned("", stamp));

  CHECK_THROWS_AS(lfdb::set(second, rejected_key, lfdb::versioned("", stamp)),
                  lfdb::libfdb_exception);
  const auto replacement = stamp_data(1);
  CHECK_THROWS_AS(lfdb::from::convert(std::span(replacement), stamp),
                  lfdb::libfdb_exception);
  REQUIRE_NOTHROW(lfdb::commit(second));
  CHECK_FALSE(lfdb::key_exists(dbh, rejected_key));
  REQUIRE_NOTHROW(lfdb::commit(first));
  CHECK(stamp.has_value());
 }

 SECTION("stamp wait failure cannot reverse confirmed commit success") {
  auto txn = lfdb::make_transaction(dbh);
  const auto key = test_key("versionstamp/wait-failure");
  lfdb::versionstamp stamp;
  lfdb::set(txn, key, lfdb::versioned("", stamp));
  lfdb::detail::commit_attempt attempt {*txn};

  REQUIRE(0 == fdb_future_block_until_ready(attempt.pending_future()));
  REQUIRE_FALSE(attempt.advance());
  const auto completed = attempt.advance(static_cast<int>(lfdb::fdb_error_code::operation_cancelled));
  REQUIRE(completed);
  REQUIRE(*completed);
  CHECK(lfdb::key_exists(dbh, key));
  REQUIRE(stamp.is_ready());
  CHECK(lfdb::fdb_error_code::operation_cancelled ==
        stamp.resolved_bytes(lfdb::with_result).error());
 }

 SECTION("resolved versionstamp cannot be reused for commit") {
  const auto first_key = test_key("versionstamp/reuse/first");
  const auto second_key = test_key("versionstamp/reuse/second");
  lfdb::versionstamp stamp;

  auto txn = lfdb::make_transaction(dbh);
  lfdb::set(txn, first_key, "value");
  REQUIRE_NOTHROW(lfdb::commit(txn, stamp));
  REQUIRE(stamp.has_value());

  auto reuse_txn = lfdb::make_transaction(dbh);
  lfdb::set(reuse_txn, second_key, "value");

  CHECK_THROWS_MATCHES(lfdb::commit(reuse_txn, stamp),
                       lfdb::libfdb_exception,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring("attempt to reuse resolved version stamp")));
 }

 SECTION("versionstamp comparison requires resolved values") {
  lfdb::versionstamp unresolved;
  const auto resolved = resolved_stamp(stamp_data(1));

  CHECK_THROWS_MATCHES((void)(unresolved == resolved),
                       lfdb::libfdb_exception,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring("attempt to access unresolved version stamp")));
  CHECK_THROWS_MATCHES((void)(resolved == unresolved),
                       lfdb::libfdb_exception,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring("attempt to access unresolved version stamp")));
  CHECK_THROWS_MATCHES((void)(unresolved < resolved),
                       lfdb::libfdb_exception,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring("attempt to access unresolved version stamp")));
  CHECK_THROWS_MATCHES((void)(resolved < unresolved),
                       lfdb::libfdb_exception,
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

  lfdb::set(dbh, options,
            lfdb::versioned(prefix, "/entry", stamp),
            "value"s);

  REQUIRE(stamp.has_value());

  std::map<std::string, std::string> out;
  lfdb::get(dbh, lfdb::select { prefix }, std::inserter(out, out.end()));

  REQUIRE(1 == out.size());
  CHECK("value" == out.begin()->second);
 }

 SECTION("versionstamp value") {
  const auto key = test_key("versionstamp/value");
  lfdb::versionstamp stamp;

  lfdb::set(dbh, options,
            key,
            lfdb::versioned("", stamp));

  REQUIRE(stamp.has_value());

  lfdb::versionstamp out;
  REQUIRE(lfdb::get(dbh, key, out));

  REQUIRE(out.has_value());
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

    CHECK(stamp.has_value());
  }

  {
    const auto key = test_key("versionstamp/overload/value");
    auto txn = lfdb::make_transaction(dbh);
    lfdb::versionstamp stamp;

    lfdb::set(txn,
              key,
              lfdb::versioned("value:", stamp),
              lfdb::commit_after_op::commit);

    CHECK(stamp.has_value());
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
                       lfdb::libfdb_exception,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring(
                         "committed_version() requires committed transaction")));
 }

 SECTION("committed version is available after commit") {
  const auto key = test_key("transaction-version/committed");
  auto txn = lfdb::make_transaction(dbh);

  lfdb::set(txn, key, "value");
  REQUIRE_NOTHROW(lfdb::commit(txn));

  CHECK(0 < lfdb::committed_version(txn));
 }

 SECTION("explicit read version can read an earlier committed value") {
  const auto key = test_key("transaction-version/read-at");

  auto first_txn = lfdb::make_transaction(dbh);
  lfdb::set(first_txn, key, "first");
  REQUIRE_NOTHROW(lfdb::commit(first_txn));
  const auto first_version = lfdb::committed_version(first_txn);

  auto second_txn = lfdb::make_transaction(dbh);
  lfdb::set(second_txn, key, "second");
  REQUIRE_NOTHROW(lfdb::commit(second_txn));

  auto read_txn = lfdb::make_transaction(dbh);
  lfdb::set_read_version(read_txn, first_version);

  std::string value;
  REQUIRE(lfdb::get(read_txn, key, value));

  CHECK(first_version == lfdb::read_version(read_txn));
  CHECK("first" == value);
 }
}

TEST_CASE("transaction commit byte estimate", "[fdb]") {
 janitor dbh;
 auto txn = lfdb::make_transaction(dbh);

 const auto empty_estimate = lfdb::approximate_commit_bytes(txn);

 lfdb::set(txn, test_key("commit-bytes/a"), "alpha");
 lfdb::set(txn, test_key("commit-bytes/b"), "bravo");

 const auto populated_estimate = lfdb::approximate_commit_bytes(txn);

CHECK(0 <= empty_estimate);
CHECK(empty_estimate < populated_estimate);
}

TEST_CASE("api and system diagnostics", "[fdb]")
{
 SECTION("api namespace reports FDB client facts") {
  CHECK_FALSE(lfdb::api::client_version().empty());
  CHECK(730 <= lfdb::api::max_version());
 }

 SECTION("system namespace rejects invalid database handles") {
  CHECK_THROWS_AS(lfdb::system::client_network_load({}), lfdb::libfdb_exception);
  CHECK_THROWS_AS(lfdb::system::client_status_json({}), lfdb::libfdb_exception);
  CHECK_THROWS_AS(lfdb::system::server_protocol({}), lfdb::libfdb_exception);
  CHECK_THROWS_AS(lfdb::system::reboot_worker({}, "127.0.0.1:4500", false, 0),
                  lfdb::libfdb_exception);
  CHECK_THROWS_AS(lfdb::system::force_recovery_with_data_loss({}, "dc1"),
                  lfdb::libfdb_exception);
  CHECK_THROWS_AS(lfdb::system::create_snapshot({}, "snapshot-id", "snapshot-command"),
                  lfdb::libfdb_exception);
 }

 SECTION("system namespace reports live database diagnostics") {
  janitor dbh;

  const auto client_load = lfdb::system::client_network_load(dbh);
  CHECK(0.0 <= client_load);

  const auto protocol = lfdb::system::server_protocol(dbh);
  CHECK(0 < protocol);

  const auto status = lfdb::system::client_status_json(dbh);
  CHECK_FALSE(status.empty());
  CHECK(std::string::npos != status.find("Healthy"));
 }
}

TEST_CASE("approximate range size composes with selections", "[fdb][query]")
{
 namespace fdbc = ceph::libfdb::layer::content;
 namespace lq = ceph::libfdb::query;

 CHECK(0 == lfdb::approximate_range_size(lfdb::transaction_handle {}, lq::empty()));

 janitor dbh;
 const auto prefix = make_key_prefix("range-size");
 const auto object_root = fdbc::keyspace(test_key("range-size-content")) / "objects";

 for (const auto i : std::views::iota(0, 8)) {
  lfdb::set(dbh, make_key(i, "range-size"), std::string(1024, 'x'));
 }

 lfdb::set(dbh, object_root / "a", "alpha");
 lfdb::set(dbh, object_root / "b", "bravo");

 auto txn = lfdb::make_transaction(dbh);
 const auto range_estimate = lfdb::approximate_range_size(txn, lq::prefix(prefix));
 const auto union_estimate =
  lfdb::approximate_range_size(dbh,
                               lq::set_union(lq::prefix(prefix),
                                             fdbc::prefix(object_root)));
 const auto batched_selection = lq::set_union(
   lq::prefix(test_key("range-size-batch-a")),
   lq::set_union(
     lq::prefix(test_key("range-size-batch-c")),
     lq::set_union(
       lq::prefix(test_key("range-size-batch-e")),
       lq::set_union(
         lq::prefix(test_key("range-size-batch-g")),
         lq::prefix(test_key("range-size-batch-i"))))));
 std::size_t interval_count = 0;

 lq::for_each_interval(batched_selection,
                       [&interval_count](const auto&) {
                         ++interval_count;
                       });

 CHECK(0 <= range_estimate);
 CHECK(0 <= union_estimate);
 REQUIRE(5 == interval_count);
 CHECK(0 <= lfdb::approximate_range_size(dbh, batched_selection));
 CHECK(0 == lfdb::approximate_range_size(dbh, lfdb::select { prefix, prefix }));
}

TEST_CASE("half-open range conversion preserves reserved keyspace bounds", "[fdb][query]")
{
 auto selection = lfdb::select {"a", "b"};
 selection.begin_inclusive = false;
 selection.end_key = std::string {"\xff\0", 2};
 selection.end_inclusive = true;
 selection.options.reverse_order = true;

 const auto converted = lfdb::detail::as_half_open_select(std::move(selection));
 CHECK(std::string("a\0", 2) == converted.begin_key);
 CHECK(std::string("\xff", 1) == converted.end_key);
 CHECK(converted.begin_inclusive);
 CHECK_FALSE(converted.end_inclusive);
 CHECK(converted.options.reverse_order);
}

TEST_CASE("partitions produce algebra-compatible managed work ranges", "[fdb]")
{
 namespace lq = ceph::libfdb::query;

 struct nondefault_partition_result final
 {
  std::vector<lfdb::select> values;

  explicit nondefault_partition_result(std::vector<lfdb::select>&& source)
   : values(std::move(source))
  {}

  auto begin() noexcept { return std::begin(values); }
  auto end() noexcept { return std::end(values); }
  auto size() const noexcept { return std::size(values); }
 };

 janitor dbh;
 constexpr auto entries_per_group = 64;
 constexpr std::int64_t target_bytes = 256;

 write_monotonic_kvs(dbh, entries_per_group, "partition-a");
 write_monotonic_kvs(dbh, entries_per_group, "partition-c");
 write_monotonic_kvs(dbh, entries_per_group, "partition-gap");

 const auto selection = lq::set_union(
   lq::prefix(make_key_prefix("partition-a")),
   lq::prefix(make_key_prefix("partition-c")));
 auto read_keys = [&dbh](const auto& query) {
  auto txn = lfdb::make_transaction(dbh);

  return lfdb::scan(txn, query)
       | std::views::keys
       | std::ranges::to<std::vector<std::string>>();
 };
 const auto expected = read_keys(selection);
 auto txn = lfdb::make_transaction(dbh);
 const auto planned = lfdb::partitions(txn, selection, target_bytes);

 STATIC_REQUIRE(lq::expression<
   std::ranges::range_value_t<decltype(planned)>>);
 REQUIRE(2 <= std::size(planned));
 CHECK(2 * entries_per_group == std::size(expected));

 std::vector<std::string> actual;

 for (const auto& partition : planned) {
  const auto within_selection = lq::intersection(partition, selection);
  auto keys = read_keys(within_selection);

  std::ranges::move(keys, std::back_inserter(actual));
 }

 CHECK_THAT(actual, Catch::Matchers::RangeEquals(expected));
 CHECK(std::ranges::none_of(actual, [](const std::string_view key) {
  return key.starts_with(make_key_prefix("partition-gap"));
 }));

 const auto deque_partitions =
   lfdb::partitions<std::deque<lfdb::select>>(
     dbh, selection, target_bytes);

 CHECK(std::size(planned) == std::size(deque_partitions));
 CHECK_THAT(deque_partitions, Catch::Matchers::RangeEquals(planned));

 STATIC_REQUIRE_FALSE(std::default_initializable<nondefault_partition_result>);

 const auto nondefault_partitions =
   lfdb::partitions<nondefault_partition_result>(
     dbh, selection, target_bytes);

 CHECK(std::size(planned) == std::size(nondefault_partitions));

 const auto reverse_selection = lq::with_options(
   selection,
   lq::query_options {
    .result_limit = 7,
    .reverse_order = true
   });
 const auto reverse_expected = read_keys(reverse_selection);
 const auto reverse_partitions = lfdb::partitions(
   dbh, reverse_selection, target_bytes);
 std::vector<std::string> reverse_actual;

 for (const auto& partition : reverse_partitions) {
  CHECK(reverse_selection.options == partition.options);

  auto keys = read_keys(partition);
  std::ranges::move(keys, std::back_inserter(reverse_actual));
 }

 CHECK_THAT(reverse_actual,
            Catch::Matchers::RangeEquals(reverse_expected));

 const auto empty_selection = lq::difference(selection, selection);

 CHECK(std::empty(lfdb::partitions(dbh, empty_selection, target_bytes)));
 CHECK_THROWS_AS(lfdb::partitions(dbh, selection, 0),
                 lfdb::libfdb_exception);
 CHECK_THROWS_AS(lfdb::partitions(
                   lfdb::transaction_handle {}, selection, target_bytes),
                 lfdb::libfdb_exception);
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

TEST_CASE("watches accept ordinary keys", "[rgw][fdb][api]")
{
 janitor dbh;
 const auto compiled = content::key("watch-api", "compiled");
 auto exercise = [&dbh](const auto& key) {
  auto watch = lfdb::make_watch(dbh, key);
  lfdb::set(dbh, key, "changed");
  CHECK(lfdb::watch_event::changed == wait_for_watch_event(watch));
  const auto txn = lfdb::make_transaction(dbh);
  auto transaction_watch = lfdb::make_watch(txn, key);
  lfdb::commit(txn);
  transaction_watch.cancel();
  CHECK(lfdb::watch_event::cancelled == transaction_watch.wait_for_event());

  std::stop_source stop;
  stop.request_stop();
  CHECK_NOTHROW(lfdb::watched_loop(dbh, key, stop.get_token(), [](std::string_view) {}));
  lfdb::erase(dbh, key);
 };
 exercise(compiled);
 exercise(std::string_view(watch_key));
 exercise("libfdb-test/watch-api/literal");
}

TEST_CASE("const handles retain commit and typed replay semantics", "[rgw][fdb][api]")
{
 janitor dbh;
 const auto key = test_key("const-handle-replay");
 const auto txn = lfdb::make_transaction(dbh);
 lfdb::set(txn, key, "pending");
 CHECK_THROWS_AS(lfdb::reset_for_replay(txn, std::error_code(1020, std::generic_category())),
                 lfdb::libfdb_exception);
 CHECK_THROWS_AS(lfdb::reset_for_replay(txn, lfdb::libfdb_error_code::invalid_result),
                 lfdb::libfdb_exception);
 CHECK(lfdb::key_exists(txn, key));
 lfdb::reset_for_replay(txn, lfdb::fdb_error_code::not_committed);
 CHECK_FALSE(lfdb::key_exists(txn, key));
 lfdb::set(txn, key, "committed");
 CHECK(lfdb::commit(lfdb::with_result, txn));
 CHECK(lfdb::key_exists(dbh, key));
 CHECK_NOTHROW(lfdb::commit(lfdb::make_transaction(dbh)));
}

TEST_CASE("transaction watches", "[rgw][fdb]") {
 janitor dbh;

 static_assert(lfdb::watch_callback<decltype([](std::string_view) {})>);
 static_assert(!lfdb::watch_callback<decltype([](std::string_view) {
  return true;
 })>);

 SECTION("watch fires after committed key change") {
  auto txn = lfdb::make_transaction(dbh);
  auto watch = lfdb::make_watch(txn, watch_key);

  REQUIRE_NOTHROW(lfdb::commit(txn));
  REQUIRE_FALSE(watch.ready());

  lfdb::set(dbh, watch_key, "value");

  CHECK(lfdb::watch_event::changed == wait_for_watch_event(watch));
  CHECK(watch.ready());
 }

 SECTION("watch fires after committed key erase") {
  lfdb::set(dbh, watch_key, "value");

  auto txn = lfdb::make_transaction(dbh);
  auto watch = lfdb::make_watch(txn, watch_key);

  REQUIRE_NOTHROW(lfdb::commit(txn));
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

 SECTION("database watch accepts transaction options") {
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag}
  };

  auto watch = lfdb::make_watch(dbh, options, watch_key);

  CHECK_THROWS_AS(watch.wait_for_event(), lfdb::libfdb_exception);
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
  CHECK_THROWS_AS(watch.wait_for_event(), lfdb::libfdb_exception);

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

 SECTION("watch wait observes a pre-requested stop token") {
  auto watch = lfdb::make_watch(dbh, watch_key);
  std::stop_source stop;

  stop.request_stop();

  CHECK(lfdb::watch_event::cancelled == watch.wait_for_event(stop.get_token()));
 }

 SECTION("watch loop re-arms until stopped") {
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_PRIORITY_BATCH, lfdb::option_flag}
  };
  std::atomic_int callbacks = 0;
  std::atomic_bool wrong_key = false;

  std::jthread watch_thread {
   [&dbh, &options, &callbacks, &wrong_key](std::stop_token stop_token) {
    lfdb::watched_loop(dbh, options, watch_key, stop_token,
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

TEST_CASE("range writes accept distinct sentinels and single-pass transaction input",
          "[fdb][rgw][api]")
{
 janitor dbh;
 const std::array rows {
  string_pair {make_key(0, "sentinel-write"), "zero"},
  string_pair {make_key(1, "sentinel-write"), "one"},
  string_pair {make_key(2, "sentinel-write"), "two"}
 };
 auto noncommon = rows | std::views::take_while([](const auto&) { return true; });
 static_assert(std::ranges::forward_range<decltype(noncommon)>);
 static_assert(not std::ranges::common_range<decltype(noncommon)>);

 SECTION("managed ranges are replayable") {
  lfdb::set(dbh, noncommon);
 }
 SECTION("managed iterator pairs accept a sentinel") {
  lfdb::set(dbh, noncommon.begin(), noncommon.end());
 }
 SECTION("transaction iterator pairs default to no commit") {
  auto txn = lfdb::make_transaction(dbh);
  lfdb::set(txn, noncommon.begin(), noncommon.end());
  CHECK_FALSE(lfdb::key_exists(dbh, rows.front().first));
  CHECK(lfdb::key_exists(txn, rows.front().first));
  lfdb::commit(txn);
 }
 SECTION("transaction input may be single-pass and noncopyable") {
  auto generated = [&rows]() -> std::generator<const string_pair&> {
   for (const auto& row : rows) {
    co_yield row;
   }
  }();
  static_assert(std::ranges::input_range<decltype(generated)>);
  static_assert(not std::copy_constructible<std::ranges::iterator_t<decltype(generated)>>);
  static_assert(not can_lfdb_set<lfdb::database_handle, decltype(generated)&>);
  auto txn = lfdb::make_transaction(dbh);
  lfdb::set(txn, generated, lfdb::commit_after_op::commit);
 }

 std::vector<string_pair> observed;
 CHECK(lfdb::get(dbh, lfdb::query::prefix(make_key_prefix("sentinel-write")), observed) == 3);
 CHECK_THAT(observed, Catch::Matchers::RangeEquals(rows));
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

  const auto reverse_active_cache = lq::with_options(
    active_cache,
    lq::query_options {
     .result_limit = 2,
     .reverse_order = true
    });
  auto reverse_active_cache_keys = active_cache_keys;
  std::ranges::reverse(reverse_active_cache_keys);

  CHECK_THAT(keys_in(reverse_active_cache),
             Catch::Matchers::RangeEquals(reverse_active_cache_keys));
  CHECK_THAT(keys_from_transaction_scan(reverse_active_cache),
             Catch::Matchers::RangeEquals(reverse_active_cache_keys));
  CHECK_THAT(keys_from_managed_scan(reverse_active_cache),
             Catch::Matchers::RangeEquals(reverse_active_cache_keys));
  CHECK_THAT(keys_from_blocks(reverse_active_cache),
             Catch::Matchers::RangeEquals(reverse_active_cache_keys));

  const auto empty_then_active = lq::set_union(
    lq::prefix(test_key("cache/absent/")),
    lq::prefix(test_key("cache/warm/")));

  CHECK_THAT(keys_from_blocks(empty_then_active),
             Catch::Matchers::RangeEquals(std::vector {
              test_key("cache/warm/a")
             }));

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
  const auto n = lfdb::detail::as_byte_view(std::string_view(msg, sizeof(msg)));

  std::vector<std::uint8_t> x;
  x = ceph::libfdb::to::convert(n);

  std::vector<std::uint8_t> o;
  ceph::libfdb::from::convert(x, o); 

  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 } 

 SECTION("NULL-as-data") {
  // with NULL data-- const char* -> vector<uint8_t> -> vector<uint8_t>
  const auto n = lfdb::detail::as_byte_view(
    std::string_view(msg_with_null, sizeof(msg_with_null)));

  std::vector<std::uint8_t> x;
  x = ceph::libfdb::to::convert(n);

  std::vector<std::uint8_t> o;
  ceph::libfdb::from::convert(x, o); 

  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
  REQUIRE_THAT(msg_with_null, Catch::Matchers::RangeEquals(o));
 }

 SECTION("fixed-size C array") {
  const std::uint8_t input[] { 1, 2, 3, 4 };
  std::uint8_t output[std::size(input)] {};

  const auto encoded = lfdb::to::convert(input);
  lfdb::from::convert(encoded, output);

  CHECK(sizeof input == std::size(encoded));
  CHECK_THAT(output, Catch::Matchers::RangeEquals(input));
 }
}

TEST_CASE("Options retain application order and repetitions", "[fdb][options]")
{
 const auto check_order = []<typename CodeT>(const CodeT first, const CodeT second) {
  const lfdb::option_list<CodeT> options {
   {first, "a"}, {second, "b"}, {first, "c"}
  };
  std::vector<CodeT> codes;
  std::vector<std::string> values;

  lfdb::detail::apply_options(options,
    [&](const CodeT code, const std::uint8_t *data, const int size) {
     codes.push_back(code);
     values.emplace_back(reinterpret_cast<const char *>(data), size);

     return fdb_error_t {0};
    });

  CHECK(3 == options.size());
  CHECK_THAT(codes, Catch::Matchers::RangeEquals(
    std::array {first, second, first}));
  CHECK_THAT(values, Catch::Matchers::RangeEquals(
    std::array {"a", "b", "c"}));
 };

 check_order(FDB_NET_OPTION_TLS_PASSWORD, FDB_NET_OPTION_TLS_KEY_BYTES);
 check_order(FDB_DB_OPTION_TRANSACTION_TIMEOUT, FDB_DB_OPTION_LOCATION_CACHE_SIZE);
 check_order(FDB_TR_OPTION_TAG, FDB_TR_OPTION_AUTO_THROTTLE_TAG);
}

TEST_CASE("Option list insertion and editing preserve sequence semantics", "[fdb][options]")
{
 using options_type = lfdb::transaction_options;
 using value_type = options_type::value_type;

 STATIC_REQUIRE(std::ranges::contiguous_range<options_type>);
 STATIC_REQUIRE(std::same_as<std::ranges::range_reference_t<options_type>,
                            const value_type&>);

 const auto string_values = std::views::values |
   std::views::transform([](const lfdb::option_value& value) -> const std::string& {
    return std::get<std::string>(value);
   });
 options_type options;
 options.reserve(8);
 CHECK(options.empty());
 options.emplace_back(FDB_TR_OPTION_TAG, "a");
 options.push_back({FDB_TR_OPTION_AUTO_THROTTLE_TAG, "b"});
 const value_type entry {FDB_TR_OPTION_TAG, "c"};
 options.push_back(entry);

 SECTION("bulk insertion and back insertion preserve all entries") {
  options.append({{FDB_TR_OPTION_TAG, "d"}, {FDB_TR_OPTION_TAG, "e"}});
  const std::array more {value_type {FDB_TR_OPTION_TAG, "f"}};
  options.append_range(more);
  std::ranges::copy(more, std::back_inserter(options));

  // A non-common range also works without collecting it first.
  auto input = std::views::iota(0) | std::views::take(2) |
    std::views::transform([](const int i) {
     return value_type {FDB_TR_OPTION_TAG, std::to_string(i)};
    });
  STATIC_REQUIRE_FALSE(std::ranges::common_range<decltype(input)>);
  options.append_range(input);

  CHECK_THAT(options | string_values,
             Catch::Matchers::RangeEquals(
               std::array {"a", "b", "c", "d", "e", "f", "f", "0", "1"}));
 }

 SECTION("single-pass ranges are consumed once and retain repetitions") {
  std::istringstream input {"one two one"};
  auto incoming = std::ranges::istream_view<std::string>(input) |
    std::views::transform([](const auto& value) {
     return value_type {FDB_TR_OPTION_TAG, value};
    });
  STATIC_REQUIRE_FALSE(std::ranges::forward_range<decltype(incoming)>);
  options.clear();
  options.append_range(incoming);

  CHECK_THAT(options | string_values,
             Catch::Matchers::RangeEquals(std::array {"one", "two", "one"}));
 }

 SECTION("replacement preserves positions and never inserts") {
  CHECK(2 == options.replace_all(FDB_TR_OPTION_TAG, "new"));
  CHECK(2 == options.replace_all(FDB_TR_OPTION_TAG, "new"));
  CHECK(0 == options.replace_all(FDB_TR_OPTION_TIMEOUT, std::int64_t {5000}));
  CHECK(3 == options.size());
  CHECK_THAT(options | std::views::keys, Catch::Matchers::RangeEquals(
    std::array {FDB_TR_OPTION_TAG, FDB_TR_OPTION_AUTO_THROTTLE_TAG, FDB_TR_OPTION_TAG}));
  CHECK_THAT(options | string_values,
             Catch::Matchers::RangeEquals(std::array {"new", "b", "new"}));
 }

 SECTION("erasure removes every match and preserves other entries") {
  CHECK(0 == options.erase(FDB_TR_OPTION_TIMEOUT));
  CHECK(2 == options.erase(FDB_TR_OPTION_TAG));
  REQUIRE(1 == options.size());
  CHECK(FDB_TR_OPTION_AUTO_THROTTLE_TAG == options.begin()->first);
  options.emplace_back(FDB_TR_OPTION_TAG, "last");
  CHECK(FDB_TR_OPTION_TAG == std::prev(options.end())->first);
  options.clear();
  CHECK(options.empty());
 }

 SECTION("copies own their values and moving ranges transfer payload buffers") {
  auto copied = options;
  options.replace_all(FDB_TR_OPTION_TAG, "changed");
  CHECK("a" == std::get<std::string>(copied.begin()->second));

  std::vector<std::uint8_t> bytes(4096, 42);
  const auto *storage = bytes.data();
  std::array incoming {value_type {FDB_TR_OPTION_TAG, std::move(bytes)}};
  copied.append_range(std::ranges::subrange(
    std::make_move_iterator(incoming.begin()), std::make_move_iterator(incoming.end())));
  auto moved = std::move(copied);

  CHECK(storage == std::get<std::vector<std::uint8_t>>(
    std::prev(moved.end())->second).data());
 }
}

TEST_CASE("FoundationDB C API argument conversion", "[fdb][rgw]")
{
 SECTION("integer options are little-endian") {
  constexpr std::int64_t value = 0x0102030405060708;
  constexpr auto versionstamp_offset =
    lfdb::detail::little_endian_bytes(std::uint32_t {0x01020304});
  const lfdb::transaction_options options {
   { FDB_TR_OPTION_TIMEOUT, value }
  };

  FDBTransactionOption option = FDB_TR_OPTION_ACCESS_SYSTEM_KEYS;
  std::vector<std::uint8_t> encoded;

  lfdb::detail::apply_options(
    options,
    [&option, &encoded](const auto received_option,
                        const std::uint8_t *data,
                        const int size) {
      option = received_option;
      encoded.assign(data, data + size);

      return fdb_error_t { 0 };
    });

  CHECK(FDB_TR_OPTION_TIMEOUT == option);
  STATIC_REQUIRE(versionstamp_offset ==
                 std::array<std::uint8_t, 4> { 4, 3, 2, 1 });
  CHECK_THAT(encoded, Catch::Matchers::RangeEquals(
    std::array<std::uint8_t, 8> { 8, 7, 6, 5, 4, 3, 2, 1 }));
 }

 SECTION("option failures retain their native code and stop application") {
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_TAG, "a"}, {FDB_TR_OPTION_TAG, "b"}
  };
  int calls = 0;

  try {
   lfdb::detail::apply_options(options, [&](auto, auto, auto) {
    ++calls;

    return fdb_error_t {1020};
   });
   FAIL("expected option application to fail");
  } catch (const lfdb::libfdb_exception& error) {
   CHECK(std::error_code(1020, lfdb::fdb_error_category()) == error.code());
   CHECK(error.retryable());
   CHECK_THAT(error.what(), Catch::Matchers::ContainsSubstring("while setting option 800"));
  }

  CHECK(1 == calls);
 }

 SECTION("sizes outside the C API range are rejected") {
  constexpr auto too_large = 1ZU + std::numeric_limits<int>::max();

  CHECK_THROWS_WITH(lfdb::detail::checked_fdb_size(too_large),
                    Catch::Matchers::ContainsSubstring("value is too large for the FoundationDB C API"));
  CHECK_THROWS_WITH(lfdb::detail::checked_result_size(-1),
                    Catch::Matchers::ContainsSubstring("FoundationDB returned a negative result size"));
 }

 SECTION("keys and values use the same checked byte representation") {
  const std::string key("a\0b", 3);
  const std::array<std::uint8_t, 3> value { 1, 0, 2 };

  const auto key_bytes = lfdb::detail::as_fdb_bytes(key);
  const auto value_bytes = lfdb::detail::as_fdb_bytes(lfdb::detail::byte_view(value));
  const auto key_result = lfdb::detail::result_bytes(key_bytes.data, key_bytes.length);
  const auto value_result = lfdb::detail::result_bytes(value_bytes.data, value_bytes.length);

  CHECK(3 == key_bytes.length);
  CHECK(3 == value_bytes.length);
  CHECK(key == lfdb::detail::as_string_view(key_result));
  CHECK_THAT(value_result, Catch::Matchers::RangeEquals(value));
 }

 SECTION("empty result buffers are accepted") {
  const auto empty = lfdb::detail::result_bytes(nullptr, 0);

  CHECK(std::empty(empty));
  CHECK(std::empty(lfdb::detail::as_string_view(empty)));
 }

 SECTION("nonempty NULL result buffers are rejected") {
  const FDBKeyValue *data = nullptr;

  CHECK_THROWS_WITH(lfdb::detail::result_span(data, 1),
                    Catch::Matchers::ContainsSubstring("FoundationDB returned a NULL result buffer"));
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

 // A scalar encoding larger than libfdb's inline buffer must spill to dynamic
 // storage without changing the public operation:
 {
 const std::string n(4 * 1024, 'x');
 std::string o;

 const auto key = test_key("large-value");
 lfdb::set(lfdb::make_transaction(j), key, n, lfdb::commit_after_op::commit);
 lfdb::get(lfdb::make_transaction(j), key, o, lfdb::commit_after_op::no_commit);

 REQUIRE(n == o);
 }

 // A large vector also exercises the generic scalar-set spill path:
 {
 const std::vector<std::uint8_t> n(4 * 1024, 42);
 std::vector<std::uint8_t> o;

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
    const auto in_span = lfdb::detail::as_byte_view(std::string_view(data, sz));
 
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

TEST_CASE("managed reads distinguish FDB failures from callback failures", "[fdb]")
{
 janitor dbh;

 // FoundationDB's transaction_not_committed error is retryable:
 constexpr fdb_error_t not_committed = 1020;
 REQUIRE(fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, not_committed));

 SECTION("successful reads do not commit") {
  const auto key = test_key("managed-read-no-commit");

  lfdb::detail::in_read_transaction(dbh, [&key](auto& txn) {
   lfdb::set(txn, key, "discarded", lfdb::commit_after_op::no_commit);
  });

  CHECK_FALSE(lfdb::key_exists(dbh, key));
 }

 SECTION("retryable read failures replay the operation") {
  std::size_t attempts = 0;

  const auto result = lfdb::detail::in_read_transaction(
   dbh, [&attempts](auto&) {
    if (1 == ++attempts) {
     throw lfdb::libfdb_exception(std::error_code(not_committed, lfdb::fdb_error_category()));
    }

    return 42;
   });

  CHECK(42 == result);
  CHECK(2 == attempts);
 }

 SECTION("callback failures are never classified as FDB read failures") {
  const auto key = test_key("managed-read-callback-error");
  std::size_t calls = 0;

  lfdb::set(dbh, key, "value");

  try {
   std::ignore = lfdb::get(
    dbh, key, [&calls](std::span<const std::uint8_t>) {
     ++calls;
     throw lfdb::libfdb_exception(std::error_code(not_committed, lfdb::fdb_error_category()));
    });
   FAIL("expected callback failure");
  } catch (const lfdb::libfdb_exception& e) {
   CHECK(std::error_code(not_committed, lfdb::fdb_error_category()) == e.code());
  }

  CHECK(1 == calls);
 }

 SECTION("for_each callback failures are never replayed") {
  const auto key = test_key("managed-read-for-each-error");
  std::size_t calls = 0;

  lfdb::set(dbh, key, "value");

  try {
   lfdb::for_each(dbh, lfdb::select {key}, [&calls](auto&&) {
    ++calls;
    throw lfdb::libfdb_exception(std::error_code(not_committed, lfdb::fdb_error_category()));
   });
   FAIL("expected callback failure");
  } catch (const lfdb::libfdb_exception& e) {
   CHECK(std::error_code(not_committed, lfdb::fdb_error_category()) == e.code());
  }

  CHECK(1 == calls);
 }

 SECTION("transform callback failures are never replayed") {
  const auto key = test_key("managed-read-transform-error");
  std::size_t calls = 0;

  lfdb::set(dbh, key, "value");

  try {
   std::ignore = lfdb::transform(
    dbh, lfdb::select {key}, [&calls](auto&&) -> std::string {
     ++calls;
     throw lfdb::libfdb_exception(std::error_code(not_committed, lfdb::fdb_error_category()));
    });
   FAIL("expected callback failure");
  } catch (const lfdb::libfdb_exception& e) {
   CHECK(std::error_code(not_committed, lfdb::fdb_error_category()) == e.code());
  }

  CHECK(1 == calls);
 }
}

TEST_CASE("functional traversal borrows callables and keeps them last", "[fdb][api]")
{
 janitor dbh;
 const auto prefix = test_key("functional-callables/");
 const std::vector<string_pair> rows {
  {prefix + "0", "first"}, {prefix + "1", "second"}, {prefix + "2", "third"}
 };
 lfdb::set(dbh, rows);

 const auto managed = GENERATE(false, true);
 const auto mode = GENERATE(lfdb::read_mode::serializable, lfdb::read_mode::snapshot);
 auto txn = lfdb::make_transaction(dbh);
 const auto selection = lfdb::select {prefix};

 struct row_counter final
 {
  std::unique_ptr<std::size_t> calls = std::make_unique<std::size_t>(0);

  void operator()(string_pair&&) { ++*calls; }
 };

 struct key_counter final
 {
  std::unique_ptr<std::size_t> calls = std::make_unique<std::size_t>(0);

  std::string operator()(string_pair&& row)
  {
   ++*calls;

   return std::move(row.first);
  }
 };

 SECTION("for_each updates a noncopyable lvalue consumer and returns its count") {
  row_counter consumer;
  const auto count = managed ? lfdb::for_each(dbh, selection, mode, consumer)
                             : lfdb::for_each(txn, selection, mode, consumer);

  CHECK(rows.size() == count);
  CHECK(rows.size() == *consumer.calls);
 }

 SECTION("transform returns the advanced output iterator") {
  key_counter key_transform;
  std::vector<std::string> keys(rows.size());
  const auto result = managed ? lfdb::transform(dbh, selection, keys.begin(), mode, key_transform)
                              : lfdb::transform(txn, selection, keys.begin(), mode, key_transform);

  CHECK(keys.end() == result);
  CHECK(rows.size() == *key_transform.calls);
  CHECK_THAT(keys, Catch::Matchers::RangeEquals(rows | std::views::keys));
 }

 SECTION("allocating transform borrows the same noncopyable callable") {
  key_counter key_transform;
  const auto keys = managed ? lfdb::transform(dbh, selection, mode, key_transform)
                            : lfdb::transform(txn, selection, mode, key_transform);

  CHECK(rows.size() == *key_transform.calls);
  CHECK_THAT(keys, Catch::Matchers::RangeEquals(rows | std::views::keys));
 }
}

TEST_CASE("synchronous transactors borrow bodies with bound arguments", "[fdb][api]")
{
 struct body_t final {
  std::unique_ptr<int> calls = std::make_unique<int>(0);
  int operator()(lfdb::transaction_handle&, int& value) {
   if (++*calls == 1) {
    throw lfdb::libfdb_exception(lfdb::fdb_error_code::not_committed);
   }
   return value + *calls;
  }
 };
 janitor dbh;
 const auto run = lfdb::make_transactor(dbh);
 body_t body;
 CHECK(run(body, 7) == 9);
 CHECK(*body.calls == 2);
 body_t policy_body;
 CHECK(run(lfdb::retry_if([](const lfdb::transaction_failure&) { return true; }),
            policy_body, 7) == 9);
 CHECK(*policy_body.calls == 2);
}

TEST_CASE("transforms consume reference results before scan rows expire", "[fdb][api]")
{
 janitor dbh;
 const std::vector<string_pair> rows {
  {test_key("transform-reference/0"), "zero"},
  {test_key("transform-reference/1"), "one"}
 };
 lfdb::set(dbh, rows);
 const auto selection = lfdb::query::prefix(test_key("transform-reference/"));
 const auto reference = [](const string_pair& row) -> const std::string& { return row.first; };
 auto txn = lfdb::make_transaction(dbh);
 const auto managed = GENERATE(false, true);
 std::vector<std::string> copied;
 if (managed) {
  lfdb::transform(dbh, selection, std::back_inserter(copied), reference);
 } else {
  lfdb::transform(txn, selection, std::back_inserter(copied), reference);
 }
 CHECK_THAT(copied, Catch::Matchers::RangeEquals(rows | std::views::keys));
 const auto collected = managed ? lfdb::transform(dbh, selection, reference)
                               : lfdb::transform(txn, selection, reference);
 CHECK(collected == copied);
}

TEST_CASE("database traversal delivers independent windows without replaying consumers", "[fdb][api]")
{
 janitor dbh;
 const auto prefix = test_key("functional-windows/");
 const std::vector<string_pair> rows {
  {prefix + "0", "first"}, {prefix + "1", "second"}, {prefix + "2", "old"}
 };
 lfdb::set(dbh, rows);

 auto selection = lfdb::select {prefix};
 selection.options.result_limit = 1;
 std::size_t calls = 0;

 const auto count = lfdb::for_each(dbh, selection, [&](string_pair&& row) {
  if (0 == calls++) {
   lfdb::set(dbh, rows.back().first, "new");
  }

  if (rows.back().first == row.first) {
   CHECK("new" == row.second);
  }
 });

 CHECK(rows.size() == calls);
 CHECK(rows.size() == count);
}

TEST_CASE("erase_if preserves predicate failures without replay or publication", "[fdb][api]")
{
 janitor dbh;
 const auto prefix = test_key("erase-predicate-failure/");
 const std::vector<string_pair> rows {
  {prefix + "0", "first"}, {prefix + "1", "second"}, {prefix + "2", "third"}
 };
 lfdb::set(dbh, rows);

 const auto failure = std::make_exception_ptr(
  lfdb::libfdb_exception(lfdb::fdb_error_code::not_committed, "predicate failure"));
 std::size_t calls = 0;
 auto predicate = [owned = std::make_unique<int>(0), &calls, &failure](const string_pair&) mutable {
  ++*owned;
  if (2 == ++calls) {
   std::rethrow_exception(failure);
  }

  return true;
 };

 try {
  lfdb::erase_if(dbh, lfdb::select {prefix}, predicate);
  FAIL("predicate failure was swallowed");
 } catch (...) {
  CHECK(failure == std::current_exception());
 }

 CHECK(2 == calls);
 for (const auto& [key, value] : rows) {
  CHECK(lfdb::key_exists(dbh, key));
 }
}

TEST_CASE("read_query_window", "[fdb]")
{
 janitor j;

 const std::size_t result_limit = 5;
 const std::size_t nkeys = 12;

 const auto kvs_in = write_monotonic_kvs(j, nkeys);

 SECTION("owns read-your-writes metadata through moves") {
  const auto key = test_key("window/read-your-writes");
  auto txn = lfdb::make_transaction(j);
  lfdb::set(txn, key, std::string {"pending"});
  auto window = lfdb::detail::read_query_window(*txn, lfdb::select {key}, 1);
  auto moved = std::move(window);
  const std::vector<string_pair> expected {{key, "pending"}};

  CHECK_THAT(decode_raw_fdb_pairs(moved.result_pairs), Catch::Matchers::RangeEquals(expected));

  auto replacement = lfdb::detail::read_query_window(*txn, lfdb::select {make_key(0)}, 1);
  replacement = std::move(moved);
  CHECK_THAT(decode_raw_fdb_pairs(replacement.result_pairs),
             Catch::Matchers::RangeEquals(expected));
 }

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

TEST_CASE("for_each provides callback-scoped raw range values", "[fdb]")
{
 janitor j;

 const std::size_t result_limit = 5;
 const std::size_t nkeys = 12;

 const auto kvs_in = write_monotonic_kvs(j, nkeys);

 SECTION("retains the transaction when a callback releases the caller's handle") {
  auto txn = lfdb::make_transaction(j);
  const std::weak_ptr<lfdb::transaction> owner = txn;
  auto selector = lfdb::select {make_key(0), make_key(nkeys)};
  selector.options.result_limit = 1;
  const auto count = lfdb::for_each(lfdb::raw, txn, selector,
   [&](std::span<const std::uint8_t>, std::span<const std::uint8_t>) {
    txn.reset();
    CHECK_FALSE(owner.expired());
   });
  CHECK(count == nkeys);
  CHECK(owner.expired());
 }

 SECTION("borrows noncopyable consumers with an explicit read mode") {
  auto consumer = [calls = std::make_unique<std::size_t>(0)](
                    std::span<const std::uint8_t>, std::span<const std::uint8_t>) mutable {
   ++*calls;
  };
  static_assert(not std::copy_constructible<decltype(consumer)>);
  const auto selector = lfdb::select {make_key(0), make_key(nkeys)};
  const auto txn = lfdb::make_transaction(j);
  CHECK(lfdb::for_each(lfdb::raw, txn, selector, lfdb::read_mode::snapshot, consumer) == nkeys);
  CHECK(lfdb::for_each(lfdb::raw, j.dbh(), lfdb::transaction_options {}, selector,
                       lfdb::read_mode::snapshot, consumer) == nkeys);
 }

 auto check_traversal = [&kvs_in](auto source, auto selector, auto expected,
                                  const auto expected_end) {
  const auto nread = lfdb::for_each(
    lfdb::raw, source, selector,
    [&](std::span<const std::uint8_t> key,
        std::span<const std::uint8_t> value) {
      REQUIRE(expected_end != expected);
      REQUIRE(std::ranges::equal(key, expected->first));

      std::string decoded;
      lfdb::from::convert(value, decoded);
      CHECK(expected->second == decoded);
      ++expected;
    });

  CHECK(std::size(kvs_in) == nread);
  CHECK(expected_end == expected);
 };

 SECTION("drains paged forward results in order") {
  auto selector = lfdb::select { make_key(0), make_key(nkeys) };
  selector.options.result_limit = result_limit;

  check_traversal(lfdb::make_transaction(j), selector,
                  std::begin(kvs_in), std::end(kvs_in));
 }

 SECTION("drains paged reverse results in order") {
  auto selector = lfdb::select { make_key(0), make_key(nkeys) };
  selector.options.result_limit = result_limit;
  selector.options.reverse_order = true;

  check_traversal(lfdb::make_transaction(j), selector,
                  std::rbegin(kvs_in), std::rend(kvs_in));
 }

 SECTION("database traversal drains paged forward results in order") {
  auto selector = lfdb::select { make_key(0), make_key(nkeys) };
  selector.options.result_limit = result_limit;

  check_traversal(j.dbh(), selector,
                  std::begin(kvs_in), std::end(kvs_in));
 }

 SECTION("database traversal drains paged reverse results in order") {
  auto selector = lfdb::select { make_key(0), make_key(nkeys) };
  selector.options.result_limit = result_limit;
  selector.options.reverse_order = true;

  check_traversal(j.dbh(), selector,
                  std::rbegin(kvs_in), std::rend(kvs_in));
 }

 SECTION("database traversal accepts options across paged results") {
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag}
  };

  auto selector = lfdb::select {make_key(0), make_key(nkeys)};
  selector.options.result_limit = result_limit;
  auto expected = std::begin(kvs_in);

  const auto nread = lfdb::for_each(
   lfdb::raw, j.dbh(), options, selector,
   [&expected](std::span<const std::uint8_t> key,
               std::span<const std::uint8_t>) {
    REQUIRE(std::ranges::equal(key, expected->first));
    ++expected;
   });

 CHECK(std::size(kvs_in) == nread);
 CHECK(std::end(kvs_in) == expected);
 }

 SECTION("database traversal starts a new transaction for each window") {
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag}
  };
  const auto inserted_key = make_key(nkeys);
  auto selector = lfdb::select {make_key(0), make_key(1 + nkeys)};
  selector.options.result_limit = 1;
  bool inserted = false;
  bool saw_inserted_key = false;

  const auto nread = lfdb::for_each(
   lfdb::raw, j.dbh(), options, selector,
   [&](std::span<const std::uint8_t> key,
       std::span<const std::uint8_t>) {
    if (not inserted) {
     lfdb::set(j, inserted_key, "late value");
     inserted = true;
    }

    saw_inserted_key |= std::ranges::equal(key, inserted_key);
   });

  CHECK(1 + std::size(kvs_in) == nread);
  CHECK(saw_inserted_key);
 }

 SECTION("executes disjoint query intervals exactly once") {
  const auto selection = lfdb::query::difference(
    lfdb::query::between(make_key(0), make_key(nkeys)),
    lfdb::query::between(make_key(4), make_key(8)));
  const std::array expected {
   make_key(0),
   make_key(1),
   make_key(2),
   make_key(3),
   make_key(8),
   make_key(9),
   make_key(10),
   make_key(11)
  };
  auto next = std::begin(expected);
  const auto expected_end = std::end(expected);

  auto txn = lfdb::make_transaction(j);
  const auto nread = lfdb::for_each(
    lfdb::raw, txn, selection,
    [&next, expected_end](std::span<const std::uint8_t> key,
                         std::span<const std::uint8_t>) {
      REQUIRE(expected_end != next);
      REQUIRE(std::ranges::equal(key, *next));
      ++next;
    });

 CHECK(std::size(expected) == nread);
 CHECK(expected_end == next);
 }

 SECTION("reverses disjoint query intervals as one result") {
  const auto selection = lfdb::query::with_options(
    lfdb::query::difference(
      lfdb::query::between(make_key(0), make_key(nkeys)),
      lfdb::query::between(make_key(4), make_key(8))),
    {.reverse_order = true});
  const std::array expected {
   make_key(11),
   make_key(10),
   make_key(9),
   make_key(8),
   make_key(3),
   make_key(2),
   make_key(1),
   make_key(0)
  };
  auto next = std::begin(expected);
  const auto expected_end = std::end(expected);

  auto txn = lfdb::make_transaction(j);
  const auto nread = lfdb::for_each(
    lfdb::raw, txn, selection,
    [&next, expected_end](std::span<const std::uint8_t> key,
                         std::span<const std::uint8_t>) {
      REQUIRE(expected_end != next);
      REQUIRE(std::ranges::equal(key, *next));
      ++next;
    });

  CHECK(std::size(expected) == nread);
  CHECK(expected_end == next);
 }

 SECTION("preserves arbitrary value bytes") {
  constexpr std::array<std::uint8_t, 5> raw_value {0, 1, 0xff, 0, 42};
  const auto key = test_key("raw-range-value");

  write_raw_fdb_value(j, key, raw_value);

  auto txn = lfdb::make_transaction(j);
  const auto nread = lfdb::for_each(
    lfdb::raw, txn, lfdb::query::prefix(key),
    [&](std::span<const std::uint8_t> found_key,
        std::span<const std::uint8_t> found_value) {
      CHECK(std::ranges::equal(found_key, key));
      CHECK(std::ranges::equal(found_value, raw_value));
    });

  CHECK(1 == nread);
 }

 SECTION("does not invoke the callback for an empty expression") {
  const auto keys = lfdb::query::prefix(make_key_prefix());
  const auto empty = lfdb::query::difference(keys, keys);
  std::size_t callbacks = 0;

  auto txn = lfdb::make_transaction(j);
  const auto nread = lfdb::for_each(
    lfdb::raw, txn, empty,
    [&callbacks](std::span<const std::uint8_t>,
                 std::span<const std::uint8_t>) {
      ++callbacks;
    });

  CHECK(0 == nread);
  CHECK(0 == callbacks);
 }

 SECTION("propagates callback exceptions and stops immediately") {
  std::size_t callbacks = 0;
  auto txn = lfdb::make_transaction(j);

  CHECK_THROWS_WITH(
    lfdb::for_each(
      lfdb::raw, txn, lfdb::select { make_key(0), make_key(nkeys) },
      [&callbacks](std::span<const std::uint8_t>,
                   std::span<const std::uint8_t>) {
        ++callbacks;

        if (2 == callbacks) {
         throw std::runtime_error("stop raw traversal");
        }
      }),
    "stop raw traversal");

  CHECK(2 == callbacks);

  std::string value;
  CHECK(lfdb::get(txn, make_key(0), value));
 }

 SECTION("database traversal never interprets callback exceptions as retries") {
  constexpr fdb_error_t not_committed = 1020;
  std::size_t callbacks = 0;

  REQUIRE(fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE,
                              not_committed));
  CHECK_THROWS_AS(
    lfdb::for_each(
      lfdb::raw, j.dbh(), lfdb::select { make_key(0), make_key(nkeys) },
      [&callbacks, not_committed](std::span<const std::uint8_t>,
                                  std::span<const std::uint8_t>) {
        ++callbacks;
        throw lfdb::libfdb_exception(std::error_code(not_committed, lfdb::fdb_error_category()));
      }),
    lfdb::libfdb_exception);

  CHECK(1 == callbacks);
 }
}

TEST_CASE("managed read helpers accept transaction options", "[fdb]")
{
 janitor j;

 constexpr std::size_t nkeys = 12;
 const auto kvs_in = write_monotonic_kvs(j, nkeys);
 const lfdb::transaction_options options {
  {FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag}
 };

 auto selector = lfdb::select {make_key(0), make_key(nkeys)};
 selector.options.result_limit = 5;

 SECTION("scan owns options for its lazy lifetime") {
  auto scan = [&] {
   const auto local_options = options;

   return lfdb::scan(j.dbh(), local_options, selector);
  }();

  auto rows = scan | std::ranges::to<std::vector<string_pair>>();

  CHECK_THAT(rows, Catch::Matchers::RangeEquals(kvs_in));
 }

 SECTION("blocks apply options to managed windows") {
  auto rows = lfdb::blocks(j.dbh(), options, selector)
            | std::views::join
            | std::ranges::to<std::vector<string_pair>>();

  CHECK_THAT(rows, Catch::Matchers::RangeEquals(kvs_in));
 }

 SECTION("collect materializes an option-aware scan") {
  const auto rows = lfdb::collect(j.dbh(), options, selector);

  CHECK_THAT(rows, Catch::Matchers::RangeEquals(kvs_in));
 }

 SECTION("transform runs with transaction options") {
  const auto keys = lfdb::transform(
   j.dbh(), options, selector,
   [](string_pair&& row) { return std::move(row.first); });

  const auto expected = kvs_in
                      | std::views::keys
                      | std::ranges::to<std::vector<std::string>>();

  CHECK_THAT(keys, Catch::Matchers::RangeEquals(expected));
 }

 SECTION("configured callbacks borrow noncopyable lvalues") {
  auto consumer = [state = std::make_unique<int>(0)](string_pair&&) mutable { ++*state; };
  CHECK(lfdb::for_each(j.dbh(), options, selector, lfdb::read_mode::snapshot, consumer) == nkeys);
  auto transform_key = [state = std::make_unique<int>(0)](string_pair&& row) mutable {
   ++*state;
   return std::move(row.first);
  };
  std::vector<std::string> keys;
  lfdb::transform(j.dbh(), options, selector, std::back_inserter(keys),
                   lfdb::read_mode::snapshot, transform_key);
  CHECK_THAT(keys, Catch::Matchers::RangeEquals(kvs_in | std::views::keys));
 }

 SECTION("paged scan runs with transaction options") {
  const auto result = lfdb::scan(j.dbh(), options, selector, lfdb::page {5});

  CHECK(result.has_more);
  CHECK_THAT(result.rows,
             Catch::Matchers::RangeEquals(first_keys(kvs_in, 5)));
 }

 SECTION("direct managed reads accept transaction options") {
  const auto key = make_key(0);
  std::string value;

  REQUIRE(lfdb::get(j.dbh(), options, key, value));
  CHECK(std::begin(kvs_in)->second == value);
  CHECK(lfdb::key_exists(j.dbh(), options, key));
  CHECK(key == lfdb::get_key(j.dbh(), options, lfdb::ceiling(key)));
  CHECK(0 <= lfdb::approximate_range_size(j.dbh(), options, selector));

  std::vector<string_pair> rows;
  CHECK(nkeys == lfdb::get(j.dbh(), options, selector, rows));
  CHECK_THAT(rows, Catch::Matchers::RangeEquals(kvs_in));

  std::vector<string_pair> iterated_rows;
  CHECK(nkeys == lfdb::get(j.dbh(), options, selector,
                           std::back_inserter(iterated_rows)));
  CHECK_THAT(iterated_rows, Catch::Matchers::RangeEquals(kvs_in));
 }
}

TEST_CASE("managed write helpers accept transaction options", "[fdb]")
{
 janitor j;

 const lfdb::transaction_options options {
  {FDB_TR_OPTION_PRIORITY_BATCH, lfdb::option_flag}
 };
 const auto prefix = make_key_prefix("managed-write-options");

 SECTION("set overloads") {
  const auto scalar_key = prefix + "scalar";
  const auto range_key = prefix + "range";
  const auto iterator_key = prefix + "iterator";

  lfdb::set(j.dbh(), options, scalar_key, "scalar-value");

  const std::vector<string_pair> range_values {{range_key, "range-value"}};
  lfdb::set(j.dbh(), options, range_values);

  const std::vector<string_pair> iterator_values {
   {iterator_key, "iterator-value"}
  };
  lfdb::set(j.dbh(), options,
            std::begin(iterator_values), std::end(iterator_values));

  std::string out;
  REQUIRE(lfdb::get(j, scalar_key, out));
  CHECK("scalar-value" == out);
  REQUIRE(lfdb::get(j, range_key, out));
  CHECK("range-value" == out);
  REQUIRE(lfdb::get(j, iterator_key, out));
  CHECK("iterator-value" == out);
 }

 SECTION("atomic mutation overloads") {
  const auto integer_key = prefix + "integer";
  const auto bytes_key = prefix + "bytes";

  lfdb::atomic::add(j.dbh(), options, integer_key, std::uint64_t {5});
  lfdb::atomic::max(j.dbh(), options, integer_key, std::uint64_t {10});
  lfdb::atomic::min(j.dbh(), options, integer_key, std::uint64_t {7});
  lfdb::atomic::bit_or(j.dbh(), options, integer_key, std::uint64_t {8});
  lfdb::atomic::bit_xor(j.dbh(), options, integer_key, std::uint64_t {3});
  lfdb::atomic::bit_and(j.dbh(), options, integer_key, std::uint64_t {10});

  lfdb::atomic::byte_max(j.dbh(), options, bytes_key, std::string_view("b"));
  lfdb::atomic::byte_min(j.dbh(), options, bytes_key, std::string_view("a"));
  lfdb::atomic::byte_max(j.dbh(), options, bytes_key, std::string_view("c"));
  lfdb::atomic::append_if_fits(
   j.dbh(), options, bytes_key, std::string_view("-tail"));
  lfdb::atomic::compare_and_clear(
   j.dbh(), options, bytes_key, std::string_view("not-the-value"));

  const auto integer_value = read_raw_fdb_value(j, integer_key);
  CHECK(8 == decode_little_endian<std::uint64_t>(integer_value));
  CHECK(std::ranges::equal(
   raw_bytes("c-tail"), read_raw_fdb_value(j, bytes_key)));

  lfdb::atomic::compare_and_clear(
   j.dbh(), options, bytes_key, std::string_view("c-tail"));
  CHECK_FALSE(lfdb::key_exists(j, bytes_key));
 }

 SECTION("erase overloads") {
  const auto first = prefix + "erase/first";
  const auto second = prefix + "erase/second";
  const auto third = prefix + "erase/third";

  lfdb::set(j, first, "first");
  lfdb::set(j, second, "second");
  lfdb::set(j, third, "third");

  lfdb::erase(j.dbh(), options, first);
  lfdb::erase(j.dbh(), options, lfdb::select {second, third});

  CHECK_FALSE(lfdb::key_exists(j, first));
  CHECK_FALSE(lfdb::key_exists(j, second));
  CHECK(lfdb::key_exists(j, third));
 }

 SECTION("erase_if") {
  const auto first = prefix + "erase-if/first";
  const auto second = prefix + "erase-if/second";

  lfdb::set(j, first, "first");
  lfdb::set(j, second, "second");

  const auto removed = lfdb::erase_if(
   j.dbh(), options, lfdb::select {first, prefix + "erase-if0"},
   [&first](const string_pair& row) { return first == row.first; });

  CHECK(1 == removed);
  CHECK_FALSE(lfdb::key_exists(j, first));
  CHECK(lfdb::key_exists(j, second));
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

    lfdb::transform(txn, lfdb::select { make_key(0), make_key(nkeys) }, std::back_inserter(keys),
                    [](string_pair&& row) {
                     return std::move(row.first);
                    });

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

    lfdb::transform(j, lfdb::select { make_key(0), make_key(nkeys) }, std::back_inserter(keys),
                    [](string_pair&& row) {
                     return std::move(row.first);
                    });

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
    REQUIRE_NOTHROW(lfdb::commit(txn));

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
    ceph::libfdb::detail::as_byte_view(make_key(3, prefix)),
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
    ceph::libfdb::detail::as_byte_view(make_key(0, prefix)),
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
  CHECK_FALSE(lfdb::commit(lfdb::with_result, serializable_txn));
  CHECK_FALSE(lfdb::key_exists(j, write_key));

  auto snapshot_txn = lfdb::make_transaction(j);
  REQUIRE(lfdb::get(snapshot_txn, read_key, out, lfdb::read_mode::snapshot));
  lfdb::set(j, read_key, "newer");
  lfdb::set(snapshot_txn, write_key, "snapshot-side-effect");
  CHECK_NOTHROW(lfdb::commit(snapshot_txn));
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
  CHECK_FALSE(lfdb::commit(lfdb::with_result, serializable_txn));
  CHECK_FALSE(lfdb::key_exists(j, write_key));

  auto snapshot_txn = lfdb::make_transaction(j);
  REQUIRE_FALSE(lfdb::collect(snapshot_txn, selector, lfdb::read_mode::snapshot).empty());
  lfdb::set(j, conflict_key, "newer");
  lfdb::set(snapshot_txn, write_key, "snapshot-side-effect");
  CHECK_NOTHROW(lfdb::commit(snapshot_txn));
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

  CHECK_NOTHROW(lfdb::commit(snapshot_txn));
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
  lfdb::for_each(snapshot_txn, selector, lfdb::read_mode::snapshot,
                 [&visited](string_pair&&) {
                  ++visited;
                 });
  REQUIRE(1 == visited);

  const auto values = lfdb::transform(snapshot_txn, selector, lfdb::read_mode::snapshot,
                                      [](string_pair&& row) {
                                       return std::move(row.second);
                                      });
  REQUIRE_THAT(values, Catch::Matchers::Equals(std::vector<std::string>{ "old" }));

  lfdb::set(j, conflict_key, "new");
  lfdb::set(snapshot_txn, write_key, "snapshot-side-effect");

  CHECK_NOTHROW(lfdb::commit(snapshot_txn));
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

  CHECK_NOTHROW(lfdb::commit(snapshot_txn));
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
  CHECK_NOTHROW(lfdb::commit(snapshot_txn));
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

  CHECK_FALSE(lfdb::commit(lfdb::with_result, txn));
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
  REQUIRE_NOTHROW(lfdb::commit(write_txn));

  lfdb::set(read_txn, side_effect_key, "side-effect");

  CHECK_FALSE(lfdb::commit(lfdb::with_result, read_txn));
  CHECK_FALSE(lfdb::key_exists(j, side_effect_key));
 }

 SECTION("selector and query overloads normalize to conflict ranges") {
  constexpr auto prefix = "conflict-selector";
  const auto lower_key = make_key(0, prefix);
  const auto middle = make_key(1, prefix);
  const auto upper_key = make_key(2, prefix);
  const auto side_effect_key = make_key(99, prefix);

  lfdb::set(j, middle, "old");

  auto selector_txn = lfdb::make_transaction(j);

  lfdb::mark_conflict_read(selector_txn, lfdb::select { lower_key, upper_key });
  lfdb::set(j, middle, "new");
  lfdb::set(selector_txn, side_effect_key, "side-effect");

  CHECK_FALSE(lfdb::commit(lfdb::with_result, selector_txn));
  CHECK_FALSE(lfdb::key_exists(j, side_effect_key));

  auto query_txn = lfdb::make_transaction(j);

  lfdb::mark_conflict_read(query_txn, lfdb::query::prefix(make_key_prefix(prefix)));
  lfdb::set(j, middle, "newer");
  lfdb::set(query_txn, side_effect_key, "query-side-effect");

  CHECK_FALSE(lfdb::commit(lfdb::with_result, query_txn));
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

  CHECK_FALSE(lfdb::commit(lfdb::with_result, txn));
  CHECK_FALSE(lfdb::key_exists(j, side_effect_key));
 }

 SECTION("empty conflict ranges are rejected") {
  const auto key = make_key(0, "conflict-empty");

  auto txn = lfdb::make_transaction(j);

  CHECK_THROWS_MATCHES(lfdb::mark_conflict_read(txn, lfdb::select { key, key }),
                       lfdb::libfdb_exception,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring(
                         "conflict expression must contain a non-empty range")));

  CHECK_THROWS_MATCHES(lfdb::mark_conflict_write(txn, lfdb::query::prefix(std::string("\xFF", 1))),
                       lfdb::libfdb_exception,
                       Catch::Matchers::MessageMatches(
                        Catch::Matchers::ContainsSubstring(
                         "conflict expression must contain a non-empty range")));
 }
}

TEST_CASE("FoundationDB integer encoding is little endian", "[fdb]") {
 constexpr auto bytes = std::array<std::uint8_t, 4> {0x78, 0x56, 0x34, 0x12};
 constexpr auto short_bytes = std::array<std::uint8_t, 2> {0x78, 0x56};
 constexpr auto negative_one = std::array<std::uint8_t, 4> {0xff, 0xff, 0xff, 0xff};

 STATIC_REQUIRE(bytes == lfdb::detail::little_endian_bytes(std::uint32_t {0x12345678}));
 STATIC_REQUIRE(std::uint32_t {0x12345678} ==
                lfdb::detail::little_endian_integer<std::uint32_t>(bytes));
 STATIC_REQUIRE(std::uint32_t {0x5678} ==
                lfdb::detail::little_endian_integer<std::uint32_t>(short_bytes));
 STATIC_REQUIRE(negative_one == lfdb::detail::little_endian_bytes(std::int32_t {-1}));
}

TEST_CASE("atomic mutations", "[fdb]") {
 janitor j;

 SECTION("add mutates little-endian integer values") {
  const auto key = test_key("atomic/add");

  lfdb::atomic::add(j, key, std::uint64_t{5});

  auto txn = lfdb::make_transaction(j);

  lfdb::atomic::add(txn, key, std::int64_t{-2});
  REQUIRE_NOTHROW(lfdb::commit(txn));

  const auto value = read_raw_fdb_value(j, key);

  REQUIRE(sizeof(std::uint64_t) == std::size(value));
  CHECK(3 == decode_little_endian<std::uint64_t>(value));
 }

 SECTION("as_fdb_integer decodes FoundationDB integer values") {
  const auto key = test_key("fdb-integer/get");
  auto value = std::uint64_t {42};

  CHECK_FALSE(lfdb::get(j, key, lfdb::as_fdb_integer(value)));
  CHECK(42 == value);

  lfdb::atomic::add(j, key, std::uint64_t{5});
  CHECK(lfdb::get(j, key, lfdb::as_fdb_integer(value)));
  CHECK(5 == value);

  auto txn = lfdb::make_transaction(j);

  lfdb::atomic::add(txn, key, std::uint64_t{7});
  REQUIRE_NOTHROW(lfdb::commit(txn));
  CHECK(lfdb::get(j, key, lfdb::as_fdb_integer(value)));
  CHECK(12 == value);
 }

 SECTION("add does not require a read conflict") {
  const auto key = test_key("atomic/add/concurrent");

  auto first_txn = lfdb::make_transaction(j);
  auto second_txn = lfdb::make_transaction(j);

  lfdb::atomic::add(first_txn, key, std::uint64_t{1});
  lfdb::atomic::add(second_txn, key, std::uint64_t{1});

  CHECK_NOTHROW(lfdb::commit(first_txn));
  CHECK_NOTHROW(lfdb::commit(second_txn));

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

TEST_CASE("split ranges follow selector direction", "[fdb]")
{
 const std::array keys {"a"s, "b"s, "c"s, "d"s};
 auto make_fdb_key = [](const std::string& key) {
  const auto bytes = lfdb::detail::as_byte_view(key);

  return FDBKey {bytes.data(), static_cast<int>(std::size(bytes))};
 };
 const std::array split_points {
  make_fdb_key(keys[0]), make_fdb_key(keys[1]),
  make_fdb_key(keys[2]), make_fdb_key(keys[3])
 };
 auto selector = lfdb::select {
  lfdb::exclusive(keys.front()), lfdb::inclusive(keys.back())
 };
 selector.options = {
  .result_limit = 17,
  .target_bytes = 4'096,
  .streaming_mode = FDB_STREAMING_MODE_WANT_ALL
 };

 CHECK(std::empty(lfdb::detail::select_ranges_from_split_points(
   std::span<const FDBKey> {}, selector)));
 CHECK(std::empty(lfdb::detail::select_ranges_from_split_points(
   std::span {split_points}.first<1>(), selector)));

 const auto forward = lfdb::detail::select_ranges_from_split_points(
  split_points, selector);

 REQUIRE(3 == std::size(forward));
 CHECK("a" == forward[0].begin_key);
 CHECK("b" == forward[1].begin_key);
 CHECK("c" == forward[2].begin_key);
 CHECK_FALSE(forward.front().begin_inclusive);
 CHECK_FALSE(forward.front().end_inclusive);
 CHECK(forward[1].begin_inclusive);
 CHECK_FALSE(forward[1].end_inclusive);
 CHECK(forward.back().begin_inclusive);
 CHECK(forward.back().end_inclusive);
 CHECK(std::ranges::all_of(forward, [&selector](const auto& range) {
  return selector.options == range.options;
 }));

 selector.options.reverse_order = true;

 const auto reverse = lfdb::detail::select_ranges_from_split_points(
  split_points, selector);

 REQUIRE(3 == std::size(reverse));
 CHECK("c" == reverse[0].begin_key);
 CHECK("b" == reverse[1].begin_key);
 CHECK("a" == reverse[2].begin_key);
 CHECK(reverse.front().end_inclusive);
 CHECK(reverse.front().begin_inclusive);
 CHECK_FALSE(reverse[1].end_inclusive);
 CHECK(reverse[1].begin_inclusive);
 CHECK_FALSE(reverse.back().begin_inclusive);
 CHECK_FALSE(reverse.back().end_inclusive);
 CHECK(std::ranges::all_of(reverse, [&selector](const auto& range) {
  return selector.options == range.options;
 }));
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

TEST_CASE("transactors reject invalid database handles", "[fdb]")
{
 lfdb::database_handle dbh;

 SECTION("direct transaction") {
  CHECK_THROWS_WITH(lfdb::transaction {dbh},
                    Catch::Matchers::ContainsSubstring("transaction requires a database handle"));
 }

 SECTION("default options") {
  auto txr = lfdb::make_transactor(dbh);

  CHECK_THROWS_WITH(txr([](auto) {}),
                    Catch::Matchers::ContainsSubstring("transaction requires a database handle"));
 }

 SECTION("explicit options") {
  lfdb::transaction_options opts;
  auto txr = lfdb::make_transactor(dbh, opts);

  CHECK_THROWS_WITH(txr([](auto) {}),
                    Catch::Matchers::ContainsSubstring("transaction requires a database handle"));
 }

 SECTION("duplicate staged target") {
  auto txr = lfdb::make_transactor(dbh);
  int output = 0;

  CHECK_THROWS_WITH(txr([](auto, auto&, auto&) {},
                        lfdb::staged(output), lfdb::staged(output)),
                    Catch::Matchers::ContainsSubstring("cannot stage one target more than once"));
  CHECK_THROWS_WITH(txr([](auto, auto&, auto&) {},
                        lfdb::staged(output),
                        lfdb::staged(output, std::in_place)),
                    Catch::Matchers::ContainsSubstring("cannot stage one target more than once"));
 }
}

SCENARIO("transactor", "[fdb]")
{
 janitor j;

 static_assert(lfdb::concepts::stageable<std::vector<int>>);
 static_assert(lfdb::concepts::in_place_stageable<std::unique_ptr<int>>);
 static_assert(not lfdb::concepts::stageable<std::unique_ptr<int>>);
 static_assert(not lfdb::concepts::supported_invocation_result<
   lfdb::staged_proxy<int>>);
 static_assert(std::same_as<void, decltype(
   std::declval<lfdb::staged_proxy<std::vector<int>>&>().push_back(1))>);
 static_assert(std::same_as<void, decltype(
   std::declval<lfdb::staged_proxy<std::vector<int>>&>().emplace_back(1))>);
 static_assert(std::same_as<void, decltype(
   std::declval<lfdb::staged_proxy<std::map<int, int>>&>().insert_or_assign(
     1, 2))>);
 static_assert(not std::invocable<lfdb::transactor&, lfdb::with_result_t,
                                  decltype([](auto) {})>);
 static_assert(not std::invocable<lfdb::transactor&, lfdb::with_result_t,
                                  decltype([](auto, int&) {}), int>);
 static_assert(lfdb::detail::transaction_option_is_persistent(
  FDB_TR_OPTION_TIMEOUT));
 static_assert(lfdb::detail::transaction_option_is_persistent(
  FDB_TR_OPTION_RETRY_LIMIT));
 static_assert(lfdb::detail::transaction_option_is_persistent(
  FDB_TR_OPTION_MAX_RETRY_DELAY));
 static_assert(lfdb::detail::transaction_option_is_persistent(
  FDB_TR_OPTION_AUTHORIZATION_TOKEN));
 static_assert(not lfdb::detail::transaction_option_is_persistent(
  FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE));

 SECTION("transaction source configures every new transaction") {
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag}
  };

  const lfdb::detail::transaction_source source(j.dbh(), options);

  for (const auto suffix : {"first", "second"}) {
   const auto key = test_key(fmt::format("source-options-{}", suffix));
   auto txn = source.make();

   lfdb::set(txn, key, "value");

   std::string out;

   CHECK_FALSE(lfdb::get(txn, key, out));
  }
 }

 SECTION("reset_for_replay handles retryable errors") {
  constexpr fdb_error_t not_committed = 1020;
  REQUIRE(0 != fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, not_committed));

  auto txn = lfdb::make_transaction(j);

  CHECK_NOTHROW(lfdb::reset_for_replay(txn, lfdb::fdb_error_code::not_committed));

  const auto key = test_key("reset-for-replay");
  lfdb::set(txn, key, "value");

  CHECK_NOTHROW(lfdb::commit(txn));
  CHECK(lfdb::key_exists(j, key));
 }

 SECTION("reset_for_replay restores nonpersistent transaction options") {
  constexpr fdb_error_t not_committed = 1020;
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag}
  };

  const auto key = test_key("reset-options");
  auto txn = lfdb::make_transaction(j, options);

  lfdb::reset_for_replay(txn, lfdb::fdb_error_code::not_committed);
  lfdb::set(txn, key, "value");

  std::string out;

  CHECK_FALSE(lfdb::get(txn, key, out));
  CHECK_NOTHROW(lfdb::commit(txn));
  CHECK(lfdb::key_exists(j, key));
 }

 SECTION("repeated flags survive transaction snapshots and every replay") {
  lfdb::transaction_options options {
   {FDB_TR_OPTION_SNAPSHOT_RYW_DISABLE, lfdb::option_flag},
   {FDB_TR_OPTION_SNAPSHOT_RYW_ENABLE, lfdb::option_flag},
   {FDB_TR_OPTION_SNAPSHOT_RYW_DISABLE, lfdb::option_flag}
  };
  const lfdb::detail::transaction_source source(j.dbh(), options);
  options.clear();

  for (const auto suffix : {"first", "second"}) {
   auto txn = source.make();
   const auto key = test_key(fmt::format("repeated-options-{}", suffix));

   for (int attempt = 0; attempt < 3; ++attempt) {
    CAPTURE(suffix, attempt);
    lfdb::set(txn, key, "pending");
    std::string out;

    CHECK_FALSE(lfdb::get(txn, key, out, lfdb::read_mode::snapshot));
    CHECK(lfdb::get(txn, key, out));
    lfdb::reset_for_replay(txn, lfdb::fdb_error_code::not_committed);
   }
  }
 }

 SECTION("reset_for_replay discards failed-attempt version stamps") {
  auto txn = lfdb::make_transaction(j);
  lfdb::versionstamp abandoned_stamp;

  lfdb::set(txn, test_key("abandoned-versionstamp"),
            lfdb::versioned("", abandoned_stamp));
  lfdb::reset_for_replay(txn, lfdb::fdb_error_code::not_committed);

  lfdb::set(txn, test_key("replayed-without-versionstamp"), "value");
  REQUIRE_NOTHROW(lfdb::commit(txn));
  CHECK_FALSE(abandoned_stamp.has_value());
 }

 SECTION("reset_for_replay rejects non-retryable errors") {
  constexpr fdb_error_t operation_cancelled = 1101;
  REQUIRE_FALSE(fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, operation_cancelled));

  auto txn = lfdb::make_transaction(j);

  CHECK_THROWS_AS(lfdb::reset_for_replay(txn, lfdb::fdb_error_code::operation_cancelled), lfdb::libfdb_exception);
 }

 SECTION("reset_for_replay requires an error") {
  auto txn = lfdb::make_transaction(j);

  CHECK_THROWS_AS(lfdb::reset_for_replay(txn, std::error_code {}), lfdb::libfdb_exception);
 }

 SECTION("reset_for_replay preserves a changed preparation error") {
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_TIMEOUT, std::int64_t {1}}
  };
  auto txn = lfdb::make_transaction(j, options);
  lfdb::detail::future_value read(fdb_transaction_get_read_version(txn->raw_handle()));
  std::ignore = fdb_future_block_until_ready(read.raw_handle());
  std::this_thread::sleep_for(20ms);

  try {
   lfdb::reset_for_replay(txn, lfdb::fdb_error_code::not_committed);
   FAIL("expired transaction was unexpectedly prepared for replay");
  } catch (const lfdb::libfdb_exception& error) {
   CHECK(lfdb::fdb_error_code::transaction_timed_out == error.code());
  }
 }

 SECTION("direct commit returns terminal errors as values or libfdb_exception") {
  const auto tagged = GENERATE(false, true);
  auto txn = lfdb::make_transaction(j);
  lfdb::versionstamp stamp;
  lfdb::set(txn, test_key("terminal-commit"), lfdb::versioned("", stamp));
  fdb_transaction_cancel(txn->raw_handle());

  if (tagged) {
   const auto result = lfdb::commit(lfdb::with_result, txn);
   REQUIRE_FALSE(result);
   CHECK(lfdb::fdb_error_code::transaction_cancelled == result.error());
  } else {
   try {
    lfdb::commit(txn);
    FAIL("cancelled transaction was unexpectedly committed");
   } catch (const lfdb::libfdb_exception& error) {
    CHECK(lfdb::fdb_error_code::transaction_cancelled == error.code());
   }
  }

  CHECK(static_cast<bool>(*txn));
  txn.reset();
  REQUIRE(stamp.is_ready());
  CHECK(lfdb::fdb_error_code::transaction_cancelled ==
        stamp.resolved_bytes(lfdb::with_result).error());
 }

 SECTION("successful commit ends ordinary transaction work") {
  auto txn = lfdb::make_transaction(j);

  lfdb::set(txn, test_key("lifecycle"), "value");
  REQUIRE_NOTHROW(lfdb::commit(txn));

  CHECK(0 < lfdb::committed_version(txn));
  CHECK_THROWS_AS(lfdb::reset_for_replay(txn, lfdb::fdb_error_code::not_committed), lfdb::libfdb_exception);
 }

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

 SECTION("transactor restores options before replay") {
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag}
  };

  const auto conflict_key = test_key("transactor-option-conflict");
  const auto output_key = test_key("transactor-option-output");
  auto txr = lfdb::make_transactor(j, options);
  std::size_t attempts = 0;

  lfdb::set(j, conflict_key, "initial");

  txr([&](auto txn) {
   ++attempts;

   std::string conflict_value;

   REQUIRE(lfdb::get(txn, conflict_key, conflict_value));

   lfdb::set(txn, output_key, "value");

   std::string hidden_value;

   CHECK_FALSE(lfdb::get(txn, output_key, hidden_value));

   if ("initial" == conflict_value) {
    lfdb::set(j, conflict_key, "conflict");
   }

   lfdb::set(txn, conflict_key, "final");
  });

  CHECK(2 == attempts);

  std::string out;

  REQUIRE(lfdb::get(j, output_key, out));
  CHECK("value" == out);
 }

 SECTION("direct commit can report success") {
  const auto key = test_key("direct-result-key");

  auto txn = lfdb::make_transaction(j);
  lfdb::set(txn, key, "value");

  const auto result = lfdb::commit(lfdb::with_result, txn);
  CHECK(result.has_value());

  std::string out;
  CHECK(lfdb::get(j, key, out));
  CHECK("value" == out);
 }

 SECTION("direct commit reports failure without preparing replay") {
  const auto key = test_key("direct-result-conflict-key");

  lfdb::set(j, key, "initial");

  auto txn = lfdb::make_transaction(j);

  std::string out;
  REQUIRE(lfdb::get(txn, key, out));
  CHECK("initial" == out);

  lfdb::set(j, key, "conflict");
  lfdb::set(txn, key, "final");

  auto result = lfdb::commit(lfdb::with_result, txn);
  REQUIRE_FALSE(result);
  CHECK(lfdb::fdb_error_code::not_committed == result.error());
  CHECK_THROWS_AS(lfdb::commit(lfdb::with_result, txn), lfdb::libfdb_exception);

  lfdb::reset_for_replay(txn, result.error());

  REQUIRE(lfdb::get(txn, key, out));
  CHECK("conflict" == out);

  lfdb::set(txn, key, "final");

  result = lfdb::commit(lfdb::with_result, txn);
  CHECK(result.has_value());

  CHECK(lfdb::get(j, key, out));
  CHECK("final" == out);
 }

 SECTION("commit replay restores nonpersistent transaction options") {
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag}
  };

  const auto conflict_key = test_key("option-replay-conflict");
  const auto output_key = test_key("option-replay-output");

  lfdb::set(j, conflict_key, "initial");

  auto txn = lfdb::make_transaction(j, options);
  std::string out;

  REQUIRE(lfdb::get(txn, conflict_key, out));

  lfdb::set(j, conflict_key, "conflict");
  lfdb::set(txn, output_key, "first-attempt");

  const auto result = lfdb::commit(lfdb::with_result, txn);
  REQUIRE_FALSE(result);
  lfdb::reset_for_replay(txn, result.error());

  lfdb::set(txn, output_key, "replay");
  CHECK_FALSE(lfdb::get(txn, output_key, out));
  CHECK_NOTHROW(lfdb::commit(txn));

  REQUIRE(lfdb::get(j, output_key, out));
  CHECK("replay" == out);
 }

 SECTION("transactor returns an application value after commit") {
  auto txr = lfdb::make_transactor(j);
  const auto key = test_key("result-key");

  const auto result = txr([&key](auto txn) {
    lfdb::set(txn, key, "value");

    return 42;
  });

  std::string out;
  CHECK(lfdb::get(j, key, out));
  CHECK("value" == out);

  CHECK(42 == result);
 }

 SECTION("transactor accepts stable invocation arguments") {
  auto txr = lfdb::make_transactor(j);
  const auto key = test_key("argument-key");
  const std::string value = "argument-value";

  txr([](auto txn, const std::string& key, const std::string& value) {
    lfdb::set(txn, key, value);
  }, key, value);

  std::string out;
  CHECK(lfdb::get(j, key, out));
  CHECK(value == out);
 }

 SECTION("staged arguments publish one successful attempt") {
  auto txr = lfdb::make_transactor(j);
  const auto key = test_key("staged-conflict-key");

  lfdb::set(j, key, "initial");

  std::vector<std::string> values {"before"};
  txr(
    [&j, &key](auto txn, auto& staged_values) {
      std::string value;
      if (not lfdb::get(txn, key, value)) {
       throw std::runtime_error("expected key does not exist");
      }

      staged_values.push_back(value);

      // Force the first attempt to conflict:
      if ("initial" == value) {
       lfdb::set(j, key, "conflict");
      }

      lfdb::set(txn, key, "final");
    }, lfdb::staged(values));

  CHECK((std::vector<std::string> {"before", "conflict"} == values));
 }

 SECTION("in-place staged arguments start fresh on every attempt") {
  auto txr = lfdb::make_transactor(j);
  const auto key = test_key("in-place-staged-conflict-key");

  lfdb::set(j, key, "initial");

  std::vector<std::string> values {"before"};
  txr(
    [&j, &key](auto txn, auto& staged_values) {
      std::string value;
      if (not lfdb::get(txn, key, value)) {
       throw std::runtime_error("expected key does not exist");
      }

      staged_values.push_back(value);

      // Force the first attempt to conflict:
      if ("initial" == value) {
       lfdb::set(j, key, "conflict");
      }

      lfdb::set(txn, key, "final");
    }, lfdb::staged(values, std::in_place));

  CHECK((std::vector<std::string> {"conflict"} == values));
 }

 SECTION("an empty in-place result replaces the target") {
  auto txr = lfdb::make_transactor(j);
  std::vector<int> values {1, 2, 3};

  txr([](auto, auto&) {}, lfdb::staged(values, std::in_place));

  CHECK(std::empty(values));
 }

 SECTION("in-place staging does not require a copyable target") {
  auto txr = lfdb::make_transactor(j);
  auto value = std::make_unique<int>(5);

  txr([](auto, auto& staged_value) {
    staged_value = std::make_unique<int>(7);
  }, lfdb::staged(value, std::in_place));

  REQUIRE(value);
  CHECK(7 == *value);
 }

 SECTION("in-place staged arguments remain unchanged after a body exception") {
  auto txr = lfdb::make_transactor(j);
  std::vector<int> values {1, 2, 3};

  CHECK_THROWS_WITH(txr([](auto, auto& staged_values) {
    staged_values.push_back(4);
    throw std::runtime_error("transaction body failed");
  }, lfdb::staged(values, std::in_place)), "transaction body failed");

  CHECK((std::vector {1, 2, 3} == values));
 }

 SECTION("staged arguments remain unchanged after retry exhaustion") {
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {2}},
   {FDB_TR_OPTION_MAX_RETRY_DELAY, std::int64_t {1}}
  };
  auto txr = lfdb::make_transactor(j, options);
  const auto key = test_key("staged-exhaustion-key");

  lfdb::set(j, key, "initial");

  std::vector<std::string> values {"before"};
  CHECK_THROWS_AS(txr(
    [&j, &key](auto txn, auto& staged_values) {
      std::string value;
      if (not lfdb::get(txn, key, value)) {
       throw std::runtime_error("expected key does not exist");
      }

      staged_values.push_back(value);

      // Force every attempt to conflict:
      lfdb::set(j, key, "conflict");
      lfdb::set(txn, key, "final");
    }, lfdb::staged(values)), lfdb::libfdb_exception);

  CHECK(std::vector<std::string> {"before"} == values);
 }

 SECTION("ordinary transactors publish staged arguments") {
  auto txr = lfdb::make_transactor(j);
  std::uint64_t total = 5;
  std::vector<int> values {0};
  std::map<std::string, int> index;

  const auto answer = txr([](auto,
                             auto& staged_total,
                             auto& staged_values,
                             auto& staged_index) {
    staged_total = 7;
    staged_total += 5;

    staged_values.clear();
    staged_values.emplace_back(1);
    staged_values.push_back(2);

    staged_index.insert_or_assign("answer", 42);

    return 42;
  }, lfdb::staged(total), lfdb::staged(values), lfdb::staged(index));

  CHECK(42 == answer);
  CHECK(12 == total);
  CHECK((std::vector {1, 2} == values));
  CHECK(42 == index.at("answer"));
 }

 SECTION("staged arguments remain unchanged after a body exception") {
  auto txr = lfdb::make_transactor(j);
  int value = 5;

  CHECK_THROWS_WITH(txr([](auto, auto& staged_value) {
    staged_value += 7;
    throw std::runtime_error("transaction body failed");
  }, lfdb::staged(value)), "transaction body failed");

  CHECK(5 == value);
 }

 SECTION("transactor returns the successful attempt value after conflict") {
  auto txr = lfdb::make_transactor(j);
  const auto key = test_key("result-conflict-key");

  lfdb::set(j, key, "initial");

  const auto result = txr([&j, &key](auto txn) {
    std::string out;
    if (not lfdb::get(txn, key, out)) {
     throw std::runtime_error("expected key does not exist");
    }

    // Force a conflict, making the transactor replay the body:
    if ("initial" == out) {
     lfdb::set(j, key, "conflict");
    }

    lfdb::set(txn, key, "final");

    return out;
  });

  std::string out;
  CHECK(lfdb::get(j, key, out));
  CHECK("final" == out);

  CHECK("conflict" == result);
 }

 SECTION("transactor throws after retry exhaustion") {
  const lfdb::transaction_options options {
   {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {2}},
   {FDB_TR_OPTION_MAX_RETRY_DELAY, std::int64_t {1}}
  };
  auto txr = lfdb::make_transactor(j, options);
  const auto key = test_key("result-retry-limit-key");

  lfdb::set(j, key, "initial");

  CHECK_THROWS_AS(txr([&j, &key](auto txn) {
    std::string out;
    if (not lfdb::get(txn, key, out)) {
     throw std::runtime_error("expected key does not exist");
    }

    // Force every commit attempt to conflict:
    lfdb::set(j, key, "conflict");
    lfdb::set(txn, key, "final");
  }), lfdb::libfdb_exception);

  std::string out;
  CHECK(lfdb::get(j, key, out));
  CHECK("conflict" == out);
 }

 SECTION("transactor propagates transaction body exceptions") {
  auto txr = lfdb::make_transactor(j);

  CHECK_THROWS_WITH(txr([](auto) {
    throw std::runtime_error("transaction body failed");
  }), "transaction body failed");
 }
}

TEST_CASE("transactors and read helpers honor native retry budgets", "[fdb]")
{
 const auto retry_limit = GENERATE(0, 2, 12);
 const auto error = GENERATE(1020, 1021);
 const auto read_only = GENERATE(false, true);
 janitor j;
 const lfdb::transaction_options options {
  {FDB_TR_OPTION_RETRY_LIMIT, std::int64_t {retry_limit}},
  {FDB_TR_OPTION_MAX_RETRY_DELAY, std::int64_t {1}}
 };
 std::size_t attempts = 0;
 auto body = [&attempts, error](auto&) {
  ++attempts;
  throw lfdb::libfdb_exception {std::error_code(error, lfdb::fdb_error_category())};
 };

 try {
  if (read_only) {
   lfdb::detail::in_read_transaction(j, options, body);
  } else {
   lfdb::make_transactor(j, options)(body);
  }

  FAIL("exhausted transaction unexpectedly succeeded");
 } catch (const lfdb::libfdb_exception& failure) {
  CHECK(std::error_code(error, lfdb::fdb_error_category()) == failure.code());
 }

 CHECK(1 + retry_limit == attempts);
}

TEST_CASE("staged proxy benchmarks", "[.benchmark][benchmark][fdb][staged]")
{
 const std::vector<int> initial_values(64, 7);
 const std::string large_value(256, 'x');
 const std::vector<std::string> large_initial_values(256, large_value);

 auto append_values = [](auto& values) {
  for (const auto value : std::views::iota(0, 64)) {
   values.push_back(value);
  }
 };

 BENCHMARK("direct mutation without replay isolation")
 {
  auto values = initial_values;
  append_values(values);

  return values;
 };

 BENCHMARK("manual replay-isolated mutation")
 {
  auto values = initial_values;
  auto attempt_values = values;

  append_values(attempt_values);
  values = std::move(attempt_values);

  return values;
 };

 BENCHMARK("manual replay-isolated replacement")
 {
  auto values = initial_values;
  std::vector<int> attempt_values;

  append_values(attempt_values);
  values = std::move(attempt_values);

  return values;
 };

 BENCHMARK("staged proxy replay-isolated mutation")
 {
  auto values = initial_values;
  auto proxy = lfdb::staged(values);

  lfdb::detail::validate_staged_arguments(proxy);
  lfdb::detail::staged_access::prepare_attempt(proxy);
  append_values(proxy);
  lfdb::detail::staged_access::publish(proxy);

  return values;
 };

 BENCHMARK("in-place staged proxy replacement")
 {
  auto values = initial_values;
  auto proxy = lfdb::staged(values, std::in_place);

  lfdb::detail::validate_staged_arguments(proxy);
  lfdb::detail::staged_access::prepare_attempt(proxy);
  append_values(proxy);
  lfdb::detail::staged_access::publish(proxy);

  return values;
 };

 auto rebuild_large_output = [&](auto& values) {
  values.clear();
  values.get_target().reserve(std::size(large_initial_values));

  for (auto remaining = std::size(large_initial_values); remaining; --remaining) {
   values.push_back(large_value);
  }
 };

 BENCHMARK("copying staged proxy large replacement")
 {
  auto values = large_initial_values;
  auto proxy = lfdb::staged(values);

  lfdb::detail::validate_staged_arguments(proxy);
  lfdb::detail::staged_access::prepare_attempt(proxy);
  rebuild_large_output(proxy);
  lfdb::detail::staged_access::publish(proxy);

  return values;
 };

 BENCHMARK("in-place staged proxy large replacement")
 {
  auto values = large_initial_values;
  auto proxy = lfdb::staged(values, std::in_place);

  lfdb::detail::validate_staged_arguments(proxy);
  lfdb::detail::staged_access::prepare_attempt(proxy);
  rebuild_large_output(proxy);
  lfdb::detail::staged_access::publish(proxy);

  return values;
 };
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
  ov = std::vector<std::uint8_t> { 1, 2, 3 }; // data
 }

  auto dbh0 = lfdb::create_database(
                { { FDB_DB_OPTION_LOCATION_CACHE_SIZE, 200'000 } },  
                { { FDB_NET_OPTION_TRACE_ENABLE, lfdb::option_flag } });         

  auto dbh1 = lfdb::create_database(
                lfdb::connection_source{std::filesystem::path{"fishing for databass!"}},
                { { FDB_DB_OPTION_LOCATION_CACHE_SIZE, 200'000 } },      // database options
                { { FDB_NET_OPTION_TRACE_ENABLE, lfdb::option_flag } }); // network options
 
  auto txn = lfdb::make_transaction(dbh0, {
   {FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag},
   {FDB_TR_OPTION_TAG, "example"},
   {FDB_TR_OPTION_TAG, "options"}
  });

 SECTION("create_database()") {
  lfdb::create_database();
  lfdb::create_database(lfdb::connection_source{std::filesystem::path{"/dev/null"}});
  lfdb::create_database(lfdb::connection_source{std::filesystem::path{"/dev/null"}}, {}, {});
  lfdb::create_database(lfdb::database_options {}, lfdb::network_options {});

  CHECK_THROWS_AS(lfdb::connection_source{std::filesystem::path{}}, lfdb::libfdb_exception);
  CHECK_THROWS_AS(lfdb::connection_source{""}, lfdb::libfdb_exception);
 }

 SECTION("piecemeal construction") {
  lfdb::network_options netopts;

  // Network options apply only at the first FoundationDB network initialization.
  netopts.emplace_back(FDB_NET_OPTION_TRACE_LOG_GROUP, "libfdb-example");

  lfdb::create_database(lfdb::database_options {}, netopts);
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


static_assert(std::is_empty_v<lfdb::retry_if<>>);
using policy_returns_int = decltype([](const lfdb::transaction_failure&) { return 1; });
static_assert(not std::constructible_from<lfdb::retry_if<policy_returns_int>, policy_returns_int>);

TEST_CASE("transactor retry policy owns decisions and preserves failures", "[fdb][policy]")
{
 janitor dbh;
 auto run = lfdb::make_transactor(dbh);
 const auto key = test_key("retry-policy");
 int attempts = 0;
 int decisions = 0;

 SECTION("false preserves 1021 before preparation") {
  lfdb::transaction_handle retained;
  auto policy = [&](const lfdb::transaction_failure& failure) {
   ++decisions;
   CHECK(lfdb::fdb_error_code::commit_unknown_result == failure.code);
   CHECK(lfdb::transaction_phase::body == failure.phase);
   return false;
  };
  CHECK_THROWS_AS(run(lfdb::retry_if(std::ref(policy)), [&](auto txn) {
   ++attempts;
   retained = txn;
   lfdb::set(txn, key, "uncommitted");
   throw lfdb::libfdb_exception(lfdb::fdb_error_code::commit_unknown_result);
  }), lfdb::libfdb_exception);
  std::string value;
  REQUIRE(lfdb::get(retained, key, value));
  CHECK("uncommitted" == value);
  CHECK_FALSE(lfdb::key_exists(dbh, key));
  CHECK(1 == attempts);
  CHECK(1 == decisions);
 }

 SECTION("mutable move-only policy is reused with staged output") {
  int published = 0;
  const auto result = run(lfdb::retry_if(
   [state = std::make_unique<int>(0), &decisions](const lfdb::transaction_failure& failure) mutable {
    CHECK(lfdb::transaction_phase::body == failure.phase);
    decisions = ++*state;
    return true;
   }), [&](auto txn, auto& output) {
    output += 1;
    if (++attempts < 3) {
     throw lfdb::libfdb_exception(lfdb::fdb_error_code::not_committed);
    }
    lfdb::set(txn, key, "committed");
    return 42;
   }, lfdb::staged(published));
  CHECK(42 == result);
  CHECK(3 == attempts);
  CHECK(2 == decisions);
  CHECK(1 == published);
  CHECK(lfdb::key_exists(dbh, key));
 }

 SECTION("commit failure reaches policy before recovery") {
  lfdb::set(dbh, key, "initial");
  lfdb::transaction_handle retained;
  CHECK_THROWS_AS(run(lfdb::retry_if([&](const lfdb::transaction_failure& failure) {
   ++decisions;
   CHECK(lfdb::transaction_phase::commit == failure.phase);
   CHECK(lfdb::fdb_error_code::not_committed == failure.code);
   return false;
  }), [&](auto txn) {
   ++attempts;
   retained = txn;
   std::string value;
   REQUIRE(lfdb::get(txn, key, value));
   lfdb::set(dbh, key, "conflict");
   lfdb::set(txn, key, "uncommitted");
  }), lfdb::libfdb_exception);
  CHECK(1 == attempts);
  CHECK(1 == decisions);
  CHECK_THROWS_AS(lfdb::commit(retained), lfdb::libfdb_exception);
 }

 SECTION("true cannot force a terminal native replay") {
  CHECK_THROWS_AS(run(lfdb::retry_if([&](const lfdb::transaction_failure& failure) {
   ++decisions;
   CHECK(lfdb::fdb_error_code::operation_cancelled == failure.code);
   return true;
  }), [&](auto) {
   ++attempts;
   throw lfdb::libfdb_exception(lfdb::fdb_error_code::operation_cancelled);
  }), lfdb::libfdb_exception);
  CHECK(1 == attempts);
  CHECK(1 == decisions);
 }

 SECTION("policy exceptions bypass classification and keep identity") {
  const auto phase = GENERATE(lfdb::transaction_phase::body, lfdb::transaction_phase::commit);
  const auto error = std::make_exception_ptr(
   lfdb::libfdb_exception(lfdb::fdb_error_code::not_committed, "policy failure"));
  try {
   run(lfdb::retry_if([&](const lfdb::transaction_failure& failure) -> bool {
    ++decisions;
    CHECK(phase == failure.phase);
    std::rethrow_exception(error);
   }), [&](auto txn) {
    ++attempts;
    if (lfdb::transaction_phase::body == phase) {
     throw lfdb::libfdb_exception(lfdb::fdb_error_code::not_committed);
    }
    fdb_transaction_cancel(txn->raw_handle());
   });
   FAIL("policy exception was suppressed");
  } catch (...) {
   CHECK(error == std::current_exception());
  }
  CHECK(1 == attempts);
  CHECK(1 == decisions);
 }

 SECTION("local errors bypass policy and keep identity") {
  const auto error = std::make_exception_ptr(
   lfdb::libfdb_exception(std::error_code(1020, std::generic_category())));
  try {
   run(lfdb::retry_if([&](const lfdb::transaction_failure&) {
    ++decisions;
    return true;
   }), [&](auto) {
    ++attempts;
    std::rethrow_exception(error);
   });
   FAIL("local exception was suppressed");
  } catch (...) {
   CHECK(error == std::current_exception());
  }
  CHECK(1 == attempts);
  CHECK(0 == decisions);
 }

 SECTION("changed preparation error is terminal without another policy call") {
  auto timed = lfdb::make_transactor(dbh, lfdb::transaction_options {
   {FDB_TR_OPTION_TIMEOUT, std::int64_t {1}}
  });
  try {
   timed(lfdb::retry_if([&](const lfdb::transaction_failure& failure) {
    ++decisions;
    CHECK(lfdb::fdb_error_code::not_committed == failure.code);
    return true;
   }), [&](auto txn) {
    ++attempts;
    lfdb::detail::future_value read(fdb_transaction_get_read_version(txn->raw_handle()));
    std::ignore = fdb_future_block_until_ready(read.raw_handle());
    std::this_thread::sleep_for(20ms);
    throw lfdb::libfdb_exception(lfdb::fdb_error_code::not_committed);
   });
   FAIL("expired transaction unexpectedly replayed");
  } catch (const lfdb::libfdb_exception& failure) {
   CHECK(lfdb::fdb_error_code::transaction_timed_out == failure.code());
  }
  CHECK(1 == attempts);
  CHECK(1 == decisions);
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
