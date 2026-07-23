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

#include <map>
#include <list>
#include <chrono>
#include <vector>
#include <ranges>
#include <iterator>
#include <algorithm>
#include <unordered_map>

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

// Be nice to Catch2's template-test macros:
using string_pair = std::pair<std::string, std::string>;

// Collect values in selection to out_values:
auto key_counter(auto txn, const auto& selector, auto& out_values) -> auto {
 out_values.clear();

 lfdb::get(txn, selector, 
           std::inserter(out_values, std::end(out_values)));

 return out_values.size();
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

  lfdb::get(txn, lfdb::select { make_key(0), make_key(100) }, std::back_inserter(out_values), lfdb::commit_after_op::no_commit);

  CHECK(100 == out_values.size());

  // Maybe not the world's most creative test, but the idea is to try getting some random keys:
  for (auto i = ceph::util::generate_random_number(out_values.size() - 1); i; --i) {
    CHECK(std::end(out_values) != std::ranges::find(out_values, string_pair { make_key(i), make_value(i) }));
  }
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

 // Make sure that there's nothing in our test range:
 dbh.drop_test_namespace();
 REQUIRE(0 == key_count(dbh, select_all));

 const auto kvs = write_monotonic_kvs(dbh, nentries);

 // Make sure there's exactly as many entries as we added:
 REQUIRE(nentries == key_count(dbh, select_all));

 std::vector<std::pair<std::string, std::string>> out;
 lfdb::get(dbh, select_all, std::back_inserter(out));

 // These /are/ the droids you're looking for:
 CHECK(nentries == out.size());
 CHECK(make_key(0) == out.front().first);
 CHECK(make_key(nentries - 1) == out.back().first);

 auto keys_in = [&dbh](lfdb::select selector) {
  std::vector<std::pair<std::string, std::string>> entries;
  lfdb::get(dbh, selector, std::back_inserter(entries));

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
 reverse_open_closed.options.stride = 2;
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

 lfdb::set(dbh, test_key("keyx"), "outside");
 out.clear();
 lfdb::get(dbh, lfdb::select { make_key_prefix() }, std::back_inserter(out));
 CHECK(nentries == out.size());

 // Get exactly no entries:
 out.clear();
 lfdb::get(dbh, lfdb::select { make_key(0), make_key(0) }, std::back_inserter(out));
 CHECK(0 == out.size());

 // Get exactly one entry: 
 out.clear();
 lfdb::get(dbh, lfdb::select { make_key(1), make_key(2) }, std::back_inserter(out));
 REQUIRE(1 == out.size());
 CHECK(make_key(1) == out.front().first);
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
    // reverse this (or else we'll see whatever artefacts the conversion produced)-- 
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

 const std::size_t stride = 5;
 const std::size_t nkeys = 12;

 const auto kvs_in = write_monotonic_kvs(j, nkeys);

 SECTION("reads one bounded forward window") {
  auto selector = lfdb::select { make_key(0), make_key(nkeys) };
  selector.options.stride = stride;

  auto txn = lfdb::make_transaction(j);
  auto window = ceph::libfdb::detail::read_query_window(*txn, selector, 1);
  const auto out = decode_raw_fdb_pairs(window.result_pairs);

  REQUIRE_FALSE(out.empty());
  CHECK(out.size() <= stride);
  CHECK(window.more_available);
  CHECK_THAT(out, Catch::Matchers::RangeEquals(first_keys(kvs_in, out.size())));
 }

 SECTION("reads one bounded reverse window") {
  auto selector = lfdb::select { make_key(0), make_key(nkeys) };
  selector.options.stride = stride;
  selector.options.reverse_order = true;

  auto txn = lfdb::make_transaction(j);
  auto window = ceph::libfdb::detail::read_query_window(*txn, selector, 1);
  const auto out = decode_raw_fdb_pairs(window.result_pairs);

  REQUIRE_FALSE(out.empty());
  CHECK(out.size() <= stride);
  CHECK(window.more_available);
  CHECK_THAT(out, Catch::Matchers::RangeEquals(last_keys_reversed(kvs_in, out.size())));
 }

 SECTION("reports terminal windows") {
  auto selector = lfdb::select { make_key(0), make_key(1) };
  selector.options.stride = stride;

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

 const std::size_t stride = 5;
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
  selector.options.stride = stride;

  const auto pages = collect_pages(selector);
  const auto out = pages | std::views::join | std::ranges::to<std::vector<string_pair>>();

  CAPTURE(pages.size());
  CAPTURE(out.size());
  CHECK(1 < pages.size());
  CHECK(std::ranges::all_of(pages, [stride](const auto& page) {
   return page.size() <= stride;
  }));
  CHECK_THAT(out, Catch::Matchers::RangeEquals(first_keys(kvs_in, nkeys)));
 }

 SECTION("drains paged reverse windows") {
  auto selector = lfdb::select { make_key(0), make_key(nkeys) };
  selector.options.stride = stride;
  selector.options.reverse_order = true;

  const auto pages = collect_pages(selector);
  const auto out = pages | std::views::join | std::ranges::to<std::vector<string_pair>>();

  CAPTURE(pages.size());
  CAPTURE(out.size());
  CHECK(1 < pages.size());
  CHECK(std::ranges::all_of(pages, [stride](const auto& page) {
   return page.size() <= stride;
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
    std::vector<std::pair<std::string, std::string>> out;
    auto txn = lfdb::make_transaction(j);

    // pair_generator returns key-value pairs:
    for (auto&& kvp : lfdb::pair_generator(txn, lfdb::select { make_key(0), make_key(nkeys) }))
     out.emplace_back(std::move(kvp));

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

    std::vector<std::pair<std::string, std::string>> out;
    auto txn = lfdb::make_transaction(j);
    std::ranges::copy(lfdb::pair_generator(txn, selector), std::back_inserter(out));

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
    selector.options.stride = 5; // one of the most prime of prime numbers

    std::vector<std::pair<std::string, std::string>> out;
    auto txn = lfdb::make_transaction(j);
    std::ranges::copy(lfdb::pair_generator(txn, selector), std::back_inserter(out));

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
    selector.options.stride = 5; // one of the most prime of prime numbers

    std::vector<std::pair<std::string, std::string>> out;
    auto txn = lfdb::make_transaction(j);
    std::ranges::copy(lfdb::pair_generator(txn, selector), std::back_inserter(out));

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
}

TEST_CASE("generators honor selector endpoints", "[fdb]") {
 janitor j;

 constexpr auto prefix = "generator-selector";
 write_monotonic_kvs(j, 10, prefix);

 auto collect_pair_keys = [&j](lfdb::select selector) {
  std::vector<std::string> out;
  auto txn = lfdb::make_transaction(j);
  std::ranges::copy(lfdb::pair_generator(txn, selector)
                  | std::views::transform([](const auto& kv) { return kv.first; }),
                    std::back_inserter(out));
  return out;
 };

 auto collect_block_keys = [&j](lfdb::select selector) {
  std::vector<std::string> out;
  std::ranges::for_each(lfdb::block_generator(j, selector), [&out](const auto& block) {
   std::ranges::copy(block | std::views::transform([](const auto& kv) { return kv.first; }),
                     std::back_inserter(out));
  });
  return out;
 };

 auto selector = lfdb::select { lfdb::exclusive(make_key(3, prefix)), lfdb::inclusive(make_key(6, prefix)) };
 selector.options.stride = 2;

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

    std::map<std::string, ValueT> from_blocks;
    for (auto&& block : lfdb::block_generator<ValueT>(dbh, selector)) {
      from_blocks.insert(std::begin(block), std::end(block));
    }

    REQUIRE(from_blocks == expected);
    REQUIRE(from_blocks == from_pairs);
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
