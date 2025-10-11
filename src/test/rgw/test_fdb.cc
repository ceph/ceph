// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- // vim: ts=8 sw=2 smarttab ft=cpp 

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 International Business Machines Corp. (IBM)
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

#define CATCH_CONFIG_MAIN

#include <fmt/format.h>
#include <fmt/chrono.h>
#include <fmt/ranges.h>

#include "rgw/fdb/fdb.h"

#include "include/random.h"

#include <chrono>
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

using namespace std::literals::string_literals;

namespace lfdb = ceph::libfdb;

// Be nice to Catch2's template-test macros:
using string_pair = std::pair<std::string, std::string>;

inline std::map<std::string, std::string> make_monotonic_kvs(const unsigned N)
{
 std::map<std::string, std::string> kvs;

 for(const auto i : std::ranges::iota_view(0u, N)) {
  auto n = std::to_string(i);
  kvs.insert({"key_"s += n, "value_"s += n});
 }

 return kvs;
}

constexpr const char* const msg = "Hello, World!"; 
constexpr const char msg_with_null[] = { '\0', 'H', 'i', '\0', ' ', 't', 'h', 'e', 'r', 'e', '!', '\0'};
constexpr const char * const pearl_msg =
"Perle, plesaunte to prynces paye"
"To clanly clos in golde so clere;"
"Oute of oryent, I hardyly saye."
"Ne proved I never her precios pere.";

// Basically, make sure we're actually linking with the library:
TEST_CASE()
{
 REQUIRE_THROWS_AS([] { throw ceph::libfdb::fdb_exception(0); }(),
                   ceph::libfdb::fdb_exception);
}

TEST_CASE("fdb simple", "[rgw][fdb]") {

 const string_view k = "key";
 const string v = fmt::format("value-{:%c}", std::chrono::system_clock::now());

 // make a database handle:
 auto dbh = lfdb::make_database();
 REQUIRE(nullptr != dbh);

 SECTION("read missing key") {
    const string_view missing_key = "missing_key";

    SECTION("with transaction") {
        std::string out_value;

        auto txn_handle = lfdb::make_transaction(dbh);
        REQUIRE(nullptr != txn_handle);
  
        CAPTURE(missing_key); 
        CAPTURE(out_value); 
	REQUIRE_FALSE(lfdb::get(txn_handle, missing_key, out_value));
        CHECK(v != out_value);
    }

    // JFW: TODO: text explicit commit
 }

 SECTION("CRD single-key") {
    std::string out_value;

    REQUIRE(nullptr != dbh);

    // The key initially either exists, or we'll write it anew, either is fine:
    CHECK_NOTHROW(lfdb::set(lfdb::make_transaction(dbh), k, v, lfdb::commit_after_op::commit));

    // Make sure that it DOES exist:
    CHECK(lfdb::get(lfdb::make_transaction(dbh), k, out_value));
    CHECK(v == out_value); 

    // "erase()" is "clear" in FDB parlance, deleting a record:
    REQUIRE_NOTHROW(lfdb::erase(lfdb::make_transaction(dbh), k, lfdb::commit_after_op::commit));

    // ...as this shouldn't be updated again, make sure there isn't an accidental match:
    out_value.clear();

    // ...and, POOF!-- it should be gone:
    CHECK_FALSE(lfdb::get(lfdb::make_transaction(dbh), k, out_value));
    CHECK(v != out_value);
 }

 SECTION("read/write single key") {
    REQUIRE(nullptr != dbh);

    // First, be sure we have a valid value written to the database:
    REQUIRE_NOTHROW(lfdb::set(lfdb::make_transaction(dbh), k, v, lfdb::commit_after_op::commit));

    SECTION("read transaction") {
      std::string out_value;
     
      CHECK(lfdb::get(lfdb::make_transaction(dbh), k, out_value));
      CHECK(v == out_value); 
    }
 }

 SECTION("check for existence of key") {
    REQUIRE(nullptr != dbh);

    // Erase the key if it's already there:
    lfdb::erase(lfdb::make_transaction(dbh), k, lfdb::commit_after_op::commit));

    // Now, we shouldn't find anything:
    CHECK_FALSE(lfdb::key_exists(lfdb::make_transaction(dbh), k));

    // Write the key:
    lfdb::set(lfdb::make_transaction(dbh), k, v, lfdb::commit_after_op::commit);

    // ...it should magically be there!
    CHECK(lfdb::key_exists(lfdb::make_transaction(dbh), k));

    // ...and now it should be gone again:
    lfdb::erase(lfdb::make_transaction(dbh), k, lfdb::commit_after_op::commit);
    CHECK_FALSE(lfdb::key_exists(lfdb::make_transaction(dbh), k));
 }
}

TEST_CASE("fdb simple (delete keys in range)", "[rgw][fdb]") {

 // Write a bunch of kvs:
 const auto kvs = make_monotonic_kvs(100);
 lfdb::set(lfdb::make_transaction(dbh), begin(kvs), end(kvs), lfdb::commit_after_op::commit);

 // Make sure that worked:
 std::vector<std::string> out_values;
 lfdb::get(lfdb::make_transaction(dbh), lfdb::select { "key_00", "key_99" }, std::back_inserter(out_values));
 CHECK(100 == out_values.size());

 // Erase some of the range:
 CHECK(0 == out_values.size());
 lfdb::erase(lfdb::make_transaction(dbh), lfdb::select { "key_40", "key_59" }, lfdb::commit_after_op::commit);

 // They should be "mostly gone":
 lfdb::get(lfdb::make_transaction(dbh), lfdb::select { "key_00", "key_99" }, std::back_inserter(out_values));
 CHECK(50 == out_values.size());

 // Aaaaand, abracadabra, they should ALL be gone:
 out_values.clear();
 lfdb::get(lfdb::make_transaction(dbh), lfdb::select { "key_00", "key_99" }, std::back_inserter(out_values));
 CHECK(0 == out_values.size());
}

TEMPLATE_PRODUCT_TEST_CASE("A Template product test case", "[template][product]", 
(std::vector, std::list), (string_pair)) {
 auto dbh = lfdb::make_database();
 CHECK(nullptr != dbh);

 // Write a sequence of keys so we have some data to work with:
 const auto kvs = make_monotonic_kvs(100);

 lfdb::set(lfdb::make_transaction(dbh), begin(kvs), end(kvs), lfdb::commit_after_op::commit);

 SECTION("check multiple key write", "[fdb]") {
  auto txn = lfdb::make_transaction(dbh);

  std::string out_value;
 
  CHECK((*(kvs.find("key_0"))).second == "value_0");
  CHECK(lfdb::get(txn, "key_0", out_value));
  CHECK("value_0" == out_value);

  out_value.clear();
  CHECK((*(kvs.find("key_99"))).second == "value_99");
  CHECK(lfdb::get(txn, "key_99", out_value));
  CHECK("value_99" == out_value);
 }

 SECTION("check multiple key selection", "[fdb]") {
  TestType out_values;

  auto txn = lfdb::make_transaction(dbh);

  // By default, key ranges are inclusive, (begin, end):
  lfdb::get(txn, lfdb::select { "key_00", "key_99" }, std::back_inserter(out_values));

  CHECK(100 == out_values.size());

  for(auto i = ceph::util::generate_random_number(20); --i;) {
    int n = ceph::util::generate_random_number(99);
 
    auto k = fmt::format("key_{}", n);
    auto v = fmt::format("value_{}", n);

    CHECK(std::end(out_values) != std::ranges::find(out_values, string_pair { k, v }));
  }
 }
}

TEST_CASE("fdb conversions (built-in)", "[fdb][rgw]") {
 // Manual tests of conversions to and from supported FDB built-in types.
 
 SECTION("serialize/deserialize built-in FDB types") {

/* JFW: No fabrication of built-ins besides span<> for now:
  // int64 -> int64 -> int64
  {
  const std::int64_t n = 1770;		// input type (T)

  std::int64_t x;			// type used by FDB (F)
  x = ceph::libfdb::to::convert(n);	// map T -> F

  std::int64_t o;			// output type (T) (i.e. user type) 
  ceph::libfdb::from::convert(x, o); 
 
  REQUIRE(n == o); 
  }
*/
  // string_view -> span<uint8> -> string
  {
  const std::string_view n = "Hello, World!";
  std::span<const std::uint8_t> x;
  x = ceph::libfdb::to::convert(n);

  std::string o;
  ceph::libfdb::from::convert(x, o); 
 
  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
  }

  // span<uint8_t> -> span<uint8_t> -> vector<uint8_t>
  {
  const std::span<const std::uint8_t> n((const std::uint8_t *)msg, sizeof(msg));

  std::span<const std::uint8_t> x;
  x = ceph::libfdb::to::convert(n);

  std::vector<std::uint8_t> o;
  ceph::libfdb::from::convert(x, o); 

  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
  }

  // with NULL data-- span<uint8_t> -> span<uint8_t> -> vector<uint8_t>
  {
  const std::span<const std::uint8_t> n((const std::uint8_t *)msg_with_null, sizeof(msg_with_null));

  std::span<const std::uint8_t> x;
  x = ceph::libfdb::to::convert(n);

  std::vector<std::uint8_t> o;
  ceph::libfdb::from::convert(x, o); 

  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
  REQUIRE_THAT(msg_with_null, Catch::Matchers::RangeEquals(o));
  }
 }
}

TEST_CASE("fdb conversions (round-trip)", "[fdb][rgw]") {
 // Actually store and retrieve converted data via the DB:
 auto dbh = lfdb::make_database();

 // string_view -> string
 {
 const std::string_view n = "Hello, World!";
 std::string o;

 lfdb::set(lfdb::make_transaction(dbh), "key", n, lfdb::commit_after_op::commit);
 lfdb::get(lfdb::make_transaction(dbh), "key", o);

 REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }

 // vector<uint8_t> -> vector<uint8_t>
 {
 const std::vector<uint8_t> n = { 1, 2, 3, 4, 5 };
 std::vector<uint8_t> o;

 lfdb::set(lfdb::make_transaction(dbh), "key", n, lfdb::commit_after_op::commit);
 lfdb::get(lfdb::make_transaction(dbh), "key", o);

 REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }

/* JFW: I'm not going to support this for the prototype, but the plumbing looks like
//it does the right thing for the most part, we need to add the encoding, etc.; there are
//a few issues around decoding in particular that need close and careful attention, and
//I want to focus on them after there are some concrete results (frankly, to take a little
//pressure off!):
 // int64 kvs -> int64
 {
 const std::int64_t k = 1;
 const std::int64_t n = 5;
 std::int64_t o = 0;

 lfdb::set(lfdb::make_transaction(dbh), k, n, lfdb::commit_after_op::commit);
 lfdb::get(lfdb::make_transaction(dbh), k, o);

 REQUIRE(n == o);
 }
*/
} 

TEST_CASE("fdb conversions (functions)", "[fdb][rgw]")
{
 SECTION("convert with a lambda function")
 {
  std::string_view n { pearl_msg };
  std::string o;

  std::span<const std::uint8_t> x;
  x = ceph::libfdb::to::convert(n);

  auto fn = [&o](const char *data, std::size_t sz) { o.insert(0, data, sz); };

  ceph::libfdb::from::convert(x, fn); 

  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }
}

TEST_CASE("fdb misc", "[fdb]")
{
 SECTION("ptr_and_sz()") {
   std::string hi = "Hello, World!";
 
   auto [ptr, sz] = lfdb::detail::ptr_and_sz(hi);
  
   CHECK(hi == std::string_view(ptr, sz));
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

