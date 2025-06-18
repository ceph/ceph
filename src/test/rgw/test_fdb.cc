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
#define CATCH_CONFIG_MAIN

#include <fmt/format.h>
#include <fmt/chrono.h>
#include <fmt/ranges.h>

#include "rgw/fdb/fdb.h"

#include "include/random.h"

#include <set>
#include <list>
#include <chrono>

using std::string;
using std::string_view;

using std::to_string;

using namespace std::literals::string_literals;

using fmt::format;
using fmt::println;

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

// Basically, make sure we're actually linking with the library:
TEST_CASE()
{
 REQUIRE_THROWS_AS([] { throw ceph::libfdb::fdb_exception(0); }(),
                   ceph::libfdb::fdb_exception);
}

TEST_CASE("fdb simple", "[rgw][fdb]") {

 const string_view missing_key = "missing_key";

 const string_view k = "key";
 const string v = fmt::format("value-{:%c}", std::chrono::system_clock::now());

 // make a database handle:
 auto dbh = lfdb::make_database();
 REQUIRE(nullptr != dbh);

 SECTION("read missing key") {
    SECTION("with transaction") {
        std::string out_value;

        auto txn_handle = lfdb::make_transaction(dbh);
        REQUIRE(nullptr != txn_handle);
  
        CAPTURE(missing_key); 
        CAPTURE(out_value); 
	REQUIRE_FALSE(lfdb::get(txn_handle, missing_key, out_value));
        CHECK(v != out_value);
    }
 }

 SECTION("CRD single-key") {
    std::string out_value;

    REQUIRE(nullptr != dbh);

    // The key initially either exists, or we'll write it anew, either is fine:
    CHECK_NOTHROW(lfdb::set(lfdb::make_transaction(dbh), k, v, lfdb::commit_after_op::commit));

    // Make sure that it DOES exist:
    CHECK(lfdb::get(lfdb::make_transaction(dbh), k, out_value));
    CHECK(v == out_value); 

    // "erase()" is "clear" in FDB parlance:
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

TEST_CASE("fdb selectors", "[fdb]") {

 lfdb::select s0 { "x", "xx" };
 lfdb::select_view s1 { "x", "xx" };
}

TEST_CASE("multi-key CRD", "[rgw][fdb]")
{
 const std::map<std::string, std::string> kvs {
    { "key", "value" }, 
    { "mortal", "wombat" } 
  };

 // JFW: TBD
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
  ceph::libfdb::detail::database_system::shutdown_fdb(); 

  return result;
}

