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

#include <map>
#include <list>
#include <chrono>
#include <vector>
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

using namespace std::literals::string_literals;

namespace lfdb = ceph::libfdb;

namespace {
// make a database handle:
auto dbh = lfdb::make_database();
} // namespace

// Be nice to Catch2's template-test macros:
using string_pair = std::pair<std::string, std::string>;

/* Importantly, FDB operations are /lexicographically/ ordered. I do not have a bunch of
time to write a fancy generator, so I've taken a "dumb as bones" approach and write a
fixed prefix followed by integers: */

// As we manipulate keys and values quite a bit, it's helpful to have a recipe for them:
std::string make_key(const int n) {
 return fmt::format("key_{:04d}", n);
}

std::string make_value(const int n) {
 return fmt::format("value_{}", n);
}

// Collect values in selection to out_values:
auto key_counter(auto txn, const auto& selector, auto& out_values) -> auto {
 out_values.clear();

 lfdb::get(txn, selector, 
           std::inserter(out_values, std::begin(out_values)), 
           lfdb::commit_after_op::no_commit);

 return out_values.size();
};

auto key_count(const auto& selector) {
 std::map<std::string, std::string> _;
 return key_counter(lfdb::make_transaction(dbh), selector, _);
}


// Note that the generated keys are ONE based, not zero:
inline std::map<std::string, std::string> make_monotonic_kvs(const int N)
{
 std::map<std::string, std::string> kvs;

 for(const auto i : std::ranges::iota_view(1, 1 + N)) {
  kvs.insert({ make_key(i), make_value(i) });
 }

 return kvs;
}

constexpr const char* const msg = "Hello, World!"; 
constexpr const char msg_with_null[] = { '\0', 'H', 'i', '\0', ' ', 't', 'h', 'e', 'r', 'e', '!', '\0'};
constexpr const char * const pearl_msg =
"Perle, plesaunte to prynces paye\n"
"To clanly clos in golde so clere;\n"
"Oute of oryent, I hardyly saye.\n"
"Ne proved I never her precios pere.\n";

struct janitor final
{
 // flip this off if you need artefacts after debugging:
 bool drop_after_scope = true;

 janitor()
 {
  drop_all();
 } 

 ~janitor()
 {
  if(drop_after_scope)
   drop_all();
 }

 static void drop_all() {
   // Nobody'd better run this in production!
   // Note: technically, 0xFF, 0xFF is needed to include the system keys (if the transaction's allowed to
   // access these), but I don't think any tests will be doing this, at least not for now; the documentation
   // with details is unfortunately light on, for instance, whether or not after 0xFF the NULL character is
   // included, but we'll assume it is (again, for our purposes):
   const char begin_key[] = { (char)0x00 };
   const char end_key[]   = { (char)0xFF };
   lfdb::erase(lfdb::make_transaction(dbh),
              lfdb::select { begin_key, end_key }, 
              lfdb::commit_after_op::commit);
   }
};

// Basically, make sure we're actually linking with the library:
TEST_CASE()
{
 REQUIRE_THROWS_AS([] { throw ceph::libfdb::libfdb_exception(0); }(),
                   ceph::libfdb::libfdb_exception);
}

TEST_CASE("fdb simple", "[rgw][fdb]") {
 janitor j;

 const string_view k = "key";
 const string v = fmt::format("value-{:%c}", std::chrono::system_clock::now());

 REQUIRE(nullptr != dbh);

 SECTION("read missing key") {
    const string_view missing_key = "missing_key";

    SECTION("with transaction") {
        std::string out_value;

        auto txn_handle = lfdb::make_transaction(dbh);
        REQUIRE(nullptr != txn_handle);
  
        CAPTURE(missing_key); 
        CAPTURE(out_value); 
	REQUIRE_FALSE(lfdb::get(txn_handle, missing_key, out_value, lfdb::commit_after_op::no_commit));
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
    CHECK(lfdb::get(lfdb::make_transaction(dbh), k, out_value, lfdb::commit_after_op::no_commit));
    CHECK(v == out_value); 

    // "erase()" is "clear" in FDB parlance, deleting a record:
    REQUIRE_NOTHROW(lfdb::erase(lfdb::make_transaction(dbh), k, lfdb::commit_after_op::commit));

    // ...as this shouldn't be updated again, make sure there isn't an accidental match:
    out_value.clear();

    // ...and, POOF!-- it should be gone:
    CHECK_FALSE(lfdb::get(lfdb::make_transaction(dbh), k, out_value, lfdb::commit_after_op::no_commit));
    CHECK(v != out_value);
 }

 SECTION("read/write single key") {
    REQUIRE(nullptr != dbh);

    // First, be sure we have a valid value written to the database:
    REQUIRE_NOTHROW(lfdb::set(lfdb::make_transaction(dbh), k, v, lfdb::commit_after_op::commit));

    SECTION("read transaction") {
      std::string out_value;
     
      CHECK(lfdb::get(lfdb::make_transaction(dbh), k, out_value, lfdb::commit_after_op::no_commit));
      CHECK(v == out_value); 
    }
 }

 SECTION("check for existence of key") {
    REQUIRE(nullptr != dbh);

    // Erase the key if it's already there:
    lfdb::erase(lfdb::make_transaction(dbh), k, lfdb::commit_after_op::commit);

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
 janitor j;

 const auto selector = lfdb::select { make_key(0), make_key(25) };

 // Make sure there's nothing in the database:
 CHECK(0 == key_count(selector));

 // Write a bunch of kvs:
 const auto kvs = make_monotonic_kvs(20);

 lfdb::set(lfdb::make_transaction(dbh), begin(kvs), end(kvs), lfdb::commit_after_op::commit);
 CHECK(20 == key_count(selector));

 // Erase some more of the range:
 lfdb::erase(lfdb::make_transaction(dbh), lfdb::select { make_key(10), make_key(15) }, lfdb::commit_after_op::commit);
 CHECK(15 == key_count(selector));

 // Erase a single value:
 lfdb::erase(lfdb::make_transaction(dbh), make_key(5), lfdb::commit_after_op::commit);
 CHECK(14 == key_count(selector));

 // Erase the entire range:
 lfdb::erase(lfdb::make_transaction(dbh), selector, lfdb::commit_after_op::commit);
 CHECK(0 == key_count(selector));
}

TEMPLATE_PRODUCT_TEST_CASE("multi-key ops", "[rgw][fdb]", 
(std::vector, std::list), (string_pair)) 
{
 janitor j;

 CHECK(nullptr != dbh);

 // Write a sequence of keys so we have some data to work with:
 const auto kvs = make_monotonic_kvs(100);

 lfdb::set(lfdb::make_transaction(dbh), begin(kvs), end(kvs), lfdb::commit_after_op::commit);

 SECTION("check multiple key write", "[fdb]") {
  auto txn = lfdb::make_transaction(dbh);

  std::string out_value;
 
  CHECK((*(kvs.find(make_key(1)))).second == make_value(1));
  CHECK(lfdb::get(txn, make_key(1), out_value, lfdb::commit_after_op::no_commit));
  CHECK(make_value(1) == out_value);

  out_value.clear();
  CHECK((*(kvs.find(make_key(100)))).second == make_value(100));
  CHECK(lfdb::get(txn, make_key(100), out_value, lfdb::commit_after_op::no_commit));
  CHECK(make_value(100) == out_value);
 }

 SECTION("check multiple key selection", "[fdb]") {
  TestType out_values;

  auto txn = lfdb::make_transaction(dbh);

  lfdb::get(txn, lfdb::select { make_key(0), make_value(100) }, std::back_inserter(out_values), lfdb::commit_after_op::no_commit);

  CHECK(100 == out_values.size());

  // Maybe not the world's most creative test, but the idea is to try getting some random keys:
  for(auto i = ceph::util::generate_random_number(out_values.size()); i; --i) {
    CHECK(std::end(out_values) != std::ranges::find(out_values, string_pair { make_key(i), make_value(i) }));
  }
 }
}

TEST_CASE("fdb conversions (built-in)", "[fdb][rgw]") {
 // Manual tests of conversions to and from supported FDB built-in types.
 
 SECTION("serialize/deserialize built-in FDB types") {

  // string_view -> span<uint8> -> string
  {
  const std::string_view n = "Hello, World!";
  std::vector<std::uint8_t> x = ceph::libfdb::to::convert(n);

  std::string o;
  ceph::libfdb::from::convert(x, o); 
  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
  }

  // span<uint8_t> -> vector<uint8_t> -> vector<uint8_t>
  {
  const std::span<const std::uint8_t> n((const std::uint8_t *)msg, sizeof(msg));

  std::vector<std::uint8_t> x;
  x = ceph::libfdb::to::convert(n);

  std::vector<std::uint8_t> o;
  ceph::libfdb::from::convert(x, o); 

  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
  }

  // with NULL data-- const char* -> vector<uint8_t> -> vector<uint8_t>
  {
  const std::span<const std::uint8_t> n((const std::uint8_t *)msg_with_null, sizeof(msg_with_null));

  std::vector<std::uint8_t> x;
  x = ceph::libfdb::to::convert(n);

  std::vector<std::uint8_t> o;
  ceph::libfdb::from::convert(x, o); 

  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
  REQUIRE_THAT(msg_with_null, Catch::Matchers::RangeEquals(o));
  }
 }
}

TEST_CASE("fdb conversions (round-trip)", "[fdb][rgw]") {
 janitor j;

 // string_view -> string
 {
 const std::string_view n = "Hello, World!";
 std::string o;

 lfdb::set(lfdb::make_transaction(dbh), "key", n, lfdb::commit_after_op::commit);
 lfdb::get(lfdb::make_transaction(dbh), "key", o, lfdb::commit_after_op::no_commit);

 REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }

 // vector<uint8_t> -> vector<uint8_t>
 {
 const std::vector<uint8_t> n = { 1, 2, 3, 4, 5 };
 std::vector<uint8_t> o;

 lfdb::set(lfdb::make_transaction(dbh), "key", n, lfdb::commit_after_op::commit);
 lfdb::get(lfdb::make_transaction(dbh), "key", o, lfdb::commit_after_op::no_commit);

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
    // the complication is a consequence of dealing with the underlying buffer directly,
    // and could be helped with a function or two-- another case of why one shouldn't use
    // test code as a source of examples!

    std::span<const std::uint8_t> in_span((const std::uint8_t *)data, sz);
 
    ceph::libfdb::from::convert(in_span, o);
  };

  ceph::libfdb::from::convert(x, fn); 

  CAPTURE(n);
  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }
}

TEMPLATE_PRODUCT_TEST_CASE("associative data", "[fdb][rgw]",
(std::map, std::unordered_map), ((std::string, std::string)))
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

 lfdb::set(lfdb::make_transaction(dbh), "key", kvs, lfdb::commit_after_op::commit);

 std::map<std::string, std::string> out_kvs;

 lfdb::get(lfdb::make_transaction(dbh), "key", out_kvs, lfdb::commit_after_op::no_commit);

 CHECK(pearl_msg == out_kvs["pearl"]);
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

