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

#include "test_fdb-common.h"

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
//JFW: auto dbh = lfdb::create_database();
} // namespace

// Be nice to Catch2's template-test macros:
using string_pair = std::pair<std::string, std::string>;

constexpr const char* const msg = "Hello, World!"; 
constexpr const char msg_with_null[] = { '\0', 'H', 'i', '\0', ' ', 't', 'h', 'e', 'r', 'e', '!', '\0'};
constexpr const char * const pearl_msg =
"Perle, plesaunte to prynces paye\n"
"To clanly clos in golde so clere;\n"
"Oute of oryent, I hardyly saye.\n"
"Ne proved I never her precios pere.\n";

// Basically, make sure we're actually linking with the library:
TEST_CASE()
{
 REQUIRE_THROWS_AS([] { throw ceph::libfdb::libfdb_exception(0); }(),
                   ceph::libfdb::libfdb_exception);
}

TEST_CASE("fdb simple", "[rgw][fdb]") 
{
 janitor j;

 const string_view k = "key";
 const string v = fmt::format("value-{:%c}", std::chrono::system_clock::now());

 auto dbh = lfdb::create_database();
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
    CHECK_FALSE(lfdb::key_exists(lfdb::make_transaction(dbh), k, lfdb::commit_after_op::no_commit));

    // Write the key:
    lfdb::set(lfdb::make_transaction(dbh), k, v, lfdb::commit_after_op::commit);

    // ...it should magically be there!
    CHECK(lfdb::key_exists(lfdb::make_transaction(dbh), k, lfdb::commit_after_op::no_commit));

    // ...and now it should be gone again:
    lfdb::erase(lfdb::make_transaction(dbh), k, lfdb::commit_after_op::commit);
    CHECK_FALSE(lfdb::key_exists(lfdb::make_transaction(dbh), k, lfdb::commit_after_op::no_commit));
 }
}

TEST_CASE("fdb simple (delete keys in range)", "[rgw][fdb]") 
{
 janitor j;

 const auto selector = lfdb::select { make_key(0), make_key(25) };

 auto dbh = lfdb::create_database();

 // Make sure there's nothing in the database:
 CHECK(0 == key_count(dbh, selector));

 // Write a bunch of kvs:
 const auto kvs = make_monotonic_kvs(20);

 lfdb::set(lfdb::make_transaction(dbh), begin(kvs), end(kvs), lfdb::commit_after_op::commit);
 CHECK(20 == key_count(dbh, selector));

 // Erase some more of the range:
 lfdb::erase(lfdb::make_transaction(dbh), lfdb::select { make_key(10), make_key(15) }, lfdb::commit_after_op::commit);
 CHECK(15 == key_count(dbh, selector));

 // Erase a single value:
 lfdb::erase(lfdb::make_transaction(dbh), make_key(5), lfdb::commit_after_op::commit);
 CHECK(14 == key_count(dbh, selector));

 // Erase the entire range:
 lfdb::erase(lfdb::make_transaction(dbh), selector, lfdb::commit_after_op::commit);
 CHECK(0 == key_count(dbh, selector));
}

TEMPLATE_PRODUCT_TEST_CASE("multi-key ops", "[rgw][fdb]", 
(std::vector, std::list), (string_pair)) 
{
 janitor j;

 auto dbh = lfdb::create_database();
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

TEST_CASE("fdb conversions (built-in)", "[fdb][rgw]") 
{
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

 auto dbh = lfdb::create_database();

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

 auto dbh = lfdb::create_database();

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

SCENARIO("implicit transactions", "[fdb][rgw]")
{
 janitor j;

 auto dbh = lfdb::create_database();

 std::string_view k = "hi", v = "there";

 CAPTURE(k);   
 CAPTURE(v);   

 SECTION("implicitly create and complete transactions") {

  REQUIRE_FALSE(lfdb::key_exists(dbh, k));
  CHECK_NOTHROW(lfdb::set(dbh, k, v));
  CHECK(lfdb::key_exists(dbh, k));

  std::string ov;
  CHECK(lfdb::get(dbh, k, ov));

  CAPTURE(ov);   

  REQUIRE(v == ov);

  CHECK_NOTHROW(lfdb::erase(dbh, k));
  REQUIRE_FALSE(lfdb::key_exists(dbh, k));

  REQUIRE_FALSE(lfdb::get(dbh, k, ov));
 }

 SECTION("implicitly create and complete transactions-- selection operations") {
  // With an implicit transaction, transactions should commit by default:
  const auto selector = lfdb::select { make_key(0), make_key(20) };

  const auto kvs = make_monotonic_kvs(20);

  CHECK_NOTHROW(lfdb::set(dbh, begin(kvs), end(kvs)));

  lfdb::erase(dbh, lfdb::select { make_key(1), make_key(6) });
  CHECK(15 == key_count(dbh, selector));

  CHECK_FALSE(lfdb::key_exists(dbh, "key_03"));
 }

 SECTION("test behavior with shared transaction") {

  SECTION("write in uncommitted transaction") {
    // With a shared transaction, transactions should NOT commit on API calls by default:
    auto txn = lfdb::make_transaction(dbh);
  
    lfdb::set(txn, "Herman", "Hollerith");
   
    // Key exists with respect to this transaction: 
    CHECK(lfdb::key_exists(txn, "Herman"));
    
    lfdb::set(txn, "John", "Backus");
  
    // Key exists with respect to this transaction: 
    CHECK(lfdb::key_exists(txn, "John"));

    // transaction is abandoned
  }

  // These were only set in the abandoned transaction:
  CHECK_FALSE(lfdb::key_exists(dbh, "Herman"));
  CHECK_FALSE(lfdb::key_exists(dbh, "John"));
 }

 SECTION("round trip") {
  using namespace ceph::libfdb;
  
  auto dbh = create_database();
 
  std::string_view in = "value"; 
  set(dbh, "key", in);
  
  std::string out;
  get(dbh, "key", out);

  CHECK(out == in);
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
  ov = true;                              // flag
  ov = 42;                                // integer
  ov = std::string("hi");                 // string
  ov = std::vector<std::uint8_t>(         // data
        (const std::uint8_t *)pearl_msg, 
        (const std::uint8_t *)(pearl_msg + sizeof(pearl_msg)));
 }

  auto dbh0 = lfdb::create_database(
                { { FDB_DB_OPTION_LOCATION_CACHE_SIZE, 200'000 } },  
                { { FDB_NET_OPTION_TRACE_ENABLE, false } });         

  auto dbh1 = lfdb::create_database("fishing for databass!",       // name
               { { FDB_DB_OPTION_LOCATION_CACHE_SIZE, 200'000 } }, // database options
               { { FDB_NET_OPTION_TRACE_ENABLE, false } });        // network options
 
  auto txn = lfdb::make_transaction(dbh0, 
               { { FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, true } });

 SECTION("create_database()") {
  lfdb::create_database();
  lfdb::create_database("");
  lfdb::create_database("", {}, {});
  lfdb::create_database(lfdb::database_options {}, lfdb::network_options {});
 }
}

TEST_CASE("select group keys", "[fdb]") 
{
 janitor j;

 const int kvcount = 1024*5;

 auto dbh = lfdb::create_database();

 set_monotonic_kvs(dbh, kvcount);

 SECTION("make_generator() for range selection") {
  auto txn = lfdb::make_transaction(dbh);
  auto kvgen = lfdb::make_generator(txn, lfdb::select { "", "\xFF" });

  std::map<std::string, std::string> kvs;
  std::ranges::copy(kvgen(), std::inserter(kvs, std::end(kvs)));

  // ...be sure we processed the same number that we inserted into the database:
  REQUIRE(kvcount == kvs.size());
 }
}

// These are /slightly/ pointless, but show how generators can work with ranges:
TEST_CASE("generator filters", "[fdb][rgw]") 
{
 janitor j;

 const int kvcount = 100;

 auto dbh = lfdb::create_database();

 set_monotonic_kvs(dbh, kvcount);

 SECTION("filter keys") {
  auto txn = lfdb::make_transaction(dbh);
  auto kvgen = lfdb::make_generator(txn, { "", "\xFF" });

  // kind of a pointless filter, but there we have it:
  auto sevens = [](const auto& kvp) { return '7' == kvp.first.back(); };

  auto l = kvgen() | std::views::filter(sevens);
 
  CHECK(10 == std::ranges::distance(l));
 }
 
 SECTION("unzip kvs") {
  auto txn = lfdb::make_transaction(dbh);
  auto kvgen = lfdb::make_generator(txn, { "", "\xFF" });

  // This is to say: unzip :: [(a, b)] -> ([a], [b])
  // ...note that we can actually desugar any /range/ or tuple-like with this(!):
  auto unzip = []<std::ranges::forward_range R>(R&& seq) {
                auto in_view = std::views::all(std::forward<R>(seq));

                return std::pair { 
                        in_view | std::views::elements<0>, 
                        in_view | std::views::elements<1> 
                       };
               };
 
  auto in_kvs = kvgen() | std::ranges::to<std::vector>();

  auto [keys, values] = unzip(in_kvs);

  CHECK(kvcount == keys.size());
  CHECK(keys.size() == values.size());
 }
}

TEST_CASE("Gal demo", "[fdb]") 
{
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
  
 auto dbh = lfdb::create_database();

 lfdb::set(dbh, "bucket_obj", bucket_entries);

 map<string, string> out;
 lfdb::get(dbh, "bucket_obj", out);

 CAPTURE(out["userId"]);
 REQUIRE(bucket_entries == out);
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

