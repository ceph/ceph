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

 for(const auto& [k, v] : kvs)
  lfdb::set(lfdb::make_transaction(dbh), k, v, lfdb::commit_after_op::commit);

 return kvs;
}

// Basically, make sure we're actually linking with the library:
TEST_CASE()
{
 REQUIRE_THROWS_AS([] { throw ceph::libfdb::libfdb_exception(0); }(),
                   ceph::libfdb::libfdb_exception);
}

TEST_CASE("fdb simple", "[rgw][fdb]") {
 janitor j;

 auto dbh = j.dbh();

 const string_view k = "key";
 const string v = fmt::format("value-{:%c}", std::chrono::system_clock::now());

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
 }

 SECTION("CRD single-key") {
    std::string out_value;

    // The key initially either exists, or we'll write it anew, either is fine:
    CHECK_NOTHROW(lfdb::set(lfdb::make_transaction(dbh), k, v, lfdb::commit_after_op::commit));

    // Make sure that it DOES exist:
    CHECK(lfdb::get(lfdb::make_transaction(dbh), k, out_value, lfdb::commit_after_op::no_commit));
    CHECK(v == out_value); 

    // "erase()" is known as "clear" in FDB parlance, deleting a record:
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
}

TEMPLATE_PRODUCT_TEST_CASE("multi-key ops", "[rgw][fdb]", 
(std::vector, std::list), (string_pair)) 
{
 janitor j;

 auto dbh = j.dbh();

 // Write a sequence of keys so we have some data to work with:
 const auto kvs = write_monotonic_kvs(dbh, 100);

 SECTION("check multiple key write", "[fdb]") {
  auto txn = lfdb::make_transaction(dbh);

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

  auto txn = lfdb::make_transaction(dbh);

  lfdb::get(txn, lfdb::select { make_key(0), make_key(100) }, std::back_inserter(out_values), lfdb::commit_after_op::no_commit);

  CHECK(100 == out_values.size());

  // Maybe not the world's most creative test, but the idea is to try getting some random keys:
  for(auto i = ceph::util::generate_random_number(out_values.size() - 1); i; --i) {
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
 dbh.drop_all();
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

 lfdb::set(dbh, "keyx", "outside");
 out.clear();
 lfdb::get(dbh, lfdb::select { "key_" }, std::back_inserter(out));
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

 auto dbh = j.dbh();

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
  auto dbh = j.dbh();

  std::string_view n { pearl_msg };
  std::string o;

  lfdb::set(dbh, "key", n);

  REQUIRE(lfdb::get(dbh, "key", [&o](std::span<const std::uint8_t> in) {
    ceph::libfdb::from::convert(in, o);
  }));

  CAPTURE(n);
  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }
}

TEST_CASE("basic generators", "[fdb]") {
 janitor j;

 const unsigned nkeys = GENERATE(0, 1, 2, 3, 10, 100, 1'000);

 auto dbh = j.dbh();

 const auto kvs_in = write_monotonic_kvs(dbh, nkeys);
 REQUIRE(nkeys == kvs_in.size());

 SECTION("pair_generator, kv pair return") {
    std::map<std::string, std::string> out;

    // pair_generator returns key-value pairs, keeping the specified transaction (or implicitly created one)
    // alive until exhausted (note that this may cause the transaction to expire if approaching 5s or so):
    for(auto&& kvp : lfdb::pair_generator(dbh, lfdb::select { make_key(0), make_key(nkeys) }))
     out.emplace(kvp);

    REQUIRE(nkeys == out.size());

    // Be sure we captured the head and the tail:
    if(0 < nkeys) {
      CHECK(out.contains(make_key(0)));
      CHECK(out.contains(make_key(nkeys - 1)));
    }
 }

 SECTION("pair_generator owns its coroutine transaction") {
    std::map<std::string, std::string> out;
    const auto selector = lfdb::select { make_key(0), make_key(nkeys) };

    // The transaction handle local to this lambda must remain alive inside the
    // generator's coroutine frame after the lambda returns:
    auto gen = [&dbh, selector] {
      auto txn = lfdb::make_transaction(dbh);
      return lfdb::pair_generator(txn, selector);
    }();

    std::ranges::copy(gen, std::inserter(out, std::end(out)));

    REQUIRE(nkeys == out.size());

    if(0 < nkeys) {
      CHECK(out.contains(make_key(0)));
      CHECK(out.contains(make_key(nkeys - 1)));
    }
 }
}

TEMPLATE_PRODUCT_TEST_CASE("associative data", "[fdb][rgw]",
(std::map, std::unordered_map, boost::container::flat_map), ((std::string, std::string)))
{
 janitor j;

 auto dbh = j.dbh();

 TestType kvs{
      { "hello", "world" },
      { "lorem", "ipsum" },
      { "perl", "camel" },
      { "pearl", pearl_msg }
    };

 // From the "database" point of view, the structure is now that we have a single 
 // key pointing (p) to an associative array, e.g. map<p, map<k, v>>:
 lfdb::set(lfdb::make_transaction(dbh), "key", kvs, lfdb::commit_after_op::commit);

 TestType out_kvs;

 lfdb::get(lfdb::make_transaction(dbh), "key", out_kvs, lfdb::commit_after_op::no_commit);

 CHECK(pearl_msg == out_kvs["pearl"]);
}

SCENARIO("implicit transactions", "[fdb][rgw]")
{
 janitor j;

 auto dbh = j.dbh();

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
  // With an implicit transaction, mutating transactions should commit by default:
  const auto selector = lfdb::select { make_key(0), make_key(20) };

  const auto kvs = write_monotonic_kvs(dbh, 20);

  lfdb::erase(dbh, lfdb::select { make_key(1), make_key(6) });

  CHECK(15 == key_count(dbh, selector));

  // Let's look around the edge cases of the selection:   
  CHECK_FALSE(lfdb::key_exists(dbh, make_key(1)));
  CHECK_FALSE(lfdb::key_exists(dbh, make_key(5)));

  CHECK(lfdb::key_exists(dbh, make_key(0)));
  CHECK(lfdb::key_exists(dbh, make_key(6)));
 }

 SECTION("test behavior with shared transaction") {
    SECTION("write in uncommitted transaction") {
      using lfdb::commit_after_op;
    
      auto txn = lfdb::make_transaction(dbh);
    
      lfdb::set(txn, "Herman", "Hollerith", commit_after_op::no_commit);
     
      // Key exists with respect to this transaction: 
      CHECK(lfdb::key_exists(txn, "Herman"));
      
      lfdb::set(txn, "John", "Backus", commit_after_op::no_commit);
    
      // Key exists with respect to this transaction: 
      CHECK(lfdb::key_exists(txn, "John", commit_after_op::no_commit));
    
      // transaction is abandoned
    }

  // These were only set in the abandoned transaction:
  CHECK_FALSE(lfdb::key_exists(dbh, "Herman"));
  CHECK_FALSE(lfdb::key_exists(dbh, "John"));
 }

 SECTION("round trip") {
  janitor j(dbh);

  using namespace ceph::libfdb;
  
  set(dbh, "key_0000", "value");
  std::string out;
  get(dbh, "key_0000", out);
  
  CHECK("value" == out);
 }

 SECTION("round trip with raw string") {
  // The underlying serializer can produce some surprising behavior; libfdb
  // works around this so that the "right" thing to do is what gets done, with
  // performance-maximzation left as an available, but explicit operation.

  janitor j(dbh);

  using namespace ceph::libfdb;
 
  // Notice the raw literal going in here: 
  set(dbh, "key_0000", "value");

  std::string out;
  CHECK_NOTHROW(get(dbh, "key_0000", out));

  CHECK(std::string_view("value") == std::string_view(out));

  // Explicit raw buffers:
  char out_buffer[9] = {}; 
  CHECK_NOTHROW(get(dbh, "key_0000", out_buffer));
  
  CHECK(std::string_view("value") == std::string_view(out));
 }
}

SCENARIO("transactor", "[fdb]")
{
 janitor j;

 auto dbh = j.dbh();

 SECTION("transaction function returns nothing") {
  auto txr = lfdb::make_transactor(dbh);

  txr([](auto txn) {
    lfdb::set(txn, "key", "value");
  });

  std::string out;
  CHECK(lfdb::get(dbh, "key", out));
  CHECK("value" == out);
 }

 SECTION("transaction function returns value") {
  auto txr = lfdb::make_transactor(dbh);

  auto [found, out] = txr([](auto txn) {
    lfdb::set(txn, "key", "value");

    std::string out;
    auto found = lfdb::get(txn, "key", out);

    return std::pair(found, out);
  });

  CHECK(found);
  CHECK("value" == out);
 }

 SECTION("construct with transaction options") {
  lfdb::transaction_options opts {
    { FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, lfdb::option_flag }
  };

  auto txr = lfdb::make_transactor(dbh, opts);

  txr([](auto txn) {
    lfdb::set(txn, "key", "value");
  });

  std::string out;
  CHECK(lfdb::get(dbh, "key", out));
  CHECK("value" == out);
 }

 SECTION("transactor replays after conflict") {
  auto txr = lfdb::make_transactor(dbh);

  lfdb::set(dbh, "key", "initial");

  txr([dbh](auto txn) {
    std::string out;
    if(not lfdb::get(txn, "key", out)) {
     throw std::runtime_error("expected key does not exist");
    }

    // Force a conflict, making the transactor replay the body:
    if("initial" == out) {
     lfdb::set(dbh, "key", "conflict");
    }

    lfdb::set(txn, "key", "final");
  });

  std::string out;
  CHECK(lfdb::get(dbh, "key", out));
  CHECK("final" == out);
 }

 SECTION("transactor propagates transaction body exceptions") {
  auto txr = lfdb::make_transactor(dbh);

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

  auto dbh1 = lfdb::create_database("fishing for databass!",       // name
                { { FDB_DB_OPTION_LOCATION_CACHE_SIZE, 200'000 } }, // database options
                { { FDB_NET_OPTION_TRACE_ENABLE, lfdb::option_flag } });        // network options
 
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
 
 // In a "real" program, it would be "auto dbh = lfdb::database_handle(...)": 
 auto dbh = j.dbh();

 // This write will make and commit its own transaction:
 lfdb::set(dbh, "bucket_obj", bucket_entries);

 map<string, string> out;
 lfdb::get(dbh, "bucket_obj", out);

 // For "demo" purposes, you can ignore everything below here:
 CAPTURE(out["userId"]);
 REQUIRE(bucket_entries == out);

 j.drop_all();
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
