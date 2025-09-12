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
//JFW: auto dbh = lfdb::create_database();
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

auto key_count(auto& dbh, const auto& selector) {
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

// Clean up test keys when we leave scope:
struct janitor final
{
 // flip this off if you need artifacts after debugging:
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
   lfdb::erase(lfdb::make_transaction(lfdb::create_database()),
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

TEST_CASE("fdb simple (delete keys in range)", "[rgw][fdb]") {
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

/* JFW:
TEST_CASE("cancel a transaction", "[fdb][rgw]") {
given
  write some data in a transaction
then:
  read it in the transaction should work
when cancel the tranaction
  should fail to read
}

template <typename SeqT=std::vector<std::pair<std::string, std::string>>>
std::generator<SeqT> get(transaction_handle& txn, const selector, const stride)
{
JFW
}

TEST_CASE("multi-key \"infinite\" results", "[fdb][rgw]") {
 janitor j;

 // This really should be left at 1000 for the test to make sense:
 constexpr int nkeys = 1000;

 auto kvs = make_monotonic_kvs(nkeys);

 lfdb::set(dbh, kvs);

 // Note that while this will select all keys, FoundationDB system keys begin with 0xFF and cannot
 // be modified without ACCESS_SYSTEM_KEYS being turned on for the transaction:
 constexpr auto all_keys = lfdb::select { "", fmt::format("\xFF") };

 // Make sure they were written:
 REQUIRE(nkeys == lfdb::key_count(dbh, all_keys));


FDBFuture *fdb_transaction_get_range(FDBTransaction *transaction, uint8_t const *begin_key_name, int begin_key_name_length, fdb_bool_t begin_or_equal, int begin_offset, uint8_t const *end_key_name, int end_key_name_length, fdb_bool_t end_or_equal, int end_offset, int limit, int target_bytes, FDBStreamingMode mode, int iteration, fdb_bool_t snapshot, fdb_bool_t reverse)

check w/ generator
 for(const auto stride : { 1, 10, 100, 1000 }) {
  auto results = lfdb::get(dbh, all_keys, stride);

  while(std::vector<std::string> rs = results) {
    ; print n results
  } 
 }

generators have iterator hooks:
 auto results = lfdb::get(dbh, all_keys, stride);

 for(auto b = std::begin(results); std::end(results) != b; ++b) {
 }
}*/

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
  
  set(dbh, "key", "value");
  
  std::string out;
  
  get(dbh, "key", out);
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

TEST_CASE("Gal demo", "[fdb]") {
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

