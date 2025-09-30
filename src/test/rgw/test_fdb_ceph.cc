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

#include "rgw/rgw_fdb.h"

#include <fmt/format.h>
#include <fmt/chrono.h>
#include <fmt/ranges.h>

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

constexpr const char * const msg = "Hello, World!"; 
constexpr const char msg_with_null[] = { '\0', 'H', 'i', '\0', ' ', 't', 'h', 'e', 'r', 'e', '!', '\0'};

TEST_CASE("fdb conversions (ceph)", "[fdb][rgw]") {

 const char *msg = "Hello, World!";

 {
  ceph::buffer::list n;
  n.append(msg);

  std::vector<std::uint8_t> x;
  x = ceph::libfdb::to::convert(n);

  std::string o;
  ceph::libfdb::from::convert(x, o); 

  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }

 {
 ceph::buffer::list n;
 n.append(msg);

 std::vector<std::uint8_t> x;
 x = ceph::libfdb::to::convert(n);

 ceph::buffer::list o;
 ceph::libfdb::from::convert(x, o);

 REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }
}

TEST_CASE("fdb conversions (round-trip, ceph)", "[fdb][rgw]") {

  auto dbh = lfdb::make_database();

  SECTION("string_view -> buffer::list")
  {
    const std::string_view n = "Hello, World!";
    ceph::buffer::list o;
  
    lfdb::set(lfdb::make_transaction(dbh), "key", n, lfdb::commit_after_op::commit);
    lfdb::get(lfdb::make_transaction(dbh), "key", o);
  
    REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
  }

  SECTION("buffer::list (and buffer::list key) -> buffer::list")
  {
    const std::string_view n { "Hello, World!" };
  
    ceph::buffer::list o;
    o.append(n);
  
    lfdb::set(lfdb::make_transaction(dbh), "key", n, lfdb::commit_after_op::commit);
    lfdb::get(lfdb::make_transaction(dbh), "key", o);
  
    REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
  }

  SECTION("buffer::list (and buffer::list key) -> buffer::list")
  {
    ceph::buffer::list n;
    n.append("Hello, World!");
  
    ceph::buffer::list o;
    o.append(n);
  
    lfdb::set(lfdb::make_transaction(dbh), "key", n, lfdb::commit_after_op::commit);
    lfdb::get(lfdb::make_transaction(dbh), "key", o);
  
    REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
  }

  SECTION("buffer::list (and buffer::list key) -> buffer::list")
  {
    ceph::buffer::list n;
    n.append("Hello, World!");
  
    ceph::buffer::list o;
    o.append(n);
  
    lfdb::set(lfdb::make_transaction(dbh), "key", n, lfdb::commit_after_op::commit);
    lfdb::get(lfdb::make_transaction(dbh), "key", o);
  
    REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
  }
}

TEST_CASE("standard container FDB conversions") {

 const std::map<int, std::string> kvs {
   { 0, "hello" },
   { 1, "world" }
 };

 CHECK(2 == kvs.size());
 CHECK("hello" == kvs.at(0));
 CHECK("world" == kvs.at(1));

 std::vector<std::uint8_t> buffer = ceph::libfdb::to::convert(kvs);

 std::map<int, std::string> kvs_out;
 ceph::libfdb::from::convert(buffer, kvs_out);

 CHECK(2 == kvs_out.size());
 CHECK("hello" == kvs_out.at(0));
 CHECK("world" == kvs_out.at(1));

 // ...all the above to basically say:
 CHECK(kvs == kvs_out);
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

