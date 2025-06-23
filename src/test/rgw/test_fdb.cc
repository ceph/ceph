// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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
#include <catch2/generators/catch_generators.hpp>
#define CATCH_CONFIG_MAIN

#include <fmt/format.h>
#include <fmt/chrono.h>

#include "rgw/fdb/fdb.h"

#include <chrono>

using std::string;
using std::string_view;

using fmt::format;
using fmt::println;

namespace lfdb = ceph::libfdb;

TEST_CASE("fdb present", "[rgw][fdb]") 
{
 REQUIRE_THROWS_AS([] { throw ceph::libfdb::fdb_exception(0); }(),
                   ceph::libfdb::fdb_exception);
}

TEST_CASE("wombat") {
 auto dbh = lfdb::make_database();
 REQUIRE(nullptr != dbh);

 auto txn_handle = lfdb::make_transaction(dbh);
 CHECK_NOTHROW(lfdb::set(txn_handle, "MORTAL", "WOMBAT", lfdb::commit_after_op::commit));
}

TEST_CASE("fdb simple", "[rgw][fdb]") {

 // Annoyingly, FDB doesn't namespace it's implementation, so choice identifiers like "key" 
 // are already taken: 
 const string_view k = "key";
 const string_view missing_k = "missing_key";

 const string v = fmt::format("value-{:%c}", std::chrono::system_clock::now());

 /* The first and perhaps most straightforward way of interacting with FDB is through
 the basic handles and functions. First, you need to make a database handle; this will
 do the initial database setup and also handle final teardown (it's essentially a smart 
 pointer kind of interface): */

 // set up foundationdb:
 auto dbh = lfdb::make_database();
 REQUIRE(nullptr != dbh);

 SECTION("read missing key") {
    SECTION("with transaction") {
        std::string out_value;

        auto txn_handle = lfdb::make_transaction(dbh);
        REQUIRE(nullptr != txn_handle);
  
        CAPTURE(missing_k); 
        CAPTURE(out_value); 
	REQUIRE_FALSE(lfdb::get(txn_handle, missing_k, out_value));
        CHECK(v != out_value);
    }
 }

 SECTION("write/delete single key") {
    REQUIRE(nullptr != dbh);

    auto txn_handle = lfdb::make_transaction(dbh);
    CHECK_NOTHROW(lfdb::set(txn_handle, k, v, lfdb::commit_after_op::commit));

    // "erase()" is "clear" in FDB parlance:
    REQUIRE_NOTHROW(lfdb::erase(txn_handle, k, lfdb::commit_after_op::commit));
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

TEST_CASE("multi-value CRUD", "[rgw][fdb]")
{
 const std::map<std::string, std::string> kvs {
    { "key", "value" }, 
    { "mortal", "wombat" } 
  };
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
