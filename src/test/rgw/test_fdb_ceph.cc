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

#include <catch2/benchmark/catch_benchmark.hpp>

#define CATCH_CONFIG_MAIN

#include "test/rgw/test_fdb_common.h"
#include "rgw/ceph_fdb.h"

#include <fmt/format.h>
#include <fmt/chrono.h>
#include <fmt/ranges.h>

#include "include/random.h"

#include <boost/container/flat_map.hpp>

#include <chrono>
#include <vector>
#include <ranges>
#include <algorithm>
#include <execution>

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

std::string make_key(const int n, std::string_view prefix = "key") {
 return fmt::format("{}_{:010d}", prefix, n);
}

std::string make_value(const int n) {
 return fmt::format("value_{:010d}", n);
}

// Clean up test keys when we leave scope:
struct janitor final
{
 ceph::libfdb::database_handle dbh_;

 // flip this off if you need artifacts after debugging:
 bool drop_after_scope = true;

 janitor(ceph::libfdb::database_handle dbh_)
  : dbh_(dbh_)
 {
  REQUIRE(nullptr != dbh_);
 }

 janitor()
  : janitor(ceph::libfdb::create_database())
 {}

 ~janitor()
 {
  if(drop_after_scope)
   drop_all(dbh_);
 }

 public:

 ceph::libfdb::database_handle dbh() { return dbh_; }

 // This type coersion turns out to be very useful:
 operator ceph::libfdb::database_handle() { return dbh(); }

 public:
 static void drop_all(ceph::libfdb::database_handle dbh_) {
   lfdb::erase(ceph::libfdb::make_transaction(dbh_),
               lfdb::select { "", "\xFF" });
 }

 void drop_all() { 
  return drop_all(dbh()); 
 }

 static void drop_all_keys(ceph::libfdb::database_handle dbh_) {

   // Note: technically, [0x00, 0xFF) is needed to include the system keys (if the transaction's allowed to
   // access these). However, special permissions are needed to access these magical "system keys" and we
   // probably don't actually want to delete them erroneously. So, we stick with our key range...
   // ("500,000,000 records aught to be enough for anybody.") 
   lfdb::erase(ceph::libfdb::make_transaction(dbh_),
               lfdb::select { make_key(0), make_key(500'000'000) });
   }

  void drop_all_keys() {
    return drop_all_keys(dbh());
  }

/* This is tempting, but I think it might also *hide* bugs at times. Thoughts?
  operator ceph::libfdb::database_handle() { ... }
*/
};

inline std::map<std::string, std::string> make_monotonic_kvs(const unsigned N)
{
 std::map<std::string, std::string> kvs;

 for(const auto i : std::ranges::iota_view(0u, N)) {
  kvs.insert({ make_key(i), make_value(i) });
 }

 return kvs;
}

// This is both a utility for the test suite AND an one example of doing parallel operations with FoundationDB to
// achieve some throughput. We make a transaction handle and then do a bunch of writes via that single handle-- in
// this case, we let that standard library figure out the threading details:
inline void populate_monotonic(lfdb::database_handle dbh, const int N, std::string_view prefix = "key")
{
 using namespace std::ranges;

 unsigned stride = 8*1024;

 for(auto block : views::iota(0, N) | views::chunk(stride)) {
  auto txn = make_transaction(dbh);
  std::for_each(std::execution::par, std::begin(block), std::end(block), [&txn, &prefix](const auto& n) mutable {
    lfdb::set(txn, make_key(n, prefix), make_value(n));
  });

  if(false == lfdb::commit(txn)) {
    throw std::runtime_error(fmt::format("unable to commit transaction; {} entires, {} stride", N, stride)); 
  }
 }
}

constexpr const char * const msg = "Hello, World!"; 
constexpr const char msg_with_null[] = { '\0', 'H', 'i', '\0', ' ', 't', 'h', 'e', 'r', 'e', '!', '\0'};

TEST_CASE("test the tester") {
 // Quis custodiet ipsos custodes?

 SECTION("check write and cleanup") {
  janitor j;

  auto dbh = j.dbh();

  j.drop_all_keys();

  // Check that there are no keys:
  std::string s;
  lfdb::get(dbh, make_key(2, "VEAL"), s);   
  REQUIRE(s.empty());

  populate_monotonic(dbh, 5, "VEAL");

  // Check that we have some keys!
  lfdb::get(dbh, make_key(2, "VEAL"), s);   
  REQUIRE(make_value(2) == s);

  j.drop_all_keys();
 }
}

TEST_CASE("don't use non-owning container types for output", "[example]") {
   SKIP("A cautionary tale! Never use non-owning container types as output.");

   // TL;DR: never use non-owning container types as output!
   //
   // It's possible to write an innocent-looking but actually
   // bad conversion (we may be able to fix this through some
   // resdesign, but it's how it is for now, and not user-facing). Anyway
   // DON'T DO THIS! :-) Non-owning conversions are bad.
   // ...since it won't throw consistently, I'll leave it as a cautionary
   // tale:
   const char *msg = "Greetings!";
  
   ceph::buffer::list n;
   n.append(msg);
  
   // This looks innocuous... however...
   std::span<const std::uint8_t> x;
  
   // BAD: span<> is non-owning!
   x = ceph::libfdb::to::convert(n);
  
   ceph::buffer::list o;
  
   // "x" potentially points at nothing; may throw or explode or work...:
   ceph::libfdb::from::convert(x, o);
}

TEST_CASE("fdb conversions (ceph)", "[fdb][rgw]") {

 const char *msg = "Hello, World!";

 auto dbh = lfdb::create_database();

 // ceph::buffer::list -> span<uint8_t> -> std::string
 {
  ceph::buffer::list n;
  n.append(msg);

  std::vector<std::uint8_t> x;
  x = ceph::libfdb::to::convert(n);

  std::string o;
  ceph::libfdb::from::convert(x, o); 

  REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));
 }

 // buffer::list -> span<uint8_t> -> buffer::list 
 {
 ceph::buffer::list n;
 n.append(msg);

 std::vector<std::uint8_t> x;
 x = ceph::libfdb::to::convert(n);

 ceph::buffer::list o;
 ceph::libfdb::from::convert(x, o);

 lfdb::set(lfdb::make_transaction(dbh), "key", n, lfdb::commit_after_op::commit);
 lfdb::get(lfdb::make_transaction(dbh), "key", o);
 
 REQUIRE_THAT(n, Catch::Matchers::RangeEquals(o));

 lfdb::erase(dbh, "key");
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

    lfdb::erase(dbh, "key");
  }

 SECTION("buffer::ptr")
 {
    string_view in("Hello, World!");
    ceph::buffer::ptr p(ceph::buffer::claim_char(in.length(), const_cast<char *>(in.data())));
    CHECK(in == string_view(p.c_str(), p.length()));

    lfdb::set(dbh, "key", p);

    std::string o;
    lfdb::get(dbh, "key", o);

    CHECK(in == o);
 }
}

std::iostream& operator<<(ceph::libfdb::select& obj, std::iostream& os)
{
 os << obj.begin_key;
 os << obj.end_key;
 return os;
}
namespace ceph::libfdb {

/* Not very coroutine-friendly, sadly; but will be interesting to benchmark, so I'm leaving
 * it be for now:
template <typename AssocT = boost::container::flat_map<std::string, std::string>>
auto tier_generator(ceph::libfdb::database_handle dbh, ceph::libfdb::select selector)
-> std::generator<AssocT>
{
 auto split_points = locate_split_points(dbh, selector);

 const unsigned local_max_block = 2*2024; // vis-a-vis remote request max

 std::transform_reduce(std::begin(split_points), std::end(split_points),
                      AssocT(),

                      [](auto&& lhs, auto&& rhs) {
                        return lhs.merge(rhs), lhs;
                      },

                      [dbh](const ceph::libfdb::select& selector) mutable {
                        AssocT out;

                        // ...my libstdc++ lacks std::from_range overloads; the Hard Way(TM) it is:
                        for(auto&& kvp : ceph::libfdb::pair_generator(dbh, selector)) {
                          out.emplace(kvp);
                        }

                        return out;                    
                      });
}
*/

} // namespace ceph::libfdb


TEST_CASE("generators", "[fdb][rgw]") {

/* This is a generator for large queries that are pretty much expected to 
exceed the FoundationDB five-second transaction limit: */
  SECTION("block generator") {
    janitor j;
    auto dbh = j.dbh();

    const size_t nkeys = GENERATE(0, 1, 1000); // , 100'000, 1'000'000);
 
    populate_monotonic(dbh, nkeys);

    SECTION("retrieve all") {
      auto selector = lfdb::select { make_key(0), make_key(1 + nkeys) };

      size_t total = 0;
      for(const auto& block : lfdb::block_generator(dbh, selector)) {
        total += block.size();
      }

      CHECK(nkeys == total);
    }
  }
}

TEST_CASE("generators", "[benchmark]") {
   janitor j;
   auto dbh = j.dbh();

   const size_t nkeys = GENERATE(0, 1, 1'000); // 5'000, 10'000, 50'000, 250'000, 1'000'000);

   fmt::println("generator benchmark, nkeys = {}", nkeys);
 
   populate_monotonic(dbh, nkeys);

  BENCHMARK("block generator read all") {
    janitor j;
    auto dbh = j.dbh();

    populate_monotonic(dbh, nkeys);

    auto selector = lfdb::select { make_key(0), make_key(1 + nkeys) };

    size_t total = 0;
    for(const auto& block : lfdb::block_generator(dbh, selector)) {
      total += block.size();
    }

    REQUIRE(nkeys == total);
  };
}

// Note that these are disabled for regular test runs. Use
//      unittest_fdb_ceph "simple benchmarks"
// ...to run:
TEST_CASE("simple benchmarks", "[benchmark]") {

using namespace std::ranges;
using std::for_each;

// Remember that these will create exponential growth; you might
// not want to run ALL combinations if you're short on time:
auto N = GENERATE(0, 1, 500); // 1'000, 250'000, 1'000'000);

unsigned stride = 2048; // 2-4K per chunk feels about right

fmt::println("N = {}", N);
fmt::println("stride = {}", stride);

std::vector<std::pair<std::string, std::string>> inputs;
inputs.reserve(N);

// Sadly, my version of libstdc++ lacks std::from_range_t:
for(auto n : views::iota(0, N)) {
 inputs.emplace_back(std::make_pair<std::string, std::string>(make_key(n), make_value(n)));
}

BENCHMARK("write simple records-- serial") {
 janitor j;
 auto dbh = j.dbh();

 // Notice that here we are passing around the database handle, winding up making a new one per-operation:
 for(auto block : inputs | views::chunk(stride)) {
  std::for_each(std::begin(block), std::end(block), [&dbh](const auto& kv) mutable {
    lfdb::set(dbh, kv.first, kv.second);
  });
  }
};

BENCHMARK("write simple records-- serial, shared transaction") {
 janitor j;
 auto dbh = j.dbh();

 // Shared-transaction:
 for(auto block : inputs | views::chunk(stride)) {
  auto txn = make_transaction(dbh);
  std::for_each(std::begin(block), std::end(block), [&txn](const auto& kv) mutable {
    lfdb::set(txn, kv.first, kv.second);
  });
  if(false == lfdb::commit(txn)) {
    throw std::runtime_error("unable to commit transaction");
  }
 }
};

BENCHMARK("write simple records-- parallel, shared transaction") {
 janitor j;
 auto dbh = j.dbh();

 // Both parallel and shared-transaction:
 for(auto block : inputs | views::chunk(stride)) {
  auto txn = make_transaction(dbh);
  std::for_each(std::execution::par, std::begin(block), std::end(block), [&txn](const auto& kv) mutable {
    lfdb::set(txn, kv.first, kv.second);
  });
  if(false == lfdb::commit(txn)) {
    throw std::runtime_error("unable to commit transaction");
  }
 }
};

}

#include <catch2/catch_session.hpp>

int main(int argc, char **argv) 
{
  int result = Catch::Session().run(argc, argv);

  // Make sure that FoundationDB is shut down once and only once:
  ceph::libfdb::shutdown_libfdb(); 

  return result;
}

