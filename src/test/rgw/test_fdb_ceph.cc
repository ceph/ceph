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

TEST_CASE("test the tester") {
 // Quis custodiet ipsos custodes?

 SECTION("check write and cleanup") {
  janitor j;

  j.drop_all_keys();

  // Check that there are no keys:
  std::string s;
  lfdb::get(j, make_key(2, "VEAL"), s);   
  REQUIRE(s.empty());

  populate_monotonic(j, 5, "VEAL");

  // Check that we have some keys!
  lfdb::get(j, make_key(2, "VEAL"), s);   
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

 SECTION("buffer::list direct and const conversion paths match")
 {
   ceph::buffer::list n;
   n.append("direct conversion should match the normal const path");

   auto direct = ceph::libfdb::to::convert(n);

   const auto& cn = n;
   auto via_serialize = ceph::libfdb::to::convert(cn);

   CHECK(direct == via_serialize);
 }

 SECTION("empty buffer::list direct and const conversion paths match")
 {
   ceph::buffer::list n;

   auto direct = ceph::libfdb::to::convert(n);

   const auto& cn = n;
   auto via_serialize = ceph::libfdb::to::convert(cn);

   CHECK(direct == via_serialize);
 }

 SECTION("buffer::list with embedded nulls uses the same conversion path")
 {
   constexpr char data[] = { '\0', 'H', 'i', '\0', '!', '\0' };

   ceph::buffer::list n;
   n.append(data, sizeof(data));

   auto direct = ceph::libfdb::to::convert(n);

   const auto& cn = n;
   auto via_serialize = ceph::libfdb::to::convert(cn);

   CHECK(direct == via_serialize);
 }

 SECTION("buffer::list key span preserves logical key bytes")
 {
   ceph::buffer::list key;
   key.append("buffer-list-key");

   auto key_span = ceph::libfdb::detail::as_fdb_span(key);

   CHECK(key.length() == key_span.size());
   CHECK(std::ranges::equal(std::span((const char *)key_span.data(), key_span.size()),
                            std::string_view("buffer-list-key")));
 }

 SECTION("empty buffer::list key span is empty")
 {
   ceph::buffer::list key;

   auto key_span = ceph::libfdb::detail::as_fdb_span(key);

   CHECK(0 == key_span.size());
 }

 SECTION("buffer::list key span preserves embedded nulls")
 {
   constexpr char data[] = { '\0', 'k', 'e', 'y', '\0' };

   ceph::buffer::list key;
   key.append(data, sizeof(data));

   auto key_span = ceph::libfdb::detail::as_fdb_span(key);

   CHECK(key.length() == key_span.size());
   CHECK(std::ranges::equal(std::span((const char *)key_span.data(), key_span.size()),
                            std::string_view(data, sizeof(data))));
 }

 SECTION("empty buffer::list round trip through public set/get")
 {
   ceph::buffer::list in;

   lfdb::set(dbh, "empty-buffer-list", in);

   ceph::buffer::list out;
   REQUIRE(lfdb::get(dbh, "empty-buffer-list", out));
   CHECK(0 == out.length());

   lfdb::erase(dbh, "empty-buffer-list");
 }

 SECTION("buffer::list with embedded nulls round trips through public set/get")
 {
   constexpr char data[] = { '\0', 'A', '\0', 'B', '\0' };

   ceph::buffer::list in;
   in.append(data, sizeof(data));

   lfdb::set(dbh, "embedded-null-buffer-list", in);

   ceph::buffer::list out;
   REQUIRE(lfdb::get(dbh, "embedded-null-buffer-list", out));
   REQUIRE_THAT(in, Catch::Matchers::RangeEquals(out));

   lfdb::erase(dbh, "embedded-null-buffer-list");
 }

 SECTION("buffer::list get replaces existing output")
 {
   ceph::buffer::list in;
   in.append("new-value");

   ceph::buffer::list out;
   out.append("old-value");

   lfdb::set(dbh, "replace-buffer-list-output", in);

   REQUIRE(lfdb::get(dbh, "replace-buffer-list-output", out));
   REQUIRE_THAT(in, Catch::Matchers::RangeEquals(out));

   lfdb::erase(dbh, "replace-buffer-list-output");
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
 auto split_ranges = plan_split_ranges(dbh, selector);

 const unsigned local_max_block = 2*2024; // vis-a-vis remote request max

 std::transform_reduce(std::begin(split_ranges), std::end(split_ranges),
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

struct ordered_block final : std::vector<std::pair<std::string, std::string>>
{
  using std::vector<std::pair<std::string, std::string>>::vector;

  void emplace(std::pair<std::string, std::string>&& value)
  {
    emplace_back(std::move(value));
  }
};


TEST_CASE("block_generator", "[fdb][rgw]") {

/* This is a generator for large queries that are pretty much expected to 
exceed the FoundationDB five-second transaction limit: */
  SECTION("block generator") {
    janitor j;

    const size_t nkeys = GENERATE(0, 1, 1000); // , 100'000, 1'000'000);
 
    populate_monotonic(j, nkeys);

    SECTION("forward") {
      auto selector = lfdb::select { "key_" };

      size_t total = 0;
      for(const auto& block : lfdb::block_generator(j, selector)) {
        total += block.size();
      }

      CAPTURE(nkeys);
      CAPTURE(total);
      CHECK(nkeys == total);
    }

    SECTION("reverse") {
      auto selector = lfdb::select { "key_" };
      selector.options.reverse_order = true;

      std::vector<std::pair<std::string, std::string>> out;

      for(const auto& block : lfdb::block_generator<ordered_block>(j, selector)) {
        std::ranges::copy(block, std::back_inserter(out));
      }

      CAPTURE(nkeys);
      CAPTURE(out.size());
      REQUIRE(nkeys == out.size());

      if(0 < nkeys) {
        CAPTURE(out.front().first);
        CAPTURE(out.back().first);
        CHECK(make_key(nkeys - 1) == out.front().first);
        CHECK(make_key(0) == out.back().first);
        CHECK(std::ranges::is_sorted(out, std::ranges::greater {},
                                     &std::pair<std::string, std::string>::first));
      }
    }
  }
}

TEST_CASE("generators", "[.benchmark][benchmark]") {
   const size_t nkeys = GENERATE(0, 1, 1'000); // 5'000, 10'000, 50'000, 250'000, 1'000'000);

   fmt::println("generator benchmark, nkeys = {}", nkeys);

  BENCHMARK_ADVANCED("block generator read all")(Catch::Benchmark::Chronometer meter) {
    janitor j;

    populate_monotonic(j, nkeys);

    auto selector = lfdb::select { "key_" };

    size_t total = 0;
    meter.measure([&j, &selector, &total] {
      total = 0;
      for(const auto& block : lfdb::block_generator(j, selector)) {
        total += block.size();
      }
    });

    REQUIRE(nkeys == total);
  };
}

TEST_CASE("read path benchmarks", "[.benchmark][benchmark]") {
  const size_t nkeys = GENERATE(0, 1, 100, 1'000, 10'000); // 250'000, 500'000, 1'000'000

  fmt::println("read path benchmark, nkeys = {}", nkeys);

  // Set up bulk data for all of the different test reads.
  janitor dbh;
  populate_monotonic(dbh, nkeys);

  const auto selector = lfdb::select { "key_" };

  // Track enough work to prevent the benchmarked reads from being optimized away:
  struct {
    size_t total = 0;
    size_t bytes = 0;
    bool ok = true;
    std::string error;

    void reset()
    {
      total = 0;
      bytes = 0;
      ok = true;
      error.clear();
    }

    void add(std::string_view value)
    {
      ++total;
      bytes += value.size();
    }

  } read_tally;

  auto mark_failure = [&read_tally](const std::exception& e) {
    read_tally.ok = false;
    read_tally.error = e.what();
  };

  auto add_kv = [&read_tally](const auto& kv) {
    read_tally.add(kv.second);
  };

  auto key_indices = [nkeys] {
    return std::views::iota(0u, static_cast<unsigned>(nkeys));
  };

  auto expected_bytes = [&] {
    size_t bytes = 0;

    std::ranges::for_each(key_indices(), [&bytes](const auto n) {
      bytes += make_value(n).size();
    });

    return bytes;
  }();

  auto require_expected = [nkeys, expected_bytes](const auto& tally) {
    if(not tally.ok) {
      WARN(tally.error);
      return;
    }

    REQUIRE(nkeys == tally.total);
    REQUIRE(expected_bytes == tally.bytes);
  };

  BENCHMARK_ADVANCED("single-key get, one shared transaction")(Catch::Benchmark::Chronometer meter) {
    meter.measure([&] {
      read_tally.reset();

      try {
        auto txn = lfdb::make_transaction(dbh);

        for(const auto n : key_indices()) {
          std::string out;

          if (lfdb::get(txn, make_key(n), out, lfdb::commit_after_op::no_commit)) {
            read_tally.add(out);
          }
        }
      } catch(const ceph::libfdb::libfdb_exception& e) {
        mark_failure(e);
      }
    });

    require_expected(read_tally);
  };

  BENCHMARK_ADVANCED("single-key get, implicit transaction per key")(Catch::Benchmark::Chronometer meter) {
    meter.measure([&] {
      read_tally.reset();

      try {
        for(const auto n : key_indices()) {
          std::string out;

          if (lfdb::get(dbh, make_key(n), out)) {
            read_tally.add(out);
          }
        }
      } catch(const ceph::libfdb::libfdb_exception& e) {
        mark_failure(e);
      }
    });

    require_expected(read_tally);
  };

  BENCHMARK_ADVANCED("pair generator read all")(Catch::Benchmark::Chronometer meter) {
    meter.measure([&] {
      read_tally.reset();

      try {
        std::ranges::for_each(lfdb::pair_generator(dbh, selector), add_kv);
      } catch(const ceph::libfdb::libfdb_exception& e) {
        mark_failure(e);
      }
    });

    require_expected(read_tally);
  };

  BENCHMARK_ADVANCED("block generator read all")(Catch::Benchmark::Chronometer meter) {
    meter.measure([&] {
      read_tally.reset();

      try {
        std::ranges::for_each(lfdb::block_generator(dbh, selector), [&](const auto& block) {
          std::ranges::for_each(block, add_kv);
        });
      } catch(const ceph::libfdb::libfdb_exception& e) {
        mark_failure(e);
      }
    });

    require_expected(read_tally);
  };
}

// Note that these are disabled for regular test runs. Use
//      unittest_fdb_ceph "simple benchmarks"
// ...to run:
TEST_CASE("simple benchmarks", "[.benchmark][benchmark]") {

using namespace std::ranges;
using std::for_each;

// Remember that these will create exponential growth; you might
// not want to run ALL combinations if you're short on time:
auto N = GENERATE(0, 1, 256, 4'096); // 250'000, 1'000'000);

unsigned stride = 512;

fmt::println("N = {}", N);
fmt::println("stride = {}", stride);

// For casual use, these simply get intolerably slow at higher values:
const bool run_slow_baselines = (N <= 256);

std::vector<std::pair<std::string, std::string>> inputs;
inputs.reserve(N);

// Sadly, my version of libstdc++ lacks std::from_range_t:
for(auto n : views::iota(0, N)) {
 inputs.emplace_back(std::make_pair<std::string, std::string>(make_key(n), make_value(n)));
}

if(run_slow_baselines) {
 BENCHMARK_ADVANCED("write simple records-- implicit transaction per key")(Catch::Benchmark::Chronometer meter) {
  janitor j;

  meter.measure([&j, &inputs, stride] {
   // Notice that here we are passing around the database handle, winding up making a new one per-operation:
   for(auto block : inputs | views::chunk(stride)) {
    std::for_each(std::begin(block), std::end(block), [&j](const auto& kv) mutable {
      lfdb::set(j, kv.first, kv.second);
    });
   }
  });
 };
}

BENCHMARK_ADVANCED("write simple records-- one transaction per block")(Catch::Benchmark::Chronometer meter) {
 janitor j;

 meter.measure([&j, &inputs, stride] {
  // Shared-transaction:
  for(auto block : inputs | views::chunk(stride)) {
   auto txn = lfdb::make_transaction(j);
   std::for_each(std::begin(block), std::end(block), [&txn](const auto& kv) mutable {
     lfdb::set(txn, kv.first, kv.second);
   });
   if(false == lfdb::commit(txn)) {
     throw std::runtime_error("unable to commit transaction");
   }
  }
 });
};

BENCHMARK_ADVANCED("write simple records-- range set, one transaction per block")(Catch::Benchmark::Chronometer meter) {
 janitor j;

 meter.measure([&j, &inputs, stride] {
  for(auto block : inputs | views::chunk(stride)) {
   lfdb::set(j, std::begin(block), std::end(block));
  }
 });
};

// On a local FDB setup, block parallelism has not outperformed serial block
// writes, but it is included as a safe comparison point; remote FDB not yet considered:
BENCHMARK_ADVANCED("write simple records-- parallel, one transaction per block")(Catch::Benchmark::Chronometer meter) {
 janitor j;

 meter.measure([&j, &inputs, stride] {
  auto blocks = inputs | views::chunk(stride);

  std::for_each(std::execution::par, std::begin(blocks), std::end(blocks),
                [&j](const auto& block) mutable {
                  auto txn = lfdb::make_transaction(j);

                  std::ranges::for_each(block, [&txn](const auto& kv) mutable {
                    lfdb::set(txn, kv.first, kv.second);
                  });

                  if(false == lfdb::commit(txn)) {
                    throw std::runtime_error("unable to commit transaction");
                  }
                });
 });
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
