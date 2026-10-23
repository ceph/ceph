// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 * Licensed under the GNU Lesser General Public License version 2.1.
 * See file COPYING.
 */

// Experimental comparison harness; build once per backend in Release mode.
#include "rgw/fdb/fdb.h"
#include "rgw/ceph_fdb_execution.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <ctime>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/io_context.hpp>

#include <unistd.h>

namespace lfdb = ceph::libfdb;
namespace lfdbx = lfdb::experimental;
using bench_clock = std::chrono::steady_clock;

namespace {

void write_bytes(const lfdb::transaction_handle& txn, const std::string_view key,
                  const std::string_view value)
{
 lfdb::detail::transaction_set_kv_bytes(txn, lfdb::detail::as_byte_view(key),
                                       lfdb::detail::as_byte_view(value));
}

struct data_set final
{
 lfdb::database_handle dbh = lfdb::create_database();
 std::string prefix = "ceph-libfdb-bench/" + std::to_string(::getpid()) + "/" +
                      std::to_string(bench_clock::now().time_since_epoch().count()) + "/";
 std::vector<std::string> keys;
 std::string value = std::string(256, 'v');

 data_set()
 {
  auto txn = lfdb::make_transaction(dbh);

  for (int i = 0; i < 64; ++i) {
   keys.push_back(prefix + "data/" + std::to_string(1000 + i) + std::string(96, 'k'));
   write_bytes(txn, keys.back(), value);
  }

  lfdb::commit(txn);
 }

 ~data_set()
 {
  try {
   lfdb::erase(lfdb::make_transaction(dbh), lfdb::select {prefix},
               lfdb::commit_after_op::commit);
  } catch (...) {
   std::cerr << "benchmark cleanup failed for " << prefix << '\n';
  }
 }
};

template <typename FactoryT, typename CheckT>
class sample final
{
 boost::asio::io_context context;
 FactoryT factory;
 CheckT check;
 std::size_t issued = 0;
 const std::size_t total;
 std::exception_ptr error;
 std::vector<double> latency;

 void submit(const std::size_t lane)
 {
  if (error || issued == total) {
   return;
  }

  ++issued;
  const auto started = bench_clock::now();
  lfdbx::async_wait(factory(lane), boost::asio::bind_executor(context.get_executor(),
    [this, lane, started](std::exception_ptr failure, auto result) {
     if (failure) {
      error = std::move(failure);
      return;
     }

     try {
      if (not result) {
       throw std::runtime_error("benchmark operation stopped");
      }

      check(*result);
     } catch (...) {
      error = std::current_exception();
      return;
     }

     latency.push_back(std::chrono::duration<double, std::micro>(
       bench_clock::now() - started).count());
     submit(lane);
    }));
 }

 public:
 sample(FactoryT factory, CheckT check, const std::size_t total)
  : factory(std::move(factory)), check(std::move(check)), total(total)
 {
  latency.reserve(total);
 }

 void run(const std::string_view workload, const std::size_t depth, const bool report)
 {
  const auto cpu_started = std::clock();
  const auto started = bench_clock::now();

  for (std::size_t lane = 0; lane < depth && issued < total; ++lane) {
   submit(lane);
  }

  context.run();
  const auto seconds = std::chrono::duration<double>(bench_clock::now() - started).count();
  const auto cpu_us = 1e6 * (std::clock() - cpu_started) / CLOCKS_PER_SEC;

  if (error) {
   std::rethrow_exception(error);
  }

  if (latency.size() != total) {
   throw std::runtime_error("incomplete benchmark sample");
  }

  if (not report) {
   return;
  }

  std::ranges::sort(latency);
  auto percentile = [this](const double quantile) {
   return latency[static_cast<std::size_t>(quantile * (latency.size() - 1))];
  };
#if defined(CEPH_LIBFDB_EXPERIMENTAL_ASIO)
  constexpr auto backend = "ASIO";
#else
  constexpr auto backend = "NVIDIA_STDEXEC";
#endif
  std::cout << backend << ',' << workload << ',' << depth << ',' << total << ','
            << total / seconds << ',' << percentile(.5) << ',' << percentile(.95) << ','
            << percentile(.99) << ',' << cpu_us / total << '\n';
 }
};

void measure(data_set& data, const std::string_view workload, const std::size_t depth,
              const std::size_t count, const bool report)
{
 std::vector<lfdb::transaction_handle> transactions;

 for (std::size_t lane = 0; lane < depth; ++lane) {
  auto txn = lfdb::make_transaction(data.dbh);

  if (workload.ends_with("ryw")) {
   for (const auto& key : data.keys) {
    write_bytes(txn, key, data.value);
   }
  }

  transactions.push_back(std::move(txn));
 }

 auto check_point = [&data](const lfdbx::point_read_result& result) {
  const auto bytes = result.bytes();

  if (not bytes || bytes->size() != data.value.size() || bytes->front() != 'v') {
   throw std::runtime_error("invalid benchmark point result");
  }
 };
 auto run = [count, depth, report, workload](auto factory, auto check) {
  sample {std::move(factory), std::move(check), count}.run(workload, depth, report);
 };

 if ("point_owned_ryw" == workload) {
  run([&data, &transactions](const auto lane) {
   return lfdbx::get(lfdb::raw, transactions[lane], std::string {data.keys.front()});
  }, check_point);
  return;
 }

 if ("point_borrowed_ryw" == workload) {
  run([&data, &transactions](const auto lane) {
   return lfdbx::get(lfdb::raw, transactions[lane], std::string_view {data.keys.front()});
  }, check_point);
  return;
 }

 if ("point_live" == workload) {
  run([&data](auto) {
   return lfdbx::get(lfdb::raw, lfdb::make_transaction(data.dbh), data.keys.front());
  }, check_point);
  return;
 }

 if ("scan_ryw" == workload) {
  run([&data, &transactions](const auto lane) {
   return lfdbx::read_window(lfdb::raw, transactions[lane],
     lfdbx::scan_cursor {lfdb::query::with_options(lfdb::select {data.prefix + "data/"},
       lfdb::query::query_options {.result_limit = 64, .streaming_mode = FDB_STREAMING_MODE_EXACT})});
  }, [](const lfdbx::scan_window& window) {
   if (64 != std::ranges::distance(window.rows())) {
    throw std::runtime_error("invalid benchmark scan result");
   }
  });
  return;
 }

 if ("commit" == workload) {
  run([&data](const auto lane) {
   auto txn = lfdb::make_transaction(data.dbh);
   write_bytes(txn, data.prefix + "write/" + std::to_string(lane), data.value);
   return lfdbx::commit(std::move(txn));
  }, [](const std::tuple<>&) {});
  return;
 }

 if ("transact" == workload) {
  run([&data](const auto lane) {
   return lfdbx::transact(data.dbh, [&data, lane](lfdb::transaction_handle& txn) {
    auto key = data.prefix + "write/" + std::to_string(lane);
    write_bytes(txn, key, data.value);
    return lfdbx::get(lfdb::raw, txn, std::move(key));
   });
  }, check_point);
  return;
 }

 throw std::invalid_argument("unknown benchmark workload");
}

} // anonymous namespace

int main(int argc, char **argv)
{
 int status = 0;

 try {
  if (argc < 4 || argc > 5) {
   throw std::invalid_argument("usage: bench_fdb_concurrency WORKLOAD DEPTH COUNT [REPEATS]");
  }

  const auto workload = std::string_view {argv[1]};
  const auto depth = std::stoul(argv[2]);
  const auto count = std::stoul(argv[3]);
  const auto repeats = argc == 5 ? std::stoul(argv[4]) : 1;

  if (0 == depth || depth > 64 || 0 == count || count > 10'000'000 || 0 == repeats || repeats > 1000) {
   throw std::invalid_argument("depth must be 1..64; count 1..10000000; repeats 1..1000");
  }

  data_set data;
  std::cout << "backend,workload,depth,count,ops_per_sec,p50_us,p95_us,p99_us,cpu_us_per_op\n";
  measure(data, workload, depth, std::min<std::size_t>(count, 256), false);

  for (std::size_t i = 0; i < repeats; ++i) {
   measure(data, workload, depth, count, true);
  }
 } catch (const std::exception& error) {
  std::cerr << error.what() << '\n';
  status = 1;
 }

 lfdb::shutdown_libfdb();
 return status;
}
