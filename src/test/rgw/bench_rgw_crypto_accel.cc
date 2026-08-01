// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Ceph contributors
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/PluginRegistry.h"
#include "rgw_common.h"
#include "rgw_crypt.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#ifdef __x86_64__
#include <immintrin.h>
#endif

std::unique_ptr<BlockCrypt> AES_256_CBC_create(
    const DoutPrefixProvider* dpp, CephContext* cct,
    const uint8_t* key, size_t len);

#define dout_subsys ceph_subsys_rgw

using std::cout;
using std::cerr;
using std::string;
using std::vector;

static bool is_supported_accel(const string& accel) {
  return accel == "crypto_isal" || accel == "crypto_openssl" ||
         accel == "crypto_qat";
}

static string human_bytes(double bytes) {
  const char* units[] = {"B", "KB", "MB", "GB", "TB"};
  int u = 0;
  while (bytes >= 1024.0 && u < 4) { bytes /= 1024.0; ++u; }
  char buf[64];
  snprintf(buf, sizeof(buf), "%.2f %s", bytes, units[u]);
  return buf;
}

static void fill_random(uint8_t* buf, size_t len) {
  static std::mt19937_64 rng(42);
  size_t i = 0;
  for (; i + sizeof(uint64_t) <= len; i += sizeof(uint64_t)) {
    uint64_t v = rng();
    memcpy(buf + i, &v, sizeof(v));
  }
  for (; i < len; ++i) buf[i] = rng() & 0xff;
}

/* Verify that a crypto instance can perform a basic encrypt operation. */
static bool probe_encrypt(std::unique_ptr<BlockCrypt>& crypt,
                          const string& err_msg) {
  if (!crypt) { cerr << err_msg << "\n"; return false; }
  bufferlist in, out;
  buffer::ptr buf(4096);
  fill_random(reinterpret_cast<uint8_t*>(buf.c_str()), 4096);
  in.append(buf);
  if (!crypt->encrypt(in, 0, 4096, out, 0, null_yield)) {
    cerr << err_msg << "\n";
    return false;
  }
  return true;
}

/* Throughput display unit, set once from --unit CLI option. */
static const char* tp_unit = "MB/s";
static double tp_divisor = 1024.0 * 1024.0;

struct BenchResult {
  string label;
  size_t data_size;
  int iterations;
  double elapsed_sec;

  double throughput() const {
    if (elapsed_sec <= 0) return 0;
    return static_cast<double>(data_size) * iterations / elapsed_sec / tp_divisor;
  }
  double ops_per_sec() const {
    if (elapsed_sec <= 0) return 0;
    return iterations / elapsed_sec;
  }
};

using CryptFactory = std::function<std::unique_ptr<BlockCrypt>()>;
using BenchFn = std::function<bool(BlockCrypt&)>;

static int probe_iters(double target_time, double min_sec,
                       int warmup_iters, BlockCrypt& crypt, BenchFn& fn) {
  for (int i = 0; i < warmup_iters; ++i) {
    if (!fn(crypt)) return 0;
  }
  int probe_n = 10;
  double probe_sec = 0;
  do {
    auto t0 = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < probe_n; ++i) {
      if (!fn(crypt)) return 0;
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    probe_sec = std::chrono::duration<double>(t1 - t0).count();
    if (probe_sec < min_sec) probe_n *= 2;
  } while (probe_sec < min_sec);
  return std::max(static_cast<int>(target_time / (probe_sec / probe_n)), 100);
}

static void spin_barrier(std::atomic<int>& barrier, int target) {
  barrier.fetch_add(1, std::memory_order_release);
  while (barrier.load(std::memory_order_acquire) < target) {
#ifdef __x86_64__
    _mm_pause();
#else
    std::this_thread::yield();
#endif
  }
}

static BenchResult run_bench(const string& label, size_t data_size,
                             int warmup_iters, double target_time,
                             int fixed_iters, int nthreads,
                             CryptFactory factory, BenchFn bench_fn) {
  if (nthreads <= 1) {
    auto crypt = factory();
    if (!crypt) {
      cerr << "FAILED: " << label << "\n";
      return {label, data_size, 0, 0.0};
    }
    int iters = fixed_iters;
    if (iters <= 0)
      iters = probe_iters(target_time, 0.1, warmup_iters, *crypt, bench_fn);
    if (iters == 0) return {label, data_size, 0, 0.0};
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iters; ++i) {
      if (!bench_fn(*crypt)) {
        cerr << "FAILED at iteration " << i << ": " << label << "\n";
        return {label, data_size, i, 0.0};
      }
    }
    auto end = std::chrono::high_resolution_clock::now();
    return {label, data_size, iters,
            std::chrono::duration<double>(end - start).count()};
  }

  int iters = fixed_iters;
  if (iters <= 0) {
    const double probe_dur = 0.25;
    std::vector<int> per_thread_count(nthreads);
    std::atomic<int> barrier{0};
    std::vector<std::thread> threads;
    std::atomic<bool> probe_failed{false};
    for (int t = 0; t < nthreads; ++t) {
      threads.emplace_back([&, t]() {
        auto crypt = factory();
        if (!crypt) { probe_failed.store(true); }
        if (!probe_failed.load()) {
          for (int i = 0; i < warmup_iters; ++i) bench_fn(*crypt);
        }
        spin_barrier(barrier, nthreads);
        if (probe_failed.load()) return;
        int count = 0;
        auto t0 = std::chrono::high_resolution_clock::now();
        double elapsed = 0;
        do {
          bench_fn(*crypt);
          ++count;
          elapsed = std::chrono::duration<double>(
              std::chrono::high_resolution_clock::now() - t0).count();
        } while (elapsed < probe_dur);
        per_thread_count[t] = count;
      });
    }
    for (auto& t : threads) t.join();
    int slowest = *std::min_element(per_thread_count.begin(),
                                     per_thread_count.end());
    iters = std::max(static_cast<int>(slowest * (target_time / probe_dur)),
                     100);
  }

  std::vector<double> thread_elapsed(nthreads);
  std::atomic<int> barrier{0};
  std::atomic<bool> failed{false};

  auto thread_body = [&](int tid) {
    auto crypt = factory();
    if (!crypt) { failed.store(true); }
    if (!failed.load()) {
      for (int i = 0; i < warmup_iters; ++i) {
        if (!bench_fn(*crypt)) { failed.store(true); break; }
      }
    }
    // always reach the barrier so other threads don't spin forever
    spin_barrier(barrier, nthreads);
    if (failed.load()) return;
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iters; ++i) {
      if (!bench_fn(*crypt)) { failed.store(true); return; }
    }
    auto end = std::chrono::high_resolution_clock::now();
    thread_elapsed[tid] = std::chrono::duration<double>(end - start).count();
  };

  std::vector<std::thread> threads;
  for (int t = 0; t < nthreads; ++t)
    threads.emplace_back(thread_body, t);
  for (auto& t : threads) t.join();

  if (failed.load()) {
    cerr << "FAILED: " << label << "\n";
    return {label, data_size, 0, 0.0};
  }

  return {label, data_size, iters * nthreads,
          *std::max_element(thread_elapsed.begin(), thread_elapsed.end())};
}

static void print_header() {
  cout << std::left
       << std::setw(36) << "Benchmark"
       << std::setw(12) << "DataSize"
       << std::setw(10) << "Iters"
       << std::setw(12) << "Time(s)"
       << std::setw(14) << "Throughput"
       << std::setw(12) << "Ops/s"
       << "\n" << string(96, '-') << "\n";
}

static void print_result(const BenchResult& r) {
  cout << std::left
       << std::setw(36) << r.label
       << std::setw(12) << human_bytes(r.data_size)
       << std::setw(10) << r.iterations
       << std::setw(12) << std::fixed << std::setprecision(4) << r.elapsed_sec
       << std::setw(14) << (std::to_string(static_cast<int>(r.throughput())) + " " + tp_unit)
       << std::setw(12) << std::fixed << std::setprecision(1) << r.ops_per_sec()
       << "\n";
}

int main(int argc, char** argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

  int warmup = 10;
  int iterations = 0;
  size_t fixed_size = 0;
  double target_time = 2.0;
  int num_threads = 1;
  string accel;
  // parse args (not argv) — global_init() already stripped Ceph options
  for (size_t i = 0; i < args.size(); ++i) {
    string arg(args[i]);
    if ((arg == "--size" || arg == "-s") && i + 1 < args.size()) {
      fixed_size = std::stoull(args[++i]);
    } else if (arg == "--iterations" && i + 1 < args.size()) {
      iterations = std::stoi(args[++i]);
    } else if ((arg == "--warmup" || arg == "-w") && i + 1 < args.size()) {
      warmup = std::stoi(args[++i]);
    } else if ((arg == "--time" || arg == "-t") && i + 1 < args.size()) {
      target_time = std::stod(args[++i]);
    } else if (arg == "--accel" && i + 1 < args.size()) {
      accel = args[++i];
    } else if ((arg == "--threads" || arg == "-j") && i + 1 < args.size()) {
      num_threads = std::stoi(args[++i]);
    } else if (arg == "--unit" && i + 1 < args.size()) {
      string u = args[++i];
      if (u == "KB")      { tp_unit = "KB/s"; tp_divisor = 1024.0; }
      else if (u == "MB") { tp_unit = "MB/s"; tp_divisor = 1024.0 * 1024.0; }
      else if (u == "GB") { tp_unit = "GB/s"; tp_divisor = 1024.0 * 1024.0 * 1024.0; }
      else if (u == "TB") { tp_unit = "TB/s"; tp_divisor = 1024.0 * 1024.0 * 1024.0 * 1024.0; }
      else {
        cerr << "Invalid --unit '" << u << "'. Supported: KB, MB, GB, TB\n";
        return 2;
      }
    } else if (arg == "--help" || arg == "-h") {
      cout << "Usage: " << argv[0] << " [options]\n"
           << "  --accel <name>         Crypto plugin (crypto_isal, crypto_openssl, crypto_qat)\n"
           << "  --size, -s <bytes>     Fixed data size (default: run multiple sizes)\n"
           << "  --iterations <N>       Fixed iteration count (default: auto-scale)\n"
           << "  --warmup, -w <N>       Warmup iterations (default: 10)\n"
           << "  --time, -t <sec>       Target time for auto-scaling (default: 2.0)\n"
           << "  --threads, -j <N>      Number of threads (default: 1)\n"
           << "  --unit <UNIT>          Throughput unit: KB, MB, GB, TB (default: MB)\n"
           << "  --help, -h             Show this help\n";
      return 0;
    }
  }

  if (!accel.empty()) {
    if (!is_supported_accel(accel)) {
      cerr << "Invalid --accel '" << accel << "'\n";
      return 2;
    }
    int r = g_ceph_context->_conf.set_val(
        "plugin_crypto_accelerator", accel);
    if (r < 0) {
      cerr << "Failed to set plugin_crypto_accelerator\n";
      return 2;
    }
  }

  g_ceph_context->_conf.apply_changes(nullptr);
  common_init_finish(g_ceph_context);

  string active_accel = g_ceph_context->_conf->plugin_crypto_accelerator;
  if (!is_supported_accel(active_accel)) {
    cerr << "Unsupported plugin_crypto_accelerator='" << active_accel << "'\n";
    return 2;
  }

  {
    auto* reg = g_ceph_context->get_plugin_registry();
    if (!reg->get_with_load("crypto", active_accel)) {
      cerr << "Failed to load crypto plugin '" << active_accel << "'\n";
      return 1;
    }
  }

  static constexpr size_t AES_256_KEYSIZE = 32;
  uint8_t key[AES_256_KEYSIZE];
  fill_random(key, sizeof(key));
  const NoDoutPrefix no_dpp(g_ceph_context, dout_subsys);

  {
    auto cbc = AES_256_CBC_create(&no_dpp, g_ceph_context, key, AES_256_KEYSIZE);
    if (!probe_encrypt(cbc, "CBC probe failed for '" + active_accel +
        "'. Plugin loaded but hardware may not be available."))
      return 1;
  }

  const bool have_gcm = (active_accel != "crypto_qat");
  if (have_gcm) {
    auto gcm = AES_256_GCM_create(&no_dpp, g_ceph_context, key, AES_256_KEYSIZE);
    if (!probe_encrypt(gcm, "GCM probe failed for '" + active_accel + "'."))
      return 1;
  }

  vector<size_t> sizes;
  if (fixed_size > 0) {
    sizes.push_back(fixed_size);
  } else {
    sizes = {4096, 65536, 262144, 1048576, 4194304, 16777216};
  }

  cout << "\n=== RGW BlockCrypt Benchmark ===\n\n"
       << "Accelerator:  " << active_accel << "\n"
       << "Threads:      " << num_threads << "\n"
       << "Modes:        AES-256-CBC";
  if (have_gcm) cout << ", AES-256-GCM";
  else cout << "  (GCM skipped)";
  cout << "\nWarmup:       " << warmup << " iterations\n";
  if (iterations > 0)
    cout << "Iters:        " << iterations << " (fixed, per thread)\n";
  else
    cout << "Target:       ~" << std::fixed << std::setprecision(1)
         << target_time << "s per benchmark\n";
  cout << "\n";

  vector<BenchResult> results;
  string suffix = (num_threads > 1)
      ? " [" + active_accel + " x" + std::to_string(num_threads) + "]"
      : " [" + active_accel + "]";
  auto emit = [&](BenchResult r) {
    print_result(r);
    cout << std::flush;
    results.push_back(std::move(r));
  };
  auto dec_fn = [](string data, size_t len) -> BenchFn {
    return [data, len](BlockCrypt& c) -> bool {
      bufferlist in, out;
      in.append(data);
      return c.decrypt(in, 0, len, out, 0, null_yield);
    };
  };
  print_header();

  for (size_t sz : sizes) {
    buffer::ptr plain_buf(sz);
    fill_random(reinterpret_cast<uint8_t*>(plain_buf.c_str()), sz);
    bufferlist input;
    input.append(plain_buf);
    string input_str = input.to_str();

    auto cbc_factory = [&]() {
      return AES_256_CBC_create(&no_dpp, g_ceph_context, key, AES_256_KEYSIZE);
    };
    auto enc_fn = [input_str, sz](BlockCrypt& c) -> bool {
      bufferlist in, out;
      in.append(input_str);
      return c.encrypt(in, 0, sz, out, 0, null_yield);
    };

    auto bench = [&](const string& lbl, size_t sz, CryptFactory f, BenchFn fn) {
      emit(run_bench(lbl, sz, warmup, target_time, iterations, num_threads,
                     std::move(f), std::move(fn)));
    };

    bench("CBC-Encrypt" + suffix, sz, cbc_factory, enc_fn);

    {
      auto pre = cbc_factory();
      bufferlist ct;
      pre->encrypt(input, 0, sz, ct, 0, null_yield);
      bench("CBC-Decrypt" + suffix, sz, cbc_factory, dec_fn(ct.to_str(), sz));
    }

    if (have_gcm) {
      auto gcm_factory = [&]() {
        return AES_256_GCM_create(&no_dpp, g_ceph_context, key, AES_256_KEYSIZE);
      };

      bench("GCM-Encrypt" + suffix, sz, gcm_factory, enc_fn);

      {
        auto pre = gcm_factory();
        bufferlist ct;
        pre->encrypt(input, 0, sz, ct, 0, null_yield);
        string salt = AES_256_GCM_get_salt(pre.get());
        size_t enc_sz = ct.length();
        auto dec_factory = [&, salt]() {
          return AES_256_GCM_create(&no_dpp, g_ceph_context, key, AES_256_KEYSIZE,
              reinterpret_cast<const uint8_t*>(salt.data()), salt.size());
        };
        bench("GCM-Decrypt" + suffix, sz, dec_factory, dec_fn(ct.to_str(), enc_sz));
      }
    }

    cout << "\n";
  }

  cout << "\n=== Summary (Throughput in " << tp_unit << ") ===\n\n"
       << std::left << std::setw(14) << "DataSize"
       << std::setw(16) << "CBC-Encrypt"
       << std::setw(16) << "CBC-Decrypt";
  if (have_gcm)
    cout << std::setw(16) << "GCM-Encrypt"
         << std::setw(16) << "GCM-Decrypt";
  cout << "\n" << string(46 + (have_gcm ? 32 : 0), '-') << "\n";

  for (size_t sz : sizes) {
    cout << std::setw(14) << human_bytes(sz);
    for (const auto& r : results) {
      if (r.data_size == sz)
        cout << std::setw(16) << std::fixed << std::setprecision(0)
             << r.throughput();
    }
    cout << "\n";
  }

  cout << "\n";
  return 0;
}
