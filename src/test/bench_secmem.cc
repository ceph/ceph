// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 Clyso GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <benchmark/benchmark.h>
#include <keyutils.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cstring>
#include <initializer_list>
#include <random>

#include "common/ceph_argparse.h"
#include "common/random_string.h"
#include "global/global_context.h"
#include "global/global_init.h"

namespace {

constexpr size_t MEMFD_SECRET_SIZE = 5 * 1024 * 1024;

struct SecMem {
  void* mem;
  static SecMem& instance() {
    static SecMem inst;
    return inst;
  }
  SecMem(const SecMem&) = delete;
  void operator=(const SecMem&) = delete;

 private:
  SecMem() {
    int fd = syscall(SYS_memfd_secret, 0);
    ceph_assertf(
        fd >= 0, "memfd_secret: %d errno:%d error:%s", fd, errno,
        strerror(errno));
    int ret = ftruncate(fd, MEMFD_SECRET_SIZE);
    ceph_assertf(
        fd >= 0, "ftruncate: %d errno:%d error:%s", ret, errno,
        strerror(errno));
    mem = ::mmap(
        nullptr, MEMFD_SECRET_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ceph_assertf(
        mem != MAP_FAILED, "mmap: %d errno:%d error:%s", ret, errno,
        strerror(errno));
  }
};

void BM_MemfdSecret_RandomRead(benchmark::State& state) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, (MEMFD_SECRET_SIZE / 128) - 1);
  static auto mem = SecMem::instance().mem;
  if (state.thread_index() == 0) {
    std::memset(mem, '$', MEMFD_SECRET_SIZE);
  }
  for (auto _ : state) {
    const size_t keys_to_read = state.range(0) / state.threads();
    thread_local size_t bytes_read = 0;
    for (int i = 0; i < keys_to_read; ++i) {
      std::string buf(128, '_');
      const size_t offset = dis(gen) * 128;
      std::memcpy(&buf[0], static_cast<char*>(mem) + offset, 128);
      bytes_read += buf.size();
    }
    state.counters["BytesRead"] =
        benchmark::Counter(bytes_read, benchmark::Counter::kIsRate);
    state.counters["KeysRead"] =
        benchmark::Counter(keys_to_read, benchmark::Counter::kIsRate);
    state.counters["KeysReadInv"] = benchmark::Counter(
        keys_to_read,
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
  }
  if (state.thread_index() == 0) {
  }
}

void BM_PlainMem_RandomRead(benchmark::State& state) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, (MEMFD_SECRET_SIZE / 128) - 1);
  static auto mem = ::mmap(
      nullptr, MEMFD_SECRET_SIZE, PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (state.thread_index() == 0) {
    std::memset(mem, '$', MEMFD_SECRET_SIZE);
  }
  for (auto _ : state) {
    const size_t keys_to_read = state.range(0) / state.threads();
    thread_local size_t bytes_read = 0;
    for (int i = 0; i < keys_to_read; ++i) {
      std::string buf(128, '_');
      const size_t offset = dis(gen) * 128;
      std::memcpy(&buf[0], static_cast<char*>(mem) + offset, 128);
      bytes_read += buf.size();
    }
    state.counters["BytesRead"] =
        benchmark::Counter(bytes_read, benchmark::Counter::kIsRate);
    state.counters["KeysRead"] =
        benchmark::Counter(keys_to_read, benchmark::Counter::kIsRate);
    state.counters["KeysReadInv"] = benchmark::Counter(
        keys_to_read,
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
  }
  if (state.thread_index() == 0) {
  }
}

void BM_KeyRing_RandomRead(benchmark::State& state) {
  const std::string secret = gen_rand_alphanumeric(g_ceph_context, 128);
  static std::vector<key_serial_t> serials;
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, state.range(0) - 1);
  if (state.thread_index() == 0) {
    for (size_t i = 0; i < state.range(0); ++i) {
      const key_serial_t serial = ::add_key(
          "user", std::to_string(i).c_str(), secret.c_str(), secret.size(),
          KEY_SPEC_PROCESS_KEYRING);
      ceph_assertf(
          serial != -1, "serial:%d i:%d errno:%d err:%s", serial, i, errno,
          strerror(errno));
      serials.push_back(serial);
    }
  }
  for (auto _ : state) {
    const size_t keys_to_read = state.range(1) / state.threads();
    thread_local size_t bytes_read = 0;
    for (int i = 0; i < keys_to_read; ++i) {
      const key_serial_t serial = serials[dis(gen)];
      std::string out('$', secret.size());
      const auto ret = ::keyctl_read(serial, &out[0], out.size());
      ceph_assert(ret >= 0);
      bytes_read += ret;
    }
    state.counters["BytesRead"] =
        benchmark::Counter(bytes_read, benchmark::Counter::kIsRate);
    state.counters["KeysRead"] =
        benchmark::Counter(keys_to_read, benchmark::Counter::kIsRate);
    state.counters["KeysReadInv"] = benchmark::Counter(
        keys_to_read,
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
  }
  if (state.thread_index() == 0) {
  }
}

void BM_KeyRing_WriteReadRemove(benchmark::State& state) {
  const std::string key_prefix = "benchmark_secret_";
  const std::string secret = "asjdfoilkuj3o8rijsdafo23yo8ifsdjadf";
  if (state.thread_index() == 0) {
  }
  for (auto _ : state) {
    const size_t keys_to_read = state.range(1) / state.threads();
    thread_local size_t bytes_read = 0;
    for (int i = 0; i < keys_to_read; ++i) {
      std::string key(key_prefix);
      key.append(std::to_string(state.thread_index()));
      const key_serial_t serial = ::add_key(
          "user", key.c_str(), secret.c_str(), secret.size(),
          KEY_SPEC_PROCESS_KEYRING);
      ceph_assertf(
          serial != -1, "add_key: serial:%d i:%d errno:%d err:%s", serial, i,
          errno, strerror(errno));
      std::string out("$", secret.size());
      const auto read = ::keyctl_read(serial, &out[0], out.size());
      ceph_assertf(
          read >= 0, "keyctl_read(3): %d errno:%d err:%s", read, errno,
          strerror(errno));
      bytes_read += read;
      ceph_assert(out == secret);

      const auto inval = ::keyctl_invalidate(serial);
      ceph_assertf(inval == 0, "keyctl_invalidate(3): %d", inval);
    }
    state.counters["BytesProcessed"] =
        benchmark::Counter(bytes_read, benchmark::Counter::kIsRate);
    state.counters["KeysProcessed"] =
        benchmark::Counter(keys_to_read, benchmark::Counter::kIsRate);
    state.counters["KeysProcessedInv"] = benchmark::Counter(
        keys_to_read,
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
  }
  if (state.thread_index() == 0) {
  }
}

// note: max number of keys is subject to quota. see /proc/key-users
// increas using /proc/sys/kernel/keys/maxkeys
BENCHMARK(BM_KeyRing_RandomRead)
    ->Args({1000, 1000000})
    ->Threads(1)
    ->Threads(std::thread::hardware_concurrency())
    ->Threads(128);
BENCHMARK(BM_MemfdSecret_RandomRead)
    ->Args({10000000})
    ->Threads(1)
    ->Threads(std::thread::hardware_concurrency())
    ->Threads(128);
BENCHMARK(BM_PlainMem_RandomRead)
    ->Args({10000000})
    ->Threads(1)
    ->Threads(std::thread::hardware_concurrency())
    ->Threads(128);
BENCHMARK(BM_KeyRing_WriteReadRemove)
    ->Args({1000, 1000000})
    ->Threads(1)
    ->Threads(std::thread::hardware_concurrency())
    ->Threads(128);

}  // namespace

int main(int argc, char** argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(
      nullptr, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
      CINIT_FLAG_NO_MON_CONFIG);
  common_init_finish(g_ceph_context);

  char arg0_default[] = "benchmark";
  char* args_default = arg0_default;
  if (argv == nullptr) {
    argc = 1;
    argv = &args_default;
  }
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  return 0;
}
