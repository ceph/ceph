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

// Standalone build:
// g++ --std=c++20 -O2 -o bench ../src/test/bench_secmem.cc -lbenchmark -lkeyutils

#include <benchmark/benchmark.h>
extern "C" {
#include <keyutils.h>
}
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cstring>
#include <format>
#include <initializer_list>
#include <mutex>
#include <random>
#include <thread>

namespace {

constexpr size_t MEMFD_SECRET_SIZE = 5L * 1024 * 1024;
constexpr size_t SECRET_SIZE = 128;

void
BM_MemfdSecret_RandomRead(benchmark::State& state)
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, (MEMFD_SECRET_SIZE / SECRET_SIZE) - 1);
  static std::once_flag mem_initialized;
  static void* mem;
  std::call_once(mem_initialized, [&]() {
    const int fd = static_cast<int>(syscall(SYS_memfd_secret, 0));
    if (fd == -1) {
      state.SkipWithError(
          std::format(
              "memfd_secret: {} errno:{} error:{}", fd, errno,
              std::strerror(errno)));
      return;
    }
    const int ret = ftruncate(fd, MEMFD_SECRET_SIZE);
    if (ret == -1) {
      state.SkipWithError(
          std::format(
              "ftruncate: {} errno:{} error:{}", ret, errno,
              std::strerror(errno)));
      return;
    }
    mem = ::mmap(
        nullptr, MEMFD_SECRET_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mem == MAP_FAILED) {
      state.SkipWithError(
          std::format(
              "mmap: {} errno:{} error:{}", ret, errno, std::strerror(errno)));
      return;
    }
    std::memset(mem, '$', MEMFD_SECRET_SIZE);
  });

  if (state.thread_index() == 0) {
  }

  for (auto _ : state) {
    const size_t keys_to_read = state.range(0) / state.threads();
    thread_local size_t bytes_read = 0;
    for (size_t i = 0; i < keys_to_read; ++i) {
      std::string buf(SECRET_SIZE, '_');
      const size_t offset = dis(gen) * SECRET_SIZE;
      std::memcpy(buf.data(), static_cast<char*>(mem) + offset, SECRET_SIZE);
      bytes_read += buf.size();
    }
    state.counters["BytesRead"] = benchmark::Counter(
        static_cast<double>(bytes_read), benchmark::Counter::kIsRate);
    state.counters["KeysRead"] = benchmark::Counter(
        static_cast<double>(keys_to_read), benchmark::Counter::kIsRate);
    state.counters["KeysReadInv"] = benchmark::Counter(
        static_cast<double>(keys_to_read),
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
  }
  if (state.thread_index() == 0) {
  }
}

void
BM_PlainMem_RandomRead(benchmark::State& state)
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, (MEMFD_SECRET_SIZE / SECRET_SIZE) - 1);
  static auto* mem = ::mmap(
      nullptr, MEMFD_SECRET_SIZE, PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (state.thread_index() == 0) {
    std::memset(mem, '$', MEMFD_SECRET_SIZE);
  }
  for (auto _ : state) {
    const size_t keys_to_read = state.range(0) / state.threads();
    thread_local size_t bytes_read = 0;
    for (size_t i = 0; i < keys_to_read; ++i) {
      std::string buf(SECRET_SIZE, '_');
      const size_t offset = dis(gen) * SECRET_SIZE;
      std::memcpy(buf.data(), static_cast<char*>(mem) + offset, SECRET_SIZE);
      bytes_read += buf.size();
    }
    state.counters["BytesRead"] = benchmark::Counter(
        static_cast<double>(bytes_read), benchmark::Counter::kIsRate);
    state.counters["KeysRead"] = benchmark::Counter(
        static_cast<double>(keys_to_read), benchmark::Counter::kIsRate);
    state.counters["KeysReadInv"] = benchmark::Counter(
        static_cast<double>(keys_to_read),
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
  }
  if (state.thread_index() == 0) {
  }
}

void
BM_KeyRing_RandomRead(benchmark::State& state)
{
  const std::string secret(SECRET_SIZE, '$');
  static std::vector<key_serial_t> serials;
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<size_t> dis(0, state.range(0) - 1);
  if (state.thread_index() == 0) {
    for (size_t i = 0; i < static_cast<size_t>(state.range(0)); ++i) {
      const key_serial_t serial = ::add_key(
          "user", std::to_string(i).c_str(), secret.c_str(), secret.size(),
          KEY_SPEC_PROCESS_KEYRING);
      if (serial == -1) {
        state.SkipWithError(
            std::format(
                "serial:{} i:{} errno:{} err:{}", serial, i, errno,
                std::strerror(errno)));
        return;
      }
      serials.push_back(serial);
    }
  }
  for (auto _ : state) {
    const size_t keys_to_read = state.range(1) / state.threads();
    thread_local size_t bytes_read = 0;
    for (size_t i = 0; i < keys_to_read; ++i) {
      const key_serial_t serial = serials[dis(gen)];
      std::string out(secret.size(), '$');
      const auto ret = ::keyctl_read(serial, out.data(), out.size());
      if (ret < 0) {
        state.SkipWithError("keyctl_read");
        return;
      }
      bytes_read += ret;
    }
    state.counters["BytesRead"] = benchmark::Counter(
        static_cast<double>(bytes_read), benchmark::Counter::kIsRate);
    state.counters["KeysRead"] = benchmark::Counter(
        static_cast<double>(keys_to_read), benchmark::Counter::kIsRate);
    state.counters["KeysReadInv"] = benchmark::Counter(
        static_cast<double>(keys_to_read),
        benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
  }
  if (state.thread_index() == 0) {
  }
}

void
BM_KeyRing_WriteReadRemove(benchmark::State& state)
{
  const std::string key_prefix = "benchmark_secret_";
  const std::string secret = "asjdfoilkuj3o8rijsdafo23yo8ifsdjadf";
  if (state.thread_index() == 0) {
  }
  for (auto _ : state) {
    const size_t keys_to_read = state.range(1) / state.threads();
    thread_local size_t bytes_read = 0;
    for (size_t i = 0; i < keys_to_read; ++i) {
      std::string key(key_prefix);
      key.append(std::to_string(state.thread_index()));
      const key_serial_t serial = ::add_key(
          "user", key.c_str(), secret.c_str(), secret.size(),
          KEY_SPEC_PROCESS_KEYRING);
      if (serial == -1) {
        state.SkipWithError(
            std::format(
                "add_key: serial:{} i:{} errno:{} err:{}", serial, i, errno,
                std::strerror(errno)));
        return;
      }
      std::string out("$", secret.size());
      const auto read = ::keyctl_read(serial, out.data(), out.size());
      if (read < 0) {
        state.SkipWithError(
            std::format(
                "keyctl_read(3): {} errno:{} err:{}", read, errno,
                std::strerror(errno)));
        return;
      }
      bytes_read += read;
      assert(out == secret);

      const auto inval = ::keyctl_invalidate(serial);
      if (inval != 0) {
        state.SkipWithError(std::format("keyctl_invalidate(3): {}", inval));
        return;
      }
    }
    state.counters["BytesProcessed"] = benchmark::Counter(
        static_cast<double>(bytes_read), benchmark::Counter::kIsRate);
    state.counters["KeysProcessed"] = benchmark::Counter(
        static_cast<double>(keys_to_read), benchmark::Counter::kIsRate);
    state.counters["KeysProcessedInv"] = benchmark::Counter(
        static_cast<double>(keys_to_read),
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
    ->Threads(static_cast<int>(std::thread::hardware_concurrency()))
    ->Threads(128);
BENCHMARK(BM_MemfdSecret_RandomRead)
    ->Args({10000000})
    ->Threads(1)
    ->Threads(static_cast<int>(std::thread::hardware_concurrency()))
    ->Threads(128);
BENCHMARK(BM_PlainMem_RandomRead)
    ->Args({10000000})
    ->Threads(1)
    ->Threads(static_cast<int>(std::thread::hardware_concurrency()))
    ->Threads(128);
BENCHMARK(BM_KeyRing_WriteReadRemove)
    ->Args({1000, 1000000})
    ->Threads(1)
    ->Threads(static_cast<int>(std::thread::hardware_concurrency()))
    ->Threads(128);

} // namespace

BENCHMARK_MAIN();
