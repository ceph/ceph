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
#include <uuid/uuid.h>
#include <xxhash.h>

#include <algorithm>
#include <atomic>
#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <initializer_list>
#include <iterator>
#include <numeric>
#include <random>
#include <thread>
#include <utility>

#include "common/async/call_once.h"
#include "common/ceph_argparse.h"
#include "common/cohort_lru.h"
#include "common/random_string.h"
#include "common/shared_cache.hpp"
#include "common/simple_cache.hpp"
#include "common/web_cache.h"
#include "global/global_context.h"
#include "global/global_init.h"

// Cache implementations in the Ceph codebase:
// ✅ cohort lru
// ✅ shared_cache.hpp SharedLRU
// ✅ simple_cache SimpleLRU
// ✅ web_cache
// Not benchmarked here (reason):
// ❌ LRUSet (not concurrent)
// ❌ intrusive_lru: lru implementation with embedded map and list hook (not concurrent)
// ❌ include/lru.h LRU - (not concurrent)

// Config

constexpr size_t SMALL_CACHE = 100;
constexpr size_t LARGE_CACHE = 1000;
constexpr size_t CACHE_OP_COUNT = 100000;
constexpr size_t THREADS_SINGLE = 1;
constexpr size_t THREADS_LOTS = 128;
constexpr size_t RAND_KEY_LEN = 16;
constexpr size_t RAND_VALUE_LEN = 32;

// Workload Generator Helper
//
// (1) insert unique items > cache size
//   - exercises cache replacement algorithm
//   - with > 1 thread - test concurrency
// (2) Inserts using pareto distributed keys
//   - approximates real world workload

namespace {

std::string random_key() {
  return gen_rand_alphanumeric_plain(g_ceph_context, RAND_KEY_LEN);
}

std::string random_value() {
  return gen_rand_alphanumeric(g_ceph_context, RAND_VALUE_LEN);
}

std::vector<std::string> key_pool(int len) {
  std::vector<std::string> result;
  result.reserve(len);
  for (size_t i = 0; i < len; ++i) {
    result.push_back(random_key());
  }
  return result;
}

std::vector<std::string_view> workload(
    const std::vector<std::string>& pool, int length) {
  const double alpha = 1.5;
  std::vector<double> weights(pool.size());
  for (size_t i = 0; i < pool.size(); ++i) {
    weights[i] = std::pow(static_cast<double>(i + 1), -alpha);
  }
  const double weights_sum =
      std::accumulate(weights.begin(), weights.end(), 0.0);
  for (auto& weight : weights) {
    weight /= weights_sum;
  }
  std::vector<double> partial_sums(weights.size());
  std::partial_sum(weights.begin(), weights.end(), partial_sums.begin());

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<double> dis(0.0, 1.0);

  std::vector<std::string_view> result;
  result.reserve(length);

  for (size_t i = 0; i < length; ++i) {
    double u = dis(gen);
    auto it = std::ranges::lower_bound(partial_sums, u);
    size_t idx = std::distance(partial_sums.begin(), it);
    result.push_back(pool[idx]);
  }
  return result;
}

//
// Cache Adapter (cache impl <-> benchmark)
//

struct CacheAdapter {
  virtual ~CacheAdapter() = default;

  // Simulate common cache operation: lookup value by key, if it isn't
  // cached add it
  virtual void cache(const std::string& key, const std::string& value) = 0;

  virtual double hit_miss_ratio() { return -23.42; };
};

// Shared LRU {{{
struct SharedLRUAdapter : public CacheAdapter {
  SharedLRU<std::string, std::string> _cache;

  std::atomic_int hits;
  std::atomic_int misses;
  explicit SharedLRUAdapter(size_t size) : _cache(g_ceph_context, size) {}
  void cache(const std::string& key, const std::string& value) override {
    bool existed = false;
    auto* copy = new std::string(value);
    const auto ptr = _cache.add(key, copy, &existed);
    if (existed) {
      hits++;
      delete copy;
    } else {
      misses++;
    }
  }

  double hit_miss_ratio() override {
    return static_cast<double>(hits) /
           (static_cast<double>(hits) + static_cast<double>(misses));
  }
};

// }}}

// Simple LRU {{{
struct SimpleLRUAdapter : public CacheAdapter {
  SimpleLRU<std::string, std::string> _cache;

  explicit SimpleLRUAdapter(size_t size) : _cache(size) {}
  void cache(const std::string& key, const std::string& value) override {
    std::string out;
    if (!_cache.lookup(key, &out)) {
      _cache.add(key, value);
    }
  }
};

// }}}

// Cohort LRU {{{
namespace cohortlru {
class Factory;

struct Object : public cohort::lru::Object {
  std::string m_key;
  std::string m_value;

  Object(const std::string& key, const std::string& value)
      : cohort::lru::Object(), m_key(key), m_value(value) {}

  bool reclaim(const cohort::lru::ObjectFactory* newobj_fac) override;

  ~Object() override {}
};

struct Factory : public cohort::lru::ObjectFactory {
  std::string m_key;
  std::string m_value;

  Factory(const std::string& key, const std::string& value)
      : cohort::lru::ObjectFactory(), m_key(key), m_value(value) {}
  ~Factory() = default;

  cohort::lru::Object* alloc() override { return new Object(m_key, m_value); }

  void recycle(cohort::lru::Object* o) override {
    auto oo = dynamic_cast<Object*>(o);
    oo->m_key = m_key;
    oo->m_value = m_value;
  }
};

bool Object::reclaim(const cohort::lru::ObjectFactory* newobj_fac) {
  auto factory = dynamic_cast<const Factory*>(newobj_fac);
  if (factory == nullptr) {
    return false;
  }
  return true;
}

}  // namespace cohortlru

struct CohortLRUAdapter : public CacheAdapter {
  cohort::lru::LRU<std::mutex> _cache;

  explicit CohortLRUAdapter(size_t size)
      : _cache(
            size / std::thread::hardware_concurrency(),
            size / std::thread::hardware_concurrency()) {}

  void cache(const std::string& key, const std::string& value) override {
    cohortlru::Factory prototype(key, value);
    uint32_t iflags{cohort::lru::FLAG_INITIAL};
    auto o = static_cast<cohortlru::Object*>(
        _cache.insert(&prototype, cohort::lru::Edge::MRU, iflags));
    ceph_assert(o->m_key == key);
    ceph_assert(o->m_value == value);
  }
};

// }}}

// Web Cache {{{

struct WebCacheAdapter : public CacheAdapter {
  using CacheValue = std::string;
  using Cache = webcache::WebCache<std::string, CacheValue>;
  Cache _cache;

  explicit WebCacheAdapter(size_t size)
      : _cache(g_ceph_context, "benchmark", size) {}
  void cache(const std::string& key, const std::string& value) override {
    if (!_cache.lookup(key).has_value()) {
      _cache.add(key, std::make_shared<std::string>(value));
    }
  }

  ~WebCacheAdapter() override { _cache.perf()->reset(); }

  double hit_miss_ratio() override {
    return static_cast<double>(
               _cache.perf()->get(static_cast<int>(webcache::Metric::hit))) /
           (static_cast<double>(
                _cache.perf()->get(static_cast<int>(webcache::Metric::hit))) +
            static_cast<double>(
                _cache.perf()->get(static_cast<int>(webcache::Metric::miss))));
  }
};

struct WebCacheLookupOrAdapter : public CacheAdapter {
  struct CacheValue {
    std::once_flag once;
    std::string value;
  };
  using Cache = webcache::WebCache<std::string, CacheValue>;
  Cache _cache;

  explicit WebCacheLookupOrAdapter(size_t size)
      : _cache(g_ceph_context, "benchmark", size) {}

  ~WebCacheLookupOrAdapter() override { _cache.perf()->reset(); }

  void cache(const std::string& key, const std::string& value) override {
    std::shared_ptr<CacheValue> cache_value =
        _cache.lookup_or(key, std::make_shared<CacheValue>());

    std::call_once(cache_value->once, [&]() { cache_value->value = value; });
  }
  double hit_miss_ratio() override {
    return static_cast<double>(
               _cache.perf()->get(static_cast<int>(webcache::Metric::hit))) /
           (static_cast<double>(
                _cache.perf()->get(static_cast<int>(webcache::Metric::hit))) +
            static_cast<double>(
                _cache.perf()->get(static_cast<int>(webcache::Metric::miss))));
  }
};

struct WebCacheLookupOrFutureAdapter : public CacheAdapter {
  struct CacheValue {
    std::promise<std::string> promise;
    std::shared_future<std::string> future;
    CacheValue(const std::string& value)
        : promise(), future(promise.get_future()) {
      promise.set_value(value);
    }
  };
  using Cache = webcache::WebCache<std::string, CacheValue>;
  Cache _cache;

  explicit WebCacheLookupOrFutureAdapter(size_t size)
      : _cache(g_ceph_context, "bechmark", size) {}

  ~WebCacheLookupOrFutureAdapter() override { _cache.perf()->reset(); }

  void cache(const std::string& key, const std::string& value) override {
    std::shared_ptr<CacheValue> cache_value =
        _cache.lookup_or(key, std::make_shared<CacheValue>(value));
    cache_value->future.wait();
  }
  double hit_miss_ratio() override {
    return static_cast<double>(
               _cache.perf()->get(static_cast<int>(webcache::Metric::hit))) /
           (static_cast<double>(
                _cache.perf()->get(static_cast<int>(webcache::Metric::hit))) +
            static_cast<double>(
                _cache.perf()->get(static_cast<int>(webcache::Metric::miss))));
  }
};

// Run io context event loops in a thread per hardware concurrency.
// cache() by spawn'ing coroutine doing lookup_or and retrieving
// result using ceph::async::call_once()
struct WebCacheLookupOrAsyncAdapter : public CacheAdapter {
  using CacheResult = tl::expected<std::string, int>;
  using CacheValue = ceph::async::once_result<CacheResult>;
  using Cache = webcache::WebCache<std::string, CacheValue>;

  Cache _cache;

  boost::asio::io_context _context;
  std::vector<std::jthread> _threads;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type>
      _guard;

  explicit WebCacheLookupOrAsyncAdapter(size_t size)
      : _cache(g_ceph_context, "bechmark", size),
        _context(),
        _guard(boost::asio::make_work_guard(_context)) {
    _threads.reserve(std::thread::hardware_concurrency());
    for (int i = 0; i < std::thread::hardware_concurrency(); ++i) {
      _threads.emplace_back([&]() { _context.run(); });
    }
  }

  ~WebCacheLookupOrAsyncAdapter() override {
    _guard.reset();
    _cache.perf()->reset();
  }

  void cache(const std::string& key, const std::string& value) override {
    boost::asio::spawn(
        _context,
        [this, key, value](boost::asio::yield_context yield) {
          std::shared_ptr<CacheValue> cache_value =
              _cache.lookup_or(key, std::make_shared<CacheValue>());
          auto result = call_once(
              *cache_value, yield, [&]() -> CacheResult { return value; });
        },
        boost::asio::detached);
  }
  double hit_miss_ratio() override {
    return static_cast<double>(
               _cache.perf()->get(static_cast<int>(webcache::Metric::hit))) /
           (static_cast<double>(
                _cache.perf()->get(static_cast<int>(webcache::Metric::hit))) +
            static_cast<double>(
                _cache.perf()->get(static_cast<int>(webcache::Metric::miss))));
  }
};

/// }}}

// Benchmarks {{{

template <class C>
void BM_UniqueAdd(benchmark::State& state) {
  static C* cache = nullptr;
  if (state.thread_index() == 0) {
    cache = new C(state.range(0));
  }
  for (auto _ : state) {
    for (int i = 0; i < state.range(1) / state.threads(); ++i) {
      cache->cache(random_key(), random_value());
    }
  }
  if (state.thread_index() == 0) {
    state.counters["hit/miss"] = cache->hit_miss_ratio();
    delete cache;
  }
}

template <class C>
void BM_Pareto(benchmark::State& state) {
  static C* cache = nullptr;
  static const auto pool = key_pool(1000);

  if (state.thread_index() == 0) {
    state.counters["key_pool_size"] = pool.size();
    cache = new C(state.range(0));
  }
  for (auto _ : state) {
    state.PauseTiming();
    const auto keys = workload(pool, state.range(1) / state.threads());
    ceph_assert(keys.size() > 10);
    state.counters["keys"] = keys.size();
    state.ResumeTiming();
    for (auto key : keys) {
      cache->cache(std::string(key), "some_value");
    }
  }
  if (state.thread_index() == 0) {
    state.counters["hit/miss"] = cache->hit_miss_ratio();
    delete cache;
  }
}

// }}}

// Benchmark Run Configuration {{{

void register_benchmarks() {
  for (const auto& [name, test] : {
           std::make_pair("UNIQUE shared", BM_UniqueAdd<SharedLRUAdapter>),
           std::make_pair("UNIQUE simple", BM_UniqueAdd<SimpleLRUAdapter>),
           std::make_pair("UNIQUE cohort", BM_UniqueAdd<CohortLRUAdapter>),
           std::make_pair("UNIQUE web   ", BM_UniqueAdd<WebCacheAdapter>),
           std::make_pair("UNIQUE web-O", BM_UniqueAdd<WebCacheLookupOrAdapter>),
           std::make_pair("UNIQUE web-F ", BM_UniqueAdd<WebCacheLookupOrFutureAdapter>),
           std::make_pair("UNIQUE web-A ", BM_UniqueAdd<WebCacheLookupOrAsyncAdapter>),
           std::make_pair("PARETO shared", BM_Pareto<SharedLRUAdapter>),
           std::make_pair("PARETO simple", BM_Pareto<SimpleLRUAdapter>),
           std::make_pair("PARETO cohort", BM_Pareto<CohortLRUAdapter>),
           std::make_pair("PARETO web   ", BM_Pareto<WebCacheAdapter>),
           std::make_pair("PARETO web-O ", BM_Pareto<WebCacheLookupOrAdapter>),
           std::make_pair("PARETO web-F ", BM_Pareto<WebCacheLookupOrFutureAdapter>),
           std::make_pair("PARETO web-A ", BM_Pareto<WebCacheLookupOrAsyncAdapter>),
       }) {
    auto* bench = benchmark::RegisterBenchmark(name, test);
    bench->Args({SMALL_CACHE, CACHE_OP_COUNT})
        ->Args({LARGE_CACHE, CACHE_OP_COUNT})
        ->Threads(THREADS_SINGLE)
        ->Threads(std::thread::hardware_concurrency())
        ->Threads(THREADS_LOTS);
  }
}
// }}}

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
  register_benchmarks();
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  return 0;
}
