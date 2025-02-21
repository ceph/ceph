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

#include <atomic>
#include <boost/asio/detached.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <initializer_list>
#include <iterator>
#include <numeric>
#include <random>
#include <thread>

#include "common/ceph_argparse.h"
#include "common/cohort_lru.h"
#include "common/shared_cache.hpp"
#include "common/simple_cache.hpp"
#include "common/web_cache.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/uuid.h"


// Cache implementations in the Ceph codebase:
// ✅ cohort lru
// ✅ shared_cache.hpp SharedLRU
// ✅ simple_cache SimpleLRU
// ✅ web_cache
// Not benchmarked here (reason):
// ❌ LRUSet (not concurrent)
// ❌ intrusive_lru: lru implementation with embedded map and list hook (not concurrent)
// ❌ include/lru.h LRU - (not concurrent)


// Workload Generator Helper
//
// (1) insert unique items > cache size
//   - exercises cache replacement algorithm
//   - with > 1 thread - test concurrency
// (2) Inserts using pareto distributed keys
//   - approximates real world workload

namespace {

// Config

constexpr size_t SMALL_CACHE = 100;
constexpr size_t LARGE_CACHE = 1000;
constexpr size_t CACHE_OP_COUNT = 1000000;
constexpr size_t THREADS_SINGLE = 1;
constexpr size_t THREADS_LOTS = 128;
constexpr size_t RAND_VALUE_LEN = 32;
constexpr size_t PARETO_KEY_POOL_SIZE = 1000;

std::string random_key() {
  uuid_d uuid;
  uuid.generate_random();
  return uuid.to_string();
}

std::string random_value() {
  std::string result(RAND_VALUE_LEN, '0');
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint16_t> dist(0, 0xFF);
  for (size_t i=0; i< RAND_VALUE_LEN; ++i) {
    result[i] = static_cast<char>(dist(gen));
  }
  return result;
}

std::vector<std::string> key_pool(size_t len) {
  std::vector<std::string> result;
  result.reserve(len);
  for (size_t i = 0; i < len; ++i) {
    result.push_back(random_key());
  }
  return result;
}

std::vector<std::string_view> workload(
    const std::vector<std::string>& pool, size_t length) {
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
  CacheAdapter() = default;
  CacheAdapter(const CacheAdapter&) = default;
  CacheAdapter(CacheAdapter&&) = delete;
  CacheAdapter& operator=(const CacheAdapter&) = delete;
  CacheAdapter& operator=(CacheAdapter&&) = delete;
  virtual ~CacheAdapter() = default;

  // Simulate common cache operation: lookup value by key, if it isn't
  // cached add it
  virtual void cache(const std::string& key, const std::string& value) = 0;

  virtual double hit_miss_ratio() { return -23.42; };
  virtual bool reset() { return false; };
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

  bool reset() override {
    _cache.clear();
    hits = 0;
    misses = 0;
    return true;
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

  Object(const Object&) = delete;
  Object(Object&&) = delete;
  Object& operator=(const Object&) = delete;
  Object& operator=(Object&&) = delete;
  ~Object() override = default;

  Object(const std::string& key, const std::string& value) :
    cohort::lru::Object(), m_key(key), m_value(value)
  {}

  bool reclaim(const cohort::lru::ObjectFactory* newobj_fac) override;

};

struct Factory : public cohort::lru::ObjectFactory {
  std::string m_key;
  std::string m_value;

  Factory(const Factory&) = default;
  Factory(Factory&&) = delete;
  Factory& operator=(const Factory&) = default;
  Factory& operator=(Factory&&) = delete;
  ~Factory() override = default;

  Factory(const std::string& key, const std::string& value) :
    cohort::lru::ObjectFactory(), m_key(key), m_value(value)
  {}

  cohort::lru::Object* alloc() override { return new Object(m_key, m_value); }

  void recycle(cohort::lru::Object* o) override {
    auto oo = dynamic_cast<Object*>(o);
    oo->m_key = m_key;
    oo->m_value = m_value;
  }
};

bool Object::reclaim(const cohort::lru::ObjectFactory* newobj_fac) {
  const auto* factory = dynamic_cast<const Factory*>(newobj_fac);
  return (factory != nullptr);
}

}  // namespace cohortlru

struct CohortLRUAdapter : public CacheAdapter {
  cohort::lru::LRU<std::mutex> _cache;

  explicit CohortLRUAdapter(size_t size)
      : _cache(
            static_cast<int>(size / std::thread::hardware_concurrency()),
            size / std::thread::hardware_concurrency()) {}

  void cache(const std::string& key, const std::string& value) override {
    cohortlru::Factory prototype(key, value);
    uint32_t iflags{cohort::lru::FLAG_INITIAL};
    auto o = dynamic_cast<cohortlru::Object*>(
        _cache.insert(&prototype, cohort::lru::Edge::MRU, iflags));
    ceph_assert(o != nullptr);
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

  WebCacheAdapter(const WebCacheAdapter&) = delete;
  WebCacheAdapter(WebCacheAdapter&&) = delete;
  WebCacheAdapter& operator=(const WebCacheAdapter&) = delete;
  WebCacheAdapter& operator=(WebCacheAdapter&&) = delete;

  explicit WebCacheAdapter(size_t size) :
    _cache(g_ceph_context, "benchmark", size)
  {}
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

  bool reset() override {
    _cache.clear();
    return true;
  }
};

struct WebCacheLookupOrAdapter : public CacheAdapter {
  struct CacheValue {
    std::once_flag once;
    std::string value;
  };
  using Cache = webcache::WebCache<std::string, CacheValue>;
  Cache _cache;

  WebCacheLookupOrAdapter(const WebCacheLookupOrAdapter&) = delete;
  WebCacheLookupOrAdapter(WebCacheLookupOrAdapter&&) = delete;
  WebCacheLookupOrAdapter& operator=(const WebCacheLookupOrAdapter&) = delete;
  WebCacheLookupOrAdapter& operator=(WebCacheLookupOrAdapter&&) = delete;

  explicit WebCacheLookupOrAdapter(size_t size) :
    _cache(g_ceph_context, "benchmark", size)
  {}

  ~WebCacheLookupOrAdapter() override {
    _cache.perf()->reset();
  }

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
  bool reset() override {
    _cache.clear();
    return true;
  }
};

/// }}}

// Benchmarks {{{

template <typename C>
class CacheFixture : public benchmark::Fixture {
 public:
  std::unique_ptr<C> cache;
  void SetUp(::benchmark::State& state) override {
    if (state.thread_index() == 0) {
      cache = std::make_unique<C>(state.range(0));
    }
  }

  void TearDown(::benchmark::State& state) override {
    if (state.thread_index() == 0) {
      state.counters["hit/miss"] = cache->hit_miss_ratio();
    }
  }
};

template <typename C>
class ParetoFixture : public CacheFixture<C> {
 public:
  std::vector<std::string> pool;
  std::array<std::vector<std::string_view>, THREADS_LOTS> pareto_keys;

  ParetoFixture() : pool(key_pool(PARETO_KEY_POOL_SIZE)) {}

  void SetUp(::benchmark::State& state) override {
    if (state.thread_index() == 0) {
      this->cache = std::make_unique<C>(state.range(0));
      state.counters["Key Pool"] = pool.size();
    }
    pareto_keys[state.thread_index()] =
        workload(pool, state.range(1) / state.threads());
    state.counters["Keys/Thread"] = benchmark::Counter(
        pareto_keys[state.thread_index()].size(),
        benchmark::Counter::kAvgThreads);
    ceph_assert(pareto_keys[state.thread_index()].size() > 1000);
  }
};

BENCHMARK_TEMPLATE_METHOD_F(CacheFixture, BM_UniqueAdd)(
    benchmark::State& state) {
  for (auto _ : state) {
    const size_t ops = state.range(1) / state.threads();
    for (size_t i = 0; i < ops; ++i) {
      this->cache->cache(random_key(), random_value());
    }
    state.counters["KeysProcessed"] =
        benchmark::Counter(ops, benchmark::Counter::kIsRate);
    state.counters["KeysProcessedInv"] = benchmark::Counter(
        ops, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
  }
}

BENCHMARK_TEMPLATE_METHOD_F(ParetoFixture, BM_Pareto)(benchmark::State& state) {
  for (auto _ : state) {
    const auto keys = this->pareto_keys[state.thread_index()];
    for (auto key : keys) {
      this->cache->cache(std::string(key), "some_value");
    }
    state.counters["KeysProcessed"] =
        benchmark::Counter(keys.size(), benchmark::Counter::kIsRate);
    state.counters["KeysProcessedInv"] = benchmark::Counter(
        keys.size(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
  }
}

// }}}

// Benchmark Run Configuration {{{

void DefaultArgs(benchmark::internal::Benchmark* bench) {
  bench->Args({SMALL_CACHE, CACHE_OP_COUNT})
      ->Args({LARGE_CACHE, CACHE_OP_COUNT})
      ->Threads(THREADS_SINGLE)
      ->Threads(static_cast<int>(std::thread::hardware_concurrency()))
      ->Threads(THREADS_LOTS);
}

BENCHMARK_TEMPLATE_INSTANTIATE_F(CacheFixture, BM_UniqueAdd, SharedLRUAdapter)			->Name("UNIQUE shared")->Apply(DefaultArgs);
BENCHMARK_TEMPLATE_INSTANTIATE_F(CacheFixture, BM_UniqueAdd, SimpleLRUAdapter)			->Name("UNIQUE simple")->Apply(DefaultArgs);
BENCHMARK_TEMPLATE_INSTANTIATE_F(CacheFixture, BM_UniqueAdd, CohortLRUAdapter)			->Name("UNIQUE cohort")->Apply(DefaultArgs);
BENCHMARK_TEMPLATE_INSTANTIATE_F(CacheFixture, BM_UniqueAdd, WebCacheAdapter)			->Name("UNIQUE web   ")->Apply(DefaultArgs);
BENCHMARK_TEMPLATE_INSTANTIATE_F(CacheFixture, BM_UniqueAdd, WebCacheLookupOrAdapter)		->Name("UNIQUE web-O ")->Apply(DefaultArgs);
BENCHMARK_TEMPLATE_INSTANTIATE_F(ParetoFixture, BM_Pareto, SharedLRUAdapter)			->Name("PARETO shared")->Apply(DefaultArgs);
BENCHMARK_TEMPLATE_INSTANTIATE_F(ParetoFixture, BM_Pareto, SimpleLRUAdapter)			->Name("PARETO simple")->Apply(DefaultArgs);
BENCHMARK_TEMPLATE_INSTANTIATE_F(ParetoFixture, BM_Pareto, CohortLRUAdapter)			->Name("PARETO cohort")->Apply(DefaultArgs);
BENCHMARK_TEMPLATE_INSTANTIATE_F(ParetoFixture, BM_Pareto, WebCacheAdapter)			->Name("PARETO web   ")->Apply(DefaultArgs);
BENCHMARK_TEMPLATE_INSTANTIATE_F(ParetoFixture, BM_Pareto, WebCacheLookupOrAdapter)		->Name("PARETO web-O ")->Apply(DefaultArgs);

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
  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  return 0;
}
