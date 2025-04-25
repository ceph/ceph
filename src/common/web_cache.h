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

#pragma once

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <atomic>
#include <boost/asio/spawn.hpp>
#include <boost/asio/ts/netfwd.hpp>
#include <chrono>
#include <functional>
#include <future>
#include <iterator>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <shared_mutex>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "common/ceph_context.h"
#include "common/ceph_time.h"
#include "include/ceph_assert.h"
#include "include/common_fwd.h"
#include "include/expected.hpp"

// A cache for data living on other systems like Key Management Systems
// Goals/Features
// (1) Thread safe, bias towards handling highly concurrent lookups
// (2) Expire entries by ttl
// (3) Cache replacement tuned to "web" workloads

// Cache Stampedes
// TODO(irq0) document problem and call_once or probabilistic solution

// Algorithm
//
// The implementation is based on SIEVE [0] with an additional TTL
// reaper that leverages SIEVE's FIFO insertion order
//
// Data Structures
// - SIEVE FIFO
// - key lookup table: key -> value ptr
//
// [0] Zhang, Yazhuo, et al. "{SIEVE} is Simpler than {LRU}: an Efficient
// {Turn-Key} Eviction Algorithm for Web Caches." 21st USENIX
// Symposium on Networked Systems Design and Implementation (NSDI 24).
// 2024.

class WebCacheTest;

namespace webcache {

enum class Metric {
  metrics_start = 84000,
  fetch_lat,
  fetch_error,
  hit,
  miss,
  expired,
  fetch_wait,
  fetch_get,
  size,
  capacity,
  clear,
  metrics_stop
};

template <typename Key, typename Value>
class WebCache {
 public:
  // TODO(irq0) let the user choose the value pointer type
  using ValuePtr = std::shared_ptr<Value>;

 protected:
  struct Node {
    std::atomic_bool visited;
    ceph::real_time expires_at;
    Key const* key;
    ValuePtr value;

    explicit Node(ValuePtr value)
        : visited(false),
          expires_at(ceph::real_clock::now()),
          key(nullptr),
          value(value) {};
    // for testing
    Node(ValuePtr value, ceph::real_time expires_at)
        : visited(false), expires_at(expires_at), key(nullptr), value(value) {};

    Node(Node&&) = default;
    Node& operator=(Node&&) = default;
    Node(const Node&) = delete;
    Node& operator=(const Node&) = delete;
    ~Node() = default;

    friend std::ostream& operator<<(
        std::ostream& os, const WebCache<Key, Value>::Node& node) {
      fmt::print(
          os, "$n[{}->{} v:{} expires:{}]", fmt::ptr(node.key),
          fmt::ptr(node.value.get()), node.visited.load(), node.expires_at);
      return os;
    }
  };

 private:
  CephContext* _cct;
  std::string _name;
  PerfCounters* _perf;
  size_t _capacity;
  std::list<Node*> _sieve_queue;
  size_t _sieve_hand;
  std::unordered_map<Key, Node> _lookup;
  mutable std::shared_mutex _cache_mutex;

 protected:
  // sieve_evict removes the next node using the SIEVE algorithm from
  // _sieve_queue and returns a pointer to the evicted node
  Node* sieve_evict();

  // sieve_expire_erase_unmutexed removes all expired nodes from the
  // sieve_queue in place. It writes the expired nodes to out_expired
  static void sieve_expire_erase_unmutexed(
      std::list<Node*>& sieve_queue, ceph::real_time eviction_cutoff,
      std::vector<Node*>& out_expired);

  static PerfCounters* initialize_perf_counters(
      CephContext* cct, const std::string& name);

  std::optional<ValuePtr> lookup_unmutexed(const Key& key);
  ceph::real_time insert_unmutexed(const Key& key, ValuePtr value);

  void perf_tinc(Metric metric, ceph::timespan elapsed) {
    if (_perf != nullptr) {
      _perf->tinc(static_cast<int>(metric), elapsed);
    }
  }
  void perf_inc(Metric metric, uint64_t inc = 1) {
    if (_perf != nullptr) {
      _perf->inc(static_cast<int>(metric), inc);
    }
  }
  void perf_set(Metric metric, uint64_t set = 1) {
    if (_perf != nullptr) {
      _perf->set(static_cast<int>(metric), set);
    }
  }

 public:
  // For testing, custom use
  explicit WebCache(size_t capacity);

  // For system use, activates perf counters
  WebCache(CephContext* cct, const std::string& name, size_t capacity);

  // lookup returns the stored value for a given key or not
  std::optional<ValuePtr> lookup(const Key& key);

  // add caches a key/value pair. If key already exists it does
  // nothing. Return the stored timestamp of the k/v mapping
  ceph::real_time add(const Key& key, ValuePtr val);

  // lookup_or returns a value for key. If none is cached yet, insert
  // new_val and return that. A common use is with
  // ceph::async::call_once to add cache stampede mitigration
  ValuePtr lookup_or(const Key& key, ValuePtr new_val);

  // update_expiration_if updates an entries TTL if its value matches val.
  // Return true if we updated an entry. False otherwise.
  bool update_ttl_if(
      const Key& key, const ValuePtr& val_ptr, ceph::timespan new_ttl);

  size_t size() const;
  size_t clear();

  // expire_erase erases all expired cache entries
  size_t expire_erase(std::chrono::seconds ttl);

  const std::string& name() { return _name; }

  PerfCounters* perf() { return _perf; }

  friend std::ostream& operator<<(
      std::ostream& os, const WebCache<Key, Value>& cache) {
    std::shared_lock<std::shared_mutex> lock(cache._cache_mutex);
    const auto now = ceph::real_clock::now();
    os << "$" << cache._name << "[";
    for (size_t i = 0; const auto& node : cache._sieve_queue) {
      const auto ttl = node->expires_at - now;
      fmt::print(
          os, "\"{}\"({}){}{}", *(node->key),
          std::chrono::duration_cast<std::chrono::seconds>(ttl),
          (i == cache._sieve_hand) ? "👉" : "", node->visited ? "▮" : "▯");
      ++i;
      if (i < cache._sieve_queue.size()) {
        os << ", ";
      }
    }
    os << "]";
    return os;
  }

  friend class WebCacheTest;
  friend class WebCacheConcurrencyTest;
  friend class WebCacheRandomizedTest;
  friend class WebCacheTest_SieveExample_Test;
  friend class WebCacheTest_ExpireEraseOne_Test;
  friend class WebCacheTest_ExpireEraseAll_Test;
  friend class WebCacheTest_ExpireEraseEmpty_Test;
};

template <typename Key, typename Value>
std::jthread make_ttl_reaper(
    WebCache<Key, Value>& cache, std::chrono::seconds ttl) {
  return std::jthread([&cache, ttl](std::stop_token stop) {
    const std::string thread_name = fmt::format("{}-ttl-reaper", cache.name());
    ceph_pthread_setname(thread_name.c_str());
    std::mutex mutex;
    std::condition_variable cond;

    std::stop_callback on_stop(stop, [&cond]() { cond.notify_all(); });

    while (!stop.stop_requested()) {
      cache.expire_erase(ttl);

      std::unique_lock<std::mutex> lock(mutex);
      cond.wait_for(lock, ttl, [&stop] { return stop.stop_requested(); });
    }
  });
}

template <typename Key, typename Value>
WebCache<Key, Value>::Node* WebCache<Key, Value>::sieve_evict() {
  size_t hand = _sieve_hand;
  Node* result = nullptr;
  const size_t queue_size_before = _sieve_queue.size();
  const auto it_from_hand = std::next(_sieve_queue.begin(), hand + 1);
  const auto rev_it = std::make_reverse_iterator(it_from_hand);
  for (auto it = rev_it; it != _sieve_queue.rend(); ++it) {
    Node* node = (*it);
    if (node->visited) {
      node->visited = false;
      --hand;
    } else {
      _sieve_queue.erase(std::next(it).base());
      result = node;
      break;
    }
  }
  // every node was visited. we need still need to evict the tail
  if (result == nullptr) {
    result = _sieve_queue.back();
    _sieve_queue.pop_back();
  }

  ceph_assertf(
      queue_size_before == 0 || (queue_size_before - _sieve_queue.size()) == 1,
      "%d -> %d capacity:%d", queue_size_before, _sieve_queue.size(),
      _capacity);
  _sieve_hand = (hand == 0) ? _capacity : hand;
  return result;
}

template <typename Key, typename Value>
WebCache<Key, Value>::WebCache(size_t capacity)
    : _cct(nullptr),
      _perf(nullptr),
      _capacity(capacity),
      _sieve_queue(),
      _sieve_hand(capacity - 1),
      _lookup() {}

template <typename Key, typename Value>
WebCache<Key, Value>::WebCache(
    CephContext* cct, const std::string& name, size_t capacity)
    : _cct(cct),
      _name(name),
      _perf(initialize_perf_counters(cct, name)),
      _capacity(capacity),
      _sieve_queue(),
      _sieve_hand(capacity - 1),
      _lookup() {
  ceph_assert(cct != nullptr);
  perf_set(Metric::capacity, capacity);
}

template <typename Key, typename Value>
PerfCounters* WebCache<Key, Value>::initialize_perf_counters(
    CephContext* cct, const std::string& name) {
  PerfCountersBuilder pcb(
      cct, name, static_cast<int>(Metric::metrics_start),
      static_cast<int>(Metric::metrics_stop));
  pcb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);
  pcb.add_time_avg(
      static_cast<int>(Metric::fetch_lat), "fetch_lat", "Fetch latency");
  pcb.add_u64_counter(
      static_cast<int>(Metric::fetch_error), "fetch_error",
      "Total number of fetch errors");
  pcb.add_u64_counter(static_cast<int>(Metric::hit), "hit", "Cache hits");
  pcb.add_u64_counter(static_cast<int>(Metric::miss), "miss", "Cache misses");
  pcb.add_u64_counter(
      static_cast<int>(Metric::expired), "expired", "Expired cache entries");
  pcb.add_u64_counter(
      static_cast<int>(Metric::fetch_wait), "fetch_wait",
      "Total number of misses that waited for a fetch get");
  pcb.add_u64_counter(
      static_cast<int>(Metric::fetch_get), "fetch_get",
      "Total number of misses that actually fetched");
  pcb.add_u64(
      static_cast<int>(Metric::size), "size", "Total number of cache entries");
  pcb.add_u64(
      static_cast<int>(Metric::capacity), "capacity", "Maximum cache capacity");
  pcb.add_u64_counter(
      static_cast<int>(Metric::clear), "clear", "Total number of cache clears");

  auto* perf = pcb.create_perf_counters();
  cct->get_perfcounters_collection()->add(perf);
  return perf;
}

template <typename Key, typename Value>
size_t WebCache<Key, Value>::size() const {
  std::shared_lock<std::shared_mutex> lock(_cache_mutex);
  ceph_assert(_lookup.size() == _sieve_queue.size());
  return _lookup.size();
}

template <typename Key, typename Value>
size_t WebCache<Key, Value>::clear() {
  std::lock_guard<std::shared_mutex> lock(_cache_mutex);
  perf_inc(Metric::clear);
  const size_t size_before = _sieve_queue.size();
  _sieve_queue.clear();
  _lookup.clear();
  return size_before;
}

template <typename Key, typename Value>
ceph::real_time WebCache<Key, Value>::add(const Key& key, ValuePtr value) {
  // cache hit - fast under read lock
  {
    std::shared_lock<std::shared_mutex> lock(_cache_mutex);
    if (auto search = _lookup.find(key); search != _lookup.end()) {
      perf_inc(Metric::hit);
      search->second.visited = true;
      return search->second.expires_at;
    }
  }
  {
    std::lock_guard<std::shared_mutex> lock(_cache_mutex);
    if (auto search = _lookup.find(key); search != _lookup.end()) {
      // cache hit - under write lock
      perf_inc(Metric::hit);
      search->second.visited = true;
      return search->second.expires_at;
    }

    // cache miss
    perf_inc(Metric::miss);
    return insert_unmutexed(key, value);
  }
}

template <typename Key, typename Value>
WebCache<Key, Value>::ValuePtr WebCache<Key, Value>::lookup_or(
    const Key& key, ValuePtr new_val) {
  {
    std::shared_lock<std::shared_mutex> cache_lock(_cache_mutex);
    auto maybe_value = lookup_unmutexed(key);
    if (maybe_value.has_value()) {
      perf_inc(Metric::hit);
      return maybe_value.value();
    }
  }

  // miss, take unique lock
  // check again or insert new val
  {
    std::lock_guard<std::shared_mutex> lock(_cache_mutex);
    auto maybe_value = lookup_unmutexed(key);
    if (maybe_value.has_value()) {
      perf_inc(Metric::hit);
      return maybe_value.value();
    }

    perf_inc(Metric::miss);
    insert_unmutexed(key, new_val);
    return new_val;
  }
}

template <typename Key, typename Value>
bool WebCache<Key, Value>::update_ttl_if(
    const Key& key, const ValuePtr& val, ceph::timespan new_ttl) {
  std::lock_guard<std::shared_mutex> lock(_cache_mutex);
  if (auto search = _lookup.find(key);
      (search != _lookup.end() && search->second.value == val)) {
    search->second.expires_at = ceph::real_clock::now() + new_ttl;
    return true;
  }
  return false;
}

template <typename Key, typename Value>
std::optional<typename WebCache<Key, Value>::ValuePtr>
WebCache<Key, Value>::lookup_unmutexed(const Key& key) {
  if (auto search = _lookup.find(key); search != _lookup.end()) {  // cache hit
    search->second.visited = true;
    return search->second.value;
  } else {
    return std::nullopt;
  }
}

template <typename Key, typename Value>
ceph::real_time WebCache<Key, Value>::insert_unmutexed(
    const Key& key, ValuePtr value) {
  // cache full? -> evict
  if (_sieve_queue.size() >= _capacity) {
    const auto node = sieve_evict();
    if (node != nullptr) {
      _lookup.erase(*(node->key));
    }
  }

  // cache insert
  const auto& [it, took_place] = _lookup.emplace(
      std::piecewise_construct, std::forward_as_tuple(key),
      std::forward_as_tuple(std::forward<ValuePtr>(value)));
  perf_set(Metric::size, _lookup.size());
  ceph_assert(took_place);
  auto& [stored_key, node] = *it;
  node.key = &stored_key;
  _sieve_queue.emplace_front(&node);
  return node.expires_at;
}

template <typename Key, typename Value>
std::optional<typename WebCache<Key, Value>::ValuePtr>
WebCache<Key, Value>::lookup(const Key& key) {
  std::shared_lock<std::shared_mutex> lock(_cache_mutex);
  auto result = lookup_unmutexed(key);
  if (result.has_value()) {
    perf_inc(Metric::hit);
  } else {
    perf_inc(Metric::miss);
  }
  return result;
}

template <typename Key, typename Value>
void WebCache<Key, Value>::sieve_expire_erase_unmutexed(
    std::list<Node*>& sieve_queue, ceph::real_time eviction_cutoff,
    std::vector<Node*>& out_expired) {
  // The sieve queue is ordered by ascending insertion time.
  // Find first not expired element from the back and erase from there
  // TODO(irq0) Fix BUG: This is no longer true if a node was updated with update_ttl_if
  auto expired_begin = sieve_queue.rend();
  for (auto it = sieve_queue.rbegin(); it != sieve_queue.rend(); ++it) {
    Node* node = (*it);
    const bool expired = node->expires_at <= eviction_cutoff;
    if (expired) {
      expired_begin = it;
    } else {
      break;
    }
  }

  if (expired_begin != sieve_queue.rend()) {
    out_expired.insert(
        out_expired.begin(), std::prev(expired_begin.base()),
        sieve_queue.end());
    sieve_queue.erase(std::prev(expired_begin.base()), sieve_queue.end());
  }
}

template <typename Key, typename Value>
size_t WebCache<Key, Value>::expire_erase(std::chrono::seconds ttl) {
  std::lock_guard<std::shared_mutex> lock(_cache_mutex);
  const auto expiration_cutoff = ceph::real_clock::now() + ttl;
  const auto lookup_size_before = _lookup.size();
  std::vector<Node*> expired;
  sieve_expire_erase_unmutexed(_sieve_queue, expiration_cutoff, expired);
  for (auto node : expired) {
    _lookup.erase(*(node->key));
  }
  ceph_assert((lookup_size_before - expired.size()) == _lookup.size());
  perf_inc(Metric::expired, expired.size());
  perf_set(Metric::size, _lookup.size());
  return expired.size();
}

}  // namespace webcache

#if FMT_VERSION >= 90000
template <typename Key, typename Value>
struct fmt::formatter<webcache::WebCache<Key, Value>> : fmt::ostream_formatter {
};
#endif
