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
#include <boost/intrusive/list.hpp>
#include <chrono>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <shared_mutex>
#include <unordered_map>
#include <utility>

#include "common/ceph_context.h"
#include "common/ceph_time.h"
#include "include/ceph_assert.h"
#include "include/common_fwd.h"
#include "common/perf_counters_collection.h"

// Web Cache
// A cache for data living on other systems like Key Management Systems
// Goals/Features
// (1) Thread safe, bias towards handling highly concurrent lookups
// (2) Expire entries by ttl
// (3) Cache replacement tuned to "web" workloads
//
// Algorithm
// The implementation is based on SIEVE [0] with additional TTL
// expiration support
//
// Data Structures
// - key lookup table: key -> Node{value ptr, metadata) (owns nodes)
// - SIEVE FIFO + hand intrusive list on lookup's nodes + pointer
//
// Cache Stampedes
//
// Occurs when numerous threads simultaneously request the same data
// that is not yet in the cache. This can lead to many redundant,
// costly requests, that may overwhelm external systems. Although
// WebCache does not have built-in stampede mitigation, the lookup_or
// function is designed to be used with additional logic, such as
// ceph::async::call_once / std::call_once.
//
// [0] Zhang, Yazhuo, et al. "{SIEVE} is Simpler than {LRU}: an Efficient
// {Turn-Key} Eviction Algorithm for Web Caches." 21st USENIX
// Symposium on Networked Systems Design and Implementation (NSDI 24).
// 2024.

class WebCacheTest;

namespace webcache {

enum class Metric {
  metrics_start = 84000,
  hit,
  miss,
  expired,
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
  struct Node : public boost::intrusive::list_base_hook<> {
    std::atomic_bool visited;
    ceph::real_time expires_at;
    Key const* key;
    ValuePtr value;

    explicit Node(ValuePtr value, ceph::timespan ttl)
        : visited(false),
          expires_at(ceph::real_clock::now() + ttl),
          key(nullptr),
          value(std::move(value)) {};
    // for testing
    Node(ValuePtr value, ceph::real_time expires_at)
        : visited(false),
          expires_at(expires_at),
          key(nullptr),
          value(std::move(value)) {};

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
  using SieveQueue = boost::intrusive::list<Node>;

 private:
  CephContext* _cct;
  std::string _name;
  PerfCounters* _perf;
  size_t _capacity;
  ceph::timespan _ttl;
  std::unordered_map<Key, Node> _lookup;
  SieveQueue _sieve_queue;
  Node* _sieve_hand;
  mutable std::shared_mutex _cache_mutex;

 protected:
  // sieve_evict removes the next node using the SIEVE algorithm from
  // _sieve_queue and returns a pointer to the evicted node
  Node* sieve_evict();

  // sieve_expire_erase_unmutexed removes all expired nodes from the
  // sieve_queue in place. It writes the expired nodes to out_expired.
  // Updates the sieve hand
  static void sieve_expire_erase_unmutexed(
      SieveQueue& sieve_queue, Node* sieve_hand,
      ceph::real_time eviction_cutoff, SieveQueue& out_expired);

  using SieveRemoveRet = std::pair<typename SieveQueue::iterator, Node*>;
  // sieve_remove_unmutexed removes a node from sieve_queue and
  // returns an iterator to next (or end()) and an updated sieve hand
  static SieveRemoveRet sieve_remove_unmutexed(
      SieveQueue& sieve_queue, Node* sieve_hand, const Node& node);

  static PerfCounters* initialize_perf_counters(
      CephContext* cct, const std::string& name);

  std::optional<ValuePtr> lookup_unmutexed(const Key& key);
  Node& insert_or_existing_unmutexed(const Key& key, ValuePtr value);

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
  explicit WebCache(size_t capacity, ceph::timespan ttl);

  // For system use, activates perf counters
  WebCache(
      CephContext* cct, const std::string& name, size_t capacity,
      ceph::timespan ttl = ceph::timespan::zero());

  // If enabled, remove perf counters (Requires perf counter subsystem
  // running)
  ~WebCache();

  // lookup returns the stored value for a given key or not
  std::optional<ValuePtr> lookup(const Key& key);

  // add caches a key/value pair. If key already exists it does
  // nothing. Return the stored timestamp of the k/v mapping
  ceph::real_time add(const Key& key, ValuePtr val);

  // lookup_or returns a value for key. If none is cached yet, insert
  // new_val and return that. A common use is with
  // ceph::async::call_once to add cache stampede mitigation
  ValuePtr lookup_or(const Key& key, ValuePtr new_val);

  // update_ttl_if updates an entry's TTL if its value matches val.
  // Return true if we updated an entry. False otherwise.
  bool update_ttl_if(
      const Key& key, const ValuePtr& val_ptr, ceph::timespan new_ttl);

  // remove_if removes an entry if it finds an entry where its values
  // matches expected_val. return true if we removed an entry, false
  // otherwise
  bool remove_if(const Key& key, const ValuePtr& expected_val);

  size_t size() const;
  size_t clear();

  // expire_erase erases all expired cache entries
  size_t expire_erase();

  const std::string& name() { return _name; }

  PerfCounters* perf() { return _perf; }

  friend std::ostream& operator<<(
      std::ostream& os, const WebCache<Key, Value>& cache) {
    std::shared_lock<std::shared_mutex> lock(cache._cache_mutex);
    const auto now = ceph::real_clock::now();
    os << "$" << cache._name << "[";

    for (const auto& node : cache._sieve_queue) {
      const auto ttl = node.expires_at - now;
      fmt::print(
          os, "\"{}\"({}){}{}", *(node.key),
          std::chrono::duration_cast<std::chrono::seconds>(ttl),
          (&node == cache._sieve_hand) ? "👉" : "", node.visited ? "▮" : "▯");
      if (&node != &cache._sieve_queue.back()) {
        os << ", ";
      }
    }
    os << "]";
    return os;
  }

  friend std::ostream& operator<<(std::ostream& os, const SieveQueue& nodes) {
    for (const auto& node : nodes) {
      os << node;
      if (&node != &nodes.back()) {
        os << ", ";
      }
    }
    return os;
  }

  friend class WebCacheTest;
  friend class WebCacheConcurrencyTest;
  friend class WebCacheRandomizedTest;
  friend class WebCacheTest_SieveExample_Test;
  friend class WebCacheTest_ExpireEraseOne_Test;
  friend class WebCacheTest_ExpireEraseAll_Test;
  friend class WebCacheTest_ExpireEraseEmpty_Test;
  friend class WebCacheTest_ExpireEraseUpdatedTTLs_Test;
  friend class WebCacheTest_SieveRemoveHand_Test;
};

template <typename Key, typename Value>
WebCache<Key, Value>::Node* WebCache<Key, Value>::sieve_evict() {
  if (_sieve_queue.empty()) {
    return nullptr;
  }
  if (_sieve_hand == nullptr) {
    _sieve_hand = &_sieve_queue.back();
  }
  Node* result = nullptr;
  const size_t queue_size_before = _sieve_queue.size();

  auto hand = _sieve_queue.iterator_to(*_sieve_hand);
  const auto rev_it = typename SieveQueue::reverse_iterator(std::next(hand));
  for (auto it = rev_it; it != _sieve_queue.rend(); ++it) {
    Node& node = (*it);
    if (node.visited) {
      node.visited = false;
      --hand;
    } else {
      hand = std::prev(_sieve_queue.erase(std::next(it).base()));
      result = &node;
      break;
    }
  }
  // every node was visited. we need still need to evict the tail
  if (result == nullptr) {
    result = &_sieve_queue.back();
    _sieve_queue.pop_back();
  }

  ceph_assertf(
      queue_size_before == 0 || (queue_size_before - _sieve_queue.size()) == 1,
      "%d -> %d capacity:%d", queue_size_before, _sieve_queue.size(),
      _capacity);
  _sieve_hand = (hand == _sieve_queue.begin()) ? &_sieve_queue.back() : &*hand;
  return result;
}

template <typename Key, typename Value>
WebCache<Key, Value>::WebCache(size_t capacity, ceph::timespan ttl)
    : _cct(nullptr),
      _perf(nullptr),
      _capacity(capacity),
      _ttl(ttl),
      _lookup(),
      _sieve_queue(),
      _sieve_hand(nullptr) {}

template <typename Key, typename Value>
WebCache<Key, Value>::WebCache(
    CephContext* cct, const std::string& name, size_t capacity,
    ceph::timespan ttl)
    : _cct(cct),
      _name(name),
      _perf(initialize_perf_counters(cct, name)),
      _capacity(capacity),
      _ttl(ttl),
      _lookup(),
      _sieve_queue(),
      _sieve_hand(nullptr) {
  ceph_assert(cct != nullptr);
  perf_set(Metric::capacity, capacity);
}

template <typename Key, typename Value>
WebCache<Key, Value>::~WebCache() {
  if (_cct != nullptr && _perf != nullptr) {
    _cct->get_perfcounters_collection()->remove(_perf);
  }
}

template <typename Key, typename Value>
PerfCounters* WebCache<Key, Value>::initialize_perf_counters(
    CephContext* cct, const std::string& name) {
  PerfCountersBuilder pcb(
      cct, name, static_cast<int>(Metric::metrics_start),
      static_cast<int>(Metric::metrics_stop));
  pcb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);
  pcb.add_u64_counter(static_cast<int>(Metric::hit), "hit", "Cache hits");
  pcb.add_u64_counter(static_cast<int>(Metric::miss), "miss", "Cache misses");
  pcb.add_u64_counter(
      static_cast<int>(Metric::expired), "expired", "Expired cache entries");
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
  _sieve_hand = nullptr;
  _lookup.clear();
  perf_set(Metric::size, 0);
  perf_set(Metric::hit, 0);
  perf_set(Metric::miss, 0);
  perf_set(Metric::expired, 0);
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
  // miss, take unique lock
  {
    std::lock_guard<std::shared_mutex> lock(_cache_mutex);
    return insert_or_existing_unmutexed(key, value).expires_at;
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
  {
    std::lock_guard<std::shared_mutex> lock(_cache_mutex);
    return insert_or_existing_unmutexed(key, new_val).value;
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
bool WebCache<Key, Value>::remove_if(
    const Key& key, const ValuePtr& expected_val) {
  std::lock_guard<std::shared_mutex> lock(_cache_mutex);
  if (auto search = _lookup.find(key);
      (search != _lookup.end() && search->second.value == expected_val)) {
    auto [_, hand_moved] =
        sieve_remove_unmutexed(_sieve_queue, _sieve_hand, search->second);
    _lookup.erase(search);
    _sieve_hand = hand_moved;
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
WebCache<Key, Value>::Node& WebCache<Key, Value>::insert_or_existing_unmutexed(
    const Key& key, ValuePtr value) {
  const auto& [it, took_place] = _lookup.emplace(
      std::piecewise_construct, std::forward_as_tuple(key),
      std::forward_as_tuple(std::move(value), _ttl));

  if (took_place) {  // cache miss
    perf_inc(Metric::miss);

    ceph_assert(_lookup.size() == _sieve_queue.size() + 1);
    // cache full? -> evict
    if (_sieve_queue.size() >= _capacity) {
      const auto node = sieve_evict();
      if (node != nullptr) {
        _lookup.erase(*(node->key));
      }
    }
    auto& [stored_key, node] = *it;
    node.key = &stored_key;
    _sieve_queue.push_front(node);
    perf_set(Metric::size, _lookup.size());
    return node;
  } else {  // cache hit
    perf_inc(Metric::hit);
    it->second.visited = true;
    return it->second;
  }
}

template <typename Key, typename Value>
WebCache<Key, Value>::SieveRemoveRet
WebCache<Key, Value>::sieve_remove_unmutexed(
    SieveQueue& sieve_queue, Node* sieve_hand, const Node& node) {
  const bool was_hand = &node == sieve_hand;
  const auto node_it = sieve_queue.iterator_to(node);
  auto it = sieve_queue.erase(node_it);
  if (sieve_queue.empty()) {
    return {sieve_queue.end(), nullptr};
  }
  if (was_hand) {
    return {
        it,
        (it == sieve_queue.begin()) ? &sieve_queue.back() : &(*std::prev(it))};
  }
  return {it, sieve_hand};
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
    SieveQueue& sieve_queue, Node* sieve_hand, ceph::real_time eviction_cutoff,
    SieveQueue& out_expired) {
  // The sieve queue is ordered by ascending insertion time which
  // would allow for efficient epiration by finding the first not
  // expired element. BUT, since we allow updating TTLs this property
  // no longer holds and we have do a full sieve queue sweep.
  for (auto it = sieve_queue.begin(); it != sieve_queue.end();) {
    Node& node = (*it);
    const bool expired = node.expires_at <= eviction_cutoff;
    if (expired) {
      auto [next_it, next_hand] =
          sieve_remove_unmutexed(sieve_queue, sieve_hand, node);
      ceph_assert(!node.is_linked());
      out_expired.push_back(node);
      it = next_it;
      sieve_hand = next_hand;
    } else {
      ++it;
    }
  }
}

template <typename Key, typename Value>
size_t WebCache<Key, Value>::expire_erase() {
  std::lock_guard<std::shared_mutex> lock(_cache_mutex);
  const auto expiration_cutoff = ceph::real_clock::now();
  const auto lookup_size_before = _lookup.size();
  SieveQueue expired;
  sieve_expire_erase_unmutexed(
      _sieve_queue, _sieve_hand, expiration_cutoff, expired);
  const auto expired_size = expired.size();
  expired.clear_and_dispose(
      [&](const Node* node) { _lookup.erase(*node->key); });
  ceph_assert((lookup_size_before - expired_size) == _lookup.size());
  perf_inc(Metric::expired, expired_size);
  perf_set(Metric::size, _lookup.size());
  return expired_size;
}

}  // namespace webcache

#if FMT_VERSION >= 90000
template <typename Key, typename Value>
struct fmt::formatter<webcache::WebCache<Key, Value>> : fmt::ostream_formatter {
};
#endif
