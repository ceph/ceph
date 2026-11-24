// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#pragma once

#include <algorithm>
#include <atomic>
#include <concepts>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "PyObjectRef.h"
#include "common/config_obs.h"
#include "common/perf_counters.h"
#include "mgr/mgr_perf_counters.h"

// Transparent hash so unordered containers accept std::string_view lookups
// without constructing a std::string.
struct StringViewHash {
  using is_transparent = void;
  size_t operator()(std::string_view sv) const noexcept {
    return std::hash<std::string_view>{}(sv);
  }
};

template<class Value>
class LFUCache {
  struct Entry {
    Value val;
    mutable std::atomic<size_t> hits{0};
    bool valid = true;

    Entry(Value v) : val(std::move(v)), hits(0) {}
  };
  std::atomic<uint64_t> hits{0}, misses{0};
  const std::unordered_set<std::string, StringViewHash, std::equal_to<>> allowed_keys;
  std::unordered_map<std::string, Entry, StringViewHash, std::equal_to<>> cache_data;
  std::unordered_map<std::string, uint64_t, StringViewHash, std::equal_to<>> generations;
  const size_t capacity;
  std::atomic<bool> enabled{true};
  mutable std::shared_mutex m;

  void mark_miss() {
    misses++;
    if (perfcounter) {
      perfcounter->inc(l_mgr_cache_miss);
    }
  }

  void mark_hit() {
    hits++;
    if (perfcounter) {
      perfcounter->inc(l_mgr_cache_hit);
    }
  }

public:
  explicit LFUCache(std::unordered_set<std::string> keys,
                    size_t cap = UINT16_MAX,
                    bool ena = true)
      : allowed_keys(keys.begin(), keys.end()), capacity{cap}, enabled{ena} {
    for (const auto& key : allowed_keys) {
      generations.emplace(key, 0);
    }
  }

  uint64_t generation(std::string_view key) const {
    std::shared_lock l(m);
    auto it = generations.find(key);
    return it == generations.end() ? 0 : it->second;
  }

  // Return retired owners so the caller controls their destruction context.
  std::vector<Value> set_enabled(bool e) {
    enabled.store(e);
    if (!e) {
      return clear();
    }
    return {};
  }

  bool is_enabled() const noexcept {
    return enabled.load();
  }

  size_t size() const {
    std::shared_lock l(m);
    return std::count_if(cache_data.begin(), cache_data.end(),
                         [](const auto& entry) { return entry.second.valid; });
  }

  bool is_cacheable(std::string_view key) const noexcept {
    return allowed_keys.find(key) != allowed_keys.end();
  }

  bool can_write_cache(std::string_view key) const noexcept {
    return is_enabled() && is_cacheable(key);
  }

  struct InsertRes {
    bool inserted{false};        // false means the cache rejected the write
    std::optional<Value> replaced; // set if an existing key was overwritten
    std::optional<Value> evicted;  // set if an entry was evicted to make room
  };

  // Cloning a live value and extracting a stale owner are both protected by
  // the lock. Retired owners are destroyed after that lock has been released.
  template <typename Clone>
  std::optional<Value> lookup(std::string_view key, Clone&& clone, bool count_hit = true) {
    [[maybe_unused]] Value retired{};
    std::unique_lock l(m);
    auto it = cache_data.find(key);
    if (!is_enabled() || it == cache_data.end()) {
      if (count_hit) {
        mark_miss();
      }
      return std::nullopt;
    }
    if (!it->second.valid) {
      retired = std::move(it->second.val);
      cache_data.erase(it);
      if (count_hit) {
        mark_miss();
      }
      return std::nullopt;
    }
    if (count_hit) {
      it->second.hits.fetch_add(1, std::memory_order_relaxed);
      mark_hit();
    }
    return std::forward<Clone>(clone)(std::as_const(it->second.val));
  }

  bool try_get(std::string_view k, Value* out, bool count_hit = true) noexcept
    requires std::copyable<Value>;
  InsertRes insert(std::string_view key, Value value,
                   std::optional<uint64_t> generation = std::nullopt);
  bool erase(std::string_view key);
  bool extract(std::string_view k, Value* out) noexcept;
  void drain(std::vector<Value>& out);
  std::vector<Value> clear();

  // Invalidation never destroys a value and is safe without its runtime lock.
  // A key retains at most one stale owner until lookup, replacement or clear.
  bool invalidate(std::string_view key) noexcept {
    std::unique_lock l(m);
    auto generation = generations.find(key);
    if (generation != generations.end()) {
      ++generation->second;
    }
    auto it = cache_data.find(key);
    if (it == cache_data.end() || !it->second.valid) {
      return false;
    }
    it->second.valid = false;
    return true;
  }

  bool exists(std::string_view key) const noexcept {
    std::shared_lock l(m);
    auto it = cache_data.find(key);
    return it != cache_data.end() && it->second.valid;
  }

  uint64_t get_hits() const noexcept {
    return hits.load();
  }

  uint64_t get_misses() const noexcept {
    return misses.load();
  }

  Value get(std::string_view k) requires std::copy_constructible<Value>;
};

// Insertion consumes an owner. Lookup clones a live owner under the cache lock.
// Notification paths only mark entries stale; Python requests reclaim them.
class MgrMapCache : public md_config_obs_t {
  LFUCache<PyObjectRef> cache;

public:
  explicit MgrMapCache(uint16_t size = UINT16_MAX);
  ~MgrMapCache() override;
  // Lookup, insertion and physical erasure require the GIL. Invalidation does
  // not. Clear, disable and destruction may acquire the GIL: callers must not
  // hold manager locks. Before Python 3.13, a subinterpreter caller must use
  // clear_with_gil() before destruction. Owners die outside the cache lock.
  [[nodiscard]] PyObjectRef get(std::string_view key);
  void insert(std::string_view key, PyObjectRef value);
  void erase(std::string_view key) noexcept;
  void invalidate(std::string_view key) noexcept { cache.invalidate(key); }
  uint64_t generation(std::string_view key) const { return cache.generation(key); }
  bool insert_if_generation(std::string_view key, PyObjectRef value, uint64_t generation);
  void clear();
  // Requires the GIL and preserves the caller's interpreter.
  void clear_with_gil();
  void set_enabled(bool enabled);
  bool is_enabled() const noexcept { return cache.is_enabled(); }
  bool is_cacheable(std::string_view key) const noexcept { return cache.is_cacheable(key); }
  bool exists(std::string_view key) const noexcept { return cache.exists(key); }
  size_t size() const { return cache.size(); }
  uint64_t get_hits() const noexcept { return cache.get_hits(); }
  uint64_t get_misses() const noexcept { return cache.get_misses(); }

private:
  std::vector<std::string> get_tracked_keys() const noexcept override {
    return {"mgr_map_cache_enabled"};
  }
  void handle_conf_change(const ConfigProxy&, const std::set<std::string>&) override;
};
