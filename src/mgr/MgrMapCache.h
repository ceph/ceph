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

#include <atomic>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "common/config_obs.h"
#include "common/perf_counters.h"
#include "mgr/mgr_perf_counters.h"
#include "PyUtil.h"

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

    Entry() = default;
    Entry(Value v) : val(std::move(v)), hits(0) {}
    Entry(const Entry& o) : val(o.val), hits(o.hits.load()) {}
    Entry(Entry&& o) noexcept : val(std::move(o.val)), hits(o.hits.load()) {}
  };
  std::atomic<uint64_t> hits{0}, misses{0};
  const std::unordered_set<std::string, StringViewHash, std::equal_to<>> allowed_keys;

protected:
  std::unordered_map<std::string, Entry, StringViewHash, std::equal_to<>> cache_data;
  const size_t capacity;
  std::atomic<bool> enabled{true};
  mutable std::shared_mutex m;

  void mark_miss() {
    misses++;
    if (perfcounter)
      perfcounter->inc(l_mgr_cache_miss);
  }

  void mark_hit() {
    hits++;
    if (perfcounter)
      perfcounter->inc(l_mgr_cache_hit);
  }

public:
  explicit LFUCache(std::unordered_set<std::string> keys,
                    size_t cap = UINT16_MAX,
                    bool ena = true)
      : allowed_keys(keys.begin(), keys.end()), capacity{cap}, enabled{ena} {}
  virtual ~LFUCache() = default;

  void set_enabled(bool e) {
    enabled.store(e);
    if (!e) {
      clear();
    }
  }

  bool is_enabled() const noexcept {
    return enabled.load();
  }

  size_t size() const {
    std::shared_lock l(m);
    return cache_data.size();
  }

  bool is_cacheable(std::string_view key) const noexcept {
    return allowed_keys.find(key) != allowed_keys.end();
  }

  bool can_read_cache(std::string_view key) const noexcept {
    return is_enabled() && is_cacheable(key) && exists(key);
  }

  bool can_write_cache(std::string_view key) const noexcept {
    return is_enabled() && is_cacheable(key);
  }

  struct InsertRes {
    bool inserted{false};        // false means the cache rejected the write
    std::optional<Value> replaced; // set if an existing key was overwritten
    std::optional<Value> evicted;  // set if an entry was evicted to make room
  };

  bool try_get(std::string_view k, Value* out, bool count_hit = true) noexcept;
  InsertRes insert(std::string_view key, Value value);

  bool erase(std::string_view key) {
    std::unique_lock l(m);
    auto it = cache_data.find(key);
    if (it == cache_data.end()) return false;
    cache_data.erase(it);
    return true;
  }

  bool extract(std::string_view k, Value* out) noexcept;
  void drain(std::vector<Value>& out) noexcept;

  virtual void clear() {
    std::unique_lock l(m);
    cache_data.clear();
    hits.store(0);
    misses.store(0);
  }

  bool exists(std::string_view key) const noexcept {
    std::shared_lock l(m);
    return cache_data.find(key) != cache_data.end();
  }

  uint64_t get_hits() const noexcept {
    return hits.load();
  }

  uint64_t get_misses() const noexcept {
    return misses.load();
  }

  Value get(std::string_view k);
};


// ---------- MgrMapCache generic ----------
template <class Value>
class MgrMapCache : public LFUCache<Value>,
                   public md_config_obs_t {
  using CacheImp = LFUCache<Value>;
public:
  explicit MgrMapCache(uint16_t sz = UINT16_MAX);
  ~MgrMapCache();
  bool try_get(std::string_view k, Value* out, bool count_hit = true) noexcept {
    return CacheImp::try_get(k, out, count_hit);
  }
  void insert(std::string_view k, Value v) { CacheImp::insert(k, v); }
  bool extract(std::string_view k, Value* out) noexcept { return CacheImp::extract(k, out); }
  void erase(std::string_view k) noexcept { Value v{}; (void)CacheImp::extract(k, &v); }
  void clear() noexcept { CacheImp::clear(); }
private:
  std::vector<std::string> get_tracked_keys() const noexcept override { return {"mgr_map_cache_enabled"}; }
  void handle_conf_change(const ConfigProxy& conf, const std::set<std::string>& changed) override;
};

// ------- Full template specialization for PyObject*. with GIL rules ----------
template <>
class MgrMapCache<PyObject*> : public LFUCache<PyObject*>,
                               public md_config_obs_t {
  using CacheImp = LFUCache<PyObject*>;
public:
  MgrMapCache(uint16_t size = UINT16_MAX);
  ~MgrMapCache();
  bool try_get(std::string_view k, PyObject** out, bool count_hit = true) noexcept = delete;
  PyObject* get(std::string_view key);
  void erase(std::string_view key) noexcept;
  void clear() noexcept override;
  void insert(std::string_view key, PyObject* value);
  void invalidate(std::string_view key) {
    erase(key);
  }
private:
  std::vector<std::string> get_tracked_keys() const noexcept override { return {"mgr_map_cache_enabled"}; }
  void handle_conf_change(const ConfigProxy& conf, const std::set<std::string>& changed) override;
};
