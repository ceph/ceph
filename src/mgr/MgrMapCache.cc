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

#include "mgr/MgrMapCache.h"

#include <algorithm>
#include <optional>

#include "common/config_proxy.h"
#include "common/debug.h"
#include "global/global_context.h"
#include "include/scope_guard.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "api cache " << __func__ << " "

static const std::unordered_set<std::string> mgr_cache_keys = {
  "osd_map", "pg_dump", "pg_stats", "mon_status", "mgr_map",
  "osd_metadata", "mds_metadata", "config"
};

namespace {
void destroy_with_gil(std::vector<PyObjectRef> objects) {
  if (objects.empty()) {
    return;
  }
#if PY_VERSION_HEX >= 0x030d0000
  // This public API returns the calling thread's attached state. Preserve its
  // interpreter when it already holds the GIL, including a subinterpreter.
  if (PyThreadState_GetUnchecked()) {
    return;
  }
#endif
  // Older Python must always acquire the GIL: its private unchecked getter
  // can return another thread's state. This also handles unattached threads
  // on Python 3.13 and later.
  const auto state = PyGILState_Ensure();
  const auto release = make_scope_guard([state] { PyGILState_Release(state); });
  // Declared after the guard: owners die before the GIL is released.
  auto to_drop = std::move(objects);
}
}

template<class Value>
bool LFUCache<Value>::extract(std::string_view k, Value* out) noexcept {
  Value extracted{};
  {
    std::unique_lock l(m);
    auto it = cache_data.find(k);
    if (it == cache_data.end()) {
      return false;
    }
    extracted = std::move(it->second.val);
    cache_data.erase(it);
  }
  *out = std::move(extracted);
  return true;
}

template<class Value>
bool LFUCache<Value>::erase(std::string_view key) {
  typename decltype(cache_data)::node_type retired;
  std::unique_lock l(m);
  auto generation = generations.find(key);
  if (generation != generations.end()) {
    ++generation->second;
  }
  auto it = cache_data.find(key);
  if (it == cache_data.end()) {
    return false;
  }
  retired = cache_data.extract(it);
  return true;
}

template<class Value>
std::vector<Value> LFUCache<Value>::clear() {
  std::vector<Value> values;
  std::unique_lock l(m);
  for (auto& [key, generation] : generations) {
    ++generation;
  }
  values.reserve(cache_data.size());
  for (auto& kv : cache_data) {
    values.push_back(std::move(kv.second.val));
  }
  cache_data.clear();
  hits.store(0);
  misses.store(0);
  return values;
}

template<class Value>
void LFUCache<Value>::drain(std::vector<Value>& out) {
  auto values = clear();
  out.reserve(out.size() + values.size());
  for (auto& value : values) {
    out.push_back(std::move(value));
  }
}

template<class Value>
Value LFUCache<Value>::get(std::string_view k) requires std::copy_constructible<Value> {
  if (!is_enabled()) {
    throw std::out_of_range("cache disabled");
  }
  auto value = lookup(k, [](const Value& value) { return value; });
  if (!value) {
    throw std::out_of_range(std::string(k));
  }
  return std::move(*value);
}

template<class Value>
bool LFUCache<Value>::try_get(std::string_view k, Value* out, bool count_hit) noexcept
  requires std::copyable<Value> {
  auto value = lookup(k, [](const Value& value) { return value; }, count_hit);
  if (!value) {
    return false;
  }
  *out = std::move(*value);
  return true;
}

template<class Value>
typename LFUCache<Value>::InsertRes
LFUCache<Value>::insert(std::string_view key, Value value,
                        std::optional<uint64_t> generation) {
  if (capacity == 0 || !can_write_cache(key)) {
    return InsertRes{false};
  }
  // Keep retired owners outside the lock, including during exception unwinding.
  InsertRes res;
  std::unique_lock<std::shared_mutex> l(m);
  // Re-check enabled after acquiring lock: state may have flipped while waiting.
  if (!is_enabled()) {
    return InsertRes{false};
  }
  if (generation) {
    auto current = generations.find(key);
    if (current == generations.end() || current->second != *generation) {
      return InsertRes{false};
    }
  }

  auto it = cache_data.find(key);
  if (it != cache_data.end()) {
    res.replaced = std::move(it->second.val);
    it->second.val = std::move(value);
    it->second.valid = true;
    res.inserted = true;
    return res;
  }

  // Allocate buckets before transferring ownership into a new node.
  cache_data.reserve(cache_data.size() + 1);
  if (cache_data.size() >= capacity) {
    auto min_it = std::min_element(cache_data.begin(),
      cache_data.end(),
      [](const auto& a, const auto& b) {
        if (a.second.valid != b.second.valid) {
          return !a.second.valid;
        }
        return a.second.hits.load(std::memory_order_relaxed) <
               b.second.hits.load(std::memory_order_relaxed);
      });
    res.evicted = std::move(min_it->second.val);
    cache_data.erase(min_it);
  }
  // Allocate std::string only here, when we actually need to store a new key.
  cache_data.try_emplace(std::string(key), std::move(value));
  mark_miss();
  res.inserted = true;
  return res;
}

MgrMapCache::MgrMapCache(uint16_t size)
    : cache(mgr_cache_keys, size, g_conf().get_val<bool>("mgr_map_cache_enabled")) {
  dout(20) << ": creating cache with size " << size << dendl;
  g_conf().add_observer(this);
}

MgrMapCache::~MgrMapCache() {
  g_conf().remove_observer(this);
  clear();
}

PyObjectRef MgrMapCache::get(std::string_view key) {
  if (!cache.is_enabled() || !cache.is_cacheable(key)) {
    return {};
  }
  auto value = cache.lookup(key, [](const PyObjectRef& value) { return value.clone(); });
  return value ? std::move(*value) : PyObjectRef{};
}

void MgrMapCache::insert(std::string_view key, PyObjectRef value) {
  if (!value) {
    return;
  }
  const bool inserted = cache.insert(key, std::move(value)).inserted;
  if (!inserted) {
    return;
  }

  dout(20) << ": inserted key: " << key << " hit/miss:"
           << cache.get_hits() << "/"
           << cache.get_misses() << dendl;
}

bool MgrMapCache::insert_if_generation(std::string_view key, PyObjectRef value,
                                       uint64_t generation) {
  if (!value) {
    return false;
  }
  return cache.insert(key, std::move(value), generation).inserted;
}

void MgrMapCache::erase(std::string_view key) noexcept {
  if (!cache.erase(key)) {
    return;
  }

  dout(20) << ": erased key: " << key
           << " hit/miss:"
           << cache.get_hits() << "/"
           << cache.get_misses() << dendl;
}

void MgrMapCache::clear() {
  auto to_drop = cache.clear();
  if (to_drop.empty()) {
    return;
  }
  destroy_with_gil(std::move(to_drop));
  dout(20) << ": Cache cleared" << dendl;
}

void MgrMapCache::clear_with_gil() {
  // The caller supplies the destruction context, including a subinterpreter.
  // clear() returns owners after unlocking; their destruction keeps that GIL.
  auto to_drop = cache.clear();
}

void MgrMapCache::set_enabled(bool enabled) {
  destroy_with_gil(cache.set_enabled(enabled));
}

void MgrMapCache::handle_conf_change(
    const ConfigProxy& conf, const std::set<std::string>& changed) {
  if (changed.count("mgr_map_cache_enabled")) {
    this->set_enabled(conf.get_val<bool>("mgr_map_cache_enabled"));
  }
}

// Explicit instantiation for unit tests
template class LFUCache<int>;
template class LFUCache<std::unique_ptr<int>>;
