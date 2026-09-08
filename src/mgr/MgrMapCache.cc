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

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "api cache " << __func__ << " "

static const std::unordered_set<std::string> mgr_cache_keys = {
  "osd_map", "pg_dump", "pg_stats", "mon_status", "mgr_map",
  "osd_metadata", "mds_metadata", "config"
};

template<class Value>
MgrMapCache<Value>::~MgrMapCache() {
  g_conf().remove_observer(this);
}

template<class Value>
bool LFUCache<Value>::extract(std::string_view k, Value* out) noexcept {
  std::unique_lock<std::shared_mutex> l(m);
  auto it = cache_data.find(k);
  if (it == cache_data.end()) return false;
  *out = it->second.val;
  cache_data.erase(it);
  return true;
}

template<class Value>
void LFUCache<Value>::drain(std::vector<Value>& out) noexcept {
  std::unique_lock<std::shared_mutex> l(m);
  out.reserve(cache_data.size());
  for (auto& kv : cache_data) out.push_back(kv.second.val);
  cache_data.clear();
  hits.store(0);
  misses.store(0);
}

template<class Value>
Value LFUCache<Value>::get(std::string_view k) {
  std::shared_lock l(m);
  if (!is_enabled()) throw std::out_of_range("cache disabled");
  auto it = cache_data.find(k);
  if (it == cache_data.end()) {
    throw std::out_of_range(std::string(k));
  }
  it->second.hits.fetch_add(1, std::memory_order_relaxed);
  mark_hit();
  return it->second.val;
}

template<class Value>
bool LFUCache<Value>::try_get(std::string_view k, Value* out, bool count_hit) noexcept {
  std::shared_lock l(m);
  auto it = cache_data.find(k);
  if (it == cache_data.end()) {
    if (count_hit) {
      mark_miss();
    }
    return false;
  }
  if (count_hit) {
    it->second.hits.fetch_add(1, std::memory_order_relaxed);
    mark_hit();
  }
  *out = it->second.val;
  return true;
}

template<class Value>
typename LFUCache<Value>::InsertRes
LFUCache<Value>::insert(std::string_view key, Value value) {
  if (!can_write_cache(key)) {
    return InsertRes{false};
  }

  std::unique_lock<std::shared_mutex> l(m);
  // Re-check enabled after acquiring lock: state may have flipped while waiting.
  if (!enabled.load(std::memory_order_relaxed)) {
    return InsertRes{false};
  }

  auto it = cache_data.find(key);
  if (it != cache_data.end()) {
    InsertRes res{true};
    res.replaced = std::move(it->second.val);
    it->second.val = std::move(value);
    return res;
  }

  // New insert counts as a miss (cache didn't have it)
  mark_miss();

  InsertRes res{true};
  if (cache_data.size() >= capacity && !cache_data.empty()) {
    auto min_it = std::min_element(cache_data.begin(),
      cache_data.end(),
      [](const auto& a, const auto& b) {
        return a.second.hits.load(std::memory_order_relaxed) <
               b.second.hits.load(std::memory_order_relaxed);
    });
    res.evicted = std::move(min_it->second.val);
    cache_data.erase(min_it);
  }

  // Allocate std::string only here, when we actually need to store a new key.
  cache_data.emplace(std::string(key), Entry(std::move(value)));
  return res;
}

template <class Value>
MgrMapCache<Value>::MgrMapCache(uint16_t size)
    : CacheImp(mgr_cache_keys, size, g_conf().get_val<bool>("mgr_map_cache_enabled")) {
  dout(20) << ": creating cache with size " << size << dendl;
  g_conf().add_observer(this);
}

template <class Value>
void MgrMapCache<Value>::handle_conf_change(
    const ConfigProxy& conf, const std::set<std::string>& changed) {
  if (changed.count("mgr_map_cache_enabled")) {
    this->set_enabled(conf.get_val<bool>("mgr_map_cache_enabled"));
  }
}

MgrMapCache<PyObject*>::MgrMapCache(uint16_t size)
    : CacheImp(mgr_cache_keys, size, g_conf().get_val<bool>("mgr_map_cache_enabled")) {
  dout(20) << ": creating cache with size " << size << dendl;
  g_conf().add_observer(this);
}

MgrMapCache<PyObject*>::~MgrMapCache() {
  g_conf().remove_observer(this);
  this->clear();
}

PyObject* MgrMapCache<PyObject*>::get(std::string_view k) {
  if (!this->is_enabled() ||
      !this->is_cacheable(k) ||
      !PyGILState_Check())
        return nullptr;
  std::shared_lock l(this->m);
  auto it = this->cache_data.find(k);
  if (it == this->cache_data.end()) {
    this->mark_miss();
    return nullptr;
  }
  PyObject* o = it->second.val;
  Py_INCREF(o);  // INCREF under lock: no window for erase() to drop refcount to zero
  it->second.hits.fetch_add(1, std::memory_order_relaxed);
  this->mark_hit();
  return o;
}

void MgrMapCache<PyObject*>::insert(std::string_view key, PyObject* value) {
  if (!this->can_write_cache(key) || !PyGILState_Check()) {
    return;
  }

  Py_INCREF(value);
  auto res = CacheImp::insert(key, value);

  if (!res.inserted) {
    // Cache was disabled between our check and lock acquisition; undo the INCREF.
    Py_DECREF(value);
    return;
  }

  auto schedule_decref = [](PyObject* obj) {
    if (!obj) return;
    if (Py_AddPendingCall(+[](void* p){ Py_DECREF((PyObject*)p); return 0; }, obj) != 0) {
      PyGILState_STATE st = PyGILState_Ensure();
      Py_DECREF(obj);
      PyGILState_Release(st);
    }
  };
  if (res.replaced.has_value()) schedule_decref(res.replaced.value());
  if (res.evicted.has_value())  schedule_decref(res.evicted.value());

  dout(20) << ": inserted key: " << key << " py count: "
           << Py_REFCNT(value) << " hit/miss:"
           << CacheImp::get_hits() << "/"
           << CacheImp::get_misses() << dendl;
}

void MgrMapCache<PyObject*>::erase(std::string_view key) noexcept {
  if (!this->is_cacheable(key)) return;
  PyObject* o = nullptr;
  if (!this->extract(key, &o)) return;

  Py_AddPendingCall(+[](void* p){ Py_DECREF((PyObject*)p); return 0; }, o);
  dout(20) << ": erased key: " << key
           << " hit/miss:"
           << CacheImp::get_hits() << "/"
           << CacheImp::get_misses() << dendl;
}

void MgrMapCache<PyObject*>::clear() noexcept {
  std::vector<PyObject*> to_drop;
  this->drain(to_drop);
  if (to_drop.empty()) return;
  PyGILState_STATE st = PyGILState_Ensure();
  for (auto* o : to_drop) Py_DECREF(o);
  PyGILState_Release(st);
  dout(20) << ": Cache cleared" << dendl;
}

void MgrMapCache<PyObject*>::handle_conf_change(
    const ConfigProxy& conf, const std::set<std::string>& changed) {
  if (changed.count("mgr_map_cache_enabled")) {
    this->set_enabled(conf.get_val<bool>("mgr_map_cache_enabled"));
  }
}

// Explicit instantiation for unit tests
template class LFUCache<int>;
