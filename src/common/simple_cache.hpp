// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_SIMPLECACHE_H
#define CEPH_SIMPLECACHE_H

#include <list>
#include <map>
#include <unordered_map>
#include <utility>

#include "common/ceph_mutex.h"

template <class K, class V, class C = std::less<K>, class H = std::hash<K> >
class SimpleLRU {
  ceph::mutex lock = ceph::make_mutex("SimpleLRU::lock");
  size_t max_size;
  size_t max_bytes = 0;
  size_t total_bytes = 0;
  std::unordered_map<K, typename std::list<std::pair<K, V>>::iterator, H> contents;
  std::list<std::pair<K, V> > lru;
  std::map<K, V, C> pinned;

  void trim_cache() {
    while (contents.size() > max_size) {
      contents.erase(lru.back().first);
      lru.pop_back();
    }
  }

  void trim_cache_bytes() {
    while(total_bytes > max_bytes) {
      total_bytes -= lru.back().second.length();
      contents.erase(lru.back().first);
      lru.pop_back();
    }
  }

  void _add(K key, V&& value) {
    lru.emplace_front(key, std::move(value)); // can't move key because we access it below
    contents[key] = lru.begin();
    trim_cache();
  }

  void _add_bytes(K key, V&& value) {
    lru.emplace_front(key, std::move(value)); // can't move key because we access it below
    contents[key] = lru.begin();
    trim_cache_bytes();
  }

public:
  SimpleLRU(size_t max_size) : max_size(max_size) {
    contents.rehash(max_size);
  }

  void pin(K key, V val) {
    std::lock_guard l(lock);
    pinned.emplace(std::move(key), std::move(val));
  }

  void clear_pinned(K e) {
    std::lock_guard l(lock);
    for (auto i = pinned.begin();
	 i != pinned.end() && i->first <= e;
	 pinned.erase(i++)) {
      auto iter = contents.find(i->first);
      if (iter == contents.end())
	_add(i->first, std::move(i->second));
      else
	lru.splice(lru.begin(), lru, iter->second);
    }
  }

  void clear(K key) {
    std::lock_guard l(lock);
    auto i = contents.find(key);
    if (i == contents.end())
      return;
    total_bytes -= i->second->second.length();
    lru.erase(i->second);
    contents.erase(i);
  }

  void set_size(size_t new_size) {
    std::lock_guard l(lock);
    max_size = new_size;
    trim_cache();
  }

  size_t get_size() {
    std::lock_guard l(lock);
    return contents.size();
  }

  void set_bytes(size_t num_bytes) {
    std::lock_guard l(lock);
    max_bytes = num_bytes;
    trim_cache_bytes();
  }

  size_t get_bytes() {
    std::lock_guard l(lock);
    return total_bytes;
  }

  bool lookup(K key, V *out) {
    std::lock_guard l(lock);
    auto i = contents.find(key);
    if (i != contents.end()) {
      *out = i->second->second;
      lru.splice(lru.begin(), lru, i->second);
      return true;
    }
    auto i_pinned = pinned.find(key);
    if (i_pinned != pinned.end()) {
      *out = i_pinned->second;
      return true;
    }
    return false;
  }

  void add(K key, V value) {
    std::lock_guard l(lock);
    _add(std::move(key), std::move(value));
  }

  void add_bytes(K key, V value) {
    std::lock_guard l(lock);
    total_bytes += value.length();
    _add_bytes(std::move(key), std::move(value));
  }
};

#endif
