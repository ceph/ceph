// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RANDOMCACHE_H
#define CEPH_RANDOMCACHE_H

#include "include/unordered_map.h"
#include "common/Mutex.h"
#include "include/compat.h"


template <class K, class V>
class RandomCache {
  ceph::unordered_map<K, pair<uint64_t, V> > contents;
  Mutex lock;
  uint64_t max_size, count;

  void trim_cache(uint64_t evict_count) {
    typename ceph::unordered_map<K, pair<uint64_t, V> >::iterator it = contents.begin();
    while (evict_count) {
      uint64_t min = UINT64_MAX;
      typename ceph::unordered_map<K, pair<uint64_t, V> >::iterator min_it;
      int compare_count = 3;
      for (; it != contents.end(); it++, compare_count--) {
        if (!compare_count)
          break;
        if (it->second.first < min) {
          min_it = it;
          min = it->second.first;
        }
      }
      if (min != UINT64_MAX) {
        contents.erase(min_it);
        count--;
      }
      evict_count--;
    }
  }

 public:
  RandomCache(size_t max_size=20) : lock("SharedLRU::lock"),
                                    max_size(max_size), count(0) {}
  ~RandomCache() {
    contents.clear();
  }

  void clear(K key) {
    Mutex::Locker l(lock);
    contents.erase(key);
    count--;
  }

  void set_size(size_t new_size) {
    Mutex::Locker l(lock);
    max_size = new_size;
    if (max_size <= count) {
      trim_cache(count - max_size);
    }
  }

  bool lookup(K key, V *out) {
    Mutex::Locker l(lock);
    typename ceph::unordered_map<K, pair<uint64_t, V> >::iterator it = contents.find(key);
    if (it != contents.end()) {
      it->second.first++;
      *out = it->second.second;
      return true;
    }
    return false;
  }

  void add(K key, V value) {
    Mutex::Locker l(lock);
    if (max_size <= count) {
      trim_cache(5);
    }
    contents[key] = make_pair(1, value);
    count++;
  }
};

#endif
