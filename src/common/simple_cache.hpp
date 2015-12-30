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

#include <map>
#include <list>
#include <memory>
#include "common/Mutex.h"
#include "common/Cond.h"
#include "include/unordered_map.h"

template <class K, class V, class C = std::less<K>, class H = std::hash<K> >
class SimpleLRU {
  Mutex lock;
  size_t max_size;
  ceph::unordered_map<K, typename list<pair<K, V> >::iterator, H> contents;
  list<pair<K, V> > lru;
  map<K, V, C> pinned;

  void trim_cache() {
    while (lru.size() > max_size) {
      contents.erase(lru.back().first);
      lru.pop_back();
    }
  }

  void _add(K key, V value) {
    lru.push_front(make_pair(key, value));
    contents[key] = lru.begin();
    trim_cache();
  }

public:
  SimpleLRU(size_t max_size) : lock("SimpleLRU::lock"), max_size(max_size) {
    contents.rehash(max_size);
  }

  void pin(K key, V val) {
    Mutex::Locker l(lock);
    pinned.insert(make_pair(key, val));
  }

  void clear_pinned(K e) {
    Mutex::Locker l(lock);
    for (typename map<K, V, C>::iterator i = pinned.begin();
	 i != pinned.end() && i->first <= e;
	 pinned.erase(i++)) {
      typename ceph::unordered_map<K, typename list<pair<K, V> >::iterator, H>::iterator iter =
        contents.find(i->first);
      if (iter == contents.end())
	_add(i->first, i->second);
      else
	lru.splice(lru.begin(), lru, iter->second);
    }
  }

  void clear(K key) {
    Mutex::Locker l(lock);
    typename ceph::unordered_map<K, typename list<pair<K, V> >::iterator, H>::iterator i =
      contents.find(key);
    if (i == contents.end())
      return;
    lru.erase(i->second);
    contents.erase(i);
  }

  void set_size(size_t new_size) {
    Mutex::Locker l(lock);
    max_size = new_size;
    trim_cache();
  }

  bool lookup(K key, V *out) {
    Mutex::Locker l(lock);
    typename ceph::unordered_map<K, typename list<pair<K, V> >::iterator, H>::iterator i =
      contents.find(key);
    if (i != contents.end()) {
      *out = i->second->second;
      lru.splice(lru.begin(), lru, i->second);
      return true;
    }
    typename map<K, V, C>::iterator i_pinned = pinned.find(key);
    if (i_pinned != pinned.end()) {
      *out = i_pinned->second;
      return true;
    }
    return false;
  }

  void add(K key, V value) {
    Mutex::Locker l(lock);
    _add(key, value);
  }
};

#endif
