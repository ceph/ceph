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

#ifndef CEPH_SHAREDCACHE_H
#define CEPH_SHAREDCACHE_H

#include <map>
#include <list>
#include <memory>
#include <utility>
#include "common/Mutex.h"
#include "common/Cond.h"

template <class K, class V>
class SharedLRU {
  typedef ceph::shared_ptr<V> VPtr;
  typedef ceph::weak_ptr<V> WeakVPtr;
  Mutex lock;
  size_t max_size;
  Cond cond;

  map<K, typename list<pair<K, VPtr> >::iterator > contents;
  list<pair<K, VPtr> > lru;

  map<K, WeakVPtr> weak_refs;

  void trim_cache(list<VPtr> *to_release) {
    while (lru.size() > max_size) {
      to_release->push_back(lru.back().second);
      lru_remove(lru.back().first);
    }
  }

  void lru_remove(K key) {
    if (!contents.count(key))
      return;
    lru.erase(contents[key]);
    contents.erase(key);
  }

  void lru_add(K key, VPtr val, list<VPtr> *to_release) {
    if (contents.count(key)) {
      lru.splice(lru.begin(), lru, contents[key]);
    } else {
      lru.push_front(make_pair(key, val));
      contents[key] = lru.begin();
      trim_cache(to_release);
    }
  }

  void remove(K key) {
    Mutex::Locker l(lock);
    weak_refs.erase(key);
    cond.Signal();
  }

  class Cleanup {
  public:
    SharedLRU<K, V> *cache;
    K key;
    Cleanup(SharedLRU<K, V> *cache, K key) : cache(cache), key(key) {}
    void operator()(V *ptr) {
      cache->remove(key);
      delete ptr;
    }
  };

public:
  SharedLRU(size_t max_size = 20) : lock("SharedLRU::lock"), max_size(max_size) {}
  
  ~SharedLRU() {
    contents.clear();
    lru.clear();
    assert(weak_refs.empty());
  }

  void clear(K key) {
    VPtr val; // release any ref we have after we drop the lock
    {
      Mutex::Locker l(lock);
      if (weak_refs.count(key)) {
	val = weak_refs[key].lock();
      }
      lru_remove(key);
    }
  }

  void set_size(size_t new_size) {
    list<VPtr> to_release;
    {
      Mutex::Locker l(lock);
      max_size = new_size;
      trim_cache(&to_release);
    }
  }

  // Returns K key s.t. key <= k for all currently cached k,v
  K cached_key_lower_bound() {
    Mutex::Locker l(lock);
    return weak_refs.begin()->first;
  }

  VPtr lower_bound(K key) {
    VPtr val;
    list<VPtr> to_release;
    {
      Mutex::Locker l(lock);
      bool retry = false;
      do {
	retry = false;
	if (weak_refs.empty())
	  break;
	typename map<K, WeakVPtr>::iterator i = weak_refs.lower_bound(key);
	if (i == weak_refs.end())
	  --i;
	val = i->second.lock();
	if (val) {
	  lru_add(i->first, val, &to_release);
	} else {
	  retry = true;
	}
	if (retry)
	  cond.Wait(lock);
      } while (retry);
    }
    return val;
  }

  VPtr lookup(K key) {
    VPtr val;
    list<VPtr> to_release;
    {
      Mutex::Locker l(lock);
      bool retry = false;
      do {
	retry = false;
	if (weak_refs.count(key)) {
	  val = weak_refs[key].lock();
	  if (val) {
	    lru_add(key, val, &to_release);
	  } else {
	    retry = true;
	  }
	}
	if (retry)
	  cond.Wait(lock);
      } while (retry);
    }
    return val;
  }

  VPtr add(K key, V *value) {
    VPtr val(value, Cleanup(this, key));
    list<VPtr> to_release;
    {
      Mutex::Locker l(lock);
      weak_refs.insert(make_pair(key, val));
      lru_add(key, val, &to_release);
    }
    return val;
  }
};

#endif
