// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <map>
#include <list>
#include <boost/smart_ptr/local_shared_ptr.hpp>
#include "common/ceph_mutex.h"
#include "common/dout.h"
#include "include/unordered_map.h"

#include "crimson/common/config_proxy.h"
#include "crimson/common/log.h"

// re-include our assert to clobber the system one; fix dout:
#include "include/ceph_assert.h"

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_);
  }
}

template <class K, class V>
class LRU {
  using VPtr = boost::local_shared_ptr<V>;
  using WeakVPtr = boost::weak_ptr<V>;
  size_t max_size;
  unsigned size;
private:
  using C = std::less<K>;
  using H = std::hash<K>;
  ceph::unordered_map<K, typename std::list<std::pair<K, VPtr> >::iterator, H> contents;
  std::list<std::pair<K, VPtr> > lru;

  std::map<K, std::pair<WeakVPtr, V*>, C> weak_refs;

  void trim_cache(std::list<VPtr> *to_release) {
    while (size > max_size) {
      to_release->push_back(lru.back().second);
      lru_remove(lru.back().first);
    }
  }

  void lru_remove(const K& key) {
    auto i = contents.find(key);
    if (i == contents.end())
      return;
    lru.erase(i->second);
    --size;
    contents.erase(i);
  }

  void lru_add(const K& key, const VPtr& val, std::list<VPtr> *to_release) {
    auto i = contents.find(key);
    if (i != contents.end()) {
      lru.splice(lru.begin(), lru, i->second);
    } else {
      ++size;
      lru.push_front(make_pair(key, val));
      contents[key] = lru.begin();
      trim_cache(to_release);
    }
  }

  void remove(const K& key, V *valptr) {
    auto i = weak_refs.find(key);
    if (i != weak_refs.end() && i->second.second == valptr) {
      weak_refs.erase(i);
    }
  }

  class Cleanup {
  public:
    LRU<K, V> *cache;
    K key;
    Cleanup(LRU<K, V> *cache, K key) : cache(cache), key(key) {}
    void operator()(V *ptr) {
      cache->remove(key, ptr);
      delete ptr;
    }
  };

public:
  LRU(size_t max_size = 20)
    : max_size(max_size),
      size(0) {
    contents.rehash(max_size);
  }

  ~LRU() {
    contents.clear();
    lru.clear();
    if (!weak_refs.empty()) {
      auto& conf = ceph::common::local_conf();
      std::ostringstream _dout;
      dump_weak_refs(_dout);
      logger().error("leaked refs:\n", _dout.str(), "\n");
      if (conf.get_val<bool>("debug_asserts_on_shutdown")) {
	ceph_assert(weak_refs.empty());
      }
    }
  }

  int get_count() {
    return size;
  }

  /// adjust container comparator (for purposes of get_next sort order)
  void reset_comparator(C comp) {
    // get_next uses weak_refs; that's the only container we need to
    // reorder.
    map<K, pair<WeakVPtr, V*>, C> temp;

    temp.swap(weak_refs);

    // reconstruct with new comparator
    weak_refs = map<K, pair<WeakVPtr, V*>, C>(comp);
    weak_refs.insert(temp.begin(), temp.end());
  }

  C get_comparator() {
    return weak_refs.key_comp();
  }

  void dump_weak_refs() {
    std::ostringstream _dout;
    dump_weak_refs(_dout);
    logger().error("leaked refs:\n",  _dout.str(), "\n");
  }

  void dump_weak_refs(std::ostream& out) {
    for (const auto& [key, ref] : weak_refs) {
      out << __func__ << " " << this << " weak_refs: "
	  << key << " = " << ref.second
	  << " with " << ref.first.use_count() << " refs"
	  << std::endl;
    }
  }

  //clear all strong reference from the lru.
  void clear() {
    while (true) {
      VPtr val; // release any ref we have after we drop the lock
      if (size == 0)
        break;

      val = lru.back().second;
      lru_remove(lru.back().first);
    }
  }

  void clear(const K& key) {
    VPtr val; // release any ref we have after we drop the lock
    {
      typename map<K, pair<WeakVPtr, V*>, C>::iterator i = weak_refs.find(key);
      if (i != weak_refs.end()) {
	val = i->second.first.lock();
      }
      lru_remove(key);
    }
  }

  void purge(const K &key) {
    VPtr val; // release any ref we have after we drop the lock
    {
      typename map<K, pair<WeakVPtr, V*>, C>::iterator i = weak_refs.find(key);
      if (i != weak_refs.end()) {
	val = i->second.first.lock();
        weak_refs.erase(i);
      }
      lru_remove(key);
    }
  }

  void set_size(size_t new_size) {
    list<VPtr> to_release;
    {
      max_size = new_size;
      trim_cache(&to_release);
    }
  }

  // Returns K key s.t. key <= k for all currently cached k,v
  K cached_key_lower_bound() {
    return weak_refs.begin()->first;
  }

  VPtr lower_bound(const K& key) {
    VPtr val;
    list<VPtr> to_release;

    if (!weak_refs.empty()) {
      auto i = weak_refs.lower_bound(key);
      if (i == weak_refs.end()) {
	--i;
      }
      val = i->second.first.lock();
      ceph_assert(val);
      lru_add(i->first, val, &to_release);
    }
    return val;
  }

  bool get_next(const K &key, std::pair<K, VPtr> *next) {
    VPtr next_val;

    typename std::map<K, std::pair<WeakVPtr, V*>, C>::iterator i = weak_refs.upper_bound(key);
    if (i == weak_refs.end())
      return false;

    next_val = i->second.first.lock();
    assert(next_val);
    if (next) {
      *next  = make_pair(i->first, next_val);
    }

    return true;
  }

  bool get_next(const K &key, std::pair<K, V> *next) {
    std::pair<K, VPtr> r;
    bool found = get_next(key, &r);
    if (!found || !next)
      return found;
    next->first = r.first;
    ceph_assert(r.second);
    next->second = *(r.second);
    return found;
  }

  VPtr lookup(const K& key) {
    VPtr val;
    std::list<VPtr> to_release;
    if (auto i = weak_refs.find(key); i != weak_refs.end()) {
      val = i->second.first.lock();
      ceph_assert(val);
      lru_add(key, val, &to_release);
    }
    return val;
  }

  VPtr lookup_or_create(const K &key) {
    VPtr val;
    list<VPtr> to_release;

    if (auto i = weak_refs.find(key); i != weak_refs.end()) {
      val = i->second.first.lock();
      ceph_assert(val);
    } else {
      val = VPtr{new V{}, Cleanup{this, key}};
      weak_refs.insert(make_pair(key, make_pair(val, val.get())));
    }
    lru_add(key, val, &to_release);

    return val;
  }

  /**
   * empty()
   *
   * Returns true iff there are no live references left to anything that has been
   * in the cache.
   */
  bool empty() {
    return weak_refs.empty();
  }

  /***
   * Inserts a key if not present, or bumps it to the front of the LRU if
   * it is, and then gives you a reference to the value. If the key already
   * existed, you are responsible for deleting the new value you tried to
   * insert.
   *
   * @param key The key to insert
   * @param value The value that goes with the key
   * @param existed Set to true if the value was already in the
   * map, false otherwise
   * @return A reference to the map's value for the given key
   */
  VPtr add(const K& key, V *value, bool *existed = NULL) {
    VPtr val;
    list<VPtr> to_release;

    if (auto i = weak_refs.find(key); i != weak_refs.end()) {
      if (existed) {
	*existed = true;
      }
      val = i->second.first.lock();
      ceph_assert(val);
    } else {
      if (existed) {
	*existed = false;
      }

      val = VPtr(value, Cleanup(this, key));
      weak_refs.insert(make_pair(key, make_pair(val, value)));
    }
    lru_add(key, val, &to_release);
    return val;
  }
};
