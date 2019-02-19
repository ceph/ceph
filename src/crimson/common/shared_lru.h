// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>
#include <optional>
#include <boost/smart_ptr/local_shared_ptr.hpp>
#include <boost/smart_ptr/weak_ptr.hpp>
#include "simple_lru.h"

/// SharedLRU does its best to cache objects. It not only tracks the objects
/// in its LRU cache with strong references, it also tracks objects with
/// weak_ptr even if the cache does not hold any strong references to them. so
/// that it can return the objects after they are evicted, as long as they've
/// ever been cached and have not been destroyed yet.
template<class K, class V>
class SharedLRU {
  using shared_ptr_t = boost::local_shared_ptr<V>;
  using weak_ptr_t = boost::weak_ptr<V>;
  using value_type = std::pair<K, shared_ptr_t>;

  // weak_refs is already ordered, and we don't use accessors like
  // LRUCache::lower_bound(), so unordered LRUCache would suffice.
  SimpleLRU<K, shared_ptr_t, false> cache;
  std::map<K, std::pair<weak_ptr_t, V*>> weak_refs;

  struct Deleter {
    SharedLRU<K,V>* cache;
    const K key;
    void operator()(V* ptr) {
      cache->_erase(key);
      delete ptr;
    }
  };
  void _erase(const K& key) {
    weak_refs.erase(key);
  }
public:
  SharedLRU(size_t max_size = 20)
    : cache{max_size}
  {}
  ~SharedLRU() {
    cache.clear();
    // use plain assert() in utiliy classes to avoid dependencies on logging
    assert(weak_refs.empty());
  }
  /**
   * Returns a reference to the given key, and perform an insertion if such
   * key does not already exist
   */
  shared_ptr_t operator[](const K& key);
  /**
   * Returns true iff there are no live references left to anything that has been
   * in the cache.
   */
  bool empty() const {
    return weak_refs.empty();
  }
  size_t size() const {
    return cache.size();
  }
  size_t capacity() const {
    return cache.capacity();
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
  shared_ptr_t insert(const K& key, std::unique_ptr<V> value);
  // clear all strong reference from the lru.
  void clear() {
    cache.clear();
  }
  shared_ptr_t find(const K& key);
  // return the last element that is not greater than key
  shared_ptr_t lower_bound(const K& key);
  // return the first element that is greater than key
  std::optional<value_type> upper_bound(const K& key);
};

template<class K, class V>
typename SharedLRU<K,V>::shared_ptr_t
SharedLRU<K,V>::insert(const K& key, std::unique_ptr<V> value)
{
  shared_ptr_t val;
  if (auto found = weak_refs.find(key); found != weak_refs.end()) {
    val = found->second.first.lock();
  }
  if (!val) {
    val.reset(value.release(), Deleter{this, key});
    weak_refs.emplace(key, std::make_pair(val, val.get()));
  }
  cache.insert(key, val);
  return val;
}

template<class K, class V>
typename SharedLRU<K,V>::shared_ptr_t
SharedLRU<K,V>::operator[](const K& key)
{
  if (auto found = cache.find(key); found) {
    return *found;
  }
  shared_ptr_t val;
  if (auto found = weak_refs.find(key); found != weak_refs.end()) {
    val = found->second.first.lock();
  }
  if (!val) {
    val.reset(new V{}, Deleter{this, key});
    weak_refs.emplace(key, std::make_pair(val, val.get()));
  }
  cache.insert(key, val);
  return val;
}

template<class K, class V>
typename SharedLRU<K,V>::shared_ptr_t
SharedLRU<K,V>::find(const K& key)
{
  if (auto found = cache.find(key); found) {
    return *found;
  }
  shared_ptr_t val;
  if (auto found = weak_refs.find(key); found != weak_refs.end()) {
    val = found->second.first.lock();
  }
  if (val) {
    cache.insert(key, val);
  }
  return val;
}

template<class K, class V>
typename SharedLRU<K,V>::shared_ptr_t
SharedLRU<K,V>::lower_bound(const K& key)
{
  if (weak_refs.empty()) {
    return {};
  }
  auto found = weak_refs.lower_bound(key);
  if (found == weak_refs.end()) {
    --found;
  }
  if (auto val = found->second.first.lock(); val) {
    cache.insert(key, val);
    return val;
  } else {
    return {};
  }
}

template<class K, class V>
std::optional<typename SharedLRU<K,V>::value_type>
SharedLRU<K,V>::upper_bound(const K& key)
{
  for (auto found = weak_refs.upper_bound(key);
       found != weak_refs.end();
       ++found) {
    if (auto val = found->second.first.lock(); val) {
      return std::make_pair(found->first, val);
    }
  }
  return std::nullopt;
}
