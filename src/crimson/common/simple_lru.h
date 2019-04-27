// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <list>
#include <map>
#include <optional>
#include <type_traits>
#include <unordered_map>

template <class Key, class Value, bool Ordered>
class SimpleLRU {
  static_assert(std::is_default_constructible_v<Value>);
  using list_type = std::list<Key>;
  template<class K, class V>
  using map_t = std::conditional_t<Ordered,
				   std::map<K, V>,
				   std::unordered_map<K, V>>;
  using map_type = map_t<Key, std::pair<Value, typename list_type::iterator>>;
  list_type lru;
  map_type cache;
  const size_t max_size;

public:
  SimpleLRU(size_t size = 20)
    : cache(size),
      max_size(size)
  {}
  size_t size() const {
    return cache.size();
  }
  size_t capacity() const {
    return max_size;
  }
  using insert_return_type = std::pair<Value, bool>;
  insert_return_type insert(const Key& key, Value value);
  std::optional<Value> find(const Key& key);
  std::optional<std::enable_if<Ordered, Value>> lower_bound(const Key& key);
  void erase(const Key& key);
  void clear();
private:
  // bump the item to the front of the lru list
  Value _lru_add(typename map_type::iterator found);
  // evict the last element of most recently used list
  void _evict();
};

template <class Key, class Value, bool Ordered>
typename SimpleLRU<Key,Value,Ordered>::insert_return_type
SimpleLRU<Key,Value,Ordered>::insert(const Key& key, Value value)
{
  if constexpr(Ordered) {
    auto found = cache.lower_bound(key);
    if (found != cache.end() && found->first == key) {
      // already exists
      return {found->second.first, true};
    } else {
      if (size() >= capacity()) {
        _evict();
      }
      lru.push_front(key);
      // use lower_bound as hint to save the lookup
      cache.emplace_hint(found, key, std::make_pair(value, lru.begin()));
      return {std::move(value), false};
    }
  } else {
    // cache is not ordered
    auto found = cache.find(key);
    if (found != cache.end()) {
      // already exists
      return {found->second.first, true};
    } else {
      if (size() >= capacity()) {
	_evict();
      }
      lru.push_front(key);
      cache.emplace(key, std::make_pair(value, lru.begin()));
      return {std::move(value), false};
    }
  }
}

template <class Key, class Value, bool Ordered>
std::optional<Value> SimpleLRU<Key,Value,Ordered>::find(const Key& key)
{
  if (auto found = cache.find(key); found != cache.end()){
    return _lru_add(found);
  } else {
    return {};
  }
}

template <class Key, class Value, bool Ordered>
std::optional<std::enable_if<Ordered, Value>>
SimpleLRU<Key,Value,Ordered>::lower_bound(const Key& key)
{
  if (auto found = cache.lower_bound(key); found != cache.end()) {
    return _lru_add(found);
  } else {
    return {};
  }
}

template <class Key, class Value, bool Ordered>
void SimpleLRU<Key,Value,Ordered>::clear()
{
  lru.clear();
  cache.clear();
}

template <class Key, class Value, bool Ordered>
void SimpleLRU<Key,Value,Ordered>::erase(const Key& key)
{
  if (auto found = cache.find(key); found != cache.end()) {
    lru.erase(found->second->second);
    cache.erase(found);
  }
}

template <class Key, class Value, bool Ordered>
Value SimpleLRU<Key,Value,Ordered>::_lru_add(
  typename SimpleLRU<Key,Value,Ordered>::map_type::iterator found)
{
  auto& [value, in_lru] = found->second;
  if (in_lru != lru.begin()){
    // move item to the front
    lru.splice(lru.begin(), lru, in_lru);
  }
  // the item is already at the front
  return value;
}

template <class Key, class Value, bool Ordered>
void SimpleLRU<Key,Value,Ordered>::_evict()
{
  // evict the last element of most recently used list
  auto last = --lru.end();
  cache.erase(*last);
  lru.erase(last);
}
