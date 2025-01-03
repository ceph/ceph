#include "TTLCache.h"

#include <chrono>
#include <functional>
#include <string>

#include "PyUtil.h"

template <class Key, class Value>
void TTLCacheBase<Key, Value>::insert(Key key, Value value) {
  auto now = std::chrono::steady_clock::now();

  if (!ttl) return;
  int16_t random_ttl_offset =
      ttl * ttl_spread_ratio * (2l * rand() / float(RAND_MAX) - 1);
  // in order not to have spikes of misses we increase or decrease by 25% of
  // the ttl
  int16_t spreaded_ttl = ttl + random_ttl_offset;
  auto expiration_date = now + std::chrono::seconds(spreaded_ttl);
  cache::insert(key, {value, expiration_date});
}

template <class Key, class Value> Value TTLCacheBase<Key, Value>::get(Key key) {
  if (!exists(key)) {
    throw_key_not_found(key);
  }
  if (expired(key)) {
    erase(key);
    throw_key_not_found(key);
  }
  Value value = {get_value(key)};
  return value;
}

template <class Key> PyObject* TTLCache<Key, PyObject*>::get(Key key) {
  if (!this->exists(key)) {
    this->throw_key_not_found(key);
  }
  if (this->expired(key)) {
    this->erase(key);
    this->throw_key_not_found(key);
  }
  PyObject* cached_value = this->get_value(key);
  Py_INCREF(cached_value);
  return cached_value;
}

template <class Key, class Value>
void TTLCacheBase<Key, Value>::erase(Key key) {
  cache::erase(key);
}

template <class Key> void TTLCache<Key, PyObject*>::erase(Key key) {
  Py_DECREF(this->get_value(key, false));
  ttl_base::erase(key);
}

template <class Key, class Value>
bool TTLCacheBase<Key, Value>::expired(Key key) {
  ttl_time_point expiration_date = get_value_time_point(key);
  auto now = std::chrono::steady_clock::now();
  if (now >= expiration_date) {
    return true;
  } else {
    return false;
  }
}

template <class Key, class Value> void TTLCacheBase<Key, Value>::clear() {
  cache::clear();
}

template <class Key, class Value>
Value TTLCacheBase<Key, Value>::get_value(Key key, bool count_hit) {
  value_type stored_value = cache::get(key, count_hit);
  Value value = std::get<0>(stored_value);
  return value;
}

template <class Key, class Value>
ttl_time_point TTLCacheBase<Key, Value>::get_value_time_point(Key key) {
  value_type stored_value = cache::get(key, false);
  ttl_time_point tp = std::get<1>(stored_value);
  return tp;
}

template <class Key, class Value>
void TTLCacheBase<Key, Value>::set_ttl(uint16_t ttl) {
  this->ttl = ttl;
}

template <class Key, class Value>
bool TTLCacheBase<Key, Value>::exists(Key key) {
  return cache::exists(key);
}

template <class Key, class Value>
void TTLCacheBase<Key, Value>::throw_key_not_found(Key key) {
  cache::throw_key_not_found(key);
}
