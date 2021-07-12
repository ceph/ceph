#include "TTLCache.h"

#include "PyUtil.h"
#include <chrono>
#include <functional>
#include <string>

template <class Key, class Value>
void TTLCacheBase<Key, Value>::insert(Key key, Value value) {
  auto now = std::chrono::steady_clock::now();

  if (!ttl)
    return;
  uint16_t ttl_spread = ttl * this->ttl_spread_percent;
  // in order not to have spikes of misses we increase or decrease by 25% of
  // the ttl
  uint16_t spreaded_ttl =
      ttl + (rand() / float(RAND_MAX) - .5) * 2 * ttl_spread;
  auto expiration_date = now + std::chrono::seconds(spreaded_ttl);
  cache::insert(key, {value, expiration_date});
}

template <class Key, class Value>
std::optional<Value> TTLCacheBase<Key, Value>::get(Key key) {
  std::optional<Value> value;
  if (!this->exists(key)) {
    return value;
  }
  if (expired(key)) {
    erase(key);
    return value;
  }
  value = {get_value(key)};
  return value;
}

template <class Key>
std::optional<PyObject *> TTLCache<Key, PyObject *>::get(Key key) {
  std::optional<PyObject *> value;
  if (!this->exists(key)) {
    return value;
  }
  if (this->expired(key)) {
    this->erase(key);
    return value;
  }
  PyObject *cached_value = this->get_value(key);
  Py_INCREF(cached_value);

  value = {cached_value};
  return value;
}

template <class Key, class Value>
void TTLCacheBase<Key, Value>::erase(Key key) {
  cache::erase(key);
}

template <class Key> void TTLCache<Key, PyObject *>::erase(Key key) {
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
  value_type stored_value = cache::get(key);
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
