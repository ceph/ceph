#pragma once

#include <Python.h>
#include <chrono>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <string>

using namespace std;

template <class Key, class Value> class Cache {
private:
  uint64_t hits = 0;
  uint64_t misses = 0;
  std::mutex hit_mutex, miss_mutex;

protected:
  unsigned int capacity;
  Cache(unsigned int size = UINT16_MAX) : capacity{size} {};
  std::map<Key, Value> content;

  void miss() {
    miss_mutex.lock();
    misses++;
    miss_mutex.unlock();
  }
  void hit() {
    hit_mutex.lock();
    hits++;
    hit_mutex.unlock();
  }

  unsigned int get_misses() { return misses; }
  unsigned int get_hits() { return hits; }

public:
  void insert(Key key, Value value) {
    if (content.size() < capacity) {
      content.insert({key, value});
    }
  }
  Value get(Key key) { return content[key]; }
  void erase(Key key) { content.erase(content.find(key)); }
  void clear() { content.clear(); }
  bool exists(Key key) { return content.find(key) != content.end(); }
  std::pair<uint64_t, uint64_t> get_hit_miss_ratio() {
    return std::make_pair(hits, misses);
  }
  ~Cache(){};
};

typedef std::chrono::time_point<std::chrono::steady_clock> ttl_time_point;
template <class Key, class Value>
class TTLCache
    : public Cache<Key, std::pair<Value, ttl_time_point>> {
private:
  uint16_t ttl;
  typedef std::pair<Value, ttl_time_point> value_type;

  Value get_value(Key key);
  ttl_time_point get_value_time_point(Key key);
  bool exists(Key key);
  bool expired(Key key);
  void update_ttl();

public:
  TTLCache(uint16_t ttl_ = 0, uint16_t size = UINT16_MAX)
      : Cache<Key, value_type>(size), ttl{ttl_} {
  }
  ~TTLCache(){};
  void insert(Key key, Value value);
  Value get(Key key);
  void erase(Key key);
  void clear();
  void set_ttl(uint16_t ttl);
};

template <class Key, class Value>
void TTLCache<Key, Value>::insert(Key key, Value value) {
  auto now = std::chrono::steady_clock::now();

  update_ttl();
  if(!ttl) return;
  uint16_t ttl_spread = ttl * 0.25;
  // in order not to have spikes of misses we increase or decrease by 25% of
  // the ttl
  uint16_t spreaded_ttl = ttl + (rand() % (ttl_spread * 2) + (-ttl_spread));
  auto expiration_date = now + std::chrono::seconds(spreaded_ttl);
  this->Cache<Key, value_type>::miss();
  this->Cache<Key, value_type>::insert(key, {value, expiration_date});
}

template <class Key, class Value>
void TTLCache<Key, Value>::update_ttl() {
	ttl = g_conf().get_val<int64_t>("mgr_ttl_cache_expire_seconds");
}

template <class Key, class Value> Value TTLCache<Key, Value>::get(Key key) {
  if (!this->exists(key)) {
    return nullptr;
  }
  if (expired(key)) {
             << Py_REFCNT(get_value(key)) << "\n";
    erase(key);
    return nullptr;
  }
  if (PyObject *obj = dynamic_cast<PyObject *>(get_value(key))) {
    Py_INCREF(obj);
  }
  this->Cache<Key, value_type>::hit();
  return get_value(key);
}

template <class Key, class Value> void TTLCache<Key, Value>::erase(Key key) {
  if (PyObject *obj = dynamic_cast<PyObject *>(get_value(key))) {
    Py_DECREF(obj);
  }
  this->Cache<Key, value_type>::erase(key);
}

template <class Key, class Value> bool TTLCache<Key, Value>::expired(Key key) {
  ttl_time_point expiration_date = get_value_time_point(key);
  auto now = std::chrono::steady_clock::now();
  if (now >= expiration_date) {
    return true;
  } else {
    return false;
  }
}

template <class Key, class Value> void TTLCache<Key, Value>::clear() {
  this->Cache<Key, value_type>::clear();
}

template <class Key, class Value>
Value TTLCache<Key, Value>::get_value(Key key) {
  value_type stored_value = this->Cache<Key, value_type>::get(key);
  Value value = std::get<0>(stored_value);
  return value;
}

template <class Key, class Value>
ttl_time_point TTLCache<Key, Value>::get_value_time_point(Key key) {
  value_type stored_value = this->Cache<Key, value_type>::get(key);
  ttl_time_point tp = std::get<1>(stored_value);
  return tp;
}

template <class Key, class Value>
void TTLCache<Key, Value>::set_ttl(uint16_t ttl) {
  this->ttl = ttl;
}

template <class Key, class Value> bool TTLCache<Key, Value>::exists(Key key) {
  return this->Cache<Key, value_type>::exists(key);
}

