#pragma once

#include <boost/optional.hpp>
#include <chrono>
#include <functional>
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
  int size() { return content.size(); }
  ~Cache(){};
};

typedef std::chrono::time_point<std::chrono::steady_clock> ttl_time_point;
template <class Key, class Value>
class TTLCache : public Cache<Key, std::pair<Value, ttl_time_point>> {
private:
  uint16_t ttl;
  typedef std::pair<Value, ttl_time_point> value_type;

  Value get_value(Key key);
  ttl_time_point get_value_time_point(Key key);
  bool exists(Key key);
  bool expired(Key key);
  std::function<void(Value)> erase_callback = nullptr;

public:
  TTLCache(uint16_t ttl_ = 0, uint16_t size = UINT16_MAX)
      : Cache<Key, value_type>(size), ttl{ttl_} {

  }
  ~TTLCache(){};
  void insert(Key key, Value value);
  boost::optional<Value> get(Key key);
  void erase(Key key);
  void clear();
  void set_ttl(uint16_t ttl);
  void set_erase_callback(std::function<void(Value)> cb) { erase_callback = cb; }
};

template <class Key, class Value>
void TTLCache<Key, Value>::insert(Key key, Value value) {
  auto now = std::chrono::steady_clock::now();

  if (!ttl)
    return;
  uint16_t ttl_spread = ttl * 0.25;
  // in order not to have spikes of misses we increase or decrease by 25% of
  // the ttl
  uint16_t spreaded_ttl = ttl + (rand() % (ttl_spread * 2) + (-ttl_spread));
  auto expiration_date = now + std::chrono::seconds(spreaded_ttl);
  this->Cache<Key, value_type>::miss();
  this->Cache<Key, value_type>::insert(key, {value, expiration_date});
}

template <class Key, class Value>
boost::optional<Value> TTLCache<Key, Value>::get(Key key) {
  boost::optional<Value> value;
  if (!this->exists(key)) {
    return value;
  }
  if (expired(key)) {
    erase(key);
    return value;
  }
  this->Cache<Key, value_type>::hit();
  value = {get_value(key)};
  return value;
}

template <class Key, class Value> void TTLCache<Key, Value>::erase(Key key) {
  if (erase_callback) {
    erase_callback(get_value(key));
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

