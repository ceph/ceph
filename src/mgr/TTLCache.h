#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "PyUtil.h"

using namespace std;

template <class Key, class Value> class Cache {
 private:
  std::atomic<uint64_t> hits, misses;

 protected:
  unsigned int capacity;
  Cache(unsigned int size = UINT16_MAX) : hits{0}, misses{0}, capacity{size} {};
  std::map<Key, Value> content;
  std::vector<string> allowed_keys = {"osd_map", "pg_dump", "pg_stats"};

  void mark_miss() {
    misses++;
  }

  void mark_hit() {
    hits++;
  }

  unsigned int get_misses() { return misses; }
  unsigned int get_hits() { return hits; }
  void throw_key_not_found(Key key) {
    std::stringstream ss;
    ss << "Key " << key << " couldn't be found\n";
    throw std::out_of_range(ss.str());
  }

 public:
  void insert(Key key, Value value) {
    mark_miss();
    if (content.size() < capacity) {
      content.insert({key, value});
    }
  }
  Value get(Key key, bool count_hit = true) {
    if (count_hit) {
      mark_hit();
    }
    return content[key];
  }
  void erase(Key key) { content.erase(content.find(key)); }
  void clear() { content.clear(); }
  bool exists(Key key) { return content.find(key) != content.end(); }
  std::pair<uint64_t, uint64_t> get_hit_miss_ratio() {
    return std::make_pair(hits.load(), misses.load());
  }
  bool is_cacheable(Key key) {
    for (auto k : allowed_keys) {
      if (key == k) return true;
    }
    return false;
  }
  int size() { return content.size(); }

  ~Cache(){};
};

using ttl_time_point = std::chrono::time_point<std::chrono::steady_clock>;
template <class Key, class Value>
class TTLCacheBase : public Cache<Key, std::pair<Value, ttl_time_point>> {
 private:
  uint16_t ttl;
  float ttl_spread_ratio;
  using value_type = std::pair<Value, ttl_time_point>;
  using cache = Cache<Key, value_type>;

 protected:
  Value get_value(Key key, bool count_hit = true);
  ttl_time_point get_value_time_point(Key key);
  bool exists(Key key);
  bool expired(Key key);
  void finish_get(Key key);
  void finish_erase(Key key);
  void throw_key_not_found(Key key);

 public:
  TTLCacheBase(uint16_t ttl_ = 0, uint16_t size = UINT16_MAX,
               float spread = 0.25)
      : Cache<Key, value_type>(size), ttl{ttl_}, ttl_spread_ratio{spread} {}
  ~TTLCacheBase(){};
  void insert(Key key, Value value);
  Value get(Key key);
  void erase(Key key);
  void clear();
  uint16_t get_ttl() { return ttl; };
  void set_ttl(uint16_t ttl);
};

template <class Key, class Value>
class TTLCache : public TTLCacheBase<Key, Value> {
 public:
  TTLCache(uint16_t ttl_ = 0, uint16_t size = UINT16_MAX, float spread = 0.25)
      : TTLCacheBase<Key, Value>(ttl_, size, spread) {}
  ~TTLCache(){};
};

template <class Key>
class TTLCache<Key, PyObject*> : public TTLCacheBase<Key, PyObject*> {
 public:
  TTLCache(uint16_t ttl_ = 0, uint16_t size = UINT16_MAX, float spread = 0.25)
      : TTLCacheBase<Key, PyObject*>(ttl_, size, spread) {}
  ~TTLCache(){};
  PyObject* get(Key key);
  void erase(Key key);

 private:
  using ttl_base = TTLCacheBase<Key, PyObject*>;
};

#include "TTLCache.cc"

