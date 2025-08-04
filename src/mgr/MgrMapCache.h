#pragma once

#include <atomic>
#include <map>
#include <sstream>
#include <unordered_set>
#include <shared_mutex>

#include "PyUtil.h"
#include "common/config_obs.h"
#include "common/perf_counters.h"
#include "mgr/mgr_perf_counters.h"

template<class Key, class Value>
class LFUCache {
  struct Entry {
    Value val;
    size_t hits = 0;
  };
  std::atomic<uint64_t> hits{0}, misses{0};
  std::unordered_set<std::string> allowed_keys = {"osd_map", "pg_dump", "pg_stats",
    "mon_status", "mgr_map", "devices", "osd_metadata",
    "mds_metadata", "config", "foo"};
protected:
  std::unordered_map<Key, Entry> cache_data;
  const size_t capacity;
  std::atomic<bool> enabled;
  mutable std::shared_mutex cache_mutex;

  void mark_miss() { misses++; perfcounter->inc(l_mgr_cache_miss); }
  void mark_hit() { hits++; perfcounter->inc(l_mgr_cache_hit); }

  void throw_key_not_found(Key key) {
    mark_miss();
    std::stringstream ss;
    ss << "Key " << key << " couldn't be found\n";
    throw std::out_of_range(ss.str());
  }
public:
  LFUCache(size_t cap = UINT16_MAX, const bool ena = true)
    : capacity{cap}, enabled{ena} {}
  ~LFUCache() = default;
  void set_enabled(bool e) {
    enabled.store(e);
    if (!e) {
      clear();
    }
  }
  bool is_enabled() const noexcept {
    return enabled.load();
  }
  void insert(const Key& key, Value value) {
    if (!can_write_cache(key)) {
      return;
    }
    std::unique_lock<std::shared_mutex> l(cache_mutex);
    auto it = cache_data.find(key);
    if (it != cache_data.end()) {
      // overwrite existing but keep hits
      it->second.val = std::move(value);
      return;
    }

    // at capacity? evict the lowest‐hit key
    if (cache_data.size() >= capacity) {
      auto min_it = std::min_element(cache_data.begin(), cache_data.end(),
        [](const auto& a, const auto& b) { return a.second.hits < b.second.hits; });
      cache_data.erase(min_it);
    }
    
    mark_miss();
    cache_data.emplace(key, Entry{std::move(value), 0});
  }

  Value get(const Key& key, bool count_hit = true) {
    if (!this->is_enabled() && !this->is_cacheable(key)) {
      return Value();
    }
    
    std::shared_lock<std::shared_mutex> l(cache_mutex);
    auto it = cache_data.find(key);
    if (it == cache_data.end())
      throw_key_not_found(key);

    if (count_hit) {
      it->second.hits++;
      mark_hit();
    }

    return it->second.val;
  }

  void erase(Key key) {
    if (!can_read_cache(key)) {
      return;
    }
    std::unique_lock<std::shared_mutex> l(cache_mutex);
    cache_data.erase(key); 
  }
  void clear() { 
    std::unique_lock<std::shared_mutex> l(cache_mutex);
    cache_data.clear();
    hits.store(0);
    misses.store(0);
  }
  bool exists(Key key) const noexcept {
    std::shared_lock<std::shared_mutex> l(cache_mutex);
    return cache_data.find(key) != cache_data.end();
  }
  int size() { return cache_data.size(); }
  bool is_cacheable(Key key) const noexcept { return allowed_keys.count(key) > 0; }

  bool can_read_cache(const Key &key) const noexcept {
    return is_enabled() && is_cacheable(key) && exists(key);
  }

  bool can_write_cache(const Key &key) const noexcept {
    return is_enabled() && is_cacheable(key);
  }
  uint64_t get_hits() const { return hits.load(); }
  uint64_t get_misses() const { return misses.load(); }
};

template <class Key, class Value>
class MgrMapCache : public LFUCache<Key, Value>,
                   public md_config_obs_t {
  using CacheImp = LFUCache<Key, Value>;
private:
  std::vector<std::string> get_tracked_keys() const noexcept override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string> &changed) override;
public:
  MgrMapCache(uint16_t size = UINT16_MAX);
  ~MgrMapCache();
  Value get(const Key &key, bool count_hit = true);
  void erase(const Key &key);
  void clear();
  void insert(const Key &key, Value value);
};

// Full template specialization for PyObject*.
template <class Key>
class MgrMapCache<Key, PyObject*> : public LFUCache<Key, PyObject*>,
                                    public md_config_obs_t {
  using CacheImp = LFUCache<Key, PyObject*>;
public:
  MgrMapCache(uint16_t size = UINT16_MAX);
  ~MgrMapCache();
  PyObject* get(const Key &key);
  void erase(const Key &key);
  void clear();
  void insert(const Key &key, PyObject* value);
  void invalidate(const Key &key) {
    erase(key);
  }
private:
  std::mutex lock;
  std::vector<std::string> get_tracked_keys() const noexcept override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string> &changed) override;
};
