#pragma once

#include <atomic>
#include <sstream>
#include <unordered_set>
#include <shared_mutex>
#include "PyUtil.h"
#include "common/config_obs.h"
#include "common/perf_counters.h"
#include "mgr/mgr_perf_counters.h"

template<class Key, class Value>
class LFUCache {
  struct Entry { Value val; size_t hits = 0;};
  std::atomic<uint64_t> hits{0}, misses{0};
  
  std::unordered_set<std::string> allowed_keys = {"osd_map", "pg_dump", "pg_stats",
    "mon_status", "mgr_map", "devices", "osd_metadata",
    "mds_metadata", "config", "foo"};
protected:
  std::unordered_map<Key, Entry> cache_data;
  const size_t capacity;
  std::atomic<bool> enabled{true};
  mutable std::shared_mutex m;

  void mark_miss() {
    misses++;
    if (perfcounter)
      perfcounter->inc(l_mgr_cache_miss);
  }
  void mark_hit() {
    hits++;
    if (perfcounter)
      perfcounter->inc(l_mgr_cache_hit);
  }

public:
  explicit LFUCache(size_t cap = UINT16_MAX, const bool ena = true) : capacity{cap}, enabled{ena} {}
  void set_enabled(bool e) { enabled.store(e); if (!e) { clear(); } }
  bool is_enabled() const noexcept { return enabled.load(); }
  int size() { return cache_data.size(); }
  bool is_cacheable(Key key) const noexcept { return allowed_keys.count(key) > 0; }
  bool can_read_cache(const Key &key) const noexcept { return is_enabled() && is_cacheable(key) && exists(key); }
  bool can_write_cache(const Key &key) const noexcept { return is_enabled() && is_cacheable(key); }

  bool try_get(const Key &k, Value* out, bool count_hit = true);

  void insert(const Key& key, Value value);

  bool erase(Key key) {
    std::shared_lock l(m);
    return cache_data.erase(key) > 0;
  }
  bool extract(const Key& k, Value* out) noexcept;
  void drain(std::vector<Value>& out) noexcept;

  void clear() { 
    std::shared_lock l(m);
    cache_data.clear();
    hits.store(0);
    misses.store(0);
  }
  bool exists(Key key) const noexcept {
    std::shared_lock l(m);
    return cache_data.find(key) != cache_data.end();
  }
  
  uint64_t get_hits() const { return hits.load(); }
  uint64_t get_misses() const { return misses.load(); }
};



// ---------- MgrMapCache generic ----------
template <class Key, class Value>
class MgrMapCache : public LFUCache<Key, Value>,
                   public md_config_obs_t {
  using CacheImp = LFUCache<Key, Value>;
public:
  explicit MgrMapCache(uint16_t sz=UINT16_MAX);
  ~MgrMapCache() { this->clear(); }
  bool try_get(const Key& k, Value* out, bool count_hit=true) noexcept { return CacheImp::try_get(k,out,count_hit); }
  void insert(const Key& k, Value v) { CacheImp::insert(k,std::move(v)); }
  bool extract(const Key& k, Value* out) noexcept { return CacheImp::extract(k,out); }
  void erase(const Key& k) noexcept { Value v{}; (void)CacheImp::extract(k,&v); }
  void clear() noexcept { CacheImp::clear(); }
private:
  std::vector<std::string> get_tracked_keys() const noexcept override { return {"mgr_map_cache_enabled"}; }
  void handle_conf_change(const ConfigProxy& conf, const std::set<std::string>& changed) override;
};

// ------- Full template specialization for PyObject*. with GIL rules ----------
template <class Key>
class MgrMapCache<Key, PyObject*> : public LFUCache<Key, PyObject*>,
                                    public md_config_obs_t {
  using CacheImp = LFUCache<Key, PyObject*>;
  static int pending_decref(void* p){ Py_DECREF(reinterpret_cast<PyObject*>(p)); return 0; }
public:
  MgrMapCache(uint16_t size = UINT16_MAX);
  ~MgrMapCache();
  PyObject* get(const Key &key);
  void erase(const Key &key)  noexcept;
  void clear() noexcept;
  void insert(const Key &key, PyObject* value);
  void invalidate(const Key &key) {
    if (CacheImp::exists(key))
      erase(key);
  }
private:
  std::vector<std::string> get_tracked_keys() const noexcept override { return {"mgr_map_cache_enabled"}; }
  void handle_conf_change(const ConfigProxy& conf, const std::set<std::string>& changed) override;
};
