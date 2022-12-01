#ifndef RGW_PERFCOUNTERS_CACHE_H
#define RGW_PERFCOUNTERS_CACHE_H

#include "common/perf_counters.h"
#include "common/ceph_context.h"

typedef std::list<std::string> labels_list;

struct CacheEntry {
  PerfCounters *counters;
  labels_list::iterator pos;

  CacheEntry(PerfCounters* _counters, labels_list::iterator _pos) {
    counters = _counters;
    pos = _pos;
  }

  ~CacheEntry() {}
};

class PerfCountersCache {

private:
  CephContext *cct;
  bool eviction;
  size_t curr_size = 0; 
  size_t target_size = 0; 
  int lower_bound = 0;
  int upper_bound = 0;
  std::function<void(PerfCountersBuilder*)> lpcb_init;

  std::unordered_map<std::string, CacheEntry*> cache;

  ceph::common::PerfCounters* base_counters;
  std::string base_counters_name;
  labels_list labels;

  // move recently updated items in the list to the front
  void update_labels_list(std::string label) {
    labels.erase(cache[label]->pos);
    cache[label]->pos = labels.insert(labels.begin(), label);
  }

  // removes least recently updated label from labels list
  // removes oldest label's CacheEntry from cache
  void remove_oldest_counters() {
    std::string removed_label = labels.back();
    labels.pop_back();

    ceph_assert(cache[removed_label]->counters);
    cct->get_perfcounters_collection()->remove(cache[removed_label]->counters);
    delete cache[removed_label]->counters;
    cache[removed_label]->counters = NULL;

    delete cache[removed_label];
    cache[removed_label] = NULL;

    cache.erase(removed_label);
    curr_size--;
  }

public:

  PerfCounters* get(std::string key) {
    auto got = cache.find(key);
    if(got != cache.end()) {
      return got->second->counters;
    }
    return NULL;
  }

  ceph::common::PerfCounters* add(std::string key, bool is_labeled = true) {
    auto counters = get(key);
    if (!counters) {
      // check to make sure cache isn't full
      if(eviction) {
        if(curr_size >= target_size) {
          remove_oldest_counters();
        }
      }

      // perf counters instance creation code
      auto lpcb = new PerfCountersBuilder(cct, key, lower_bound, upper_bound, is_labeled);
      lpcb_init(lpcb);

      // add counters to builder
      counters = lpcb->create_perf_counters();
      delete lpcb;

      // add new counters to collection, cache
      cct->get_perfcounters_collection()->add(counters);
      labels_list::iterator pos = labels.insert(labels.begin(), key);
      CacheEntry *m = new CacheEntry(counters, pos);
      cache[key] = m;
      curr_size++;
    }
    return counters;
  }

  void inc(std::string label, int indx, uint64_t v) {
    auto counters = get(label);
    if(counters) {
      counters->inc(indx, v);
      base_counters->inc(indx, v);
    }
  }

  void dec(std::string label, int indx, uint64_t v) {
    auto counters = get(label);
    if(counters) {
      counters->dec(indx, v);
      base_counters->dec(indx, v);
    }
  }

  void dec(std::string label, int indx, int64_t x, int64_t y) {
    auto counters = get(label);
    if(counters) {
      counters->hinc(indx, x, y);
      base_counters->hinc(indx, x, y);
    }
  }

  void tinc(std::string label, int indx, utime_t amt) {
    auto counters = get(label);
    if(counters) {
      counters->tinc(indx, amt);
      base_counters->tinc(indx, amt);
    }
  }

  void tinc(std::string label, int indx, ceph::timespan amt) {
    auto counters = get(label);
    if(counters) {
      counters->tinc(indx, amt);
      base_counters->tinc(indx, amt);
    }
  }

  void set_counter(std::string label, int indx, uint64_t val) {
    auto counters = get(label);
    if(counters) {
      uint64_t old_val = counters->get(indx);
      counters->set(indx, val);

      // apply new value to totals in base counters
      // need to make sure value of delta is positive since it is unsigned
      uint64_t delta = 0;
      if((old_val > val) && base_counters) {
        delta = old_val - val;
        base_counters->dec(indx, delta);
      } else if(base_counters) {
        delta = val - old_val;
        base_counters->inc(indx, delta);
      }
    }
  }

  uint64_t get_counter(std::string label, int indx) {
    auto counters = get(label);
    uint64_t val = 0;
    if(counters) {
      val = counters->get(indx);
    }
    return val;
  }

  utime_t tget(std::string label, int indx) {
    auto counters = get(label);
    utime_t val;
    if(counters) {
      val = counters->tget(indx);
      return val;
    } else {
      return utime_t();
    }
  }

  void tset(std::string label, int indx, utime_t amt) {
    auto counters = get(label);
    if(counters) {
      utime_t old_amt = counters->tget(indx);
      counters->tset(indx, amt);
      // TODO write a unit test for possible values of delta
      utime_t delta = old_amt - amt;
      base_counters->tinc(indx, delta);
    }
  }

  // for use right before destructor would get called
  void clear_cache() {
    for(auto it = cache.begin(); it != cache.end(); ++it ) {
      ceph_assert(it->second->counters);
      cct->get_perfcounters_collection()->remove(it->second->counters);
      delete it->second->counters;
      curr_size--;
    }
  }

  PerfCountersCache(CephContext *_cct, bool _eviction, size_t _target_size, int _lower_bound, int _upper_bound, 
      std::function<void(PerfCountersBuilder*)> _lpcb_init, std::string _base_counters_name) : cct(_cct), 
      eviction(_eviction), target_size(_target_size), lower_bound(_lower_bound), upper_bound(_upper_bound), 
      lpcb_init(_lpcb_init) {
      base_counters = add(_base_counters_name, false);
  }

  ~PerfCountersCache() {}

};

#endif
