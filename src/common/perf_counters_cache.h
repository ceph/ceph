#ifndef RGW_PERFCOUNTERS_CACHE_H
#define RGW_PERFCOUNTERS_CACHE_H

#include "common/labeled_perf_counters.h"
#include "common/ceph_context.h"

class PerfCountersCache {

private:
  CephContext *cct;
  size_t curr_size = 0; 
  size_t target_size = 0; 
  int lower_bound = 0;
  int upper_bound = 0;
  std::function<void(ceph::common::LabeledPerfCountersBuilder*)> lpcb_init;

  std::unordered_map<std::string, ceph::common::LabeledPerfCounters*> cache;

  ceph::common::LabeledPerfCounters* base_counters;
  std::string base_counters_name;

public:

  ceph::common::LabeledPerfCounters* get(std::string key) {
    auto got = cache.find(key);
    if(got != cache.end()) {
      return got->second;
    }
    return NULL;
  }

  ceph::common::LabeledPerfCounters* add(std::string key) {
    auto labeled_counters = get(key);
    if (!labeled_counters) {
      // perf counters instance creation code
      if(curr_size < target_size) {
        auto lpcb = new ceph::common::LabeledPerfCountersBuilder(cct, key, lower_bound, upper_bound);
        lpcb_init(lpcb);

        // add counters to builder
        labeled_counters = lpcb->create_perf_counters();
        delete lpcb;

        // add new labeled counters to collection, cache
        cct->get_labeledperfcounters_collection()->add(labeled_counters);
        cache[key] = labeled_counters;
        curr_size++;
      }
    }
    return labeled_counters;
  }

  void inc(std::string label, int indx, uint64_t v) {
    auto labeled_counters = get(label);
    if(labeled_counters) {
      labeled_counters->inc(indx, v);
      base_counters->inc(indx, v);
    }
  }

  void dec(std::string label, int indx, uint64_t v) {
    auto labeled_counters = get(label);
    if(labeled_counters) {
      labeled_counters->dec(indx, v);
      base_counters->dec(indx, v);
    }
  }

  void set_counter(std::string label, int indx, uint64_t val) {
    auto labeled_counters = get(label);
    if(labeled_counters) {
      uint64_t old_val = labeled_counters->get(indx);
      labeled_counters->set(indx, val);

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
    auto labeled_counters = get(label);
    uint64_t val = 0;
    if(labeled_counters) {
      val = labeled_counters->get(indx);
    }
    return val;
  }

  // for use right before destructor would get called
  void clear_cache() {
    for(auto it = cache.begin(); it != cache.end(); ++it ) {
      ceph_assert(it->second);
      cct->get_labeledperfcounters_collection()->remove(it->second);
      delete it->second;
      curr_size--;
    }
  }

  PerfCountersCache(CephContext *_cct, size_t _target_size, int _lower_bound, int _upper_bound, 
      std::function<void(ceph::common::LabeledPerfCountersBuilder*)> _lpcb_init, std::string _base_counters_name) : cct(_cct), 
      target_size(_target_size), lower_bound(_lower_bound), upper_bound(_upper_bound), 
      lpcb_init(_lpcb_init), base_counters_name(_base_counters_name) {
      base_counters = add(base_counters_name);
  }

  ~PerfCountersCache() {}

};

#endif
