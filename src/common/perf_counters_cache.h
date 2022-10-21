#ifndef RGW_PERFCOUNTERS_CACHE_H
#define RGW_PERFCOUNTERS_CACHE_H

#include "common/labeled_perf_counters.h"
#include "common/ceph_context.h"

class PerfCountersCache {

private:
  CephContext *cct;
  size_t curr_size = 0; 
  size_t target_size = 0; 
  ceph::common::LabeledPerfCountersBuilder *default_lplb;
  std::unordered_map<std::string, ceph::common::LabeledPerfCounters*> cache;

public:

  ceph::common::LabeledPerfCounters* get(std::string key) {
    auto got = cache.find(key);
    if(got != cache.end()) {
      return got->second;
    }
    return NULL;
  }

  ceph::common::LabeledPerfCounters* add(std::string key, ceph::common::LabeledPerfCountersBuilder *lplb = NULL) {
  //void add(std::string key) {
    auto labeled_counters = get(key);
    if (!labeled_counters) {
      if(curr_size < target_size) {
        // perf counters instance creation code
        if(lplb) {
          labeled_counters = lplb->create_perf_counters();
        } else {
          labeled_counters = default_lplb->create_perf_counters();
        }
        labeled_counters->set_name(key);
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
    }
  }

  void dec(std::string label, int indx, uint64_t v) {
    auto labeled_counters = get(label);
    if(labeled_counters) {
      labeled_counters->inc(indx, v);
    }
  }

  void set_counter(std::string label, int indx, uint64_t val) {
    auto labeled_counters = get(label);
    if(labeled_counters) {
      labeled_counters->set(indx, val);
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

  PerfCountersCache(CephContext *_cct, size_t _target_size, ceph::common::LabeledPerfCountersBuilder *_lplb) : cct(_cct), target_size(_target_size), default_lplb(_lplb) {}

  ~PerfCountersCache() {
    delete default_lplb;
    default_lplb = NULL;
  }

};

#endif
