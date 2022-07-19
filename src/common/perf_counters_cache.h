#ifndef RGW_PERFCOUNTERS_CACHE_H
#define RGW_PERFCOUNTERS_CACHE_H

#include "common/intrusive_lru.h"
#include "common/perf_counters.h"
#include "common/ceph_context.h"

enum {
  l_rgw_metrics_first = 15050,
  l_rgw_metrics_req,
  l_rgw_metrics_failed_req,
  l_rgw_metrics_put_b,
  l_rgw_metrics_get_b,
  l_rgw_metrics_last,
};

template <typename LRUItem>
struct item_to_key {
  using type = std::string;
  const type &operator()(const LRUItem &item) {
    return item.instance_labels;
  }
};

struct PerfCountersCacheEntry : public ceph::common::intrusive_lru_base<
  ceph::common::intrusive_lru_config<
    std::string, PerfCountersCacheEntry, item_to_key<PerfCountersCacheEntry>>> {
  std::string instance_labels;
  PerfCounters *perfcounters_instance = NULL;
  //CephContext *cct = NULL;
  //PerfCountersCollection *collection = NULL;

  PerfCountersCacheEntry(std::string key) : instance_labels(key) {}

  ~PerfCountersCacheEntry() {
    // perf counters instance clean up code
    if(perfcounters_instance) {
      // TODO: figure out removal from perfcounters_collection
      //ceph_assert(perfcounters_instance);
      //collection->remove(perfcounters_instance);
      //cct->get_perfcounters_collection()->remove(perfcounters_instance);
      //delete perfcounters_instance;
      //perfcounters_instance = NULL;
    }
  }
};

class PerfCountersCache : public PerfCountersCacheEntry::lru_t {
private:
  CephContext *cct;
public:
  void add(std::string key) {
    auto [ref, key_existed] = get_or_create(key);
    if (!key_existed) {
      // perf counters instance creation code
      PerfCountersBuilder plb(cct, key, l_rgw_metrics_first, l_rgw_metrics_last);
      plb.add_u64_counter(l_rgw_metrics_req, "req", "number of reqs");
      plb.add_u64_counter(l_rgw_metrics_failed_req, "failed_req", "Aborted Requests");
      plb.add_u64_counter(l_rgw_metrics_put_b, "put_b", "Size of puts");
      plb.add_u64_counter(l_rgw_metrics_get_b, "get_b", "Size of gets");

      PerfCounters *counters = plb.create_perf_counters();
      cct->get_perfcounters_collection()->add(counters);
      ref->perfcounters_instance = counters;
      //ref->collection = cct->get_perfcounters_collection();
      //ref->collection->add(counters);
      //ref->cct = cct;
    }
  }

  void inc(std::string label, int indx, uint64_t v) {
    auto ref = get(label);
    if(ref) {
      if(ref->perfcounters_instance) {
        PerfCounters *counters = ref->perfcounters_instance;
        counters->inc(indx, v);
      }
    }
  }

  void dec(std::string label, int indx, uint64_t v) {
    auto ref = get(label);
    if(ref) {
      if(ref->perfcounters_instance) {
        PerfCounters *counters = ref->perfcounters_instance;
        counters->dec(indx, v);
      }
    }
  }

  void set_counter(std::string label, int indx, uint64_t val) {
    auto ref = get(label);
    if(ref) {
      if(ref->perfcounters_instance) {
        PerfCounters *counters = ref->perfcounters_instance;
        counters->set(indx, val);
      }
    }
  }

  uint64_t get_counter(std::string label, int indx) {
    auto ref = get(label);
    uint64_t val= 0;
    if(ref) {
      if(ref->perfcounters_instance) {
        PerfCounters *counters = ref->perfcounters_instance;
        val = counters->get(indx);
      }
    }
    return val;
  }

  PerfCountersCache(CephContext *_cct, size_t _cache_size) {
    cct = _cct;
    set_target_size(_cache_size);
  }

  ~PerfCountersCache() {}
};

#endif
