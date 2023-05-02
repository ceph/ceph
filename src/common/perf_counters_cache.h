#pragma once

#include "common/perf_counters.h"
#include "common/ceph_context.h"
#include "common/intrusive_lru.h"

namespace ceph::perf_counters {

struct perf_counters_cache_item_to_key;

struct PerfCountersCacheEntry : public ceph::common::intrusive_lru_base<
                                       ceph::common::intrusive_lru_config<
                                       std::string, PerfCountersCacheEntry, perf_counters_cache_item_to_key>> {
  std::string key;
  std::shared_ptr<PerfCounters> counters;
  CephContext *cct;

  PerfCountersCacheEntry(const std::string &_key) : key(_key) {}

  ~PerfCountersCacheEntry() {
    if (counters) {
      cct->get_perfcounters_collection()->remove(counters.get());
    }
  }
};

struct perf_counters_cache_item_to_key {
  using type = std::string;
  const type &operator()(const PerfCountersCacheEntry &entry) {
    return entry.key;
  }
};

class PerfCountersCache {
private:
  CephContext *cct;
  std::function<std::shared_ptr<PerfCounters>(const std::string&, CephContext*)> create_counters;
  PerfCountersCacheEntry::lru_t cache;
  mutable ceph::mutex m_lock;

  /* check to make sure key name is non-empty and non-empty labels
   *
   * A valid key has the the form 
   * key\0label1\0val1\0label2\0val2 ... label\0valN
   * The following 3 properties checked for in this function
   * 1. A non-empty key
   * 2. At least 1 set of labels
   * 3. Each label has a non-empty key and value
   *
   * See perf_counters_key.h
   */
  void check_key(const std::string &key);

  // adds a new entry to the cache and returns its respective PerfCounter*
  // or returns the PerfCounter* of an existing entry in the cache
  std::shared_ptr<PerfCounters> add(const std::string &key);

public:

  // get() and its associated shared_ptr reference counting should be avoided 
  // unless the caller intends to modify multiple counter values at the same time.
  // If multiple counter values will not be modified at the same time, inc/dec/etc. 
  // are recommended.
  std::shared_ptr<PerfCounters> get(const std::string &key);

  void inc(const std::string &key, int indx, uint64_t v);
  void dec(const std::string &key, int indx, uint64_t v);
  void tinc(const std::string &key, int indx, utime_t amt);
  void tinc(const std::string &key, int indx, ceph::timespan amt);
  void set_counter(const std::string &key, int indx, uint64_t val);
  uint64_t get_counter(const std::string &key, int indx);
  utime_t tget(const std::string &key, int indx);
  void tset(const std::string &key, int indx, utime_t amt);

  // _create_counters should be a function that returns a valid, newly created perf counters instance
  // Ceph components utilizing the PerfCountersCache are encouraged to pass in a factory function that would
  // create and initialize different kinds of counters based on the name returned from ceph::perfcounters::key_name(key)
  PerfCountersCache(CephContext *_cct, size_t _target_size,
                    std::function<std::shared_ptr<PerfCounters>(const std::string&, CephContext*)> _create_counters);
  ~PerfCountersCache();
};

} // namespace ceph::perf_counters
