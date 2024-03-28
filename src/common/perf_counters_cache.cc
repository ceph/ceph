#include "common/perf_counters_cache.h"
#include "common/perf_counters_key.h"

namespace ceph::perf_counters {

typedef std::chrono::nanoseconds ns;

void PerfCountersCache::check_key(const std::string &key) {
  std::string_view key_name = ceph::perf_counters::key_name(key);
  // don't accept an empty key name
  assert(key_name != "");

  // if there are no labels, key name is not valid
  auto key_labels = ceph::perf_counters::key_labels(key);
  assert(key_labels.begin() != key_labels.end());

  // don't accept keys where any labels in the key have an empty key name
  for (auto key_label : key_labels) {
    assert(key_label.first != "");
  }
}

std::shared_ptr<PerfCounters> PerfCountersCache::add(const std::string &key) {
  check_key(key);

  auto [ref, key_existed] = cache.get_or_create(key);
  if (!key_existed) {
    ref->counters = create_counters(key, cct);
    assert(ref->counters);
    ref->cct = cct;
  }
  return ref->counters;
}


std::shared_ptr<PerfCounters> PerfCountersCache::get(const std::string &key) {
  std::lock_guard lock(m_lock);
  return add(key);
}

void PerfCountersCache::inc(const std::string &key, int indx, uint64_t v) {
  std::lock_guard lock(m_lock);
  auto counters = add(key);
  if (counters) {
    counters->inc(indx, v);
    if (counters->time_alive != ns::zero()) {
      counters->last_updated = ceph::coarse_real_clock::now();
    }
  }
}

void PerfCountersCache::dec(const std::string &key, int indx, uint64_t v) {
  std::lock_guard lock(m_lock);
  auto counters = add(key);
  if (counters) {
    counters->dec(indx, v);
    if (counters->time_alive != ns::zero()) {
      counters->last_updated = ceph::coarse_real_clock::now();
    }
  }
}

void PerfCountersCache::tinc(const std::string &key, int indx, utime_t amt) {
  std::lock_guard lock(m_lock);
  auto counters = add(key);
  if (counters) {
    counters->tinc(indx, amt);
    if (counters->time_alive != ns::zero()) {
      counters->last_updated = ceph::coarse_real_clock::now();
    }
  }
}

void PerfCountersCache::tinc(const std::string &key, int indx, ceph::timespan amt) {
  std::lock_guard lock(m_lock);
  auto counters = add(key);
  if (counters) {
    counters->tinc(indx, amt);
    if (counters->time_alive != ns::zero()) {
      counters->last_updated = ceph::coarse_real_clock::now();
    }
  }
}

void PerfCountersCache::set_counter(const std::string &key, int indx, uint64_t val) {
  std::lock_guard lock(m_lock);
  auto counters = add(key);
  if (counters) {
    counters->set(indx, val);
    if (counters->time_alive != ns::zero()) {
      counters->last_updated = ceph::coarse_real_clock::now();
    }
  }
}

uint64_t PerfCountersCache::get_counter(const std::string &key, int indx) {
  std::lock_guard lock(m_lock);
  auto counters = add(key);
  uint64_t val = 0;
  if (counters) {
    val = counters->get(indx);
  }
  return val;
}

utime_t PerfCountersCache::tget(const std::string &key, int indx) {
  std::lock_guard lock(m_lock);
  auto counters = add(key);
  utime_t val;
  if (counters) {
    val = counters->tget(indx);
    return val;
  } else {
    return utime_t();
  }
}

void PerfCountersCache::tset(const std::string &key, int indx, utime_t amt) {
  std::lock_guard lock(m_lock);
  auto counters = add(key);
  if (counters) {
    counters->tset(indx, amt);
    if (counters->time_alive != ns::zero()) {
      counters->last_updated = ceph::coarse_real_clock::now();
    }
  }
}

PerfCountersCache::PerfCountersCache(CephContext *_cct, size_t _target_size,
      std::function<std::shared_ptr<PerfCounters>(const std::string&, CephContext*)> _create_counters)
      : cct(_cct), create_counters(_create_counters), m_lock(ceph::make_mutex("PerfCountersCache")) { cache.set_target_size(_target_size); }

PerfCountersCache::~PerfCountersCache() { cache.set_target_size(0); }

} // namespace ceph::perf_counters
