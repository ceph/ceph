#pragma once

#include "common/perf_counters.h"
#include "common/ceph_mutex.h"
#include "include/common_fwd.h"

namespace ceph::common {
class PerfCountersCollection
{
  CephContext *m_cct;

  /** Protects perf_impl->m_loggers */
  mutable ceph::mutex m_lock;
  PerfCountersCollectionImpl perf_impl;
public:
  PerfCountersCollection(CephContext *cct);
  ~PerfCountersCollection();
  void add(PerfCounters *l);
  void remove(PerfCounters *l);
  void clear();
  bool reset(const std::string &name);

  void dump_formatted(ceph::Formatter *f, bool schema, bool dump_labeled,
                      const std::string &logger = "",
                      const std::string &counter = "");
  void dump_formatted_histograms(ceph::Formatter *f, bool schema,
                                 const std::string &logger = "",
                                 const std::string &counter = "");

  void with_counters(std::function<void(const PerfCountersCollectionImpl::CounterMap &)>) const;

  friend class PerfCountersCollectionTest;
};

class PerfCountersDeleter {
  CephContext* cct;

public:
  PerfCountersDeleter() noexcept : cct(nullptr) {}
  PerfCountersDeleter(CephContext* cct) noexcept : cct(cct) {}
  void operator()(PerfCounters* p) noexcept;
};
}
using PerfCountersRef = std::unique_ptr<ceph::common::PerfCounters, ceph::common::PerfCountersDeleter>;
