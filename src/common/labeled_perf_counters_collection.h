#pragma once

#include "common/labeled_perf_counters.h"
#include "common/ceph_mutex.h"
#include "include/common_fwd.h"

namespace ceph::common {
class LabeledPerfCountersCollection
{
  CephContext *m_cct;

  /** Protects perf_impl->m_loggers */
  mutable ceph::mutex m_lock;
  LabeledPerfCountersCollectionImpl perf_impl;
public:
  LabeledPerfCountersCollection(CephContext *cct);
  ~LabeledPerfCountersCollection();
  void add(LabeledPerfCounters *l);
  void remove(LabeledPerfCounters *l);
  void clear();
  bool reset(const std::string &name);

  void dump_formatted(ceph::Formatter *f, bool schema,
                      const std::string &logger = "",
                      const std::string &counter = "");
  void dump_formatted_histograms(ceph::Formatter *f, bool schema,
                                 const std::string &logger = "",
                                 const std::string &counter = "");

  void with_counters(std::function<void(const LabeledPerfCountersCollectionImpl::CounterMap &)>) const;
  friend class PerfCountersCollectionTest;
};
}
