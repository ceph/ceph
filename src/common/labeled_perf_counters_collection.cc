#include "common/labeled_perf_counters_collection.h"
#include "common/ceph_mutex.h"
#include "common/ceph_context.h"

namespace ceph::common {
LabeledPerfCountersCollection::LabeledPerfCountersCollection(CephContext *cct)
  : m_cct(cct),
    m_lock(ceph::make_mutex("PerfCountersCollection"))
{
}
LabeledPerfCountersCollection::~LabeledPerfCountersCollection()
{
  clear();
}
void LabeledPerfCountersCollection::add(LabeledPerfCounters *l)
{
  std::lock_guard lck(m_lock);
  perf_impl.add(l);
}
void LabeledPerfCountersCollection::remove(LabeledPerfCounters *l)
{
  std::lock_guard lck(m_lock);
  perf_impl.remove(l);
}
void LabeledPerfCountersCollection::clear()
{
  std::lock_guard lck(m_lock);
  perf_impl.clear();
}
bool LabeledPerfCountersCollection::reset(const std::string &name)
{
  std::lock_guard lck(m_lock);
  return perf_impl.reset(name);
}
void LabeledPerfCountersCollection::dump_formatted(ceph::Formatter *f, bool schema,
                      const std::string &logger,
                      const std::string &counter)
{
  std::lock_guard lck(m_lock);
  perf_impl.dump_formatted(f,schema,logger,counter);
}
void LabeledPerfCountersCollection::dump_formatted_histograms(ceph::Formatter *f, bool schema,
                                 const std::string &logger,
                                 const std::string &counter)
{
  std::lock_guard lck(m_lock);
  perf_impl.dump_formatted_histograms(f,schema,logger,counter);
}
void LabeledPerfCountersCollection::with_counters(std::function<void(const LabeledPerfCountersCollectionImpl::CounterMap &)> fn) const
{
  std::lock_guard lck(m_lock);
  perf_impl.with_counters(fn);
}

}
