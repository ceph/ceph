#include "common/perf_counters_collection.h"
#include "common/ceph_mutex.h"
#include "common/ceph_context.h"

namespace ceph::common {
/* PerfcounterCollection hold the lock for PerfCounterCollectionImp */
PerfCountersCollection::PerfCountersCollection(CephContext *cct)
  : m_cct(cct),
    m_lock(ceph::make_mutex("PerfCountersCollection"))
{
}
PerfCountersCollection::~PerfCountersCollection()
{
  clear();
}
void PerfCountersCollection::add(PerfCounters *l)
{
  std::lock_guard lck(m_lock);
  perf_impl.add(l);
}
void PerfCountersCollection::remove(PerfCounters *l)
{
  std::lock_guard lck(m_lock);
  perf_impl.remove(l);
}
void PerfCountersCollection::clear()
{
  std::lock_guard lck(m_lock);
  perf_impl.clear();
}
bool PerfCountersCollection::reset(const std::string &name)
{
  std::lock_guard lck(m_lock);
  return perf_impl.reset(name);
}
void PerfCountersCollection::dump_formatted(ceph::Formatter *f, bool schema,
                      bool dump_labeled,
                      const std::string &logger,
                      const std::string &counter)
{
  std::lock_guard lck(m_lock);
  perf_impl.dump_formatted(f, schema, dump_labeled, logger, counter);
}
void PerfCountersCollection::dump_formatted_histograms(ceph::Formatter *f, bool schema,
                                 const std::string &logger,
                                 const std::string &counter)
{
  std::lock_guard lck(m_lock);
  perf_impl.dump_formatted_histograms(f,schema,logger,counter);
}
void PerfCountersCollection::with_counters(std::function<void(const PerfCountersCollectionImpl::CounterMap &)> fn) const
{
  std::lock_guard lck(m_lock);
  perf_impl.with_counters(fn);
}
void PerfCountersDeleter::operator()(PerfCounters* p) noexcept
{
  if (cct)
    cct->get_perfcounters_collection()->remove(p);
  delete p;
}

}
