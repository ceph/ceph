#include <boost/format.hpp>

#include "include/health.h"
#include "include/types.h"
#include "OSDHealthMetricCollector.h"


using namespace std;

ostream& operator<<(ostream& os,
                    const OSDHealthMetricCollector::DaemonKey& daemon) {
  return os << daemon.first << "." << daemon.second;
}

namespace {

class PendingPGs final : public OSDHealthMetricCollector {
  bool _is_relevant(osd_metric type) const override {
    return type == osd_metric::PENDING_CREATING_PGS;
  }
  health_check_t& _get_check(health_check_map_t& cm) const override {
    return cm.get_or_add("PENDING_CREATING_PGS", HEALTH_WARN, "");
  }
  bool _update(const DaemonKey& osd,
               const OSDHealthMetric& metric) override {
    value.n += metric.get_n();
    if (metric.get_n()) {
      osds.push_back(osd);
      return true;
    } else {
      return false;
    }
  }
  void _summarize(health_check_t& check) const override {
    if (osds.empty()) {
      return;
    }
    static const char* fmt = "%1% PGs pending on creation";
    check.summary = boost::str(boost::format(fmt) % value.n);
    ostringstream ss;
    if (osds.size() > 1) {
      ss << "osds " << osds << " have pending PGs.";
    } else {
      ss << osds.front() << " has pending PGs";
    }
    check.detail.push_back(ss.str());
  }
  vector<DaemonKey> osds;
};

} // anonymous namespace

unique_ptr<OSDHealthMetricCollector>
OSDHealthMetricCollector::create(osd_metric m)
{
  switch (m) {
  case osd_metric::PENDING_CREATING_PGS:
    return unique_ptr<OSDHealthMetricCollector>{new PendingPGs};
  default:
    return unique_ptr<OSDHealthMetricCollector>{};
  }
}
