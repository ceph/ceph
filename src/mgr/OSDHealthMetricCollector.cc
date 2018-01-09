#include <boost/format.hpp>

#include "include/health.h"
#include "include/types.h"
#include "OSDHealthMetricCollector.h"



ostream& operator<<(ostream& os,
                    const OSDHealthMetricCollector::DaemonKey& daemon) {
  return os << daemon.first << "." << daemon.second;
}

namespace {

class SlowOps final : public OSDHealthMetricCollector {
  bool _is_relevant(osd_metric type) const override {
    return type == osd_metric::SLOW_OPS;
  }
  health_check_t& _get_check(health_check_map_t& cm) const override {
    return cm.get_or_add("SLOW_OPS", HEALTH_WARN, "");
  }
  bool _update(const DaemonKey& osd,
               const OSDHealthMetric& metric) override {
    auto num_slow = metric.get_n1();
    auto blocked_time = metric.get_n2();
    value.n1 += num_slow;
    value.n2 = std::max(value.n2, blocked_time);
    if (num_slow || blocked_time) {
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
    static const char* fmt = "%1% slow ops, oldest one blocked for %2% sec";
    check.summary = boost::str(boost::format(fmt) % value.n1 % value.n2);
    ostringstream ss;
    if (osds.size() > 1) {
      ss << "osds " << osds << " have slow ops.";
    } else {
      ss << osds.front() << " has slow ops";
    }
    check.detail.push_back(ss.str());
  }
  vector<DaemonKey> osds;
};


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
  case osd_metric::SLOW_OPS:
    return unique_ptr<OSDHealthMetricCollector>{new SlowOps};
  case osd_metric::PENDING_CREATING_PGS:
    return unique_ptr<OSDHealthMetricCollector>{new PendingPGs};
  default:
    return unique_ptr<OSDHealthMetricCollector>{};
  }
}
