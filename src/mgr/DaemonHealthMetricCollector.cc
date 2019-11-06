#include <boost/format.hpp>

#include "include/health.h"
#include "include/types.h"
#include "DaemonHealthMetricCollector.h"



ostream& operator<<(ostream& os,
                    const DaemonHealthMetricCollector::DaemonKey& daemon) {
  return os << daemon.first << "." << daemon.second;
}

// define operator<<(ostream&, const vector<DaemonKey>&) after
// ostream& operator<<(ostream&, const DaemonKey&), so that C++'s
// ADL can use the former instead of using the generic one:
// operator<<(ostream&, const std::pair<A,B>&)
ostream& operator<<(
   ostream& os,
   const vector<DaemonHealthMetricCollector::DaemonKey>& daemons)
{
  os << "[";
  for (auto d = daemons.begin(); d != daemons.end(); ++d) {
    if (d != daemons.begin()) os << ",";
    os << *d;
  }
  os << "]";
  return os;
}

namespace {

class SlowOps final : public DaemonHealthMetricCollector {
  bool _is_relevant(daemon_metric type) const override {
    return type == daemon_metric::SLOW_OPS;
  }
  health_check_t& _get_check(health_check_map_t& cm) const override {
    return cm.get_or_add("SLOW_OPS", HEALTH_WARN, "");
  }
  bool _update(const DaemonKey& daemon,
               const DaemonHealthMetric& metric) override {
    auto num_slow = metric.get_n1();
    auto blocked_time = metric.get_n2();
    value.n1 += num_slow;
    value.n2 = std::max(value.n2, blocked_time);
    if (num_slow || blocked_time) {
      daemons.push_back(daemon);
      return true;
    } else {
      return false;
    }
  }
  void _summarize(health_check_t& check) const override {
    if (daemons.empty()) {
      return;
    }
    static const char* fmt = "%1% slow ops, oldest one blocked for %2% sec, %3%";
    ostringstream ss;
    if (daemons.size() > 1) {
      if (daemons.size() > 10) {
        ss << "daemons " << vector<DaemonKey>(daemons.begin(), daemons.begin()+10)
           << "..." << " have slow ops.";
      } else {
        ss << "daemons " << daemons << " have slow ops.";
      }
    } else {
      ss << daemons.front() << " has slow ops";
    }
    check.summary = boost::str(boost::format(fmt) % value.n1 % value.n2 % ss.str());
    // No detail
  }
  vector<DaemonKey> daemons;
};


class PendingPGs final : public DaemonHealthMetricCollector {
  bool _is_relevant(daemon_metric type) const override {
    return type == daemon_metric::PENDING_CREATING_PGS;
  }
  health_check_t& _get_check(health_check_map_t& cm) const override {
    return cm.get_or_add("PENDING_CREATING_PGS", HEALTH_WARN, "");
  }
  bool _update(const DaemonKey& osd,
               const DaemonHealthMetric& metric) override {
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

unique_ptr<DaemonHealthMetricCollector>
DaemonHealthMetricCollector::create(daemon_metric m)
{
  switch (m) {
  case daemon_metric::SLOW_OPS:
    return unique_ptr<DaemonHealthMetricCollector>{new SlowOps};
  case daemon_metric::PENDING_CREATING_PGS:
    return unique_ptr<DaemonHealthMetricCollector>{new PendingPGs};
  default:
    return unique_ptr<DaemonHealthMetricCollector>{};
  }
}
