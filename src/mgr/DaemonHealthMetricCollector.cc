#include "DaemonHealthMetricCollector.h"

#include <fmt/format.h>

#include "include/health.h"
#include "include/types.h"

#include <sstream>
#include <vector>

namespace {

using std::unique_ptr;
using std::vector;
using std::ostringstream;

class SlowOps final : public DaemonHealthMetricCollector {
  bool _is_relevant(daemon_metric type) const override {
    return type == daemon_metric::SLOW_OPS;
  }
  health_check_t& _get_check(health_check_map_t& cm) const override {
    return cm.get_or_add("SLOW_OPS", HEALTH_WARN, "", 1);
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
    // Note this message format is used in mgr/prometheus, so any change in format
    // requires a corresponding change in the mgr/prometheus module.
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
    check.summary =
        fmt::format("{} slow ops, oldest one blocked for {} sec, {}",
                    value.n1, value.n2, ss.str());
    // No detail
  }
  vector<DaemonKey> daemons;
};


class PendingPGs final : public DaemonHealthMetricCollector {
  bool _is_relevant(daemon_metric type) const override {
    return type == daemon_metric::PENDING_CREATING_PGS;
  }
  health_check_t& _get_check(health_check_map_t& cm) const override {
    return cm.get_or_add("PENDING_CREATING_PGS", HEALTH_WARN, "", 1);
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
    check.summary = fmt::format("{} PGs pending on creation", value.n);
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
    return std::make_unique<SlowOps>();
  case daemon_metric::PENDING_CREATING_PGS:
    return std::make_unique<PendingPGs>();
  default:
    return {};
  }
}
