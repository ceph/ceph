#pragma once

#include <memory>
#include <string>

#include "DaemonHealthMetric.h"
#include "mon/health_check.h"

class DaemonHealthMetricCollector {
public:
  using DaemonKey = std::pair<std::string, std::string>;
  static std::unique_ptr<DaemonHealthMetricCollector> create(daemon_metric m);
  void update(const DaemonKey& daemon, const DaemonHealthMetric& metric) {
    if (_is_relevant(metric.get_type())) {
      reported |= _update(daemon, metric);
    }
  }
  void summarize(health_check_map_t& cm) {
    if (reported) {
      _summarize(_get_check(cm));
    }
  }
  virtual ~DaemonHealthMetricCollector() {}
private:
  virtual bool _is_relevant(daemon_metric type) const = 0;
  virtual health_check_t& _get_check(health_check_map_t& cm) const = 0;
  virtual bool _update(const DaemonKey& daemon, const DaemonHealthMetric& metric) = 0;
  virtual void _summarize(health_check_t& check) const = 0;
protected:
  daemon_metric_t value;
  bool reported = false;
};
