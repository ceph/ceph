#include <memory>
#include <string>

#include "osd/OSDHealthMetric.h"
#include "mon/health_check.h"

class OSDHealthMetricCollector {
public:
  using DaemonKey = std::pair<std::string, std::string>;
  static std::unique_ptr<OSDHealthMetricCollector> create(osd_metric m);
  void update(const DaemonKey& osd, const OSDHealthMetric& metric) {
    if (_is_relevant(metric.get_type())) {
      reported = _update(osd, metric);
    }
  }
  void summarize(health_check_map_t& cm) {
    if (reported) {
      _summarize(_get_check(cm));
    }
  }
  virtual ~OSDHealthMetricCollector() {}
private:
  virtual bool _is_relevant(osd_metric type) const = 0;
  virtual health_check_t& _get_check(health_check_map_t& cm) const = 0;
  virtual bool _update(const DaemonKey& osd, const OSDHealthMetric& metric) = 0;
  virtual void _summarize(health_check_t& check) const = 0;
protected:
  osd_metric_t value;
  bool reported = false;
};
