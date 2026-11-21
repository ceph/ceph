// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>
#include <optional>

#include "mon/health_check.h"
#include "include/types.h"

class OSDMap;
class PGMap;

struct fail_slow_osd_detector_config {
  uint64_t min_osds = 5;
  double score_threshold = 10.0;
  double min_latency_ms = 100.0;
  double mad_floor_ms = 1.0;
};

struct fail_slow_osd_score {
  int osd = -1;
  double latency_ms = 0.0;
  double score = 0.0;
  std::string cohort;
  double cohort_median = 0.0;
  double cohort_mad = 0.0;
};

struct fail_slow_device_score {
  std::string device;
  std::vector<fail_slow_osd_score> osds;
  double score = 0.0;
  double latency_ms = 0.0;
  std::string cohort;
  double cohort_median = 0.0;
  double cohort_mad = 0.0;
};


class FailSlowOSDDetector {
public:
  std::optional<double> median(std::vector<double> &values);
  using get_osd_device_func_t =
    std::function<std::vector<std::string>(int)>;
  std::vector<fail_slow_device_score> find_fail_slow_devices(
    const OSDMap& osdmap,
    const PGMap& pgmap,
    const fail_slow_osd_detector_config& config,
    const get_osd_device_func_t& get_osd_device,
    bool all_devices = false // this is for unit tests only
  );
  std::vector<fail_slow_device_score> _find_fail_slow_devices(
    const OSDMap& osdmap,
    const PGMap& pgmap,
    const get_osd_device_func_t &get_osd_device);
  void _check_fail_slow_osds(
    const OSDMap& osdmap,
    const PGMap& pgmap,
    health_check_map_t *checks,
    const get_osd_device_func_t &get_osd_device);
private:
  using osd_crush_root_map_t = std::map<int, std::string>;
  std::string fail_slow_cohort(
    const OSDMap& osdmap,
    int osd);
  void refresh_osd_crush_root_map(const OSDMap& osdmap);
  std::optional<double> median_absolute_deviation(
    const std::vector<double>& values,
    double center);
  std::map<std::string, uint64_t> fail_slow_device_counts;
  utime_t last_fail_slow;
  std::vector<fail_slow_device_score> fail_slow_devices;
  struct cached_osd_root_map_t {
    epoch_t epoch = 0;
    osd_crush_root_map_t osd_root_map;
  } or_map;
};
