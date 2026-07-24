// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>
#include <optional>

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


std::optional<double> median(std::vector<double> values);
std::vector<fail_slow_device_score> find_fail_slow_devices(
  const OSDMap& osdmap,
  const PGMap& pgmap,
  const fail_slow_osd_detector_config& config,
  const std::function<std::vector<std::string>(int)>& get_osd_device,
  bool all_devices = false // this is for unit tests only
);
