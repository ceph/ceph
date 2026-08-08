// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "mgr/FailSlowOSDDetector.h"

#include "include/ceph_assert.h"
#include "include/stringify.h"
#include "crush/CrushWrapper.h"
#include "mon/PGMap.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include "common/debug.h"

#include <algorithm>
#include <cmath>
#include <map>
#include <set>
#include <boost/algorithm/string.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

namespace {
constexpr uint64_t FAIL_SLOW_BACKGROUND_PG_STATES =
  PG_STATE_RECOVERING |
  PG_STATE_PREMERGE |
  PG_STATE_DEEP_SCRUB |
  PG_STATE_BACKFILLING;
}

std::optional<double>
FailSlowOSDDetector::median_absolute_deviation(
  const std::vector<double>& values,
  double center)
{
  std::vector<double> deviations;
  deviations.reserve(values.size());
  for (auto value : values) {
    deviations.push_back(std::abs(value - center));
  }
  return median(deviations);
}

void FailSlowOSDDetector::refresh_osd_crush_root_map(const OSDMap& osdmap)
{
  if (!osdmap.crush) {
    return;
  }
  if (auto e = osdmap.get_epoch(); e == or_map.epoch) {
    return;
  } else {
    ceph_assert(e > or_map.epoch);
    or_map.epoch = e;
  }
  std::set<int> roots;
  osdmap.crush->find_nonshadow_roots(&roots);
  for (int root : roots) {
    std::string root_name = osdmap.crush->get_item_name(root);
    std::list<int> buckets;
    buckets.push_back(root);
    while (!buckets.empty()) {
      int bucket = buckets.front();
      buckets.pop_front();
      int size = osdmap.crush->get_bucket_size(bucket);
      for (int i = 0; i < size; i++) {
        int item = osdmap.crush->get_bucket_item(bucket, i);
        if (item >= 0) {
          // this is an OSD
          or_map.osd_root_map.emplace(item, root_name);
        } else {
          buckets.push_back(item);
        }
      }
    }
  }
}

std::string FailSlowOSDDetector::fail_slow_cohort(
  const OSDMap& osdmap,
  int osd)
{
  std::string_view crush_root = "null";
  auto it = or_map.osd_root_map.find(osd);
  if (it != or_map.osd_root_map.end()) {
    crush_root = it->second;
  }
  std::string_view class_name = "unknown";
  if (osdmap.crush) {
    if (const char *item_class = osdmap.crush->get_item_class(osd)) {
      class_name = item_class;
    }
  }
  return fmt::format("{{crush_root:{}, device_class:{}}}",
    crush_root, class_name);
}

std::optional<double>
FailSlowOSDDetector::median(std::vector<double> &values)
{
  if (values.empty()) {
    return std::nullopt;
  }
  const std::size_t n = values.size();
  const auto middle = values.begin() + n / 2;

  std::nth_element(values.begin(), middle, values.end());

  if (n % 2 == 1) {
    return *middle;
  }

  const double upper = *middle;
  const double lower =
    *std::max_element(values.begin(), middle);
  return (lower + upper) / 2.0;
}

/*
 * find_fail_slow_device works in the following way:
 *
 * 1. Find the commit latencies of all OSDs that are NOT under
 *    background works;
 * 2. Calculate the scores of all OSDs that are NOT under background
 *    works, the scores are calculated as:
 *    osd_score = (commit_latency_ms[osd] - cohort_median) / cohort_MAD
 * 3. Ground the above OSDs by their under lying devices, and calculate
 *    each device's score:
 *    device_score = median(osd_score[osd] for osd on device)
 * 4. Return the devices whose scores exceeds both score_threshold and
 *    min_latency_ms
 */
std::vector<fail_slow_device_score>
FailSlowOSDDetector::find_fail_slow_devices(
  const OSDMap& osdmap,
  const PGMap& pgmap,
  const fail_slow_osd_detector_config& config,
  const std::function<std::vector<std::string>(int)>& get_osd_device,
  bool all_devices)
{
  std::set<int> background_osds;
  for (const auto& [_, pg_stat] : pgmap.pg_stat) {
    if ((pg_stat.state & FAIL_SLOW_BACKGROUND_PG_STATES) == 0) {
      continue;
    }
    for (auto osd : pg_stat.acting) {
      if (osd >= 0) {
        dout(20) << "found acting osd " << osd << dendl;
        background_osds.insert(osd);
      }
    }
    for (auto osd : pg_stat.up) {
      if (osd >= 0) {
        dout(20) << "found up osd " << osd << dendl;
        background_osds.insert(osd);
      }
    }
  }

  refresh_osd_crush_root_map(osdmap);
  std::map<int, double> latency_by_osd;
  std::map<int, std::string> cohort_by_osd;
  std::map<std::string, std::vector<double>> latencies_by_cohort;
  for (const auto& [osd, osd_stat] : pgmap.osd_stat) {
    if (!osdmap.is_up(osd) ||
        !osdmap.is_in(osd) ||
        background_osds.contains(osd)) {
      continue;
    }

    auto latency_ms =
      static_cast<double>(osd_stat.os_perf_stat.os_commit_latency_ns) /
      1000000.0;
    if (latency_ms == 0.0) {
      continue;
    }

    auto cohort = fail_slow_cohort(osdmap, osd);
    dout(20) << "osd." << osd << " latency: " << latency_ms
      << " cohort: " << cohort << dendl;
    latency_by_osd[osd] = latency_ms;
    cohort_by_osd[osd] = cohort;
    latencies_by_cohort[cohort].push_back(latency_ms);
  }

  struct cohort_stats_t {
    double median = 0.0;
    double mad = 0.0;
  };
  std::map<std::string, cohort_stats_t> stats_by_cohort;
  for (auto &[cohort, cohort_latencies] : latencies_by_cohort) {
    if (cohort_latencies.size() < config.min_osds) {
      dout(20) << "skipping cohort: " << cohort
        << ", as too few osds in the cohort" << dendl;
      continue;
    }

    auto cohort_median = median(cohort_latencies);
    if (!cohort_median) {
      dout(20) << "skipping cohort: " << cohort
        << ", as can't calc median" << dendl;
      continue;
    }

    auto cohort_mad = median_absolute_deviation(
      cohort_latencies,
      *cohort_median);
    if (!cohort_mad) {
      dout(20) << "skipping cohort: " << cohort
        << ", as can't calc MAD" << dendl;
      continue;
    }
    *cohort_mad = std::max(*cohort_mad, config.mad_floor_ms);
    stats_by_cohort.emplace(
      cohort, cohort_stats_t{*cohort_median, *cohort_mad});
  }

  std::map<int, fail_slow_osd_score> scored_osds;
  for (const auto& [osd, latency_ms] : latency_by_osd) {
    const auto& cohort = cohort_by_osd[osd];
    auto it = stats_by_cohort.find(cohort);
    if (it == stats_by_cohort.end()) {
      dout(20) << "skipping osd." << osd
        << ", as its cohort has no usable stats" << dendl;
      continue;
    }
    const auto [cohort_median, cohort_mad] = it->second;

    fail_slow_osd_score score;
    score.osd = osd;
    score.latency_ms = latency_ms;
    score.score = (latency_ms - cohort_median) / cohort_mad;
    dout(20) << "osd." << osd << " score: " << score.score
      << " cohort_median: " << cohort_median
      << " cohort_MAD: " << cohort_mad << dendl;
    score.cohort = cohort;
    score.cohort_median = cohort_median;
    score.cohort_mad = cohort_mad;
    scored_osds[osd] = std::move(score);
  }

  std::map<std::string, std::vector<fail_slow_osd_score>> osds_by_device;
  for (const auto& [osd, score] : scored_osds) {
    auto devices = get_osd_device(osd);
    for (auto &device : devices) {
      osds_by_device[device].push_back(score);
    }
  }

  std::vector<fail_slow_device_score> devices;
  for (auto& [device, osds] : osds_by_device) {
    std::vector<double> scores;
    std::vector<double> latencies;
    scores.reserve(osds.size());
    latencies.reserve(osds.size());
    for (const auto& osd : osds) {
      scores.push_back(osd.score);
      latencies.push_back(osd.latency_ms);
    }

    auto device_score = median(scores);
    auto device_latency = median(latencies);
    if (!device_score || !device_latency) {
      continue;
    }
    if (!all_devices &&
        (*device_score < config.score_threshold ||
         *device_latency < config.min_latency_ms)) {
      continue;
    }

    std::sort(osds.begin(), osds.end(),
              [](const auto& lhs, const auto& rhs) {
                return lhs.osd < rhs.osd;
              });
    auto representative = std::max_element(
      osds.begin(),
      osds.end(),
      [](const auto& lhs, const auto& rhs) {
        return lhs.score < rhs.score;
      });

    dout(20) << "device: " << device << " score: " << *device_score << dendl;
    fail_slow_device_score device_score_entry;
    device_score_entry.device = device;
    device_score_entry.score = *device_score;
    device_score_entry.latency_ms = *device_latency;
    device_score_entry.cohort = representative->cohort;
    device_score_entry.cohort_median = representative->cohort_median;
    device_score_entry.cohort_mad = representative->cohort_mad;
    device_score_entry.osds = std::move(osds);
    devices.push_back(std::move(device_score_entry));
  }

  std::sort(devices.begin(), devices.end(),
            [](const auto& lhs, const auto& rhs) {
              return lhs.score > rhs.score;
            });
  return devices;
}

std::vector<fail_slow_device_score>
FailSlowOSDDetector::_find_fail_slow_devices(
  const OSDMap& osdmap,
  const PGMap& pgmap,
  const get_osd_device_func_t &get_osd_device)
{
  fail_slow_osd_detector_config config;
  config.min_osds =
    g_conf().get_val<uint64_t>("mgr_fail_slow_osd_min_osds");
  config.score_threshold =
    g_conf().get_val<double>("mgr_fail_slow_osd_score_threshold");
  config.min_latency_ms =
    g_conf().get_val<double>("mgr_fail_slow_osd_min_latency_ms");
  config.mad_floor_ms =
    g_conf().get_val<double>("mgr_fail_slow_osd_mad_floor_ms");

  return find_fail_slow_devices(
    osdmap,
    pgmap,
    config,
    get_osd_device);
}

void FailSlowOSDDetector::_check_fail_slow_osds(
  const OSDMap& osdmap,
  const PGMap& pgmap,
  health_check_map_t *checks,
  const get_osd_device_func_t &get_osd_device)
{
  dout(20) << "" << dendl;
  if (!g_conf().get_val<bool>("mgr_fail_slow_osd_enabled")) {
    fail_slow_device_counts.clear();
    fail_slow_devices.clear();
    return;
  }

  auto now = ceph_clock_now();
  if (now - last_fail_slow <
      g_conf().get_val<int64_t>("mgr_fail_slow_osd_check_period")) {
    dout(20) << "skipped, waiting for fail slow check period" << dendl;
  } else {
    last_fail_slow = now;

    auto devices = _find_fail_slow_devices(osdmap, pgmap, get_osd_device);
    std::set<std::string> current_devices;
    for (const auto& device : devices) {
      current_devices.insert(device.device);
    }

    for (auto p = fail_slow_device_counts.begin();
         p != fail_slow_device_counts.end(); ) {
      if (auto found = current_devices.contains(p->first);
          !found && p->second < 2) {
        p = fail_slow_device_counts.erase(p);
      } else {
        if (!found) {
          p->second /= 2;
        }
        ++p;
      }
    }
    for (const auto& device : current_devices) {
      ++fail_slow_device_counts[device];
    }
    const auto persistence =
      g_conf().get_val<uint64_t>("mgr_fail_slow_osd_persistence");
    std::vector<fail_slow_device_score> persistent_devices;
    for (const auto& device : devices) {
      if (fail_slow_device_counts[device.device] >= persistence) {
        persistent_devices.push_back(device);
      }
    }
    fail_slow_devices = std::move(persistent_devices);
  }

  if (fail_slow_devices.empty()) {
    return;
  }
  auto& check = checks->add(
    "OSD_FAIL_SLOW",
    HEALTH_WARN,
    stringify(fail_slow_devices.size()) +
    " device(s) with abnormally high OSD commit latency",
    fail_slow_devices.size());

  for (const auto& device : fail_slow_devices) {
    std::vector<std::string> osds;
    osds.reserve(device.osds.size());
    for (const auto& osd : device.osds) {
      osds.push_back("osd." + stringify(osd.osd));
    }

    std::ostringstream ss;
    ss << std::fixed << std::setprecision(2)
       << "device " << device.device
       << " has fail-slow score " << device.score
       << " after " << fail_slow_device_counts[device.device]
       << " observations ("
       << boost::algorithm::join(osds, ",")
       << "; median commit latency " << device.latency_ms << " ms"
       << "; " << device.cohort
       << " median " << device.cohort_median << " ms"
       << "; MAD " << device.cohort_mad << " ms)";
    check.detail.push_back(ss.str());
  }
}
