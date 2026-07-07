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
#include "crush/CrushWrapper.h"
#include "mon/PGMap.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include "common/debug.h"

#include <algorithm>
#include <cmath>
#include <map>
#include <set>

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

std::optional<double> median_absolute_deviation(
  const std::vector<double>& values,
  double center)
{
  std::vector<double> deviations;
  deviations.reserve(values.size());
  for (auto value : values) {
    deviations.push_back(std::abs(value - center));
  }
  return median(std::move(deviations));
}

using osd_crush_root_map_t = std::map<int, std::string>;
osd_crush_root_map_t get_osd_crush_root_map(const OSDMap& osdmap)
{
  osd_crush_root_map_t osd_root_map;
  if (osdmap.crush) {
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
            osd_root_map.emplace(item, root_name);
          } else {
            buckets.push_back(item);
          }
        }
      }
    }
  }
  return osd_root_map;
}

std::string fail_slow_cohort(
  const osd_crush_root_map_t &osd_root_map,
  const OSDMap& osdmap,
  int osd)
{
  const char *class_name = nullptr;
  if (osdmap.crush) {
    class_name = osdmap.crush->get_item_class(osd);
  }
  auto it = osd_root_map.find(osd);
  assert(it != osd_root_map.end());
  return std::string{"{crush_root:"} + it->second +
    std::string{", device_class:"} +
    (class_name ? class_name : "unknown") + "}";
}
}

std::optional<double> median(std::vector<double> values)
{
  if (values.empty()) {
    return std::nullopt;
  }
  std::sort(values.begin(), values.end());
  auto mid = values.size() / 2;
  if (values.size() % 2) {
    return values[mid];
  }
  return (values[mid - 1] + values[mid]) / 2.0;
}

std::vector<fail_slow_device_score> find_fail_slow_devices(
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

  osd_crush_root_map_t osd_root_map = get_osd_crush_root_map(osdmap);
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

    auto cohort = fail_slow_cohort(osd_root_map, osdmap, osd);
    dout(20) << "osd." << osd << " latency: " << latency_ms
      << " cohort: " << cohort << dendl;
    latency_by_osd[osd] = latency_ms;
    cohort_by_osd[osd] = cohort;
    latencies_by_cohort[cohort].push_back(latency_ms);
  }

  std::map<int, fail_slow_osd_score> scored_osds;
  for (const auto& [osd, latency_ms] : latency_by_osd) {
    const auto& cohort = cohort_by_osd[osd];
    const auto& cohort_latencies = latencies_by_cohort[cohort];
    if (cohort_latencies.size() < config.min_osds) {
      dout(20) << "skipping osd." << osd
        << ", as too few osds in the cohort" << dendl;
      continue;
    }

    auto cohort_median = median(cohort_latencies);
    if (!cohort_median) {
      dout(20) << "skipping osd." << osd << ", as can't calc median" << dendl;
      continue;
    }

    auto cohort_mad = median_absolute_deviation(
      cohort_latencies,
      *cohort_median);
    if (!cohort_mad) {
      dout(20) << "skipping osd." << osd << ", as can't calc MAD" << dendl;
      continue;
    }
    *cohort_mad = std::max(*cohort_mad, config.mad_floor_ms);

    fail_slow_osd_score score;
    score.osd = osd;
    score.latency_ms = latency_ms;
    score.score = (latency_ms - *cohort_median) / *cohort_mad;
    dout(20) << "osd." << osd << " score: " << score.score
      << " cohort_median: " << *cohort_median
      << " cohort_MAD: " << cohort_mad << dendl;
    score.cohort = cohort;
    score.cohort_median = *cohort_median;
    score.cohort_mad = *cohort_mad;
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

    auto device_score = median(std::move(scores));
    auto device_latency = median(std::move(latencies));
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
