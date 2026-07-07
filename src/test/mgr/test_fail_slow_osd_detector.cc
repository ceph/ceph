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

#include "common/common_init.h"
#include "crush/CrushWrapper.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "gtest/gtest.h"
#include "include/ceph_assert.h"
#include "mon/PGMap.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace {
constexpr uint64_t NS_PER_MS = 1000000;

fail_slow_osd_detector_config make_config()
{
  fail_slow_osd_detector_config config;
  config.min_osds = 5;
  config.score_threshold = 10.0;
  config.min_latency_ms = 100.0;
  config.mad_floor_ms = 1.0;
  return config;
}

std::unique_ptr<OSDMap> make_osdmap(
  int num_osds,
  const std::map<int, std::string>& classes = {})
{
  auto osdmap = std::make_unique<OSDMap>();
  uuid_d fsid;
  osdmap->build_simple(g_ceph_context, 0, fsid, num_osds);
  OSDMap::Incremental pending_inc(osdmap->get_epoch() + 1);
  pending_inc.fsid = osdmap->get_fsid();
  entity_addrvec_t sample_addrs;
  sample_addrs.v.push_back(entity_addr_t());
  uuid_d sample_uuid;
  for (int i = 0; i < num_osds; ++i) {
    sample_uuid.generate_random();
    sample_addrs.v[0].nonce = i;
    pending_inc.new_state[i] = CEPH_OSD_EXISTS | CEPH_OSD_NEW;
    pending_inc.new_up_client[i] = sample_addrs;
    pending_inc.new_up_cluster[i] = sample_addrs;
    pending_inc.new_hb_back_up[i] = sample_addrs;
    pending_inc.new_hb_front_up[i] = sample_addrs;
    pending_inc.new_weight[i] = CEPH_OSD_IN;
    pending_inc.new_uuid[i] = sample_uuid;
  }
  osdmap->apply_incremental(pending_inc);

  for (const auto& [osd, device_class] : classes) {
    ceph_assert(osdmap->crush);
    ceph_assert(osdmap->crush->set_item_class(osd, device_class) == 0);
  }
  return osdmap;
}

PGMap make_pgmap(const std::vector<uint64_t>& latencies_ms)
{
  PGMap pgmap;
  for (size_t osd = 0; osd < latencies_ms.size(); ++osd) {
    osd_stat_t stat;
    stat.os_perf_stat.os_commit_latency_ns = latencies_ms[osd] * NS_PER_MS;
    pgmap.osd_stat[osd] = stat;
  }
  return pgmap;
}

std::function<std::vector<std::string>(int)> make_device_lookup(
  std::map<int, std::string> devices)
{
  return [devices = std::move(devices)](int osd) -> std::vector<std::string> {
    auto p = devices.find(osd);
    if (p != devices.end()) {
      return {p->second};
    }
    return {std::string{"osd."} + std::to_string(osd)};
  };
}
}

TEST(FailSlowOSDDetector, DetectsSingleSlowDevice)
{
  auto osdmap = make_osdmap(6);
  auto pgmap = make_pgmap({10, 11, 12, 12, 13, 500});

  auto devices = find_fail_slow_devices(
    *osdmap,
    pgmap,
    make_config(),
    make_device_lookup({{5, "dev5"}}));

  ASSERT_EQ(1u, devices.size());
  EXPECT_EQ("dev5", devices[0].device);
  ASSERT_EQ(1u, devices[0].osds.size());
  EXPECT_EQ(5, devices[0].osds[0].osd);
  EXPECT_DOUBLE_EQ(500.0, devices[0].latency_ms);
  EXPECT_GT(devices[0].score, 400.0);
}

TEST(FailSlowOSDDetector, ExcludesOSDsWithBackgroundWork)
{
  auto osdmap = make_osdmap(6);
  auto pgmap = make_pgmap({10, 11, 12, 12, 13, 500});
  pg_stat_t pg_stat;
  pg_stat.state = PG_STATE_RECOVERING;
  pg_stat.acting = {5};
  pgmap.pg_stat[pg_t{1, 1}] = pg_stat;

  auto devices = find_fail_slow_devices(
    *osdmap,
    pgmap,
    make_config(),
    make_device_lookup({{5, "dev5"}}));

  EXPECT_TRUE(devices.empty());
}

TEST(FailSlowOSDDetector, GroupsOSDsOnSameDevice)
{
  auto osdmap = make_osdmap(7);
  auto pgmap = make_pgmap({10, 11, 12, 12, 13, 500, 510});

  auto devices = find_fail_slow_devices(
    *osdmap,
    pgmap,
    make_config(),
    make_device_lookup({{5, "dev-shared"}, {6, "dev-shared"}}));

  ASSERT_EQ(1u, devices.size());
  EXPECT_EQ("dev-shared", devices[0].device);
  ASSERT_EQ(2u, devices[0].osds.size());
  EXPECT_EQ(5, devices[0].osds[0].osd);
  EXPECT_EQ(6, devices[0].osds[1].osd);
  EXPECT_DOUBLE_EQ(505.0, devices[0].latency_ms);
  EXPECT_GT(devices[0].score, 400.0);
}

TEST(FailSlowOSDDetector, KeepsDeviceClassesSeparate)
{
  std::map<int, std::string> classes;
  std::vector<uint64_t> latencies;
  std::vector<double> latencies_ms_ssd;
  for (int osd = 0; osd < 20; ++osd) {
    classes[osd] = "ssd";
    latencies.push_back(10 + osd % 4);
    latencies_ms_ssd.push_back(10 + osd % 4);
  }
  std::vector<double> latencies_ms_hdd;
  for (int osd = 20; osd < 25; ++osd) {
    classes[osd] = "hdd";
    latencies.push_back(500 + osd - 20);
    latencies_ms_hdd.push_back(500 + osd - 20);
  }

  auto osdmap = make_osdmap(25, classes);
  auto pgmap = make_pgmap(latencies);

  std::map<int, std::string> device_lookups;
  for (int osd = 0; osd < 25; osd++) {
    device_lookups.emplace(osd, std::string("dev") + std::to_string(osd));
  }
  auto devices = find_fail_slow_devices(
    *osdmap,
    pgmap,
    make_config(),
    make_device_lookup(std::move(device_lookups)),
    true);

  ASSERT_EQ(25u, devices.size());
  auto lat_median_ssd = median(latencies_ms_ssd);
  auto lat_median_hdd = median(latencies_ms_hdd);
  for (int i = 0; i < 25; i++) {
    ASSERT_EQ(1u, devices[i].osds.size());
    if (devices[i].osds.front().osd < 20) {
      ASSERT_EQ(*lat_median_ssd, devices[i].cohort_median);
      ASSERT_EQ(std::string("{crush_root:default, device_class:ssd}"),
                devices[i].cohort);
    } else {
      ASSERT_EQ(*lat_median_hdd, devices[i].cohort_median);
      ASSERT_EQ(std::string("{crush_root:default, device_class:hdd}"),
                devices[i].cohort);
    }
  }
}

int main(int argc, char **argv)
{
  std::map<std::string, std::string> defaults = {
    {"osd_pool_default_size", "3"},
    {"osd_crush_chooseleaf_type", "0"},
  };
  std::vector<const char*> args(argv, argv + argc);
  auto cct = global_init(
    &defaults,
    args,
    CEPH_ENTITY_TYPE_CLIENT,
    CODE_ENVIRONMENT_UTILITY,
    CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
