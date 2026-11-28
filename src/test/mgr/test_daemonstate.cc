// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sstream>

#include "common/Formatter.h"
#include "global/global_init.h"
#include "gtest/gtest.h"
#include "include/utime.h"
#include "mgr/DaemonState.h"
#include "test/mgr/TestMgr.h"

// Test macro for utime_t comparisons
#define EXPECT_UTIME_EQ(actual, expected)                              \
  do {                                                                 \
    const utime_t& _a = (actual);                                      \
    const utime_t& _e = (expected);                                    \
    if (!(_a == _e)) {                                                 \
      ADD_FAILURE() << "EXPECT_UTIME_EQ failed:\n"                     \
                    << "  actual:   " << _a.sec() << "s " << _a.nsec() \
                    << "ns\n"                                          \
                    << "  expected: " << _e.sec() << "s " << _e.nsec() \
                    << "ns\n";                                         \
    }                                                                  \
  } while (false);

TEST_F(DeviceStateTest, SetMetadata)
{
  std::map<std::string, std::string> metadata = {
      {"life_expectancy_min", "1111111111.000000"},
      {"life_expectancy_max", "2222222222.000000"},
      {"life_expectancy_stamp", "1234567890.000000"},
      {"wear_level", "0.75"}};

  device->set_metadata(std::move(metadata));

  ASSERT_EQ(device->wear_level, 0.75f);
  utime_t expected_min(1111111111, 0);
  EXPECT_UTIME_EQ(device->life_expectancy.first, expected_min);
  utime_t expected_max(2222222222, 0);
  EXPECT_UTIME_EQ(device->life_expectancy.second, expected_max);
  utime_t expected_stamp(1234567890, 0);
  EXPECT_UTIME_EQ(device->life_expectancy_stamp, expected_stamp);
}

TEST_F(DeviceStateTest, SetLifeExpectancy)
{
  utime_t from(1000, 0);
  utime_t to(2000, 0);
  utime_t now(900, 0);

  device->set_life_expectancy(from, to, now);

  ASSERT_EQ(device->life_expectancy.first, from);
  ASSERT_EQ(device->life_expectancy.second, to);
  ASSERT_EQ(device->life_expectancy_stamp, now);
  ASSERT_EQ(device->metadata["life_expectancy_min"], "1000.000000");
  ASSERT_EQ(device->metadata["life_expectancy_max"], "2000.000000");
  ASSERT_EQ(device->metadata["life_expectancy_stamp"], "900.000000");
}

TEST_F(DeviceStateTest, RemoveLifeExpectancy)
{
  utime_t from(1000, 0);
  utime_t to(2000, 0);
  utime_t now(900, 0);

  device->set_life_expectancy(from, to, now);
  device->rm_life_expectancy();

  ASSERT_EQ(device->life_expectancy.first, utime_t());
  ASSERT_EQ(device->life_expectancy.second, utime_t());
  ASSERT_EQ(device->life_expectancy_stamp, utime_t());
  ASSERT_FALSE(device->metadata.contains("life_expectancy_min"));
  ASSERT_FALSE(device->metadata.contains("life_expectancy_max"));
  ASSERT_FALSE(device->metadata.contains("life_expectancy_stamp"));
}

TEST_F(DeviceStateTest, SetWearLevel)
{
  device->set_wear_level(0.75f);

  ASSERT_EQ(device->wear_level, 0.75f);
  ASSERT_EQ(device->metadata["wear_level"], "0.75");

  device->set_wear_level(-1.0f);

  ASSERT_EQ(device->wear_level, -1.0f);
  ASSERT_FALSE(device->metadata.contains("wear_level"));
}

TEST_F(DeviceStateTest, GetLifeExpectancyStr)
{
  utime_t now(1500, 0);
  std::string result = device->get_life_expectancy_str(now);

  ASSERT_EQ(result, "");

  utime_t from(1000, 0);
  utime_t to(1400, 0);
  device->set_life_expectancy(from, to, now);
  result = device->get_life_expectancy_str(now);

  ASSERT_EQ(result, "now");

  from = utime_t(2000, 0);
  to = utime_t(3000, 0);
  device->set_life_expectancy(from, to, now);
  result = device->get_life_expectancy_str(now);

  ASSERT_EQ(result, "8m to 25m");

  from = utime_t(2500, 0);
  to = utime_t();
  device->set_life_expectancy(from, to, now);
  result = device->get_life_expectancy_str(now);

  ASSERT_EQ(result, ">16m");

  from = utime_t(2000, 0);
  to = utime_t(2000, 1000);
  device->set_life_expectancy(from, to, now);
  result = device->get_life_expectancy_str(now);

  ASSERT_EQ(result, "8m");
}

/* Begin Negative Tests */

TEST_F(DeviceStateTest, SetMetadataWithEmptyMap)
{
  std::map<std::string, std::string> empty_metadata;
  device->set_metadata(std::move(empty_metadata));

  ASSERT_EQ(device->wear_level, -1.0f);
  ASSERT_EQ(device->life_expectancy.first, utime_t());
  ASSERT_EQ(device->life_expectancy.second, utime_t());
  ASSERT_EQ(device->life_expectancy_stamp, utime_t());
}

TEST_F(DeviceStateTest, SetMetadataWithInvalidValues)
{
  std::map<std::string, std::string> invalid_metadata = {
      {"life_expectancy_min", "bad"},
      {"life_expectancy_max", "test"},
      {"life_expectancy_stamp", "input"},
      {"wear_level", "zeropointsevenfive"}};
  device->set_metadata(std::move(invalid_metadata));

  ASSERT_EQ(device->wear_level, -1.0f);
  ASSERT_EQ(device->life_expectancy.first, utime_t());
  ASSERT_EQ(device->life_expectancy.second, utime_t());
  ASSERT_EQ(device->life_expectancy_stamp, utime_t());
}

TEST_F(DeviceStateTest, SetMetadataWithEmptyStrings)
{
  std::map<std::string, std::string> empty_str_metadata = {
      {"life_expectancy_min", ""},
      {"life_expectancy_max", ""},
      {"life_expectancy_stamp", ""},
      {"wear_level", ""}};
  device->set_metadata(std::move(empty_str_metadata));

  ASSERT_EQ(device->wear_level, -1.0f);
  ASSERT_EQ(device->life_expectancy.first, utime_t());
  ASSERT_EQ(device->life_expectancy.second, utime_t());
  ASSERT_EQ(device->life_expectancy_stamp, utime_t());
}

TEST_F(DeviceStateTest, SetLifeExpectancyInvalidTime)
{
  utime_t from(2000, 0);
  utime_t to(1000, 0); // to > from
  utime_t now(900, 0);

  device->set_life_expectancy(from, to, now);

  ASSERT_EQ(device->life_expectancy.first, from);
  ASSERT_EQ(device->life_expectancy.second, to);
  ASSERT_EQ(device->life_expectancy_stamp, now);
  ASSERT_EQ(device->metadata["life_expectancy_min"], "2000.000000");
  ASSERT_EQ(device->metadata["life_expectancy_max"], "1000.000000");
  ASSERT_EQ(device->metadata["life_expectancy_stamp"], "900.000000");
}

TEST_F(DeviceStateTest, SetLifeExpectancyZeros)
{
  utime_t from(0, 0);
  utime_t to(0, 0);
  utime_t now(0, 0);

  device->set_life_expectancy(from, to, now);

  ASSERT_EQ(device->life_expectancy.first, from);
  ASSERT_EQ(device->life_expectancy.second, to);
  ASSERT_EQ(device->life_expectancy_stamp, now);
  ASSERT_EQ(device->metadata["life_expectancy_min"], "");
  ASSERT_EQ(device->metadata["life_expectancy_max"], "");
  ASSERT_EQ(device->metadata["life_expectancy_stamp"], "");
}

TEST_F(DeviceStateTest, SetWearLevelInvalidRange)
{
  // > 1.0 (1.0 = 100% per conversion in DaemonServer.cc)
  device->set_wear_level(1.5f);

  ASSERT_EQ(device->wear_level, 1.5f);
  ASSERT_EQ(device->metadata["wear_level"], "1.5");

  device->set_wear_level(-1.5f); // < -1.0 which is used as not set / unknown

  ASSERT_EQ(device->wear_level, -1.5f);
  ASSERT_FALSE(device->metadata.contains("wear_level"));
}

TEST_F(DeviceStateTest, RemoveLifeExpectancyBeforeSet)
{
  device->rm_life_expectancy();

  ASSERT_EQ(device->life_expectancy.first, utime_t());
  ASSERT_EQ(device->life_expectancy.second, utime_t());
  ASSERT_EQ(device->life_expectancy_stamp, utime_t());
  ASSERT_FALSE(device->metadata.contains("life_expectancy_min"));
  ASSERT_FALSE(device->metadata.contains("life_expectancy_max"));
  ASSERT_FALSE(device->metadata.contains("life_expectancy_stamp"));
}

/* End Negative Tests */

class DaemonStateIndexTest : public ::testing::Test {
public:
  // cull() and cull_services() log through g_ceph_context
  static void
  SetUpTestSuite()
  {
    if (!cct) {
      std::vector<const char*> args = {"unittest_mgr_daemonstate"};
      cct = global_init(
          nullptr, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
          CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
      common_init_finish(cct.get());
    }
  }

protected:
  static inline boost::intrusive_ptr<CephContext> cct;
  DaemonStateIndex index;

  DaemonStatePtr
  make_daemon(
      const std::string& type,
      const std::string& name,
      const std::map<std::string, std::string>& meta = {})
  {
    auto state = std::make_shared<DaemonState>(index.types);
    state->key = DaemonKey{type, name};
    state->set_metadata(meta);
    return state;
  }

  bool
  device_attached(
      const std::string& devid,
      const DaemonKey& key,
      const std::tuple<std::string, std::string, std::string>& attachment)
  {
    bool found = false;
    index.with_device(devid, [&](const DeviceState& dev) {
      found = dev.daemons.contains(key) && dev.attachments.contains(attachment);
    });
    return found;
  }
};

// Metadata in the format the OSD and mon send it, see get_device_metadata()
const std::map<std::string, std::string> osd0_meta = {
    {"hostname", "host1"},
    {"device_ids", "sda=VENDOR_A_1,sdb=VENDOR_B_2"},
    {"device_paths",
     "sda=/dev/disk/by-path/pci-0000:00:01.0,"
     "sdb=/dev/disk/by-path/pci-0000:00:02.0"}};

TEST_F(DaemonStateIndexTest, SetMetadataParsesDevices)
{
  auto osd0 = make_daemon("osd", "0", osd0_meta);

  ASSERT_EQ(osd0->hostname, "host1");
  ASSERT_EQ(osd0->devices.size(), 2u);
  EXPECT_EQ(osd0->devices["VENDOR_A_1"], "sda");
  EXPECT_EQ(osd0->devices["VENDOR_B_2"], "sdb");
  ASSERT_EQ(osd0->devices_bypath.size(), 2u);
  EXPECT_EQ(
      osd0->devices_bypath["VENDOR_A_1"], "/dev/disk/by-path/pci-0000:00:01.0");
  EXPECT_EQ(
      osd0->devices_bypath["VENDOR_B_2"], "/dev/disk/by-path/pci-0000:00:02.0");
}

TEST_F(DaemonStateIndexTest, SetMetadataSkipsBlankIds)
{
  auto osd0 = make_daemon(
      "osd", "0",
      {{"device_ids", "sda=,sdb=VENDOR_B_2"},
       {"device_paths", "sda=/dev/disk/by-path/a,sdb=/dev/disk/by-path/b"}});

  ASSERT_EQ(osd0->devices.size(), 1u);
  EXPECT_EQ(osd0->devices["VENDOR_B_2"], "sdb");
  ASSERT_EQ(osd0->devices_bypath.size(), 1u);
  EXPECT_EQ(osd0->devices_bypath["VENDOR_B_2"], "/dev/disk/by-path/b");
}

TEST_F(DaemonStateIndexTest, SetMetadataReplacesDevices)
{
  auto osd0 = make_daemon("osd", "0", osd0_meta);
  osd0->set_metadata({{"hostname", "host1"}});

  EXPECT_TRUE(osd0->devices.empty());
  EXPECT_TRUE(osd0->devices_bypath.empty());
}

TEST_F(DaemonStateIndexTest, InsertAndLookup)
{
  auto osd0 = make_daemon("osd", "0", osd0_meta);
  auto mon_a = make_daemon("mon", "a", {{"hostname", "host2"}});
  index.insert(osd0);
  index.insert(mon_a);

  EXPECT_TRUE(index.exists(DaemonKey{"osd", "0"}));
  EXPECT_FALSE(index.exists(DaemonKey{"osd", "1"}));
  EXPECT_EQ(index.get(DaemonKey{"osd", "0"}), osd0);
  EXPECT_EQ(index.get(DaemonKey{"osd", "1"}), nullptr);
  EXPECT_EQ(index.get_all().size(), 2u);

  auto by_service = index.get_by_service("osd");
  ASSERT_EQ(by_service.size(), 1u);
  EXPECT_EQ(by_service.begin()->second, osd0);
  EXPECT_TRUE(index.get_by_service("mds").empty());

  auto by_server = index.get_by_server("host2");
  ASSERT_EQ(by_server.size(), 1u);
  EXPECT_EQ(by_server.begin()->second, mon_a);
  EXPECT_TRUE(index.get_by_server("host3").empty());
}

TEST_F(DaemonStateIndexTest, InsertWithoutHostname)
{
  // daemons register before their metadata arrives
  index.insert(make_daemon("osd", "0"));

  EXPECT_TRUE(index.exists(DaemonKey{"osd", "0"}));
  EXPECT_TRUE(index.get_by_server("").empty());
  index.with_daemons_by_server([](const auto& by_server) {
    EXPECT_TRUE(by_server.empty());
  });

  index.rm(DaemonKey{"osd", "0"});
  EXPECT_TRUE(index.get_all().empty());
}

TEST_F(DaemonStateIndexTest, ReinsertReplacesState)
{
  index.insert(make_daemon("osd", "0", osd0_meta));
  auto moved = make_daemon("osd", "0", {{"hostname", "host2"}});
  index.insert(moved);

  EXPECT_EQ(index.get_all().size(), 1u);
  EXPECT_EQ(index.get(DaemonKey{"osd", "0"}), moved);
  EXPECT_TRUE(index.get_by_server("host1").empty());
  EXPECT_EQ(index.get_by_server("host2").size(), 1u);
  index.with_daemons_by_server([](const auto& by_server) {
    EXPECT_FALSE(by_server.contains("host1"));
  });
  // the new state has no devices, so the old ones are dropped
  EXPECT_FALSE(index.with_device("VENDOR_A_1", [](const DeviceState&) {}));
  EXPECT_FALSE(index.with_device("VENDOR_B_2", [](const DeviceState&) {}));
}

TEST_F(DaemonStateIndexTest, DevicesFollowDaemons)
{
  const DaemonKey osd0_key{"osd", "0"};
  const DaemonKey mon_a_key{"mon", "a"};
  index.insert(make_daemon("osd", "0", osd0_meta));
  // a mon sharing one of the osd's devices, without a by-path path
  index.insert(make_daemon(
      "mon", "a", {{"hostname", "host1"}, {"device_ids", "sda=VENDOR_A_1"}}));

  EXPECT_TRUE(device_attached(
      "VENDOR_A_1", osd0_key,
      {"host1", "sda", "/dev/disk/by-path/pci-0000:00:01.0"}));
  EXPECT_TRUE(device_attached("VENDOR_A_1", mon_a_key, {"host1", "sda", ""}));
  EXPECT_TRUE(device_attached(
      "VENDOR_B_2", osd0_key,
      {"host1", "sdb", "/dev/disk/by-path/pci-0000:00:02.0"}));

  std::set<std::string> devids;
  index.list_devids_by_server("host1", &devids);
  EXPECT_EQ(devids, (std::set<std::string>{"VENDOR_A_1", "VENDOR_B_2"}));

  // a device is forgotten once no daemon uses it
  index.rm(osd0_key);
  EXPECT_FALSE(index.with_device("VENDOR_B_2", [](const DeviceState&) {}));
  index.with_device("VENDOR_A_1", [&](const DeviceState& dev) {
    EXPECT_EQ(dev.daemons.size(), 1u);
    EXPECT_TRUE(dev.daemons.contains(mon_a_key));
    EXPECT_EQ(dev.attachments.size(), 1u);
  });

  index.rm(mon_a_key);
  EXPECT_FALSE(index.with_device("VENDOR_A_1", [](const DeviceState&) {}));
}

TEST_F(DaemonStateIndexTest, DeviceWithMetadataOutlivesDaemons)
{
  index.insert(make_daemon("osd", "0", osd0_meta));
  index.with_device_write("VENDOR_A_1", [](DeviceState& dev) {
    dev.set_wear_level(0.5f);
  });
  index.rm(DaemonKey{"osd", "0"});

  // the stored wear level keeps the device around
  EXPECT_TRUE(index.with_device("VENDOR_A_1", [](const DeviceState& dev) {
    EXPECT_TRUE(dev.daemons.empty());
    EXPECT_TRUE(dev.attachments.empty());
  }));
  EXPECT_FALSE(index.with_device("VENDOR_B_2", [](const DeviceState&) {}));
}

TEST_F(DaemonStateIndexTest, RmUnknownDaemon)
{
  index.insert(make_daemon("osd", "0", osd0_meta));
  index.rm(DaemonKey{"osd", "1"});

  EXPECT_EQ(index.get_all().size(), 1u);
  EXPECT_EQ(index.get_by_server("host1").size(), 1u);
}

TEST_F(DaemonStateIndexTest, UpdateMetadataReindexes)
{
  auto osd0 = make_daemon("osd", "0", osd0_meta);
  index.insert(osd0);
  index.update_metadata(
      osd0, {{"hostname", "host2"},
             {"device_ids", "sdc=VENDOR_C_3"},
             {"device_paths", "sdc=/dev/disk/by-path/pci-0000:00:03.0"}});

  EXPECT_EQ(index.get(DaemonKey{"osd", "0"}), osd0);
  EXPECT_TRUE(index.get_by_server("host1").empty());
  EXPECT_EQ(index.get_by_server("host2").size(), 1u);
  EXPECT_FALSE(index.with_device("VENDOR_A_1", [](const DeviceState&) {}));
  EXPECT_FALSE(index.with_device("VENDOR_B_2", [](const DeviceState&) {}));
  EXPECT_TRUE(device_attached(
      "VENDOR_C_3", DaemonKey{"osd", "0"},
      {"host2", "sdc", "/dev/disk/by-path/pci-0000:00:03.0"}));
}

TEST_F(DaemonStateIndexTest, Cull)
{
  for (auto name : {"0", "1", "2"}) {
    index.insert(make_daemon("osd", name, {{"hostname", "host1"}}));
  }
  index.insert(make_daemon("mon", "a", {{"hostname", "host1"}}));
  // sorts right after "osd" and must not be treated as one
  index.insert(make_daemon("osdx", "0", {{"hostname", "host1"}}));

  index.cull("osd", {"1", "5"});

  EXPECT_FALSE(index.exists(DaemonKey{"osd", "0"}));
  EXPECT_TRUE(index.exists(DaemonKey{"osd", "1"}));
  EXPECT_FALSE(index.exists(DaemonKey{"osd", "2"}));
  EXPECT_TRUE(index.exists(DaemonKey{"mon", "a"}));
  EXPECT_TRUE(index.exists(DaemonKey{"osdx", "0"}));
  EXPECT_EQ(index.get_by_server("host1").size(), 3u);

  // culling a type that has no entries leaves everything alone
  index.cull("mds", {});
  EXPECT_EQ(index.get_all().size(), 3u);
}

TEST_F(DaemonStateIndexTest, CullServices)
{
  auto rgw = make_daemon("rgw", "foo", {{"hostname", "host1"}});
  rgw->service_daemon = true;
  auto nfs = make_daemon("nfs", "bar", {{"hostname", "host1"}});
  nfs->service_daemon = true;
  index.insert(rgw);
  index.insert(nfs);
  index.insert(make_daemon("osd", "0", {{"hostname", "host1"}}));

  // only service daemons of types that are gone are removed
  index.cull_services({"nfs"});

  EXPECT_FALSE(index.exists(DaemonKey{"rgw", "foo"}));
  EXPECT_TRUE(index.exists(DaemonKey{"nfs", "bar"}));
  EXPECT_TRUE(index.exists(DaemonKey{"osd", "0"}));
}

TEST_F(DaemonStateIndexTest, Updating)
{
  const DaemonKey key{"osd", "0"};
  EXPECT_FALSE(index.is_updating(key));
  index.notify_updating(key);
  EXPECT_TRUE(index.is_updating(key));
  EXPECT_FALSE(index.is_updating(DaemonKey{"osd", "1"}));
  index.clear_updating(key);
  EXPECT_FALSE(index.is_updating(key));
}
