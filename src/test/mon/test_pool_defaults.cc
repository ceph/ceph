// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Test the osd_pool_default_* options that ceph osd pool create takes its
 * defaults from.
 */

#include "gtest/gtest.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/config_proxy.h"
#include "global/global_context.h"
#include "osd/OSDMap.h"

#include <memory>
#include <string>

using namespace std;

class PoolDefaultsTest : public ::testing::Test {
protected:
  unique_ptr<CephContext> cct;

  void SetUp() override {
    cct.reset(new CephContext(CEPH_ENTITY_TYPE_MON));
    g_ceph_context = cct.get();
    common_init_finish(g_ceph_context);
  }

  void TearDown() override {
    g_ceph_context = nullptr;
  }

  void set(const string& name, const string& value) {
    ASSERT_EQ(0, cct->_conf.set_val(name, value));
  }

  uint64_t total_size() const {
    return cct->_conf.get_osd_pool_default_total_size();
  }

  pg_pool_t simple_pool() {
    OSDMap osdmap;
    uuid_d fsid;
    EXPECT_EQ(0, osdmap.build_simple_with_pool(cct.get(), 1, fsid, 3, 3, 3));
    const int64_t id = osdmap.lookup_pg_pool_name("rbd");
    EXPECT_GE(id, 0);
    return *osdmap.get_pg_pool(id);
  }
};

// Test the built-in replicas per zone default is 3
TEST_F(PoolDefaultsTest, SizeDefaultIsThree) {
  EXPECT_EQ(3u, cct->_conf.get_val<uint64_t>("osd_pool_default_size"));
}

// Test failure when osd_pool_default_size is above the maximum
TEST_F(PoolDefaultsTest, SizeAboveMaximumFails) {
  EXPECT_NE(0, cct->_conf.set_val("osd_pool_default_size", "11"));
}

// Test the zone failure domain default keeps its built-in value
TEST_F(PoolDefaultsTest, ZoneFailureDomainDefault) {
  EXPECT_EQ("datacenter",
            cct->_conf.get_val<string>("osd_pool_default_zone_failure_domain"));
}

// Test the OSD failure domain default is host
TEST_F(PoolDefaultsTest, OsdFailureDomainDefault) {
  EXPECT_EQ("host",
            cct->_conf.get_val<string>("osd_pool_default_osd_failure_domain"));
}

// Test the CRUSH root default is the default root
TEST_F(PoolDefaultsTest, RootDefault) {
  EXPECT_EQ("default", cct->_conf.get_val<string>("osd_pool_default_root"));
}

// Test the device class default is empty, meaning any class
TEST_F(PoolDefaultsTest, ClassDefaultIsEmpty) {
  EXPECT_EQ("", cct->_conf.get_val<string>("osd_pool_default_class"));
}

// Test the placement defaults can be changed
TEST_F(PoolDefaultsTest, PlacementDefaultsCanBeSet) {
  set("osd_pool_default_zone_failure_domain", "rack");
  set("osd_pool_default_osd_failure_domain", "osd");
  set("osd_pool_default_root", "dc1");
  set("osd_pool_default_class", "ssd");
  EXPECT_EQ("rack",
            cct->_conf.get_val<string>("osd_pool_default_zone_failure_domain"));
  EXPECT_EQ("osd",
            cct->_conf.get_val<string>("osd_pool_default_osd_failure_domain"));
  EXPECT_EQ("dc1", cct->_conf.get_val<string>("osd_pool_default_root"));
  EXPECT_EQ("ssd", cct->_conf.get_val<string>("osd_pool_default_class"));
}

// Test the replica options and the old zone option name are gone
TEST_F(PoolDefaultsTest, OldOptionNamesAreGone) {
  EXPECT_EQ(nullptr, cct->_conf.find_option("osd_pool_default_replica"));
  EXPECT_EQ(nullptr, cct->_conf.find_option("osd_pool_stretch_default_replica"));
  EXPECT_EQ(nullptr, cct->_conf.find_option("default_crush_zone_failure_domain"));
}

// Test the size of a new pool is 3 by default
TEST_F(PoolDefaultsTest, TotalSizeDefaultIsThree) {
  EXPECT_EQ(3u, total_size());
}

// Test the size of a new pool is num_zones times the replicas per zone
TEST_F(PoolDefaultsTest, TotalSizeTwoZones) {
  set("osd_pool_default_num_zones", "2");
  set("osd_pool_default_size", "3");
  EXPECT_EQ(6u, total_size());
}

// Test a simple OSDMap's pool has the default replica count
TEST_F(PoolDefaultsTest, SimplePoolTakesSize) {
  set("osd_pool_default_size", "2");
  const pg_pool_t p = simple_pool();
  EXPECT_EQ(2u, p.size);
  EXPECT_EQ(2u, p.replica);
  EXPECT_EQ(1u, p.num_zones);
}
