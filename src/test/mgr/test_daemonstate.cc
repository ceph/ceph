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

  ASSERT_EQ(device->wear_level, 0.0f);
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

  ASSERT_EQ(device->wear_level, 0.0f);
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
