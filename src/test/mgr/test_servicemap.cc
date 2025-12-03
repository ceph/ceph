// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/JSONFormatter.h"
#include "common/Formatter.h"
#include "mgr/ServiceMap.h"
#include "include/buffer.h"
#include "msg/msg_types.h"
#include "gtest/gtest.h"

#include <fmt/format.h>
#include <sstream>
#include <string>
#include <map>

TEST(ServiceDaemon, Dump)
{
  ServiceMap::Daemon test_daemon;
  std::ostringstream oss;
  JSONFormatter f;
  std::string json_str;
  entity_addr_t addr;

  test_daemon.gid = 11;
  addr.parse("127.0.0.1:1234", nullptr);
  test_daemon.addr = addr;  
  test_daemon.start_epoch = 33;
  test_daemon.start_stamp = utime_t(55, 5500000);

  //dump_object will call ServiceMap::Daemon::dump wrapped in an open/close section
  f.dump_object("daemon", test_daemon);
  f.flush(oss);
  json_str = oss.str();

  ASSERT_TRUE(json_str.find("\"gid\":11") != std::string::npos);
  ASSERT_TRUE(json_str.find("127.0.0.1:1234") != std::string::npos);
  ASSERT_TRUE(json_str.find("\"start_epoch\":33") != std::string::npos);
  ASSERT_TRUE(json_str.find("\"start_stamp\":\"55.005500\"") != std::string::npos);

}

TEST(ServiceMapService, GetSummary)
{
  ServiceMap::Service sms;
  EXPECT_EQ(sms.get_summary(), "no daemons active");
  sms.summary = "test summary";
  EXPECT_EQ(sms.get_summary(), "test summary");
}

TEST(ServiceMapService, HasRunningTasks)
{
  ServiceMap::Service sms;
  EXPECT_FALSE(sms.has_running_tasks());
  sms.daemons["test"].task_status["task"] = "running";
  EXPECT_TRUE(sms.has_running_tasks());
}

TEST(ServiceMapService, GetTaskSummary)
{
  const char* expected =
    "\n"
    "    task:\n"
    "        svc.test: running\n"
    "        svc.test2: idle";
  ServiceMap::Service sms;
  std::string summary;

  sms.daemons["test"].task_status["task"] = "running";
  sms.daemons["test2"].task_status["task"] = "idle";
  summary = sms.get_task_summary("svc");

  EXPECT_EQ(summary, expected);
}

TEST(ServiceMapService, CountMetadata)
{
  ServiceMap::Service sms;
  std::map<std::string, int> metadata_info;

  sms.daemons["daemon1"].metadata["test"] = "t1";
  sms.daemons["daemon2"].metadata["test"] = "t2";
  sms.daemons["daemon3"].task_status["abc"] = "xyz";
  sms.count_metadata("test", &metadata_info);

  EXPECT_EQ(metadata_info["t1"], 1);
  EXPECT_EQ(metadata_info["t2"], 1);
  EXPECT_EQ(metadata_info["unknown"], 1);
}

TEST(ServiceMapService, Dump)
{
  ServiceMap::Service sms;
  std::ostringstream oss;
  JSONFormatter f;
  entity_addr_t addr;
  std::string json_str;
  std::string expected = R"({"daemons":{"summary":"testSummary","d123":{"start_epoch":0,"start_stamp":"0.000000","gid":123,"addr":"127.0.0.1:1234/0","metadata":{},"task_status":{}}}})";

  addr.parse("127.0.0.1:1234", nullptr);
  sms.summary = "testSummary";
  sms.daemons["d123"].gid = 123;
  sms.daemons["d123"].addr = addr;
  f.dump_object("service", sms);
  f.flush(oss);
  json_str = oss.str();

  EXPECT_EQ(json_str, expected);
}

TEST(ServiceMapMap, Dump)
{
  std::ostringstream oss;
  JSONFormatter f;
  std::string json_str;
  ServiceMap sm;
  std::string expected = R"({"epoch":123,"modified":"0.000000","services":{"svc":{"daemons":{"summary":"testSummary"}}}})";

  sm.epoch = 123;
  sm.services["svc"].summary = "testSummary";
  f.dump_object("servicemap", sm);
  f.flush(oss);
  json_str = oss.str();

  EXPECT_EQ(json_str, expected);
}
