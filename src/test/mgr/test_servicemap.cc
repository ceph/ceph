// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <fmt/format.h>

#include <map>
#include <sstream>
#include <string>

#include "common/Formatter.h"
#include "common/JSONFormatter.h"
#include "gtest/gtest.h"
#include "include/buffer.h"
#include "mgr/ServiceMap.h"
#include "msg/msg_types.h"

TEST(ServiceMapDaemon, EncodeDecode)
{
  ServiceMap::Daemon d_1;
  d_1.gid = 123;
  d_1.metadata["test"] = "ing";

  bufferlist bl;
  d_1.encode(bl, 0);
  auto p = bl.cbegin();

  ServiceMap::Daemon d_2;
  d_2.decode(p);

  EXPECT_EQ(d_2.gid, 123);
  EXPECT_EQ(d_2.metadata["test"], "ing");
}

TEST(ServiceMapDaemon, Dump)
{
  ServiceMap::Daemon test_daemon;
  test_daemon.gid = 11;

  entity_addr_t addr;
  addr.parse("127.0.0.1:1234", nullptr);
  test_daemon.addr = addr;
  test_daemon.start_epoch = 33;
  test_daemon.start_stamp = utime_t(55, 5500000);

  //dump_object will call ServiceMap::Daemon::dump wrapped in an open/close section
  JSONFormatter f;
  f.dump_object("daemon", test_daemon);

  std::ostringstream oss;
  f.flush(oss);

  std::string json_str;
  json_str = oss.str();

  EXPECT_TRUE(json_str.contains("\"gid\":11"));
  EXPECT_TRUE(json_str.contains("127.0.0.1:1234"));
  EXPECT_TRUE(json_str.contains("\"start_epoch\":33"));
  EXPECT_TRUE(json_str.contains("\"start_stamp\":\"55.005500\""));
}

TEST(ServiceMapService, GetSummary)
{
  ServiceMap::Service sms;
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
  ServiceMap::Service sms;
  sms.daemons["test"].task_status["task"] = "running";
  sms.daemons["test2"].task_status["task"] = "idle";

  const std::string expected =
      "\n"
      "    task:\n"
      "        svc.test: running\n"
      "        svc.test2: idle";
  EXPECT_EQ(sms.get_task_summary("svc"), expected);
}

TEST(ServiceMapService, CountMetadata)
{
  ServiceMap::Service sms;
  sms.daemons["daemon1"].metadata["test"] = "t1";
  sms.daemons["daemon2"].metadata["test"] = "t2";
  sms.daemons["daemon3"].task_status["abc"] = "xyz";

  std::map<std::string, int> metadata_info;
  sms.count_metadata("test", &metadata_info);

  EXPECT_EQ(metadata_info["t1"], 1);
  EXPECT_EQ(metadata_info["t2"], 1);
  EXPECT_EQ(metadata_info["unknown"], 1);
}

TEST(ServiceMapService, EncodeDecode)
{
  ServiceMap::Service s_1;
  s_1.summary = "summary";
  s_1.daemons["d"].gid = 99;

  bufferlist bl;
  s_1.encode(bl, 0);
  auto p = bl.cbegin();

  ServiceMap::Service s_2;
  s_2.decode(p);

  EXPECT_EQ(s_2.summary, "summary");
  EXPECT_EQ(s_2.daemons["d"].gid, 99);
}

TEST(ServiceMapService, Dump)
{
  entity_addr_t addr;
  addr.parse("127.0.0.1:1234", nullptr);

  ServiceMap::Service sms;
  sms.summary = "testSummary";
  sms.daemons["d123"].gid = 123;
  sms.daemons["d123"].addr = addr;

  JSONFormatter f;
  f.dump_object("service", sms);

  std::ostringstream oss;
  f.flush(oss);

  std::string json_str;
  json_str = oss.str();

  const std::string expected =
      R"({"daemons":{"summary":"testSummary","d123":{"start_epoch":0,"start_stamp":"0.000000","gid":123,"addr":"127.0.0.1:1234/0","metadata":{},"task_status":{}}}})";
  EXPECT_EQ(json_str, expected);
}

TEST(ServiceMapMap, EncodeDecode)
{
  ServiceMap m_1;
  m_1.epoch = 123;
  m_1.services["svc"].summary = "abc";

  bufferlist bl;
  m_1.encode(bl, 0);
  auto p = bl.cbegin();

  ServiceMap m_2;
  m_2.decode(p);

  EXPECT_EQ(m_2.epoch, 123);
  EXPECT_EQ(m_2.services["svc"].summary, "abc");
}

TEST(ServiceMapMap, Dump)
{
  ServiceMap sm;
  sm.epoch = 123;
  sm.services["svc"].summary = "testSummary";

  JSONFormatter f;
  f.dump_object("servicemap", sm);

  std::ostringstream oss;
  f.flush(oss);

  std::string json_str;
  json_str = oss.str();

  const std::string expected =
      R"({"epoch":123,"modified":"0.000000","services":{"svc":{"daemons":{"summary":"testSummary"}}}})";
  EXPECT_EQ(json_str, expected);
}

/* Begin Negative Tests */

TEST(ServiceMapDaemon, EncodeDecodeWithEmptyMetadata)
{
  ServiceMap::Daemon d_1;

  d_1.gid = 123;
  bufferlist bl;
  d_1.encode(bl, 0);
  auto p = bl.cbegin();

  ServiceMap::Daemon d_2;
  d_2.decode(p);

  EXPECT_EQ(d_2.gid, 123);
  EXPECT_TRUE(d_2.metadata.empty());
}

TEST(ServiceMapDaemon, DumpWithEmptyMetadata)
{
  std::ostringstream oss;
  JSONFormatter f(true);
  ServiceMap::Daemon d;

  d.gid = 123;
  f.dump_object("daemon", d);
  f.flush(oss);
  const std::string output = oss.str();

  EXPECT_TRUE(output.contains("\"gid\": 123"));
  EXPECT_TRUE(output.contains("\"metadata\": {}"));
}

TEST(ServiceMapService, GetSummaryEmptyString)
{
  ServiceMap::Service s;

  s.summary = "";

  EXPECT_EQ(s.get_summary(), "no daemons active");
}

TEST(ServiceMapService, GetTaskSummaryEmptyDaemons)
{
  ServiceMap::Service s;
  std::string summary = s.get_task_summary("svc");

  EXPECT_EQ(summary, "");
}

TEST(ServiceMapService, GetTaskSummaryEmptyTaskStatus)
{
  ServiceMap::Service s;

  s.daemons["d1"].gid = 1;
  std::string summary = s.get_task_summary("svc");

  EXPECT_EQ(summary, "");
}

TEST(ServiceMapService, CountMetadataEmptyKey)
{
  ServiceMap::Service s;
  std::map<std::string, int> out;

  s.daemons["d1"].metadata["zone"] = "z1";
  s.count_metadata("", &out);

  EXPECT_EQ(out["unknown"], 1);
}

TEST(ServiceMapService, CountMetadataDNEKey)
{
  ServiceMap::Service s;
  std::map<std::string, int> out;

  s.daemons["d1"].metadata["zone"] = "z1";
  s.count_metadata("DNE", &out);

  EXPECT_EQ(out["unknown"], 1);
}

TEST(ServiceMapService, EncodeDecodeEmptyDaemons)
{
  ServiceMap::Service s_1;
  ServiceMap::Service s_2;
  bufferlist bl;

  s_1.summary = "summary";
  s_1.encode(bl, 0);
  auto p = bl.cbegin();
  s_2.decode(p);

  EXPECT_EQ(s_2.summary, "summary");
  EXPECT_TRUE(s_2.daemons.empty());
}

TEST(ServiceMapMap, EncodeDecodeEmptyServices)
{
  ServiceMap m_1;
  ServiceMap m_2;
  bufferlist bl;

  m_1.epoch = 123;
  m_1.encode(bl, 0);
  auto p = bl.cbegin();
  m_2.decode(p);

  EXPECT_EQ(m_2.epoch, 123);
  EXPECT_TRUE(m_2.services.empty());
}

/* End Negative Tests*/
