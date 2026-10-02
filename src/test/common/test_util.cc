// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sstream>
#include <filesystem>

#include "gtest/gtest.h"
#include "common/ceph_context.h"
#include "common/JSONFormatter.h"
#include "include/util.h"

using namespace std;

namespace fs = std::filesystem;

TEST(util, dump_service_ids)
{
  const map<string, vector<int>> services {
    {"host-b", {3}},
    {"host-a", {1, 2}}
  };
  ostringstream output;
  JSONFormatter formatter(false);

  dump_services(&formatter, services, "osd");
  formatter.flush(output);

  EXPECT_EQ(R"({"host-a":[1,2],"host-b":[3]})", output.str());
}

TEST(util, dump_service_names)
{
  const map<string, vector<string>> services {
    {"host-b", {"standby"}},
    {"host-a", {"alpha", "bravo"}}
  };
  ostringstream output;
  JSONFormatter formatter(false);

  dump_services(&formatter, services, "mds");
  formatter.flush(output);

  EXPECT_EQ(R"({"host-a":["alpha","bravo"],"host-b":["standby"]})",
            output.str());
}

#if defined(__linux__)
TEST(util, collect_sys_info)
{
  if (!fs::exists("/etc/os-release")) {
    GTEST_SKIP() << "skipping as '/etc/os-release' does not exist";
  }

  map<string, string> sys_info;

  boost::intrusive_ptr<CephContext> cct{new CephContext(CEPH_ENTITY_TYPE_CLIENT), false};

  collect_sys_info(&sys_info, cct.get());

  ASSERT_TRUE(sys_info.find("distro") != sys_info.end());
  ASSERT_TRUE(sys_info.find("distro_description") != sys_info.end());
}

#endif
