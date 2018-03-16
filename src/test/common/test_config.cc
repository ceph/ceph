// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 *
 */
#include "common/config.h"
#include "common/errno.h"
#include "gtest/gtest.h"

extern std::string exec(const char* cmd); // defined in test_hostname.cc

class test_md_config_t : public md_config_t, public ::testing::Test {
public:

  test_md_config_t()
    : md_config_t(true), Test()
  {}

  void test_expand_meta() {
    Mutex::Locker l(lock);
    // successfull meta expansion $run_dir and ${run_dir}
    {
      ostringstream oss;
      std::string before = " BEFORE ";
      std::string after = " AFTER ";

      std::string val(before + "$run_dir${run_dir}" + after);
      early_expand_meta(val, &oss);
      EXPECT_EQ(before + "/var/run/ceph/var/run/ceph" + after, val);
      EXPECT_EQ("", oss.str());
    }
    {
      ostringstream oss;
      std::string before = " BEFORE ";
      std::string after = " AFTER ";

      std::string val(before + "$$1$run_dir$2${run_dir}$3$" + after);
      early_expand_meta(val, &oss);
      EXPECT_EQ(before + "$$1/var/run/ceph$2/var/run/ceph$3$" + after, val);
      EXPECT_EQ("", oss.str());
    }
    {
      ostringstream oss;
      std::string before = " BEFORE ";
      std::string after = " AFTER ";
      std::string val(before + "$host${host}" + after);
      early_expand_meta(val, &oss);
      std::string hostname = exec("hostname -s");
      EXPECT_EQ(before + hostname + hostname + after, val);
      EXPECT_EQ("", oss.str());
    }
    // no meta expansion if variables are unknown
    {
      ostringstream oss;
      std::string expected = "expect $foo and ${bar} to not expand";
      std::string val = expected;
      early_expand_meta(val, &oss);
      EXPECT_EQ(expected, val);
      EXPECT_EQ("", oss.str());
    }
    // recursive variable expansion
    {
      std::string host = "localhost";
      EXPECT_EQ(0, set_val("host", host.c_str()));

      std::string mon_host = "$cluster_network";
      EXPECT_EQ(0, set_val("mon_host", mon_host.c_str()));

      std::string lockdep = "true";
      EXPECT_EQ(0, set_val("lockdep", lockdep.c_str()));

      std::string cluster_network = "$public_network $public_network $lockdep $host";
      EXPECT_EQ(0, set_val("cluster_network", cluster_network.c_str()));

      std::string public_network = "NETWORK";
      EXPECT_EQ(0, set_val("public_network", public_network.c_str()));

      ostringstream oss;
      std::string val = "$mon_host";
      early_expand_meta(val, &oss);
      EXPECT_EQ(public_network + " " +
                public_network + " " +
                lockdep + " " +
                "localhost", val);
      EXPECT_EQ("", oss.str());
    }
    // variable expansion loops are non fatal
    {
      std::string mon_host = "$cluster_network";
      EXPECT_EQ(0, set_val("mon_host", mon_host.c_str()));

      std::string cluster_network = "$public_network";
      EXPECT_EQ(0, set_val("cluster_network", cluster_network.c_str()));

      std::string public_network = "$mon_host";
      EXPECT_EQ(0, set_val("public_network", public_network.c_str()));

      ostringstream oss;
      std::string val = "$mon_host";
      early_expand_meta(val, &oss);
      EXPECT_EQ("$mon_host", val);
      const char *expected_oss =
        "variable expansion loop at mon_host=$cluster_network\n"
        "expansion stack:\n"
        "public_network=$mon_host\n"
        "cluster_network=$public_network\n"
        "mon_host=$cluster_network\n";
      EXPECT_EQ(expected_oss, oss.str());
    }
  }
};

TEST_F(test_md_config_t, expand_meta)
{
  test_expand_meta();
}

TEST(md_config_t, set_val)
{
  int buf_size = 1024;
  md_config_t conf;
  {
    char *run_dir = (char*)malloc(buf_size);
    EXPECT_EQ(0, conf.get_val("run_dir", &run_dir, buf_size));
    EXPECT_EQ(0, conf.set_val("admin_socket", "$run_dir"));
    char *admin_socket = (char*)malloc(buf_size);
    EXPECT_EQ(0, conf.get_val("admin_socket", &admin_socket, buf_size));
    EXPECT_EQ(std::string(run_dir), std::string(admin_socket));
    free(run_dir);
    free(admin_socket);
  }
  // set_val should support SI conversion
  {
    auto expected = Option::size_t{512 << 20};
    EXPECT_EQ(0, conf.set_val("mgr_osd_bytes", "512M", nullptr));
    EXPECT_EQ(expected, conf.get_val<Option::size_t>("mgr_osd_bytes"));
    EXPECT_EQ(-EINVAL, conf.set_val("mgr_osd_bytes", "512 bits", nullptr));
    EXPECT_EQ(expected, conf.get_val<Option::size_t>("mgr_osd_bytes"));
  }
  // set_val should support 1 days 2 hours 4 minutes
  {
    using namespace std::chrono;
    const string s{"1 days 2 hours 4 minutes"};
    using days_t = duration<int, std::ratio<3600 * 24>>;
    auto expected = (duration_cast<seconds>(days_t{1}) +
		     duration_cast<seconds>(hours{2}) +
		     duration_cast<seconds>(minutes{4}));
    EXPECT_EQ(0, conf.set_val("mgr_tick_period",
			      "1 days 2 hours 4 minutes", nullptr));
    EXPECT_EQ(expected.count(), conf.get_val<seconds>("mgr_tick_period").count());
    EXPECT_EQ(-EINVAL, conf.set_val("mgr_tick_period", "21 centuries", nullptr));
    EXPECT_EQ(expected.count(), conf.get_val<seconds>("mgr_tick_period").count());
  }
}

TEST(Option, validation)
{
  Option opt_int("foo", Option::TYPE_INT, Option::LEVEL_BASIC);
  opt_int.set_min_max(5, 10);

  std::string msg;
  EXPECT_EQ(-EINVAL, opt_int.validate(Option::value_t(int64_t(4)), &msg));
  EXPECT_EQ(-EINVAL, opt_int.validate(Option::value_t(int64_t(11)), &msg));
  EXPECT_EQ(0, opt_int.validate(Option::value_t(int64_t(7)), &msg));

  Option opt_enum("foo", Option::TYPE_STR, Option::LEVEL_BASIC);
  opt_enum.set_enum_allowed({"red", "blue"});
  EXPECT_EQ(0, opt_enum.validate(Option::value_t(std::string("red")), &msg));
  EXPECT_EQ(0, opt_enum.validate(Option::value_t(std::string("blue")), &msg));
  EXPECT_EQ(-EINVAL, opt_enum.validate(Option::value_t(std::string("green")), &msg));

  Option opt_validator("foo", Option::TYPE_INT, Option::LEVEL_BASIC);
  opt_validator.set_validator([](std::string *value, std::string *error_message){
      if (*value == std::string("one")) {
        *value = "1";
        return 0;
      } else if (*value == std::string("666")) {
        return -EINVAL;
      } else {
        return 0;
      }
  });

  std::string input = "666";  // An explicitly forbidden value
  EXPECT_EQ(-EINVAL, opt_validator.pre_validate(&input, &msg));
  EXPECT_EQ(input, "666");

  input = "123";  // A permitted value with no special behaviour
  EXPECT_EQ(0, opt_validator.pre_validate(&input, &msg));
  EXPECT_EQ(input, "123");

  input = "one";  // A value that has a magic conversion
  EXPECT_EQ(0, opt_validator.pre_validate(&input, &msg));
  EXPECT_EQ(input, "1");
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ;
 *   make unittest_config &&
 *    valgrind \
 *    --max-stackframe=20000000 --tool=memcheck \
 *   ./unittest_config # --gtest_filter=md_config_t.set_val
 * "
 * End:
 */
