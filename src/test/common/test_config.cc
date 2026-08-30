// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <iostream> // for std::cout

#include "common/ceph_argparse.h"
#include "common/config_proxy.h"
#include "common/errno.h"
#include "gtest/gtest.h"
#include "common/hostname.h"

using namespace std;

extern std::string exec(const char* cmd); // defined in test_hostname.cc

class test_config_proxy : public ConfigProxy, public ::testing::Test {
public:

  test_config_proxy()
    : ConfigProxy{true}, Test()
  {}

  void test_expand_meta() {
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
      std::string hostname = ceph_get_short_hostname();
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

TEST_F(test_config_proxy, expand_meta)
{
  test_expand_meta();
}

TEST(md_config_t, parse_env)
{
  {
    ConfigProxy conf{false};
    setenv("POD_MEMORY_REQUEST", "1", 1);
    conf.parse_env(CEPH_ENTITY_TYPE_OSD);
  }
  {
    ConfigProxy conf{false};
    setenv("POD_MEMORY_REQUEST", "0", 1);
    conf.parse_env(CEPH_ENTITY_TYPE_OSD);
  }
  {
    ConfigProxy conf{false};
    setenv("CEPH_KEYRING", "", 1);
    conf.parse_env(CEPH_ENTITY_TYPE_OSD);
  }
}

TEST(md_config_t, set_val)
{
  int buf_size = 1024;
  ConfigProxy conf{false};
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

  using namespace std::chrono;

  using days_t = duration<int, std::ratio<3600 * 24>>;

  struct testcase {
    std::string s;
    std::chrono::seconds r;
  };
  std::vector<testcase> good = {
    { "23"s, duration_cast<seconds>(seconds{23}) },
    { " 23 "s, duration_cast<seconds>(seconds{23}) },
    { " 23s "s, duration_cast<seconds>(seconds{23}) },
    { " 23 s "s, duration_cast<seconds>(seconds{23}) },
    { " 23 sec "s, duration_cast<seconds>(seconds{23}) },
    { "23 second "s, duration_cast<seconds>(seconds{23}) },
    { "23 seconds"s, duration_cast<seconds>(seconds{23}) },
    { "2m5s"s,  duration_cast<seconds>(seconds{2*60+5}) },
    { "2 m 5 s "s,  duration_cast<seconds>(seconds{2*60+5}) },
    { "2 m5"s,  duration_cast<seconds>(seconds{2*60+5}) },
    { "2 min5"s,  duration_cast<seconds>(seconds{2*60+5}) },
    { "2 minutes  5"s,  duration_cast<seconds>(seconds{2*60+5}) },
    { "1w"s, duration_cast<seconds>(seconds{3600*24*7}) },
    { "1wk"s, duration_cast<seconds>(seconds{3600*24*7}) },
    { "1week"s, duration_cast<seconds>(seconds{3600*24*7}) },
    { "1weeks"s, duration_cast<seconds>(seconds{3600*24*7}) },
    { "1month"s, duration_cast<seconds>(seconds{3600*24*30}) },
    { "1months"s, duration_cast<seconds>(seconds{3600*24*30}) },
    { "1mo"s, duration_cast<seconds>(seconds{3600*24*30}) },
    { "1y"s, duration_cast<seconds>(seconds{3600*24*365}) },
    { "1yr"s, duration_cast<seconds>(seconds{3600*24*365}) },
    { "1year"s, duration_cast<seconds>(seconds{3600*24*365}) },
    { "1years"s, duration_cast<seconds>(seconds{3600*24*365}) },
    { "1d2h3m4s"s,
      duration_cast<seconds>(days_t{1}) +
      duration_cast<seconds>(hours{2}) +
      duration_cast<seconds>(minutes{3}) +
      duration_cast<seconds>(seconds{4}) },
    { "1 days 2 hours 4 minutes"s,
      duration_cast<seconds>(days_t{1}) +
      duration_cast<seconds>(hours{2}) +
      duration_cast<seconds>(minutes{4}) },
  };

  for (auto& i : good) {
    cout << "good: " << i.s << " -> " << i.r.count() << std::endl;
    EXPECT_EQ(0, conf.set_val("mgr_tick_period", i.s, nullptr));
    EXPECT_EQ(i.r.count(), conf.get_val<seconds>("mgr_tick_period").count());
  }

  std::vector<std::string> bad = {
    "12x",
    "_ 12",
    "1 2",
    "21 centuries",
    "1 y m",
  };
  for (auto& i : bad) {
    std::stringstream err;
    EXPECT_EQ(-EINVAL, conf.set_val("mgr_tick_period", i, &err));
    cout << "bad: " << i << " -> " << err.str() << std::endl;
  }

  for (int i = 0; i < 100; ++i) {
    std::chrono::seconds j = std::chrono::seconds(rand());
    string s = exact_timespan_str(j);
    std::chrono::seconds k = parse_timespan(s);
    cout << "rt: " << j.count() << " -> " << s << " -> " << k.count() << std::endl;
    EXPECT_EQ(j.count(), k.count());
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

TEST(daemon_config, drop_unknown_conf_opts)
{
  // compare by value: the arguments are pointers, and comparing those would
  // only be testing whether the compiler happened to pool the literals
  auto as_strings = [](const std::vector<const char*>& args) {
    return std::vector<std::string>(args.begin(), args.end());
  };
  // an unknown config given as an argument is dropped and reported, so that
  // the caller does not treat it as a fatal unrecognized argument
  {
    std::vector<const char*> args{"--debug_foijdf=1"};
    EXPECT_EQ(std::vector<std::string>{"debug_foijdf"},
	      ceph_argparse_drop_unknown_conf_opts(args));
    EXPECT_TRUE(args.empty());
  }
  // a bare flag may be a command argument, so it is left for the caller
  {
    std::vector<const char*> args{"--hot-standby"};
    EXPECT_TRUE(ceph_argparse_drop_unknown_conf_opts(args).empty());
    EXPECT_EQ(1u, args.size());
  }
  // so is anything that is not an option at all
  {
    std::vector<const char*> args{"a", "-i", "-"};
    EXPECT_TRUE(ceph_argparse_drop_unknown_conf_opts(args).empty());
    EXPECT_EQ(3u, args.size());
  }
  // an option name is required
  {
    std::vector<const char*> args{"--=1"};
    EXPECT_TRUE(ceph_argparse_drop_unknown_conf_opts(args).empty());
    EXPECT_EQ(1u, args.size());
  }
  // only the offending options are removed, order is otherwise preserved
  {
    std::vector<const char*> args{"-i", "a", "--debug_a=1", "-f",
				  "--debug_b=2", "--hot-standby"};
    std::vector<std::string> expected{"debug_a", "debug_b"};
    EXPECT_EQ(expected, ceph_argparse_drop_unknown_conf_opts(args));
    std::vector<std::string> remaining{"-i", "a", "-f", "--hot-standby"};
    EXPECT_EQ(remaining, as_strings(args));
  }
  // the value may be a separate argument: both are dropped
  {
    std::vector<const char*> args{"--debug_ms", "1"};
    EXPECT_EQ(std::vector<std::string>{"debug_ms"},
	      ceph_argparse_drop_unknown_conf_opts(args));
    EXPECT_TRUE(args.empty());
  }
  // ... but only when there is something that can be a value
  {
    std::vector<const char*> args{"--debug_ms"};
    EXPECT_TRUE(ceph_argparse_drop_unknown_conf_opts(args).empty());
    EXPECT_EQ(1u, args.size());
  }
  // an option is not the value of the option before it
  {
    std::vector<const char*> args{"--debug_ms", "--debug_mds=1"};
    EXPECT_EQ(std::vector<std::string>{"debug_mds"},
	      ceph_argparse_drop_unknown_conf_opts(args));
    std::vector<std::string> remaining{"--debug_ms"};
    EXPECT_EQ(remaining, as_strings(args));
  }
  // a value that looks like an option is left alone, so that a bare flag is
  // still rejected rather than silently swallowing the next argument
  {
    std::vector<const char*> args{"--mds_numa_nod", "-1"};
    EXPECT_TRUE(ceph_argparse_drop_unknown_conf_opts(args).empty());
    EXPECT_EQ(2u, args.size());
  }
  // a value may itself contain an '=', and may be empty
  {
    std::vector<const char*> args{"--debug_a=b=c", "--debug_b="};
    std::vector<std::string> expected{"debug_a", "debug_b"};
    EXPECT_EQ(expected, ceph_argparse_drop_unknown_conf_opts(args));
    EXPECT_TRUE(args.empty());
  }
  // everything past a double dash belongs to the caller
  {
    std::vector<const char*> args{"--debug_a=1", "--", "--debug_b=2"};
    EXPECT_EQ(std::vector<std::string>{"debug_a"},
	      ceph_argparse_drop_unknown_conf_opts(args));
    std::vector<std::string> remaining{"--", "--debug_b=2"};
    EXPECT_EQ(remaining, as_strings(args));
  }
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
