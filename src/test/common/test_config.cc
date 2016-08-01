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

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

static struct config_option config_optionsp[] = {
#define OPTION(name, type, def_val) \
       { STRINGIFY(name), type, offsetof(struct md_config_t, name) },
#define SUBSYS(name, log, gather)
#define DEFAULT_SUBSYS(log, gather)
#include "common/config_opts.h"
#undef OPTION
#undef SUBSYS
#undef DEFAULT_SUBSYS
};

static const int NUM_CONFIG_OPTIONS = sizeof(config_optionsp) / sizeof(config_option);

class test_md_config_t : public md_config_t, public ::testing::Test {
public:
  void test_expand_meta() {
    Mutex::Locker l(lock);
    // successfull meta expansion $run_dir and ${run_dir}
    {
      ostringstream oss;
      std::string before = " BEFORE ";
      std::string after = " AFTER ";
      std::string val(before + "$run_dir${run_dir}" + after);
      EXPECT_TRUE(expand_meta(val, &oss));
      EXPECT_EQ(before + "/var/run/ceph/var/run/ceph" + after, val);
      EXPECT_EQ("", oss.str());
    }
    // no meta expansion if variables are unknown
    {
      ostringstream oss;
      std::string expected = "expect $foo and ${bar} to not expand";
      std::string val = expected;
      EXPECT_FALSE(expand_meta(val, &oss));
      EXPECT_EQ(expected, val);
      EXPECT_EQ("", oss.str());
    }
    // recursive variable expansion
    {
      std::string mon_host = "$cluster_network";
      EXPECT_EQ(0, set_val("mon_host", mon_host.c_str(), false));

      std::string lockdep = "true";
      EXPECT_EQ(0, set_val("lockdep", lockdep.c_str(), false));

      std::string cluster_network = "$public_network $public_network $lockdep $host";
      EXPECT_EQ(0, set_val("cluster_network", cluster_network.c_str(), false));

      std::string public_network = "NETWORK";
      EXPECT_EQ(0, set_val("public_network", public_network.c_str(), false));

      ostringstream oss;
      std::string val = "$mon_host";
      EXPECT_TRUE(expand_meta(val, &oss));
      EXPECT_EQ(public_network + " " +
                public_network + " " +
                lockdep + " " +
                "localhost", val);
      EXPECT_EQ("", oss.str());
    }
    // variable expansion loops are non fatal
    {
      std::string mon_host = "$cluster_network";
      EXPECT_EQ(0, set_val("mon_host", mon_host.c_str(), false));

      std::string cluster_network = "$public_network";
      EXPECT_EQ(0, set_val("cluster_network", cluster_network.c_str(), false));

      std::string public_network = "$mon_host";
      EXPECT_EQ(0, set_val("public_network", public_network.c_str(), false));

      ostringstream oss;
      std::string val = "$mon_host";
      EXPECT_TRUE(expand_meta(val, &oss));
      EXPECT_EQ("$cluster_network", val);
      const char *expected_oss =
        "variable expansion loop at mon_host=$cluster_network\n"
        "expansion stack: \n"
        "public_network=$mon_host\n"
        "cluster_network=$public_network\n"
        "mon_host=$cluster_network\n";
      EXPECT_EQ(expected_oss, oss.str());
    }
  }

  void test_expand_all_meta() {
    Mutex::Locker l(lock);
    int before_count = 0, data_dir = 0;
    for (int i = 0; i < NUM_CONFIG_OPTIONS; i++) {
      config_option *opt = config_optionsp + i;
      if (opt->type == OPT_STR) {
        std::string *str = (std::string *)opt->conf_ptr(this);
        if (str->find("$") != string::npos)
          before_count++;
        if (str->find("$data_dir") != string::npos)
          data_dir++;
      }
    }
    // if there are no meta variables in the default configuration,
    // something must be done to check the expected side effect
    // of expand_all_meta
    ASSERT_LT(0, before_count);
    expand_all_meta();
    int after_count = 0;
    for (int i = 0; i < NUM_CONFIG_OPTIONS; i++) {
      config_option *opt = config_optionsp + i;
      if (opt->type == OPT_STR) {
        std::string *str = (std::string *)opt->conf_ptr(this);

        size_t pos = 0;
        while ((pos = str->find("$", pos)) != string::npos) {
          if (str->substr(pos, 8) != "$channel") {
            std::cout << "unexpected meta-variable found at pos " << pos
                      << " of '" << *str << "'" << std::endl;
            after_count++;
          }
          pos++;
        }
      }
    }
    ASSERT_EQ(data_dir, after_count);
  }
};

TEST_F(test_md_config_t, expand_meta)
{
  test_expand_meta();
}

TEST_F(test_md_config_t, expand_all_meta)
{
  test_expand_all_meta();
}

TEST(md_config_t, set_val)
{
  int buf_size = 1024;
  md_config_t conf;
  // disable meta variable expansion
  {
    char *buf = (char*)malloc(buf_size);
    std::string expected = "$host";
    EXPECT_EQ(0, conf.set_val("mon_host", expected.c_str(), false));
    EXPECT_EQ(0, conf.get_val("mon_host", &buf, buf_size));
    EXPECT_EQ(expected, buf);
    free(buf);
  }
  // meta variable expansion is enabled by default
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
