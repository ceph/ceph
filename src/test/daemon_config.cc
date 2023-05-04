// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "global/global_context.h"
#include "include/cephfs/libcephfs.h"
#include "include/rados/librados.h"

#include <errno.h>
#include <sstream>
#include <string>
#include <string.h>

#include <boost/lexical_cast.hpp>


using namespace std;

TEST(DaemonConfig, SimpleSet) {
  int ret;
  ret = g_ceph_context->_conf.set_val("log_graylog_port", "21");
  ASSERT_EQ(0, ret);
  g_ceph_context->_conf.apply_changes(nullptr);
  char buf[128];
  memset(buf, 0, sizeof(buf));
  char *tmp = buf;
  ret = g_ceph_context->_conf.get_val("log_graylog_port", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("21"), string(buf));
  g_ceph_context->_conf.rm_val("log_graylog_port");
}

TEST(DaemonConfig, Substitution) {
  int ret;
  g_conf()._clear_safe_to_start_threads();
  ret = g_ceph_context->_conf.set_val("host", "foo");
  ASSERT_EQ(0, ret);
  ret = g_ceph_context->_conf.set_val("public_network", "bar$host.baz");
  ASSERT_EQ(0, ret);
  g_ceph_context->_conf.apply_changes(nullptr);
  char buf[128];
  memset(buf, 0, sizeof(buf));
  char *tmp = buf;
  ret = g_ceph_context->_conf.get_val("public_network", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("barfoo.baz"), string(buf));
}

TEST(DaemonConfig, SubstitutionTrailing) {
  int ret;
  g_conf()._clear_safe_to_start_threads();
  ret = g_ceph_context->_conf.set_val("host", "foo");
  ASSERT_EQ(0, ret);
  ret = g_ceph_context->_conf.set_val("public_network", "bar$host");
  ASSERT_EQ(0, ret);
  g_ceph_context->_conf.apply_changes(nullptr);
  char buf[128];
  memset(buf, 0, sizeof(buf));
  char *tmp = buf;
  ret = g_ceph_context->_conf.get_val("public_network", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("barfoo"), string(buf));
}

TEST(DaemonConfig, SubstitutionBraces) {
  int ret;
  g_conf()._clear_safe_to_start_threads();
  ret = g_ceph_context->_conf.set_val("host", "foo");
  ASSERT_EQ(0, ret);
  ret = g_ceph_context->_conf.set_val("public_network", "bar${host}baz");
  ASSERT_EQ(0, ret);
  g_ceph_context->_conf.apply_changes(nullptr);
  char buf[128];
  memset(buf, 0, sizeof(buf));
  char *tmp = buf;
  ret = g_ceph_context->_conf.get_val("public_network", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("barfoobaz"), string(buf));
}
TEST(DaemonConfig, SubstitutionBracesTrailing) {
  int ret;
  g_conf()._clear_safe_to_start_threads();
  ret = g_ceph_context->_conf.set_val("host", "foo");
  ASSERT_EQ(0, ret);
  ret = g_ceph_context->_conf.set_val("public_network", "bar${host}");
  ASSERT_EQ(0, ret);
  g_ceph_context->_conf.apply_changes(nullptr);
  char buf[128];
  memset(buf, 0, sizeof(buf));
  char *tmp = buf;
  ret = g_ceph_context->_conf.get_val("public_network", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("barfoo"), string(buf));
}

// config: variable substitution happen only once http://tracker.ceph.com/issues/7103
TEST(DaemonConfig, SubstitutionMultiple) {
  int ret;
  ret = g_ceph_context->_conf.set_val("mon_host", "localhost");
  ASSERT_EQ(0, ret);
  ret = g_ceph_context->_conf.set_val("keyring", "$mon_host/$cluster.keyring,$mon_host/$cluster.mon.keyring");
  ASSERT_EQ(0, ret);
  g_ceph_context->_conf.apply_changes(nullptr);
  char buf[512];
  memset(buf, 0, sizeof(buf));
  char *tmp = buf;
  ret = g_ceph_context->_conf.get_val("keyring", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("localhost/ceph.keyring,localhost/ceph.mon.keyring"), tmp);
  ASSERT_TRUE(strchr(buf, '$') == NULL);
}

TEST(DaemonConfig, ArgV) {
  g_conf()._clear_safe_to_start_threads();

  int ret;
  const char *argv[] = { "foo", "--log-graylog-port", "22",
			 "--key", "my-key", NULL };
  size_t argc = (sizeof(argv) / sizeof(argv[0])) - 1;
  auto args = argv_to_vec(argc, argv);
  g_ceph_context->_conf.parse_argv(args);
  g_ceph_context->_conf.apply_changes(nullptr);

  char buf[128];
  char *tmp = buf;
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf.get_val("key", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("my-key"), string(buf));

  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf.get_val("log_graylog_port", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("22"), string(buf));

  g_conf().set_safe_to_start_threads();
}

TEST(DaemonConfig, InjectArgs) {
  int ret;
  std::string injection("--log-graylog-port 56 --log_max_new 42");
  ret = g_ceph_context->_conf.injectargs(injection, &cout);
  ASSERT_EQ(0, ret);

  char buf[128];
  char *tmp = buf;
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf.get_val("log_max_new", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("42"), string(buf));

  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf.get_val("log_graylog_port", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("56"), string(buf));

  injection = "--log-graylog-port 57";
  ret = g_ceph_context->_conf.injectargs(injection, &cout);
  ASSERT_EQ(0, ret);
  ret = g_ceph_context->_conf.get_val("log_graylog_port", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("57"), string(buf));
}

TEST(DaemonConfig, InjectArgsReject) {
  int ret;
  char buf[128];
  char *tmp = buf;
  char buf2[128];
  char *tmp2 = buf2;

  // We should complain about the garbage in the input
  std::string injection("--random-garbage-in-injectargs 26 --log-graylog-port 28");
  ret = g_ceph_context->_conf.injectargs(injection, &cout);
  ASSERT_EQ(-EINVAL, ret);

  // But, debug should still be set...
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf.get_val("log_graylog_port", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("28"), string(buf));

  // What's the current value of osd_data?
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf.get_val("osd_data", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);

  // Injectargs shouldn't let us change this, since it is a string-valued
  // variable and there isn't an observer for it.
  std::string injection2("--osd_data /tmp/some-other-directory --log-graylog-port 4");
  ret = g_ceph_context->_conf.injectargs(injection2, &cout);
  ASSERT_EQ(-EPERM, ret);

  // It should be unchanged.
  memset(buf2, 0, sizeof(buf2));
  ret = g_ceph_context->_conf.get_val("osd_data", &tmp2, sizeof(buf2));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string(buf), string(buf2));

  // We should complain about the missing arguments.
  std::string injection3("--log-graylog-port 28 --debug_ms");
  ret = g_ceph_context->_conf.injectargs(injection3, &cout);
  ASSERT_EQ(-EINVAL, ret);
}

TEST(DaemonConfig, InjectArgsBooleans) {
  int ret;
  char buf[128];
  char *tmp = buf;

  // Change log_to_syslog
  std::string injection("--log_to_syslog --log-graylog-port 28");
  ret = g_ceph_context->_conf.injectargs(injection, &cout);
  ASSERT_EQ(0, ret);

  // log_to_syslog should be set...
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf.get_val("log_to_syslog", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("true"), string(buf));

  // Turn off log_to_syslog
  injection = "--log_to_syslog=false --log-graylog-port 28";
  ret = g_ceph_context->_conf.injectargs(injection, &cout);
  ASSERT_EQ(0, ret);

  // log_to_syslog should be cleared...
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf.get_val("log_to_syslog", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("false"), string(buf));

  // Turn on log_to_syslog
  injection = "--log-graylog-port=1 --log_to_syslog=true --log_max_new 40";
  ret = g_ceph_context->_conf.injectargs(injection, &cout);
  ASSERT_EQ(0, ret);

  // log_to_syslog should be set...
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf.get_val("log_to_syslog", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("true"), string(buf));

  // parse error
  injection = "--log-graylog-port 1 --log_to_syslog=falsey --log_max_new 42";
  ret = g_ceph_context->_conf.injectargs(injection, &cout);
  ASSERT_EQ(-EINVAL, ret);

  // log_to_syslog should still be set...
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf.get_val("log_to_syslog", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("true"), string(buf));

  // debug-ms should still become 42...
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf.get_val("log_max_new", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("42"), string(buf));
}

TEST(DaemonConfig, InjectArgsLogfile) {
  int ret;
  char tmpfile[PATH_MAX];
  const char *tmpdir = getenv("TMPDIR");
  if (!tmpdir)
    tmpdir = "/tmp";
  snprintf(tmpfile, sizeof(tmpfile), "%s/daemon_config_test.%d",
	   tmpdir, getpid());
  std::string injection("--log_file ");
  injection += tmpfile;
  // We're allowed to change log_file because there is an observer.
  ret = g_ceph_context->_conf.injectargs(injection, &cout);
  ASSERT_EQ(0, ret);

  // It should have taken effect.
  char buf[128];
  char *tmp = buf;
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf.get_val("log_file", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string(buf), string(tmpfile));

  // The logfile should exist.
  ASSERT_EQ(0, access(tmpfile, R_OK));

  // Let's turn off the logfile.
  ret = g_ceph_context->_conf.set_val("log_file", "");
  ASSERT_EQ(0, ret);
  g_ceph_context->_conf.apply_changes(nullptr);
  ret = g_ceph_context->_conf.get_val("log_file", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string(""), string(buf));

  // Clean up the garbage
  unlink(tmpfile);
}

TEST(DaemonConfig, ThreadSafety1) {
  int ret;
  // Verify that we can't change this, since safe_to_start_threads has
  // been set.
  ret = g_ceph_context->_conf.set_val("osd_data", "");
  ASSERT_EQ(-EPERM, ret);

  g_conf()._clear_safe_to_start_threads();

  // Ok, now we can change this. Since this is just a test, and there are no
  // OSD threads running, we know changing osd_data won't actually blow up the
  // world.
  ret = g_ceph_context->_conf.set_val("osd_data", "/tmp/crazydata");
  ASSERT_EQ(0, ret);

  char buf[128];
  char *tmp = buf;
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf.get_val("osd_data", &tmp, sizeof(buf));
  ASSERT_EQ(0, ret);
  ASSERT_EQ(string("/tmp/crazydata"), string(buf));

  g_conf()._clear_safe_to_start_threads();
  ASSERT_EQ(0, ret);
}

TEST(DaemonConfig, InvalidIntegers) {
  {
    int ret = g_ceph_context->_conf.set_val("log_graylog_port", "rhubarb");
    ASSERT_EQ(-EINVAL, ret);
  }

  {
    int64_t max = std::numeric_limits<int64_t>::max();
    string str = boost::lexical_cast<string>(max);
    str = str + "999"; // some extra digits to take us out of bounds
    int ret = g_ceph_context->_conf.set_val("log_graylog_port", str);
    ASSERT_EQ(-EINVAL, ret);
  }

  g_ceph_context->_conf.rm_val("log_graylog_port");
}

TEST(DaemonConfig, InvalidFloats) {
  {
    double bad_value = 2 * (double)std::numeric_limits<float>::max();
    string str = boost::lexical_cast<string>(-bad_value);
    int ret = g_ceph_context->_conf.set_val("log_stop_at_utilization", str);
    ASSERT_EQ(-EINVAL, ret);
  }
  {
    double bad_value = 2 * (double)std::numeric_limits<float>::max();
    string str = boost::lexical_cast<string>(bad_value);
    int ret = g_ceph_context->_conf.set_val("log_stop_at_utilization", str);
    ASSERT_EQ(-EINVAL, ret);
  }
  {
    int ret = g_ceph_context->_conf.set_val("log_stop_at_utilization", "not a float");
    ASSERT_EQ(-EINVAL, ret);
  }
}

/*
 * Local Variables:
 * compile-command: "cd ../../build ; \
 * make unittest_daemon_config && ./bin/unittest_daemon_config"
 * End:
 */
