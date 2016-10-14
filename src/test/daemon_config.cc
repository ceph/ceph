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

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "include/cephfs/libcephfs.h"
#include "include/rados/librados.h"
#include "test/unit.h"

#include <errno.h>
#include <sstream>
#include <string>
#include <string.h>

#include <boost/lexical_cast.hpp>


using std::string;

TEST(DaemonConfig, SimpleSet) {
  int ret;
  ret = g_ceph_context->_conf->set_val("num_client", "21");
  ASSERT_EQ(ret, 0);
  g_ceph_context->_conf->apply_changes(NULL);
  char buf[128];
  memset(buf, 0, sizeof(buf));
  char *tmp = buf;
  ret = g_ceph_context->_conf->get_val("num_client", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("21"), string(buf));
}

TEST(DaemonConfig, Substitution) {
  int ret;
  ret = g_ceph_context->_conf->set_val("internal_safe_to_start_threads", "false");
  ASSERT_EQ(ret, 0);
  ret = g_ceph_context->_conf->set_val("host", "foo");
  ASSERT_EQ(ret, 0);
  ret = g_ceph_context->_conf->set_val("public_network", "bar$host.baz", false);
  ASSERT_EQ(ret, 0);
  g_ceph_context->_conf->apply_changes(NULL);
  char buf[128];
  memset(buf, 0, sizeof(buf));
  char *tmp = buf;
  ret = g_ceph_context->_conf->get_val("public_network", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("barfoo.baz"), string(buf));
}

TEST(DaemonConfig, SubstitutionTrailing) {
  int ret;
  ret = g_ceph_context->_conf->set_val("internal_safe_to_start_threads", "false");
  ASSERT_EQ(ret, 0);
  ret = g_ceph_context->_conf->set_val("host", "foo");
  ASSERT_EQ(ret, 0);
  ret = g_ceph_context->_conf->set_val("public_network", "bar$host", false);
  ASSERT_EQ(ret, 0);
  g_ceph_context->_conf->apply_changes(NULL);
  char buf[128];
  memset(buf, 0, sizeof(buf));
  char *tmp = buf;
  ret = g_ceph_context->_conf->get_val("public_network", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("barfoo"), string(buf));
}

TEST(DaemonConfig, SubstitutionBraces) {
  int ret;
  ret = g_ceph_context->_conf->set_val("internal_safe_to_start_threads", "false");
  ASSERT_EQ(ret, 0);
  ret = g_ceph_context->_conf->set_val("host", "foo");
  ASSERT_EQ(ret, 0);
  ret = g_ceph_context->_conf->set_val("public_network", "bar${host}baz", false);
  ASSERT_EQ(ret, 0);
  g_ceph_context->_conf->apply_changes(NULL);
  char buf[128];
  memset(buf, 0, sizeof(buf));
  char *tmp = buf;
  ret = g_ceph_context->_conf->get_val("public_network", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("barfoobaz"), string(buf));
}
TEST(DaemonConfig, SubstitutionBracesTrailing) {
  int ret;
  ret = g_ceph_context->_conf->set_val("internal_safe_to_start_threads", "false");
  ASSERT_EQ(ret, 0);
  ret = g_ceph_context->_conf->set_val("host", "foo");
  ASSERT_EQ(ret, 0);
  ret = g_ceph_context->_conf->set_val("public_network", "bar${host}", false);
  ASSERT_EQ(ret, 0);
  g_ceph_context->_conf->apply_changes(NULL);
  char buf[128];
  memset(buf, 0, sizeof(buf));
  char *tmp = buf;
  ret = g_ceph_context->_conf->get_val("public_network", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("barfoo"), string(buf));
}

// config: variable substitution happen only once http://tracker.ceph.com/issues/7103
TEST(DaemonConfig, SubstitutionMultiple) {
  int ret;
  ret = g_ceph_context->_conf->set_val("mon_host", "localhost", false);
  ASSERT_EQ(ret, 0);
  ret = g_ceph_context->_conf->set_val("keyring", "$mon_host/$cluster.keyring,$mon_host/$cluster.mon.keyring", false);
  ASSERT_EQ(ret, 0);
  g_ceph_context->_conf->apply_changes(NULL);
  char buf[512];
  memset(buf, 0, sizeof(buf));
  char *tmp = buf;
  ret = g_ceph_context->_conf->get_val("keyring", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("localhost/ceph.keyring,localhost/ceph.mon.keyring"), tmp);
  ASSERT_TRUE(strchr(buf, '$') == NULL);
}

TEST(DaemonConfig, ArgV) {
  ASSERT_EQ(0, g_ceph_context->_conf->set_val("internal_safe_to_start_threads",
				       "false"));

  int ret;
  const char *argv[] = { "foo", "--num-client", "22",
			 "--keyfile", "/tmp/my-keyfile", NULL };
  size_t argc = (sizeof(argv) / sizeof(argv[0])) - 1;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  g_ceph_context->_conf->parse_argv(args);
  g_ceph_context->_conf->apply_changes(NULL);

  char buf[128];
  char *tmp = buf;
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("keyfile", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("/tmp/my-keyfile"), string(buf));

  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("num_client", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("22"), string(buf));

  ASSERT_EQ(0, g_ceph_context->_conf->set_val("internal_safe_to_start_threads",
				       "true"));
}

TEST(DaemonConfig, InjectArgs) {
  int ret;
  std::string injection("--num-client 56 --max-open-files 42");
  ret = g_ceph_context->_conf->injectargs(injection, &cout);
  ASSERT_EQ(ret, 0);

  char buf[128];
  char *tmp = buf;
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("max_open_files", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("42"), string(buf));

  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("num_client", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("56"), string(buf));

  injection = "--num-client 57";
  ret = g_ceph_context->_conf->injectargs(injection, &cout);
  ASSERT_EQ(ret, 0);
  ret = g_ceph_context->_conf->get_val("num_client", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("57"), string(buf));
}

TEST(DaemonConfig, InjectArgsReject) {
  int ret;
  char buf[128];
  char *tmp = buf;
  char buf2[128];
  char *tmp2 = buf2;

  // We should complain about the garbage in the input
  std::string injection("--random-garbage-in-injectargs 26 --num-client 28");
  ret = g_ceph_context->_conf->injectargs(injection, &cout);
  ASSERT_EQ(ret, -EINVAL); 

  // But, debug should still be set...
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("num_client", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("28"), string(buf));

  // What's the current value of osd_data?
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("osd_data", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);

  // Injectargs shouldn't let us change this, since it is a string-valued
  // variable and there isn't an observer for it.
  std::string injection2("--osd_data /tmp/some-other-directory --num-client 4");
  ret = g_ceph_context->_conf->injectargs(injection2, &cout);
  ASSERT_EQ(ret, -ENOSYS); 

  // It should be unchanged.
  memset(buf2, 0, sizeof(buf2));
  ret = g_ceph_context->_conf->get_val("osd_data", &tmp2, sizeof(buf2));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string(buf), string(buf2));
}

TEST(DaemonConfig, InjectArgsBooleans) {
  int ret;
  char buf[128];
  char *tmp = buf;

  // Change log_to_syslog
  std::string injection("--log_to_syslog --num-client 28");
  ret = g_ceph_context->_conf->injectargs(injection, &cout);
  ASSERT_EQ(ret, 0);

  // log_to_syslog should be set...
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("log_to_syslog", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("true"), string(buf));

  // Turn off log_to_syslog
  injection = "--log_to_syslog=false --num-client 28";
  ret = g_ceph_context->_conf->injectargs(injection, &cout);
  ASSERT_EQ(ret, 0);

  // log_to_syslog should be cleared...
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("log_to_syslog", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("false"), string(buf));

  // Turn on log_to_syslog
  injection = "--num-client 1 --log_to_syslog=true --max-open-files 40";
  ret = g_ceph_context->_conf->injectargs(injection, &cout);
  ASSERT_EQ(ret, 0);

  // log_to_syslog should be set...
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("log_to_syslog", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("true"), string(buf));

  // parse error
  injection = "--num-client 1 --log_to_syslog=falsey --max-open-files 42";
  ret = g_ceph_context->_conf->injectargs(injection, &cout);
  ASSERT_EQ(ret, -EINVAL);

  // log_to_syslog should still be set...
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("log_to_syslog", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("true"), string(buf));

  // debug-ms should still become 42...
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("max_open_files", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
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
  ret = g_ceph_context->_conf->injectargs(injection, &cout);
  ASSERT_EQ(ret, 0);

  // It should have taken effect.
  char buf[128];
  char *tmp = buf;
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("log_file", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string(buf), string(tmpfile));

  // The logfile should exist.
  ASSERT_EQ(access(tmpfile, R_OK), 0);

  // Let's turn off the logfile.
  ret = g_ceph_context->_conf->set_val("log_file", "");
  ASSERT_EQ(ret, 0);
  g_ceph_context->_conf->apply_changes(NULL);
  ret = g_ceph_context->_conf->get_val("log_file", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string(""), string(buf));

  // Clean up the garbage
  unlink(tmpfile);
}

TEST(DaemonConfig, ThreadSafety1) {
  int ret;
  // Verify that we can't change this, since internal_safe_to_start_threads has
  // been set.
  ret = g_ceph_context->_conf->set_val("osd_data", "");
  ASSERT_EQ(ret, -ENOSYS);

  ASSERT_EQ(0, g_ceph_context->_conf->set_val("internal_safe_to_start_threads",
				       "false"));

  // Ok, now we can change this. Since this is just a test, and there are no
  // OSD threads running, we know changing osd_data won't actually blow up the
  // world.
  ret = g_ceph_context->_conf->set_val("osd_data", "/tmp/crazydata");
  ASSERT_EQ(ret, 0);

  char buf[128];
  char *tmp = buf;
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("osd_data", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("/tmp/crazydata"), string(buf));

  ASSERT_EQ(0, g_ceph_context->_conf->set_val("internal_safe_to_start_threads",
				       "false"));
  ASSERT_EQ(ret, 0);
}

TEST(DaemonConfig, InvalidIntegers) {
  {
    long long bad_value = (long long)std::numeric_limits<int>::max() + 1;
    string str = boost::lexical_cast<string>(bad_value);
    int ret = g_ceph_context->_conf->set_val("num_client", str);
    ASSERT_EQ(ret, -EINVAL);
  }
  {
    // 4G must be greater than INT_MAX
    ASSERT_GT(4LL * 1024 * 1024 * 1024, (long long)std::numeric_limits<int>::max());
    int ret = g_ceph_context->_conf->set_val("num_client", "4G");
    ASSERT_EQ(ret, -EINVAL);
  }
}

TEST(DaemonConfig, InvalidFloats) {
  {
    double bad_value = 2 * (double)std::numeric_limits<float>::max();
    string str = boost::lexical_cast<string>(-bad_value);
    int ret = g_ceph_context->_conf->set_val("log_stop_at_utilization", str);
    ASSERT_EQ(ret, -EINVAL);
  }
  {
    double bad_value = 2 * (double)std::numeric_limits<float>::max();
    string str = boost::lexical_cast<string>(bad_value);
    int ret = g_ceph_context->_conf->set_val("log_stop_at_utilization", str);
    ASSERT_EQ(ret, -EINVAL);
  }
  {
    int ret = g_ceph_context->_conf->set_val("log_stop_at_utilization", "not a float");
    ASSERT_EQ(ret, -EINVAL);
  }
}

/*
 * Local Variables:
 * compile-command: "cd .. ; make unittest_daemon_config && ./unittest_daemon_config"
 * End:
 */
