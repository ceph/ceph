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
#include "include/ceph/libceph.h"
#include "include/rados/librados.h"
#include "test/unit.h"

#include <errno.h>
#include <sstream>
#include <string>
#include <string.h>

using std::string;

TEST(DaemonConfig, SimpleSet) {
  int ret;
  ret = g_ceph_context->_conf->set_val("debug", "21");
  ASSERT_EQ(ret, 0);
  g_ceph_context->_conf->apply_changes(NULL);
  char buf[128];
  memset(buf, 0, sizeof(buf));
  char *tmp = buf;
  ret = g_ceph_context->_conf->get_val("debug", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("21"), string(buf));
}

TEST(DaemonConfig, ArgV) {
  ASSERT_EQ(0, g_ceph_context->_conf->set_val("internal_safe_to_start_threads",
				       "false"));

  int ret;
  const char *argv[] = { "foo", "--debug", "22",
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
  ret = g_ceph_context->_conf->get_val("debug", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("22"), string(buf));

  ASSERT_EQ(0, g_ceph_context->_conf->set_val("internal_safe_to_start_threads",
				       "true"));
}

TEST(DaemonConfig, InjectArgs) {
  int ret;
  std::ostringstream chat;
  std::string injection("--debug 56 --debug-mds 42");
  ret = g_ceph_context->_conf->injectargs(injection, &chat);
  ASSERT_EQ(ret, 0);

  char buf[128];
  char *tmp = buf;
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("debug_mds", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("42"), string(buf));

  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("debug", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("56"), string(buf));

  injection = "--debug 57";
  ret = g_ceph_context->_conf->injectargs(injection, &chat);
  ASSERT_EQ(ret, 0);
  ret = g_ceph_context->_conf->get_val("debug", &tmp, sizeof(buf));
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
  std::ostringstream chat;
  std::string injection("--random-garbage-in-injectargs 26 --debug 28");
  ret = g_ceph_context->_conf->injectargs(injection, &chat);
  ASSERT_EQ(ret, -EINVAL); 

  // But, debug should still be set...
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("debug", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("28"), string(buf));

  // What's the current value of osd_data?
  memset(buf, 0, sizeof(buf));
  ret = g_ceph_context->_conf->get_val("osd_data", &tmp, sizeof(buf));
  ASSERT_EQ(ret, 0);

  // Injectargs shouldn't let us change this, since it is a string-valued
  // variable and there isn't an observer for it.
  std::string injection2("--osd_data /tmp/some-other-directory --debug 4");
  ret = g_ceph_context->_conf->injectargs(injection2, &chat);
  ASSERT_EQ(ret, -ENOSYS); 

  // It should be unchanged.
  memset(buf2, 0, sizeof(buf2));
  ret = g_ceph_context->_conf->get_val("osd_data", &tmp2, sizeof(buf2));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string(buf), string(buf2));
}

TEST(DaemonConfig, InjectArgsLogfile) {
  int ret;
  std::ostringstream chat;
  char tmpfile[PATH_MAX];
  const char *tmpdir = getenv("TMPDIR");
  if (!tmpdir)
    tmpdir = "/tmp";
  snprintf(tmpfile, sizeof(tmpfile), "%s/daemon_config_test.%d",
	   tmpdir, getpid());
  std::string injection("--log_file ");
  injection += tmpfile;
  // We're allowed to change log_file because there is an observer.
  ret = g_ceph_context->_conf->injectargs(injection, &chat);
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
