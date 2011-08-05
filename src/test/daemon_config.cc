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
}
