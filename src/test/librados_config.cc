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
#include "include/rados/librados.h"

#include <sstream>
#include <string>
#include <string.h>

using std::string;

TEST(LibRadosConfig, SimpleSet) {
  rados_t cl;
  int ret = rados_create(&cl, NULL);
  ASSERT_EQ(ret, 0);

  ret = rados_conf_set(cl, "debug", "21");
  ASSERT_EQ(ret, 0);

  char buf[128];
  memset(buf, 0, sizeof(buf));
  ret = rados_conf_get(cl, "debug", buf, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("21"), string(buf));

  rados_shutdown(cl);
}

TEST(LibRadosConfig, ArgV) {
  rados_t cl;
  int ret = rados_create(&cl, NULL);
  ASSERT_EQ(ret, 0);

  const char *argv[] = { "foo", "--debug", "2",
			 "--keyfile", "/tmp/my-keyfile", NULL };
  size_t argc = (sizeof(argv) / sizeof(argv[0])) - 1;
  rados_conf_parse_argv(cl, argc, argv);

  char buf[128];
  memset(buf, 0, sizeof(buf));
  ret = rados_conf_get(cl, "keyfile", buf, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("/tmp/my-keyfile"), string(buf));

  memset(buf, 0, sizeof(buf));
  ret = rados_conf_get(cl, "debug", buf, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("2"), string(buf));

  rados_shutdown(cl);
}
