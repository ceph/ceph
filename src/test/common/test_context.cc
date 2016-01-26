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
#include "gtest/gtest.h"
#include "include/types.h"
#include "include/msgr.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "log/Log.h"

TEST(CephContext, do_command)
{
  CephContext *cct = (new CephContext(CEPH_ENTITY_TYPE_CLIENT))->get();

  cct->_conf->cluster = "ceph";

  string key("key");
  string value("value");
  cct->_conf->set_val(key.c_str(), value.c_str(), false);
  cmdmap_t cmdmap;
  cmdmap["var"] = key;

  {
    bufferlist out;
    cct->do_command("config get", cmdmap, "xml", &out);
    string s(out.c_str(), out.length());
    EXPECT_EQ("<config_get><key>" + value + "</key></config_get>", s);
  }

  {
    bufferlist out;
    cct->do_command("config get", cmdmap, "UNSUPPORTED", &out);
    string s(out.c_str(), out.length());
    EXPECT_EQ("{\n    \"key\": \"value\"\n}\n", s);
  }

  cct->put();
}

TEST(CephContext, experimental_features)
{
  CephContext *cct = (new CephContext(CEPH_ENTITY_TYPE_CLIENT))->get();

  cct->_conf->cluster = "ceph";

  ASSERT_FALSE(cct->check_experimental_feature_enabled("foo"));
  ASSERT_FALSE(cct->check_experimental_feature_enabled("bar"));
  ASSERT_FALSE(cct->check_experimental_feature_enabled("baz"));

  cct->_conf->set_val("enable_experimental_unrecoverable_data_corrupting_features",
		      "foo,bar");
  cct->_conf->apply_changes(&cout);
  ASSERT_TRUE(cct->check_experimental_feature_enabled("foo"));
  ASSERT_TRUE(cct->check_experimental_feature_enabled("bar"));
  ASSERT_FALSE(cct->check_experimental_feature_enabled("baz"));

  cct->_conf->set_val("enable_experimental_unrecoverable_data_corrupting_features",
		      "foo bar");
  cct->_conf->apply_changes(&cout);
  ASSERT_TRUE(cct->check_experimental_feature_enabled("foo"));
  ASSERT_TRUE(cct->check_experimental_feature_enabled("bar"));
  ASSERT_FALSE(cct->check_experimental_feature_enabled("baz"));

  cct->_conf->set_val("enable_experimental_unrecoverable_data_corrupting_features",
		      "baz foo");
  cct->_conf->apply_changes(&cout);
  ASSERT_TRUE(cct->check_experimental_feature_enabled("foo"));
  ASSERT_FALSE(cct->check_experimental_feature_enabled("bar"));
  ASSERT_TRUE(cct->check_experimental_feature_enabled("baz"));

  cct->_conf->set_val("enable_experimental_unrecoverable_data_corrupting_features",
		      "*");
  cct->_conf->apply_changes(&cout);
  ASSERT_TRUE(cct->check_experimental_feature_enabled("foo"));
  ASSERT_TRUE(cct->check_experimental_feature_enabled("bar"));
  ASSERT_TRUE(cct->check_experimental_feature_enabled("baz"));

  cct->_log->flush();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ;
 *   make unittest_context &&
 *    valgrind \
 *    --max-stackframe=20000000 --tool=memcheck \
 *   ./unittest_context # --gtest_filter=CephContext.*
 * "
 * End:
 */
