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

TEST(CephContext, do_command)
{
  CephContext *cct = (new CephContext(CEPH_ENTITY_TYPE_CLIENT))->get();

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
    EXPECT_EQ("{ \"key\": \"value\"}", s);
  }

  cct->put();
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
