// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/ceph_context.h"
#include "include/util.h"
#include "gtest/gtest.h"

#include <sstream>

TEST(util, unit_to_bytesize)
{
  ASSERT_EQ(1234ll, unit_to_bytesize("1234", &cerr));
  ASSERT_EQ(1024ll, unit_to_bytesize("1K", &cerr));
  ASSERT_EQ(1024ll, unit_to_bytesize("1k", &cerr));
  ASSERT_EQ(1048576ll, unit_to_bytesize("1M", &cerr));
  ASSERT_EQ(1073741824ll, unit_to_bytesize("1G", &cerr));
  ASSERT_EQ(1099511627776ll, unit_to_bytesize("1T", &cerr));
  ASSERT_EQ(1125899906842624ll, unit_to_bytesize("1P", &cerr));
  ASSERT_EQ(1152921504606846976ll, unit_to_bytesize("1E", &cerr));

  ASSERT_EQ(65536ll, unit_to_bytesize(" 64K", &cerr));
}

#if defined(__linux__)
TEST(util, collect_sys_info)
{
  map<string, string> sys_info;

  CephContext *cct = (new CephContext(CEPH_ENTITY_TYPE_CLIENT))->get();
  collect_sys_info(&sys_info, cct);

  ASSERT_TRUE(sys_info.find("distro") != sys_info.end());
  ASSERT_TRUE(sys_info.find("distro_version") != sys_info.end());
  ASSERT_TRUE(sys_info.find("distro_description") != sys_info.end());

  cct->put();
}
#endif
