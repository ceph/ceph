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

#if defined(__linux__)
TEST(util, collect_sys_info)
{
  map<string, string> sys_info;

  CephContext *cct = (new CephContext(CEPH_ENTITY_TYPE_CLIENT))->get();
  collect_sys_info(&sys_info, cct);

  ASSERT_TRUE(sys_info.find("distro") != sys_info.end());
  ASSERT_TRUE(sys_info.find("distro_description") != sys_info.end());

  cct->put();
}
#endif
