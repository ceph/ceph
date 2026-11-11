// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/dout_fmt.h"
#include <gtest/gtest.h>

TEST(DoutFmt, SubDout)
{
  // expect level 0 to always be gathered
  lsubdout_fmt(g_ceph_context, test, 0, "{}: {}", "value", 42);
  // expect level 99 to be compiled out
  lsubdout_fmt(g_ceph_context, test, 99, "{}: {}", "value", 42);
}

#define dout_subsys ceph_subsys_test

TEST(DoutFmt, Dout)
{
  ldout_fmt(g_ceph_context, 0, "{}: {}", "value", 42);
  ldout_fmt(g_ceph_context, 99, "{}: {}", "value", 42);
}

#define dout_context g_ceph_context

TEST(DoutFmt, DoutContext)
{
  dout_fmt(0, "{}: {}", "value", 42);
  dout_fmt(99, "{}: {}", "value", 42);
}

#undef dout_prefix
#define dout_prefix *_dout << "prefix: "

TEST(DoutFmt, DoutPrefix)
{
  ldout_fmt(g_ceph_context, 0, "{}: {}", "value", 42);
  ldout_fmt(g_ceph_context, 99, "{}: {}", "value", 42);
}

TEST(DoutFmt, DppDout)
{
  const DoutPrefix dpp{g_ceph_context, dout_subsys, "prefix: "};
  ldpp_dout_fmt(&dpp, 0, "{}: {}", "value", 42);
  ldpp_dout_fmt(&dpp, 99, "{}: {}", "value", 42);
}
