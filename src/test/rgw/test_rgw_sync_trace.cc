// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <gtest/gtest.h>
#include "rgw/driver/rados/rgw_sync_trace.h"

TEST(TestSSTR, using_a_var_named_ss)
{
  std::stringstream ss;

  ss << "aaa";
  auto result = SSTR("this is ss=" << ss.str());
  ASSERT_EQ(result, "this is ss=aaa");
}

TEST(TestSSTR, using_a_var_named_other_than_ss)
{
  std::stringstream ss2;

  ss2 << "aaa";
  auto result = SSTR("this is ss2=" << ss2.str());
  ASSERT_EQ(result, "this is ss2=aaa");
}
