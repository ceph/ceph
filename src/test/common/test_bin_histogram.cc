// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM Corp
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <gtest/gtest.h>

#include "common/lockstat.h"

using namespace ceph::lockstat_detail;

TEST(BinHistogram, Basic)
{
  // bin_histogram<num_bins + 2, 1, 1ul << bin_low_bits, 1ul << bin_hi_bits>
  // num_bins = 25, bin_low_bits = 4, bin_hi_bits = 29
  // bin_histogram<27, 1, 16, 1ul << 29>
  typedef bin_histogram<27, 1, 16, 1ul << 29> Hist;

  EXPECT_EQ(Hist::get_index(0), 0);
  EXPECT_EQ(Hist::get_index(15), 0);
  EXPECT_EQ(Hist::get_index(16), 1);
  EXPECT_EQ(Hist::get_index(31), 1);
  EXPECT_EQ(Hist::get_index(32), 2);
  EXPECT_EQ(Hist::get_index(63), 2);
  EXPECT_EQ(Hist::get_index(64), 3);

  // MaxVal = 1 << 29 = 536870912
  EXPECT_EQ(Hist::get_index((1ul << 29) - 1), 25);
  EXPECT_EQ(Hist::get_index(1ul << 29), 26);
  EXPECT_EQ(Hist::get_index(1ul << 30), 26);
}

int
main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
