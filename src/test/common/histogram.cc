// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Inktank <info@inktank.com>
 *
 * LGPL2.1 (see COPYING-LGPL2.1) or later
 */

#include <iostream>
#include <gtest/gtest.h>

#include "common/histogram.h"
#include "include/stringify.h"

TEST(Histogram, Basic) {
  pow2_hist_t h;

  h.add(0);
  h.add(0);
  h.add(0);
  ASSERT_EQ(3, h.h[0]);
  ASSERT_EQ(1u, h.h.size());

  h.add(1);
  ASSERT_EQ(3, h.h[0]);
  ASSERT_EQ(1, h.h[1]);
  ASSERT_EQ(2u, h.h.size());

  h.add(2);
  h.add(2);
  ASSERT_EQ(3, h.h[0]);
  ASSERT_EQ(1, h.h[1]);
  ASSERT_EQ(2, h.h[2]);
  ASSERT_EQ(3u, h.h.size());
}

TEST(Histogram, Set) {
  pow2_hist_t h;
  h.set_bin(0, 12);
  h.set_bin(2, 12);
  ASSERT_EQ(12, h.h[0]);
  ASSERT_EQ(0, h.h[1]);
  ASSERT_EQ(12, h.h[2]);
  ASSERT_EQ(3u, h.h.size());
}

TEST(Histogram, Position) {
  pow2_hist_t h;
  uint64_t lb, ub;
  h.add(0);
  ASSERT_EQ(-1, h.get_position_micro(-20, &lb, &ub));
}

TEST(Histogram, Position1) {
  pow2_hist_t h;
  h.add(0);
  uint64_t lb, ub;
  h.get_position_micro(0, &lb, &ub);
  ASSERT_EQ(0u, lb);
  ASSERT_EQ(1000000u, ub);
  h.add(0);
  h.add(0);
  h.add(0);
  h.get_position_micro(0, &lb, &ub);
  ASSERT_EQ(0u, lb);
  ASSERT_EQ(1000000u, ub);
}

TEST(Histogram, Position2) {
  pow2_hist_t h;
  h.add(1);
  h.add(1);
  uint64_t lb, ub;
  h.get_position_micro(0, &lb, &ub);
  ASSERT_EQ(0u, lb);
  ASSERT_EQ(0u, ub);
  h.add(0);
  h.get_position_micro(0, &lb, &ub);
  ASSERT_EQ(0u, lb);
  ASSERT_EQ(333333u, ub);
  h.get_position_micro(1, &lb, &ub);
  ASSERT_EQ(333333u, lb);
  ASSERT_EQ(1000000u, ub);
}

TEST(Histogram, Position3) {
  pow2_hist_t h;
  h.h.resize(10, 0);
  h.h[0] = 1;
  h.h[5] = 1;
  uint64_t lb, ub;
  h.get_position_micro(4, &lb, &ub);
  ASSERT_EQ(500000u, lb);
  ASSERT_EQ(500000u, ub);
}

TEST(Histogram, Position4) {
  pow2_hist_t h;
  h.h.resize(10, 0);
  h.h[0] = UINT_MAX;
  h.h[5] = UINT_MAX;
  uint64_t lb, ub;
  h.get_position_micro(4, &lb, &ub);
  ASSERT_EQ(0u, lb);
  ASSERT_EQ(0u, ub);
}

TEST(Histogram, Decay) {
  pow2_hist_t h;
  h.set_bin(0, 123);
  h.set_bin(3, 12);
  h.set_bin(5, 1);
  h.decay(1);
  ASSERT_EQ(61, h.h[0]);
  ASSERT_EQ(6, h.h[3]);
  ASSERT_EQ(4u, h.h.size());
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 &&
 *   make unittest_histogram &&
 *   valgrind --tool=memcheck --leak-check=full \
 *      ./unittest_histogram
 *   "
 * End:
 */
