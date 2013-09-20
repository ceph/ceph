// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank <info@inktank.com>
 *
 * LGPL2.1 (see COPYING-LGPL2.1) or later
 */

#include <iostream>
#include <gtest/gtest.h>

#include "include/stringify.h"
#include "common/bloom_filter.hpp"

TEST(BloomFilter, Basic) {
  bloom_filter bf(10, .1, 1);
  bf.insert("foo");
  bf.insert("bar");

  ASSERT_TRUE(bf.contains("foo"));
  ASSERT_TRUE(bf.contains("bar"));
}

TEST(BloomFilter, Sweep) {
  std::cout << "# max\tfpp\tactual\tsize\tB/insert" << std::endl;
  for (int ex = 3; ex < 12; ex++) {
    for (float fpp = .001; fpp < .5; fpp *= 2.0) {
      int max = 2 << ex;
      bloom_filter bf(max, fpp, 1);
      bf.insert("foo");
      bf.insert("bar");

      ASSERT_TRUE(bf.contains("foo"));
      ASSERT_TRUE(bf.contains("bar"));

      for (int n = 0; n < max; n++)
	bf.insert("ok" + stringify(n));

      int test = max * 100;
      int hit = 0;
      for (int n = 0; n < test; n++)
	if (bf.contains("asdf" + stringify(n)))
	  hit++;

      ASSERT_TRUE(bf.contains("foo"));
      ASSERT_TRUE(bf.contains("bar"));

      double actual = (double)hit / (double)test;

      bufferlist bl;
      ::encode(bf, bl);

      double byte_per_insert = (double)bl.length() / (double)max;

      std::cout << max << "\t" << fpp << "\t" << actual << "\t" << bl.length() << "\t" << byte_per_insert << std::endl;
      ASSERT_TRUE(actual < fpp * 10);

    }
  }
}
