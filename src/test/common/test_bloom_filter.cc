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

  ASSERT_EQ(2U, bf.element_count());
}

TEST(BloomFilter, Empty) {
  bloom_filter bf;
  for (int i=0; i<100; ++i) {
    ASSERT_FALSE(bf.contains(i));
    ASSERT_FALSE(bf.contains(stringify(i)));
  }
}

TEST(BloomFilter, Sweep) {
  std::cout.setf(std::ios_base::fixed, std::ios_base::floatfield);
  std::cout.precision(5);
  std::cout << "# max\tfpp\tactual\tsize\tB/insert" << std::endl;
  for (int ex = 3; ex < 12; ex += 2) {
    for (float fpp = .001; fpp < .5; fpp *= 4.0) {
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

TEST(BloomFilter, SweepInt) {
  std::cout.setf(std::ios_base::fixed, std::ios_base::floatfield);
  std::cout.precision(5);
  std::cout << "# max\tfpp\tactual\tsize\tB/insert\tdensity\tapprox_element_count" << std::endl;
  for (int ex = 3; ex < 12; ex += 2) {
    for (float fpp = .001; fpp < .5; fpp *= 4.0) {
      int max = 2 << ex;
      bloom_filter bf(max, fpp, 1);
      bf.insert("foo");
      bf.insert("bar");

      ASSERT_TRUE(123);
      ASSERT_TRUE(456);

      for (int n = 0; n < max; n++)
	bf.insert(n);

      int test = max * 100;
      int hit = 0;
      for (int n = 0; n < test; n++)
	if (bf.contains(100000 + n))
	  hit++;

      ASSERT_TRUE(123);
      ASSERT_TRUE(456);

      double actual = (double)hit / (double)test;

      bufferlist bl;
      ::encode(bf, bl);

      double byte_per_insert = (double)bl.length() / (double)max;

      std::cout << max << "\t" << fpp << "\t" << actual << "\t" << bl.length() << "\t" << byte_per_insert
		<< "\t" << bf.density() << "\t" << bf.approx_unique_element_count() << std::endl;
      ASSERT_TRUE(actual < fpp * 10);
      ASSERT_TRUE(actual > fpp / 10);
      ASSERT_TRUE(bf.density() > 0.40);
      ASSERT_TRUE(bf.density() < 0.60);
    }
  }
}


TEST(BloomFilter, CompressibleSweep) {
  std::cout.setf(std::ios_base::fixed, std::ios_base::floatfield);
  std::cout.precision(5);
  std::cout << "# max\tins\test ins\tafter\ttgtfpp\tactual\tsize\tb/elem\n";
  float fpp = .01;
  int max = 1024;
  for (int div = 1; div < 10; div++) {
    compressible_bloom_filter bf(max, fpp, 1);
    int t = max/div;
    for (int n = 0; n < t; n++)
      bf.insert(n);

    unsigned est = bf.approx_unique_element_count();
    if (div > 1)
      bf.compress(1.0 / div);

    for (int n = 0; n < t; n++)
      ASSERT_TRUE(bf.contains(n));

    int test = max * 100;
    int hit = 0;
    for (int n = 0; n < test; n++)
      if (bf.contains(100000 + n))
	hit++;

    double actual = (double)hit / (double)test;

    bufferlist bl;
    ::encode(bf, bl);

    double byte_per_insert = (double)bl.length() / (double)max;
    unsigned est_after = bf.approx_unique_element_count();
    std::cout << max
	      << "\t" << t
	      << "\t" << est
	      << "\t" << est_after
	      << "\t" << fpp
	      << "\t" << actual
	      << "\t" << bl.length() << "\t" << byte_per_insert
	      << std::endl;

    ASSERT_TRUE(actual < fpp * 2.0);
    ASSERT_TRUE(actual > fpp / 2.0);
    ASSERT_TRUE(est_after < est * 2);
    ASSERT_TRUE(est_after > est / 2);
  }
}



TEST(BloomFilter, BinSweep) {
  std::cout.setf(std::ios_base::fixed, std::ios_base::floatfield);
  std::cout.precision(5);
  int total_max = 16384;
  float total_fpp = .01;
  std::cout << "total_inserts " << total_max << " target-fpp " << total_fpp << std::endl;
  for (int bins = 1; bins < 16; ++bins) {
    int max = total_max / bins;
    float fpp = total_fpp / bins;//pow(total_fpp, bins);

    std::vector<bloom_filter*> ls;
    bufferlist bl;
    for (int i=0; i<bins; i++) {
      ls.push_back(new bloom_filter(max, fpp, i));
      for (int j=0; j<max; j++) {
	ls.back()->insert(10000 * (i+1) + j);
      }
      ::encode(*ls.front(), bl);
    }

    int hit = 0;
    int test = max * 100;
    for (int i=0; i<test; ++i) {
      for (std::vector<bloom_filter*>::iterator j = ls.begin(); j != ls.end(); ++j) {
	if ((*j)->contains(i * 732)) {  // note: sequential i does not work here; the intenral int hash is weak!!
	  hit++;
	  break;
	}
      }
    }

    double actual = (double)hit / (double)test;
    std::cout << "bins " << bins << " bin-max " << max << " bin-fpp " << fpp
	      << " actual-fpp " << actual
	      << " total-size " << bl.length() << std::endl;
  }
}

// disable these tests; doing dual insertions in consecutive filters
// appears to be equivalent to doing a single insertion in a bloom
// filter that is twice as big.
#if 0

// test the fpp over a sequence of bloom filters, each with unique
// items inserted into it.
//
// we expect:  actual_fpp = num_filters * per_filter_fpp
TEST(BloomFilter, Sequence) {

  int max = 1024;
  double fpp = .01;
  for (int seq = 2; seq <= 128; seq *= 2) {
    std::vector<bloom_filter*> ls;
    for (int i=0; i<seq; i++) {
      ls.push_back(new bloom_filter(max*2, fpp, i));
      for (int j=0; j<max; j++) {
	ls.back()->insert("ok" + stringify(j) + "_" + stringify(i));
	if (ls.size() > 1)
	  ls[ls.size() - 2]->insert("ok" + stringify(j) + "_" + stringify(i));
      }
    }

    int hit = 0;
    int test = max * 100;
    for (int i=0; i<test; ++i) {
      for (std::vector<bloom_filter*>::iterator j = ls.begin(); j != ls.end(); ++j) {
	if ((*j)->contains("bad" + stringify(i))) {
	  hit++;
	  break;
	}
      }
    }

    double actual = (double)hit / (double)test;
    std::cout << "seq " << seq << " max " << max << " fpp " << fpp << " actual " << actual << std::endl;
  }
}

// test the ffp over a sequence of bloom filters, where actual values
// are always inserted into a consecutive pair of filters.  in order
// to have a false positive, we need to falsely match two consecutive
// filters.
//
// we expect:  actual_fpp = num_filters * per_filter_fpp^2
TEST(BloomFilter, SequenceDouble) {
  int max = 1024;
  double fpp = .01;
  for (int seq = 2; seq <= 128; seq *= 2) {
    std::vector<bloom_filter*> ls;
    for (int i=0; i<seq; i++) {
      ls.push_back(new bloom_filter(max*2, fpp, i));
      for (int j=0; j<max; j++) {
	ls.back()->insert("ok" + stringify(j) + "_" + stringify(i));
	if (ls.size() > 1)
	  ls[ls.size() - 2]->insert("ok" + stringify(j) + "_" + stringify(i));
      }
    }

    int hit = 0;
    int test = max * 100;
    int run = 0;
    for (int i=0; i<test; ++i) {
      for (std::vector<bloom_filter*>::iterator j = ls.begin(); j != ls.end(); ++j) {
	if ((*j)->contains("bad" + stringify(i))) {
	  run++;
	  if (run >= 2) {
	    hit++;
	    break;
	  }
	} else {
	  run = 0;
	}
      }
    }

    double actual = (double)hit / (double)test;
    std::cout << "seq " << seq << " max " << max << " fpp " << fpp << " actual " << actual
	      << " expected " << (fpp*fpp*(double)seq) << std::endl;
  }
}

#endif

TEST(BloomFilter, Assignement) {
  bloom_filter bf1(10, .1, .1), bf2;

  bf1.insert("foo");
  bf2 = bf1;
  bf1.insert("bar");

  ASSERT_TRUE(bf2.contains("foo"));
  ASSERT_FALSE(bf2.contains("bar"));

  ASSERT_EQ(2U, bf1.element_count());
  ASSERT_EQ(1U, bf2.element_count());
}
