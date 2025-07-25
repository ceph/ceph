// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Inktank <info@inktank.com>
 *
 * LGPL-2.1 (see COPYING-LGPL2.1) or later
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

// test to see if values are recorded in hist , verify by checking for min, max latencies and total latency 

TEST(HistogramLinear, BasicRecordValue) {
  adaptive_linear_hist_t<uint64_t> h(10,0,100);  //interval size is 10

  h.record_value(5);
  h.record_value(15);
  h.record_value(25);

  ASSERT_EQ(h.get_total_count(), 3u);
  ASSERT_EQ(h.get_min_value(), 5u);
  ASSERT_EQ(h.get_max_value(), 25u);
}
 // test to see if values are put in the right bucket 
 // also checks the export csv function because it outputs the count of each bucket 

TEST(HistogramLinear, Bucketing) {
  adaptive_linear_hist_t<uint64_t> h(10,0,100);  

  h.record_value(19);  // goes into raw bucket 10 lower bound
  h.record_value(21);  // goes into raw bucket 20

  std::stringstream out;
  h.export_csv(out, "latency", "count",
    [](uint64_t v){ return std::to_string(v); },
    [](uint64_t c){ return std::to_string(c); });

  std::string csv = out.str();
  ASSERT_NE(csv.find("10,1"), -1); //count in bucket 10 should be 1 
  ASSERT_NE(csv.find("20,1"), -1); //count in bucket 20 should be 1 
}

// tets to check percentile function 

TEST(HistogramLinear, Percentiles) {
  adaptive_linear_hist_t<uint64_t> h(10,15,100);
  h.record_value(5);   
  h.record_value(15);  
  h.record_value(25); 


  ASSERT_EQ(h.value_at_percentile(0), std::numeric_limits<uint64_t>::min());    // First bucket (rounded to 0)
  ASSERT_EQ(h.value_at_percentile(50), 15u);  // Middle value in [15,25)
  ASSERT_EQ(h.value_at_percentile(90), 25u);  // Should be in [25,35)
}

//single value recorded this should be the value for every percentile 
TEST(HistogramLinear, SingleValuePercentile) {
  adaptive_linear_hist_t<uint64_t> h(10,20,100);
  h.record_value(42);

  ASSERT_EQ(h.get_min_value(), 42u);
  ASSERT_EQ(h.get_max_value(), 42u);
  ASSERT_EQ(h.get_total_count(), 1u);
  ASSERT_EQ(h.value_at_percentile(50), 40u); // there is only 1 bucket so this should be the value for any percentile

}
//case when no value is recorded when we call get percentile function it should return maximum value of the type of histogram created 
TEST(HistogramLinear, EmptyHistogramPercentileReturnsInfinity) {
  adaptive_linear_hist_t<uint64_t> h(10,0,100);  
  double result = h.value_at_percentile(50);
  ASSERT_EQ(result,std::numeric_limits<uint64_t>::min() );
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
