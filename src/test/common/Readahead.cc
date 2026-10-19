// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Adam Crume <adamcrume@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/Readahead.h"
#include "common/ceph_context.h"
#include "common/perf_counters.h"
#include "gtest/gtest.h"
#include <stdint.h>
#include <boost/foreach.hpp>
#include <cstdarg>
#include <memory>


#define ASSERT_RA(expected_offset, expected_length, ra) \
  do { \
    Readahead::extent_t e = ra; \
    ASSERT_EQ((uint64_t)expected_length, e.second);	\
    if (expected_length) { \
      ASSERT_EQ((uint64_t)expected_offset, e.first); \
    } \
  } while(0)

using namespace std;

TEST(Readahead, random_access) {
  Readahead r;
  r.set_trigger_requests(2);
  ASSERT_RA(0, 0, r.update(1000, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1010, 10, Readahead::NO_LIMIT));
  ASSERT_RA(1030, 20, r.update(1020, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1040, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1060, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1080, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1100, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1200, 10, Readahead::NO_LIMIT));
}

TEST(Readahead, min_size_limit) {
  Readahead r;
  r.set_trigger_requests(2);
  r.set_min_readahead_size(40);
  ASSERT_RA(0, 0, r.update(1000, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1010, 10, Readahead::NO_LIMIT));
  ASSERT_RA(1030, 40, r.update(1020, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1030, 10, Readahead::NO_LIMIT));
  ASSERT_RA(1070, 80, r.update(1040, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1050, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1060, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1070, 10, Readahead::NO_LIMIT));
}

TEST(Readahead, max_size_limit) {
  Readahead r;
  r.set_trigger_requests(2);
  r.set_max_readahead_size(50);
  ASSERT_RA(0, 0, r.update(1000, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1010, 10, Readahead::NO_LIMIT));
  ASSERT_RA(1030, 20, r.update(1020, 10, Readahead::NO_LIMIT));
  ASSERT_RA(1050, 40, r.update(1030, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1040, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1050, 10, Readahead::NO_LIMIT));
  ASSERT_RA(1090, 50, r.update(1060, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1070, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1080, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1090, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1100, 10, Readahead::NO_LIMIT));
  ASSERT_RA(1140, 50, r.update(1110, 10, Readahead::NO_LIMIT));
}

TEST(Readahead, limit) {
  Readahead r;
  r.set_trigger_requests(2);
  r.set_max_readahead_size(50);
  uint64_t limit = 1100;
  ASSERT_RA(0, 0, r.update(1000, 10, limit));
  ASSERT_RA(0, 0, r.update(1010, 10, limit));
  ASSERT_RA(1030, 20, r.update(1020, 10, limit));
  ASSERT_RA(1050, 40, r.update(1030, 10, limit));
  ASSERT_RA(0, 0, r.update(1040, 10, limit));
  ASSERT_RA(0, 0, r.update(1050, 10, limit));
  ASSERT_RA(1090, 10, r.update(1060, 10, limit));
  ASSERT_RA(0, 0, r.update(1070, 10, limit));
  ASSERT_RA(0, 0, r.update(1080, 10, limit));
  ASSERT_RA(0, 0, r.update(1090, 10, limit));
}

TEST(Readahead, alignment) {
  Readahead r;
  r.set_trigger_requests(2);
  vector<uint64_t> alignment;
  alignment.push_back(100);
  r.set_alignments(alignment);
  ASSERT_RA(0, 0, r.update(1000, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1010, 10, Readahead::NO_LIMIT));
  ASSERT_RA(1030, 20, r.update(1020, 10, Readahead::NO_LIMIT));
  ASSERT_RA(1050, 50, r.update(1030, 10, Readahead::NO_LIMIT)); // internal readahead size 40
  ASSERT_RA(0, 0, r.update(1040, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1050, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1060, 10, Readahead::NO_LIMIT));
  ASSERT_RA(1100, 100, r.update(1070, 10, Readahead::NO_LIMIT)); // internal readahead size 80
  ASSERT_RA(0, 0, r.update(1080, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1090, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1100, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1110, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1120, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1130, 10, Readahead::NO_LIMIT));
  ASSERT_RA(1200, 200, r.update(1140, 10, Readahead::NO_LIMIT)); // internal readahead size 160
  ASSERT_RA(0, 0, r.update(1150, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1160, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1170, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1180, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1190, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1200, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1210, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1220, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1230, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1240, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1250, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1260, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1270, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1280, 10, Readahead::NO_LIMIT));
  ASSERT_RA(1400, 300, r.update(1290, 10, Readahead::NO_LIMIT)); // internal readahead size 320
  ASSERT_RA(0, 0, r.update(1300, 10, Readahead::NO_LIMIT));
}

TEST(Readahead, perf_counters) {
  boost::intrusive_ptr<CephContext> cct{new CephContext(CEPH_ENTITY_TYPE_CLIENT),
					false};
  PerfCountersBuilder bld(cct.get(), "readahead", l_readahead_first,
			  l_readahead_last);
  bld.add_u64_counter(l_readahead_observes, "observes");
  bld.add_u64_counter(l_readahead_resets, "resets");
  bld.add_u64_counter(l_readahead_sequential, "sequential");
  bld.add_u64_counter(l_readahead_issued, "issued");
  bld.add_u64_counter(l_readahead_issued_bytes, "issued_bytes");
  std::unique_ptr<PerfCounters> logger(bld.create_perf_counters());

  Readahead r;
  r.set_perf_counters(logger.get());
  r.set_trigger_requests(2);

  // First read is a reset (m_last_pos starts at 0).
  ASSERT_RA(0, 0, r.update(1000, 10, Readahead::NO_LIMIT));
  ASSERT_RA(0, 0, r.update(1010, 10, Readahead::NO_LIMIT));
  // Third consecutive read triggers readahead of 20 bytes.
  ASSERT_RA(1030, 20, r.update(1020, 10, Readahead::NO_LIMIT));
  // Gap resets sequential state.
  ASSERT_RA(0, 0, r.update(2000, 10, Readahead::NO_LIMIT));

  EXPECT_EQ(4u, logger->get(l_readahead_observes));
  EXPECT_EQ(2u, logger->get(l_readahead_resets));      // 1000, 2000
  EXPECT_EQ(2u, logger->get(l_readahead_sequential)); // 1010, 1020
  EXPECT_EQ(1u, logger->get(l_readahead_issued));
  EXPECT_EQ(20u, logger->get(l_readahead_issued_bytes));
}
