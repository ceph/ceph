// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <sstream>
#include <errno.h>
#include <signal.h>
#include "osd/ECCommon.h"
#include "osd/ECBackend.h"
#include "gtest/gtest.h"

using namespace std;

TEST(ECUtil, stripe_info_t)
{
  const uint64_t swidth = 4096;
  const uint64_t ssize = 4;

  ECUtil::stripe_info_t s(ssize, swidth);
  ASSERT_EQ(s.get_stripe_width(), swidth);

  ASSERT_EQ(s.logical_to_next_chunk_offset(0), 0u);
  ASSERT_EQ(s.logical_to_next_chunk_offset(1), s.get_chunk_size());
  ASSERT_EQ(s.logical_to_next_chunk_offset(swidth - 1),
	    s.get_chunk_size());

  ASSERT_EQ(s.logical_to_prev_chunk_offset(0), 0u);
  ASSERT_EQ(s.logical_to_prev_chunk_offset(swidth), s.get_chunk_size());
  ASSERT_EQ(s.logical_to_prev_chunk_offset((swidth * 2) - 1),
	    s.get_chunk_size());

  ASSERT_EQ(s.logical_to_next_stripe_offset(0), 0u);
  ASSERT_EQ(s.logical_to_next_stripe_offset(swidth - 1),
	    s.get_stripe_width());

  ASSERT_EQ(s.logical_to_prev_stripe_offset(swidth), s.get_stripe_width());
  ASSERT_EQ(s.logical_to_prev_stripe_offset(swidth), s.get_stripe_width());
  ASSERT_EQ(s.logical_to_prev_stripe_offset((swidth * 2) - 1),
	    s.get_stripe_width());

  ASSERT_EQ(s.aligned_logical_offset_to_chunk_offset(2*swidth),
	    2*s.get_chunk_size());
  ASSERT_EQ(s.aligned_chunk_offset_to_logical_offset(2*s.get_chunk_size()),
	    2*s.get_stripe_width());

  ASSERT_EQ(s.chunk_aligned_offset_len_to_chunk(
	      make_pair(swidth+s.get_chunk_size(), 10*swidth)),
	    make_pair(s.get_chunk_size(), 10*s.get_chunk_size()));

  ASSERT_EQ(s.chunk_aligned_offset_len_to_chunk(make_pair(swidth, 10*swidth)),
	    make_pair(s.get_chunk_size(), 10*s.get_chunk_size()));

  // round down offset if it's under stripe width
  ASSERT_EQ(s.chunk_aligned_offset_len_to_chunk(make_pair(s.get_chunk_size(), 10*swidth)),
	    make_pair<uint64_t>(0, 10*s.get_chunk_size()));

  // round up size if above stripe
  ASSERT_EQ(s.chunk_aligned_offset_len_to_chunk(make_pair(s.get_chunk_size(),
							  10*swidth + s.get_chunk_size())),
	    make_pair<uint64_t>(0, 11*s.get_chunk_size()));

  ASSERT_EQ(s.offset_len_to_stripe_bounds(make_pair(swidth-10, (uint64_t)20)),
            make_pair((uint64_t)0, 2*swidth));
}

TEST(ECUtil, offset_length_is_same_stripe)
{
  const uint64_t swidth = 4096;
  const uint64_t schunk = 1024;
  const uint64_t ssize = 4;

  ECUtil::stripe_info_t s(ssize, swidth);
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), schunk);

  // read nothing at the very beginning
  //   +---+---+---+---+
  //   |  0|   |   |   |
  //   +---+---+---+---+
  //   |   |   |   |   |
  //   +---+---+---+---+
  ASSERT_TRUE(s.offset_length_is_same_stripe(0, 0));

  // read nothing at the stripe end
  //   +---+---+---+---+
  //   |   |   |   |  0|
  //   +---+---+---+---+
  //   |   |   |   |   |
  //   +---+---+---+---+
  ASSERT_TRUE(s.offset_length_is_same_stripe(swidth, 0));

  // read single byte at the stripe end
  //   +---+---+---+---+
  //   |   |   |   | ~1|
  //   +---+---+---+---+
  //   |   |   |   |   |
  //   +---+---+---+---+
  ASSERT_TRUE(s.offset_length_is_same_stripe(swidth - 1, 1));

  // read single stripe
  //   +---+---+---+---+
  //   | 1k| 1k| 1k| 1k|
  //   +---+---+---+---+
  //   |   |   |   |   |
  //   +---+---+---+---+
  ASSERT_TRUE(s.offset_length_is_same_stripe(0, swidth));

  // read single chunk
  //   +---+---+---+---+
  //   | 1k|   |   |   |
  //   +---+---+---+---+
  //   |   |   |   |   |
  //   +---+---+---+---+
  ASSERT_TRUE(s.offset_length_is_same_stripe(0, schunk));

  // read single stripe except its first chunk
  //   +---+---+---+---+
  //   |   | 1k| 1k| 1k|
  //   +---+---+---+---+
  //   |   |   |   |   |
  //   +---+---+---+---+
  ASSERT_TRUE(s.offset_length_is_same_stripe(schunk, swidth - schunk));

  // read two stripes
  //   +---+---+---+---+
  //   | 1k| 1k| 1k| 1k|
  //   +---+---+---+---+
  //   | 1k| 1k| 1k| 1k|
  //   +---+---+---+---+
  ASSERT_FALSE(s.offset_length_is_same_stripe(0, 2*swidth));

  // multistripe read: 1st stripe without 1st byte + 1st byte of 2nd stripe
  //   +-----+---+---+---+
  //   | 1k-1| 1k| 1k| 1k|
  //   +-----+---+---+---+
  //   |    1|   |   |   |
  //   +-----+---+---+---+
  ASSERT_FALSE(s.offset_length_is_same_stripe(1, swidth));
}


TEST(ECCommon, get_min_want_to_read_shards)
{
  const uint64_t swidth = 4096;
  const uint64_t ssize = 4;

  ECUtil::stripe_info_t s(ssize, swidth);
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), 1024);

  const std::vector<int> chunk_mapping = {}; // no remapping

  // read nothing at the very beginning
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      0, 0, s, chunk_mapping, &want_to_read);
    ASSERT_TRUE(want_to_read == std::set<int>{});
  }

  // read nothing at the middle (0-sized partial read)
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      2048, 0, s, chunk_mapping, &want_to_read);
    ASSERT_TRUE(want_to_read == std::set<int>{});
  }

  // read not-so-many (< chunk_size) bytes at the middle (partial read)
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      2048, 42, s, chunk_mapping, &want_to_read);
    ASSERT_TRUE(want_to_read == std::set<int>{2});
  }

  // read more (> chunk_size) bytes at the middle (partial read)
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      1024, 1024+42, s, chunk_mapping, &want_to_read);
    // extra () due to a language / macro limitation
    ASSERT_TRUE(want_to_read == (std::set<int>{1, 2}));
  }

  // full stripe except last chunk
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      0, 3*1024, s, chunk_mapping, &want_to_read);
    // extra () due to a language / macro limitation
    ASSERT_TRUE(want_to_read == (std::set<int>{0, 1, 2}));
  }

  // full stripe except 1st chunk
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      1024, swidth-1024, s, chunk_mapping, &want_to_read);
    // extra () due to a language / macro limitation
    ASSERT_TRUE(want_to_read == (std::set<int>{1, 2, 3}));
  }

  // large, multi-stripe read starting just after 1st chunk
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      1024, swidth*42, s, chunk_mapping, &want_to_read);
    // extra () due to a language / macro limitation
    ASSERT_TRUE(want_to_read == (std::set<int>{0, 1, 2, 3}));
  }

  // large read from the beginning
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      0, swidth*42, s, chunk_mapping, &want_to_read);
    // extra () due to a language / macro limitation
    ASSERT_TRUE(want_to_read == (std::set<int>{0, 1, 2, 3}));
  }
}

TEST(ECCommon, get_min_want_to_read_shards_bug67087)
{
  const uint64_t swidth = 4096;
  const uint64_t ssize = 4;

  ECUtil::stripe_info_t s(ssize, swidth);
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), 1024);

  const std::vector<int> chunk_mapping = {}; // no remapping

  std::set<int> want_to_read;

  // multitple calls with the same want_to_read can happen during
  // multi-region reads.
  {
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      512, 512, s, chunk_mapping, &want_to_read);
    ASSERT_EQ(want_to_read, std::set<int>{0});
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      512+16*1024, 512, s, chunk_mapping, &want_to_read);
    ASSERT_EQ(want_to_read, std::set<int>{0});
  }
}
