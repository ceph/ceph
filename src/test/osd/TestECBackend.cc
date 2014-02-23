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
#include "osd/ECBackend.h"
#include "gtest/gtest.h"

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

  ASSERT_EQ(s.aligned_offset_len_to_chunk(make_pair(swidth, 10*swidth)),
	    make_pair(s.get_chunk_size(), 10*s.get_chunk_size()));

  ASSERT_EQ(s.offset_len_to_stripe_bounds(make_pair(swidth-10, (uint64_t)20)),
            make_pair((uint64_t)0, 2*swidth));
}

