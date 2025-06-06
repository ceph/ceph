// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_reshard.h"
#include <gtest/gtest.h>


TEST(TestRGWReshard, max_prime_shards)
{
  // assuming we have prime numbers up to 1999
  ASSERT_EQ(1999u, RGWBucketReshard::get_max_prime_shards()) <<
    "initial list has primes up to 1999";
}

TEST(TestRGWReshard, prime_lookups)
{
  ASSERT_EQ(1u, RGWBucketReshard::get_prime_shards_greater_or_equal(1)) <<
    "we allow for 1 shard even though it's not prime";
  ASSERT_EQ(809u, RGWBucketReshard::get_prime_shards_greater_or_equal(808)) <<
    "809 is prime";
  ASSERT_EQ(809u, RGWBucketReshard::get_prime_shards_greater_or_equal(809)) <<
    "809 is prime";
  ASSERT_EQ(811u, RGWBucketReshard::get_prime_shards_greater_or_equal(810)) <<
    "811 is prime";
  ASSERT_EQ(811u, RGWBucketReshard::get_prime_shards_greater_or_equal(811)) <<
    "811 is prime";
  ASSERT_EQ(821u, RGWBucketReshard::get_prime_shards_greater_or_equal(812)) <<
    "821 is prime";

  ASSERT_EQ(1u, RGWBucketReshard::get_prime_shards_less_or_equal(1)) <<
    "we allow for 1 shard even though it's not prime";
  ASSERT_EQ(797u, RGWBucketReshard::get_prime_shards_less_or_equal(808)) <<
    "809 is prime";
  ASSERT_EQ(809u, RGWBucketReshard::get_prime_shards_less_or_equal(809)) <<
    "809 is prime";
  ASSERT_EQ(809u, RGWBucketReshard::get_prime_shards_less_or_equal(810)) <<
    "811 is prime";
  ASSERT_EQ(811u, RGWBucketReshard::get_prime_shards_less_or_equal(811)) <<
    "811 is prime";
  ASSERT_EQ(811u, RGWBucketReshard::get_prime_shards_less_or_equal(812)) <<
    "821 is prime";
}

TEST(TestRGWReshard, nearest_prime)
{
  // tests when max dynamic shards is equal to end of prime list

  ASSERT_EQ(239u, RGWBucketReshard::nearest_prime(238));
  ASSERT_EQ(239u, RGWBucketReshard::nearest_prime(239));
  ASSERT_EQ(241u, RGWBucketReshard::nearest_prime(240));
  ASSERT_EQ(241u, RGWBucketReshard::nearest_prime(241));
  ASSERT_EQ(251u, RGWBucketReshard::nearest_prime(242));

  ASSERT_EQ(1997u, RGWBucketReshard::nearest_prime(1995));
  ASSERT_EQ(1997u, RGWBucketReshard::nearest_prime(1996));
  ASSERT_EQ(1997u, RGWBucketReshard::nearest_prime(1997));
  ASSERT_EQ(1999u, RGWBucketReshard::nearest_prime(1998));
  ASSERT_EQ(1999u, RGWBucketReshard::nearest_prime(1999));
  ASSERT_EQ(2000u, RGWBucketReshard::nearest_prime(2000));
}

TEST(TestRGWReshard, calculate_preferred_shards)
{
  bool needs_resharding;
  uint32_t suggested_shard_count = 0;

  RGWBucketReshard::calculate_preferred_shards(nullptr, 1999, 101, 100000, false, 10000000, 200,
					       needs_resharding, &suggested_shard_count);

  ASSERT_EQ(false, needs_resharding) << "no need to reshard when shards are half-used";


  RGWBucketReshard::calculate_preferred_shards(nullptr, 1999, 101, 100000, false, 20200000, 200,
					       needs_resharding, &suggested_shard_count, false);
  ASSERT_EQ(true, needs_resharding);
  ASSERT_EQ(404, suggested_shard_count) << "number of shards when primes are not preferred";

  RGWBucketReshard::calculate_preferred_shards(nullptr, 1999, 101, 100000, false, 20200000, 200,
					       needs_resharding, &suggested_shard_count, true);
  ASSERT_EQ(true, needs_resharding);
  ASSERT_EQ(409, suggested_shard_count) << "number of shards when primes are preferred";

  RGWBucketReshard::calculate_preferred_shards(nullptr, 1999, 101, 100000, true, 20200000, 200,
					       needs_resharding, &suggested_shard_count, true);
  ASSERT_EQ(true, needs_resharding);
  ASSERT_EQ(1619, suggested_shard_count) <<
    "number of shards under multisite with primes preferred since "
    "multisite quadruples number of shards to reduce need to reshaard";

  RGWBucketReshard::calculate_preferred_shards(nullptr, 1999, 3, 100000, false, 650000, 700,
					       needs_resharding, &suggested_shard_count, true);
  // 650,000 objs across 700 shards -> <1000 objs per shard; 650000 /
  // 50000 = 13
  ASSERT_EQ(true, needs_resharding);
  ASSERT_EQ(13, suggested_shard_count) << "shard reduction without hitting min_layout_shards";

  RGWBucketReshard::calculate_preferred_shards(nullptr, 1999, 3, 100000, false, 350000, 400,
					       needs_resharding, &suggested_shard_count, true);
  // 350,000 objs across 400 shards -> <1000 objs per shard; 350000 /
  // 50000 = 7, but hard-coded minimum of 11
  ASSERT_EQ(true, needs_resharding);
  ASSERT_EQ(11, suggested_shard_count) << "shard reduction and hitting hard-coded minimum of 11";

  RGWBucketReshard::calculate_preferred_shards(nullptr, 1999, 51, 100000, false, 650000, 700,
					       needs_resharding, &suggested_shard_count, true);
  // 650,000 objs across 700 shards -> <1000 objs per shard; 650000 /
  // 50000 = 13, but bucket min of 51
  ASSERT_EQ(true, needs_resharding);
  ASSERT_EQ(51, suggested_shard_count) << "shard reduction and hitting min_layout_shards";
}
