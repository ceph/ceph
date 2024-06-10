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


TEST(TestRGWReshard, dynamic_reshard_shard_count)
{
  // assuming we have prime numbers up to 1999
  ASSERT_EQ(1999u, RGWBucketReshard::get_max_prime_shards()) <<
    "initial list has primes up to 1999";

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

  // tests when max dynamic shards is equal to end of prime list
  ASSERT_EQ(1999u, RGWBucketReshard::get_prime_shard_count(1998, 1999, 11));
  ASSERT_EQ(1999u, RGWBucketReshard::get_prime_shard_count(1999, 1999, 11));
  ASSERT_EQ(1999u, RGWBucketReshard::get_prime_shard_count(2000, 1999, 11));

  // tests when max dynamic shards is above end of prime list
  ASSERT_EQ(1999u, RGWBucketReshard::get_prime_shard_count(1998, 3000, 11));
  ASSERT_EQ(1999u, RGWBucketReshard::get_prime_shard_count(1999, 3000, 11));
  ASSERT_EQ(2000u, RGWBucketReshard::get_prime_shard_count(2000, 3000, 11));
  ASSERT_EQ(2001u, RGWBucketReshard::get_prime_shard_count(2001, 3000, 11));

  // tests when max dynamic shards is below end of prime list
  ASSERT_EQ(500u, RGWBucketReshard::get_prime_shard_count(1998, 500, 11));
  ASSERT_EQ(500u, RGWBucketReshard::get_prime_shard_count(2001, 500, 11));

  // tests when max dynamic shards is below end of prime list
  ASSERT_EQ(499u, RGWBucketReshard::get_prime_shard_count(498, 1999, 499));
  ASSERT_EQ(499u, RGWBucketReshard::get_prime_shard_count(499, 1999, 499));
  ASSERT_EQ(503u, RGWBucketReshard::get_prime_shard_count(500, 1999, 499));
}
