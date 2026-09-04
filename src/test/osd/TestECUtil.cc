// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include <errno.h>
#include <signal.h>
#include "osd/ECUtil.h"
#include "gtest/gtest.h"
#include "osd/osd_types.h"
#include "common/ceph_argparse.h"
#include "osd/ECTransaction.h"
#include "osd/ECCommon.h"
#include "test/osd/ECListenerStub.h"
#include "test/osd/MockErasureCode.h"
using namespace std;
using namespace ECUtil;

namespace {

void verify_offset_cache(const shard_extent_map_t& sem)
{
  shard_extent_map_t cached = sem;
  cached.compute_ro_range();
  ASSERT_EQ(cached, sem);
}

} // anonymous namespace

TEST(ECUtil, stripe_info_t)
{
  const uint64_t swidth = 4096;
  const unsigned int k = 4;
  const unsigned int m = 2;

  stripe_info_t s(k, m, swidth);
  ASSERT_EQ(s.get_stripe_width(), swidth);

  ASSERT_EQ(s.ro_offset_to_next_chunk_offset(0), 0u);
  ASSERT_EQ(s.ro_offset_to_next_chunk_offset(1), s.get_chunk_size());
  ASSERT_EQ(s.ro_offset_to_next_chunk_offset(swidth - 1),
	    s.get_chunk_size());

  ASSERT_EQ(s.ro_offset_to_prev_chunk_offset(0), 0u);
  ASSERT_EQ(s.ro_offset_to_prev_chunk_offset(swidth), s.get_chunk_size());
  ASSERT_EQ(s.ro_offset_to_prev_chunk_offset((swidth * 2) - 1),
	    s.get_chunk_size());

  ASSERT_EQ(s.ro_offset_to_next_stripe_ro_offset(0), 0u);
  ASSERT_EQ(s.ro_offset_to_next_stripe_ro_offset(swidth - 1),
	    s.get_stripe_width());

  ASSERT_EQ(s.ro_offset_to_prev_stripe_ro_offset(swidth), s.get_stripe_width());
  ASSERT_EQ(s.ro_offset_to_prev_stripe_ro_offset(swidth), s.get_stripe_width());
  ASSERT_EQ(s.ro_offset_to_prev_stripe_ro_offset((swidth * 2) - 1),
	    s.get_stripe_width());

  ASSERT_EQ(s.aligned_ro_offset_to_chunk_offset(2*swidth),
	    2*s.get_chunk_size());
  ASSERT_EQ(s.shard_offset_to_ro_offset(shard_id_t(0), 2*s.get_chunk_size()),
	    2*s.get_stripe_width());

  // Stripe 1 + 1 chunk for 10 stripes needs to read 11 stripes starting
  // from 1 because there is a partial stripe at the start and end
  ASSERT_EQ(s.chunk_aligned_ro_range_to_shard_ro_range(swidth+s.get_chunk_size(), 10*swidth),
	    make_pair(s.get_chunk_size(), 11*s.get_chunk_size()));

  // Stripe 1 + 0 chunks for 10 stripes needs to read 10 stripes starting
  // from 1 because there are no partial stripes
  ASSERT_EQ(s.chunk_aligned_ro_range_to_shard_ro_range(swidth, 10*swidth),
	    make_pair(s.get_chunk_size(), 10*s.get_chunk_size()));

  // Stripe 0 + 1 chunk for 10 stripes needs to read 11 stripes starting
  // from 0 because there is a partial stripe at the start and end
  ASSERT_EQ(s.chunk_aligned_ro_range_to_shard_ro_range(s.get_chunk_size(), 10*swidth),
	    make_pair<uint64_t>(0, 11*s.get_chunk_size()));

  // Stripe 0 + 1 chunk for (10 stripes + 1 chunk) needs to read 11 stripes
  // starting from 0 because there is a partial stripe at the start and end
  ASSERT_EQ(s.chunk_aligned_ro_range_to_shard_ro_range(s.get_chunk_size(),
							  10*swidth + s.get_chunk_size()),
	    make_pair<uint64_t>(0, 11*s.get_chunk_size()));

  // Stripe 0 + 2 chunks for (10 stripes + 2 chunks) needs to read 11 stripes
  // starting from 0 because there is a partial stripe at the start
  ASSERT_EQ(s.chunk_aligned_ro_range_to_shard_ro_range(2*s.get_chunk_size(),
    10*swidth + 2*s.get_chunk_size()),
    make_pair<uint64_t>(0, 11*s.get_chunk_size()));

  ASSERT_EQ(s.ro_offset_len_to_stripe_ro_offset_len(swidth-10, (uint64_t)20),
            make_pair((uint64_t)0, 2*swidth));
}


TEST(ECUtil, stripe_info_t_chunk_mapping)
{
  int k=4;
  int m=2;
  int chunk_size = 4096;
  vector<shard_id_t> forward_cm(k+m);
  vector<shard_id_t> reverse_cm(k+m);

  std::iota(forward_cm.begin(), forward_cm.end(), 0);
  std::iota(reverse_cm.rbegin(), reverse_cm.rend(), 0);

  stripe_info_t forward_sinfo1(k, m, chunk_size*k);
  stripe_info_t forward_sinfo2(k, m, chunk_size*k, forward_cm);
  stripe_info_t reverse_sinfo(k, m, chunk_size*k, reverse_cm);

  for (shard_id_t shard_id : forward_cm) {
    raw_shard_id_t raw_shard_id((int)shard_id);
    ASSERT_EQ(shard_id, forward_sinfo1.get_shard(raw_shard_id));
    ASSERT_EQ(raw_shard_id, forward_sinfo1.get_raw_shard(shard_id));
    ASSERT_EQ(shard_id, forward_sinfo2.get_shard(raw_shard_id));
    ASSERT_EQ(raw_shard_id, forward_sinfo2.get_raw_shard(shard_id));
    ASSERT_EQ(shard_id, reverse_sinfo.get_shard(raw_shard_id_t(k + m - int(raw_shard_id) - 1)));
    ASSERT_EQ(raw_shard_id_t(k + m- int(shard_id) - 1), reverse_sinfo.get_raw_shard(shard_id));
  }

  ASSERT_EQ(k, forward_sinfo1.get_k());
  ASSERT_EQ(m, forward_sinfo1.get_m());
  ASSERT_EQ(k+m, forward_sinfo1.get_k_plus_m());
}

TEST(ECUtil, shard_extent_map_t)
{
  int k=4;
  int m=2;
  int chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size*k, vector<shard_id_t>(0));

  // insert_in_shard
  {
    shard_extent_map_t semap(&sinfo);
    int new_off = 512;
    int new_len = 1024;
    shard_id_t shard0(0);
    shard_id_t shard2(2);

    // Empty
    ASSERT_FALSE(semap.contains_shard(shard_id_t(0)));
    ASSERT_FALSE(semap.contains_shard(shard_id_t(1)));
    ASSERT_FALSE(semap.contains_shard(shard_id_t(2)));
    ASSERT_FALSE(semap.contains_shard(shard_id_t(3)));
    ASSERT_TRUE(semap.empty());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), semap.get_ro_start());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), semap.get_ro_end());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), semap.get_start_offset());
    ASSERT_EQ(std::numeric_limits<uint64_t>::max(), semap.get_end_offset());


    // Insert a 1k buffer in shard 2
    buffer::list bl;
    bl.append_zero(new_len);
    semap.insert_in_shard(shard2, new_off, bl);
    ASSERT_FALSE(semap.contains_shard(shard_id_t(0)));
    ASSERT_FALSE(semap.contains_shard(shard_id_t(1)));
    ASSERT_TRUE(semap.contains_shard(shard_id_t(2)));
    ASSERT_FALSE(semap.contains_shard(shard_id_t(3)));
    ASSERT_FALSE(semap.empty());
    ASSERT_EQ(int(shard2) * chunk_size + new_off, semap.get_ro_start());
    ASSERT_EQ(int(shard2) * chunk_size + new_off + new_len, semap.get_ro_end());
    ASSERT_EQ(new_off, semap.get_start_offset());
    ASSERT_EQ(new_off + bl.length(), semap.get_end_offset());
    auto iter = semap.get_extent_map(shard2).begin();
    ASSERT_EQ(new_off, iter.get_off());
    ASSERT_EQ(new_len, iter.get_len());
    ++iter;
    ASSERT_EQ(semap.get_extent_map(shard2).end(), iter);

    // Insert a 1k buffer in shard 0
    semap.insert_in_shard(shard0, new_off, bl);
    ASSERT_TRUE(semap.contains_shard(shard_id_t(0)));
    ASSERT_FALSE(semap.contains_shard(shard_id_t(1)));
    ASSERT_TRUE(semap.contains_shard(shard_id_t(2)));
    ASSERT_FALSE(semap.contains_shard(shard_id_t(3)));
    ASSERT_FALSE(semap.empty());
    ASSERT_EQ(int(shard0) * chunk_size + new_off, semap.get_ro_start());
    ASSERT_EQ(int(shard2) * chunk_size + new_off + new_len, semap.get_ro_end());
    ASSERT_EQ(new_off, semap.get_start_offset());
    ASSERT_EQ(new_off + bl.length(), semap.get_end_offset());
    iter = semap.get_extent_map(shard0).begin();
    ASSERT_EQ(new_off, iter.get_off());
    ASSERT_EQ(new_len, iter.get_len());
    ++iter;
    ASSERT_EQ(semap.get_extent_map(shard0).end(), iter);
    iter = semap.get_extent_map(shard2).begin();
    ASSERT_EQ(new_off, iter.get_off());
    ASSERT_EQ(new_len, iter.get_len());
    ++iter;
    ASSERT_EQ(semap.get_extent_map(shard2).end(), iter);

    /* Insert overlapping into next stripe */
    semap.insert_in_shard(shard2, chunk_size - 512, bl);
    ASSERT_EQ(int(shard0) * chunk_size + new_off, semap.get_ro_start());
    ASSERT_EQ((int(shard2) + k) * chunk_size + 512, semap.get_ro_end());
    ASSERT_EQ(new_off, semap.get_start_offset());
    ASSERT_EQ(chunk_size - 512 + bl.length(), semap.get_end_offset());

    iter = semap.get_extent_map(shard2).begin();
    ASSERT_EQ(new_off, iter.get_off());
    ASSERT_EQ(new_len, iter.get_len());
    ++iter;
    ASSERT_EQ(chunk_size - 512, iter.get_off());
    ASSERT_EQ(new_len, iter.get_len());
    ++iter;
    ASSERT_EQ(semap.get_extent_map(shard2).end(), iter);
  }

  //insert_ro_extent_map
  //erase_after_ro_offset
  {
    shard_extent_map_t semap(&sinfo);
    extent_map emap;
    buffer::list bl1k;
    buffer::list bl16k;
    buffer::list bl64k;

    bl1k.append_zero(1024);
    bl16k.append_zero(chunk_size * k);
    bl64k.append_zero(chunk_size * k * 4);
    shard_extent_set_t ref(sinfo.get_k_plus_m());

    // 1: Strangely aligned. (shard 0 [5~1024])
    emap.insert(5, 1024, bl1k);
    ref[shard_id_t(0)].insert(5, 1024);
    // 2: Start of second chunk (shard 1 [0~1024])
    emap.insert(chunk_size, 1024, bl1k);
    ref[shard_id_t(1)].insert(0, 1024);
    // 3: Overlap two chunks (shard1[3584~512], shard2[0~512])
    emap.insert(chunk_size*2 - 512, 1024, bl1k);
    ref[shard_id_t(1)].insert(3584, 512);
    ref[shard_id_t(2)].insert(0, 512);
    // 4: Overlap two stripes (shard3[3584~512], shard0[4096~512])
    emap.insert(chunk_size*4 - 512, 1024, bl1k);
    ref[shard_id_t(3)].insert(3584, 512);
    ref[shard_id_t(0)].insert(4096, 512);
    // 5: Full stripe (shard*[8192~4096])
    emap.insert(chunk_size*k*2, chunk_size*k, bl16k);
    for (auto &&[_, eset] : ref)
      eset.insert(8192, 4096);
    // 6: Two half stripes (shard0,1[20480~4096], shard 2,3[16384~4096])
    emap.insert(chunk_size*k*4 + 2*chunk_size, chunk_size * k, bl16k);
    ref[shard_id_t(0)].insert(20480, 4096);
    ref[shard_id_t(1)].insert(20480, 4096);
    ref[shard_id_t(2)].insert(16384, 4096);
    ref[shard_id_t(3)].insert(16384, 4096);

    // 7: Two half stripes, strange alignment (shard0,1[36864~4096], shard2[32773~4096], shard3[32784~4096])
    emap.insert(chunk_size*k*8 + 2*chunk_size + 5, chunk_size * k, bl16k);
    ref[shard_id_t(0)].insert(36864, 4096);
    ref[shard_id_t(1)].insert(36864, 4096);
    ref[shard_id_t(2)].insert(32773, 4096);
    ref[shard_id_t(3)].insert(32768, 4096);

    // 8: Multiple stripes (shard*[49152, 16384]
    emap.insert(chunk_size*k*12, chunk_size * k * 4, bl64k);
    for (auto &&[_, eset] : ref)
      eset.insert(49152, 16384);

    semap.insert_ro_extent_map(emap);
    for (auto &&[shard, eset] : ref) {
      ASSERT_EQ(eset, semap.get_extent_set(shard)) << "shard=" << shard;
    }
    ASSERT_EQ(emap.get_start_off(), semap.get_ro_start());
    ASSERT_EQ(emap.get_end_off(), semap.get_ro_end());
    ASSERT_EQ(0, semap.get_start_offset());
    ASSERT_EQ(chunk_size * 16, semap.get_end_offset());

    /* Erase the later parts at an obscure offset. */
    semap.erase_after_ro_offset(chunk_size * k * 8 + 2 * chunk_size + 512);

    {
      extent_set tmp;

      tmp.union_insert(0, chunk_size * 8);
      ref[shard_id_t(3)].intersection_of(tmp);
      tmp.union_insert(0, chunk_size * 8 + 512);
      ref[shard_id_t(2)].intersection_of(tmp);
      tmp.union_insert(0, chunk_size * 9);
      ref[shard_id_t(1)].intersection_of(tmp);
      ref[shard_id_t(0)].intersection_of(tmp);
    }

    for (auto &&[shard, eset] : ref) {
      ASSERT_EQ(eset, semap.get_extent_set(shard)) << "shard=" << shard;
    }
    ASSERT_EQ(5, semap.get_ro_start());
    ASSERT_EQ(chunk_size * k * 8 + 2 * chunk_size + 512, semap.get_ro_end());
    ASSERT_EQ(0, semap.get_start_offset());
    ASSERT_EQ(33280, semap.get_end_offset());

    /* Append again */
    semap.append_zeros_to_ro_offset(chunk_size * k * 9 + 2 * chunk_size + 512);
    ref[shard_id_t(0)].insert(chunk_size * 9, chunk_size);
    ref[shard_id_t(1)].insert(chunk_size * 9, chunk_size);
    ref[shard_id_t(2)].insert(chunk_size * 8 + 512, chunk_size);
    ref[shard_id_t(3)].insert(chunk_size * 8, chunk_size);

    for (auto &&[shard, eset] : ref) {
      ASSERT_EQ(eset, semap.get_extent_set(shard)) << "shard=" << shard;
    }
    ASSERT_EQ(5, semap.get_ro_start());
    ASSERT_EQ(chunk_size * k * 9 + 2 * chunk_size + 512, semap.get_ro_end());
    ASSERT_EQ(0, semap.get_start_offset());
    ASSERT_EQ(chunk_size * 10, semap.get_end_offset());

    /* Append nothing */
    semap.append_zeros_to_ro_offset(chunk_size * k * 9 + 2 * chunk_size + 512);
    for (auto &&[shard, eset] : ref) {
      ASSERT_EQ(eset, semap.get_extent_set(shard)) << "shard=" << shard;
    }
    ASSERT_EQ(5, semap.get_ro_start());
    ASSERT_EQ(chunk_size * k * 9 + 2 * chunk_size + 512, semap.get_ro_end());
    ASSERT_EQ(0, semap.get_start_offset());
    ASSERT_EQ(chunk_size * 10, semap.get_end_offset());

    /* Append, to an offset before the end */
    semap.append_zeros_to_ro_offset(chunk_size * k * 8 + 2 * chunk_size + 512);
    for (auto &&[shard, eset] : ref) {
      ASSERT_EQ(eset, semap.get_extent_set(shard)) << "shard=" << shard;
    }
    ASSERT_EQ(5, semap.get_ro_start());
    ASSERT_EQ(chunk_size * k * 9 + 2 * chunk_size + 512, semap.get_ro_end());
    ASSERT_EQ(0, semap.get_start_offset());
    ASSERT_EQ(chunk_size * 10, semap.get_end_offset());

    /* Intersect the beginning ro range */
    shard_extent_map_t semap2 = semap.intersect_ro_range(chunk_size * 2 - 256,
      chunk_size * k * 8);

    /* The original semap should be untouched */
    for (auto &&[shard, eset] : ref) {
      ASSERT_EQ(eset, semap.get_extent_set(shard)) << "shard=" << shard;
    }
    ASSERT_EQ(5, semap.get_ro_start());
    ASSERT_EQ(chunk_size * k * 9 + 2 * chunk_size + 512, semap.get_ro_end());
    ASSERT_EQ(0, semap.get_start_offset());
    ASSERT_EQ(chunk_size * 10, semap.get_end_offset());
    {
      extent_set tmp;
      tmp.insert(chunk_size, chunk_size * 8);
      ref[shard_id_t(0)].intersection_of(tmp);
    }
    {
      extent_set tmp;
      tmp.insert(chunk_size - 256, chunk_size * 8);
      ref[shard_id_t(1)].intersection_of(tmp);
    }
    {
      extent_set tmp;
      tmp.insert(0, chunk_size * 8);
      ref[shard_id_t(2)].intersection_of(tmp);
      ref[shard_id_t(3)].intersection_of(tmp);
    }

    for (auto &&[shard, eset] : ref) {
      ASSERT_EQ(eset, semap2.get_extent_set(shard)) << "shard=" << shard;
    }
    ASSERT_EQ(chunk_size*2 - 256, semap2.get_ro_start());
    ASSERT_EQ(chunk_size * (k * 5 + 2), semap2.get_ro_end())
      << "semap2=" << semap2;
    ASSERT_EQ(0, semap2.get_start_offset());
    ASSERT_EQ(chunk_size * 6, semap2.get_end_offset());

    // intersect with somethning bigger and it should be identical
    semap2 = semap2.intersect_ro_range(0, chunk_size * k * 10);
    for (auto &&[shard, eset] : ref) {
      ASSERT_EQ(eset, semap2.get_extent_set(shard)) << "shard=" << shard;
    }
    ASSERT_EQ(chunk_size * 2 - 256, semap2.get_ro_start());
    ASSERT_EQ(chunk_size * (k * 5 + 2), semap2.get_ro_end());
    ASSERT_EQ(0, semap2.get_start_offset());
    ASSERT_EQ(chunk_size * 6, semap2.get_end_offset());

    extent_set superset;
    for (auto &&[_, eset] : ref)
      superset.union_of(eset);

    ASSERT_EQ(superset, semap2.get_extent_superset());
  }

  // To test "encode" we need more framework... So will leave to higher level
  // tests.
}

// This scenario went wrong in ec transaction code in a cluster-based test.
TEST(ECUtil, shard_extent_map_t_scenario_1)
{
  int k=2;
  int m=2;
  int chunk_size = 4096;
  stripe_info_t sinfo(k, m,  chunk_size*k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  bufferlist bl;
  bl.append_zero(chunk_size);
  semap.insert_in_shard(shard_id_t(0), chunk_size, bl);
  semap.insert_in_shard(shard_id_t(0), chunk_size*3, bl);
  semap.insert_in_shard(shard_id_t(1), chunk_size, bl);
  semap.insert_in_shard(shard_id_t(1), chunk_size*3, bl);

  for (int i=0; i<k; i++) {
    auto &&iter = semap.get_extent_map(shard_id_t(i)).begin();
    ASSERT_EQ(chunk_size, iter.get_off());
    ASSERT_EQ(chunk_size, iter.get_len());
    ++iter;
    ASSERT_EQ(chunk_size*3, iter.get_off());
    ASSERT_EQ(chunk_size, iter.get_len());
    ++iter;
    ASSERT_EQ(semap.get_extent_map(shard_id_t(i)).end(), iter);
  }
  ASSERT_FALSE(semap.contains_shard(shard_id_t(2)));
  ASSERT_FALSE(semap.contains_shard(shard_id_t(3)));
  ASSERT_EQ(2*chunk_size, semap.get_ro_start());
  ASSERT_EQ(8*chunk_size, semap.get_ro_end());
  ASSERT_EQ(chunk_size, semap.get_start_offset());
  ASSERT_EQ(4*chunk_size, semap.get_end_offset());

  bufferlist bl2;
  bl2.append_zero(2048);
  bl2.c_str()[0]='A';
  ASSERT_EQ('A', bl2.c_str()[0]);
  bufferlist bl3;
  bl3.append_zero(2048);
  bl3.c_str()[0]='B';
  ASSERT_EQ('B', bl3.c_str()[0]);
  sinfo.ro_range_to_shard_extent_map(3*chunk_size, 2048, bl2, semap);
  sinfo.ro_range_to_shard_extent_map(6*chunk_size, 2048, bl3, semap);

  for (int i=0; i<k; i++) {
    auto &&iter = semap.get_extent_map(shard_id_t(i)).begin();
    ASSERT_EQ(chunk_size, iter.get_off());
    ASSERT_EQ(chunk_size, iter.get_len());
    ++iter;
    ASSERT_EQ(chunk_size*3, iter.get_off());
    ASSERT_EQ(chunk_size, iter.get_len());
    ++iter;
    ASSERT_EQ(semap.get_extent_map(shard_id_t(i)).end(), iter);
  }
  ASSERT_FALSE(semap.contains_shard(shard_id_t(2)));
  ASSERT_FALSE(semap.contains_shard(shard_id_t(3)));
  ASSERT_EQ(2*chunk_size, semap.get_ro_start());
  ASSERT_EQ(8*chunk_size, semap.get_ro_end());
  ASSERT_EQ(chunk_size, semap.get_start_offset());
  ASSERT_EQ(4*chunk_size, semap.get_end_offset());


  shard_extent_map_t semap2 = semap.intersect_ro_range(0, 8*chunk_size);
  for (int i=0; i<k; i++) {
    auto &&iter = semap.get_extent_map(shard_id_t(i)).begin();
    ASSERT_EQ(chunk_size, iter.get_off());
    ASSERT_EQ(chunk_size, iter.get_len());
    ++iter;
    ASSERT_EQ(chunk_size*3, iter.get_off());
    ASSERT_EQ(chunk_size, iter.get_len());
    ++iter;
    ASSERT_EQ(semap.get_extent_map(shard_id_t(i)).end(), iter);
  }

  ASSERT_FALSE(semap.contains_shard(shard_id_t(2)));
  ASSERT_FALSE(semap.contains_shard(shard_id_t(3)));

  for (int i=0; i<k; i++) {
    auto &&iter = semap2.get_extent_map(shard_id_t(i)).begin();
    ASSERT_EQ(chunk_size, iter.get_off());
    ASSERT_EQ(chunk_size, iter.get_len());
    ++iter;
    ASSERT_EQ(chunk_size*3, iter.get_off());
    ASSERT_EQ(chunk_size, iter.get_len());
    ++iter;
    ASSERT_EQ(semap2.get_extent_map(shard_id_t(i)).end(), iter);
  }

  ASSERT_FALSE(semap2.contains_shard(shard_id_t(2)));
  ASSERT_FALSE(semap2.contains_shard(shard_id_t(3)));

  semap2.insert_parity_buffers();
  for (int i=0; i<(k+m); i++) {
    auto &&iter = semap2.get_extent_map(shard_id_t(i)).begin();
    ASSERT_EQ(chunk_size, iter.get_off());
    ASSERT_EQ(chunk_size, iter.get_len());
    ++iter;
    ASSERT_EQ(chunk_size*3, iter.get_off());
    ASSERT_EQ(chunk_size, iter.get_len());
    ++iter;
    ASSERT_EQ(semap2.get_extent_map(shard_id_t(i)).end(), iter);
  }
}


// This scenario went wrong in ec transaction code in a cluster-based test.
/*
 *Recreate of this failure:
-171> 2024-10-07T11:38:23.746+0100 7fa0df6f4800  0 == test 1 Random offset, random length read/write I/O with queue depth 1 (seqseed 1137522502) ==
-170> 2024-10-07T11:38:23.746+0100 7fa0df6f4800  5 test Step 0: Create (size=44K)
-169> 2024-10-07T11:38:23.787+0100 7fa0df6f4800  5 test Step 1: Barrier
-168> 2024-10-07T11:38:23.787+0100 7fa0df6f4800  5 test Step 2: Write (offset=38K,length=4K)
-167> 2024-10-07T11:38:23.829+0100 7fa0df6f4800  5 test Step 3: Barrier
-166> 2024-10-07T11:38:23.829+0100 7fa0df6f4800  5 test Step 4: Write (offset=38K,length=4K)
-165> 2024-10-07T11:38:23.876+0100 7fa0df6f4800  5 test Step 5: Barrier
-164> 2024-10-07T11:38:23.876+0100 7fa0df6f4800  5 test Step 6: Write (offset=10K,length=6K)
-163> 2024-10-07T11:38:23.963+0100 7fa0df6f4800  5 test Step 7: Barrier
-162> 2024-10-07T11:38:23.963+0100 7fa0df6f4800  5 test Step 8: Write (offset=30K,length=2K)
*/
TEST(ECUtil, shard_extent_map_t_insert_ro_buffer)
{
  int k=2;
  int m=2;
  int chunk_size = 4096;
  char c = 1;
  stripe_info_t sinfo(k, m, chunk_size*k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  bufferlist bl;
  bl.append_zero(44*1024);

  char *buf = bl.c_str();

  shard_extent_map_t ref_semap(&sinfo);
  ref_semap.append_zeros_to_ro_offset(48*1024);

  for (char i=0; i<44; i++) {
    buf[i*1024] = c;
    int chunk = i/4;
    shard_id_t shard(chunk % k);
    int offset = chunk_size * (chunk / k) + i % 4 * 1024;
    bufferlist tmp;
    ref_semap.get_buffer(shard, offset, 1024, tmp);
    tmp.c_str()[0] = c++;
  }

  sinfo.ro_range_to_shard_extent_map(0, 44*1024, bl, semap);
  semap.assert_buffer_contents_equal(ref_semap);
  bufferlist insert_bl;
  insert_bl.append_zero(2*1024);
  insert_bl.c_str()[0] = c;
  {
    bufferlist tmp;
    ref_semap.get_buffer(shard_id_t(1), 14*1024, 1024, tmp);
    tmp.c_str()[0] = c++;
  }
  insert_bl.c_str()[1024] = c;
  {
    bufferlist tmp;
    ref_semap.get_buffer(shard_id_t(1), 15*1024, 1024, tmp);
    tmp.c_str()[0] = c++;
  }

  sinfo.ro_range_to_shard_extent_map(30*1024, 1024, insert_bl, semap);
  semap.assert_buffer_contents_equal(ref_semap);
}

// Sanity check that k=3 buffer inserts work
TEST(ECUtil, shard_extent_map_t_insert_ro_buffer_3)
{
  int k=3;
  int m=2;
  int chunk_size = 4096;
  uint64_t ro_offset = 10 * 1024;
  uint64_t ro_length = 32 * 1024;

  char c = 5;
  stripe_info_t sinfo(k, m, chunk_size*k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);
  bufferlist ref;
  bufferlist in;
  ref.append_zero(ro_length);
  in.append_zero(ro_length);

  for (uint64_t i=0; i<ro_length; i += 2048) {
    ref.c_str()[i+8] = c;
    in.c_str()[i+8] = c;
    c++;
  }

  extent_map emap_in;
  emap_in.insert(ro_offset, ro_length, in);
  semap.insert_ro_extent_map(emap_in);
  bufferlist out = semap.get_ro_buffer(ro_offset, ro_length);

  ASSERT_TRUE(out.contents_equal(ref)) << semap.debug_string(2048, 0);
}

TEST(ECUtil, sinfo_ro_size_to_read_mask_lrc) {
  std::vector<shard_id_t> chunk_mapping = {shard_id_t(1), shard_id_t(2), shard_id_t(0)};
  stripe_info_t sinfo(2, 1, 2 * 4096, chunk_mapping);

  {
    shard_extent_set_t read_mask(sinfo.get_k_plus_m());
    shard_extent_set_t zero_mask(sinfo.get_k_plus_m());
    sinfo.ro_size_to_read_mask(1, read_mask);
    sinfo.ro_size_to_zero_mask(1, zero_mask);

    shard_extent_set_t ref_read(sinfo.get_k_plus_m());
    shard_extent_set_t ref_zero(sinfo.get_k_plus_m());
    ref_read[shard_id_t(1)].insert(0, 4096);
    ref_zero[shard_id_t(2)].insert(0, 4096);
    ref_read[shard_id_t(0)].insert(0, 4096);

    ASSERT_EQ(ref_read, read_mask);
    ASSERT_EQ(ref_zero, zero_mask);
  }

  {
    shard_extent_set_t read_mask(sinfo.get_k_plus_m());
    shard_extent_set_t zero_mask(sinfo.get_k_plus_m());
    sinfo.ro_size_to_read_mask(38912, read_mask);
    sinfo.ro_size_to_zero_mask(38912, zero_mask);

    shard_extent_set_t ref_read(sinfo.get_k_plus_m());
    shard_extent_set_t ref_zero(sinfo.get_k_plus_m());
    ref_read[shard_id_t(1)].insert(0, 20480);
    ref_read[shard_id_t(2)].insert(0, 20480);
    ref_read[shard_id_t(0)].insert(0, 20480);

    ASSERT_EQ(ref_read, read_mask);
    ASSERT_EQ(ref_zero, zero_mask);
  }
}

TEST(ECUtil, sinfo_ro_size_to_read_mask) {
  stripe_info_t sinfo(2, 1, 16*4096);

  {
    shard_extent_set_t read_mask(sinfo.get_k_plus_m());
    shard_extent_set_t zero_mask(sinfo.get_k_plus_m());
    sinfo.ro_size_to_read_mask(1, read_mask);
    sinfo.ro_size_to_zero_mask(1, zero_mask);

    shard_extent_set_t ref_read(sinfo.get_k_plus_m());
    shard_extent_set_t ref_zero(sinfo.get_k_plus_m());
    ref_read[shard_id_t(0)].insert(0, 4096);
    ref_zero[shard_id_t(1)].insert(0, 4096);
    ref_read[shard_id_t(2)].insert(0, 4096);

    ASSERT_EQ(ref_read, read_mask);
    ASSERT_EQ(ref_zero, zero_mask);
  }

  {
    shard_extent_set_t read_mask(sinfo.get_k_plus_m());
    shard_extent_set_t zero_mask(sinfo.get_k_plus_m());
    sinfo.ro_size_to_read_mask(4096, read_mask);
    sinfo.ro_size_to_zero_mask(4096, zero_mask);

    shard_extent_set_t ref_read(sinfo.get_k_plus_m());
    shard_extent_set_t ref_zero(sinfo.get_k_plus_m());
    ref_read[shard_id_t(0)].insert(0, 4096);
    ref_zero[shard_id_t(1)].insert(0, 4096);
    ref_read[shard_id_t(2)].insert(0, 4096);

    ASSERT_EQ(ref_read, read_mask);
    ASSERT_EQ(ref_zero, zero_mask);
  }

  {
    shard_extent_set_t read_mask(sinfo.get_k_plus_m());
    shard_extent_set_t zero_mask(sinfo.get_k_plus_m());
    sinfo.ro_size_to_read_mask(4097, read_mask);
    sinfo.ro_size_to_zero_mask(4097, zero_mask);

    shard_extent_set_t ref_read(sinfo.get_k_plus_m());
    shard_extent_set_t ref_zero(sinfo.get_k_plus_m());
    ref_read[shard_id_t(0)].insert(0, 8192);
    ref_zero[shard_id_t(1)].insert(0, 8192);
    ref_read[shard_id_t(2)].insert(0, 8192);

    ASSERT_EQ(ref_read, read_mask);
    ASSERT_EQ(ref_zero, zero_mask);
  }

  {
    shard_extent_set_t read_mask(sinfo.get_k_plus_m());
    shard_extent_set_t zero_mask(sinfo.get_k_plus_m());
    sinfo.ro_size_to_read_mask(8*4096+1, read_mask);
    sinfo.ro_size_to_zero_mask(8*4096+1, zero_mask);

    shard_extent_set_t ref_read(sinfo.get_k_plus_m());
    shard_extent_set_t ref_zero(sinfo.get_k_plus_m());
    ref_read[shard_id_t(0)].insert(0, 8*4096);
    ref_read[shard_id_t(1)].insert(0, 4096);
    ref_zero[shard_id_t(1)].insert(4096, 7*4096);
    ref_read[shard_id_t(2)].insert(0, 8*4096);

    ASSERT_EQ(ref_read, read_mask);
    ASSERT_EQ(ref_zero, zero_mask);
  }

  {
    shard_extent_set_t read_mask(sinfo.get_k_plus_m());
    shard_extent_set_t zero_mask(sinfo.get_k_plus_m());
    sinfo.ro_size_to_read_mask(16*4096+1, read_mask);
    sinfo.ro_size_to_zero_mask(16*4096+1, zero_mask);

    shard_extent_set_t ref_read(sinfo.get_k_plus_m());
    shard_extent_set_t ref_zero(sinfo.get_k_plus_m());
    ref_read[shard_id_t(0)].insert(0, 9*4096);
    ref_read[shard_id_t(1)].insert(0, 8*4096);
    ref_zero[shard_id_t(1)].insert(8*4096, 1*4096);
    ref_read[shard_id_t(2)].insert(0, 9*4096);

    ASSERT_EQ(ref_read, read_mask);
    ASSERT_EQ(ref_zero, zero_mask);
  }
}

TEST(ECUtil, slice_iterator)
{
  stripe_info_t sinfo(2, 1, 2*4096);
  shard_id_set out_set;
  out_set.insert_range(shard_id_t(0), 3);
  shard_extent_map_t sem(&sinfo);
  {
    auto iter = sem.begin_slice_iterator(out_set, nullptr);
    ASSERT_TRUE(iter.get_out_bufferptrs().empty());
  }

  bufferlist a, b;
  a.append_zero(8192);
  a.c_str()[0] = 'A';
  a.c_str()[4096] = 'C';
  b.append_zero(4096);
  b.c_str()[0] = 'B';

  sem.insert_in_shard(shard_id_t(0), 0, a);
  sem.insert_in_shard(shard_id_t(1), 0, b);
  {
    auto iter = sem.begin_slice_iterator(out_set, nullptr);

    {
      auto out = iter.get_out_bufferptrs();
      ASSERT_EQ(0, iter.get_offset());
      ASSERT_EQ(4096, iter.get_length());
      ASSERT_EQ(2, out.size());
      ASSERT_EQ(4096, out[shard_id_t(0)].length());
      ASSERT_EQ(4096, out[shard_id_t(1)].length());
      ASSERT_EQ('A', out[shard_id_t(0)].c_str()[0]);
      ASSERT_EQ('B', out[shard_id_t(1)].c_str()[0]);
    }

    ++iter;
    {
      auto out = iter.get_out_bufferptrs();

      ASSERT_EQ(4096, iter.get_offset());
      ASSERT_EQ(4096, iter.get_length());
      ASSERT_FALSE(out.empty());
      ASSERT_EQ(1, out.size());
      ASSERT_EQ(4096, out[shard_id_t(0)].length());
      ASSERT_EQ('C', out[shard_id_t(0)].c_str()[0]);
    }

    ++iter;
    ASSERT_TRUE(iter.is_end());
  }

  // Create a gap.
  bufferlist d, e;
  d.append_zero(4096);
  d.c_str()[0] = 'D';
  e.append_zero(4096);
  e.c_str()[0] = 'E';
  sem.insert_in_shard(shard_id_t(0), 4096*4, d);
  sem.insert_in_shard(shard_id_t(1), 4096*4, e);

  {
    auto iter = sem.begin_slice_iterator(out_set, nullptr);

    {
      auto out = iter.get_out_bufferptrs();
      ASSERT_EQ(0, iter.get_offset());
      ASSERT_EQ(4096, iter.get_length());
      ASSERT_FALSE(out.empty());
      ASSERT_EQ(2, out.size());
      ASSERT_EQ(4096, out[shard_id_t(0)].length());
      ASSERT_EQ(4096, out[shard_id_t(1)].length());
      ASSERT_EQ('A', out[shard_id_t(0)].c_str()[0]);
      ASSERT_EQ('B', out[shard_id_t(1)].c_str()[0]);
    }

    ++iter;
    {
      auto out = iter.get_out_bufferptrs();
      ASSERT_EQ(4096, iter.get_offset());
      ASSERT_EQ(4096, iter.get_length());
      ASSERT_FALSE(out.empty());
      ASSERT_EQ(1, out.size());
      ASSERT_EQ(4096, out[shard_id_t(0)].length());
      ASSERT_EQ('C', out[shard_id_t(0)].c_str()[0]);
    }

    ++iter;
    {
      auto out = iter.get_out_bufferptrs();
      ASSERT_EQ(4*4096, iter.get_offset());
      ASSERT_EQ(4096, iter.get_length());
      ASSERT_FALSE(out.empty());
      ASSERT_EQ(2, out.size());
      ASSERT_EQ(4096, out[shard_id_t(0)].length());
      ASSERT_EQ('D', out[shard_id_t(0)].c_str()[0]);
      ASSERT_EQ('E', out[shard_id_t(1)].c_str()[0]);
    }

    ++iter;
    ASSERT_TRUE(iter.is_end());
  }

  // Multiple buffers in each shard and gap at start.
  sem.clear();
  a.clear();
  a.append_zero(4096);
  a.c_str()[0] = 'A';
  bufferlist c;
  c.append_zero(4096);
  c.c_str()[0] = 'C';

  sem.insert_in_shard(shard_id_t(0), 4096*1, a);
  sem.insert_in_shard(shard_id_t(1), 4096*1, b);
  sem.insert_in_shard(shard_id_t(0), 4096*2, c);
  sem.insert_in_shard(shard_id_t(1), 4096*2, d);

  {
    auto iter = sem.begin_slice_iterator(out_set, nullptr);

    {
      auto out = iter.get_out_bufferptrs();
      ASSERT_EQ(4096, iter.get_offset());
      ASSERT_EQ(4096, iter.get_length());
      ASSERT_FALSE(out.empty());
      ASSERT_EQ(2, out.size());
      ASSERT_EQ(4096, out[shard_id_t(0)].length());
      ASSERT_EQ(4096, out[shard_id_t(1)].length());
      ASSERT_EQ('A', out[shard_id_t(0)].c_str()[0]);
      ASSERT_EQ('B', out[shard_id_t(1)].c_str()[0]);
    }

    ++iter;
    {
      auto out = iter.get_out_bufferptrs();
      ASSERT_EQ(2*4096, iter.get_offset());
      ASSERT_EQ(4096, iter.get_length());
      ASSERT_FALSE(out.empty());
      ASSERT_EQ(2, out.size());
      ASSERT_EQ(4096, out[shard_id_t(0)].length());
      ASSERT_EQ(4096, out[shard_id_t(1)].length());
      ASSERT_EQ('C', out[shard_id_t(0)].c_str()[0]);
      ASSERT_EQ('D', out[shard_id_t(1)].c_str()[0]);
    }

    ++iter;
    ASSERT_TRUE(iter.is_end());
  }

}
TEST(ECUtil, slice_iterator_subset_out)
{
  stripe_info_t sinfo(2, 1, 2*4096);
  shard_id_set out_set;
  out_set.insert(shard_id_t(1));
  shard_extent_map_t sem(&sinfo);
  {
    auto iter = sem.begin_slice_iterator(out_set, nullptr);
    ASSERT_TRUE(iter.get_in_bufferptrs().empty());
    ASSERT_TRUE(iter.get_out_bufferptrs().empty());
  }

  bufferlist a, b;
  a.append_zero(8192);
  a.c_str()[0] = 'A';
  a.c_str()[4096] = 'C';
  b.append_zero(4096);
  b.c_str()[0] = 'B';

  sem.insert_in_shard(shard_id_t(0), 0, a);
  sem.insert_in_shard(shard_id_t(1), 0, b);
  {
    auto iter = sem.begin_slice_iterator(out_set, nullptr);

    {
      auto in = iter.get_in_bufferptrs();
      auto out = iter.get_out_bufferptrs();
      ASSERT_EQ(0, iter.get_offset());
      ASSERT_EQ(4096, iter.get_length());
      ASSERT_EQ(1, in.size());
      ASSERT_EQ(1, out.size());
      ASSERT_EQ(4096, in[shard_id_t(0)].length());
      ASSERT_EQ(4096, out[shard_id_t(1)].length());
      ASSERT_EQ('A', in[shard_id_t(0)].c_str()[0]);
      ASSERT_EQ('B', out[shard_id_t(1)].c_str()[0]);
    }

    /* The iterator only cares about outputs, so doesn't care that there is an
     * extra 4k to go.
     */
    ++iter;
    ASSERT_TRUE(iter.is_end());
  }

  // Create a gap.
  bufferlist d, e;
  d.append_zero(4096);
  d.c_str()[0] = 'D';
  e.append_zero(4096);
  e.c_str()[0] = 'E';
  sem.insert_in_shard(shard_id_t(0), 4096*4, d);
  sem.insert_in_shard(shard_id_t(1), 4096*4, e);

  {
    auto iter = sem.begin_slice_iterator(out_set, nullptr);

    {
      auto in = iter.get_in_bufferptrs();
      auto out = iter.get_out_bufferptrs();

      ASSERT_EQ(0, iter.get_offset());
      ASSERT_EQ(4096, iter.get_length());
      ASSERT_FALSE(in.empty());
      ASSERT_FALSE(out.empty());
      ASSERT_EQ(1, in.size());
      ASSERT_EQ(1, out.size());
      ASSERT_EQ(4096, in[shard_id_t(0)].length());
      ASSERT_EQ(4096, out[shard_id_t(1)].length());
      ASSERT_EQ('A', in[shard_id_t(0)].c_str()[0]);
      ASSERT_EQ('B', out[shard_id_t(1)].c_str()[0]);
    }

    // Skip the next 4k, since it is not in the output buffer.

    ++iter;
    {
      auto in = iter.get_in_bufferptrs();
      auto out = iter.get_out_bufferptrs();

      ASSERT_EQ(4*4096, iter.get_offset());
      ASSERT_EQ(4096, iter.get_length());
      ASSERT_FALSE(in.empty());
      ASSERT_FALSE(out.empty());
      ASSERT_EQ(1, in.size());
      ASSERT_EQ(1, out.size());
      ASSERT_EQ(4096, in[shard_id_t(0)].length());
      ASSERT_EQ('D', in[shard_id_t(0)].c_str()[0]);
      ASSERT_EQ('E', out[shard_id_t(1)].c_str()[0]);
    }

    ++iter;
    ASSERT_TRUE(iter.is_end());
  }

  // Multiple buffers in each shard and gap at start.
  sem.clear();
  a.clear();
  a.append_zero(4096);
  a.c_str()[0] = 'A';
  bufferlist c;
  c.append_zero(4096);
  c.c_str()[0] = 'C';

  sem.insert_in_shard(shard_id_t(0), 4096*1, a);
  sem.insert_in_shard(shard_id_t(1), 4096*1, b);
  sem.insert_in_shard(shard_id_t(0), 4096*2, c);
  sem.insert_in_shard(shard_id_t(1), 4096*2, d);

  {
    auto iter = sem.begin_slice_iterator(out_set, nullptr);

    {
      auto in = iter.get_in_bufferptrs();
      auto out = iter.get_out_bufferptrs();

      ASSERT_EQ(4096, iter.get_offset());
      ASSERT_EQ(4096, iter.get_length());
      ASSERT_FALSE(in.empty());
      ASSERT_FALSE(out.empty());
      ASSERT_EQ(1, in.size());
      ASSERT_EQ(1, out.size());
      ASSERT_EQ(4096, in[shard_id_t(0)].length());
      ASSERT_EQ(4096, out[shard_id_t(1)].length());
      ASSERT_EQ('A', in[shard_id_t(0)].c_str()[0]);
      ASSERT_EQ('B', out[shard_id_t(1)].c_str()[0]);
    }

    ++iter;
    {
      auto in = iter.get_in_bufferptrs();
      auto out = iter.get_out_bufferptrs();

      ASSERT_EQ(2*4096, iter.get_offset());
      ASSERT_EQ(4096, iter.get_length());
      ASSERT_FALSE(in.empty());
      ASSERT_FALSE(out.empty());
      ASSERT_EQ(1, in.size());
      ASSERT_EQ(1, out.size());
      ASSERT_EQ(4096, in[shard_id_t(0)].length());
      ASSERT_EQ(4096, out[shard_id_t(1)].length());
      ASSERT_EQ('C', in[shard_id_t(0)].c_str()[0]);
      ASSERT_EQ('D', out[shard_id_t(1)].c_str()[0]);
    }

    ++iter;
    ASSERT_TRUE(iter.is_end());
  }

}


TEST(ECUtil, object_size_to_shard_size)
{
  // This should return aligned values, inputs verifying that the result is
  // aligned to the next page
  std::vector<uint64_t> inputs = {0x4D000, 0x4CCFF, 0x4C001};

  stripe_info_t sinfo(4, 2, 4*4096);
  for (uint64_t input : inputs)
  {
    ASSERT_EQ(0x14000, sinfo.object_size_to_shard_size(input, shard_id_t(0)));
    ASSERT_EQ(0x13000, sinfo.object_size_to_shard_size(input, shard_id_t(1)));
    ASSERT_EQ(0x13000, sinfo.object_size_to_shard_size(input, shard_id_t(2)));
    ASSERT_EQ(0x13000, sinfo.object_size_to_shard_size(input, shard_id_t(3)));
    ASSERT_EQ(0x14000, sinfo.object_size_to_shard_size(input, shard_id_t(4)));
    ASSERT_EQ(0x14000, sinfo.object_size_to_shard_size(input, shard_id_t(5)));
  }

  // Verify +/-1 also rounds correctly
  ASSERT_EQ(0x13000, sinfo.object_size_to_shard_size(0x4C000, shard_id_t(0)));
  ASSERT_EQ(0x14000, sinfo.object_size_to_shard_size(0x4D001, shard_id_t(1)));
}

TEST(ECUtil, slice)
{
  int k=4;
  int m=2;
  int chunk_size = 4096;
  stripe_info_t sinfo(k, m, k*4096);
  shard_extent_map_t sem(&sinfo);

  extent_map emap;
  buffer::list bl1k;
  buffer::list bl4k;
  buffer::list bl16k;
  buffer::list bl64k;

  bl1k.append_zero(1024);
  bl4k.append_zero(4096);
  bl16k.append_zero(chunk_size * k);
  bl64k.append_zero(chunk_size * k * 4);
  shard_extent_set_t ref(sinfo.get_k_plus_m());

  sem.insert_in_shard(shard_id_t(1), 512, bl1k);
  sem.insert_in_shard(shard_id_t(2), 5, bl4k);
  sem.insert_in_shard(shard_id_t(3), 256, bl16k);
  sem.insert_in_shard(shard_id_t(4), 5, bl64k);

  {
    auto slice_map = sem.slice_map(512, 1024);
    ASSERT_EQ(4, slice_map.get_extent_maps().size());
    verify_offset_cache(slice_map);
  }

  {
    shard_extent_map_t single(&sinfo);
    single.insert_in_shard(shard_id_t(1), 512, bl1k);

    auto slice_map = single.slice_map(512, 1024);
    ASSERT_EQ(1, slice_map.get_extent_maps().size());
    verify_offset_cache(slice_map);
  }

  {
    shard_extent_map_t single(&sinfo);
    single.insert_in_shard(shard_id_t(1), 512, bl1k);

    auto slice_map = single.slice_map(0, 4096);
    ASSERT_EQ(1, slice_map.get_extent_maps().size());
    verify_offset_cache(slice_map);
  }

  {
    auto slice_map = sem.slice_map(0, 4096);
    ASSERT_EQ(4, slice_map.get_extent_maps().size());
    verify_offset_cache(slice_map);
  }

  {
    auto slice_map = sem.slice_map(0, 5);
    ASSERT_TRUE(slice_map.empty());
  }

  {
    auto slice_map = sem.slice_map(64*1024+5, 5);
    ASSERT_TRUE(slice_map.empty());
  }

  {
    auto slice_map = sem.slice_map(5, 64*1024);
    ASSERT_EQ(slice_map, sem);
  }

  {
    auto slice_map = sem.slice_map(0, 65*1024);
    ASSERT_EQ(slice_map, sem);
  }
}

TEST(ECUtil, insert_parity_buffer_into_sem) {
  int k=2;
  int m=2;
  int chunk_size = 4096;
  stripe_info_t sinfo(k, m, k*chunk_size);

  buffer::list bl1k;
  buffer::list bl4k;
  bl1k.append_zero(1024);
  bl4k.append_zero(4096);

  {
    shard_extent_map_t sem(&sinfo);
    sem.insert_in_shard(shard_id_t(2), 0, bl1k);
    ASSERT_EQ(-1, sem.ro_start);
    ASSERT_EQ(-1, sem.ro_end);
  }

  {
    shard_extent_map_t sem(&sinfo);
    sem.insert_in_shard(shard_id_t(0), 0, bl4k);
    ASSERT_EQ(0, sem.ro_start);
    ASSERT_EQ(4096, sem.ro_end);
    sem.insert_in_shard(shard_id_t(2), 0, bl4k);
    ASSERT_EQ(0, sem.ro_start);
    ASSERT_EQ(4096, sem.ro_end);
  }

  {
    shard_extent_map_t sem(&sinfo);
    sem.insert_in_shard(shard_id_t(1), 0, bl4k);
    ASSERT_EQ(4096, sem.ro_start);
    ASSERT_EQ(8192, sem.ro_end);
    sem.insert_in_shard(shard_id_t(2), 0, bl4k);
    ASSERT_EQ(4096, sem.ro_start);
    ASSERT_EQ(8192, sem.ro_end);
  }

  {
    shard_extent_map_t sem(&sinfo);
    sem.insert_in_shard(shard_id_t(1), 0, bl4k);
    ASSERT_EQ(4096, sem.ro_start);
    ASSERT_EQ(8192, sem.ro_end);
    sem.insert_in_shard(shard_id_t(3), 0, bl4k);
    ASSERT_EQ(4096, sem.ro_start);
    ASSERT_EQ(8192, sem.ro_end);
  }
}

// Debug String test, to track down seg-fault found by teuthology.
TEST(ECUtil, debug_string)
{
  int k=3;
  int m=2;
  int chunk_size = 4096;

  stripe_info_t sinfo(k, m, chunk_size*k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  bufferlist bl0, bl1;
  bl0.append_zero(750);
  bl1.append_zero(3516);

  semap.insert_in_shard(shard_id_t(0), 352256, bl0);
  semap.insert_in_shard(shard_id_t(0), 348740, bl1);

  semap.debug_string(2048, 0);
}

// Comprehensive tests for erase_after_ro_offset
// These tests cover various edge cases and scenarios that were not previously tested

TEST(ECUtil, erase_after_ro_offset_empty_map)
{
  // Test erasing on an empty shard extent map
  int k = 2;
  int m = 1;
  int chunk_size = 4096;

  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  // Should not crash on empty map
  semap.erase_after_ro_offset(0);
  semap.erase_after_ro_offset(chunk_size);
  semap.erase_after_ro_offset(chunk_size * k);

  ASSERT_TRUE(semap.empty());
}

TEST(ECUtil, erase_after_ro_offset_at_stripe_boundary)
{
  // Test erasing exactly at stripe boundaries
  int k = 2;
  int m = 1;
  int chunk_size = 4096;

  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  // Insert two full stripes
  bufferlist bl;
  bl.append_zero(chunk_size * k * 2);
  extent_map emap;
  emap.insert(0, chunk_size * k * 2, bl);
  semap.insert_ro_extent_map(emap);

  // Verify initial state: both shards should have 2 chunks each
  ASSERT_EQ(0, semap.get_ro_start());
  ASSERT_EQ(chunk_size * k * 2, semap.get_ro_end());
  ASSERT_TRUE(semap.contains_shard(shard_id_t(0)));
  ASSERT_TRUE(semap.contains_shard(shard_id_t(1)));

  // Erase after first stripe boundary
  semap.erase_after_ro_offset(chunk_size * k);

  // Should have exactly one stripe left
  ASSERT_EQ(0, semap.get_ro_start());
  ASSERT_EQ(chunk_size * k, semap.get_ro_end());
  ASSERT_TRUE(semap.contains_shard(shard_id_t(0)));
  ASSERT_TRUE(semap.contains_shard(shard_id_t(1)));

  extent_set shard0_extents = semap.get_extent_set(shard_id_t(0));
  extent_set shard1_extents = semap.get_extent_set(shard_id_t(1));
  ASSERT_EQ(chunk_size, shard0_extents.range_end());
  ASSERT_EQ(chunk_size, shard1_extents.range_end());
}

TEST(ECUtil, erase_after_ro_offset_within_stripe)
{
  // Test erasing in the middle of a stripe
  int k = 2;
  int m = 1;
  int chunk_size = 4096;

  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  // Insert one full stripe
  bufferlist bl;
  bl.append_zero(chunk_size * k);
  extent_map emap;
  emap.insert(0, chunk_size * k, bl);
  semap.insert_ro_extent_map(emap);

  // Erase after half a stripe (should affect shard 1)
  semap.erase_after_ro_offset(chunk_size);

  ASSERT_EQ(0, semap.get_ro_start());
  ASSERT_EQ(chunk_size, semap.get_ro_end());
  
  // Shard 0 should still have data
  ASSERT_TRUE(semap.contains_shard(shard_id_t(0)));
  extent_set shard0_extents = semap.get_extent_set(shard_id_t(0));
  ASSERT_EQ(chunk_size, shard0_extents.range_end());

  // Shard 1 should be empty
  ASSERT_FALSE(semap.contains_shard(shard_id_t(1)));
}

TEST(ECUtil, erase_after_ro_offset_misaligned)
{
  // Test erasing at misaligned offsets
  int k = 2;
  int m = 1;
  int chunk_size = 4096;

  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  // Insert data
  bufferlist bl;
  bl.append_zero(chunk_size * k * 2);
  extent_map emap;
  emap.insert(0, chunk_size * k * 2, bl);
  semap.insert_ro_extent_map(emap);

  // Erase at misaligned offset (1.5 chunks)
  uint64_t erase_offset = chunk_size + chunk_size / 2;
  semap.erase_after_ro_offset(erase_offset);

  ASSERT_EQ(0, semap.get_ro_start());
  ASSERT_EQ(erase_offset, semap.get_ro_end());

  // Shard 0 should have full chunk
  ASSERT_TRUE(semap.contains_shard(shard_id_t(0)));
  extent_set shard0_extents = semap.get_extent_set(shard_id_t(0));
  ASSERT_EQ(chunk_size, shard0_extents.range_end());

  // Shard 1 should have partial chunk
  ASSERT_TRUE(semap.contains_shard(shard_id_t(1)));
  extent_set shard1_extents = semap.get_extent_set(shard_id_t(1));
  ASSERT_EQ(chunk_size / 2, shard1_extents.range_end());
}

TEST(ECUtil, erase_after_ro_offset_before_start)
{
  // NOTE: This tests detects the issue fixed in the same commit in ECUtil.cc
  // Test erasing before the start of data (should erase everything)
  int k = 2;
  int m = 1;
  int chunk_size = 4096;

  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  // Insert data starting at offset chunk_size
  bufferlist bl;
  bl.append_zero(chunk_size * k);
  extent_map emap;
  emap.insert(chunk_size, chunk_size * k, bl);
  semap.insert_ro_extent_map(emap);

  ASSERT_EQ(chunk_size, semap.get_ro_start());
  ASSERT_EQ(chunk_size * k + chunk_size, semap.get_ro_end());

  // Erase before the start
  semap.erase_after_ro_offset(512);

  // Everything should be erased
  ASSERT_TRUE(semap.empty());
}

TEST(ECUtil, erase_after_ro_offset_after_end)
{
  // Test erasing after the end of data (should do nothing)
  int k = 2;
  int m = 1;
  int chunk_size = 4096;

  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  // Insert data
  bufferlist bl;
  bl.append_zero(chunk_size * k);
  extent_map emap;
  emap.insert(0, chunk_size * k, bl);
  semap.insert_ro_extent_map(emap);

  uint64_t original_start = semap.get_ro_start();
  uint64_t original_end = semap.get_ro_end();

  // Erase after the end
  semap.erase_after_ro_offset(chunk_size * k * 2);

  // Nothing should change
  ASSERT_EQ(original_start, semap.get_ro_start());
  ASSERT_EQ(original_end, semap.get_ro_end());
  ASSERT_TRUE(semap.contains_shard(shard_id_t(0)));
  ASSERT_TRUE(semap.contains_shard(shard_id_t(1)));
}

TEST(ECUtil, erase_after_ro_offset_partial_shard_data)
{
  int k = 2;
  int m = 2;
  int chunk_size = 16384;

  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  // Insert 12KB of data (less than one full stripe of 32KB)
  // This should only populate shard 0
  bufferlist bl;
  bl.append_zero(12288);
  extent_map emap;
  emap.insert(0, 12288, bl);
  semap.insert_ro_extent_map(emap);

  // Verify initial state
  ASSERT_EQ(0, semap.get_ro_start());
  ASSERT_EQ(12288, semap.get_ro_end());
  ASSERT_TRUE(semap.contains_shard(shard_id_t(0)));
  ASSERT_FALSE(semap.contains_shard(shard_id_t(1)));

  // Now simulate what happens during recovery: decode adds data to shard 1
  // (This simulates the bug where decode incorrectly populates shard 1)
  bufferlist shard1_bl;
  shard1_bl.append_zero(4096);
  semap.insert_in_shard(shard_id_t(1), 8192, shard1_bl);

  // Now shard 1 incorrectly has data
  ASSERT_TRUE(semap.contains_shard(shard_id_t(1)));

  // Erase after aligned object size (16KB)
  uint64_t aligned_size = 16384;
  semap.erase_after_ro_offset(aligned_size);

  // After erase, shard 1 should be completely empty
  // This is the critical assertion that would fail with the bug
  ASSERT_FALSE(semap.contains_shard(shard_id_t(1)));

  // Shard 0 should still have data up to 12KB
  ASSERT_TRUE(semap.contains_shard(shard_id_t(0)));
  extent_set shard0_extents = semap.get_extent_set(shard_id_t(0));
  ASSERT_EQ(12288, shard0_extents.range_end());
}

TEST(ECUtil, erase_after_ro_offset_multiple_stripes)
{
  // Test erasing across multiple stripes
  int k = 3;
  int m = 2;
  int chunk_size = 4096;

  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  // Insert 4 full stripes
  bufferlist bl;
  bl.append_zero(chunk_size * k * 4);
  extent_map emap;
  emap.insert(0, chunk_size * k * 4, bl);
  semap.insert_ro_extent_map(emap);

  // Erase after 2.5 stripes
  uint64_t erase_offset = chunk_size * k * 2 + chunk_size;
  semap.erase_after_ro_offset(erase_offset);

  ASSERT_EQ(0, semap.get_ro_start());
  ASSERT_EQ(erase_offset, semap.get_ro_end());

  // All shards should still exist
  for (int i = 0; i < k; i++) {
    ASSERT_TRUE(semap.contains_shard(shard_id_t(i)));
  }

  // Shard 0 should have 3 chunks (2 full stripes + 1 from partial)
  extent_set shard0_extents = semap.get_extent_set(shard_id_t(0));
  ASSERT_EQ(chunk_size * 3, shard0_extents.range_end());

  // Shards 1 and 2 should have 2 chunks each
  extent_set shard1_extents = semap.get_extent_set(shard_id_t(1));
  extent_set shard2_extents = semap.get_extent_set(shard_id_t(2));
  ASSERT_EQ(chunk_size * 2, shard1_extents.range_end());
  ASSERT_EQ(chunk_size * 2, shard2_extents.range_end());
}

TEST(ECUtil, erase_after_ro_offset_sparse_data)
{
  // Test erasing with sparse (non-contiguous) data
  int k = 2;
  int m = 1;
  int chunk_size = 4096;

  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  // Insert sparse data: first stripe and third stripe
  bufferlist bl1, bl2;
  bl1.append_zero(chunk_size * k);
  bl2.append_zero(chunk_size * k);
  extent_map emap;
  emap.insert(0, chunk_size * k, bl1);
  emap.insert(chunk_size * k * 2, chunk_size * k, bl2);
  semap.insert_ro_extent_map(emap);

  ASSERT_EQ(0, semap.get_ro_start());
  ASSERT_EQ(chunk_size * k * 3, semap.get_ro_end());

  // Erase after 2.5 stripes (in the middle of the third stripe)
  semap.erase_after_ro_offset(chunk_size * k * 2 + chunk_size);

  ASSERT_EQ(0, semap.get_ro_start());
  ASSERT_EQ(chunk_size * k * 2 + chunk_size, semap.get_ro_end());

  // Shard 0 should have data from first stripe and partial third stripe
  extent_set shard0_extents = semap.get_extent_set(shard_id_t(0));
  ASSERT_TRUE(shard0_extents.contains(0, chunk_size));
  ASSERT_TRUE(shard0_extents.contains(chunk_size * 2, chunk_size));

  // Shard 1 should only have data from first stripe
  extent_set shard1_extents = semap.get_extent_set(shard_id_t(1));
  ASSERT_TRUE(shard1_extents.contains(0, chunk_size));
  ASSERT_FALSE(shard1_extents.contains(chunk_size * 2, chunk_size));
}

TEST(ECUtil, erase_after_ro_offset_with_offset_data)
{
  // Test erasing when data doesn't start at offset 0
  int k = 2;
  int m = 1;
  int chunk_size = 4096;

  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  // Insert data starting at offset 1024 (within first chunk)
  // This will only affect shard 0
  bufferlist bl;
  bl.append_zero(chunk_size - 1024);
  extent_map emap;
  emap.insert(1024, chunk_size - 1024, bl);
  semap.insert_ro_extent_map(emap);

  ASSERT_EQ(1024, semap.get_ro_start());
  ASSERT_EQ(chunk_size, semap.get_ro_end());

  // Verify only shard 0 has data
  ASSERT_TRUE(semap.contains_shard(shard_id_t(0)));
  ASSERT_FALSE(semap.contains_shard(shard_id_t(1)));

  // Erase after 1024 + 512 (keep only first 512 bytes)
  semap.erase_after_ro_offset(1024 + 512);

  ASSERT_EQ(1024, semap.get_ro_start());
  ASSERT_EQ(1024 + 512, semap.get_ro_end());

  // Shard 0 should have partial data
  ASSERT_TRUE(semap.contains_shard(shard_id_t(0)));
  extent_set shard0_extents = semap.get_extent_set(shard_id_t(0));
  // The shard extent goes from 1024 to 1024+512 = 1536
  ASSERT_EQ(1536, shard0_extents.range_end());

  // Shard 1 should still be empty
  ASSERT_FALSE(semap.contains_shard(shard_id_t(1)));
}

TEST(ECUtil, erase_after_ro_offset_k4_m2)
{
  // Test with different k/m configuration (k=4, m=2)
  int k = 4;
  int m = 2;
  int chunk_size = 4096;

  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  // Insert 2 full stripes
  bufferlist bl;
  bl.append_zero(chunk_size * k * 2);
  extent_map emap;
  emap.insert(0, chunk_size * k * 2, bl);
  semap.insert_ro_extent_map(emap);

  // Erase after 1.5 stripes (should affect shards 2 and 3)
  uint64_t erase_offset = chunk_size * k + chunk_size * 2;
  semap.erase_after_ro_offset(erase_offset);

  ASSERT_EQ(0, semap.get_ro_start());
  ASSERT_EQ(erase_offset, semap.get_ro_end());

  // Shards 0 and 1 should have 2 chunks each
  extent_set shard0_extents = semap.get_extent_set(shard_id_t(0));
  extent_set shard1_extents = semap.get_extent_set(shard_id_t(1));
  ASSERT_EQ(chunk_size * 2, shard0_extents.range_end());
  ASSERT_EQ(chunk_size * 2, shard1_extents.range_end());

  // Shards 2 and 3 should have 1 chunk each
  extent_set shard2_extents = semap.get_extent_set(shard_id_t(2));
  extent_set shard3_extents = semap.get_extent_set(shard_id_t(3));
  ASSERT_EQ(chunk_size, shard2_extents.range_end());
  ASSERT_EQ(chunk_size, shard3_extents.range_end());
}

TEST(ECUtil, erase_after_ro_offset_exact_chunk_boundary)
{
  // Test erasing exactly at chunk boundaries within a stripe
  int k = 3;
  int m = 1;
  int chunk_size = 4096;

  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  // Insert one full stripe
  bufferlist bl;
  bl.append_zero(chunk_size * k);
  extent_map emap;
  emap.insert(0, chunk_size * k, bl);
  semap.insert_ro_extent_map(emap);

  // Erase after exactly 2 chunks (should keep shards 0 and 1, remove shard 2)
  semap.erase_after_ro_offset(chunk_size * 2);

  ASSERT_EQ(0, semap.get_ro_start());
  ASSERT_EQ(chunk_size * 2, semap.get_ro_end());

  ASSERT_TRUE(semap.contains_shard(shard_id_t(0)));
  ASSERT_TRUE(semap.contains_shard(shard_id_t(1)));
  ASSERT_FALSE(semap.contains_shard(shard_id_t(2)));

  extent_set shard0_extents = semap.get_extent_set(shard_id_t(0));
  extent_set shard1_extents = semap.get_extent_set(shard_id_t(1));
  ASSERT_EQ(chunk_size, shard0_extents.range_end());
  ASSERT_EQ(chunk_size, shard1_extents.range_end());
}

TEST(ECUtil, erase_after_ro_offset_single_byte)
{
  // Test erasing after just one byte
  int k = 2;
  int m = 1;
  int chunk_size = 4096;

  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t semap(&sinfo);

  // Insert data
  bufferlist bl;
  bl.append_zero(chunk_size * k);
  extent_map emap;
  emap.insert(0, chunk_size * k, bl);
  semap.insert_ro_extent_map(emap);

  // Erase after 1 byte
  semap.erase_after_ro_offset(1);

  ASSERT_EQ(0, semap.get_ro_start());
  ASSERT_EQ(1, semap.get_ro_end());

  // Shard 0 should have just 1 byte
  ASSERT_TRUE(semap.contains_shard(shard_id_t(0)));
  extent_set shard0_extents = semap.get_extent_set(shard_id_t(0));
  ASSERT_EQ(1, shard0_extents.range_end());

  // Shard 1 should be empty
  ASSERT_FALSE(semap.contains_shard(shard_id_t(1)));
}

// Tests for merge_shard_extent_maps()

TEST(ECUtil, merge_shard_extent_maps_empty)
{
  int k = 2, m = 1;
  int chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));

  shard_id_map<std::map<uint64_t, uint64_t>> shard_extents(k + m);
  auto result = merge_shard_extent_maps(shard_extents, sinfo);
  ASSERT_TRUE(result.empty());
}

TEST(ECUtil, merge_shard_extent_maps_contiguous)
{
  // k=2, m=1, chunk_size=4096 => stripe_width=8192
  // Each shard holds one chunk per stripe.
  // Shard 0: [0, 4096) => RO [0, 4096)
  // Shard 1: [0, 4096) => RO [4096, 8192)
  // Together the full stripe [0, 8192) is covered.
  int k = 2, m = 1;
  int chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));

  shard_id_map<std::map<uint64_t, uint64_t>> shard_extents(k + m);
  shard_extents[shard_id_t(0)][0] = chunk_size;
  shard_extents[shard_id_t(1)][0] = chunk_size;

  auto result = merge_shard_extent_maps(shard_extents, sinfo);
  // Shard 0 covers RO [0,4096) and shard 1 covers RO [4096,8192); they are
  // contiguous so they must be merged into a single extent.
  ASSERT_EQ(1u, result.size());
  ASSERT_EQ((uint64_t)chunk_size * 2, result.at(0));
}

TEST(ECUtil, merge_shard_extent_maps_sparse_one_shard)
{
  // Only shard 0 has data; shard 1 is absent (hole).
  // Shard 0: [0, 4096) => RO [0, 4096)
  int k = 2, m = 1;
  int chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));

  shard_id_map<std::map<uint64_t, uint64_t>> shard_extents(k + m);
  shard_extents[shard_id_t(0)][0] = chunk_size;

  auto result = merge_shard_extent_maps(shard_extents, sinfo);
  ASSERT_EQ(1u, result.size());
  ASSERT_EQ((uint64_t)chunk_size, result.at(0));
}

TEST(ECUtil, merge_shard_extent_maps_parity_ignored)
{
  // Parity shard (shard k=2) should not contribute to the result.
  int k = 2, m = 1;
  int chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));

  shard_id_map<std::map<uint64_t, uint64_t>> shard_extents(k + m);
  shard_extents[shard_id_t(2)][0] = chunk_size; // parity shard

  auto result = merge_shard_extent_maps(shard_extents, sinfo);
  ASSERT_TRUE(result.empty());
}

TEST(ECUtil, merge_shard_extent_maps_multi_stripe)
{
  // k=2, m=1, chunk_size=4096.
  // Two stripes: shards each have two chunks [0,4096) and [4096,8192).
  // Shard 0, stripe 0: RO [0, 4096); stripe 1: RO [8192, 12288)
  // Shard 1, stripe 0: RO [4096, 8192); stripe 1: RO [12288, 16384)
  int k = 2, m = 1;
  int chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));

  shard_id_map<std::map<uint64_t, uint64_t>> shard_extents(k + m);
  // Two contiguous chunks per shard = [0, 8192) in shard space
  shard_extents[shard_id_t(0)][0] = chunk_size * 2;
  shard_extents[shard_id_t(1)][0] = chunk_size * 2;

  auto result = merge_shard_extent_maps(shard_extents, sinfo);
  // Two full stripes covering RO [0,16384) — all four chunk extents are
  // contiguous and must be merged into a single extent.
  ASSERT_EQ(1u, result.size());
  ASSERT_EQ((uint64_t)chunk_size * 4, result.at(0));
}

TEST(ECUtil, get_sparse_buffer_zero_not_force_allocated)
{
  int k = 2;
  int m = 1;
  int chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  // append_zero2() produces is_zero_fast() buffer_ptrs backed by raw_zeros.
  bufferlist bl;
  bl.append_zero2(chunk_size);
  ASSERT_TRUE(bl.buffers().front().is_zero_fast());
  sem.insert_in_shard(shard_id_t(0), 0, bl);

  bufferlist out;
  interval_set<uint64_t> iset;

  // No force_alloc hint: zero-fast buffer must be skipped.
  sem.get_sparse_buffer(shard_id_t(0), out, iset);

  ASSERT_EQ(0u, out.length());
  ASSERT_TRUE(iset.empty());
}

TEST(ECUtil, get_sparse_buffer_zero_force_allocated)
{
  int k = 2;
  int m = 1;
  int chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  // append_zero2() produces is_zero_fast() buffer_ptrs backed by raw_zeros.
  bufferlist bl;
  bl.append_zero2(chunk_size);
  ASSERT_TRUE(bl.buffers().front().is_zero_fast());
  sem.insert_in_shard(shard_id_t(0), 0, bl);

  // Mark the whole first chunk as force-allocated in shard space.
  interval_set<uint64_t> force_alloc;
  force_alloc.insert(0, chunk_size);

  bufferlist out;
  interval_set<uint64_t> iset;

  sem.get_sparse_buffer(shard_id_t(0), out, iset, &force_alloc);

  // The zero-fast buffer must be included because it is force-allocated.
  ASSERT_EQ((uint64_t)chunk_size, out.length());
  ASSERT_EQ(1u, iset.num_intervals());
  ASSERT_TRUE(iset.contains(0, chunk_size));
}

TEST(ECUtil, get_sparse_buffer_nonzero_always_kept)
{
  int k = 2;
  int m = 1;
  int chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  bufferlist bl;
  bl.append_zero(chunk_size);
  bl.c_str()[0] = 'X'; // make it non-zero
  sem.insert_in_shard(shard_id_t(0), 0, bl);

  bufferlist out;
  interval_set<uint64_t> iset;

  // Without force_alloc: non-zero block must be kept.
  sem.get_sparse_buffer(shard_id_t(0), out, iset);

  ASSERT_EQ((uint64_t)chunk_size, out.length());
  ASSERT_TRUE(iset.contains(0, chunk_size));
}

TEST(ECUtil, get_sparse_buffer_mixed_force_alloc)
{
  int k = 2;
  int m = 1;
  int chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  // Build a bufferlist with two separate buffer_ptrs:
  //   chunk 0: regular (non-zero-fast) buffer with a non-zero byte
  //   chunk 1: zero-fast buffer via append_zero2()
  bufferlist bl;
  {
    bufferlist nonzero;
    nonzero.append_zero(chunk_size);
    nonzero.c_str()[0] = 'X';
    bl.append(nonzero);
  }
  bl.append_zero2(chunk_size); // produces an is_zero_fast() buffer_ptr

  // Verify our setup: the second buffer_ptr must be zero-fast.
  auto buf_iter = bl.buffers().begin();
  ++buf_iter; // advance to the second buffer_ptr
  ASSERT_TRUE(buf_iter->is_zero_fast());

  sem.insert_in_shard(shard_id_t(0), 0, bl);

  // Only the second chunk (offset chunk_size) is force-allocated.
  interval_set<uint64_t> force_alloc;
  force_alloc.insert(chunk_size, chunk_size);

  bufferlist out;
  interval_set<uint64_t> iset;
  sem.get_sparse_buffer(shard_id_t(0), out, iset, &force_alloc);

  // Both chunks must appear: chunk 0 because non-zero, chunk 1 because FAE.
  ASSERT_EQ((uint64_t)chunk_size * 2, out.length());
  ASSERT_TRUE(iset.contains(0, chunk_size));
  ASSERT_TRUE(iset.contains(chunk_size, chunk_size));
}

TEST(ECUtil, get_sparse_buffer_absent_shard)
{
  int k = 2;
  int m = 1;
  int chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  // No data inserted for shard 0.
  interval_set<uint64_t> force_alloc;
  force_alloc.insert(0, chunk_size);

  bufferlist out;
  interval_set<uint64_t> iset;
  sem.get_sparse_buffer(shard_id_t(0), out, iset, &force_alloc);

  ASSERT_EQ(0u, out.length());
  ASSERT_TRUE(iset.empty());
}

TEST(ECUtil, subtask4_absent_shard_no_fae)
{
  int k = 2, m = 1, chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size, nullptr, data, iset);
  ASSERT_TRUE(iset.empty());
  ASSERT_EQ(0u, data.length());
}

TEST(ECUtil, subtask4_absent_shard_full_fae)
{
  int k = 2, m = 1, chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  interval_set<uint64_t> force_alloc;
  force_alloc.insert(0, (uint64_t)chunk_size);

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size, &force_alloc, data, iset);

  // The entire force-allocated range must appear in data_included.
  ASSERT_TRUE(iset.contains(0, (uint64_t)chunk_size));
  // The data buffer must be all zeros of matching length.
  ASSERT_EQ((uint64_t)chunk_size, data.length());
  ASSERT_EQ(data.length(), iset.size());
  bufferlist expected;
  expected.append_zero(chunk_size);
  ASSERT_TRUE(data.contents_equal(expected));
}

TEST(ECUtil, subtask4_absent_shard_sparse_fae)
{
  int k = 2, m = 1, chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  interval_set<uint64_t> force_alloc;
  force_alloc.insert(0, (uint64_t)chunk_size);
  force_alloc.insert((uint64_t)(2 * chunk_size), (uint64_t)chunk_size);

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size, &force_alloc, data, iset);

  ASSERT_TRUE(iset.contains(0, (uint64_t)chunk_size));
  ASSERT_TRUE(iset.contains((uint64_t)(2 * chunk_size), (uint64_t)chunk_size));
  ASSERT_EQ(2u, iset.num_intervals());
  ASSERT_EQ(data.length(), iset.size());
  ASSERT_EQ((uint64_t)(2 * chunk_size), data.length());
}

TEST(ECUtil, subtask4_partial_data_plus_fae)
{
  int k = 2, m = 1, chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  // Insert non-zero real data at [0, chunk_size) for shard 0.
  bufferlist real_data;
  real_data.append(std::string(chunk_size, 'A'));
  sem.insert_in_shard(shard_id_t(0), 0, real_data);

  // FAE covers [0, chunk_size) AND [2*chunk_size, 3*chunk_size).
  // [0, chunk_size) is non-zero so the scan includes it directly.
  // [2*chunk_size, 3*chunk_size) has no decoded extent; the FAE synthesis
  // path appends zeros for it.
  interval_set<uint64_t> force_alloc;
  force_alloc.insert(0, (uint64_t)chunk_size);
  force_alloc.insert((uint64_t)(2 * chunk_size), (uint64_t)chunk_size);

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size, &force_alloc, data, iset);

  ASSERT_TRUE(iset.contains(0, (uint64_t)chunk_size));
  ASSERT_TRUE(iset.contains((uint64_t)(2 * chunk_size), (uint64_t)chunk_size));
  ASSERT_EQ(2u, iset.num_intervals());
  ASSERT_EQ(data.length(), iset.size());
  ASSERT_EQ((uint64_t)(2 * chunk_size), data.length());
}

TEST(ECUtil, subtask4_fae_fully_covered_by_data)
{
  int k = 2, m = 1, chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  // Insert non-zero real data at [0, chunk_size).
  bufferlist real_data;
  real_data.append(std::string(chunk_size, 'B'));
  sem.insert_in_shard(shard_id_t(0), 0, real_data);

  // FAE also covers exactly [0, chunk_size) — entirely inside returned data.
  interval_set<uint64_t> force_alloc;
  force_alloc.insert(0, (uint64_t)chunk_size);

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size, &force_alloc, data, iset);

  // Only one interval — no synthetic zeros added.
  ASSERT_EQ(1u, iset.num_intervals());
  ASSERT_TRUE(iset.contains(0, (uint64_t)chunk_size));
  ASSERT_EQ((uint64_t)chunk_size, data.length());
}

// ---------------------------------------------------------------------------
// ec_recovery_compute_shard_push — multi-chunk tests
//
// Each test uses k=4, m=2, chunk_size=4096 and exercises shard 0 at three
// distinct shard-space offsets: chunk 0 = [0, cs), chunk 1 = [cs, 2cs),
// chunk 2 = [2cs, 3cs).  These correspond to the same shard across three
// consecutive stripes.
//
// "data"  = non-zero bufferlist (append(std::string(cs, 'X')))
// "zeros" = decoded-zero bufferlist (append_zero(cs)) — what the EC decoder
//           actually produces; treated as a sparse hole unless in FAE.
// ---------------------------------------------------------------------------

// chunk 0 and 2 have real data; chunk 1 is absent.
// Expected: chunks 0 and 2 allocated, chunk 1 absent.
TEST(ECUtil, recovery_push_data_at_0_and_2)
{
  int k = 4, m = 2, chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  bufferlist d0, d2;
  d0.append(std::string(chunk_size, 'A'));
  d2.append(std::string(chunk_size, 'B'));
  sem.insert_in_shard(shard_id_t(0), 0, d0);
  sem.insert_in_shard(shard_id_t(0), 2 * chunk_size, d2);

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size, nullptr, data, iset);

  ASSERT_EQ(2u, iset.num_intervals());
  ASSERT_TRUE(iset.contains(0, (uint64_t)chunk_size));
  ASSERT_FALSE(iset.intersects((uint64_t)chunk_size, (uint64_t)chunk_size));
  ASSERT_TRUE(iset.contains((uint64_t)(2 * chunk_size), (uint64_t)chunk_size));
  ASSERT_EQ((uint64_t)(2 * chunk_size), data.length());
  ASSERT_EQ(data.length(), iset.size());
}

// chunk 0 and 2 have decoded-zero content; no FAE.
// Expected: all chunks unallocated.
TEST(ECUtil, recovery_push_zeros_at_0_and_2_no_fae)
{
  int k = 4, m = 2, chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  bufferlist z0, z2;
  z0.append_zero(chunk_size);
  z2.append_zero(chunk_size);
  sem.insert_in_shard(shard_id_t(0), 0, z0);
  sem.insert_in_shard(shard_id_t(0), 2 * chunk_size, z2);

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size, nullptr, data, iset);

  ASSERT_TRUE(iset.empty());
  ASSERT_EQ(0u, data.length());
}

// chunk 0 and 2 have decoded-zero content; only chunk 0 is in FAE.
// Expected: chunk 0 allocated (zero data via FAE intersect path), chunk 2 unallocated.
TEST(ECUtil, recovery_push_zeros_at_0_and_2_fae_covers_0_only)
{
  int k = 4, m = 2, chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  bufferlist z0, z2;
  z0.append_zero(chunk_size);
  z2.append_zero(chunk_size);
  sem.insert_in_shard(shard_id_t(0), 0, z0);
  sem.insert_in_shard(shard_id_t(0), 2 * chunk_size, z2);

  interval_set<uint64_t> fae;
  fae.insert(0, (uint64_t)chunk_size);   // chunk 0 only

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size, &fae, data, iset);

  ASSERT_EQ(1u, iset.num_intervals());
  ASSERT_TRUE(iset.contains(0, (uint64_t)chunk_size));
  ASSERT_FALSE(iset.intersects((uint64_t)(2 * chunk_size), (uint64_t)chunk_size));
  ASSERT_EQ((uint64_t)chunk_size, data.length());
  ASSERT_EQ(data.length(), iset.size());
  bufferlist expected;
  expected.append_zero(chunk_size);
  ASSERT_TRUE(data.contents_equal(expected));
}

// chunk 0 and 2 have decoded-zero content; only chunk 2 is in FAE.
// Expected: chunk 2 allocated (zero data via FAE intersect path), chunk 0 unallocated.
TEST(ECUtil, recovery_push_zeros_at_0_and_2_fae_covers_2_only)
{
  int k = 4, m = 2, chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  bufferlist z0, z2;
  z0.append_zero(chunk_size);
  z2.append_zero(chunk_size);
  sem.insert_in_shard(shard_id_t(0), 0, z0);
  sem.insert_in_shard(shard_id_t(0), 2 * chunk_size, z2);

  interval_set<uint64_t> fae;
  fae.insert((uint64_t)(2 * chunk_size), (uint64_t)chunk_size);  // chunk 2 only

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size, &fae, data, iset);

  ASSERT_EQ(1u, iset.num_intervals());
  ASSERT_FALSE(iset.intersects(0, (uint64_t)chunk_size));
  ASSERT_TRUE(iset.contains((uint64_t)(2 * chunk_size), (uint64_t)chunk_size));
  ASSERT_EQ((uint64_t)chunk_size, data.length());
  ASSERT_EQ(data.length(), iset.size());
  bufferlist expected;
  expected.append_zero(chunk_size);
  ASSERT_TRUE(data.contents_equal(expected));
}

// chunk 0 has decoded-zero content, chunk 2 has real data; no FAE.
// Expected: only chunk 2 allocated.
TEST(ECUtil, recovery_push_zero_at_0_data_at_2_no_fae)
{
  int k = 4, m = 2, chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  bufferlist z0, d2;
  z0.append_zero(chunk_size);
  d2.append(std::string(chunk_size, 'C'));
  sem.insert_in_shard(shard_id_t(0), 0, z0);
  sem.insert_in_shard(shard_id_t(0), 2 * chunk_size, d2);

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size, nullptr, data, iset);

  ASSERT_EQ(1u, iset.num_intervals());
  ASSERT_FALSE(iset.intersects(0, (uint64_t)chunk_size));
  ASSERT_TRUE(iset.contains((uint64_t)(2 * chunk_size), (uint64_t)chunk_size));
  ASSERT_EQ((uint64_t)chunk_size, data.length());
  ASSERT_EQ(data.length(), iset.size());
}

// chunk 0 has decoded-zero content (in FAE), chunk 2 has real data; no chunk 1.
// Expected: chunk 0 allocated (zero data via FAE intersect path), chunk 1 absent,
//           chunk 2 allocated (real data via non-zero scan).
TEST(ECUtil, recovery_push_zero_at_0_fae_data_at_2)
{
  int k = 4, m = 2, chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  bufferlist z0, d2;
  z0.append_zero(chunk_size);
  d2.append(std::string(chunk_size, 'D'));
  sem.insert_in_shard(shard_id_t(0), 0, z0);
  sem.insert_in_shard(shard_id_t(0), 2 * chunk_size, d2);

  interval_set<uint64_t> fae;
  fae.insert(0, (uint64_t)chunk_size);  // chunk 0 in FAE

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size, &fae, data, iset);

  ASSERT_EQ(2u, iset.num_intervals());
  ASSERT_TRUE(iset.contains(0, (uint64_t)chunk_size));
  ASSERT_FALSE(iset.intersects((uint64_t)chunk_size, (uint64_t)chunk_size));
  ASSERT_TRUE(iset.contains((uint64_t)(2 * chunk_size), (uint64_t)chunk_size));
  ASSERT_EQ((uint64_t)(2 * chunk_size), data.length());
  ASSERT_EQ(data.length(), iset.size());
}

// chunk 0 has real data, chunk 2 has decoded-zero content (in FAE);
// FAE also covers chunk 1 which has no decoded extent at all.
// Expected: chunk 0 (real data) + chunk 1 (synthesised zeros, absent from
//           extent_map) + chunk 2 (zero data from extent_map, FAE-covered).
TEST(ECUtil, recovery_push_data_at_0_fae_at_1_and_2_zeros_at_2)
{
  int k = 4, m = 2, chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  bufferlist d0, z2;
  d0.append(std::string(chunk_size, 'E'));
  z2.append_zero(chunk_size);
  sem.insert_in_shard(shard_id_t(0), 0, d0);
  sem.insert_in_shard(shard_id_t(0), 2 * chunk_size, z2);
  // chunk 1 has no entry in sem at all.

  interval_set<uint64_t> fae;
  fae.insert((uint64_t)chunk_size, (uint64_t)chunk_size);       // chunk 1
  fae.insert((uint64_t)(2 * chunk_size), (uint64_t)chunk_size); // chunk 2

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size, &fae, data, iset);

  // All three adjacent chunks merge into a single contiguous interval.
  ASSERT_TRUE(iset.contains(0, (uint64_t)(3 * chunk_size)));
  ASSERT_EQ((uint64_t)(3 * chunk_size), iset.size());
  ASSERT_EQ((uint64_t)(3 * chunk_size), data.length());
}

// FAE covers chunk 1 but no data exists anywhere in the shard.
// Expected: only chunk 1 (synthesised zeros), chunks 0 and 2 absent.
TEST(ECUtil, recovery_push_absent_shard_fae_middle_chunk_only)
{
  int k = 4, m = 2, chunk_size = 4096;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  interval_set<uint64_t> fae;
  fae.insert((uint64_t)chunk_size, (uint64_t)chunk_size);  // chunk 1 only

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size, &fae, data, iset);

  ASSERT_EQ(1u, iset.num_intervals());
  ASSERT_FALSE(iset.intersects(0, (uint64_t)chunk_size));
  ASSERT_TRUE(iset.contains((uint64_t)chunk_size, (uint64_t)chunk_size));
  ASSERT_FALSE(iset.intersects((uint64_t)(2 * chunk_size), (uint64_t)chunk_size));
  ASSERT_EQ((uint64_t)chunk_size, data.length());
  ASSERT_EQ(data.length(), iset.size());
  bufferlist expected;
  expected.append_zero(chunk_size);
  ASSERT_TRUE(data.contents_equal(expected));
}

// ---------------------------------------------------------------------------
// ec_recovery_compute_shard_push — chunk_size > FAE_BLOCK_SIZE tests
//
// These tests use chunk_size = 2 * FAE_BLOCK_SIZE (8 KiB) so that each shard
// chunk spans two 4 KiB FAE sub-blocks.  They verify that the scan_stride is
// FAE_BLOCK_SIZE (not chunk_size) in this regime, so that:
//   - FAE coverage of only part of a chunk forces only that sub-block.
//   - A zero sub-block inside an otherwise-non-zero chunk is excluded.
// ---------------------------------------------------------------------------

// chunk_size = 8 KiB; only the first 4 KiB sub-block of chunk 0 is in FAE;
// the entire chunk is decoded zeros.
// Expected: only [0, FAE_BLOCK_SIZE) allocated (via FAE), second half excluded.
TEST(ECUtil, recovery_push_large_chunk_fae_half_of_chunk)
{
  const uint64_t fae = FAE_BLOCK_SIZE;            // 4096
  const uint64_t chunk_size = 2 * fae;            // 8192
  int k = 2, m = 1;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  // Whole chunk is decoded zeros (matches what _decode() actually produces).
  bufferlist z;
  z.append_zero(chunk_size);
  sem.insert_in_shard(shard_id_t(0), 0, z);

  // FAE only covers the first 4 KiB sub-block of the chunk.
  interval_set<uint64_t> force_alloc;
  force_alloc.insert(0, fae);

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size,
                                         &force_alloc, data, iset);

  // Only the FAE-covered sub-block should be included.
  ASSERT_EQ(1u, iset.num_intervals());
  ASSERT_TRUE(iset.contains(0, fae));
  ASSERT_FALSE(iset.intersects(fae, fae));     // second half excluded
  ASSERT_EQ(fae, data.length());
  ASSERT_EQ(data.length(), iset.size());
  // Data must be all zeros.
  bufferlist expected;
  expected.append_zero(fae);
  ASSERT_TRUE(data.contents_equal(expected));
}

// chunk_size = 8 KiB; first sub-block zero, second sub-block non-zero; no FAE.
// Expected: only the second sub-block [FAE_BLOCK_SIZE, 2*FAE_BLOCK_SIZE) allocated.
TEST(ECUtil, recovery_push_large_chunk_zero_then_data_no_fae)
{
  const uint64_t fae = FAE_BLOCK_SIZE;
  const uint64_t chunk_size = 2 * fae;
  int k = 2, m = 1;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);

  // Build one shard extent: first half zero, second half non-zero.
  bufferlist bl;
  bl.append_zero(fae);
  bl.append(std::string(fae, 'X'));
  sem.insert_in_shard(shard_id_t(0), 0, bl);

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size,
                                         nullptr, data, iset);

  // Only the non-zero second sub-block must be included.
  ASSERT_EQ(1u, iset.num_intervals());
  ASSERT_FALSE(iset.intersects(0, fae));
  ASSERT_TRUE(iset.contains(fae, fae));
  ASSERT_EQ(fae, data.length());
  ASSERT_EQ(data.length(), iset.size());
  bufferlist expected;
  expected.append(std::string(fae, 'X'));
  ASSERT_TRUE(data.contents_equal(expected));
}

// chunk_size = 8 KiB; absent shard (no extent_map), FAE covers only second
// sub-block of chunk 0.
// Expected: only [FAE_BLOCK_SIZE, 2*FAE_BLOCK_SIZE) synthesised as zeros.
TEST(ECUtil, recovery_push_large_chunk_absent_shard_fae_second_half)
{
  const uint64_t fae = FAE_BLOCK_SIZE;
  const uint64_t chunk_size = 2 * fae;
  int k = 2, m = 1;
  stripe_info_t sinfo(k, m, chunk_size * k, vector<shard_id_t>(0));
  shard_extent_map_t sem(&sinfo);  // shard absent from sem

  interval_set<uint64_t> force_alloc;
  force_alloc.insert(fae, fae);   // second sub-block only

  bufferlist data;
  interval_set<uint64_t> iset;
  ECUtil::ec_recovery_compute_shard_push(sem, shard_id_t(0), chunk_size,
                                         &force_alloc, data, iset);

  ASSERT_EQ(1u, iset.num_intervals());
  ASSERT_FALSE(iset.intersects(0, fae));
  ASSERT_TRUE(iset.contains(fae, fae));
  ASSERT_EQ(fae, data.length());
  ASSERT_EQ(data.length(), iset.size());
  bufferlist expected;
  expected.append_zero(fae);
  ASSERT_TRUE(data.contents_equal(expected));
}

TEST(ECUtil, ro_intervals_to_shard_intervals_empty)
{
  // Empty input always produces an empty result for any shard.
  stripe_info_t sinfo(3, 2, 3 * 4096, vector<shard_id_t>(0));
  interval_set<uint64_t> ro;
  ASSERT_TRUE(sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(0)).empty());
  ASSERT_TRUE(sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(1)).empty());
  ASSERT_TRUE(sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(2)).empty());
}

TEST(ECUtil, ro_intervals_to_shard_intervals_single_chunk_shard0)
{
  // RO [0, 4096) lives entirely in shard 0 of stripe 0.
  stripe_info_t sinfo(3, 2, 3 * 4096, vector<shard_id_t>(0));
  const int chunk_size = 4096;

  interval_set<uint64_t> ro;
  ro.insert(0, chunk_size);

  auto s0 = sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(0));
  ASSERT_EQ(1u, s0.num_intervals());
  ASSERT_TRUE(s0.contains(0, chunk_size));

  auto s1 = sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(1));
  ASSERT_TRUE(s1.empty());

  auto s2 = sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(2));
  ASSERT_TRUE(s2.empty());
}

TEST(ECUtil, ro_intervals_to_shard_intervals_single_chunk_shard1)
{
  // RO [4096, 8192) lives entirely in shard 1 of stripe 0.
  stripe_info_t sinfo(3, 2, 3 * 4096, vector<shard_id_t>(0));
  const int chunk_size = 4096;

  interval_set<uint64_t> ro;
  ro.insert(chunk_size, chunk_size);

  auto s0 = sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(0));
  ASSERT_TRUE(s0.empty());

  auto s1 = sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(1));
  ASSERT_EQ(1u, s1.num_intervals());
  ASSERT_TRUE(s1.contains(0, chunk_size));

  auto s2 = sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(2));
  ASSERT_TRUE(s2.empty());
}

TEST(ECUtil, ro_intervals_to_shard_intervals_full_stripe)
{
  // RO [0, 12288) = one full stripe; each shard gets [0, 4096).
  stripe_info_t sinfo(3, 2, 3 * 4096, vector<shard_id_t>(0));
  const int chunk_size = 4096;
  const int stripe_width = 3 * chunk_size;

  interval_set<uint64_t> ro;
  ro.insert(0, stripe_width);

  for (int s = 0; s < 3; ++s) {
    auto shard_iset = sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(s));
    ASSERT_EQ(1u, shard_iset.num_intervals()) << "shard=" << s;
    ASSERT_TRUE(shard_iset.contains(0, chunk_size)) << "shard=" << s;
  }
}

TEST(ECUtil, ro_intervals_to_shard_intervals_multi_stripe)
{
  // RO [0, 24576) = two full stripes; each shard gets [0, 8192).
  stripe_info_t sinfo(3, 2, 3 * 4096, vector<shard_id_t>(0));
  const int chunk_size = 4096;
  const int stripe_width = 3 * chunk_size;

  interval_set<uint64_t> ro;
  ro.insert(0, 2 * stripe_width);

  for (int s = 0; s < 3; ++s) {
    auto shard_iset = sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(s));
    ASSERT_EQ(1u, shard_iset.num_intervals()) << "shard=" << s;
    ASSERT_TRUE(shard_iset.contains(0, 2 * chunk_size)) << "shard=" << s;
  }
}

TEST(ECUtil, ro_intervals_to_shard_intervals_partial_spanning_shards)
{
  // RO [2048, 10240) spans part of shard0, all of shard1, part of shard2.
  // shard0: [2048, 4096) — len 2048
  // shard1: [0,    4096) — len 4096
  // shard2: [0,    2048) — len 2048
  stripe_info_t sinfo(3, 2, 3 * 4096, vector<shard_id_t>(0));
  const int chunk_size = 4096;

  interval_set<uint64_t> ro;
  ro.insert(2048, 8192); // [2048, 10240)

  auto s0 = sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(0));
  ASSERT_EQ(1u, s0.num_intervals());
  ASSERT_TRUE(s0.contains(2048, 2048));

  auto s1 = sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(1));
  ASSERT_EQ(1u, s1.num_intervals());
  ASSERT_TRUE(s1.contains(0, chunk_size));

  auto s2 = sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(2));
  ASSERT_EQ(1u, s2.num_intervals());
  ASSERT_TRUE(s2.contains(0, 2048));
}

TEST(ECUtil, ro_intervals_to_shard_intervals_cross_stripe_boundary)
{
  // RO [8192, 16384) = last chunk of stripe 0 (shard2) + first chunk of stripe1 (shard0).
  // shard1 has no bytes in this interval (stripe-0 chunk ends at 8192, stripe-1 starts at 16384).
  //
  // Detailed derivation:
  //   shard0: start=ro_offset_to_shard_offset(8192,0) = full=0, offset_shard=2, raw(0)<2 → 4096
  //           end  =ro_offset_to_shard_offset(16384,0)= full=4096, offset_shard=1, raw(0)<1 → 8192
  //           → [4096, 8192)
  //   shard1: start=ro_offset_to_shard_offset(8192,1) = full=0, offset_shard=2, raw(1)<2 → 4096
  //           end  =ro_offset_to_shard_offset(16384,1)= full=4096, offset_shard=1, raw(1)==1 → 4096+0=4096
  //           → empty (start == end)
  //   shard2: start=ro_offset_to_shard_offset(8192,2) = full=0, offset_shard=2, raw(2)==2 → 0+0=0
  //           end  =ro_offset_to_shard_offset(16384,2)= full=4096, offset_shard=1, raw(2)>1 → 4096
  //           → [0, 4096)
  stripe_info_t sinfo(3, 2, 3 * 4096, vector<shard_id_t>(0));
  const int chunk_size = 4096;

  interval_set<uint64_t> ro;
  ro.insert(2 * chunk_size, 2 * chunk_size); // [8192, 16384)

  auto s0 = sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(0));
  ASSERT_EQ(1u, s0.num_intervals());
  ASSERT_TRUE(s0.contains(chunk_size, chunk_size)); // [4096, 8192)

  auto s1 = sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(1));
  ASSERT_TRUE(s1.empty()); // shard 1 has no bytes in [8192, 16384)

  auto s2 = sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(2));
  ASSERT_EQ(1u, s2.num_intervals());
  ASSERT_TRUE(s2.contains(0, chunk_size)); // [0, 4096)
}

TEST(ECUtil, ro_intervals_to_shard_intervals_parity_shard)
{
  // Parity shards (index >= k) must always return an empty interval_set.
  stripe_info_t sinfo(3, 2, 3 * 4096, vector<shard_id_t>(0));
  const int chunk_size = 4096;

  interval_set<uint64_t> ro;
  ro.insert(0, 3 * chunk_size);

  // shards 3 and 4 are parity (raw_shards 3 and 4 >= k=3)
  ASSERT_TRUE(sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(3)).empty());
  ASSERT_TRUE(sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(4)).empty());
}

TEST(ECUtil, ro_intervals_to_shard_intervals_multiple_intervals)
{
  // Two disjoint RO intervals on shard 0:
  //   [0, 4096)     → shard0 [0, 4096)
  //   [12288, 16384) → shard0 [4096, 8192)
  // Together they should produce [0, 8192) for shard 0.
  stripe_info_t sinfo(3, 2, 3 * 4096, vector<shard_id_t>(0));
  const int chunk_size = 4096;
  const int stripe_width = 3 * chunk_size;

  interval_set<uint64_t> ro;
  ro.insert(0, chunk_size);
  ro.insert(stripe_width, chunk_size);

  auto s0 = sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(0));
  // Both project to contiguous shard-space → merged into one interval.
  ASSERT_EQ(1u, s0.num_intervals());
  ASSERT_TRUE(s0.contains(0, 2 * chunk_size));

  // shard 1 and 2 get nothing from either interval.
  ASSERT_TRUE(sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(1)).empty());
  ASSERT_TRUE(sinfo.ro_intervals_to_shard_intervals(ro, shard_id_t(2)).empty());
}

// ==========================================================================
// Tests for EC sparse-read / mapext helper functions
// ==========================================================================

// --------------------------------------------------------------------------
// Shared test helpers
// --------------------------------------------------------------------------

// Build a bufferlist of `len` bytes filled with `fill_byte`.
// ---------------------------------------------------------------------------
// XorErasureCode — a real k=2, m=1 XOR erasure codec for unit testing.
//
// Encode: parity[i] = data_0[i] XOR data_1[i]
// Decode: missing_data[i] = other_data[i] XOR parity[i]
//
// decode_chunks actually writes reconstructed data into the output bufferptr,
// unlike MockErasureCode which is a no-op.  Use this codec whenever a test
// needs to verify reconstructed data content.
// ---------------------------------------------------------------------------
class XorErasureCode : public ErasureCodeInterface {
public:
  // k=2 data shards, 1 parity shard (raw shards: 0=D0, 1=D1, 2=P).
  static constexpr int K = 2;
  static constexpr int M = 1;

  uint64_t get_supported_optimizations() const override {
    return FLAG_EC_PLUGIN_PARTIAL_READ_OPTIMIZATION |
           FLAG_EC_PLUGIN_PARTIAL_WRITE_OPTIMIZATION |
           FLAG_EC_PLUGIN_ZERO_INPUT_ZERO_OUTPUT_OPTIMIZATION |
           FLAG_EC_PLUGIN_ZERO_PADDING_OPTIMIZATION |
           FLAG_EC_PLUGIN_PARITY_DELTA_OPTIMIZATION;
  }

  int init(ErasureCodeProfile &, std::ostream *) override { return 0; }
  const ErasureCodeProfile &get_profile() const override { return _profile; }
  int create_rule(const std::string &, CrushWrapper &,
                  std::ostream *) const override { return 0; }

  unsigned int get_chunk_count() const override { return K + M; }
  unsigned int get_data_chunk_count() const override { return K; }
  unsigned int get_coding_chunk_count() const override { return M; }
  int get_sub_chunk_count() override { return 1; }
  unsigned int get_chunk_size(unsigned int stripe_width) const override {
    return stripe_width / K;
  }
  size_t get_minimum_granularity() override { return 0; }

  const std::vector<shard_id_t> &get_chunk_mapping() const override {
    return _chunk_mapping;
  }

  int minimum_to_decode(const shard_id_set &want_to_read,
                        const shard_id_set &available,
                        shard_id_set &minimum_set,
                        shard_id_map<std::vector<std::pair<int,int>>>
                            *minimum_sub_chunks) override {
    // If all wanted shards are available, return them directly.
    bool all_available = true;
    for (shard_id_t s : want_to_read) {
      if (!available.contains(s)) { all_available = false; break; }
    }
    if (all_available) {
      minimum_set = want_to_read;
    } else {
      // Need reconstruction: return any K available shards.
      minimum_set.clear();
      for (shard_id_t s : available) {
        minimum_set.insert(s);
        if ((int)minimum_set.size() == K) break;
      }
      if ((int)minimum_set.size() < K) return -EIO;
    }
    if (minimum_sub_chunks) {
      for (shard_id_t s : minimum_set) {
        (*minimum_sub_chunks)[s] = {{0, 1}};
      }
    }
    return 0;
  }

  int encode_chunks(const shard_id_map<bufferptr> &in,
                    shard_id_map<bufferptr> &out) override {
    // XOR D0 and D1 into parity.
    const bufferptr &d0 = in.at(shard_id_t(0));
    const bufferptr &d1 = in.at(shard_id_t(1));
    bufferptr &p        = out.at(shard_id_t(2));
    ceph_assert(d0.length() == d1.length());
    ceph_assert(d0.length() == p.length());
    const char *s0 = d0.c_str();
    const char *s1 = d1.c_str();
    char       *sp = p.c_str();
    for (unsigned i = 0; i < d0.length(); ++i) {
      sp[i] = s0[i] ^ s1[i];
    }
    return 0;
  }

  int decode_chunks(const shard_id_set &want_to_read,
                    shard_id_map<bufferptr> &in,
                    shard_id_map<bufferptr> &out) override {
    ceph_assert(in.size() == 2);
    ceph_assert(out.size() == 1);

    // Find which shard to reconstruct and the two source shards.
    shard_id_t missing = out.begin()->first;
    bufferptr &dst     = out.begin()->second;

    // Collect the two source shards (any two of D0, D1, P).
    auto it = in.begin();
    const bufferptr &src0 = it->second; ++it;
    const bufferptr &src1 = it->second;
    ceph_assert(src0.length() == src1.length());
    ceph_assert(src0.length() == dst.length());
    (void)missing; // XOR of any two shards recovers the third.

    const char *a = src0.c_str();
    const char *b = src1.c_str();
    char       *d = dst.c_str();
    for (unsigned i = 0; i < src0.length(); ++i) {
      d[i] = a[i] ^ b[i];
    }
    return 0;
  }

  // --------------- deprecated stubs ---------------
  [[deprecated]] int minimum_to_decode(const std::set<int>&, const std::set<int>&,
      std::map<int, std::vector<std::pair<int,int>>>*) override
      { ADD_FAILURE(); return 0; }
  [[deprecated]] int minimum_to_decode_with_cost(const std::set<int>&,
      const std::map<int,int>&, std::set<int>*) override
      { ADD_FAILURE(); return 0; }
  int minimum_to_decode_with_cost(const shard_id_set&, const shard_id_map<int>&,
      shard_id_set*) override { return 0; }
  [[deprecated]] int encode(const std::set<int>&, const bufferlist&,
      std::map<int,bufferlist>*) override { ADD_FAILURE(); return 0; }
  int encode(const shard_id_set&, const bufferlist&,
      shard_id_map<bufferlist>*) override { return 0; }
  [[deprecated]] int encode_chunks(const std::set<int>&,
      std::map<int,bufferlist>*) override { ADD_FAILURE(); return 0; }
  [[deprecated]] int decode(const std::set<int>&, const std::map<int,bufferlist>&,
      std::map<int,bufferlist>*, int) override { ADD_FAILURE(); return 0; }
  int decode(const shard_id_set&, const shard_id_map<bufferlist>&,
      shard_id_map<bufferlist>*, int) override { return 0; }
  [[deprecated]] int decode_chunks(const std::set<int>&,
      const std::map<int,bufferlist>&, std::map<int,bufferlist>*) override
      { ADD_FAILURE(); return 0; }
  [[deprecated]] int decode_concat(const std::set<int>&,
      const std::map<int,bufferlist>&, bufferlist*) override
      { ADD_FAILURE(); return 0; }
  [[deprecated]] int decode_concat(const std::map<int,bufferlist>&,
      bufferlist*) override { ADD_FAILURE(); return 0; }
  void encode_delta(const bufferptr&, const bufferptr&, bufferptr*) override {}
  void apply_delta(const shard_id_map<bufferptr>&,
                   shard_id_map<bufferptr>&) override {}

private:
  ErasureCodeProfile _profile;
  const std::vector<shard_id_t> _chunk_mapping = {};
};

// ---------------------------------------------------------------------------
// Test buffer helpers
// ---------------------------------------------------------------------------

static bufferlist make_buf(uint64_t len, char fill_byte = '\x00')
{
  bufferlist bl;
  bufferptr ptr = buffer::create(len);
  memset(ptr.c_str(), fill_byte, len);
  bl.append(ptr);
  return bl;
}

// Build a non-zero bufferlist of `len` bytes (fill 0xAB).
static bufferlist make_nonzero_buf(uint64_t len)
{
  return make_buf(len, '\xAB');
}

// XOR two same-length bufferlists byte-by-byte to produce the parity shard.
static bufferlist make_xor_parity(const bufferlist &d0, const bufferlist &d1)
{
  ceph_assert(d0.length() == d1.length());
  bufferlist result;
  bufferptr ptr = buffer::create(d0.length());
  // Flatten both into contiguous buffers for easy XOR.
  bufferlist tmp0 = d0, tmp1 = d1;
  tmp0.rebuild();
  tmp1.rebuild();
  const char *a = tmp0.c_str();
  const char *b = tmp1.c_str();
  char *out = ptr.c_str();
  for (unsigned i = 0; i < d0.length(); ++i) {
    out[i] = a[i] ^ b[i];
  }
  result.append(ptr);
  return result;
}

// Build a read_request_t covering both data shards over [0, stripe_width).
// Used as the request for XorDecode tests.
static ECCommon::read_request_t make_xor_stripe_request(
    const ECUtil::stripe_info_t &s)
{
  const uint64_t swidth = s.get_stripe_width();
  ECUtil::shard_extent_set_t want(s.get_k_plus_m());
  s.ro_range_to_shard_extent_set(0, swidth, want);
  return ECCommon::read_request_t(want, ECCommon::WantAttrs::No,
                                  ECCommon::WantOmapHeader::No,
                                  ECCommon::WantOmapKeys::No,
                                  "", 0, swidth);
}

// Standard k=2, m=1 stripe geometry: chunk_size=4K, stripe_width=8K.
static constexpr unsigned k2m1_k = 2;
static constexpr unsigned k2m1_m = 1;
static constexpr uint64_t k2m1_chunk = 4096;   // 4 KiB
static constexpr uint64_t k2m1_swidth = k2m1_k * k2m1_chunk; // 8 KiB

// --------------------------------------------------------------------------
// ec_sparse_clip_to_map
// --------------------------------------------------------------------------

TEST(ECSparseFuncs_ClipToMap, CL1_NoClipNeeded)
{
  // [0, 8K) clipped to [0, 8K) → unchanged
  interval_set<uint64_t> extents;
  extents.insert(0, 8192);
  auto out = ec_sparse_clip_to_map(extents, 0, 8192);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out.at(0), 8192u);
}

TEST(ECSparseFuncs_ClipToMap, CL2_BothEndsCLipped)
{
  // [0, 8K) clipped to [2K, 6K)
  interval_set<uint64_t> extents;
  extents.insert(0, 8192);
  auto out = ec_sparse_clip_to_map(extents, 2048, 6144);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out.at(2048), 4096u);
}

TEST(ECSparseFuncs_ClipToMap, CL3_StartClippedOnly)
{
  // [0, 4K) clipped to [2K, 8K) — end not reached
  interval_set<uint64_t> extents;
  extents.insert(0, 4096);
  auto out = ec_sparse_clip_to_map(extents, 2048, 8192);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out.at(2048), 2048u);
}

TEST(ECSparseFuncs_ClipToMap, CL4_EndClippedOnly)
{
  // [4K, 8K) clipped to [0, 6K)
  interval_set<uint64_t> extents;
  extents.insert(4096, 4096);
  auto out = ec_sparse_clip_to_map(extents, 0, 6144);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out.at(4096), 2048u);
}

TEST(ECSparseFuncs_ClipToMap, CL5_EntirelyBeforeOffset)
{
  // [0, 4K) vs offset=4K → empty
  interval_set<uint64_t> extents;
  extents.insert(0, 4096);
  auto out = ec_sparse_clip_to_map(extents, 4096, 8192);
  ASSERT_TRUE(out.empty());
}

TEST(ECSparseFuncs_ClipToMap, CL6_EntirelyAfterReqEnd)
{
  // [8K, 12K) vs req_end=8K → empty
  interval_set<uint64_t> extents;
  extents.insert(8192, 4096);
  auto out = ec_sparse_clip_to_map(extents, 0, 8192);
  ASSERT_TRUE(out.empty());
}

TEST(ECSparseFuncs_ClipToMap, CL7_TwoExtentsPartiallyClipped)
{
  // [0, 4K) and [8K, 12K) vs [2K, 10K)
  interval_set<uint64_t> extents;
  extents.insert(0, 4096);
  extents.insert(8192, 4096);
  auto out = ec_sparse_clip_to_map(extents, 2048, 10240);
  ASSERT_EQ(out.size(), 2u);
  ASSERT_EQ(out.at(2048), 2048u);
  ASSERT_EQ(out.at(8192), 2048u);
}

TEST(ECSparseFuncs_ClipToMap, CL8_EmptyInput)
{
  interval_set<uint64_t> extents;
  auto out = ec_sparse_clip_to_map(extents, 0, 8192);
  ASSERT_TRUE(out.empty());
}

// --------------------------------------------------------------------------
// prepare_sparse_read_request
// --------------------------------------------------------------------------

// Helper: set up a k=2, m=1 pipeline with a given acting set.
struct PrepareTestFixture {
  const uint64_t swidth = k2m1_swidth;
  ECUtil::stripe_info_t s{k2m1_k, k2m1_m, swidth, std::vector<shard_id_t>{}};
  ECListenerStub listener;
  MockErasureCode *ecode;
  ErasureCodeInterfaceRef ec_impl;
  std::unique_ptr<ECCommon::ReadPipeline> pipeline;

  explicit PrepareTestFixture(std::initializer_list<int> acting_shards)
    : ecode(new MockErasureCode(k2m1_k, k2m1_k + k2m1_m)),
      ec_impl(ecode)
  {
    listener.acting_shards.clear();
    for (int sh : acting_shards) {
      listener.acting_shards.insert(pg_shard_t(sh, shard_id_t(sh)));
    }
    pipeline = std::make_unique<ECCommon::ReadPipeline>(
        g_ceph_context, ec_impl, s, &listener);
  }
};

TEST(ECSparseFuncs_PrepareRequest, PR1_AllShardsHealthy_Mapext)
{
  // All K data shards available; for_mapext=true → drop_data=true, needs_reconstruct=false
  PrepareTestFixture fix{0, 1, 2};
  const uint64_t object_size = k2m1_swidth;
  std::list<ec_align_t> to_read = {{0, object_size, 0}};
  bool needs_reconstruct = true;
  int out_r = -1;
  auto req = prepare_sparse_read_request(hobject_t{}, to_read, object_size,
                                         /*for_mapext=*/true,
                                         *fix.pipeline, needs_reconstruct, out_r);
  ASSERT_TRUE(req.has_value());
  ASSERT_EQ(out_r, 0);
  ASSERT_FALSE(needs_reconstruct);
  ASSERT_TRUE(req->drop_data);
  ASSERT_TRUE(req->want_sparse_read);
}

TEST(ECSparseFuncs_PrepareRequest, PR2_MissingDataShard_Mapext)
{
  // Shard 1 unavailable; for_mapext=true → drop_data=false, needs_reconstruct=true
  PrepareTestFixture fix{0, 2};
  const uint64_t object_size = k2m1_swidth;
  std::list<ec_align_t> to_read = {{0, object_size, 0}};
  bool needs_reconstruct = false;
  int out_r = -1;
  auto req = prepare_sparse_read_request(hobject_t{}, to_read, object_size,
                                         /*for_mapext=*/true,
                                         *fix.pipeline, needs_reconstruct, out_r);
  ASSERT_TRUE(req.has_value());
  ASSERT_EQ(out_r, 0);
  ASSERT_TRUE(needs_reconstruct);
  ASSERT_FALSE(req->drop_data);
}

TEST(ECSparseFuncs_PrepareRequest, PR3_MissingDataShard_SparseRead)
{
  // Shard 1 unavailable; for_mapext=false → drop_data always false
  PrepareTestFixture fix{0, 2};
  const uint64_t object_size = k2m1_swidth;
  std::list<ec_align_t> to_read = {{0, object_size, 0}};
  bool needs_reconstruct = false;
  int out_r = -1;
  auto req = prepare_sparse_read_request(hobject_t{}, to_read, object_size,
                                         /*for_mapext=*/false,
                                         *fix.pipeline, needs_reconstruct, out_r);
  ASSERT_TRUE(req.has_value());
  ASSERT_EQ(out_r, 0);
  ASSERT_TRUE(needs_reconstruct);
  ASSERT_FALSE(req->drop_data);
}

TEST(ECSparseFuncs_PrepareRequest, PR4_TailStripeZerosForDecode)
{
  // Object is 1 byte into the second stripe; shard 1 falls in the zero mask
  // for the partial last stripe — should not trigger needs_reconstruct.
  PrepareTestFixture fix{0, 1, 2};
  const uint64_t object_size = k2m1_swidth + 1;
  std::list<ec_align_t> to_read = {{0, object_size, 0}};
  bool needs_reconstruct = true;
  int out_r = -1;
  auto req = prepare_sparse_read_request(hobject_t{}, to_read, object_size,
                                         /*for_mapext=*/true,
                                         *fix.pipeline, needs_reconstruct, out_r);
  ASSERT_TRUE(req.has_value());
  ASSERT_EQ(out_r, 0);
  ASSERT_FALSE(needs_reconstruct);
  ASSERT_TRUE(req->drop_data);
}

TEST(ECSparseFuncs_PrepareRequest, PR5_ParityShardMissing)
{
  // Parity shard (2) unavailable but both data shards present → no reconstruct
  PrepareTestFixture fix{0, 1};
  const uint64_t object_size = k2m1_swidth;
  std::list<ec_align_t> to_read = {{0, object_size, 0}};
  bool needs_reconstruct = true;
  int out_r = -1;
  auto req = prepare_sparse_read_request(hobject_t{}, to_read, object_size,
                                         /*for_mapext=*/true,
                                         *fix.pipeline, needs_reconstruct, out_r);
  ASSERT_TRUE(req.has_value());
  ASSERT_EQ(out_r, 0);
  ASSERT_FALSE(needs_reconstruct);
  ASSERT_TRUE(req->drop_data);
}

TEST(ECSparseFuncs_PrepareRequest, PR6_InsufficientShards)
{
  // Only one shard available in a k=2 pool → minimum_to_decode returns error
  PrepareTestFixture fix{0};
  const uint64_t object_size = k2m1_swidth;
  std::list<ec_align_t> to_read = {{0, object_size, 0}};
  bool needs_reconstruct = false;
  int out_r = 0;
  auto req = prepare_sparse_read_request(hobject_t{}, to_read, object_size,
                                         /*for_mapext=*/true,
                                         *fix.pipeline, needs_reconstruct, out_r);
  ASSERT_FALSE(req.has_value());
  ASSERT_LT(out_r, 0);
  ASSERT_FALSE(needs_reconstruct);
}

// --------------------------------------------------------------------------
// ec_sparse_decode
// --------------------------------------------------------------------------

TEST(ECSparseFuncs_Decode, DC1_MissingShardDecoded)
{
  // k=2, m=1; shard 1 missing; verify decode succeeds and shard 1 is populated.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECUtil::shard_extent_map_t sem(&s);
  // Populate shard 0 and shard 2 (parity) only; shard 1 is absent.
  sem.insert_in_shard(shard_id_t(0), 0, make_nonzero_buf(k2m1_chunk));
  sem.insert_in_shard(shard_id_t(2), 0, make_nonzero_buf(k2m1_chunk));

  ECUtil::shard_extent_set_t want(k2m1_k + k2m1_m);
  want[shard_id_t(0)].insert(0, k2m1_chunk);
  want[shard_id_t(1)].insert(0, k2m1_chunk);

  ECUtil::shard_extent_set_t zeros(k2m1_k + k2m1_m);

  shard_id_map<std::map<uint64_t, uint64_t>> sparse_extents(k2m1_k + k2m1_m);
  sparse_extents[shard_id_t(0)];  // shard 0 replied (has data)
  sparse_extents[shard_id_t(2)];  // shard 2 replied (has data)
  int r = ec_sparse_decode(sem, want, zeros, sparse_extents, ec_impl, k2m1_swidth, nullptr);
  ASSERT_EQ(r, 0);
  // After decode, shard 1 must be present.
  ASSERT_TRUE(sem.contains_shard(shard_id_t(1)));
}

TEST(ECSparseFuncs_Decode, DC2_TailStripePaddingFromZerosForDecode)
{
  // object_size = chunk_size + 1: shard 1 gets a zero-padding entry in
  // zeros_for_decode.  Verify decode succeeds without crashing.
  const uint64_t object_size = k2m1_chunk + 1;
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  ECListenerStub listener;
  listener.acting_shards.insert(pg_shard_t(0, shard_id_t(0)));
  listener.acting_shards.insert(pg_shard_t(1, shard_id_t(1)));
  listener.acting_shards.insert(pg_shard_t(2, shard_id_t(2)));
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listener);

  ECUtil::shard_extent_set_t want(k2m1_k + k2m1_m);
  want[shard_id_t(0)].insert(0, k2m1_chunk);
  want[shard_id_t(1)].insert(0, k2m1_chunk);

  ECCommon::read_request_t req(want, ECCommon::WantAttrs::No,
                               ECCommon::WantOmapHeader::No,
                               ECCommon::WantOmapKeys::No,
                               "", 0, object_size);
  ASSERT_EQ(0, pipeline.get_min_avail_to_read_shards(hobject_t{}, false, false, req));

  ECUtil::shard_extent_map_t sem(&s);
  for (auto &[shard, sr] : req.shard_reads) {
    for (auto [off, len] : sr.extents) {
      sem.insert_in_shard(shard, off, make_nonzero_buf(len));
    }
  }

  shard_id_map<std::map<uint64_t, uint64_t>> sparse_extents(k2m1_k + k2m1_m);
  for (auto &[shard, sr] : req.shard_reads) {
    sparse_extents[shard];  // mark every read shard as healthy
  }
  int r = ec_sparse_decode(sem, want, req.zeros_for_decode, sparse_extents,
                           ec_impl, object_size, nullptr);
  ASSERT_EQ(r, 0);
}

TEST(ECSparseFuncs_Decode, DC3_NoShardsMissing_NoOp)
{
  // All shards present — decode is a no-op; returns 0.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(0), 0, make_nonzero_buf(k2m1_chunk));
  sem.insert_in_shard(shard_id_t(1), 0, make_nonzero_buf(k2m1_chunk));

  ECUtil::shard_extent_set_t want(k2m1_k + k2m1_m);
  want[shard_id_t(0)].insert(0, k2m1_chunk);
  want[shard_id_t(1)].insert(0, k2m1_chunk);

  ECUtil::shard_extent_set_t zeros(k2m1_k + k2m1_m);

  shard_id_map<std::map<uint64_t, uint64_t>> sparse_extents(k2m1_k + k2m1_m);
  sparse_extents[shard_id_t(0)];  // both data shards replied
  sparse_extents[shard_id_t(1)];
  int r = ec_sparse_decode(sem, want, zeros, sparse_extents, ec_impl, k2m1_swidth, nullptr);
  ASSERT_EQ(r, 0);
}

// --------------------------------------------------------------------------
// ec_sparse_merge_ro_fiemap
// --------------------------------------------------------------------------

TEST(ECSparseFuncs_MergeRoFiemap, MH1_AllShardsPresent)
{
  // k=2, chunk=4K: shard 0 [0,4K), shard 1 [0,4K)
  // RO projection: shard 0 at RO [0,4K), shard 1 at RO [4K,8K)
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  shard_id_map<std::map<uint64_t, uint64_t>> extents(k2m1_k + k2m1_m);
  extents[shard_id_t(0)][0] = k2m1_chunk;
  extents[shard_id_t(1)][0] = k2m1_chunk;

  auto out = merge_shard_extent_maps(extents, s);
  // Both shards together cover [0, 8K) continuously.
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out.at(0), k2m1_swidth);
}

TEST(ECSparseFuncs_MergeRoFiemap, MH2_OneMissingDataShard)
{
  // Shard 1 absent (missing) — only shard 0 [0,4K) contributes.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  shard_id_map<std::map<uint64_t, uint64_t>> extents(k2m1_k + k2m1_m);
  extents[shard_id_t(0)][0] = k2m1_chunk;
  // shard 1 intentionally absent

  auto out = merge_shard_extent_maps(extents, s);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out.at(0), k2m1_chunk);  // only shard 0's RO range [0,4K)
}

TEST(ECSparseFuncs_MergeRoFiemap, MH3_EmptyExtents)
{
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  shard_id_map<std::map<uint64_t, uint64_t>> extents(k2m1_k + k2m1_m);
  auto out = merge_shard_extent_maps(extents, s);
  ASSERT_TRUE(out.empty());
}

TEST(ECSparseFuncs_MergeRoFiemap, MH4_ParityShardIgnored)
{
  // Parity shard (2) present in extents — must be ignored.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  shard_id_map<std::map<uint64_t, uint64_t>> extents(k2m1_k + k2m1_m);
  extents[shard_id_t(0)][0] = k2m1_chunk;
  extents[shard_id_t(2)][0] = k2m1_chunk;  // parity — must not appear in output

  auto out = merge_shard_extent_maps(extents, s);
  // Only shard 0 contributes RO [0,4K).
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out.at(0), k2m1_chunk);
}

TEST(ECSparseFuncs_MergeRoFiemap, MH5_SmallChunkProjection)
{
  // chunk_size=2K, k=2: shard 0 maps [0,2K)→RO[0,2K), shard 1 [0,2K)→RO[2K,4K)
  const uint64_t chunk = 2048;
  ECUtil::stripe_info_t s(2u, 1u, 2 * chunk, std::vector<shard_id_t>{});
  shard_id_map<std::map<uint64_t, uint64_t>> extents(3);
  extents[shard_id_t(0)][0] = chunk;
  extents[shard_id_t(1)][0] = chunk;

  auto out = merge_shard_extent_maps(extents, s);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out.at(0), 2 * chunk);
}

// --------------------------------------------------------------------------
// ec_sparse_scan_shard_extents
// --------------------------------------------------------------------------
//
// Helper: project an RO-space interval_set to shard space for the given shard.
static interval_set<uint64_t> shard_window(
    const ECUtil::stripe_info_t &s,
    shard_id_t shard,
    uint64_t ro_start,
    uint64_t ro_end)
{
  interval_set<uint64_t> ro_win;
  if (ro_end > ro_start) {
    ro_win.insert(ro_start, ro_end - ro_start);
  }
  return s.ro_intervals_to_shard_intervals(ro_win, shard);
}

TEST(ECSparseFuncs_ScanShardExtents, SC1_AllZeroNoFAE)
{
  // k=2, chunk=4K: shard 0 holds shard-space [0,4K); all-zero; no FAE → empty output.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(0), 0, make_buf(k2m1_chunk, '\x00'));

  interval_set<uint64_t> fae;
  // RO scan window [0, 4K) → shard 0 shard-space [0, 4K).
  auto win  = shard_window(s, shard_id_t(0), 0, FAE_BLOCK_SIZE);
  auto out  = ec_sparse_scan_shard_extents(sem, shard_id_t(0), fae, win, nullptr);
  ASSERT_TRUE(out.empty());
}

TEST(ECSparseFuncs_ScanShardExtents, SC2_AllZeroWithFAE)
{
  // Shard 1, shard-space [0,4K); all-zero; shard-space FAE covers [0,4K) → allocated.
  // RO FAE [4K,8K) → shard 1 shard-space FAE [0,4K).
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(1), 0, make_buf(k2m1_chunk, '\x00'));

  interval_set<uint64_t> ro_fae;
  ro_fae.insert(FAE_BLOCK_SIZE, FAE_BLOCK_SIZE);
  interval_set<uint64_t> shard_fae =
      s.ro_intervals_to_shard_intervals(ro_fae, shard_id_t(1));
  // RO scan window [4K,8K) → shard 1 shard-space [0,4K).
  auto win = shard_window(s, shard_id_t(1), FAE_BLOCK_SIZE, 2 * FAE_BLOCK_SIZE);

  auto out = ec_sparse_scan_shard_extents(sem, shard_id_t(1), shard_fae, win, nullptr);
  ASSERT_FALSE(out.empty());
  // Shard-space result: [0,4K) allocated.
  ASSERT_EQ(out.count(0), 1u);
  ASSERT_EQ(out.at(0), k2m1_chunk);
}

TEST(ECSparseFuncs_ScanShardExtents, SC3_NonZeroBlock)
{
  // Shard 0, shard-space [0,4K); non-zero → allocated.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(0), 0, make_nonzero_buf(k2m1_chunk));

  interval_set<uint64_t> fae;
  auto win = shard_window(s, shard_id_t(0), 0, FAE_BLOCK_SIZE);

  auto out = ec_sparse_scan_shard_extents(sem, shard_id_t(0), fae, win, nullptr);
  ASSERT_FALSE(out.empty());
  // Shard-space result: [0,4K) allocated.
  ASSERT_EQ(out.count(0), 1u);
  ASSERT_EQ(out.at(0), k2m1_chunk);
}

TEST(ECSparseFuncs_ScanShardExtents, SC5_SmallChunk_NonZeroShard)
{
  // chunk=2K, k=2: scan_stride=2K.  Shard 0 shard-space [0,2K) is non-zero.
  // The RO window covers [0,4K); shard 0's projection is [0,2K).
  const uint64_t chunk = 2048;
  ECUtil::stripe_info_t s(2u, 1u, 2 * chunk, std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(0), 0, make_nonzero_buf(chunk));

  interval_set<uint64_t> fae;
  // RO window [0,4K) → shard 0 shard-space [0,2K).
  auto win = shard_window(s, shard_id_t(0), 0, FAE_BLOCK_SIZE);

  auto out = ec_sparse_scan_shard_extents(sem, shard_id_t(0), fae, win, nullptr);
  ASSERT_FALSE(out.empty());
  // Shard-space: [0,2K) allocated.
  ASSERT_EQ(out.count(0), 1u);
  ASSERT_EQ(out.at(0), chunk);
}

TEST(ECSparseFuncs_ScanShardExtents, SC6_SmallChunk_BothShardsZeroNoFAE)
{
  // chunk=2K: shard 0 and shard 1 both all-zero; no FAE → both empty.
  const uint64_t chunk = 2048;
  ECUtil::stripe_info_t s(2u, 1u, 2 * chunk, std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(0), 0, make_buf(chunk, '\x00'));
  sem.insert_in_shard(shard_id_t(1), 0, make_buf(chunk, '\x00'));

  interval_set<uint64_t> fae;
  auto win0 = shard_window(s, shard_id_t(0), 0, FAE_BLOCK_SIZE);
  auto win1 = shard_window(s, shard_id_t(1), 0, FAE_BLOCK_SIZE);

  auto out0 = ec_sparse_scan_shard_extents(sem, shard_id_t(0), fae, win0, nullptr);
  auto out1 = ec_sparse_scan_shard_extents(sem, shard_id_t(1), fae, win1, nullptr);
  ASSERT_TRUE(out0.empty());
  ASSERT_TRUE(out1.empty());
}

TEST(ECSparseFuncs_ScanShardExtents, SC7_LargeChunk_NonZeroBlock)
{
  // chunk=64K, k=2: scan_stride=FAE_BLOCK_SIZE=4K.
  // Shard 0 has a full 64K chunk of non-zero data.
  // RO window covers just [0,4K) → shard 0 shard-space window [0,4K).
  // Expect: shard-space [0,4K) allocated.
  const uint64_t chunk = 64 * 1024;
  ECUtil::stripe_info_t s(2u, 1u, 2 * chunk, std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(0), 0, make_nonzero_buf(chunk));

  interval_set<uint64_t> fae;
  auto win = shard_window(s, shard_id_t(0), 0, FAE_BLOCK_SIZE);

  auto out = ec_sparse_scan_shard_extents(sem, shard_id_t(0), fae, win, nullptr);
  ASSERT_FALSE(out.empty());
  // Only the windowed sub-block [0,4K) is scanned and allocated.
  ASSERT_EQ(out.count(0), 1u);
  ASSERT_EQ(out.at(0), FAE_BLOCK_SIZE);
}

TEST(ECSparseFuncs_ScanShardExtents, SC8_PartialLastBlock_NonZero)
{
  // k=2, chunk=4K; shard 1 has 2K of data at shard-space [0,2K); non-zero.
  // RO scan window [4K,6K) → shard 1 shard-space [0,2K).
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(1), 0, make_nonzero_buf(2048));

  interval_set<uint64_t> fae;
  auto win = shard_window(s, shard_id_t(1), FAE_BLOCK_SIZE, 6 * 1024);

  auto out = ec_sparse_scan_shard_extents(sem, shard_id_t(1), fae, win, nullptr);
  ASSERT_FALSE(out.empty());
  // Shard-space: [0,2K) allocated.
  ASSERT_EQ(out.count(0), 1u);
  ASSERT_EQ(out.at(0), 2048u);
}

TEST(ECSparseFuncs_ScanShardExtents, SC9_PartialLastBlock_AllZeroNoFAE)
{
  // k=2, chunk=4K; shard 1 has 2K of zeros; no FAE → empty.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(1), 0, make_buf(2048, '\x00'));

  interval_set<uint64_t> fae;
  auto win = shard_window(s, shard_id_t(1), FAE_BLOCK_SIZE, 6 * 1024);

  auto out = ec_sparse_scan_shard_extents(sem, shard_id_t(1), fae, win, nullptr);
  ASSERT_TRUE(out.empty());
}

// --------------------------------------------------------------------------
// ec_sparse_finish_read (whole-path)
// --------------------------------------------------------------------------

// Helper: build a minimal read_request_t for ec_sparse_finish_read tests.
static ECCommon::read_request_t make_finish_read_req(
    const ECUtil::stripe_info_t &s,
    uint64_t offset,
    uint64_t length,
    uint64_t object_size)
{
  ECUtil::shard_extent_set_t want(s.get_k_plus_m());
  s.ro_range_to_shard_extent_set(offset, length, want);
  ECCommon::read_request_t req(want, ECCommon::WantAttrs::No,
                               ECCommon::WantOmapHeader::No,
                               ECCommon::WantOmapKeys::No,
                               "", 0, object_size);
  return req;
}

TEST(ECSparseFuncs_FinishRead, WP1_AllShardsHealthy_NoReconstruct)
{
  // Case A: both shards healthy; fiemaps cover the full object.
  // Expects out_map = full extent, decode not called.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;
  res.sparse_extents_read[shard_id_t(1)][0] = k2m1_chunk;

  auto req = make_finish_read_req(s, 0, k2m1_swidth, k2m1_swidth);
  std::map<uint64_t, uint64_t> out_map;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_swidth,
                                /*needs_reconstruct=*/false,
                                ec_impl, fae, out_map, nullptr, nullptr);
  ASSERT_EQ(r, 0);
  ASSERT_EQ(out_map.size(), 1u);
  ASSERT_EQ(out_map.at(0), k2m1_swidth);
}

TEST(ECSparseFuncs_FinishRead, WP2_AllShardsHealthy_EmptyFiemap)
{
  // Case A: healthy shards but fiemaps are empty (object has no allocated extents).
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECCommon::read_result_t res(&s);
  // sparse_extents_read intentionally empty

  auto req = make_finish_read_req(s, 0, k2m1_swidth, k2m1_swidth);
  std::map<uint64_t, uint64_t> out_map;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_swidth,
                                /*needs_reconstruct=*/false,
                                ec_impl, fae, out_map, nullptr, nullptr);
  ASSERT_EQ(r, 0);
  ASSERT_TRUE(out_map.empty());
}

TEST(ECSparseFuncs_FinishRead, WP3_ReconstructShard_SparseHole)
{
  // Case B: shard 1 missing; its RO range [4K,8K) is a sparse hole (decoded zeros,
  // not in FAE) — must be absent from out_map.
  //
  // Must use XorErasureCode (not MockErasureCode) because decode() calls
  // pad_on_shards() which allocates an uninitialised buffer for the missing
  // shard before handing it to decode_chunks().  MockErasureCode is a no-op
  // and leaves that garbage in place; the zero scan then wrongly marks shard 1
  // as allocated.  XorErasureCode XORs shard0 (0xAB) with parity (0xAB) to
  // produce shard1 = 0x00, which is the sparse-hole scenario under test.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  XorErasureCode *ecode = new XorErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECCommon::read_result_t res(&s);
  // Shard 0 healthy: allocated at [0,4K); shard 1 missing (absent from sparse_extents_read).
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;
  // D0 = 0xAB, P = D0 XOR D1 = 0xAB XOR 0x00 = 0xAB.
  // decode will reconstruct D1 = D0 XOR P = 0xAB XOR 0xAB = 0x00.
  res.buffers_read.insert_in_shard(shard_id_t(0), 0, make_nonzero_buf(k2m1_chunk));
  res.buffers_read.insert_in_shard(shard_id_t(2), 0, make_nonzero_buf(k2m1_chunk));

  ECUtil::shard_extent_set_t want(k2m1_k + k2m1_m);
  want[shard_id_t(0)].insert(0, k2m1_chunk);
  want[shard_id_t(1)].insert(0, k2m1_chunk);
  ECCommon::read_request_t req(want, ECCommon::WantAttrs::No,
                               ECCommon::WantOmapHeader::No,
                               ECCommon::WantOmapKeys::No,
                               "", 0, k2m1_swidth);

  std::map<uint64_t, uint64_t> out_map;
  interval_set<uint64_t> fae;  // empty — zero block is a hole

  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_swidth,
                                /*needs_reconstruct=*/true,
                                ec_impl, fae, out_map, nullptr, nullptr);
  ASSERT_EQ(r, 0);
  // [0,4K) from shard 0 is non-zero → allocated.
  // [4K,8K) from shard 1 decoded as zero, not in FAE → absent.
  // The entry at 0 must end at exactly 4K (not coalesced with the absent block).
  ASSERT_EQ(out_map.count(0), 1u);
  ASSERT_EQ(out_map.at(0), k2m1_chunk);
  ASSERT_EQ(out_map.count(k2m1_chunk), 0u);
}

TEST(ECSparseFuncs_FinishRead, WP4_ReconstructShard_ZeroBlockInFAE)
{
  // Case B: shard 1 missing; its RO range [4K,8K) is zero but in FAE → allocated.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;
  res.buffers_read.insert_in_shard(shard_id_t(0), 0, make_nonzero_buf(k2m1_chunk));
  res.buffers_read.insert_in_shard(shard_id_t(2), 0, make_nonzero_buf(k2m1_chunk));

  ECUtil::shard_extent_set_t want(k2m1_k + k2m1_m);
  want[shard_id_t(0)].insert(0, k2m1_chunk);
  want[shard_id_t(1)].insert(0, k2m1_chunk);
  ECCommon::read_request_t req(want, ECCommon::WantAttrs::No,
                               ECCommon::WantOmapHeader::No,
                               ECCommon::WantOmapKeys::No,
                               "", 0, k2m1_swidth);

  std::map<uint64_t, uint64_t> out_map;
  interval_set<uint64_t> fae;
  fae.insert(k2m1_chunk, k2m1_chunk);  // [4K,8K) force-allocated

  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_swidth,
                                /*needs_reconstruct=*/true,
                                ec_impl, fae, out_map, nullptr, nullptr);
  ASSERT_EQ(r, 0);
  // [4K,8K) is zero but in FAE → must be covered by out_map.
  // Adjacent allocated blocks coalesce into one interval_set entry, so check
  // that some out_map entry spans into [4K,8K) rather than requiring a
  // separate key at exactly 4K.
  {
    std::ostringstream oss;
    oss << "out_map={";
    for (auto &[k, v] : out_map) oss << k << ":" << v << " ";
    oss << "}";
    bool covered = false;
    for (auto &[k, v] : out_map) {
      if (k <= k2m1_chunk && k + v > k2m1_chunk) { covered = true; break; }
    }
    ASSERT_TRUE(covered) << oss.str();
  }
}

TEST(ECSparseFuncs_FinishRead, WP5_ReconstructShard_NonZeroData)
{
  // Case B: shard 1 missing but decoded as non-zero → allocated.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;
  // Populate both shards (shard 1 present in buffers_read to simulate a
  // pre-decoded scenario where the caller already has the data).
  res.buffers_read.insert_in_shard(shard_id_t(0), 0, make_nonzero_buf(k2m1_chunk));
  res.buffers_read.insert_in_shard(shard_id_t(1), 0, make_nonzero_buf(k2m1_chunk));

  ECUtil::shard_extent_set_t want(k2m1_k + k2m1_m);
  want[shard_id_t(0)].insert(0, k2m1_chunk);
  want[shard_id_t(1)].insert(0, k2m1_chunk);
  ECCommon::read_request_t req(want, ECCommon::WantAttrs::No,
                               ECCommon::WantOmapHeader::No,
                               ECCommon::WantOmapKeys::No,
                               "", 0, k2m1_swidth);

  std::map<uint64_t, uint64_t> out_map;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_swidth,
                                /*needs_reconstruct=*/true,
                                ec_impl, fae, out_map, nullptr, nullptr);
  ASSERT_EQ(r, 0);
  // Both shards have non-zero data → both RO ranges must be covered.
  // Adjacent blocks coalesce, so check coverage rather than exact keys.
  {
    std::ostringstream oss;
    oss << "out_map={";
    for (auto &[k, v] : out_map) oss << k << ":" << v << " ";
    oss << "}";
    // [0,4K) covered
    bool covered0 = false;
    for (auto &[k, v] : out_map) {
      if (k == 0 && v >= k2m1_chunk) { covered0 = true; break; }
    }
    ASSERT_TRUE(covered0) << oss.str();
    // [4K,8K) covered
    bool covered1 = false;
    for (auto &[k, v] : out_map) {
      if (k <= k2m1_chunk && k + v >= k2m1_swidth) { covered1 = true; break; }
    }
    ASSERT_TRUE(covered1) << oss.str();
  }
}

TEST(ECSparseFuncs_FinishRead, WP6_NonAlignedOffsetClipped)
{
  // Case A: request starts at offset=2K (not 4K-aligned); out_map must be clipped.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;

  auto req = make_finish_read_req(s, 2048, k2m1_chunk, k2m1_swidth);
  std::map<uint64_t, uint64_t> out_map;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 2048, k2m1_chunk,
                                /*needs_reconstruct=*/false,
                                ec_impl, fae, out_map, nullptr, nullptr);
  ASSERT_EQ(r, 0);
  // Shard 0 covers RO [0,4K); clipped to [2K,4K+2K)=[2K,6K) but capped at 4K.
  ASSERT_FALSE(out_map.empty());
  // Start must be >= offset=2K.
  ASSERT_GE(out_map.begin()->first, 2048u);
}

TEST(ECSparseFuncs_FinishRead, WP7_EndMidBlock_Clipped)
{
  // Case A: request ends at offset+length=6K (mid-4K-block); out_map trimmed.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;
  res.sparse_extents_read[shard_id_t(1)][0] = k2m1_chunk;

  auto req = make_finish_read_req(s, 0, 6144, k2m1_swidth);
  std::map<uint64_t, uint64_t> out_map;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 0, 6144,
                                /*needs_reconstruct=*/false,
                                ec_impl, fae, out_map, nullptr, nullptr);
  ASSERT_EQ(r, 0);
  // Total allocated bytes must not exceed the requested length.
  uint64_t total = 0;
  for (auto &[off, len] : out_map) {
    ASSERT_LT(off, 6144u);
    ASSERT_LE(off + len, 6144u);
    total += len;
  }
  ASSERT_LE(total, 6144u);
}

TEST(ECSparseFuncs_FinishRead, WP10_SparseReadDataBytes)
{
  // Case A: out_bl must be populated with correct bytes for each extent in out_map.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;
  // Insert distinct non-zero data into shard 0 so we can verify the bytes.
  bufferlist shard0_data = make_nonzero_buf(k2m1_chunk);
  res.buffers_read.insert_in_shard(shard_id_t(0), 0, shard0_data);

  auto req = make_finish_read_req(s, 0, k2m1_chunk, k2m1_swidth);
  std::map<uint64_t, uint64_t> out_map;
  bufferlist out_bl;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_chunk,
                                /*needs_reconstruct=*/false,
                                ec_impl, fae, out_map, &out_bl, nullptr);
  ASSERT_EQ(r, 0);
  ASSERT_FALSE(out_map.empty());
  // out_bl length must equal the sum of all extents in out_map.
  uint64_t total_len = 0;
  for (auto &[off, len] : out_map) {
    total_len += len;
  }
  ASSERT_EQ(out_bl.length(), total_len);
}

TEST(ECSparseFuncs_FinishRead, WP11_MapextNullOutBl)
{
  // Mapext path: out_bl = nullptr; out_map still populated correctly.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;
  res.sparse_extents_read[shard_id_t(1)][0] = k2m1_chunk;

  auto req = make_finish_read_req(s, 0, k2m1_swidth, k2m1_swidth);
  std::map<uint64_t, uint64_t> out_map;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_swidth,
                                /*needs_reconstruct=*/false,
                                ec_impl, fae, out_map,
                                /*out_bl=*/nullptr, nullptr);
  ASSERT_EQ(r, 0);
  ASSERT_FALSE(out_map.empty());
}

// --------------------------------------------------------------------------
// ec_sparse_clip_to_map — additional edge cases
// --------------------------------------------------------------------------

TEST(ECSparseFuncs_ClipToMap, CL9_StdMapInput)
{
  // Template overload: std::map input instead of interval_set — no conversion.
  std::map<uint64_t, uint64_t> extents;
  extents[0] = 4096;
  extents[8192] = 4096;
  auto out = ec_sparse_clip_to_map(extents, 0, 16384);
  ASSERT_EQ(out.size(), 2u);
  ASSERT_EQ(out.at(0), 4096u);
  ASSERT_EQ(out.at(8192), 4096u);
}

TEST(ECSparseFuncs_ClipToMap, CL10_ZeroLengthWindow)
{
  // offset == req_end → empty output regardless of extents.
  interval_set<uint64_t> extents;
  extents.insert(0, 4096);
  auto out = ec_sparse_clip_to_map(extents, 4096, 4096);
  ASSERT_TRUE(out.empty());
}

TEST(ECSparseFuncs_ClipToMap, CL11_ExtentEndsExactlyAtOffset)
{
  // [0, 4K) with offset=4K: extent end == offset → excluded (half-open [offset, req_end)).
  interval_set<uint64_t> extents;
  extents.insert(0, 4096);
  auto out = ec_sparse_clip_to_map(extents, 4096, 8192);
  ASSERT_TRUE(out.empty());
}

TEST(ECSparseFuncs_ClipToMap, CL12_ExtentStartsExactlyAtReqEnd)
{
  // [8K, 12K) with req_end=8K: extent start == req_end → excluded.
  interval_set<uint64_t> extents;
  extents.insert(8192, 4096);
  auto out = ec_sparse_clip_to_map(extents, 0, 8192);
  ASSERT_TRUE(out.empty());
}

TEST(ECSparseFuncs_ClipToMap, CL13_ManyExtentsOnlyMiddleInRange)
{
  // Three extents; only the middle one falls inside [4K, 8K).
  interval_set<uint64_t> extents;
  extents.insert(0, 4096);      // entirely before offset
  extents.insert(4096, 4096);   // entirely inside
  extents.insert(8192, 4096);   // entirely after req_end
  auto out = ec_sparse_clip_to_map(extents, 4096, 8192);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out.at(4096), 4096u);
}

// --------------------------------------------------------------------------
// merge_shard_extent_maps — additional edge cases
// --------------------------------------------------------------------------

TEST(ECSparseFuncs_MergeRoFiemap, MH6_MultiStripeObject)
{
  // k=2, chunk=4K: two stripes → shard 0 covers [0,4K) and [8K,12K) in shard
  // space, projecting to RO [0,4K) and [8K,12K); shard 1 similarly for the
  // interleaved stripes.  All four intervals must appear and coalesce to [0,16K).
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  shard_id_map<std::map<uint64_t, uint64_t>> extents(k2m1_k + k2m1_m);
  // Each shard has two chunk-sized extents at offsets 0 and 4K (two stripes).
  extents[shard_id_t(0)][0]            = k2m1_chunk;
  extents[shard_id_t(0)][k2m1_chunk]   = k2m1_chunk;
  extents[shard_id_t(1)][0]            = k2m1_chunk;
  extents[shard_id_t(1)][k2m1_chunk]   = k2m1_chunk;

  auto out = merge_shard_extent_maps(extents, s);
  // Two full stripes = 16K of contiguous RO space.
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out.at(0), 2 * k2m1_swidth);
}

TEST(ECSparseFuncs_MergeRoFiemap, MH7_SparseObject_NonContiguous)
{
  // k=2, chunk=4K: shard 0 has data only in the second stripe (offset=4K in
  // shard space → RO offset=8K); shard 1 absent entirely.
  // Result must be a single interval [8K, 12K), not touching [0, 8K).
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  shard_id_map<std::map<uint64_t, uint64_t>> extents(k2m1_k + k2m1_m);
  extents[shard_id_t(0)][k2m1_chunk] = k2m1_chunk;  // shard 0, second stripe

  auto out = merge_shard_extent_maps(extents, s);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out.at(k2m1_swidth), k2m1_chunk);
  ASSERT_EQ(out.count(0), 0u);
}

TEST(ECSparseFuncs_MergeRoFiemap, MH8_OnlyParityShard)
{
  // Only parity shard (2) in extents — output must be empty.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  shard_id_map<std::map<uint64_t, uint64_t>> extents(k2m1_k + k2m1_m);
  extents[shard_id_t(2)][0] = k2m1_chunk;
  auto out = merge_shard_extent_maps(extents, s);
  ASSERT_TRUE(out.empty());
}

// --------------------------------------------------------------------------
// ec_sparse_decode — additional edge cases
// --------------------------------------------------------------------------

TEST(ECSparseFuncs_Decode, DC4_ZerosForDecodePopulated)
{
  // zeros_for_decode carries a zero-padded range; verify decode doesn't crash
  // and the shard ends up present.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(0), 0, make_nonzero_buf(k2m1_chunk));
  sem.insert_in_shard(shard_id_t(2), 0, make_nonzero_buf(k2m1_chunk));

  ECUtil::shard_extent_set_t want(k2m1_k + k2m1_m);
  want[shard_id_t(0)].insert(0, k2m1_chunk);
  want[shard_id_t(1)].insert(0, k2m1_chunk);

  // Simulate a zero-padding request for shard 1 (tail-stripe case).
  ECUtil::shard_extent_set_t zeros(k2m1_k + k2m1_m);
  zeros[shard_id_t(1)].insert(0, k2m1_chunk);

  shard_id_map<std::map<uint64_t, uint64_t>> sparse_extents(k2m1_k + k2m1_m);
  sparse_extents[shard_id_t(0)];  // shard 0 replied (has data)
  sparse_extents[shard_id_t(2)];  // shard 2 replied (has data)
  int r = ec_sparse_decode(sem, want, zeros, sparse_extents, ec_impl, k2m1_swidth, nullptr);
  ASSERT_EQ(r, 0);
  ASSERT_TRUE(sem.contains_shard(shard_id_t(1)));
}

TEST(ECSparseFuncs_Decode, DC5_AllShardsPresentNoDecodeNeeded)
{
  // Both data shards and parity all present — decode is a no-op; existing
  // buffers must be unmodified.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECUtil::shard_extent_map_t sem(&s);
  bufferlist orig0 = make_nonzero_buf(k2m1_chunk);
  bufferlist orig1 = make_buf(k2m1_chunk, '\xCD');
  sem.insert_in_shard(shard_id_t(0), 0, orig0);
  sem.insert_in_shard(shard_id_t(1), 0, orig1);

  ECUtil::shard_extent_set_t want(k2m1_k + k2m1_m);
  want[shard_id_t(0)].insert(0, k2m1_chunk);
  want[shard_id_t(1)].insert(0, k2m1_chunk);
  ECUtil::shard_extent_set_t zeros(k2m1_k + k2m1_m);

  shard_id_map<std::map<uint64_t, uint64_t>> sparse_extents(k2m1_k + k2m1_m);
  sparse_extents[shard_id_t(0)];  // both data shards replied
  sparse_extents[shard_id_t(1)];
  int r = ec_sparse_decode(sem, want, zeros, sparse_extents, ec_impl, k2m1_swidth, nullptr);
  ASSERT_EQ(r, 0);
  // Shard 0 must still be present with original data.
  ASSERT_TRUE(sem.contains_shard(shard_id_t(0)));
  ASSERT_TRUE(sem.contains_shard(shard_id_t(1)));
}

TEST(ECSparseFuncs_Decode, DC6_EmptyFiemapShardUsedForReconstruction)
{
  // k=2, m=1.  Shard 1 is missing (no reply at all).  Shard 0 replied with
  // an empty fiemap — its content in the read range is all-zero.  Parity
  // shard 2 has actual data.  With sparse_extents_read entries for shards 0
  // and 2, shard 0 must be zero-padded and combined with the parity shard to
  // reconstruct shard 1.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECUtil::shard_extent_map_t sem(&s);
  // Shard 0: empty fiemap — not in sem at all.
  // Shard 2 (parity): has data.
  sem.insert_in_shard(shard_id_t(2), 0, make_nonzero_buf(k2m1_chunk));

  ECUtil::shard_extent_set_t want(k2m1_k + k2m1_m);
  want[shard_id_t(0)].insert(0, k2m1_chunk);
  want[shard_id_t(1)].insert(0, k2m1_chunk);

  ECUtil::shard_extent_set_t zeros(k2m1_k + k2m1_m);

  // Shard 0 replied with empty fiemap; shard 2 replied with data; shard 1 is absent.
  shard_id_map<std::map<uint64_t, uint64_t>> sparse_extents(k2m1_k + k2m1_m);
  sparse_extents[shard_id_t(0)];  // empty fiemap
  sparse_extents[shard_id_t(2)];  // has data

  int r = ec_sparse_decode(sem, want, zeros, sparse_extents, ec_impl, k2m1_swidth, nullptr);
  ASSERT_EQ(r, 0);
  // Shard 1 must have been reconstructed.
  ASSERT_TRUE(sem.contains_shard(shard_id_t(1)));
  // Shard 0 must have been zero-padded into sem.
  ASSERT_TRUE(sem.contains_shard(shard_id_t(0)));
}

TEST(ECSparseFuncs_Decode, DC7_AllShardsEmptyFiemap)
{
  // k=2, m=1.  Shard 1 is missing.  All other shards (0 and parity 2)
  // replied with empty fiemaps — the entire range is a sparse hole.
  // decode() must succeed, reconstructing shard 1 as all-zeros.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  // buffers_read is completely empty (all shards returned empty fiemaps).
  ECUtil::shard_extent_map_t sem(&s);

  ECUtil::shard_extent_set_t want(k2m1_k + k2m1_m);
  want[shard_id_t(0)].insert(0, k2m1_chunk);
  want[shard_id_t(1)].insert(0, k2m1_chunk);

  ECUtil::shard_extent_set_t zeros(k2m1_k + k2m1_m);

  // Shards 0 and 2 replied with empty fiemaps; shard 1 is truly absent.
  shard_id_map<std::map<uint64_t, uint64_t>> sparse_extents(k2m1_k + k2m1_m);
  sparse_extents[shard_id_t(0)];  // empty fiemap
  sparse_extents[shard_id_t(2)];  // empty fiemap

  int r = ec_sparse_decode(sem, want, zeros, sparse_extents, ec_impl, k2m1_swidth, nullptr);
  ASSERT_EQ(r, 0);
  // Shard 1 must have been reconstructed (as all-zeros).
  ASSERT_TRUE(sem.contains_shard(shard_id_t(1)));
}

TEST(ECSparseFuncs_Decode, DC8_UnevenShardAllocationDecodeSucceeds)
{
  // Regression test for the double-erasure failure seen in production.
  //
  // k=2, m=1.  Shard 0 is missing and must be reconstructed.  Shard 1 has
  // data only in [0, chunk) — the second chunk is a sparse hole.  Parity
  // shard 2 has data across the full [0, 2*chunk) range.
  //
  // shard_want_to_read is uniform: both shard 0 and shard 1 are requested
  // over [0, 2*chunk), as prepare_sparse_read_request now produces when
  // reconstruction is needed.
  //
  // Without the fix, shard 1 would be zero-padded only to its own per-shard
  // want [0, chunk), dropping out of the slice iterator at the second stripe.
  // That leaves both shard 0 (the target) and shard 1 (dropped) as erasures,
  // giving nerrs=2=m and causing isa_decode to return -1.
  //
  // With the fix, shard 1 is padded to decode_range=[0, 2*chunk) and stays
  // in the 'in' map for both stripes, so nerrs=1 throughout.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECUtil::shard_extent_map_t sem(&s);
  // Shard 1: data only in first chunk (second chunk is a sparse hole).
  sem.insert_in_shard(shard_id_t(1), 0, make_nonzero_buf(k2m1_chunk));
  // Parity shard 2: data across the full two-chunk range.
  sem.insert_in_shard(shard_id_t(2), 0, make_nonzero_buf(2 * k2m1_chunk));

  // Uniform shard_want_to_read: both shard 0 and shard 1 over [0, 2*chunk).
  ECUtil::shard_extent_set_t want(k2m1_k + k2m1_m);
  want[shard_id_t(0)].insert(0, 2 * k2m1_chunk);
  want[shard_id_t(1)].insert(0, 2 * k2m1_chunk);

  ECUtil::shard_extent_set_t zeros(k2m1_k + k2m1_m);

  // Shards 1 and 2 replied; shard 0 is absent (to be reconstructed).
  shard_id_map<std::map<uint64_t, uint64_t>> sparse_extents(k2m1_k + k2m1_m);
  sparse_extents[shard_id_t(1)];  // has data (only first chunk allocated)
  sparse_extents[shard_id_t(2)];  // has data (full range)

  int r = ec_sparse_decode(sem, want, zeros, sparse_extents, ec_impl,
                           2 * k2m1_swidth, nullptr);
  ASSERT_EQ(r, 0);
  // Shard 0 must have been reconstructed.
  ASSERT_TRUE(sem.contains_shard(shard_id_t(0)));
}

// --------------------------------------------------------------------------
// ec_sparse_scan_shard_extents — additional edge cases
// --------------------------------------------------------------------------

TEST(ECSparseFuncs_ScanShardExtents, SC10_EmptyScanWindow)
{
  // An empty shard_scan_window means no block intersects it → output always empty.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(0), 0, make_nonzero_buf(k2m1_chunk));

  interval_set<uint64_t> fae;
  fae.insert(0, k2m1_swidth);  // FAE covers everything — irrelevant when window is empty
  interval_set<uint64_t> empty_win;
  auto out = ec_sparse_scan_shard_extents(sem, shard_id_t(0), fae, empty_win, nullptr);
  ASSERT_TRUE(out.empty());
}

TEST(ECSparseFuncs_ScanShardExtents, SC11_AlternatingZeroNonZeroBlocks)
{
  // k=2, chunk=4K: two stripes.
  // Shard 0: stripe 0 non-zero, stripe 1 non-zero → shard-space [0,4K) and [4K,8K).
  // Shard 1: stripe 0 zero, stripe 1 zero → shard-space [0,4K) and [4K,8K).
  // Full RO window [0,16K) → shard 0 shard-space [0,8K), shard 1 shard-space [0,8K).
  const uint64_t chunk = k2m1_chunk;
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth, std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(0), 0,     make_nonzero_buf(chunk));
  sem.insert_in_shard(shard_id_t(1), 0,     make_buf(chunk, '\x00'));
  sem.insert_in_shard(shard_id_t(0), chunk, make_nonzero_buf(chunk));
  sem.insert_in_shard(shard_id_t(1), chunk, make_buf(chunk, '\x00'));

  interval_set<uint64_t> fae;
  auto win0 = shard_window(s, shard_id_t(0), 0, 4 * chunk);
  auto win1 = shard_window(s, shard_id_t(1), 0, 4 * chunk);
  auto out0 = ec_sparse_scan_shard_extents(sem, shard_id_t(0), fae, win0, nullptr);
  auto out1 = ec_sparse_scan_shard_extents(sem, shard_id_t(1), fae, win1, nullptr);

  // Shard 0: both stripe-blocks are non-zero → [0,8K) allocated (merged).
  ASSERT_FALSE(out0.empty());
  ASSERT_EQ(out0.at(0), 2 * chunk);
  // Shard 1: both stripe-blocks are zero → nothing allocated.
  ASSERT_TRUE(out1.empty());
}

TEST(ECSparseFuncs_ScanShardExtents, SC12_FAECoversOnlyPartOfZeroBlock)
{
  // RO FAE [2K,4K) on shard 0 (chunk=4K) → shard-space FAE [2K,4K).
  // scan_stride=4K so the block [0,4K) intersects FAE [2K,4K) → allocated.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(0), 0, make_buf(k2m1_chunk, '\x00'));

  interval_set<uint64_t> ro_fae;
  ro_fae.insert(2048, 2048);  // RO [2K,4K) — within shard 0's chunk
  interval_set<uint64_t> shard_fae =
      s.ro_intervals_to_shard_intervals(ro_fae, shard_id_t(0));
  auto win = shard_window(s, shard_id_t(0), 0, FAE_BLOCK_SIZE);

  auto out = ec_sparse_scan_shard_extents(sem, shard_id_t(0), shard_fae, win, nullptr);
  ASSERT_FALSE(out.empty());
  // Shard-space: [0,4K) allocated.
  ASSERT_EQ(out.count(0), 1u);
  ASSERT_EQ(out.at(0), k2m1_chunk);
}

TEST(ECSparseFuncs_ScanShardExtents, SC13_ScanWindowRestrictsToOneShard)
{
  // RO window [4K,8K) → shard 0 shard-space window is empty (shard 0 covers
  // RO [0,4K) in stripe 0); shard 1 window is [0,4K).
  // Shard 0 data is non-zero but its window is empty → not scanned.
  // Shard 1 data is non-zero and within its window → allocated.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(0), 0, make_nonzero_buf(k2m1_chunk));
  sem.insert_in_shard(shard_id_t(1), 0, make_nonzero_buf(k2m1_chunk));

  interval_set<uint64_t> fae;
  // RO window [4K,8K): shard 0 → empty; shard 1 → [0,4K).
  auto win0 = shard_window(s, shard_id_t(0), k2m1_chunk, k2m1_swidth);
  auto win1 = shard_window(s, shard_id_t(1), k2m1_chunk, k2m1_swidth);

  auto out0 = ec_sparse_scan_shard_extents(sem, shard_id_t(0), fae, win0, nullptr);
  auto out1 = ec_sparse_scan_shard_extents(sem, shard_id_t(1), fae, win1, nullptr);

  // Shard 0 window is empty → output empty.
  ASSERT_TRUE(out0.empty());
  // Shard 1 block [0,4K) is within the window and non-zero → allocated.
  ASSERT_EQ(out1.count(0), 1u);
  ASSERT_EQ(out1.at(0), k2m1_chunk);
}

// --------------------------------------------------------------------------
// ec_sparse_finish_read — additional edge cases
// --------------------------------------------------------------------------

TEST(ECSparseFuncs_FinishRead, WP8_ObjectSizeClampedByReqEnd)
{
  // object_size < offset+length: req_end must be clamped to object_size.
  // Shard 0 has data at [0,4K); object_size=3K so req_end=3K.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;

  const uint64_t object_size = 3 * 1024;  // 3K — smaller than one chunk
  auto req = make_finish_read_req(s, 0, k2m1_chunk, object_size);
  std::map<uint64_t, uint64_t> out_map;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_chunk,
                                /*needs_reconstruct=*/false,
                                ec_impl, fae, out_map, nullptr, nullptr);
  ASSERT_EQ(r, 0);
  // No out_map entry must extend past object_size.
  for (auto &[off, len] : out_map) {
    ASSERT_LE(off + len, object_size);
  }
}

TEST(ECSparseFuncs_FinishRead, WP9_SparseObject_HoleInMiddle)
{
  // Good path: two allocated extents with a hole between them.
  // Shard 0: [0,4K) allocated.  Shard 1: absent (hole at [4K,8K)).
  // Next stripe: shard 0 at offset 4K in shard space → RO [8K,12K) allocated.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  const uint64_t object_size = 3 * k2m1_swidth;  // 24K
  ECCommon::read_result_t res(&s);
  // Stripe 0 shard 0 present, shard 1 absent → RO hole at [4K,8K).
  res.sparse_extents_read[shard_id_t(0)][0]          = k2m1_chunk;
  // Stripe 1 shard 0 present.
  res.sparse_extents_read[shard_id_t(0)][k2m1_chunk] = k2m1_chunk;

  auto req = make_finish_read_req(s, 0, 3 * k2m1_swidth, object_size);
  std::map<uint64_t, uint64_t> out_map;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 0, 3 * k2m1_swidth,
                                /*needs_reconstruct=*/false,
                                ec_impl, fae, out_map, nullptr, nullptr);
  ASSERT_EQ(r, 0);
  // [0,4K) must be present.
  bool has_first = false;
  for (auto &[off, len] : out_map) {
    if (off <= 0 && off + len >= k2m1_chunk) { has_first = true; break; }
  }
  ASSERT_TRUE(has_first);
  // [4K,8K) must be absent (hole — shard 1 not in sparse_extents_read).
  for (auto &[off, len] : out_map) {
    ASSERT_FALSE(off < k2m1_swidth && off + len > k2m1_chunk)
        << "hole at [4K,8K) must not appear in out_map";
  }
  // [8K,12K) must be present (second stripe shard 0).
  bool has_second = false;
  for (auto &[off, len] : out_map) {
    if (off <= k2m1_swidth && off + len >= k2m1_swidth + k2m1_chunk) {
      has_second = true; break;
    }
  }
  ASSERT_TRUE(has_second);
}

TEST(ECSparseFuncs_FinishRead, WP12_ReconstructPath_OutBlPopulated)
{
  // Reconstruct path: out_bl must be populated with bytes matching out_map.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;
  res.buffers_read.insert_in_shard(shard_id_t(0), 0, make_nonzero_buf(k2m1_chunk));
  res.buffers_read.insert_in_shard(shard_id_t(1), 0, make_nonzero_buf(k2m1_chunk));

  ECUtil::shard_extent_set_t want(k2m1_k + k2m1_m);
  want[shard_id_t(0)].insert(0, k2m1_chunk);
  want[shard_id_t(1)].insert(0, k2m1_chunk);
  ECCommon::read_request_t req(want, ECCommon::WantAttrs::No,
                               ECCommon::WantOmapHeader::No,
                               ECCommon::WantOmapKeys::No,
                               "", 0, k2m1_swidth);

  std::map<uint64_t, uint64_t> out_map;
  bufferlist out_bl;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_swidth,
                                /*needs_reconstruct=*/true,
                                ec_impl, fae, out_map, &out_bl, nullptr);
  ASSERT_EQ(r, 0);
  uint64_t total = 0;
  for (auto &[off, len] : out_map) total += len;
  ASSERT_EQ(out_bl.length(), total);
  ASSERT_GT(out_bl.length(), 0u);
}

TEST(ECSparseFuncs_FinishRead, WP13_GoodPath_FiemapHoleIsHole)
{
  // Good path (!needs_reconstruct): a shard with no fiemap entry must produce
  // no out_map entry even if offset/length covers its RO range.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECCommon::read_result_t res(&s);
  // Only shard 0 reported in fiemap; shard 1's RO range [4K,8K) is a hole.
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;

  auto req = make_finish_read_req(s, 0, k2m1_swidth, k2m1_swidth);
  std::map<uint64_t, uint64_t> out_map;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_swidth,
                                /*needs_reconstruct=*/false,
                                ec_impl, fae, out_map, nullptr, nullptr);
  ASSERT_EQ(r, 0);
  // [0,4K) allocated.
  ASSERT_EQ(out_map.size(), 1u);
  ASSERT_EQ(out_map.at(0), k2m1_chunk);
  // [4K,8K) must be absent.
  for (auto &[off, len] : out_map) {
    ASSERT_LE(off + len, k2m1_chunk)
        << "[4K,8K) hole must not appear on good path";
  }
}

// --------------------------------------------------------------------------
// ec_sparse_scan_shard_extents — new shard-space-specific tests
// --------------------------------------------------------------------------

TEST(ECSparseFuncs_ScanShardExtents, SC_Stage1NonZeroFirstWord)
{
  // Block whose first 8 bytes are non-zero and the rest are zero.
  // Stage 1 fires immediately; stage 2 is not reached.
  // k=2, chunk=4K, shard 0.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);

  // Build a 4K buffer: first 8 bytes non-zero, rest zero.
  bufferlist bl;
  bufferptr ptr = buffer::create(k2m1_chunk);
  memset(ptr.c_str(), '\x00', k2m1_chunk);
  memset(ptr.c_str(), '\xAB', sizeof(uint64_t));  // first 8 bytes non-zero
  bl.append(ptr);
  sem.insert_in_shard(shard_id_t(0), 0, bl);

  interval_set<uint64_t> fae;
  auto win = shard_window(s, shard_id_t(0), 0, FAE_BLOCK_SIZE);
  auto out = ec_sparse_scan_shard_extents(sem, shard_id_t(0), fae, win, nullptr);

  // Block is allocated via stage-1.
  ASSERT_EQ(out.count(0), 1u);
  ASSERT_EQ(out.at(0), k2m1_chunk);
}

TEST(ECSparseFuncs_ScanShardExtents, SC_Stage1PassesStage2Finds)
{
  // Block whose first 8 bytes are zero but remaining bytes are non-zero.
  // Stage 1 passes (first word is zero); stage 2 finds the block non-zero.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);

  // Build a 4K buffer: first 8 bytes zero, remainder non-zero.
  bufferlist bl;
  bufferptr ptr = buffer::create(k2m1_chunk);
  memset(ptr.c_str(), '\xAB', k2m1_chunk);
  memset(ptr.c_str(), '\x00', sizeof(uint64_t));  // first 8 bytes zero
  bl.append(ptr);
  sem.insert_in_shard(shard_id_t(0), 0, bl);

  interval_set<uint64_t> fae;
  auto win = shard_window(s, shard_id_t(0), 0, FAE_BLOCK_SIZE);
  auto out = ec_sparse_scan_shard_extents(sem, shard_id_t(0), fae, win, nullptr);

  // Block is allocated via stage-2.
  ASSERT_EQ(out.count(0), 1u);
  ASSERT_EQ(out.at(0), k2m1_chunk);
}

TEST(ECSparseFuncs_ScanShardExtents, SC_FAEShardSpace)
{
  // Shard-space FAE covers [0,4K); buffer is all-zero.
  // FAE check fires; zero-detection is never reached.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(0), 0, make_buf(k2m1_chunk, '\x00'));

  // FAE already in shard space — covers the block directly.
  interval_set<uint64_t> shard_fae;
  shard_fae.insert(0, k2m1_chunk);
  auto win = shard_window(s, shard_id_t(0), 0, FAE_BLOCK_SIZE);
  auto out = ec_sparse_scan_shard_extents(sem, shard_id_t(0), shard_fae, win, nullptr);

  ASSERT_EQ(out.count(0), 1u);
  ASSERT_EQ(out.at(0), k2m1_chunk);
}

TEST(ECSparseFuncs_ScanShardExtents, SC_ScanWindowRestricts)
{
  // Shard 0 has two 4K blocks at shard-space [0,4K) and [4K,8K).
  // Scan window covers only the first block [0,4K).
  // The second block is non-zero but outside the window → absent from output.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(0), 0,          make_buf(k2m1_chunk, '\x00'));
  sem.insert_in_shard(shard_id_t(0), k2m1_chunk, make_nonzero_buf(k2m1_chunk));

  interval_set<uint64_t> fae;
  // Window covers only shard-space [0,4K) (RO [0,4K) for shard 0).
  auto win = shard_window(s, shard_id_t(0), 0, FAE_BLOCK_SIZE);
  auto out = ec_sparse_scan_shard_extents(sem, shard_id_t(0), fae, win, nullptr);

  // First block is zero and outside FAE → absent.
  ASSERT_TRUE(out.empty());
  // (The second block is non-zero but outside the window → not scanned.)
}

TEST(ECSparseFuncs_ScanShardExtents, SC_LargeChunkStrideIsFAEBlock)
{
  // chunk=64K, k=2: scan_stride = min(64K, 4K) = 4K.
  // Shard 0 has a 64K buffer: sub-block [0,4K) zero, [4K,64K) non-zero.
  // Scan window covers the full 64K shard-space chunk.
  // Expected: [0,4K) excluded (zero, no FAE); [4K,64K) allocated (non-zero),
  //           split into 4K sub-blocks each independently determined.
  const uint64_t chunk = 64 * 1024;
  const uint64_t fae_bs = FAE_BLOCK_SIZE;
  ECUtil::stripe_info_t s(2u, 1u, 2 * chunk, std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);

  bufferlist bl;
  bl.append_zero(fae_bs);                        // first 4K: zero
  bl.append(std::string(chunk - fae_bs, '\xAB')); // remainder: non-zero
  sem.insert_in_shard(shard_id_t(0), 0, bl);

  interval_set<uint64_t> fae;
  // RO window [0, chunk) covers only shard 0's first chunk.
  auto win = shard_window(s, shard_id_t(0), 0, chunk);
  auto out = ec_sparse_scan_shard_extents(sem, shard_id_t(0), fae, win, nullptr);

  // First 4K sub-block is zero → absent.
  ASSERT_EQ(out.count(0), 0u);
  // Second sub-block [4K,8K) is non-zero → present.
  ASSERT_EQ(out.count(fae_bs), 1u);
  // All allocated sub-blocks coalesce into one entry starting at fae_bs.
  ASSERT_EQ(out.at(fae_bs), chunk - fae_bs);
}

TEST(ECSparseFuncs_ScanShardExtents, SC_SmallChunkStride)
{
  // chunk=2K, k=2: scan_stride = min(2K, 4K) = 2K.
  // Shard 0 shard-space [0,2K): zero.  Shard 1 shard-space [0,2K): non-zero.
  // Each shard's 2K block is independently determined.
  const uint64_t chunk = 2048;
  ECUtil::stripe_info_t s(2u, 1u, 2 * chunk, std::vector<shard_id_t>{});
  ECUtil::shard_extent_map_t sem(&s);
  sem.insert_in_shard(shard_id_t(0), 0, make_buf(chunk, '\x00'));
  sem.insert_in_shard(shard_id_t(1), 0, make_nonzero_buf(chunk));

  interval_set<uint64_t> fae;
  // RO window [0,4K) covers one stripe: shard 0 → [0,2K), shard 1 → [0,2K).
  auto win0 = shard_window(s, shard_id_t(0), 0, FAE_BLOCK_SIZE);
  auto win1 = shard_window(s, shard_id_t(1), 0, FAE_BLOCK_SIZE);

  auto out0 = ec_sparse_scan_shard_extents(sem, shard_id_t(0), fae, win0, nullptr);
  auto out1 = ec_sparse_scan_shard_extents(sem, shard_id_t(1), fae, win1, nullptr);

  // Shard 0 block is zero → absent.
  ASSERT_TRUE(out0.empty());
  // Shard 1 block is non-zero → shard-space [0,2K) allocated.
  ASSERT_EQ(out1.count(0), 1u);
  ASSERT_EQ(out1.at(0), chunk);
}

// --------------------------------------------------------------------------
// ec_sparse_finish_read — scan clipped to client range by Stage 3
// --------------------------------------------------------------------------

TEST(ECSparseFuncs_FinishRead, WP_ScanRangeRestriction)
{
  // Reconstruct path: both shards decoded as non-zero.  Client reads only
  // RO [0,4K) (shard 0's range).  Shard 1's RO range is [4K,8K).
  //
  // The scan uses shard_want_to_read as the window, so shard 1 IS scanned
  // and found allocated.  However, ec_sparse_clip_to_map at Stage 3 clips
  // the merged result to [offset, req_end) = [0, 4K), so shard 1's range
  // [4K,8K) must be absent from out_map.
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  MockErasureCode *ecode = new MockErasureCode(k2m1_k, k2m1_k + k2m1_m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  ECCommon::read_result_t res(&s);
  // Only shard 0 was read from disk.
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;
  // Provide non-zero buffers for both shards so MockErasureCode has data.
  res.buffers_read.insert_in_shard(shard_id_t(0), 0, make_nonzero_buf(k2m1_chunk));
  res.buffers_read.insert_in_shard(shard_id_t(1), 0, make_nonzero_buf(k2m1_chunk));

  // shard_want_to_read covers both shards (whole-stripe decode).
  ECUtil::shard_extent_set_t want(k2m1_k + k2m1_m);
  want[shard_id_t(0)].insert(0, k2m1_chunk);
  want[shard_id_t(1)].insert(0, k2m1_chunk);
  ECCommon::read_request_t req(want, ECCommon::WantAttrs::No,
                               ECCommon::WantOmapHeader::No,
                               ECCommon::WantOmapKeys::No,
                               "", 0, k2m1_swidth);

  std::map<uint64_t, uint64_t> out_map;
  interval_set<uint64_t> fae;

  // Client requested offset=0, length=k2m1_chunk → req_end=4K.
  // Stage 3 clips to [0,4K): shard 1's RO range [4K,8K) is excluded.
  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_chunk,
                                /*needs_reconstruct=*/true,
                                ec_impl, fae, out_map, nullptr, nullptr);
  ASSERT_EQ(r, 0);
  // [0,4K) from shard 0 is present (healthy shard, in client range).
  ASSERT_GE(out_map.count(0), 1u);
  // Nothing from shard 1's RO range [4K,8K) must appear — clipped by Stage 3.
  for (auto &[off, len] : out_map) {
    ASSERT_LE(off + len, k2m1_chunk)
        << "shard 1 range [4K,8K) must not appear after clip: out_map entry ["
        << off << "," << off + len << ")";
  }
}
// ==========================================================================
// ECSparseFuncs_XorDecode — integration tests using XorErasureCode
//
// These tests call ec_sparse_finish_read with needs_reconstruct=true.
// D0 (shard 0) is always present and non-zero; D1 (shard 1) is missing and
// must be reconstructed by XOR from D0 and P (shard 2 = parity = D0 XOR D1).
//
// Because XorErasureCode::decode_chunks actually performs XOR, the content of
// the reconstructed shard is deterministic and verifiable.
//
// Standard geometry: k=2, chunk=4K, stripe=8K, scan_stride=min(4K,4K)=4K.
// Large-chunk geometry: k=2, chunk=8K, stripe=16K, scan_stride=min(8K,4K)=4K.
//
// Shard-space → RO-space mapping (standard, chunk=4K):
//   shard 0 [0,4K) → RO [0,4K)
//   shard 1 [0,4K) → RO [4K,8K)
//
// Shard-space → RO-space mapping (large-chunk, chunk=8K):
//   shard 0 [0,8K) → RO [0,8K)   (two 4K scan blocks)
//   shard 1 [0,8K) → RO [8K,16K) (two 4K scan blocks)
// ==========================================================================

// ---------------------------------------------------------------------------
// XD1: standard geometry, D1 reconstructed as all-zero, no FAE.
// Expected: D1's RO range [4K,8K) absent from out_map (sparse hole).
// ---------------------------------------------------------------------------
TEST(ECSparseFuncs_XorDecode, XD1_AllZeroNoFAE)
{
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  XorErasureCode *xcode = new XorErasureCode();
  ErasureCodeInterfaceRef ec_impl(xcode);

  // D0 = non-zero; expected_D1 = all-zero; P = D0 XOR D1 = D0 XOR 0 = D0.
  bufferlist d0   = make_nonzero_buf(k2m1_chunk);
  bufferlist exp1 = make_buf(k2m1_chunk, '\x00');
  bufferlist p    = make_xor_parity(d0, exp1);

  ECCommon::read_result_t res(&s);
  // D0 healthy, in sparse_extents_read; D1 missing (not present).
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;
  res.buffers_read.insert_in_shard(shard_id_t(0), 0, d0);
  res.buffers_read.insert_in_shard(shard_id_t(2), 0, p);

  ECCommon::read_request_t req = make_xor_stripe_request(s);
  std::map<uint64_t, uint64_t> out_map;
  bufferlist out_bl;
  interval_set<uint64_t> fae;  // empty — no force-allocated extents

  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_swidth,
                                /*needs_reconstruct=*/true,
                                ec_impl, fae, out_map, &out_bl, nullptr);
  ASSERT_EQ(r, 0);

  // D0's RO range [0,4K) present (non-zero, healthy shard).
  ASSERT_GE(out_map.count(0), 1u);
  // D1's RO range [4K,8K) absent (all-zero, no FAE → sparse hole).
  for (auto &[off, len] : out_map) {
    ASSERT_LE(off + len, k2m1_chunk)
        << "D1 RO range [4K,8K) must be absent (all-zero, no FAE)";
  }
  // out_bl length must match total allocated bytes.
  uint64_t total = 0;
  for (auto &[off, len] : out_map) total += len;
  ASSERT_EQ(out_bl.length(), total);
}

// ---------------------------------------------------------------------------
// XD2: standard geometry, D1 reconstructed as all-zero, FAE covers D1's range.
// Expected: D1's RO range [4K,8K) present (force-allocated); portion is zero.
// ---------------------------------------------------------------------------
TEST(ECSparseFuncs_XorDecode, XD2_AllZeroWithFAE)
{
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  XorErasureCode *xcode = new XorErasureCode();
  ErasureCodeInterfaceRef ec_impl(xcode);

  bufferlist d0   = make_nonzero_buf(k2m1_chunk);
  bufferlist exp1 = make_buf(k2m1_chunk, '\x00');
  bufferlist p    = make_xor_parity(d0, exp1);

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;
  res.buffers_read.insert_in_shard(shard_id_t(0), 0, d0);
  res.buffers_read.insert_in_shard(shard_id_t(2), 0, p);

  ECCommon::read_request_t req = make_xor_stripe_request(s);
  std::map<uint64_t, uint64_t> out_map;
  bufferlist out_bl;
  interval_set<uint64_t> fae;
  fae.insert(k2m1_chunk, k2m1_chunk);  // RO [4K,8K) force-allocated

  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_swidth,
                                /*needs_reconstruct=*/true,
                                ec_impl, fae, out_map, &out_bl, nullptr);
  ASSERT_EQ(r, 0);

  // Both ranges must appear (D0 non-zero, D1 zero-but-FAE).
  bool d1_covered = false;
  for (auto &[off, len] : out_map) {
    if (off <= k2m1_chunk && off + len > k2m1_chunk) { d1_covered = true; break; }
  }
  ASSERT_TRUE(d1_covered) << "D1 RO range [4K,8K) must be present due to FAE";

  // Isolate D1's bytes in out_bl.  Entries may be coalesced across the
  // D0/D1 boundary, so count only the portion on each side.
  uint64_t d0_bytes = 0;
  for (auto &[off, len] : out_map) {
    uint64_t d0_end = std::min(off + len, k2m1_chunk);
    if (d0_end > off) d0_bytes += d0_end - off;
  }
  // D1 bytes start at d0_bytes in out_bl.
  // An entry may span the D0/D1 boundary (coalesced), so count only the
  // portion at or beyond k2m1_chunk.
  uint64_t d1_bytes = 0;
  for (auto &[off, len] : out_map) {
    if (off + len > k2m1_chunk) {
      uint64_t start = std::max(off, k2m1_chunk);
      d1_bytes += (off + len) - start;
    }
  }
  if (d1_bytes > 0) {
    bufferlist d1_bl;
    d1_bl.substr_of(out_bl, d0_bytes, d1_bytes);
    ASSERT_TRUE(d1_bl.is_zero())
        << "D1 portion of out_bl must be all-zero (reconstructed from zero)";
  }
}

// ---------------------------------------------------------------------------
// XD3: standard geometry, D1 reconstructed as all-data (0xAB), no FAE.
// Expected: D1's RO range [4K,8K) present; D1 bytes are all 0xAB.
// ---------------------------------------------------------------------------
TEST(ECSparseFuncs_XorDecode, XD3_AllData)
{
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  XorErasureCode *xcode = new XorErasureCode();
  ErasureCodeInterfaceRef ec_impl(xcode);

  bufferlist d0   = make_nonzero_buf(k2m1_chunk);       // 0xAB
  bufferlist exp1 = make_buf(k2m1_chunk, '\xAB');        // 0xAB
  bufferlist p    = make_xor_parity(d0, exp1);           // 0xAB XOR 0xAB = 0x00

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;
  res.buffers_read.insert_in_shard(shard_id_t(0), 0, d0);
  res.buffers_read.insert_in_shard(shard_id_t(2), 0, p);

  ECCommon::read_request_t req = make_xor_stripe_request(s);
  std::map<uint64_t, uint64_t> out_map;
  bufferlist out_bl;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_swidth,
                                /*needs_reconstruct=*/true,
                                ec_impl, fae, out_map, &out_bl, nullptr);
  ASSERT_EQ(r, 0);

  // D1's RO range [4K,8K) must be present (non-zero data).
  // The entry may be coalesced with D0 (e.g. {0: 8192}), so check that
  // some entry spans into [4K,8K) rather than requiring a separate key.
  bool d1_present = false;
  for (auto &[off, len] : out_map) {
    if (off + len > k2m1_chunk) { d1_present = true; break; }
  }
  ASSERT_TRUE(d1_present) << "D1 RO range [4K,8K) must be present (non-zero data)";

  // Isolate D1's bytes: for entries spanning the boundary, take only the
  // portion at or beyond k2m1_chunk.
  uint64_t d0_bytes = 0;
  uint64_t d1_bytes = 0;
  for (auto &[off, len] : out_map) {
    uint64_t d0_end = std::min(off + len, k2m1_chunk);
    if (d0_end > off) d0_bytes += d0_end - off;
    if (off + len > k2m1_chunk) {
      uint64_t start = std::max(off, k2m1_chunk);
      d1_bytes += (off + len) - start;
    }
  }
  ASSERT_GT(d1_bytes, 0u);
  bufferlist d1_bl;
  d1_bl.substr_of(out_bl, d0_bytes, d1_bytes);
  ASSERT_FALSE(d1_bl.is_zero())
      << "D1 portion must be non-zero (decoded from 0xAB data)";
  // Content check: D1 must equal exp1 (0xAB pattern).
  bufferlist expected;
  expected.append(make_buf(d1_bytes, '\xAB'));
  ASSERT_TRUE(d1_bl.contents_equal(expected));
}

// ---------------------------------------------------------------------------
// XD4: standard geometry, D1 first 8 bytes zero, rest 0xAB (stage-2 trigger).
// Expected: whole D1 chunk allocated; D1 bytes are non-zero.
// ---------------------------------------------------------------------------
TEST(ECSparseFuncs_XorDecode, XD4_ZeroFirstWordDataRest)
{
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  XorErasureCode *xcode = new XorErasureCode();
  ErasureCodeInterfaceRef ec_impl(xcode);

  // Build exp1: first 8 bytes zero, remaining bytes 0xAB.
  bufferlist exp1;
  exp1.append(make_buf(8, '\x00'));
  exp1.append(make_buf(k2m1_chunk - 8, '\xAB'));

  bufferlist d0 = make_nonzero_buf(k2m1_chunk);  // 0xAB
  bufferlist p  = make_xor_parity(d0, exp1);

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;
  res.buffers_read.insert_in_shard(shard_id_t(0), 0, d0);
  res.buffers_read.insert_in_shard(shard_id_t(2), 0, p);

  ECCommon::read_request_t req = make_xor_stripe_request(s);
  std::map<uint64_t, uint64_t> out_map;
  bufferlist out_bl;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_swidth,
                                /*needs_reconstruct=*/true,
                                ec_impl, fae, out_map, &out_bl, nullptr);
  ASSERT_EQ(r, 0);

  // D1's RO range must be allocated (stage-2 scan finds non-zero rest).
  bool d1_present = false;
  for (auto &[off, len] : out_map) {
    if (off + len > k2m1_chunk) { d1_present = true; break; }
  }
  ASSERT_TRUE(d1_present)
      << "D1 must be allocated: first word zero but rest non-zero (stage 2)";

  // D1 bytes must be non-zero overall.
  uint64_t d0_bytes = 0, d1_bytes = 0;
  for (auto &[off, len] : out_map) {
    uint64_t d0_end = std::min(off + len, k2m1_chunk);
    if (d0_end > off) d0_bytes += d0_end - off;
    if (off + len > k2m1_chunk) {
      uint64_t start = std::max(off, k2m1_chunk);
      d1_bytes += (off + len) - start;
    }
  }
  ASSERT_GT(d1_bytes, 0u);
  bufferlist d1_bl;
  d1_bl.substr_of(out_bl, d0_bytes, d1_bytes);
  ASSERT_FALSE(d1_bl.is_zero());
}

// ---------------------------------------------------------------------------
// XD5: standard geometry, D1 first 8 bytes 0xAB, rest zero (stage-1 trigger).
// Expected: whole D1 chunk allocated; D1 bytes are non-zero.
// ---------------------------------------------------------------------------
TEST(ECSparseFuncs_XorDecode, XD5_DataFirstWordZeroRest)
{
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, k2m1_swidth,
                          std::vector<shard_id_t>{});
  XorErasureCode *xcode = new XorErasureCode();
  ErasureCodeInterfaceRef ec_impl(xcode);

  // Build exp1: first 8 bytes 0xAB, remaining bytes zero.
  bufferlist exp1;
  exp1.append(make_buf(8, '\xAB'));
  exp1.append(make_buf(k2m1_chunk - 8, '\x00'));

  bufferlist d0 = make_nonzero_buf(k2m1_chunk);  // 0xAB
  bufferlist p  = make_xor_parity(d0, exp1);

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = k2m1_chunk;
  res.buffers_read.insert_in_shard(shard_id_t(0), 0, d0);
  res.buffers_read.insert_in_shard(shard_id_t(2), 0, p);

  ECCommon::read_request_t req = make_xor_stripe_request(s);
  std::map<uint64_t, uint64_t> out_map;
  bufferlist out_bl;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 0, k2m1_swidth,
                                /*needs_reconstruct=*/true,
                                ec_impl, fae, out_map, &out_bl, nullptr);
  ASSERT_EQ(r, 0);

  // D1's RO range must be allocated (stage-1 fires on first word 0xAB).
  bool d1_present = false;
  for (auto &[off, len] : out_map) {
    if (off + len > k2m1_chunk) { d1_present = true; break; }
  }
  ASSERT_TRUE(d1_present)
      << "D1 must be allocated: first word non-zero (stage 1)";

  uint64_t d0_bytes = 0, d1_bytes = 0;
  for (auto &[off, len] : out_map) {
    uint64_t d0_end = std::min(off + len, k2m1_chunk);
    if (d0_end > off) d0_bytes += d0_end - off;
    if (off + len > k2m1_chunk) {
      uint64_t start = std::max(off, k2m1_chunk);
      d1_bytes += (off + len) - start;
    }
  }
  ASSERT_GT(d1_bytes, 0u);
  bufferlist d1_bl;
  d1_bl.substr_of(out_bl, d0_bytes, d1_bytes);
  ASSERT_FALSE(d1_bl.is_zero());
}

// ---------------------------------------------------------------------------
// Large-chunk geometry constants used by XD6–XD8.
// chunk=8K, stripe=16K, scan_stride=min(8K,4K)=4K → two 4K scan blocks per shard.
// Shard 1 shard-space [0,8K) maps to RO [8K,16K).
// ---------------------------------------------------------------------------
static constexpr uint64_t lc_chunk  = 8192;   // 8 KiB
static constexpr uint64_t lc_swidth = 2 * lc_chunk;  // 16 KiB

// ---------------------------------------------------------------------------
// XD6: large-chunk, D1 first 4K block zero, second 4K block 0xAB.
// Expected: only the second 4K of D1's RO range present (RO [12K,16K)).
// ---------------------------------------------------------------------------
TEST(ECSparseFuncs_XorDecode, XD6_LargeChunk_FirstBlockZeroSecondData)
{
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, lc_swidth,
                          std::vector<shard_id_t>{});
  XorErasureCode *xcode = new XorErasureCode();
  ErasureCodeInterfaceRef ec_impl(xcode);

  // exp1: first 4K zero, second 4K 0xAB.
  bufferlist exp1;
  exp1.append(make_buf(4096, '\x00'));
  exp1.append(make_buf(4096, '\xAB'));

  bufferlist d0 = make_nonzero_buf(lc_chunk);   // 0xAB throughout
  bufferlist p  = make_xor_parity(d0, exp1);

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = lc_chunk;
  res.buffers_read.insert_in_shard(shard_id_t(0), 0, d0);
  res.buffers_read.insert_in_shard(shard_id_t(2), 0, p);

  ECCommon::read_request_t req = make_xor_stripe_request(s);
  std::map<uint64_t, uint64_t> out_map;
  bufferlist out_bl;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 0, lc_swidth,
                                /*needs_reconstruct=*/true,
                                ec_impl, fae, out_map, &out_bl, nullptr);
  ASSERT_EQ(r, 0);

  // D1 shard-space: first 4K block (→ RO [8K,12K)) must be absent.
  // D1 shard-space: second 4K block (→ RO [12K,16K)) must be present.
  bool first_block_absent = true;
  bool second_block_present = false;
  for (auto &[off, len] : out_map) {
    // RO [8K,12K) — first scan block of D1.
    if (off < lc_chunk + 4096 && off + len > lc_chunk) {
      first_block_absent = false;
    }
    // RO [12K,16K) — second scan block of D1.
    if (off <= lc_chunk + 4096 && off + len > lc_chunk + 4096) {
      second_block_present = true;
    }
  }
  ASSERT_TRUE(first_block_absent)
      << "D1 first 4K scan block (RO [8K,12K)) must be absent (all-zero)";
  ASSERT_TRUE(second_block_present)
      << "D1 second 4K scan block (RO [12K,16K)) must be present (0xAB)";
}

// ---------------------------------------------------------------------------
// XD7: large-chunk, D1 first 4K block 0xAB, second 4K block zero.
// Expected: only the first 4K of D1's RO range present (RO [8K,12K)).
// ---------------------------------------------------------------------------
TEST(ECSparseFuncs_XorDecode, XD7_LargeChunk_FirstBlockDataSecondZero)
{
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, lc_swidth,
                          std::vector<shard_id_t>{});
  XorErasureCode *xcode = new XorErasureCode();
  ErasureCodeInterfaceRef ec_impl(xcode);

  // exp1: first 4K 0xAB, second 4K zero.
  bufferlist exp1;
  exp1.append(make_buf(4096, '\xAB'));
  exp1.append(make_buf(4096, '\x00'));

  bufferlist d0 = make_nonzero_buf(lc_chunk);
  bufferlist p  = make_xor_parity(d0, exp1);

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = lc_chunk;
  res.buffers_read.insert_in_shard(shard_id_t(0), 0, d0);
  res.buffers_read.insert_in_shard(shard_id_t(2), 0, p);

  ECCommon::read_request_t req = make_xor_stripe_request(s);
  std::map<uint64_t, uint64_t> out_map;
  bufferlist out_bl;
  interval_set<uint64_t> fae;

  int r = ec_sparse_finish_read(s, res, req, 0, lc_swidth,
                                /*needs_reconstruct=*/true,
                                ec_impl, fae, out_map, &out_bl, nullptr);
  ASSERT_EQ(r, 0);

  // D1 first 4K block (RO [8K,12K)) must be present.
  bool first_block_present = false;
  // D1 second 4K block (RO [12K,16K)) must be absent.
  bool second_block_present = false;
  for (auto &[off, len] : out_map) {
    if (off <= lc_chunk && off + len > lc_chunk) {
      first_block_present = true;
    }
    if (off < lc_swidth && off + len > lc_chunk + 4096) {
      second_block_present = true;
    }
  }
  ASSERT_TRUE(first_block_present)
      << "D1 first 4K scan block (RO [8K,12K)) must be present (0xAB)";
  ASSERT_FALSE(second_block_present)
      << "D1 second 4K scan block (RO [12K,16K)) must be absent (all-zero)";
}

// ---------------------------------------------------------------------------
// XD8: large-chunk, both D1 blocks zero but FAE covers only first 4K of D1.
// Expected: only first 4K of D1's RO range present (RO [8K,12K));
// second 4K absent (zero, not in FAE).
// ---------------------------------------------------------------------------
TEST(ECSparseFuncs_XorDecode, XD8_LargeChunk_FAECoversOnlyFirstBlock)
{
  ECUtil::stripe_info_t s(k2m1_k, k2m1_m, lc_swidth,
                          std::vector<shard_id_t>{});
  XorErasureCode *xcode = new XorErasureCode();
  ErasureCodeInterfaceRef ec_impl(xcode);

  // Both D1 blocks are zero.
  bufferlist exp1 = make_buf(lc_chunk, '\x00');

  bufferlist d0 = make_nonzero_buf(lc_chunk);
  bufferlist p  = make_xor_parity(d0, exp1);  // p = d0 XOR 0 = d0

  ECCommon::read_result_t res(&s);
  res.sparse_extents_read[shard_id_t(0)][0] = lc_chunk;
  res.buffers_read.insert_in_shard(shard_id_t(0), 0, d0);
  res.buffers_read.insert_in_shard(shard_id_t(2), 0, p);

  ECCommon::read_request_t req = make_xor_stripe_request(s);
  std::map<uint64_t, uint64_t> out_map;
  bufferlist out_bl;
  // FAE covers RO [8K,12K) — only the first 4K scan block of D1.
  interval_set<uint64_t> fae;
  fae.insert(lc_chunk, 4096);

  int r = ec_sparse_finish_read(s, res, req, 0, lc_swidth,
                                /*needs_reconstruct=*/true,
                                ec_impl, fae, out_map, &out_bl, nullptr);
  ASSERT_EQ(r, 0);

  // First 4K of D1's RO range [8K,12K) must be present (FAE).
  bool first_block_present = false;
  // Second 4K of D1's RO range [12K,16K) must be absent (zero, no FAE).
  bool second_block_present = false;
  for (auto &[off, len] : out_map) {
    if (off <= lc_chunk && off + len > lc_chunk) {
      first_block_present = true;
    }
    if (off < lc_swidth && off + len > lc_chunk + 4096) {
      second_block_present = true;
    }
  }
  ASSERT_TRUE(first_block_present)
      << "D1 first 4K scan block (RO [8K,12K)) must be present (FAE)";
  ASSERT_FALSE(second_block_present)
      << "D1 second 4K scan block (RO [12K,16K)) must be absent (zero, no FAE)";

  // D1 first-block bytes in out_bl must be all-zero.
  uint64_t d0_bytes = 0, d1_bytes = 0;
  for (auto &[off, len] : out_map) {
    uint64_t d0_end = std::min(off + len, lc_chunk);
    if (d0_end > off) d0_bytes += d0_end - off;
    if (off + len > lc_chunk) {
      uint64_t start = std::max(off, lc_chunk);
      d1_bytes += (off + len) - start;
    }
  }
  ASSERT_GT(d1_bytes, 0u);
  bufferlist d1_bl;
  d1_bl.substr_of(out_bl, d0_bytes, d1_bytes);
  ASSERT_TRUE(d1_bl.is_zero())
      << "D1 FAE portion of out_bl must be all-zero (reconstructed as zero)";
}
