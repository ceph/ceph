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
#include <errno.h>
#include <signal.h>
#include "osd/ECUtil.h"
#include "gtest/gtest.h"
#include "osd/osd_types.h"
#include "common/ceph_argparse.h"
#include "osd/ECTransaction.h"

using namespace std;
using namespace ECUtil;

// FIXME: Once PRs are in, we should move the other ECUtil tests are moved here.

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
    auto iter = sem.begin_slice_iterator(out_set);
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
    auto iter = sem.begin_slice_iterator(out_set);

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
    auto iter = sem.begin_slice_iterator(out_set);

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
    auto iter = sem.begin_slice_iterator(out_set);

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
    auto iter = sem.begin_slice_iterator(out_set);
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
    auto iter = sem.begin_slice_iterator(out_set);

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
    auto iter = sem.begin_slice_iterator(out_set);

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
    auto iter = sem.begin_slice_iterator(out_set);

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
    ASSERT_EQ(512, slice_map.get_start_offset());
    ASSERT_EQ(512+1024, slice_map.get_end_offset());

    for (int i=1; i<5; i++) {
      ASSERT_EQ(512, slice_map.get_extent_map(shard_id_t(i)).get_start_off());
      ASSERT_EQ(512+1024, slice_map.get_extent_map(shard_id_t(i)).get_end_off());
    }
  }

  {
    auto slice_map = sem.slice_map(0, 4096);
    ASSERT_EQ(4, slice_map.get_extent_maps().size());
    ASSERT_EQ(5, slice_map.get_start_offset());
    ASSERT_EQ(4096, slice_map.get_end_offset());
    ASSERT_EQ(512, slice_map.get_extent_map(shard_id_t(1)).get_start_off());
    ASSERT_EQ(512 + 1024, slice_map.get_extent_map(shard_id_t(1)).get_end_off());
    ASSERT_EQ(5, slice_map.get_extent_map(shard_id_t(2)).get_start_off());
    ASSERT_EQ(4096, slice_map.get_extent_map(shard_id_t(2)).get_end_off());
    ASSERT_EQ(256, slice_map.get_extent_map(shard_id_t(3)).get_start_off());
    ASSERT_EQ(4096, slice_map.get_extent_map(shard_id_t(3)).get_end_off());
    ASSERT_EQ(5, slice_map.get_extent_map(shard_id_t(4)).get_start_off());
    ASSERT_EQ(4096, slice_map.get_extent_map(shard_id_t(4)).get_end_off());
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