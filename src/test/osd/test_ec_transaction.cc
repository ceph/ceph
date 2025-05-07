// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <gtest/gtest.h>
#include "osd/PGTransaction.h"
#include "osd/ECTransaction.h"
#include "common/debug.h"
#include "osd/ECBackend.h"

#include "test/unit.cc"

struct mydpp : public DoutPrefixProvider {
  std::ostream& gen_prefix(std::ostream& out) const override { return out << "foo"; }
  CephContext *get_cct() const override { return g_ceph_context; }
  unsigned get_subsys() const override { return ceph_subsys_osd; }
} dpp;

#define dout_context g_ceph_context

struct ECTestOp : ECCommon::RMWPipeline::Op {
  PGTransactionUPtr t;
};

TEST(ectransaction, two_writes_separated_append)
{
  hobject_t h;
  PGTransaction::ObjectOperation op;
  bufferlist a, b;
  a.append_zero(565760);
  op.buffer_updates.insert(0, a.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{a, 0});
  b.append_zero(2437120);
  op.buffer_updates.insert(669856, b.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{b, 0});

  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(2, 2, 8192, &pool);
  shard_id_set shards;
  shards.insert_range(shard_id_t(), 4);
  ECTransaction::WritePlanObj plan(
    h,
    op,
    sinfo,
    shards,
    shards,
    false,
    0,
    std::nullopt,
    std::nullopt,
    ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
    nullptr,
    0);

  generic_derr << "plan " << plan << dendl;

  ASSERT_FALSE(plan.to_read);
  ASSERT_EQ(4u, plan.will_write.shard_count());
}

TEST(ectransaction, two_writes_separated_misaligned_overwrite)
{
  hobject_t h;
  PGTransaction::ObjectOperation op;
  bufferlist a, b;
  a.append_zero(565760);
  op.buffer_updates.insert(0, a.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{a, 0});
  b.append_zero(2437120);
  op.buffer_updates.insert(669856, b.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{b, 0});

  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(2, 2, 8192, &pool, std::vector<shard_id_t>(0));
  object_info_t oi;
  oi.size = 3112960;
  shard_id_set shards;
  shards.insert_range(shard_id_t(), 4);

  ECTransaction::WritePlanObj plan(
    h,
    op,
    sinfo,
    shards,
    shards,
    false,
    oi.size,
    oi,
    std::nullopt,
    ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
    nullptr,
    0);

  generic_derr << "plan " << plan << dendl;

  ASSERT_EQ(2u, (*plan.to_read).shard_count());
  ASSERT_EQ(4u, plan.will_write.shard_count());
}

// Test writing to an object at an offset which is beyond the end of the
// current object.
TEST(ectransaction, partial_write)
{
  hobject_t h;
  PGTransaction::ObjectOperation op;
  bufferlist a;

  // Start by writing 8 bytes to the start of an object.
  a.append_zero(8);
  op.buffer_updates.insert(0, a.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{a, 0});

  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(2, 1, 8192, &pool, std::vector<shard_id_t>(0));
  object_info_t oi;
  oi.size = 8;
  shard_id_set shards;
  shards.insert_range(shard_id_t(), 3);

  ECTransaction::WritePlanObj plan(
    h,
    op,
    sinfo,
    shards,
    shards,
    false,
    0,
    oi,
    std::nullopt,
    ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
    nullptr,
    0);

  generic_derr << "plan " << plan << dendl;

  // The object is empty, so we should have no reads and an 4k write.
  ASSERT_FALSE(plan.to_read);
  extent_set ref_write;
  ref_write.insert(0, EC_ALIGN_SIZE);
  ASSERT_EQ(2u, plan.will_write.shard_count());
  ASSERT_EQ(ref_write, plan.will_write.at(shard_id_t(0)));
  ASSERT_EQ(ref_write, plan.will_write.at(shard_id_t(2)));
}

TEST(ectransaction, overlapping_write_non_aligned)
{
  hobject_t h;
  PGTransaction::ObjectOperation op;
  bufferlist a;

  // Start by writing 8 bytes to the start of an object.
  a.append_zero(8);
  op.buffer_updates.insert(0, a.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{a, 0});

  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(2, 1, 8192, &pool, std::vector<shard_id_t>(0));
  object_info_t oi;
  oi.size = 8;
  shard_id_set shards;
  shards.insert_range(shard_id_t(), 4);
  ECTransaction::WritePlanObj plan(
    h,
    op,
    sinfo,
    shards,
    shards,
    false,
    8,
    oi,
    std::nullopt,
    ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
    nullptr,
    0);

  generic_derr << "plan " << plan << dendl;

  // There should be no overlap of this read.
  ASSERT_EQ(1u, (*plan.to_read).shard_count());
  extent_set ref;
  ref.insert(0, EC_ALIGN_SIZE);
  ASSERT_EQ(2u, plan.will_write.shard_count());
  ASSERT_EQ(1u, (*plan.to_read).shard_count());
  ASSERT_EQ(ref, plan.will_write.at(shard_id_t(0)));
  ASSERT_EQ(ref, plan.will_write.at(shard_id_t(2)));
}

TEST(ectransaction, test_appending_write_non_aligned)
{
  hobject_t h;
  PGTransaction::ObjectOperation op;
  bufferlist a;

  // Start by writing 8 bytes to the start of an object.
  a.append_zero(4096);
  op.buffer_updates.insert(3*4096, a.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{a, 0});

  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(2, 1, 8192, &pool, std::vector<shard_id_t>(0));
  object_info_t oi;
  oi.size = 4*4096;
  shard_id_set shards;
  shards.insert_range(shard_id_t(), 4);
  ECTransaction::WritePlanObj plan(
    h,
    op,
    sinfo,
    shards,
    shards,
    false,
    8,
    oi,
    std::nullopt,
    ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
    nullptr,
    0);

  generic_derr << "plan " << plan << dendl;

  // We are growing an option from zero with a hole.
  ASSERT_FALSE(plan.to_read);

  // The writes will cover not cover the zero parts
  ECUtil::shard_extent_set_t ref_write(sinfo.get_k_plus_m());
  ref_write[shard_id_t(1)].insert(4096, 4096);
  ref_write[shard_id_t(2)].insert(4096, 4096);
  ASSERT_EQ(ref_write, plan.will_write);
}

TEST(ectransaction, append_with_large_hole)
{
  hobject_t h;
  PGTransaction::ObjectOperation op;
  bufferlist a;

  // We have a 4k write quite a way after the current limit of a 4k object
  a.append_zero(4096);
  op.buffer_updates.insert(24*4096, a.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{a, 0});

  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(2, 1, 8192, &pool, std::vector<shard_id_t>(0));
  object_info_t oi;
  oi.size = 25*4096;
  shard_id_set shards;
  shards.insert_range(shard_id_t(), 4);
  ECTransaction::WritePlanObj plan(
    h,
    op,
    sinfo,
    shards,
    shards,
    false,
    4096,
    oi,
    std::nullopt,
    ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
    nullptr,
    0);

  generic_derr << "plan " << plan << dendl;

  // Should not require any reads.
  ASSERT_FALSE(plan.to_read);

  // The writes will cover the new zero parts.
  ECUtil::shard_extent_set_t ref_write(sinfo.get_k_plus_m());
  ref_write[shard_id_t(0)].insert(12*4096, 4096);
  ref_write[shard_id_t(2)].insert(12*4096, 4096);
  ASSERT_EQ(ref_write, plan.will_write);
}

TEST(ectransaction, test_append_not_page_aligned_with_large_hole)
{
  hobject_t h;
  PGTransaction::ObjectOperation op;
  bufferlist a;

  // We have a 4k write quite a way after the current limit of a EC_ALIGN_SIZE object
  a.append_zero(EC_ALIGN_SIZE / 2);
  op.buffer_updates.insert(24 * EC_ALIGN_SIZE + EC_ALIGN_SIZE / 4, a.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{a, 0});

  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(2, 1, 2 * EC_ALIGN_SIZE, &pool, std::vector<shard_id_t>(0));
  object_info_t oi;
  oi.size = 25*EC_ALIGN_SIZE;
  shard_id_set shards;
  shards.insert_range(shard_id_t(), 3);
  ECTransaction::WritePlanObj plan(
    h,
    op,
    sinfo,
    shards,
    shards,
    false,
    EC_ALIGN_SIZE,
    oi,
    std::nullopt,
    ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
    nullptr,
    0);

  generic_derr << "plan " << plan << dendl;

  // No reads (because not yet written)
  ASSERT_FALSE(plan.to_read);

  // Writes should grow to 4k
  ECUtil::shard_extent_set_t ref_write(sinfo.get_k_plus_m());
  ref_write[shard_id_t(0)].insert(12*EC_ALIGN_SIZE, EC_ALIGN_SIZE);
  ref_write[shard_id_t(2)].insert(12*EC_ALIGN_SIZE, EC_ALIGN_SIZE);
  ASSERT_EQ(ref_write, plan.will_write);
}

TEST(ectransaction, test_overwrite_with_missing)
{
  hobject_t h;
  PGTransaction::ObjectOperation op, op2;
  bufferlist a;

  // We have a 4k write quite a way after the current limit of a 4k object
  a.append_zero(14 * (EC_ALIGN_SIZE / 4));
  op.buffer_updates.insert(0, a.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{a, 0});

  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(2, 1, 2 * EC_ALIGN_SIZE, &pool, std::vector<shard_id_t>(0));
  object_info_t oi;
  oi.size = 42*(EC_ALIGN_SIZE / 4);
  shard_id_set shards;
  shards.insert(shard_id_t(0));
  shards.insert(shard_id_t(1));

  ECTransaction::WritePlanObj plan(
    h,
    op,
    sinfo,
    shards,
    shards,
    false,
    42*(EC_ALIGN_SIZE / 4),
    oi,
    std::nullopt,
    ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
    nullptr,
    0);

  generic_derr << "plan " << plan << dendl;

  // No reads (because not yet written)
  ASSERT_TRUE(plan.to_read);
  ECUtil::shard_extent_set_t ref_read(sinfo.get_k_plus_m());
  ref_read[shard_id_t(1)].insert(EC_ALIGN_SIZE, EC_ALIGN_SIZE);
  ASSERT_EQ(ref_read, plan.to_read);

  // Writes should grow to 4k
  ECUtil::shard_extent_set_t ref_write(sinfo.get_k_plus_m());
  ref_write[shard_id_t(0)].insert(0, 2 * EC_ALIGN_SIZE);
  ref_write[shard_id_t(1)].insert(0, 2 * EC_ALIGN_SIZE);
  ASSERT_EQ(ref_write, plan.will_write);
}

TEST(ectransaction, truncate_to_bigger_without_write)
{
  hobject_t h;
  PGTransaction::ObjectOperation op;

  op.truncate = std::pair(8192, 8192);

  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(2, 2, 8192, &pool);
  shard_id_set shards;
  shards.insert_range(shard_id_t(), 4);
  ECTransaction::WritePlanObj plan(
    h,
    op,
    sinfo,
    shards,
    shards,
    false,
    4096,
    std::nullopt,
    std::nullopt,
    ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
    nullptr,
    0);

  generic_derr << "plan " << plan << dendl;

  ASSERT_FALSE(plan.to_read);
  ASSERT_EQ(0u, plan.will_write.shard_count());
}

TEST(ectransaction, truncate_to_smalelr_without_write) {
  hobject_t h;
  PGTransaction::ObjectOperation op;

  op.truncate = std::pair(EC_ALIGN_SIZE/4, EC_ALIGN_SIZE/4);

  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(2, 2, EC_ALIGN_SIZE*2, &pool);
  shard_id_set shards;
  shards.insert_range(shard_id_t(), 4);
  ECTransaction::WritePlanObj plan(
    h,
    op,
    sinfo,
    shards,
    shards,
    false,
    16*EC_ALIGN_SIZE,
    std::nullopt,
    std::nullopt,
    ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
    nullptr,
    0);

  generic_derr << "plan " << plan << dendl;

  ASSERT_TRUE(plan.to_read);
  ECUtil::shard_extent_set_t ref_read(sinfo.get_k_plus_m());
  ref_read[shard_id_t(0)].insert(0, EC_ALIGN_SIZE);
  ASSERT_EQ(ref_read, plan.to_read);

  // Writes should cover parity only.
  ECUtil::shard_extent_set_t ref_write(sinfo.get_k_plus_m());
  ref_write[shard_id_t(2)].insert(0, EC_ALIGN_SIZE);
  ref_write[shard_id_t(3)].insert(0, EC_ALIGN_SIZE);
  ASSERT_EQ(ref_write, plan.will_write);
}

TEST(ectransaction, delete_and_write_misaligned) {
  hobject_t h;
  PGTransaction::ObjectOperation op;
  bufferlist a;
  uint64_t new_size = 14 * (EC_ALIGN_SIZE / 4);

  // We have a 4k write quite a way after the current limit of a 4k object
  a.append_zero(new_size);
  op.buffer_updates.insert(0, new_size, PGTransaction::ObjectOperation::BufferUpdate::Write{a, 0});
  op.delete_first = true;

  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(2, 1, 2 * EC_ALIGN_SIZE, &pool, std::vector<shard_id_t>(0));
  object_info_t oi;
  oi.size = new_size;
  shard_id_set shards;
  shards.insert_range(shard_id_t(0), 3);

  ECTransaction::WritePlanObj plan(
    h,
    op,
    sinfo,
    shards,
    shards,
    false,
    16*EC_ALIGN_SIZE,
    std::nullopt,
    std::nullopt,
    ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
    nullptr,
    0);

  generic_derr << "plan " << plan << dendl;

  /* We are going to delete the object before writing it.  Best not write anything
   * from the old object... */
  ASSERT_FALSE(plan.to_read);

  // Writes should cover parity only.
  ECUtil::shard_extent_set_t ref_write(sinfo.get_k_plus_m());
  ref_write[shard_id_t(0)].insert(0, 2*EC_ALIGN_SIZE);
  ref_write[shard_id_t(1)].insert(0, 2*EC_ALIGN_SIZE);
  ref_write[shard_id_t(2)].insert(0, 2*EC_ALIGN_SIZE);
  ASSERT_EQ(ref_write, plan.will_write);
}