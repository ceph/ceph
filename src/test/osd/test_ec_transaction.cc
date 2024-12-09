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

  ECUtil::stripe_info_t sinfo(2, 8192, 0);
  ECTransaction::WritePlanObj plan(
    op,    sinfo,
    0,
    std::nullopt,
    std::nullopt,
    ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
    nullptr);

  generic_derr << "plan " << plan << dendl;

  ASSERT_FALSE(plan.to_read);
  ASSERT_EQ(2u, plan.will_write.shard_count());
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

  ECUtil::stripe_info_t sinfo(2, 8192, 0, std::vector<int>(0));
  object_info_t oi;
  oi.size = 3112960;

  ECTransaction::WritePlanObj plan(
  op,
  sinfo,
  oi.size,
  oi,
  std::nullopt,
  ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
  nullptr);

  generic_derr << "plan " << plan << dendl;

  ASSERT_EQ(2u, (*plan.to_read).shard_count());
  ASSERT_EQ(2u, plan.will_write.shard_count());
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

  ECUtil::stripe_info_t sinfo(2, 8192, 1, std::vector<int>(0));
  object_info_t oi;
  oi.size = 8;
  ECTransaction::WritePlanObj plan(
  op,
  sinfo,
  0,
  oi,
  std::nullopt,
  ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
  nullptr);

  generic_derr << "plan " << plan << dendl;

  // The object is empty, so we should have no reads and an 4k write.
  ASSERT_FALSE(plan.to_read);
  extent_set ref_write;
  ref_write.insert(0, 4096);
  ASSERT_EQ(2u, plan.will_write.shard_count());
  ASSERT_EQ(ref_write, plan.will_write.at(0));
  ASSERT_EQ(ref_write, plan.will_write.at(2));
}

TEST(ectransaction, overlapping_write_non_aligned)
{
  hobject_t h;
  PGTransaction::ObjectOperation op;
  bufferlist a;

  // Start by writing 8 bytes to the start of an object.
  a.append_zero(8);
  op.buffer_updates.insert(0, a.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{a, 0});

  ECUtil::stripe_info_t sinfo(2, 8192, 1, std::vector<int>(0));
  object_info_t oi;
  oi.size = 8;
  ECTransaction::WritePlanObj plan(
    op,
    sinfo,
    8,
    oi,
    std::nullopt,
    ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
    nullptr);

  generic_derr << "plan " << plan << dendl;

  // There should be no overlap of this read.
  ASSERT_EQ(1u, (*plan.to_read).shard_count());
  extent_set ref;
  ref.insert(0, 4096);
  ASSERT_EQ(2u, plan.will_write.shard_count());
  ASSERT_EQ(1u, (*plan.to_read).shard_count());
  ASSERT_EQ(ref, plan.will_write.at(0));
  ASSERT_EQ(ref, plan.will_write.at(2));
}

TEST(ectransaction, test_appending_write_non_aligned)
{
  hobject_t h;
  PGTransaction::ObjectOperation op;
  bufferlist a;

  // Start by writing 8 bytes to the start of an object.
  a.append_zero(4096);
  op.buffer_updates.insert(3*4096, a.length(), PGTransaction::ObjectOperation::BufferUpdate::Write{a, 0});

  ECUtil::stripe_info_t sinfo(2, 8192, 1, std::vector<int>(0));
  object_info_t oi;
  oi.size = 4*4096;
  ECTransaction::WritePlanObj plan(
    op,
    sinfo,
    8,
    oi,
    std::nullopt,
    ECUtil::HashInfoRef(new ECUtil::HashInfo(1)),
    nullptr);

  generic_derr << "plan " << plan << dendl;

  // There should be an overlapping read, as we need to write zeros to the
  // empty part.
  ECUtil::shard_extent_set_t ref_read;
  ref_read[0].insert(0, 4096);
  ASSERT_EQ(ref_read, *plan.to_read);

  // The writes will cover the new zero parts.
  ECUtil::shard_extent_set_t ref_write;
  ref_write[0].insert(4096, 4096);
  ref_write[1].insert(0, 8192);
  ref_write[2].insert(0, 8192);
  ASSERT_EQ(ref_write, plan.will_write);
}

