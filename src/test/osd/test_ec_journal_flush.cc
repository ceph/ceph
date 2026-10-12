// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// SPDX-License-Identifier: LGPL-2.1-or-later
#include <array>
#include <set>

#include <gtest/gtest.h>
#include <random>
#include "osd/ECJournalFlush.h"
#include "osd/ECJournalQueue.h"
#include "osd/ECOmapJournal.h"
#include "osd/OSDMap.h"
#include "osd/PGLog.h"
#include "osd/osd_internal_types.h"
#include "global/global_context.h"

namespace {
// Uses the same distinct variant alternatives as ConfigProxy. A regression to
// get_val<uint64_t> for the size setting throws here, just as in an OSD.
struct JournalConfig {
  std::map<std::string, Option::value_t> values = {
    {"osd_ec_journal_max_bytes", Option::size_t{16u << 20}},
  };
  template<typename T> T get_val(const std::string& key) const {
    return std::get<T>(values.at(key));
  }
};

TEST(JournalAdmission, SizeOptionVariantAndInvalidLimits)
{
  JournalConfig conf;
  // The option's minimum: 16 slots of 1 MiB.
  auto limits = ECWriteJournal::read_limits(conf, 32768);
  EXPECT_TRUE(limits.valid());
  EXPECT_EQ(1u << 20, limits.segment_bytes);
  EXPECT_EQ(16u << 20, limits.max_bytes);
  EXPECT_EQ(16u, limits.max_segments);
  conf.values["osd_ec_journal_max_bytes"] = Option::size_t{256u << 20};
  limits = ECWriteJournal::read_limits(conf, 32768);
  EXPECT_TRUE(limits.valid());
  EXPECT_EQ(16u << 20, limits.segment_bytes);
  EXPECT_EQ(16u, limits.max_segments);
  // Far below the minimum a slot cannot hold one aligned record.
  conf.values["osd_ec_journal_max_bytes"] = Option::size_t{16 * 4096 - 1};
  EXPECT_FALSE(ECWriteJournal::read_limits(conf, 32768).valid());
  conf.values["osd_ec_journal_max_bytes"] = Option::size_t{0};
  EXPECT_FALSE(ECWriteJournal::read_limits(conf, 32768).valid());
  conf.values["osd_ec_journal_max_bytes"] = Option::size_t{16u << 20};
  EXPECT_FALSE(ECWriteJournal::read_limits(conf, 0).valid());
}

TEST(JournalAdmission, UnsupportedProfilesRefuseEnrollment)
{
  EXPECT_TRUE(ECWriteJournal::supported_geometry(4, 3, 4096));
  EXPECT_TRUE(ECWriteJournal::supported_geometry(8, 3, 4096));
  EXPECT_TRUE(ECWriteJournal::supported_geometry(12, 3, 4096));
  EXPECT_FALSE(ECWriteJournal::supported_geometry(6, 3, 4096));
  EXPECT_FALSE(ECWriteJournal::supported_geometry(4, 2, 4096));
  EXPECT_FALSE(ECWriteJournal::supported_geometry(4, 3, 8192));
  EXPECT_FALSE(ECWriteJournal::supported_geometry(8, 2, 4096));
  EXPECT_FALSE(ECWriteJournal::supported_geometry(12, 3, 8192));
}

TEST(JournalAdmission, ErrorLogBarrierCannotPassBlockedWrite)
{
  ECWriteJournal::AdmissionQueue<int> pending;
  std::vector<int> submitted;
  pending.push(1);
  pending.ordered([&] { submitted.push_back(2); });
  pending.push(3);
  auto forward = [](std::function<void()> cb) { cb(); };
  // Repeated pressure/deadline wakes leave version 1 blocked, not version 2
  // appended to the PG log ahead of it, even with an idle extent cache.
  for (unsigned i = 0; i < 4; ++i) {
    EXPECT_EQ(1, pending.next(forward));
    EXPECT_TRUE(submitted.empty());
  }
  submitted.push_back(1);
  pending.pop();
  EXPECT_EQ(3, pending.next(forward));
  EXPECT_EQ((std::vector<int>{1, 2}), submitted);
  submitted.push_back(3);
  pending.pop();
  EXPECT_FALSE(pending.next(forward));
}

TEST(JournalAdmission, OrderedCallbackMayEnqueueAndResetCancels)
{
  ECWriteJournal::AdmissionQueue<int> pending;
  pending.ordered([&] { pending.push(4); });
  EXPECT_EQ(4, pending.next([](std::function<void()> cb) { cb(); }));
  bool called = false;
  pending.ordered([&] { called = true; });
  pending.clear();
  EXPECT_FALSE(pending.next([](std::function<void()> cb) { cb(); }));
  EXPECT_FALSE(called);
}

TEST(JournalAdmission, ShutdownWaitsForTrimAndPendingAdmission)
{
  ECWriteJournal::DrainWaiters drain;
  unsigned completed = 0;
  EXPECT_FALSE(drain.requested());
  drain.request([&](int r) { EXPECT_EQ(0, r); ++completed; });
  EXPECT_TRUE(drain.requested());
  drain.maybe_finish(false, true); // data committed, but trim still outstanding
  drain.maybe_finish(true, false); // journal gone, but queued mutations remain
  EXPECT_EQ(0u, completed);
  drain.maybe_finish(true, true);
  drain.maybe_finish(true, true);
  EXPECT_EQ(1u, completed);
  EXPECT_TRUE(drain.requested()); // cannot enroll again before actual stop
}

TEST(JournalAdmission, ShutdownRetryAndCancellationCompleteOnce)
{
  ECWriteJournal::DrainWaiters drain;
  unsigned completed = 0;
  // First shutdown caller can time out; retry registers another owned waiter.
  for (unsigned i = 0; i < 2; ++i) {
    drain.request([&](int r) { EXPECT_EQ(-ECANCELED, r); ++completed; });
  }
  drain.finish(-ECANCELED);
  drain.maybe_finish(true, true);
  EXPECT_EQ(2u, completed);
  EXPECT_TRUE(drain.requested());
}

// do_op's admission: only what eligible_overwrite() will accept once the op
// is versioned, so an admitted write never waits after that.
TEST(JournalAdmission, CandidateWriteOnlyForPlainInStripeOverwrites)
{
  constexpr uint64_t width = 8 * 4096;
  const hobject_t head(object_t("rbd_data.7f1cb2a3d4e5.0000000000000001"), "",
                       CEPH_NOSNAP, 0, 1, "");
  ObjectState obs(object_info_t(head), true);
  obs.oi.size = 4 * width;
  SnapSet snapset;
  auto write = [](uint64_t off, uint64_t len, int op = CEPH_OSD_OP_WRITE) {
    std::vector<OSDOp> ops(1);
    ops[0].op.op = op;
    ops[0].op.extent.offset = off;
    ops[0].op.extent.length = len;
    ops[0].indata.append(std::string(len, 'x'));
    return ops;
  };
  using ECWriteJournal::candidate_write;
  EXPECT_TRUE(candidate_write(head, write(4096, 4096), obs, snapset, 0, width));
  EXPECT_TRUE(candidate_write(head, write(1024, 1024), obs, snapset, 0, width));
  EXPECT_TRUE(candidate_write(head, write(width, width), obs, snapset, 0, width));
  // A write of a whole (one-stripe) object sets a data digest.
  ObjectState small = obs;
  small.oi.size = 8192;
  EXPECT_FALSE(candidate_write(head, write(0, 8192), small, snapset, 0, width));
  EXPECT_TRUE(candidate_write(head, write(0, 4096), small, snapset, 0, width));
  // Partial blocks, a stripe boundary, past EOF, empty.
  EXPECT_FALSE(candidate_write(head, write(512, 1024), obs, snapset, 0, width));
  EXPECT_FALSE(candidate_write(head, write(1024, 1536), obs, snapset, 0, width));
  EXPECT_FALSE(candidate_write(head, write(width - 1024, 2048), obs, snapset, 0, width));
  EXPECT_FALSE(candidate_write(head, write(4 * width - 1024, 2048), obs, snapset, 0, width));
  EXPECT_FALSE(candidate_write(head, write(0, 0), obs, snapset, 0, width));
  // Other ops, or more than one.
  EXPECT_FALSE(candidate_write(head, write(0, 4096, CEPH_OSD_OP_WRITEFULL),
                               obs, snapset, 0, width));
  auto two = write(0, 4096);
  two.push_back(write(8192, 4096).front());
  EXPECT_FALSE(candidate_write(head, two, obs, snapset, 0, width));
  auto truncating = write(0, 4096);
  truncating[0].op.extent.truncate_seq = 1;
  EXPECT_FALSE(candidate_write(head, truncating, obs, snapset, 0, width));
  auto short_data = write(0, 4096);
  short_data[0].indata.clear();
  EXPECT_FALSE(candidate_write(head, short_data, obs, snapset, 0, width));
  // Snapshot state: a snap context, a snapset, clones, a clone object.
  EXPECT_FALSE(candidate_write(head, write(0, 4096), obs, snapset, 5, width));
  SnapSet with_seq;
  with_seq.seq = 3;
  EXPECT_FALSE(candidate_write(head, write(0, 4096), obs, with_seq, 0, width));
  SnapSet with_clone;
  with_clone.clones.push_back(snapid_t(2));
  EXPECT_FALSE(candidate_write(head, write(0, 4096), obs, with_clone, 0, width));
  hobject_t clone = head;
  clone.snap = snapid_t(2);
  EXPECT_FALSE(candidate_write(clone, write(0, 4096), obs, snapset, 0, width));
  // Missing, whiteout.
  ObjectState missing(object_info_t(head), false);
  missing.oi.size = obs.oi.size;
  EXPECT_FALSE(candidate_write(head, write(0, 4096), missing, snapset, 0, width));
  ObjectState whiteout = obs;
  whiteout.oi.set_flag(object_info_t::FLAG_WHITEOUT);
  EXPECT_FALSE(candidate_write(head, write(0, 4096), whiteout, snapset, 0, width));
}

TEST(JournalAdmission, MaterializedAttrCoversStripesAndBase)
{
  ECWriteJournal::Materialized m;
  m.base = eversion_t(3, 10);
  m.stripes[32768] = eversion_t(3, 20);
  ceph::bufferlist bl;
  encode(m, bl);
  ECWriteJournal::Materialized decoded;
  auto p = bl.cbegin();
  decode(decoded, p);
  EXPECT_EQ(m.base, decoded.base);
  EXPECT_EQ(m.stripes, decoded.stripes);
  EXPECT_TRUE(decoded.covers(0, eversion_t(3, 10)));      // at or below base
  EXPECT_FALSE(decoded.covers(0, eversion_t(3, 11)));     // stripe 0 unflushed
  EXPECT_TRUE(decoded.covers(32768, eversion_t(3, 20)));  // flushed stripe
  EXPECT_FALSE(decoded.covers(32768, eversion_t(3, 21))); // appended after it
  EXPECT_FALSE(decoded.covers(65536, eversion_t(3, 15)));
}

// The generator logs through a prefix provider; give it the test's context.
struct FlushTestDpp : public DoutPrefixProvider {
  std::ostream& gen_prefix(std::ostream& out) const override { return out; }
  CephContext* get_cct() const override { return g_ceph_context; }
  unsigned get_subsys() const override { return ceph_subsys_osd; }
} flush_test_dpp;

class JournalFlushTest : public testing::TestWithParam<unsigned> {
 protected:
  uint64_t width() const { return GetParam() * 4096; }
  static object_info_t projected_oi(uint64_t size) {
    object_info_t oi;
    oi.size = size;
    return oi;
  }
  bool eligible(const hobject_t& object,
                const PGTransaction::ObjectOperation& update, uint64_t new_size,
                const SnapSet& ss = SnapSet{}) {
    return ECWriteJournal::eligible_overwrite(object, update, projected_oi(new_size),
                                              ss, width());
  }
  PGTransaction::ObjectOperation head_write(uint64_t size, uint64_t len = 1024) {
    PGTransaction::ObjectOperation update;
    object_info_t oi;
    oi.size = size;
    ceph::bufferlist attr;
    encode(oi, attr, 0);
    update.attr_updates[OI_ATTR] = attr;
    attr.clear();
    encode(SnapSet{}, attr);
    update.attr_updates[SS_ATTR] = attr;
    ceph::bufferlist data;
    data.append(std::string(len, 'x'));
    update.buffer_updates.insert(0, len,
      PGTransaction::ObjectOperation::BufferUpdate::Write{data, 0});
    return update;
  }
  ECWriteJournal::Stripe stripe(uint64_t offset = 0, uint64_t tail = 0) {
    ECWriteJournal::Stripe s;
    s.object = hobject_t(object_t("data"), "", CEPH_NOSNAP, 0, 1, "");
    s.offset = offset;
    s.width = width();
    s.object_size = tail ? offset + tail : width() * 4;
    s.valid_bytes = tail ? tail : width();
    return s;
  }
  void write(ECWriteJournal::Stripe& stripe, uint64_t offset, uint64_t length) {
    for (uint64_t off = offset; off < offset + length; off += 1024) {
      ceph::bufferlist b;
      b.append(std::string(1024, 'x'));
      stripe.blocks[off] = b;
    }
  }
  uint64_t reads(const ECTransaction::WritePlanObj& plan) {
    uint64_t bytes = 0;
    if (plan.to_read) {
      for (const auto& [shard, extents] : *plan.to_read) {
        bytes += extents.size();
      }
    }
    return bytes;
  }
  void verify(ECWriteJournal::Stripe stripe, uint64_t expected_reads) {
    pg_pool_t pool;
    pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
    ECUtil::stripe_info_t sinfo(GetParam(), 3, width(), &pool);
    shard_id_set shards;
    shards.insert_range(shard_id_t(0), GetParam() + 3);
    // The conventional plan, whose data this checks against a reference;
    // plans the planner may turn into parity-delta writes are tested below.
    auto plan = ECWriteJournal::plan_flush(stripe, sinfo, shards, false, 1);
    EXPECT_FALSE(plan.do_parity_delta_write);
    EXPECT_EQ(stripe.object_size, plan.orig_size);
    EXPECT_EQ(stripe.object_size, plan.projected_size);
    EXPECT_EQ(expected_reads, reads(plan));
    // Only the changed data chunks and parity are written.
    std::set<uint64_t> changed;
    for (const auto& [off, data] : stripe.blocks) {
      changed.insert(off / 4096);
    }
    EXPECT_EQ(changed.size() + 3, plan.will_write.shard_count());

    // Emulate exactly the planner-requested base reads, including page padding.
    ECUtil::shard_extent_map_t base(&sinfo);
    if (plan.to_read) {
      for (const auto& [shard, extents] : *plan.to_read) {
        for (const auto& [off, len] : extents) {
          ceph::bufferlist bytes;
          bytes.append(std::string(len, 'z'));
          base.insert_in_shard(shard, off, bytes);
        }
      }
    }
    auto data = ECWriteJournal::assemble_flush(stripe, sinfo, base);
    std::string expected(stripe.valid_bytes, 'z');
    for (const auto& [off, bytes] : stripe.blocks) {
      expected.replace(off, bytes.length(), bytes.to_str());
    }
    expected.resize(width(), '\0');
    std::string got;
    for (unsigned raw = 0; raw < GetParam(); ++raw) {
      ceph::bufferlist chunk;
      data.get_buffer(sinfo.get_shard(raw_shard_id_t(raw)),
                      stripe.offset / GetParam(), 4096, chunk);
      EXPECT_EQ(4096u, chunk.length());
      got += chunk.to_str();
    }
    EXPECT_EQ(expected, got);
  }
};

TEST_P(JournalFlushTest, OneKNeedsPartialPageAndOtherDataChunks)
{
  auto s = stripe(width());
  write(s, 1024, 1024);
  verify(std::move(s), width());
}

TEST_P(JournalFlushTest, FourKNeedsOnlyOtherDataChunks)
{
  auto s = stripe(width());
  write(s, 4096, 4096);
  verify(std::move(s), width() - 4096);
}

TEST_P(JournalFlushTest, FullyCoveredStripeNeedsNoReads)
{
  for (unsigned size : {1024u, 4096u}) {
    auto s = stripe(width() * 2);
    for (uint64_t off = 0; off < width(); off += size) {
      write(s, off, size);
    }
    verify(std::move(s), 0);
  }
}

TEST_P(JournalFlushTest, UnalignedFourKPreservesBothPartialPages)
{
  // Admission permits 1 KiB alignment, not just page-aligned 4 KiB writes.
  // Exercise both ends of a data chunk and nonzero logical stripe offsets.
  for (uint64_t off = 1024; off + 4096 <= width(); off += 4096) {
    auto s = stripe(width());
    write(s, off, 4096);
    verify(std::move(s), width());
  }
}

TEST_P(JournalFlushTest, MetadataOnlyPlanNeedsNoReadsOrDataWrites)
{
  auto s = stripe();
  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(GetParam(), 3, width(), &pool);
  shard_id_set shards;
  shards.insert_range(shard_id_t(0), GetParam() + 3);
  object_info_t oi;
  oi.size = s.object_size;
  auto update = head_write(oi.size);
  ASSERT_TRUE(eligible(s.object, update, oi.size));
  ASSERT_TRUE(ECWriteJournal::overwrite_fits(0, 1024, oi.size, oi.size));
  // The real foreground hook keeps BOTH finish_ctx attrs and removes the data.
  update.buffer_updates.clear();
  ECTransaction::WritePlanObj plan(s.object, update, sinfo, shards, shards,
    false, oi.size, oi, std::nullopt, 1);
  EXPECT_EQ(0u, reads(plan));
  EXPECT_TRUE(plan.will_write.empty());
  EXPECT_EQ(oi.size, plan.orig_size);
  EXPECT_EQ(oi.size, plan.projected_size);
}

TEST_P(JournalFlushTest, RealHeadAttrsEnrollButCompoundAndSnapshotWritesDoNot)
{
  const auto object = stripe().object;
  for (auto size : {1024u, 4096u}) {
    auto update = head_write(width(), size);
    EXPECT_TRUE(eligible(object, update, width()));
    // Structural only: the size relation is checked at admission time.
    EXPECT_TRUE(eligible(object, update, width() + 4096));
    EXPECT_FALSE(eligible(object, update, size - 1)); // write past new EOF
    update.attr_updates.erase(SS_ATTR);
    EXPECT_FALSE(eligible(object, update, width()));
    update = head_write(width(), size);
    update.attr_updates["user-attr"] = ceph::bufferlist{};
    EXPECT_FALSE(eligible(object, update, width()));
    update = head_write(width(), size);
    update.truncate = std::make_pair(uint64_t{0}, uint64_t{0});
    EXPECT_FALSE(eligible(object, update, width()));
    update = head_write(width(), size);
    SnapSet ss;
    ss.seq = 1;
    EXPECT_FALSE(eligible(object, update, width(), ss));
    // A whole-object digest would be compared by scrub before the flush.
    auto digest = projected_oi(width());
    digest.set_data_digest(0x1234);
    EXPECT_FALSE(ECWriteJournal::eligible_overwrite(object, update, digest,
                                                    SnapSet{}, width()));
  }
}

TEST_P(JournalFlushTest, LoneBlockFlushIsAParityDeltaWriteOnWideStripes)
{
  // A flush costs what a classic write of the same blocks would: for one
  // 4 KiB block the planner reads either the old chunk and the three parity
  // chunks (parity delta) or the other k-1 data chunks, whichever is fewer,
  // and writes only the changed chunk and parity.
  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(GetParam(), 3, width(), &pool);
  shard_id_set shards;
  shards.insert_range(shard_id_t(0), GetParam() + 3);
  auto s = stripe(width());
  write(s, 4096, 4096);
  auto plan = ECWriteJournal::plan_flush(s, sinfo, shards);
  EXPECT_EQ(GetParam() > 4, plan.do_parity_delta_write);
  ASSERT_TRUE(plan.to_read);
  EXPECT_EQ(GetParam() > 4 ? 4u : GetParam() - 1u, plan.to_read->shard_count());
  EXPECT_EQ(4u, plan.will_write.shard_count());
  // The extent cache already holds the object: never parity delta, as for a
  // client write.
  EXPECT_FALSE(ECWriteJournal::plan_flush(s, sinfo, shards, true)
                 .do_parity_delta_write);
}

TEST_P(JournalFlushTest, CompleteStripeFlushNeedsNoReadsAndWritesEveryShard)
{
  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(GetParam(), 3, width(), &pool);
  shard_id_set shards;
  shards.insert_range(shard_id_t(0), GetParam() + 3);
  auto s = stripe(width());
  write(s, 0, width());
  auto plan = ECWriteJournal::plan_flush(s, sinfo, shards);
  EXPECT_FALSE(plan.do_parity_delta_write);
  EXPECT_EQ(0u, reads(plan));
  EXPECT_EQ(GetParam() + 3, plan.will_write.shard_count());
}

TEST_P(JournalFlushTest, MergedWritesEnrollOnlyWithinOneStripe)
{
  // librbd's default scheduler merges adjacent 4 KiB writes to one object.
  const auto object = stripe().object;
  const uint64_t size = 2 * width();
  auto at = [&](uint64_t off, uint64_t len) {
    auto update = head_write(size, len);
    update.buffer_updates.clear();
    ceph::bufferlist data;
    data.append(std::string(len, 'x'));
    update.buffer_updates.insert(off, len,
      PGTransaction::ObjectOperation::BufferUpdate::Write{data, 0});
    return eligible(object, update, size);
  };
  EXPECT_TRUE(at(4096, 8192));
  EXPECT_TRUE(at(width() - 8192, 8192));
  EXPECT_TRUE(at(1024, 3072));
  EXPECT_TRUE(at(width(), width())); // a whole stripe in one record
  EXPECT_FALSE(at(width() - 4096, 8192)); // spans two stripes
  EXPECT_FALSE(at(0, 1536));              // not whole blocks
  EXPECT_FALSE(at(0, 2 * width()));
}

TEST_P(JournalFlushTest, AdmissionSizeCheckUsesPlannerViewOfOldSize)
{
  // Submission saw size S in the OBC. By admission a started truncate to S2
  // (or an extension) has changed the planner's projected size.
  const uint64_t S = 2 * width();
  const uint64_t S2 = width();
  EXPECT_TRUE(ECWriteJournal::overwrite_fits(S - 4096, 4096, S, S));
  EXPECT_TRUE(ECWriteJournal::overwrite_fits(S - 1024, 1024, S, S));
  // Same write with the object truncated underneath: now an extension.
  EXPECT_FALSE(ECWriteJournal::overwrite_fits(S - 4096, 4096, S, S2));
  // Any mismatch between this write's projected size and the planner's old
  // size means a size change this write did not account for: not a record.
  EXPECT_FALSE(ECWriteJournal::overwrite_fits(0, 4096, S, S2));
  // Extension by this write itself.
  EXPECT_FALSE(ECWriteJournal::overwrite_fits(S, 4096, S + 4096, S));
  // Write ending exactly at EOF is fine; one byte past is not.
  EXPECT_TRUE(ECWriteJournal::overwrite_fits(S - 1024, 1024, S, S));
  EXPECT_FALSE(ECWriteJournal::overwrite_fits(S - 1023, 1024, S, S));
}

TEST_P(JournalFlushTest, DeferredWriteAndTruncateKeepSubmittedSizes)
{
  const auto object = stripe().object;
  PGTransaction t;
  auto obc = std::make_shared<ObjectContext>();
  t.obc_map[object] = obc;
  obc->obs.oi.size = 2 * width();
  const auto write_oi = ECTransaction::snapshot_object_info(t);
  obc->obs.oi.size = 1024;
  const auto truncate_oi = ECTransaction::snapshot_object_info(t);
  // A newer client reextends the live OBC while both older ops are queued.
  obc->obs.oi.size = 4 * width();
  auto update = head_write(2 * width());
  update.buffer_updates.clear(); // a metadata-only op
  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(GetParam(), 3, width(), &pool);
  shard_id_set shards;
  shards.insert_range(shard_id_t(0), GetParam() + 3);
  ECTransaction::WritePlanObj write_plan(object, update, sinfo, shards, shards,
    false, 2 * width(), write_oi.at(object), std::nullopt, 1);
  EXPECT_EQ(2 * width(), write_plan.projected_size);
  EXPECT_EQ(0u, reads(write_plan));
  EXPECT_TRUE(write_plan.will_write.empty());
  update.truncate = std::make_pair(uint64_t{1024}, uint64_t{1024});
  ECTransaction::WritePlanObj truncate_plan(object, update, sinfo, shards, shards,
    false, write_plan.projected_size, truncate_oi.at(object), std::nullopt, 1);
  EXPECT_EQ(2 * width(), truncate_plan.orig_size);
  EXPECT_EQ(1024u, truncate_plan.projected_size);
  EXPECT_TRUE(truncate_plan.invalidates_cache);
  EXPECT_GT(reads(truncate_plan), 0u);
}

TEST_P(JournalFlushTest, QueuedFlushesPreserveMaterializedStripes)
{
  const auto object = stripe().object;
  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  pool.nonprimary_shards.insert_range(shard_id_t(1), GetParam() - 1);
  ECUtil::stripe_info_t sinfo(GetParam(), 3, width(), &pool);
  shard_id_set shards;
  shards.insert_range(shard_id_t(0), GetParam() + 3);
  auto osdmap = std::make_shared<OSDMap>();
  ErasureCodeInterfaceRef no_encoding;
  auto obc = std::make_shared<ObjectContext>();
  obc->obs.exists = true;
  obc->obs.oi.soid = object;
  obc->obs.oi.size = 4 * width();
  obc->obs.oi.version = eversion_t(1, 20);
  bufferlist cached;
  obc->obs.oi.encode(cached, 0);
  obc->attr_cache[OI_ATTR] = cached;
  const std::string marker(ECWriteJournal::materialized_attr);
  ECWriteJournal::Materialized initial{eversion_t(1, 5),
    {{2 * width(), eversion_t(1, 9)}}};
  bufferlist encoded_initial;
  encode(initial, encoded_initial);
  obc->attr_cache[marker] = encoded_initial;

  std::array<PGTransaction, 2> queued;
  for (size_t index = 0; index < queued.size(); ++index) {
    auto& transaction = queued[index];
    transaction.ec_journal_flush = true;
    transaction.obc_map[object] = obc;
    auto& update = transaction.op_map[object];
    update = head_write(obc->obs.oi.size, 4096);
    update.buffer_updates.clear();
    object_info_t next = obc->obs.oi;
    next.version = eversion_t(1, 21 + index);
    next.prior_version = eversion_t(1, 20 + index);
    bufferlist encoded;
    next.encode(encoded, 0);
    update.attr_updates[OI_ATTR] = encoded;
    ECWriteJournal::Materialized materialized;
    materialized.stripes[index * width()] = eversion_t(1, 11 + index);
    bufferlist encoded_marker;
    encode(materialized, encoded_marker);
    transaction.setattr(object, marker, encoded_marker);
  }

  for (size_t index = 0; index < queued.size(); ++index) {
    auto& transaction = queued[index];
    ECTransaction::WritePlan plans;
    plans.want_read = false;
    plans.plans.emplace_back(object, transaction.op_map.at(object), sinfo,
      shards, shards, false, obc->obs.oi.size, obc->obs.oi, std::nullopt, 1);
    std::vector<pg_log_entry_t> entries;
    entries.emplace_back(pg_log_entry_t::MODIFY, object,
      eversion_t(1, 21 + index), eversion_t(1, 20 + index), 0,
      osd_reqid_t(), utime_t(), 0);
    shard_id_map<ceph::os::Transaction> transactions(GetParam() + 3);
    for (auto shard : shards) {
      transactions[shard];
    }
    std::map<hobject_t, ECUtil::shard_extent_map_t> written;
    std::set<hobject_t> temp_added, temp_removed;
    bool first_in_interval = false;
    ECOmapJournal ec_omap_journal(flush_test_dpp);
    PGLog pg_log(g_ceph_context);
    ECTransaction::generate_transactions(&transaction, plans, no_encoding,
      pg_t(1, 9), sinfo, {}, entries, &written, &transactions, &temp_added,
      &temp_removed, &flush_test_dpp, osdmap, first_in_interval,
      ec_omap_journal, pg_log);
  }
  ECWriteJournal::Materialized materialized;
  auto cursor = obc->attr_cache.at(marker).cbegin();
  decode(materialized, cursor);
  EXPECT_EQ(initial.base, materialized.base);
  EXPECT_TRUE(materialized.covers(0, eversion_t(1, 11)));
  EXPECT_TRUE(materialized.covers(width(), eversion_t(1, 12)));
  EXPECT_TRUE(materialized.covers(2 * width(), eversion_t(1, 9)));
}

TEST_P(JournalFlushTest, FlushMarkerBaseDropsTheEntriesItCovers)
{
  // A flush whose object has no live record older than (1,10) on another
  // stripe carries base (1,9): the merged attr keeps only newer entries, and
  // the last flush of an object with nothing else live empties the map.
  const auto object = stripe().object;
  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  pool.nonprimary_shards.insert_range(shard_id_t(1), GetParam() - 1);
  ECUtil::stripe_info_t sinfo(GetParam(), 3, width(), &pool);
  shard_id_set shards;
  shards.insert_range(shard_id_t(0), GetParam() + 3);
  auto osdmap = std::make_shared<OSDMap>();
  ErasureCodeInterfaceRef no_encoding;
  auto obc = std::make_shared<ObjectContext>();
  obc->obs.exists = true;
  obc->obs.oi.soid = object;
  obc->obs.oi.size = 4 * width();
  obc->obs.oi.version = eversion_t(1, 20);
  bufferlist cached;
  obc->obs.oi.encode(cached, 0);
  obc->attr_cache[OI_ATTR] = cached;
  const std::string marker(ECWriteJournal::materialized_attr);
  const ECWriteJournal::Materialized initial{eversion_t(1, 5),
    {{0, eversion_t(1, 6)}, {width(), eversion_t(1, 8)},
     {2 * width(), eversion_t(1, 12)}}};
  bufferlist encoded_initial;
  encode(initial, encoded_initial);
  obc->attr_cache[marker] = encoded_initial;

  const std::array<ECWriteJournal::Materialized, 2> markers{{
    {ECWriteJournal::just_below(eversion_t(1, 10)), {{3 * width(), eversion_t(1, 14)}}},
    {eversion_t(1, 22), {{2 * width(), eversion_t(1, 15)}}}}};
  for (size_t index = 0; index < markers.size(); ++index) {
    PGTransaction transaction;
    transaction.ec_journal_flush = true;
    transaction.obc_map[object] = obc;
    auto& update = transaction.op_map[object];
    update = head_write(obc->obs.oi.size, 4096);
    update.buffer_updates.clear();
    object_info_t next = obc->obs.oi;
    next.version = eversion_t(1, 21 + index);
    next.prior_version = eversion_t(1, 20 + index);
    bufferlist encoded;
    next.encode(encoded, 0);
    update.attr_updates[OI_ATTR] = encoded;
    bufferlist encoded_marker;
    encode(markers[index], encoded_marker);
    transaction.setattr(object, marker, encoded_marker);
    ECTransaction::WritePlan plans;
    plans.want_read = false;
    plans.plans.emplace_back(object, transaction.op_map.at(object), sinfo,
      shards, shards, false, obc->obs.oi.size, obc->obs.oi, std::nullopt, 1);
    std::vector<pg_log_entry_t> entries;
    entries.emplace_back(pg_log_entry_t::MODIFY, object,
      eversion_t(1, 21 + index), eversion_t(1, 20 + index), 0,
      osd_reqid_t(), utime_t(), 0);
    shard_id_map<ceph::os::Transaction> transactions(GetParam() + 3);
    for (auto shard : shards) {
      transactions[shard];
    }
    std::map<hobject_t, ECUtil::shard_extent_map_t> written;
    std::set<hobject_t> temp_added, temp_removed;
    bool first_in_interval = false;
    ECOmapJournal ec_omap_journal(flush_test_dpp);
    PGLog pg_log(g_ceph_context);
    ECTransaction::generate_transactions(&transaction, plans, no_encoding,
      pg_t(1, 9), sinfo, {}, entries, &written, &transactions, &temp_added,
      &temp_removed, &flush_test_dpp, osdmap, first_in_interval,
      ec_omap_journal, pg_log);

    ECWriteJournal::Materialized materialized;
    auto cursor = obc->attr_cache.at(marker).cbegin();
    decode(materialized, cursor);
    if (index == 0) {
      EXPECT_EQ(eversion_t(1, 9), materialized.base);
      const std::map<uint64_t, eversion_t> kept{{2 * width(), eversion_t(1, 12)},
                                                {3 * width(), eversion_t(1, 14)}};
      EXPECT_EQ(kept, materialized.stripes);
      for (const auto& [offset, version] : initial.stripes) {
        EXPECT_TRUE(materialized.covers(offset, version)) << offset;
      }
      EXPECT_FALSE(materialized.covers(0, eversion_t(1, 10)));
    } else {
      EXPECT_EQ(eversion_t(1, 22), materialized.base);
      EXPECT_TRUE(materialized.stripes.empty());
    }
  }
}

// The transaction generator encodes parity for every data write, and these
// tests load no erasure code plugin. Zero parity is enough to emit a client
// write's shard transactions; nothing here checks parity contents.
class ZeroParityCode : public ceph::ErasureCodeInterface {
 public:
  ZeroParityCode(unsigned k, unsigned m) : k(k), m(m) {}
  int init(ceph::ErasureCodeProfile&, std::ostream*) override { return 0; }
  const ceph::ErasureCodeProfile& get_profile() const override {
    return profile;
  }
  int create_rule(const std::string&, CrushWrapper&,
                  std::ostream*) const override {
    return -EOPNOTSUPP;
  }
  unsigned int get_chunk_count() const override { return k + m; }
  unsigned int get_data_chunk_count() const override { return k; }
  unsigned int get_coding_chunk_count() const override { return m; }
  int get_sub_chunk_count() override { return 1; }
  unsigned int get_chunk_size(unsigned int stripe_width) const override {
    return stripe_width / k;
  }
  size_t get_minimum_granularity() override { return 1; }
  plugin_flags get_supported_optimizations() const override {
    return FLAG_EC_PLUGIN_PARTIAL_READ_OPTIMIZATION |
      FLAG_EC_PLUGIN_PARTIAL_WRITE_OPTIMIZATION |
      FLAG_EC_PLUGIN_ZERO_INPUT_ZERO_OUTPUT_OPTIMIZATION |
      FLAG_EC_PLUGIN_ZERO_PADDING_OPTIMIZATION |
      FLAG_EC_PLUGIN_OPTIMIZED_SUPPORTED;
  }
  const std::vector<shard_id_t>& get_chunk_mapping() const override {
    return mapping;
  }
  int encode_chunks(const shard_id_map<bufferptr>&,
                    shard_id_map<bufferptr>& out) override {
    for (auto&& [shard, parity] : out) {
      parity.zero();
    }
    return 0;
  }
  // Nothing below is used to generate a conventional (not parity-delta)
  // write.
  int minimum_to_decode(const shard_id_set&, const shard_id_set&,
      shard_id_set&) override {
    return -EOPNOTSUPP;
  }
  int minimum_to_decode(const std::set<int>&, const std::set<int>&,
      std::map<int, std::vector<std::pair<int, int>>>*) override {
    return -EOPNOTSUPP;
  }
  int minimum_to_decode_with_cost(const shard_id_set&,
      const shard_id_map<int>&, shard_id_set*) override {
    return -EOPNOTSUPP;
  }
  int minimum_to_decode_with_cost(const std::set<int>&,
      const std::map<int, int>&, std::set<int>*) override {
    return -EOPNOTSUPP;
  }
  int encode(const shard_id_set&, const bufferlist&,
             shard_id_map<bufferlist>*) override {
    return -EOPNOTSUPP;
  }
  int encode(const std::set<int>&, const bufferlist&,
             std::map<int, bufferlist>*) override {
    return -EOPNOTSUPP;
  }
  int encode_chunks(const std::set<int>&, std::map<int, bufferlist>*) override {
    return -EOPNOTSUPP;
  }
  void encode_delta(const bufferptr&, const bufferptr&, bufferptr*) override {
    ADD_FAILURE() << "parity-delta write";
  }
  void apply_delta(const shard_id_map<bufferptr>&,
                   shard_id_map<bufferptr>&) override {
    ADD_FAILURE() << "parity-delta write";
  }
  int decode(const shard_id_set&, const shard_id_map<bufferlist>&,
             shard_id_map<bufferlist>*, int) override {
    return -EOPNOTSUPP;
  }
  int decode(const std::set<int>&, const std::map<int, bufferlist>&,
             std::map<int, bufferlist>*, int) override {
    return -EOPNOTSUPP;
  }
  int decode_chunks(const shard_id_set&, shard_id_map<bufferptr>&,
                    shard_id_map<bufferptr>&) override {
    return -EOPNOTSUPP;
  }
  int decode_chunks(const std::set<int>&, const std::map<int, bufferlist>&,
                    std::map<int, bufferlist>*) override {
    return -EOPNOTSUPP;
  }
  int decode_concat(const std::set<int>&, const std::map<int, bufferlist>&,
                    bufferlist*) override {
    return -EOPNOTSUPP;
  }
  int decode_concat(const std::map<int, bufferlist>&, bufferlist*) override {
    return -EOPNOTSUPP;
  }

 private:
  const unsigned k, m;
  ceph::ErasureCodeProfile profile;
  std::vector<shard_id_t> mapping; // none: shard n is raw shard n
};

TEST_P(JournalFlushTest, DirectWriteBesideLiveRecordsMarksOnlyItsStripe)
{
  // A whole-stripe write, or one that finds the log full, goes the ordinary
  // EC way although other stripes of the object are journaled. Its marker
  // names only the stripes it writes, with no base, and the generator merges
  // it into the attr as it merges a flush's: base and the other stripes'
  // entries stay, and a record still journaled on another stripe stays
  // uncovered. Setting base to the write's version would let replay take
  // that record for materialized.
  const hobject_t object(object_t("rbd_data.7f1cb2a3d4e5.0000000000000002"), "",
                         CEPH_NOSNAP, 0, 1, "");
  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  pool.nonprimary_shards.insert_range(shard_id_t(1), GetParam() - 1);
  ECUtil::stripe_info_t sinfo(GetParam(), 3, width(), &pool);
  shard_id_set shards;
  shards.insert_range(shard_id_t(0), GetParam() + 3);
  auto osdmap = std::make_shared<OSDMap>();
  ErasureCodeInterfaceRef zero_parity =
    std::make_shared<ZeroParityCode>(GetParam(), 3);
  auto obc = std::make_shared<ObjectContext>();
  obc->obs.exists = true;
  obc->obs.oi.soid = object;
  obc->obs.oi.size = 4 * width();
  obc->obs.oi.version = eversion_t(1, 20);
  bufferlist cached;
  obc->obs.oi.encode(cached, 0);
  obc->attr_cache[OI_ATTR] = cached;
  // An earlier direct write at (1,5), then a flush of stripe 2's records up
  // to (1,9); a record of stripe 0 at (1,6) is still journaled.
  const std::string marker(ECWriteJournal::materialized_attr);
  ECWriteJournal::Materialized initial{eversion_t(1, 5),
    {{2 * width(), eversion_t(1, 9)}}};
  bufferlist encoded_initial;
  encode(initial, encoded_initial);
  obc->attr_cache[marker] = encoded_initial;

  PGTransaction transaction;
  transaction.ec_journal_marker_delta = true;
  transaction.obc_map[object] = obc;
  auto& update = transaction.op_map[object];
  update = head_write(obc->obs.oi.size, 4096);
  update.buffer_updates.clear();
  ceph::bufferlist data;
  data.append(std::string(4096, 'x'));
  update.buffer_updates.insert(width(), 4096,
    PGTransaction::ObjectOperation::BufferUpdate::Write{data, 0});
  object_info_t next = obc->obs.oi;
  next.version = eversion_t(1, 21);
  next.prior_version = eversion_t(1, 20);
  bufferlist encoded;
  next.encode(encoded, 0);
  update.attr_updates[OI_ATTR] = encoded;
  ECWriteJournal::Materialized delta;
  delta.stripes[width()] = eversion_t(1, 21);
  bufferlist encoded_delta;
  encode(delta, encoded_delta);
  transaction.setattr(object, marker, encoded_delta);

  ECTransaction::WritePlan plans;
  plans.plans.emplace_back(object, update, sinfo, shards, shards, false,
    obc->obs.oi.size, obc->obs.oi, std::nullopt, 1);
  const auto& plan = plans.plans.back();
  // A partial write of stripe 1: its first data chunk and parity, after a
  // read of the other data chunks (conventional, as pdw mode 1 forces).
  ASSERT_TRUE(plan.to_read);
  EXPECT_EQ(GetParam() - 1, plan.to_read->shard_count());
  EXPECT_EQ(4u, plan.will_write.shard_count());
  plans.want_read = true;
  // The RMW read the backend would issue, emulated.
  ECUtil::shard_extent_map_t other_chunks(&sinfo);
  for (const auto& [shard, extents] : *plan.to_read) {
    for (const auto& [off, len] : extents) {
      ceph::bufferlist bytes;
      bytes.append(std::string(len, 'z'));
      other_chunks.insert_in_shard(shard, off, bytes);
    }
  }
  const std::map<hobject_t, ECUtil::shard_extent_map_t> partial_extents{
    {object, other_chunks}};
  std::vector<pg_log_entry_t> entries;
  entries.emplace_back(pg_log_entry_t::MODIFY, object, eversion_t(1, 21),
    eversion_t(1, 20), 0, osd_reqid_t(), utime_t(), 0);
  shard_id_map<ceph::os::Transaction> transactions(GetParam() + 3);
  for (auto shard : shards) {
    transactions[shard];
  }
  std::map<hobject_t, ECUtil::shard_extent_map_t> written;
  std::set<hobject_t> temp_added, temp_removed;
  bool first_in_interval = false;
  ECOmapJournal ec_omap_journal(flush_test_dpp);
  PGLog pg_log(g_ceph_context);
  ECTransaction::generate_transactions(&transaction, plans, zero_parity,
    pg_t(1, 9), sinfo, partial_extents, entries, &written, &transactions,
    &temp_added, &temp_removed, &flush_test_dpp, osdmap, first_in_interval,
    ec_omap_journal, pg_log);
  // The client data went to stripe 1's first chunk.
  ceph::bufferlist chunk;
  written.at(object).get_buffer(sinfo.get_shard(raw_shard_id_t(0)),
                                width() / GetParam(), 4096, chunk);
  EXPECT_EQ(std::string(4096, 'x'), chunk.to_str());

  ECWriteJournal::Materialized materialized;
  auto cursor = obc->attr_cache.at(marker).cbegin();
  decode(materialized, cursor);
  EXPECT_EQ(initial.base, materialized.base);
  EXPECT_TRUE(materialized.covers(width(), eversion_t(1, 21)));
  EXPECT_FALSE(materialized.covers(width(), eversion_t(1, 22))); // journaled later
  EXPECT_TRUE(materialized.covers(2 * width(), eversion_t(1, 9)));
  EXPECT_FALSE(materialized.covers(0, eversion_t(1, 6))); // still live
}

TEST_P(JournalFlushTest, JournalGenerationsUseProductionShardAssembly)
{
  // Unlike the test-side assemble (a reference model), this goes from real
  // append records through begin_flush, plan_flush and assemble_flush on
  // every generation: the same stripe is journaled and materialized three
  // times over its previous physical contents.
  auto object = stripe().object;
  ECWriteJournal::Limits limits;
  limits.stripe_width = width();
  ECWriteJournal::Journal journal(ghobject_t(object), limits);
  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(GetParam(), 3, width(), &pool);
  shard_id_set shards;
  shards.insert_range(shard_id_t(0), GetParam() + 3);
  std::string physical(width(), 'z');
  std::string expected = physical;
  std::mt19937 rng(71);
  for (unsigned generation = 0; generation < 3; ++generation) {
    for (unsigned n = 0; n < 65; ++n) {
      uint64_t off = (rng() % (width() / 1024)) * 1024;
      char value = 'a' + n % 26;
      ceph::bufferlist bytes;
      bytes.append(std::string(1024, value));
      ECWriteJournal::Placement placement;
      ECWriteJournal::Ticket ticket;
      ASSERT_EQ(0, journal.append(object, eversion_t(1, generation * 65 + n + 1),
        width(), off, bytes, &placement, &ticket));
      ASSERT_EQ(0, journal.committed(ticket, 0));
      expected.replace(off, 1024, bytes.to_str());
    }
    // Incomplete after 65 random 1 KiB writes at 8+3/12+3, sometimes complete
    // at 4+3: either way a reclaim/drain flush takes the oldest stripe.
    auto stripe = journal.begin_flush(true);
    ASSERT_TRUE(stripe);
    EXPECT_FALSE(journal.partial_ready()); // the only stripe is flushing
    auto& s = *stripe;
    auto plan = ECWriteJournal::plan_flush(s, sinfo, shards, false, 1);
    ECUtil::shard_extent_map_t base(&sinfo);
    if (plan.to_read) {
      for (const auto& [shard, extents] : *plan.to_read) {
        for (const auto& [off, len] : extents) {
          ceph::bufferlist bytes;
          bytes.append(physical.substr(uint64_t(shard.id) * 4096 + off, len));
          base.insert_in_shard(shard, off, bytes);
        }
      }
    }
    auto data = ECWriteJournal::assemble_flush(s, sinfo, base);
    physical.clear();
    for (unsigned raw = 0; raw < GetParam(); ++raw) {
      ceph::bufferlist chunk;
      data.get_buffer(sinfo.get_shard(raw_shard_id_t(raw)), 0, 4096, chunk);
      physical += chunk.to_str();
    }
    EXPECT_EQ(expected, physical);
    ASSERT_EQ(0, journal.finish_flush(s, 0));
    EXPECT_FALSE(journal.dirty());
    EXPECT_FALSE(journal.begin_flush(true));
  }
  // Everything is on the shards; sealing releases the dead log.
  journal.seal();
  EXPECT_TRUE(journal.empty());
}

TEST_P(JournalFlushTest, TailReadsOnlyExistingPages)
{
  auto s = stripe(width(), 4609);
  write(s, 1024, 1024);
  verify(std::move(s), 8192);
}

TEST_P(JournalFlushTest, FullyCoveredTailNeedsNoReads)
{
  auto s = stripe(width(), 4096);
  write(s, 0, 4096);
  verify(std::move(s), 0);
}

TEST_P(JournalFlushTest, SparseFragmentsPreserveUntouchedBytes)
{
  auto s = stripe();
  write(s, 0, 1024);
  write(s, 2048, 1024);
  write(s, 8192, 4096);
  write(s, width() - 1024, 1024);
  verify(std::move(s), width() - 4096);
}

TEST_P(JournalFlushTest, AbsentBaseRangesAreZeroLikeTheCacheContract)
{
  // The extent cache never reads object holes or regions created by a size
  // extension (do_not_read); ECTransaction::Generate zero-pads them and so
  // must the flush, instead of aborting on a legitimately empty read map.
  auto s = stripe();
  write(s, 0, 1024);
  pg_pool_t pool;
  pool.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
  ECUtil::stripe_info_t sinfo(GetParam(), 3, width(), &pool);
  ECUtil::shard_extent_map_t missing(&sinfo);
  auto data = ECWriteJournal::assemble_flush(s, sinfo, missing);
  std::string expected(width(), '\0');
  expected.replace(0, 1024, std::string(1024, 'x'));
  std::string got;
  for (unsigned raw = 0; raw < GetParam(); ++raw) {
    ceph::bufferlist chunk;
    data.get_buffer(sinfo.get_shard(raw_shard_id_t(raw)), 0, 4096, chunk);
    ASSERT_EQ(4096u, chunk.length());
    got += chunk.to_str();
  }
  EXPECT_EQ(expected, got);
  // Partially supplied base: only the absent part is zero.
  ECUtil::shard_extent_map_t partial(&sinfo);
  ceph::bufferlist bytes;
  bytes.append(std::string(4096, 'z'));
  partial.insert_in_shard(sinfo.get_shard(raw_shard_id_t(1)), 0, bytes);
  data = ECWriteJournal::assemble_flush(s, sinfo, partial);
  expected.replace(4096, 4096, std::string(4096, 'z'));
  got.clear();
  for (unsigned raw = 0; raw < GetParam(); ++raw) {
    ceph::bufferlist chunk;
    data.get_buffer(sinfo.get_shard(raw_shard_id_t(raw)), 0, 4096, chunk);
    got += chunk.to_str();
  }
  EXPECT_EQ(expected, got);
}

INSTANTIATE_TEST_SUITE_P(EC4plus3And8plus3And12plus3, JournalFlushTest,
                        testing::Values(4, 8, 12));
} // anonymous namespace