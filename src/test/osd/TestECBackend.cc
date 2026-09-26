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
#include <sstream>
#include <errno.h>
#include <signal.h>
#include "osd/ECCommon.h"
#include "osd/ECBackend.h"
#include "osd/ECMsgTypes.h"
#include "gtest/gtest.h"
#include "osd/osd_types.h"
#include "common/ceph_argparse.h"
#include "erasure-code/ErasureCode.h"
#include "test/osd/MockErasureCode.h"

using namespace std;

class ECListenerStub : public ECListener {


private:
  OSDMapRef osd_map_ref;
  pg_info_t pg_info;
  set<pg_shard_t> backfill_shards;
  shard_id_set backfill_shard_id_set;
  map<pg_shard_t, pg_missing_t> shard_missing;
  pg_missing_set<false> shard_not_missing_const;
  map<pg_shard_t, pg_info_t> shard_info;
  PGLog pg_log;
  pg_info_t shard_pg_info;
  std::string dbg_prefix = "stub";

public:
  set<pg_shard_t> acting_shards;
  shard_id_set acting_recovery_backfill_shard_id_set;
  // Settable candidate-location map used by get_missing_loc_shards();
  // tests can populate this directly to simulate an object whose
  // pg_missing_loc lists more than one candidate pg_shard for the same
  // hoid (e.g. several historical owners of the same EC shard slot).
  map<hobject_t, set<pg_shard_t>> missing_loc_shards;
  // Settable pg_shard_t set backing get_acting_recovery_backfill_shards();
  // tests that drive RMWPipeline::cache_ready() directly need this (the
  // full pg_shard_t set), not just the shard_id_set below.
  set<pg_shard_t> acting_recovery_backfill_shards;
  // Settable pool used by get_pool(); tests can set flags (e.g. FLAG_OMAP)
  // and nonprimary_shards on it directly.
  pg_pool_t pg_pool;
  // Settable "self" shard used by whoami_shard(); defaults to the old
  // hard-coded pg_shard_t() (relative shard NO_SHARD, zone 0) so existing
  // tests are unaffected. Tests that need to simulate a primary living in
  // a non-zero zone can set this before constructing/using a ReadPipeline.
  pg_shard_t whoami = pg_shard_t();

  ECListenerStub()
    : pg_log(NULL) {}

  const OSDMapRef &pgb_get_osdmap() const override {
    return osd_map_ref;
  }

  epoch_t pgb_get_osdmap_epoch() const override {
    return 0;
  }

  const pg_info_t &get_info() const override {
    return pg_info;
  }

  void cancel_pull(const hobject_t &soid) override {

  }

  pg_shard_t primary_shard() const override {
    return pg_shard_t();
  }

  bool pgb_is_primary() const override {
    return false;
  }

  void on_failed_pull(const set<pg_shard_t> &from, const hobject_t &soid, const eversion_t &v) override {

  }

  void
  on_local_recover(const hobject_t &oid, const ObjectRecoveryInfo &recovery_info, ObjectContextRef obc, bool is_delete,
		   ceph::os::Transaction *t) override {

  }

  void on_global_recover(const hobject_t &oid, const object_stat_sum_t &stat_diff, bool is_delete) override {

  }

  void on_peer_recover(pg_shard_t peer, const hobject_t &oid, const ObjectRecoveryInfo &recovery_info) override {

  }

  void begin_peer_recover(pg_shard_t peer, const hobject_t oid) override {

  }

  bool pg_is_repair() const override {
    return false;
  }

  ObjectContextRef
  get_obc(const hobject_t &hoid, const map<std::string, ceph::buffer::list, std::less<>> &attrs) override {
    return ObjectContextRef();
  }

  bool check_failsafe_full() override {
    return false;
  }

  hobject_t get_temp_recovery_object(const hobject_t &target, eversion_t version) override {
    return hobject_t();
  }

  bool pg_is_remote_backfilling() override {
    return false;
  }

  void pg_add_local_num_bytes(int64_t num_bytes) override {

  }

  void pg_add_num_bytes(int64_t num_bytes) override {

  }

  void inc_osd_stat_repaired() override {

  }

  void add_temp_obj(const hobject_t &oid) override {

  }

  void clear_temp_obj(const hobject_t &oid) override {

  }

  epoch_t get_last_peering_reset_epoch() const override {
    return 0;
  }

  GenContext<ThreadPool::TPHandle &> *bless_unlocked_gencontext(GenContext<ThreadPool::TPHandle &> *c) override {
    return nullptr;
  }

  void schedule_recovery_work(GenContext<ThreadPool::TPHandle &> *c, uint64_t cost) override {

  }

  epoch_t get_interval_start_epoch() const override {
    return 0;
  }

  const set<pg_shard_t> &get_acting_shards() const override {
    return acting_shards;
  }

  const set<pg_shard_t> &get_backfill_shards() const override {
    return backfill_shards;
  }

  const map<hobject_t, std::set<pg_shard_t>> &get_missing_loc_shards() const override {
    return missing_loc_shards;
  }

  const map<pg_shard_t, pg_missing_t> &get_shard_missing() const override {
    return shard_missing;
  }

  const pg_missing_const_i &get_shard_missing(pg_shard_t peer) const override {
    return shard_not_missing_const;
  }

  const pg_missing_const_i *maybe_get_shard_missing(pg_shard_t peer) const override {
    return nullptr;
  }

  const pg_info_t &get_shard_info(pg_shard_t peer) const override {
    return shard_pg_info;
  }

  ceph_tid_t get_tid() override {
    return 0;
  }

  pg_shard_t whoami_shard() const override {
    return whoami;
  }

  void send_message_osd_cluster(vector<std::pair<int, Message *>> &messages, epoch_t from_epoch) override {

  }

  void send_message_osd_cluster(int osd, MOSDPGPush* msg, epoch_t from_epoch) override {

  }

  ostream &gen_dbg_prefix(ostream &out) const override {
    out << dbg_prefix;
    return out;
  }

  const pg_pool_t &get_pool() const override {
    return pg_pool;
  }

  const set<pg_shard_t> &get_acting_recovery_backfill_shards() const override {
    return acting_recovery_backfill_shards;
  }

  const shard_id_set &get_acting_recovery_backfill_shard_id_set() const override {
    return acting_recovery_backfill_shard_id_set;
  }

  // Settable result for should_send_op(); defaults to false to preserve
  // the stub's original hard-coded behaviour for existing tests.
  bool should_send_op_result = false;

  bool should_send_op(pg_shard_t peer, const hobject_t &hoid) override {
    return should_send_op_result;
  }

  const map<pg_shard_t, pg_info_t> &get_shard_info() const override {
    return shard_info;
  }

  spg_t primary_spg_t() const override {
    return spg_t();
  }

  const PGLog &get_log() const override {
    return pg_log;
  }

  DoutPrefixProvider *get_dpp() override {
    return nullptr;
  }

  void apply_stats(const hobject_t &soid, const object_stat_sum_t &delta_stats) override {

  }

  bool is_missing_object(const hobject_t &oid) const override {
    return false;
  }

  void add_local_next_event(const pg_log_entry_t &e) override {

  }

  void log_operation(vector<pg_log_entry_t> &&logv, const optional<pg_hit_set_history_t> &hset_history,
		     const eversion_t &trim_to, const eversion_t &roll_forward_to,
		     const eversion_t &min_last_complete_ondisk, bool transaction_applied, os::Transaction &t,
		     bool async) override {

  }

  void op_applied(const eversion_t &applied_version) override {

  }

  uint64_t min_peer_features() const {
    return 0;
  }
};

namespace {

struct TestDpp : public DoutPrefixProvider {
  std::ostream &gen_prefix(std::ostream &out) const override {
    return out << "TestRMWPipeline";
  }
  CephContext *get_cct() const override { return g_ceph_context; }
  unsigned get_subsys() const override { return ceph_subsys_osd; }
};

// Minimal concrete ECCommon so a test can directly construct a real
// ECCommon::RMWPipeline (which needs an ECCommon& to hand write completions
// to). Only handle_sub_write() is exercised by the tests below (a write
// routed to "ourself" via ECListenerStub::whoami); the read paths are never
// reached because the tests call RMWPipeline::cache_ready() directly instead
// of going through start_rmw()/the extent cache, so ADD_FAILURE() there is
// a trip-wire, not a real implementation.
struct FakeECBackendForRMW : public ECCommon {
  explicit FakeECBackendForRMW(const DoutPrefixProvider &dpp) : ECCommon(dpp) {}

  // Captured from the sub-write, for inspection once cache_ready() returns.
  int sub_writes_seen = 0;
  ObjectStore::Transaction last_sub_write_t;

  void handle_sub_write(pg_shard_t from, OpRequestRef msg, ECSubWrite &op,
                         const ZTracer::Trace &trace,
                         ECListener &eclistener) override {
    ++sub_writes_seen;
    last_sub_write_t = op.t;
  }

  void objects_read_and_reconstruct(
      const std::map<hobject_t, std::list<ec_align_t>> &reads,
      bool fast_read, uint64_t object_size,
      GenContextURef<ec_extents_t &&> &&func) override {
    ADD_FAILURE() << "not used by this test";
  }

  void objects_read_and_reconstruct_for_rmw(
      std::map<hobject_t, read_request_t> &&to_read,
      GenContextURef<ec_extents_t &&> &&func) override {
    ADD_FAILURE() << "not used by this test";
  }

#ifdef WITH_CRIMSON
  void handle_sub_read_n_reply(pg_shard_t from, ECSubRead &op,
                                const ZTracer::Trace &trace) override {
    ADD_FAILURE() << "not used by this test";
  }
#endif
};

// An Op whose generate_transactions() populates relative shard 0's
// Transaction with real content (touch + setattr, so Transaction's
// coll_index/object_index std::map members are non-empty and an extra copy
// of them is actually observable), mirroring the existing ECDummyOp defined
// in ECCommon.cc but with a non-trivial transaction instead of a no-op.
struct ECShard0WriteTestOp final : ECCommon::RMWPipeline::Op {
  explicit ECShard0WriteTestOp(ECCommon::RMWPipeline &rmw_pipeline)
    : Op(rmw_pipeline) {
  }

  void generate_transactions(
      ceph::ErasureCodeInterfaceRef &ec_impl,
      pg_t pgid,
      const ECUtil::stripe_info_t &sinfo,
      map<hobject_t, ECUtil::shard_extent_map_t> *written,
      shard_id_map<ObjectStore::Transaction> *transactions,
      DoutPrefixProvider *dpp,
      const OSDMapRef &osdmap,
      bool &first_write_in_interval,
      ECOmapJournal &ec_omap_journal) override {
    const shard_id_t rel_shard(0);
    ObjectStore::Transaction &t = transactions->at(rel_shard);
    coll_t coll(spg_t(pgid, rel_shard));
    ghobject_t obj(hoid, ghobject_t::NO_GEN, rel_shard);
    t.touch(coll, obj);
    bufferlist bl;
    bl.append("x");
    t.setattr(coll, obj, "_test", bl);
  }

  bool skip_transaction(
      std::set<shard_id_t> &pending_roll_forward,
      shard_id_t shard,
      ObjectStore::Transaction &transaction) override {
    return false;
  }
};

} // namespace

// For a multi-zone pool, RMWPipeline::cache_ready() has to get a non-zone-0
// shard's Transaction into its sub-write remapped from the shard's relative
// id to its absolute one. It does that by handing ECSubWrite's constructor an
// empty transaction and swapping the remapped content in immediately after,
// which avoids a redundant second deep copy of the transaction. This test
// pins down the observable half of that: the sub-write must still come out
// carrying the shard's real content, addressed to the absolute shard. If the
// swap were dropped the sub-write would go out empty; if the remap were
// dropped it would be addressed to the wrong shard. Either would be silent
// data loss, so it is worth a test even though the copy count itself is not
// observable from here.
TEST(ECCommon, cache_ready_remaps_transaction_into_sub_write_for_remapped_shard)
{
  const unsigned int k = 4;
  const unsigned int m = 2;
  const uint64_t swidth = EC_ALIGN_SIZE * k;

  pg_pool_t pool;
  pool.size = 2 * (k + m); // 2 zones, k+m shards each
  pool.opts.set(pool_opts_t::NUM_ZONES, 2);
  ECUtil::stripe_info_t sinfo(k, m, swidth, &pool);

  // Relative shard 0 of zone 1: absolute id k+m, which get_rel_shard() maps
  // back down to relative id 0 -- this is exactly the abs_shard != rel_shard
  // case the defect is in.
  const shard_id_t abs_shard(k + m);
  const pg_shard_t pg_shard(101, abs_shard);

  ECListenerStub listenerStub;
  listenerStub.acting_recovery_backfill_shard_id_set.insert(abs_shard);
  listenerStub.acting_recovery_backfill_shards.insert(pg_shard);
  // Route the write through the "local write" branch (handle_sub_write)
  // rather than the "send an OSD message" branch: the redundant copy under
  // test happens before that split, and the local branch avoids needing to
  // construct/send a real MOSDECSubOpWrite message.
  listenerStub.whoami = pg_shard;
  listenerStub.should_send_op_result = true;

  MockErasureCode *ecode = new MockErasureCode(k, k + m);
  ErasureCodeInterfaceRef ec_impl(ecode);

  TestDpp dpp;
  FakeECBackendForRMW ec_backend(dpp);
  ECExtentCache::LRU lru(0);
  ECCommon::RMWPipeline pipeline(
    g_ceph_context, ec_impl, sinfo, &listenerStub, ec_backend, lru);

  auto op = std::make_shared<ECShard0WriteTestOp>(pipeline);
  op->hoid = hobject_t(sobject_t("double-transaction-copy-test", CEPH_NOSNAP));

  // Call cache_ready() directly (rather than going through start_rmw() and
  // the extent cache) since this write needs no reads: it exercises exactly
  // the code path under test with no further scaffolding required.
  pipeline.cache_ready(*op);

  ASSERT_EQ(ec_backend.sub_writes_seen, 1);
  EXPECT_FALSE(ec_backend.last_sub_write_t.empty())
    << "the remapped shard's sub-write went out with an empty transaction";

  // generate_transactions() above put a touch and a setattr on relative
  // shard 0; both must now be addressed to the absolute shard.
  size_t ops_seen = 0;
  auto i = ec_backend.last_sub_write_t.begin();
  while (i.have_op()) {
    const ObjectStore::Transaction::Op *t_op = i.decode_op();
    ++ops_seen;
    spg_t pgid;
    ASSERT_TRUE(i.get_cid(t_op->cid).is_pg(&pgid));
    EXPECT_EQ(pgid.shard, abs_shard);
    EXPECT_EQ(i.get_oid(t_op->oid).shard_id, abs_shard);
  }
  EXPECT_EQ(ops_seen, 2u);
}

TEST(ECCommon, get_min_want_to_read_shards)
{
  const uint64_t swidth = 4096;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const uint64_t csize = 1024;

  ECUtil::stripe_info_t s(k, m, swidth);
  ECListenerStub listenerStub;
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), csize);

  const std::vector<int> chunk_mapping = {}; // no remapping
  ErasureCodeInterfaceRef ec_impl(new MockErasureCode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  ECUtil::shard_extent_set_t empty_extent_set_map(s.get_k_plus_m());

  // read nothing at the very beginning
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(0, 0, 0);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ASSERT_EQ(want_to_read,  empty_extent_set_map);
  }

  // read nothing at the middle (0-sized partial read)
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(2048, 0, 0);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ASSERT_EQ(want_to_read,  empty_extent_set_map);
  }
  // read nothing at the the second stripe (0-sized partial read)
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(swidth, 0, 0);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ASSERT_EQ(want_to_read,  empty_extent_set_map);
  }

  // read not-so-many (< chunk_size) bytes at the middle (partial read)
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(2048, 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());
    ref[shard_id_t(2)].insert(0, 42);
    ASSERT_EQ(want_to_read, ref);
  }

  // read not-so-many (< chunk_size) bytes after the first stripe.
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(swidth+2048, 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());
    ref[shard_id_t(2)].insert(csize, 42);
    ASSERT_EQ(want_to_read, ref);
  }

  // read more (> chunk_size) bytes at the middle (partial read)
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(csize, csize + 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());
    ref[shard_id_t(1)].insert(0, csize);
    ref[shard_id_t(2)].insert(0, 42);
    ASSERT_EQ(want_to_read, ref);
  }

  // read more (> chunk_size) bytes at the middle (partial read), second stripe
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(swidth + csize, csize + 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());
    ref[shard_id_t(1)].insert(csize, csize);
    ref[shard_id_t(2)].insert(csize, 42);
    ASSERT_EQ(want_to_read, ref);
  }

  // full stripe except last chunk
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(0, 3*csize, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());
    ref[shard_id_t(0)].insert(0, csize);
    ref[shard_id_t(1)].insert(0, csize);
    ref[shard_id_t(2)].insert(0, csize);
    ASSERT_EQ(want_to_read, ref);
  }

  // full stripe except last chunk (second stripe)
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(swidth, 3*csize, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());
    ref[shard_id_t(0)].insert(csize, csize);
    ref[shard_id_t(1)].insert(csize, csize);
    ref[shard_id_t(2)].insert(csize, csize);
    ASSERT_EQ(want_to_read, ref);
  }

  // full stripe except 1st chunk
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(csize, swidth - csize, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());
    ref[shard_id_t(1)].insert(0, csize);
    ref[shard_id_t(2)].insert(0, csize);
    ref[shard_id_t(3)].insert(0, csize);
    ASSERT_EQ(want_to_read, ref);
  }

  // full stripe except 1st chunk (second stripe)
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(swidth + csize, swidth - csize, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());
    ref[shard_id_t(1)].insert(csize, csize);
    ref[shard_id_t(2)].insert(csize, csize);
    ref[shard_id_t(3)].insert(csize, csize);
    ASSERT_EQ(want_to_read, ref);
  }

  // large, multi-stripe read starting just after 1st chunk
  // 0XXX
  // XXXX x41
  // X000
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(csize, swidth * 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());
    ref[shard_id_t(0)].insert(csize, csize*42);
    ref[shard_id_t(1)].insert(0, csize*42);
    ref[shard_id_t(2)].insert(0, csize*42);
    ref[shard_id_t(3)].insert(0, csize*42);
    ASSERT_EQ(want_to_read, ref);
  }

  // large, multi-stripe read starting just after 1st chunk (second stripe)
  // 0XXX
  // XXXX x41
  // X000
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(swidth + csize, swidth * 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());
    ref[shard_id_t(0)].insert(csize*2, csize*42);
    ref[shard_id_t(1)].insert(csize, csize*42);
    ref[shard_id_t(2)].insert(csize, csize*42);
    ref[shard_id_t(3)].insert(csize, csize*42);
    ASSERT_EQ(want_to_read, ref);
  }

  // large read from the beginning
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(0, swidth * 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());

    ref[shard_id_t(0)].insert(0, csize*42);
    ref[shard_id_t(1)].insert(0, csize*42);
    ref[shard_id_t(2)].insert(0, csize*42);
    ref[shard_id_t(3)].insert(0, csize*42);
    ASSERT_EQ(want_to_read, ref);
  }

  // large read from the beginning
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(0, swidth * 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());

    ref[shard_id_t(0)].insert(0, csize*42);
    ref[shard_id_t(1)].insert(0, csize*42);
    ref[shard_id_t(2)].insert(0, csize*42);
    ref[shard_id_t(3)].insert(0, csize*42);
    ASSERT_EQ(want_to_read, ref);
  }

  // large read from the beginning (second stripe)
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(swidth, swidth * 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());

    ref[shard_id_t(0)].insert(csize, csize*42);
    ref[shard_id_t(1)].insert(csize, csize*42);
    ref[shard_id_t(2)].insert(csize, csize*42);
    ref[shard_id_t(3)].insert(csize, csize*42);
    ASSERT_EQ(want_to_read, ref);
  }

  // large read that starts and ends on same shard.
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(swidth, swidth+csize/2, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());

    ref[shard_id_t(0)].insert(csize, csize+csize/2);
    ref[shard_id_t(1)].insert(csize, csize);
    ref[shard_id_t(2)].insert(csize, csize);
    ref[shard_id_t(3)].insert(csize, csize);
    ASSERT_EQ(want_to_read, ref);
  }

  // large read that starts and ends on last shard
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(swidth-csize, swidth+csize/2, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());

    ref[shard_id_t(0)].insert(csize, csize);
    ref[shard_id_t(1)].insert(csize, csize);
    ref[shard_id_t(2)].insert(csize, csize);
    ref[shard_id_t(3)].insert(0, csize+csize/2);
    ASSERT_EQ(want_to_read, ref);
  }
  // large read that starts and ends on last shard, partial first shard.
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ec_align_t to_read(swidth-csize/2, swidth, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECUtil::shard_extent_set_t ref(s.get_k_plus_m());

    ref[shard_id_t(0)].insert(csize, csize);
    ref[shard_id_t(1)].insert(csize, csize);
    ref[shard_id_t(2)].insert(csize, csize);
    ref[shard_id_t(3)].insert(csize/2, csize);
    ASSERT_EQ(want_to_read, ref);
  }
}

TEST(ECCommon, get_min_avail_to_read_shards) {
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 64*align_size;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const int nshards = 6;
  const uint64_t object_size = swidth * 1024;

  std::vector<ECCommon::shard_read_t> empty_shard_vector(k);

  ECUtil::stripe_info_t s(k, m, swidth, vector<shard_id_t>(0));
  ECListenerStub listenerStub;
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), swidth / k);

  const std::vector<int> chunk_mapping = {}; // no remapping
  MockErasureCode *ecode = new MockErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  for (int i = 0; i < nshards; i++) {
    listenerStub.acting_shards.insert(pg_shard_t(i, shard_id_t(i)));
  }

  // read nothing
  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
    ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());
    hobject_t hoid;
    ECCommon::read_request_t read_request(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );

    ASSERT_EQ(read_request,  ref);
  }

  /* Read to every data shard. */
  {
    ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());
    hobject_t hoid;

    for (shard_id_t i; i<k; ++i) {
      to_read_list[i].insert(int(i) * 2 * align_size, align_size);
    }

    ECCommon::read_request_t read_request(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    for (shard_id_t shard_id; shard_id < k; ++shard_id) {
      ref.shard_reads[shard_id].extents = to_read_list[shard_id];
      ref.shard_reads[shard_id].pg_shard = pg_shard_t(int(shard_id));
      ref.shard_reads[shard_id].pg_shard = pg_shard_t(int(shard_id), shard_id);
    }
    ASSERT_EQ(read_request,  ref);
  }

  /* Read to every data shard. */
  {
    ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());
    hobject_t hoid;
    for (shard_id_t i; i<k; ++i) {
      to_read_list[i].insert(int(i) * 2 * align_size, align_size);
    }

    ECCommon::read_request_t read_request(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );

    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    for (shard_id_t i; i<k; ++i) {
      shard_id_t shard_id(i);
      ref.shard_reads[shard_id].extents = to_read_list[i];
      ref.shard_reads[shard_id].pg_shard = pg_shard_t(int(i), shard_id);
    }

    ASSERT_EQ(read_request,  ref);
  }


  /* Read to every data shard - small read */
  {
    ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());
    hobject_t hoid;

    for (shard_id_t i; i < (int)k; ++i) {
      to_read_list[i].insert(int(i) * 2 * align_size + int(i) + 1, int(i) + 1);
    }
    ECCommon::read_request_t ref(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    ECCommon::read_request_t read_request(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    for (int i=0; i < (int)k; i++) {
      shard_id_t shard_id(i);
      ECCommon::shard_read_t &ref_shard_read = ref.shard_reads[shard_id];
      ref_shard_read.extents.insert(i*2*align_size, align_size);
      ref_shard_read.pg_shard = pg_shard_t(i, shard_id_t(i));
    }

    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);
    ASSERT_EQ(read_request,  ref);
  }

  /* Read to every data shard, missing shard. */
  {
    ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());
    hobject_t hoid;

    for (shard_id_t i; i<k; ++i) {
      to_read_list[i].insert(int(i) * 2 * align_size, align_size);
    }

    ECCommon::read_request_t read_request(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );

    shard_id_t missing_shard(1);
    int parity_shard = k;
    listenerStub.acting_shards.erase(pg_shard_t(int(missing_shard), shard_id_t(missing_shard)));

    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    for (shard_id_t i; i<k; ++i) {
      if (i != missing_shard) {
        shard_id_t shard_id(i);
	to_read_list[i].union_of(to_read_list[missing_shard]);
	ref.shard_reads[shard_id].extents = to_read_list[i];
        ref.shard_reads[shard_id].pg_shard = pg_shard_t(int(i), shard_id);
      } else {
	ECCommon::shard_read_t parity_shard_read;
	parity_shard_read.extents.union_of(to_read_list[i]);
	ref.shard_reads[shard_id_t(parity_shard)] = parity_shard_read;
        ref.shard_reads[shard_id_t(parity_shard)].pg_shard = pg_shard_t(parity_shard, shard_id_t(parity_shard));
      }
    }

    ASSERT_EQ(read_request,  ref);

    listenerStub.acting_shards.insert(pg_shard_t(1, shard_id_t(1)));
  }


  /* Read to every data shard, missing shard, missing shard is adjacent. */
  {
    ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());
    hobject_t hoid;
    unsigned int missing_shard = 1;

    to_read_list[shard_id_t(0)].insert(0, align_size);
    to_read_list[shard_id_t(1)].insert(align_size, align_size);
    to_read_list[shard_id_t(2)].insert(2*align_size, align_size);
    to_read_list[shard_id_t(3)].insert(3*align_size, align_size);
    ECCommon::read_request_t read_request(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    ECCommon::read_request_t ref(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );

    // Populating reference manually to check that adjacent shards get correctly combined.
    ref.shard_reads[shard_id_t(0)].extents.insert(0, align_size*2);
    ref.shard_reads[shard_id_t(2)].extents.insert(align_size, align_size*2);
    ref.shard_reads[shard_id_t(3)].extents.insert(align_size, align_size);
    ref.shard_reads[shard_id_t(3)].extents.insert(3*align_size, align_size);
    ref.shard_reads[shard_id_t(4)].extents.insert(align_size, align_size);
    ref.shard_reads[shard_id_t(0)].pg_shard = pg_shard_t(0, shard_id_t(0));
    ref.shard_reads[shard_id_t(2)].pg_shard = pg_shard_t(2, shard_id_t(2));
    ref.shard_reads[shard_id_t(3)].pg_shard = pg_shard_t(3, shard_id_t(3));
    ref.shard_reads[shard_id_t(4)].pg_shard = pg_shard_t(4, shard_id_t(4));
    for (unsigned int i=0; i<k+1; i++) {
      if (i==missing_shard) {
	continue;
      }
    }

    listenerStub.acting_shards.erase(pg_shard_t(missing_shard, shard_id_t(missing_shard)));

    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ASSERT_EQ(read_request,  ref);

    listenerStub.acting_shards.insert(pg_shard_t(1, shard_id_t(1)));
  }

  /* Read to every data shard, but with "fast" (redundant) reads */
  {
    ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());
    hobject_t hoid;

    extent_set extents_to_read;
    for (shard_id_t i; i<k; ++i) {
      to_read_list[i].insert(int(i) * 2 * align_size, align_size);
      extents_to_read.insert(int(i) * 2 * align_size, align_size);
    }
    ECCommon::read_request_t read_request(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );

    pipeline.get_min_avail_to_read_shards(hoid, false, true, read_request);

    ECCommon::read_request_t ref(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    for (unsigned int i=0; i<k+2; i++) {
      ECCommon::shard_read_t shard_read;
      shard_read.extents = extents_to_read;
      shard_read.pg_shard = pg_shard_t(i, shard_id_t(i));
      ref.shard_reads[shard_id_t(i)] = shard_read;
    }

    ASSERT_EQ(read_request,  ref);
  }

  /* Read to every data shard, missing shard. */
  {
    ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());
    hobject_t hoid;

    for (shard_id_t i; i<k; ++i) {
      to_read_list[i].insert(int(i) * 2 * align_size, align_size);
    }
    ECCommon::read_request_t read_request(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );

    shard_id_t missing_shard(1);
    int parity_shard = k;
    std::set<pg_shard_t> error_shards;
    error_shards.emplace(int(missing_shard), shard_id_t(missing_shard));
    // Similar to previous tests with missing shards, but this time, emulate
    // the shard being missing as a result of a bad read.
    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request, error_shards);

    ECCommon::read_request_t ref(
      to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    std::vector<ECCommon::shard_read_t> want_to_read(empty_shard_vector);
    for (shard_id_t i; i<k; ++i) {
      if (i != missing_shard) {
        want_to_read[int(i)].extents.union_of(to_read_list[missing_shard]);
        want_to_read[int(i)].extents.union_of(to_read_list[i]);
        want_to_read[int(i)].pg_shard = pg_shard_t(int(i), shard_id_t(i));
        ref.shard_reads[shard_id_t(i)] = want_to_read[int(i)];
      } else {
        ECCommon::shard_read_t parity_shard_read;
        parity_shard_read.extents.union_of(to_read_list[missing_shard]);
        parity_shard_read.pg_shard = pg_shard_t(parity_shard, shard_id_t(parity_shard));
        ref.shard_reads[shard_id_t(parity_shard)] = parity_shard_read;
      }
    }

    ASSERT_EQ(read_request,  ref);

    listenerStub.acting_shards.insert(pg_shard_t(1, shard_id_t(1)));
  }
}

TEST(ECCommon, shard_read_combo_tests)
{
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 2*align_size;
  const unsigned int k = 2;
  const unsigned int m = 2;
  const int nshards = 4;
  const uint64_t object_size = swidth * 1024;
  hobject_t hoid;

  ECUtil::stripe_info_t s(k, m, swidth, vector<shard_id_t>(0));
  ECListenerStub listenerStub;
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), swidth/k);

  const std::vector<int> chunk_mapping = {}; // no remapping
  MockErasureCode *ecode = new MockErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  for (int i = 0; i < nshards; i++) {
    listenerStub.acting_shards.insert(pg_shard_t(i, shard_id_t(i)));
  }

  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());

    ec_align_t to_read(36*1024,10*1024, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECCommon::read_request_t read_request(
      want_to_read, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );

    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(
      want_to_read, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    {
      ECCommon::shard_read_t shard_read;
      shard_read.extents.insert(20*1024, 4*1024);
      shard_read.pg_shard = pg_shard_t(0, shard_id_t(0));
      ref.shard_reads[shard_id_t(0)] = shard_read;
    }
    {
      ECCommon::shard_read_t shard_read;
      shard_read.extents.insert(16*1024, 8*1024);
      shard_read.pg_shard = pg_shard_t(1, shard_id_t(1));
      ref.shard_reads[shard_id_t(1)] = shard_read;
    }

    ASSERT_EQ(read_request,  ref);
  }

  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());

    ec_align_t to_read(12*1024,12*1024, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECCommon::read_request_t read_request(
      want_to_read, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(
      want_to_read, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    {
      ECCommon::shard_read_t shard_read;
      shard_read.extents.insert(8*1024, 4*1024);
      shard_read.pg_shard = pg_shard_t(0, shard_id_t(0));
      ref.shard_reads[shard_id_t(0)] = shard_read;
    }
    {
      ECCommon::shard_read_t shard_read;
      shard_read.extents.insert(4*1024, 8*1024);
      shard_read.pg_shard = pg_shard_t(1, shard_id_t(1));
      ref.shard_reads[shard_id_t(1)] = shard_read;
    }

    ASSERT_EQ(read_request,  ref);
  }
}

TEST(ECCommon, get_min_want_to_read_shards_bug67087)
{
  const uint64_t swidth = 4096;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const uint64_t csize = 1024;

  ECUtil::stripe_info_t s(k, m, swidth);
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), 1024);

  ECListenerStub listenerStub;
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), csize);

  const std::vector<int> chunk_mapping = {}; // no remapping
  ErasureCodeInterfaceRef ec_impl(new MockErasureCode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
  ec_align_t to_read1(512,512, 1);
  ec_align_t to_read2(512+16*1024,512, 1);

  ECUtil::shard_extent_set_t ref(s.get_k_plus_m());

  ref[shard_id_t(0)].insert(512, 512);

  // multitple calls with the same want_to_read can happen during
  // multi-region reads. This will create multiple extents in want_to_read,
  {
    pipeline.get_min_want_to_read_shards(
     to_read1, want_to_read);
    ASSERT_EQ(want_to_read, ref);

    pipeline.get_min_want_to_read_shards(
     to_read2, want_to_read);
    // We have 4 data shards per stripe.
    ref[shard_id_t(0)].insert(512+4*1024, 512);
  }
}

TEST(ECCommon, get_remaining_shards)
{
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 64*align_size;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const int nshards = 6;
  const uint64_t chunk_size = swidth / k;
  const uint64_t object_size = swidth * 1024;

  ECUtil::stripe_info_t s(k, m, swidth, vector<shard_id_t>(0));
  ECListenerStub listenerStub;
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), swidth/k);

  const std::vector<int> chunk_mapping = {}; // no remapping
  MockErasureCode *ecode = new MockErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  std::vector<ECCommon::shard_read_t> empty_shard_vector(k);
  ECCommon::shard_read_t empty_shard_read;
  fill(empty_shard_vector.begin(), empty_shard_vector.end(), empty_shard_read);

  vector<pg_shard_t> pg_shards(nshards);
  for (int i = 0; i < nshards; i++) {
    pg_shards[i] = pg_shard_t(i, shard_id_t(i));
    listenerStub.acting_shards.insert(pg_shards[i]);
  }

  {
    hobject_t hoid;

    // Mock up a read request
    ECUtil::shard_extent_set_t to_read(s.get_k_plus_m());
    to_read[shard_id_t(0)].insert(0, 4096);
    ECCommon::read_request_t read_request(
      to_read, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    int missing_shard = 0;

    // Mock up a read result.
    ECCommon::read_result_t read_result(&s);
    read_result.errors.emplace(pg_shards[missing_shard], -EIO);

    pipeline.get_remaining_shards(hoid, read_result, read_request, false, false, false, false);

    ECCommon::read_request_t ref(
      to_read, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    int parity_shard = 4;
    for (unsigned int i=0; i<k; i++) {
      ECCommon::shard_read_t shard_read;
      shard_read.extents.insert(0,4096);
      unsigned int shard_id = std::cmp_equal(i, missing_shard) ? parity_shard : i;
      shard_read.pg_shard = pg_shard_t(shard_id, shard_id_t(shard_id));
      ref.shard_reads[shard_id_t(shard_id)] = shard_read;
    }

    ASSERT_EQ(read_request,  ref);
  }

  // Request re-read. There is a page of overlap in what is already read.
  {
    hobject_t hoid;

    ECUtil::shard_extent_set_t to_read(s.get_k_plus_m());
    s.ro_range_to_shard_extent_set(chunk_size/2, chunk_size+align_size, to_read);
    ECCommon::read_request_t read_request(
      to_read, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    unsigned int missing_shard = 1;

    // Mock up a read result.
    ECCommon::read_result_t read_result(&s);
    read_result.errors.emplace(pg_shards[missing_shard], -EIO);
    buffer::list bl;
    bl.append_zero(chunk_size/2);
    read_result.buffers_read.insert_in_shard(shard_id_t(0), chunk_size/2, bl);
    read_result.processed_read_requests[shard_id_t(0)].insert(chunk_size/2, bl.length());

    pipeline.get_remaining_shards(hoid, read_result, read_request, false, false, false, false);

    // The result should be a read request for the first 4k of shard 0, as that
    // is currently missing.
    ECCommon::read_request_t ref(
      to_read, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
      ECCommon::WantOmapKeys::No, "", 0, object_size
    );
    int parity_shard = 4;
    for (unsigned int i=0; i<k; i++) {
      ECCommon::shard_read_t shard_read;
      unsigned int shard_id = i==missing_shard?parity_shard:i;
      ref.shard_reads[shard_id_t(shard_id)] = shard_read;
    }
    ref.shard_reads[shard_id_t(0)].extents.insert(0, chunk_size/2);
    ref.shard_reads[shard_id_t(0)].pg_shard = pg_shards[0];
    ref.shard_reads[shard_id_t(2)].extents.insert(0, chunk_size/2+align_size);
    ref.shard_reads[shard_id_t(2)].pg_shard = pg_shards[2];
    ref.shard_reads[shard_id_t(3)].extents.insert(0, chunk_size/2+align_size);
    ref.shard_reads[shard_id_t(3)].pg_shard = pg_shards[3];
    ref.shard_reads[shard_id_t(4)].extents.insert(0, chunk_size/2+align_size);
    ref.shard_reads[shard_id_t(4)].pg_shard = pg_shards[4];
    ASSERT_EQ(read_request,  ref);
  }
}

TEST(ECCommon, encode)
{
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 2*align_size;
  const unsigned int k = 2;
  const unsigned int m = 2;

  ECUtil::stripe_info_t s(k, m, swidth, vector<shard_id_t>(0));
  ECListenerStub listenerStub;
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), swidth/k);

  const std::vector<int> chunk_mapping = {}; // no remapping
  MockErasureCode *ecode = new MockErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  ECUtil::shard_extent_map_t semap(&s);

  for (shard_id_t i; i<k+m; ++i) {
    bufferlist bl;
    bl.append_zero(i>=k?4096:2048);
    semap.insert_in_shard(i, 12*1024, bl);
  }
  semap.encode(ec_impl);
}

bufferlist create_buf(uint64_t len) {
  bufferlist bl;

  while (bl.length() < len) {
    uint64_t pages = std::rand() % 5 + 1;  // 1-5 pages to avoid infinite loop
    uint64_t len_to_add = std::min(len - bl.length(), pages * EC_ALIGN_SIZE);
    // Create page-aligned buffer to ensure memory alignment
    bufferptr ptr = buffer::create_page_aligned(len_to_add);
    memset(ptr.c_str(), 0, len_to_add);
    bl.append(ptr);
  }
  ceph_assert(bl.is_aligned(EC_ALIGN_SIZE));
  ceph_assert(len == bl.length());
  return bl;
}


void test_decode(unsigned int k, unsigned int m, uint64_t chunk_size, uint64_t object_size, const ECUtil::shard_extent_set_t &want, const shard_id_set &acting_set)
{
  const uint64_t swidth = k*chunk_size;

  ECUtil::stripe_info_t s(k, m, swidth, vector<shard_id_t>(0));
  ECListenerStub listenerStub;
  listenerStub.acting_shards.clear();
  for (auto s : acting_set) {
    listenerStub.acting_shards.insert(pg_shard_t(int(s), s));
  }
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), swidth/k);

  const std::vector<int> chunk_mapping = {}; // no remapping
  MockErasureCode *ecode = new MockErasureCode(k, k + m);
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);


  ECUtil::shard_extent_map_t semap(&s);
  hobject_t hoid;
  ECCommon::read_request_t read_request(
    want, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No,
    ECCommon::WantOmapKeys::No, "", 0, object_size
  );
  ASSERT_EQ(0, pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request));
  for (auto [shard, read] : read_request.shard_reads) {
    for (auto [off, len] : read.extents) {
      semap.insert_in_shard(shard, off, create_buf(len));
    }
  }

  semap.add_zero_padding_for_decode(read_request.zeros_for_decode);
  ASSERT_EQ(0, semap.decode(ec_impl, want, object_size, nullptr, true));
}

TEST(ECCommon, decode) {
  unsigned int k = 4;
  unsigned int m = 2;
  uint64_t chunk_size = 4096;
  uint64_t object_size = k * 256 * 1024 + 4096 + 1;
  ECUtil::shard_extent_set_t want(k+m);
  shard_id_set acting_set;

  want[shard_id_t(1)].insert(256 * 1024, 4096);
  want[shard_id_t(4)].insert(256 * 1024, 4096);

  acting_set.insert_range(shard_id_t(1), 4);
  test_decode(k, m, chunk_size, object_size, want, acting_set);
}


TEST(ECCommon, decode2)
{
  unsigned int k = 4;
  unsigned int m = 2;
  uint64_t chunk_size = 4096;
  uint64_t object_size = 2104*1024;

  ECUtil::shard_extent_set_t want(k+m);
  shard_id_set acting_set;

  want[shard_id_t(1)].insert(0, 528*1024);
  want[shard_id_t(2)].insert(0, 524*1024);
  want[shard_id_t(3)].insert(0, 524*1024);
  want[shard_id_t(4)].insert(0, 528*1024);
  want[shard_id_t(5)].insert(0, 528*1024);

  acting_set.insert(shard_id_t(0));
  acting_set.insert(shard_id_t(1));
  acting_set.insert(shard_id_t(3));
  acting_set.insert(shard_id_t(4));

  test_decode(k, m, chunk_size, object_size, want, acting_set);
}

TEST(ECCommon, decode3) {
  /* For this problematic IO, we want to reads:
   * first is readable - shard 0, 0~4k
   * second is on missing shard - shard 2, 16k~4k
   *
   * Recovery would work out it needs to recover shard 2, so would need
   * shards 0,1,3,4 - howecer it works out that shard 3 does not need a read
   *                  because the object is off the end!
   *
   * So the reads we end up doing are to 0,1 and 4 only.
   */
  unsigned int k = 4;
  unsigned int m = 1;
  uint64_t chunk_size = 4096;
  uint64_t object_size = 4 * chunk_size * k + 2 * chunk_size + 1;

  ECUtil::shard_extent_set_t want(k+m);
  shard_id_set acting_set;
  want[shard_id_t(0)].insert(0, chunk_size);
  want[shard_id_t(2)].insert(4 * chunk_size, chunk_size);

  acting_set.insert(shard_id_t(0));
  acting_set.insert(shard_id_t(1));
  acting_set.insert(shard_id_t(3));
  acting_set.insert(shard_id_t(4));

  test_decode(k, m, chunk_size, object_size, want, acting_set);
}

TEST(ECCommon, decode4) {
  const unsigned int k = 5;
  const unsigned int m = 2;
  const uint64_t chunk_size = 4096;
  const uint64_t object_size = 3243718;

  ECUtil::shard_extent_set_t want(k+m);
  shard_id_set acting_set;
  want[shard_id_t(0)].insert(544768, 106496);
  want[shard_id_t(1)].insert(544151, 106799);
  want[shard_id_t(2)].insert(540672, 106496);
  want[shard_id_t(3)].insert(540672, 106496);
  want[shard_id_t(4)].insert(540672, 106496);

  acting_set.insert(shard_id_t(0));
  acting_set.insert_range(shard_id_t(2), 4);

  test_decode(k, m, chunk_size, object_size, want, acting_set);
}

TEST(ECCommon, decode5) {
  const unsigned int k = 6;
  const unsigned int m = 4;
  const uint64_t chunk_size = 4096;
  const uint64_t object_size = 3428595;

  ECUtil::shard_extent_set_t want(k+m);
  shard_id_set acting_set;
  want[shard_id_t(0)].insert(0, 573440);
  want[shard_id_t(1)].insert(0, 573440);
  want[shard_id_t(2)].insert(0, 573440);
  want[shard_id_t(3)].insert(0, 569587);
  want[shard_id_t(4)].insert(0, 569344);
  want[shard_id_t(5)].insert(0, 569344);

  acting_set.insert(shard_id_t(0));
  acting_set.insert(shard_id_t(3));
  acting_set.insert_range(shard_id_t(6), 4);

  test_decode(k, m, chunk_size, object_size, want, acting_set);
}

TEST(ECCommon, decode6) {
  const unsigned int k = 8;
  const unsigned int m = 4;
  const uint64_t chunk_size = 4096;
  const uint64_t object_size = 3092488;


  ECUtil::shard_extent_set_t want(k+m);
  shard_id_set acting_set;
  want[shard_id_t(0)].insert(262144, 126976);
  want[shard_id_t(1)].insert(262144, 126976);
  want[shard_id_t(2)].insert(262144, 126976);
  want[shard_id_t(3)].insert(262144, 126976);
  want[shard_id_t(4)].insert(262144, 122880);
  want[shard_id_t(5)].insert(262144, 122880);
  want[shard_id_t(6)].insert(262144, 122880);
  want[shard_id_t(7)].insert(262144, 122880);
  want[shard_id_t(8)].insert(262144, 126976);
  want[shard_id_t(9)].insert(262144, 126976);
  want[shard_id_t(10)].insert(262144, 126976);

  acting_set.insert(shard_id_t(0));
  acting_set.insert_range(shard_id_t(2), 2);
  acting_set.insert_range(shard_id_t(5), 5);

  test_decode(k, m, chunk_size, object_size, want, acting_set);
}

TEST(ECCommon, decode7) {
  const unsigned int k = 3;
  const unsigned int m = 3;
  const uint64_t chunk_size = 4096;
  const uint64_t object_size = 89236;


  ECUtil::shard_extent_set_t want(k+m);
  shard_id_set acting_set;
  want[shard_id_t(5)].insert(0, 32*1024);

  acting_set.insert_range(shard_id_t(0), 3);

  test_decode(k, m, chunk_size, object_size, want, acting_set);
}

TEST(ECCommon, decode8) {
  const unsigned int k = 3;
  const unsigned int m = 2;
  const uint64_t chunk_size = 64 * 1024;
  const uint64_t object_size = 672 * 1024;


  ECUtil::shard_extent_set_t want(k+m);
  shard_id_set acting_set;
  want[shard_id_t(0)].insert(64 * 1024, 64 * 1024);
  want[shard_id_t(2)].insert(32 * 1024, 32 * 1024);
  want[shard_id_t(3)].insert(32 * 1024, 64 * 1024);
  want[shard_id_t(4)].insert(32 * 1024, 64 * 1024);


  acting_set.insert(shard_id_t(0));
  acting_set.insert_range(shard_id_t(2), 2);

  test_decode(k, m, chunk_size, object_size, want, acting_set);
}

// Zone support tests for get_min_avail_to_read_shards
TEST(ECCommon, get_min_avail_to_read_shards_zones_local_zone_available) {
  // Test that when all shards are available in local zone, they are preferred
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 64*align_size;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const int nshards = 6;
  const uint64_t object_size = swidth * 1024;

  pg_pool_t pool;
  pool.size = 12; // 2 zones with k+m shards each
  pool.opts.set(pool_opts_t::NUM_ZONES, 2);

  ECUtil::stripe_info_t s(k, m, swidth, &pool);
  ECListenerStub listenerStub;

  MockErasureCode *ecode = new MockErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  // Set up shards in both zones (0-5 in zone 0, 6-11 in zone 1)
  for (int i = 0; i < 12; i++) {
    listenerStub.acting_shards.insert(pg_shard_t(i, shard_id_t(i)));
  }

  hobject_t hoid;
  ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());

  // Request reads from data shards in zone 0
  for (shard_id_t i; i < k; ++i) {
    to_read_list[i].insert(int(i) * 2 * align_size, align_size);
  }

  ECCommon::read_request_t read_request(to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No, ECCommon::WantOmapKeys::No, "", 0, object_size);
  int r = pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

  ASSERT_EQ(r, 0);

  // Verify that only local zone shards (0-5) are used
  for (auto &[shard_id, shard_read] : read_request.shard_reads) {
    ASSERT_LT(int(shard_id), nshards) << "Should only use local zone shards";
    ASSERT_EQ(shard_read.pg_shard.shard, shard_id);
  }
}

// Every other get_min_avail_to_read_shards_zones_* test above leaves
// ECListenerStub::whoami at its default (a zone-0 shard), so "local" has
// only ever meant zone 0 in this suite. select_shards_for_read() picks its
// local zone purely from get_parent()->whoami_shard() (see
// ECCommon.cc select_shards_for_read: `sinfo.get_shard_zone(whoami_shard().shard)`),
// so a primary actually living in zone 1 -- exactly the configuration in
// which the zone-1 parity bug and the handle_sub_write relative-shard bug
// lived -- was never exercised through this path. Set whoami to a zone-1
// shard and confirm "local" correctly shifts to zone 1.
TEST(ECCommon, get_min_avail_to_read_shards_zones_primary_in_zone1) {
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 64*align_size;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const uint64_t object_size = swidth * 1024;

  pg_pool_t pool;
  pool.size = 12; // 2 zones with k+m shards each
  pool.opts.set(pool_opts_t::NUM_ZONES, 2);

  ECUtil::stripe_info_t s(k, m, swidth, &pool);
  ECListenerStub listenerStub;
  // The primary (whoami) is osd 6, absolute shard 6 -- relative shard 0 of
  // zone 1 -- not the zone-0 default every other test in this file uses.
  listenerStub.whoami = pg_shard_t(6, shard_id_t(6));

  MockErasureCode *ecode = new MockErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  // Both zones fully available: 0-5 in zone 0, 6-11 in zone 1.
  for (int i = 0; i < 12; i++) {
    listenerStub.acting_shards.insert(pg_shard_t(i, shard_id_t(i)));
  }

  hobject_t hoid;
  ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());

  for (shard_id_t i; i < k; ++i) {
    to_read_list[i].insert(int(i) * 2 * align_size, align_size);
  }

  ECCommon::read_request_t read_request(to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No, ECCommon::WantOmapKeys::No, "", 0, object_size);
  int r = pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

  ASSERT_EQ(r, 0);

  // With a zone-1 primary, "local" is zone 1: every shard picked should be
  // the zone-1 absolute copy (id >= k+m == 6), not the zone-0 one, even
  // though both are fully available.
  for (auto &[shard_id, shard_read] : read_request.shard_reads) {
    ASSERT_GE(int(shard_read.pg_shard.shard), k + m)
      << "relative shard " << shard_id << ": primary is in zone 1, so the "
      << "zone-1 copy should be preferred, not the zone-0 one";
  }
}

TEST(ECCommon, get_min_avail_to_read_shards_zones_fallback_to_remote) {
  // Test that when local zone shards are missing, remote zone shards are used
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 64*align_size;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const uint64_t object_size = swidth * 1024;

  pg_pool_t pool;
  pool.size = 12; // 2 zones
  pool.opts.set(pool_opts_t::NUM_ZONES, 2);

  ECUtil::stripe_info_t s(k, m, swidth, &pool);
  ECListenerStub listenerStub;

  MockErasureCode *ecode = new MockErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  // Only add shards from remote zone (6-11) and one from local zone
  listenerStub.acting_shards.insert(pg_shard_t(0, shard_id_t(0))); // Local zone
  for (int i = 6; i < 12; i++) {
    listenerStub.acting_shards.insert(pg_shard_t(i, shard_id_t(i))); // Remote zone
  }

  hobject_t hoid;
  ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());

  // Request reads from all data shards
  for (shard_id_t i; i < k; ++i) {
    to_read_list[i].insert(int(i) * 2 * align_size, align_size);
  }

  ECCommon::read_request_t read_request(to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No, ECCommon::WantOmapKeys::No, "", 0, object_size);
  int r = pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

  ASSERT_EQ(r, 0);

  // Verify that remote zone shards are used (should have shards >= 6)
  bool has_remote_shard = false;
  for (auto &[shard_id, shard_read] : read_request.shard_reads) {
    if (int(shard_read.pg_shard.shard) >= 6) {
      has_remote_shard = true;
      break;
    }
  }
  ASSERT_TRUE(has_remote_shard) << "Should use remote zone shards when local unavailable";

  // Relative shards 1-3 have no local-zone copy at all, so the check above
  // is trivially satisfied by them alone. Relative shard 0, however, DOES
  // have a local copy (pg_shard_t(0, shard_id_t(0))) as well as a same-
  // relative-shard remote duplicate (pg_shard_t(6, shard_id_t(6))): this is
  // exactly the collision get_all_avail_shards() must resolve in favour of
  // the local copy. Assert that specifically, so a regression that lets the
  // remote duplicate win the collision is actually caught here.
  ASSERT_EQ(read_request.shard_reads.at(shard_id_t(0)).pg_shard,
            pg_shard_t(0, shard_id_t(0)))
    << "Relative shard 0 has a local copy available and must not fall back "
    << "to its remote-zone duplicate";
}

TEST(ECCommon, get_min_avail_to_read_shards_zones_missing_shard_local) {
  // Test handling of missing shard in local zone with zones enabled
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 64*align_size;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const uint64_t object_size = swidth * 1024;

  pg_pool_t pool;
  pool.size = 12;
  pool.opts.set(pool_opts_t::NUM_ZONES, 2);

  ECUtil::stripe_info_t s(k, m, swidth, &pool);
  ECListenerStub listenerStub;

  MockErasureCode *ecode = new MockErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  // Add all local zone shards except shard 1
  for (int i = 0; i < 6; i++) {
    if (i != 1) {
      listenerStub.acting_shards.insert(pg_shard_t(i, shard_id_t(i)));
    }
  }

  hobject_t hoid;
  ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());

  for (shard_id_t i; i < k; ++i) {
    to_read_list[i].insert(int(i) * 2 * align_size, align_size);
  }

  ECCommon::read_request_t read_request(to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No, ECCommon::WantOmapKeys::No, "", 0, object_size);
  int r = pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

  ASSERT_EQ(r, 0);

  // Should use parity shard to recover missing data shard
  bool has_parity = false;
  for (auto &[shard_id, shard_read] : read_request.shard_reads) {
    if (std::cmp_greater_equal(int(shard_id), k)) {
      has_parity = true;
    }
  }
  ASSERT_TRUE(has_parity) << "Should use parity shard when data shard missing";
}

TEST(ECCommon, get_min_avail_to_read_shards_zones_error_shards) {
  // Test that error_shards parameter works correctly with zones
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 64*align_size;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const uint64_t object_size = swidth * 1024;

  pg_pool_t pool;
  pool.size = 12;
  pool.opts.set(pool_opts_t::NUM_ZONES, 2);

  ECUtil::stripe_info_t s(k, m, swidth, &pool);
  ECListenerStub listenerStub;

  MockErasureCode *ecode = new MockErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  // Add all shards from both zones
  for (int i = 0; i < 12; i++) {
    listenerStub.acting_shards.insert(pg_shard_t(i, shard_id_t(i)));
  }

  hobject_t hoid;
  ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());

  for (shard_id_t i; i < k; ++i) {
    to_read_list[i].insert(int(i) * 2 * align_size, align_size);
  }

  // Mark shard 1 as having an error
  std::set<pg_shard_t> error_shards;
  error_shards.emplace(1, shard_id_t(1));

  ECCommon::read_request_t read_request(to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No, ECCommon::WantOmapKeys::No, "", 0, object_size);
  int r = pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request, error_shards);

  ASSERT_EQ(r, 0);

  // Verify shard 1 is not in the read request
  ASSERT_EQ(read_request.shard_reads.count(shard_id_t(1)), 0u)
    << "Error shard should not be in read request";
}

TEST(ECCommon, get_min_avail_to_read_shards_zones_three_zones) {
  // Test with 3 zones
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 64*align_size;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const uint64_t object_size = swidth * 1024;

  pg_pool_t pool;
  pool.size = 18; // 3 zones
  pool.opts.set(pool_opts_t::NUM_ZONES, 3);

  ECUtil::stripe_info_t s(k, m, swidth, &pool);
  ECListenerStub listenerStub;

  MockErasureCode *ecode = new MockErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  // Add shards from all three zones
  for (int i = 0; i < 18; i++) {
    listenerStub.acting_shards.insert(pg_shard_t(i, shard_id_t(i)));
  }

  hobject_t hoid;
  ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());

  for (shard_id_t i; i < k; ++i) {
    to_read_list[i].insert(int(i) * 2 * align_size, align_size);
  }

  ECCommon::read_request_t read_request(to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No, ECCommon::WantOmapKeys::No, "", 0, object_size);
  int r = pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

  ASSERT_EQ(r, 0);

  // Should prefer local zone (0-5)
  for (auto &[shard_id, shard_read] : read_request.shard_reads) {
    ASSERT_LT(int(shard_id), 6) << "Should prefer local zone shards";
  }
}

TEST(ECCommon, get_min_avail_to_read_shards_zones_insufficient_shards) {
  // Test error case when not enough shards available even with zones
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 64*align_size;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const uint64_t object_size = swidth * 1024;

  pg_pool_t pool;
  pool.size = 12;
  pool.opts.set(pool_opts_t::NUM_ZONES, 2);

  ECUtil::stripe_info_t s(k, m, swidth, &pool);
  ECListenerStub listenerStub;

  MockErasureCode *ecode = new MockErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  // Only add 2 shards - not enough to decode
  listenerStub.acting_shards.insert(pg_shard_t(0, shard_id_t(0)));
  listenerStub.acting_shards.insert(pg_shard_t(1, shard_id_t(1)));

  hobject_t hoid;
  ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());

  for (shard_id_t i; i < k; ++i) {
    to_read_list[i].insert(int(i) * 2 * align_size, align_size);
  }

  ECCommon::read_request_t read_request(to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No, ECCommon::WantOmapKeys::No, "", 0, object_size);
  int r = pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

  ASSERT_NE(r, 0) << "Should fail when insufficient shards available";
}

TEST(ECCommon, get_min_avail_to_read_shards_zones_redundant_reads) {
  // Test redundant reads with zones enabled
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 64*align_size;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const uint64_t object_size = swidth * 1024;

  pg_pool_t pool;
  pool.size = 12;
  pool.opts.set(pool_opts_t::NUM_ZONES, 2);

  ECUtil::stripe_info_t s(k, m, swidth, &pool);
  ECListenerStub listenerStub;

  MockErasureCode *ecode = new MockErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  // Add all local zone shards
  for (int i = 0; i < 6; i++) {
    listenerStub.acting_shards.insert(pg_shard_t(i, shard_id_t(i)));
  }

  hobject_t hoid;
  ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());

  for (shard_id_t i; i < k; ++i) {
    to_read_list[i].insert(int(i) * 2 * align_size, align_size);
  }

  ECCommon::read_request_t read_request(to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No, ECCommon::WantOmapKeys::No, "", 0, object_size);
  // Enable redundant reads
  int r = pipeline.get_min_avail_to_read_shards(hoid, false, true, read_request);

  ASSERT_EQ(r, 0);

  // With redundant reads, should read from all available shards
  ASSERT_EQ(read_request.shard_reads.size(), 6u)
    << "Redundant reads should use all available shards";
}

TEST(ECCommon, get_min_avail_to_read_shards_zones_recovery_mode) {
  // Test recovery mode with zones
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 64*align_size;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const uint64_t object_size = swidth * 1024;

  pg_pool_t pool;
  pool.size = 12;
  pool.opts.set(pool_opts_t::NUM_ZONES, 2);

  ECUtil::stripe_info_t s(k, m, swidth, &pool);
  ECListenerStub listenerStub;

  MockErasureCode *ecode = new MockErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  // Add shards from both zones
  for (int i = 0; i < 12; i++) {
    listenerStub.acting_shards.insert(pg_shard_t(i, shard_id_t(i)));
  }

  hobject_t hoid;
  ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());

  for (shard_id_t i; i < k; ++i) {
    to_read_list[i].insert(int(i) * 2 * align_size, align_size);
  }

  ECCommon::read_request_t read_request(to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No, ECCommon::WantOmapKeys::No, "", 0, object_size);
  // Enable recovery mode (for_recovery = true)
  int r = pipeline.get_min_avail_to_read_shards(hoid, true, false, read_request);

  ASSERT_EQ(r, 0);

  // In recovery mode, should still prefer local zone
  for (auto &[shard_id, shard_read] : read_request.shard_reads) {
    ASSERT_LT(int(shard_id), 6) << "Recovery should prefer local zone";
  }
}

TEST(ECCommon, get_min_avail_to_read_shards_zones_mixed_availability) {
  // Test with mixed shard availability across zones
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 64*align_size;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const uint64_t object_size = swidth * 1024;

  pg_pool_t pool;
  pool.size = 12;
  pool.opts.set(pool_opts_t::NUM_ZONES, 2);

  ECUtil::stripe_info_t s(k, m, swidth, &pool);
  ECListenerStub listenerStub;

  MockErasureCode *ecode = new MockErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  // Add some shards from local zone (0, 2, 4) and some from remote (7, 9, 11)
  listenerStub.acting_shards.insert(pg_shard_t(0, shard_id_t(0)));
  listenerStub.acting_shards.insert(pg_shard_t(2, shard_id_t(2)));
  listenerStub.acting_shards.insert(pg_shard_t(4, shard_id_t(4)));
  listenerStub.acting_shards.insert(pg_shard_t(7, shard_id_t(7)));
  listenerStub.acting_shards.insert(pg_shard_t(9, shard_id_t(9)));
  listenerStub.acting_shards.insert(pg_shard_t(11, shard_id_t(11)));

  hobject_t hoid;
  ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());

  for (shard_id_t i; i < k; ++i) {
    to_read_list[i].insert(int(i) * 2 * align_size, align_size);
  }

  ECCommon::read_request_t read_request(to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No, ECCommon::WantOmapKeys::No, "", 0, object_size);
  int r = pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

  ASSERT_EQ(r, 0);

  // Should use a mix, but prefer local when possible
  int remote_count = 0;
  for (auto &[shard_id, shard_read] : read_request.shard_reads) {
    if (int(shard_read.pg_shard.shard) >= 6) {
      remote_count++;
    }
  }

  // Should have used some remote shards since local doesn't have enough
  ASSERT_GT(remote_count, 0) << "Should use remote shards when local insufficient";

  // The above only proves that relative shards 1 and 3 (which have NO
  // local-zone copy at all) went remote -- that is trivially true and
  // would hold even if local preference were completely broken. Pin down
  // the shards that DO have a local copy (relative shards 0 and 2, from
  // pg_shard_t(0,0) and pg_shard_t(2,2)) and assert those specific local
  // copies were the ones actually selected, not merely "a mix" of shards.
  ASSERT_EQ(read_request.shard_reads.at(shard_id_t(0)).pg_shard,
            pg_shard_t(0, shard_id_t(0)))
    << "Relative shard 0 has a local copy and should use it";
  ASSERT_EQ(read_request.shard_reads.at(shard_id_t(2)).pg_shard,
            pg_shard_t(2, shard_id_t(2)))
    << "Relative shard 2 has a local copy and should use it";
}

TEST(ECCommon, get_min_avail_to_read_shards_zones_prefers_local_over_low_osd_remote) {
  // get_all_avail_shards() walks get_parent()->get_acting_shards(), which is a
  // plain std::set<pg_shard_t> ordered by OSD id (pg_shard_t's operator<=>
  // compares osd before shard) - it has no notion of zone. In the
  // allow_remote_zone fallback path, whichever physical copy of a relative
  // shard is visited FIRST wins, regardless of which zone it is in. If a
  // remote-zone OSD happens to have a lower id than the local-zone OSD
  // holding the same relative shard, the remote copy wins even though the
  // local copy was fully available - breaking "prefer local zone".
  //
  // Here the local zone (zone 0) only has relative shards 0 and 1, on
  // deliberately HIGH osd ids (100, 101). The remote zone (zone 1) has
  // relative shards 0-3, on deliberately LOW osd ids (1-4). Reading data
  // shards 0-3 forces the local-only pass to fail (only 2 of 4 needed
  // shards are local), triggering the allow_remote_zone fallback. Since
  // relative shards 0 and 1 are available locally, the fallback must still
  // prefer those local copies over the lower-osd-id remote ones.
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 64*align_size;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const uint64_t object_size = swidth * 1024;

  pg_pool_t pool;
  pool.size = 12; // 2 zones with k+m shards each
  pool.opts.set(pool_opts_t::NUM_ZONES, 2);

  ECUtil::stripe_info_t s(k, m, swidth, &pool);
  ECListenerStub listenerStub;

  MockErasureCode *ecode = new MockErasureCode();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  // Local zone (zone 0): only relative shards 0 and 1, on high osd ids.
  listenerStub.acting_shards.insert(pg_shard_t(100, shard_id_t(0)));
  listenerStub.acting_shards.insert(pg_shard_t(101, shard_id_t(1)));
  // Remote zone (zone 1): relative shards 0-3, on low osd ids.
  listenerStub.acting_shards.insert(pg_shard_t(1, shard_id_t(6)));
  listenerStub.acting_shards.insert(pg_shard_t(2, shard_id_t(7)));
  listenerStub.acting_shards.insert(pg_shard_t(3, shard_id_t(8)));
  listenerStub.acting_shards.insert(pg_shard_t(4, shard_id_t(9)));

  hobject_t hoid;
  ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());

  // Request reads from all k data shards.
  for (shard_id_t i; i < k; ++i) {
    to_read_list[i].insert(int(i) * 2 * align_size, align_size);
  }

  ECCommon::read_request_t read_request(to_read_list, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::No, ECCommon::WantOmapKeys::No, "", 0, object_size);
  int r = pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

  ASSERT_EQ(r, 0);

  // Relative shards 0 and 1 are available locally (zone 0) and must resolve
  // to their local pg_shard (absolute shard id < 6), not to the remote copy
  // that merely happens to have a lower osd id.
  for (auto &[shard_id, shard_read] : read_request.shard_reads) {
    if (int(shard_id) == 0 || int(shard_id) == 1) {
      ASSERT_LT(int(shard_read.pg_shard.shard), 6)
        << "relative shard " << shard_id
        << " has a local copy available and should not fall back to the "
        << "remote zone just because a remote osd id is lower; got pg_shard "
        << shard_read.pg_shard;
    }
  }
}

// Test for the fix in 9a9c55e: get_readable_writable_shard_id_sets() must return
// relative shard IDs (zone-local), not absolute IDs, so that downstream consumers
// such as WritePlanObj::intersect with get_parity_shards() produce correct results
// for zone-1 PGs in stretch mode.
TEST(ECCommon, get_readable_writable_shard_id_sets_returns_relative_shards) {
  // Use k=2, m=1 so k+m=3.  Zone-0 absolute shards: {0,1,2}.
  // Zone-1 absolute shards: {3,4,5}.  Relative shards are always {0,1,2}.
  const unsigned int k = 2;
  const unsigned int m = 1;
  const uint64_t swidth = 4096 * k;

  pg_pool_t pool;
  pool.size = 6; // 2 zones * (k+m)
  pool.opts.set(pool_opts_t::NUM_ZONES, 2);

  ECUtil::stripe_info_t s(k, m, swidth, &pool);
  ECListenerStub listenerStub;
  ErasureCodeInterfaceRef ec_impl(new MockErasureCode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  // Simulate a zone-1 PG: acting shards carry absolute IDs 3, 4, 5
  listenerStub.acting_shards.insert(pg_shard_t(0, shard_id_t(3)));
  listenerStub.acting_shards.insert(pg_shard_t(1, shard_id_t(4)));
  listenerStub.acting_shards.insert(pg_shard_t(2, shard_id_t(5)));

  // acting_recovery_backfill also uses absolute shard IDs for zone-1
  listenerStub.acting_recovery_backfill_shard_id_set.insert(shard_id_t(3));
  listenerStub.acting_recovery_backfill_shard_id_set.insert(shard_id_t(4));
  listenerStub.acting_recovery_backfill_shard_id_set.insert(shard_id_t(5));

  auto [readable, writable] = pipeline.get_readable_writable_shard_id_sets();

  // Both sets must contain only relative shard IDs {0, 1, 2}, not {3, 4, 5}.
  shard_id_set expected;
  expected.insert(shard_id_t(0));
  expected.insert(shard_id_t(1));
  expected.insert(shard_id_t(2));

  EXPECT_EQ(readable, expected)
    << "readable set must use relative shard IDs, not absolute zone-1 IDs";
  EXPECT_EQ(writable, expected)
    << "writable set must use relative shard IDs, not absolute zone-1 IDs";

  // Sanity check: the relative set does not contain the absolute zone-1 IDs
  EXPECT_FALSE(readable.contains(shard_id_t(3)));
  EXPECT_FALSE(readable.contains(shard_id_t(4)));
  EXPECT_FALSE(readable.contains(shard_id_t(5)));
}

// Reproduces a bug in ECCommon::ReadPipeline::ensure_primary_shard_for_omap():
// it looks for a primary-capable shard to satisfy an omap read by calling
// get_all_avail_shards() with a hard-coded local_zone of 0 and a literal 0
// (rather than a bool) for allow_remote_zone, and never retries with a
// remote-zone fallback the way get_min_avail_to_read_shards()/
// select_shards_for_read() do a few lines earlier. If the primary itself
// lives in a non-zero zone and zone 0 is entirely down, the hard-coded scan
// of zone 0 finds nothing, even though the primary's own zone has a
// perfectly good primary-capable shard.
TEST(ECCommon, ensure_primary_shard_for_omap_zone1_primary_zone0_down) {
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 64 * align_size;
  const unsigned int k = 4;
  const unsigned int m = 2;
  const uint64_t object_size = swidth * 1024;

  pg_pool_t pool;
  pool.size = 12; // 2 zones of k+m=6 shards each
  pool.opts.set(pool_opts_t::NUM_ZONES, 2);
  // Only relative shard 0 is primary-capable in each zone; relative
  // shards 1-5 cannot become primary.
  pool.nonprimary_shards.insert(shard_id_t(1));
  pool.nonprimary_shards.insert(shard_id_t(2));
  pool.nonprimary_shards.insert(shard_id_t(3));
  pool.nonprimary_shards.insert(shard_id_t(4));
  pool.nonprimary_shards.insert(shard_id_t(5));

  ECUtil::stripe_info_t s(k, m, swidth, &pool);
  ECListenerStub listenerStub;
  // get_parent()->get_pool() must independently report supports_omap();
  // set the same nonprimary_shards on it as get_pool() is a separate
  // pg_pool_t instance from the one stripe_info_t was built from.
  listenerStub.pg_pool.size = 12;
  listenerStub.pg_pool.set_flag(pg_pool_t::FLAG_OMAP);

  MockErasureCode *ecode = new MockErasureCode(k, k + m);
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  // The primary is relative shard 0 of zone 1, i.e. absolute shard 6.
  listenerStub.whoami = pg_shard_t(6, shard_id_t(6));

  // Zone 1 (absolute shards 6-11) is fully up. Zone 0 (absolute shards
  // 0-5) is entirely down - none of its shards appear in acting_shards.
  for (int i = 6; i < 12; i++) {
    listenerStub.acting_shards.insert(pg_shard_t(i, shard_id_t(i)));
  }

  hobject_t hoid;
  ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());
  // Want only non-primary-capable data shards, so nothing in shard_reads
  // will already be able to serve the omap read.
  want_to_read[shard_id_t(1)].insert(0, align_size);
  want_to_read[shard_id_t(2)].insert(0, align_size);
  want_to_read[shard_id_t(3)].insert(0, align_size);

  ECCommon::read_request_t read_request(
      want_to_read, ECCommon::WantAttrs::No, ECCommon::WantOmapHeader::Yes,
      ECCommon::WantOmapKeys::Yes, "", 0, object_size);
  ECCommon::read_result_t read_result(&s);

  int r = pipeline.get_remaining_shards(
      hoid, read_result, read_request, /*for_recovery=*/false,
      /*want_attrs=*/false, /*want_omap_header=*/true,
      /*want_omap_keys=*/true);

  ASSERT_EQ(r, 0)
      << "omap read should succeed using the primary's own (zone 1) "
         "primary-capable shard, even though zone 0 is entirely down";

  // shard_reads should contain relative shard 0 (primary-capable),
  // mapped to the zone-1 primary's own pg_shard (absolute shard 6).
  ASSERT_TRUE(read_request.shard_reads.contains(shard_id_t(0)))
      << "expected a primary-capable shard to have been added for the "
         "omap read";
  EXPECT_EQ(read_request.shard_reads[shard_id_t(0)].pg_shard,
            pg_shard_t(6, shard_id_t(6)));
}
// get_all_avail_shards()'s acting/backfill passes guard have.insert(rel_shard)
// with "if (have.contains(rel_shard)) { ceph_assert(allow_remote_zone); ... }"
// because, for THOSE two containers, a collision on the same relative shard
// can only legitimately happen by crossing zones (within one zone,
// get_rel_shard_and_zone() is a bijection over the container's distinct
// shard ids). The missing_loc pass looks almost identical and also guards
// have.insert() with an "already have this shard" skip - but it must NOT
// carry that same ceph_assert(allow_remote_zone), because
// get_missing_loc_shards() has a different invariant: PG peering/recovery
// routinely lists more than one historical candidate OSD for the very same
// EC shard slot (e.g. an old and a new owner of the same shard id) for a
// single hoid, entirely within one zone. This is normal, single-zone,
// zone-unaware behaviour that predates zones altogether.
//
// This regression test locks in that the current code tolerates such a
// same-zone, same-relative-shard collision in missing_loc without
// asserting. It exists because the "obvious" consistency fix - making the
// missing_loc pass assert allow_remote_zone just like the acting/backfill
// passes above it - is actually wrong and crashes on exactly this scenario;
// see this commit's message for how that was demonstrated (ceph_assert
// added, test run, seen to abort, then reverted) before writing this test
// against the unmodified code.
TEST(ECCommon, get_all_avail_shards_missing_loc_same_zone_duplicate_shard) {
  const unsigned int k = 2;
  const unsigned int m = 1;
  const uint64_t swidth = 4096 * k;

  pg_pool_t pool;
  pool.size = k + m; // single zone (NUM_ZONES defaults to 1)

  ECUtil::stripe_info_t s(k, m, swidth, &pool);
  ECListenerStub listenerStub;
  ErasureCodeInterfaceRef ec_impl(new MockErasureCode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  // Shards 0 and 2 are acting and available; shard 1 is deliberately left
  // out of acting_shards, as if it were currently missing/down.
  listenerStub.acting_shards.insert(pg_shard_t(0, shard_id_t(0)));
  listenerStub.acting_shards.insert(pg_shard_t(2, shard_id_t(2)));

  hobject_t hoid;
  // Two different, still-in-zone-0 OSDs (5 and 9) are both listed as
  // candidate locations for shard 1 of this object - a routine situation
  // when an EC shard slot has changed owning OSD across intervals and the
  // old owner has not yet been ruled out.
  listenerStub.missing_loc_shards[hoid].insert(pg_shard_t(5, shard_id_t(1)));
  listenerStub.missing_loc_shards[hoid].insert(pg_shard_t(9, shard_id_t(1)));

  shard_id_set have;
  shard_id_map<pg_shard_t> shards(s.get_k_plus_m());

  // for_recovery=true is required to reach the missing_loc pass;
  // local_zone=0, allow_remote_zone=false: a single-zone pool has no
  // remote zone to fall back to, so this must not depend on it.
  pipeline.get_all_avail_shards(hoid, have, shards, /*for_recovery=*/true,
                                 /*local_zone=*/0, /*allow_remote_zone=*/false);

  ASSERT_TRUE(have.contains(shard_id_t(1)))
      << "shard 1 has two candidate locations in missing_loc; one of them "
         "should have been picked up";
  // Whichever candidate is picked, exactly one must win - the duplicate
  // must be silently skipped rather than overwriting or asserting.
  EXPECT_TRUE(shards[shard_id_t(1)] == pg_shard_t(5, shard_id_t(1)) ||
              shards[shard_id_t(1)] == pg_shard_t(9, shard_id_t(1)));
}
