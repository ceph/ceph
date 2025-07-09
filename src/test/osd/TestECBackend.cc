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
#include "osd/osd_types.h"
#include "common/ceph_argparse.h"
#include "erasure-code/ErasureCode.h"

using namespace std;

TEST(ECUtil, stripe_info_t)
{
  const uint64_t swidth = 4096;
  const unsigned int k = 4;
  const unsigned int m = 2;

  ECUtil::stripe_info_t s(k, m, swidth);
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
  ASSERT_EQ(s.chunk_aligned_shard_offset_to_ro_offset(2*s.get_chunk_size()),
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

class ErasureCodeDummyImpl : public ErasureCodeInterface {
public:

  uint64_t get_supported_optimizations() const override {
    return FLAG_EC_PLUGIN_PARTIAL_READ_OPTIMIZATION |
          FLAG_EC_PLUGIN_PARTIAL_WRITE_OPTIMIZATION |
          FLAG_EC_PLUGIN_ZERO_INPUT_ZERO_OUTPUT_OPTIMIZATION |
          FLAG_EC_PLUGIN_ZERO_PADDING_OPTIMIZATION |
          FLAG_EC_PLUGIN_PARITY_DELTA_OPTIMIZATION;
  }

  ErasureCodeProfile _profile;
  const std::vector<shard_id_t> chunk_mapping = {}; // no remapping
  std::vector<std::pair<int, int>> default_sub_chunk = {std::pair(0,1)};
  int data_chunk_count = 4;
  int chunk_count = 6;

  int init(ErasureCodeProfile &profile, std::ostream *ss) override {
    return 0;
  }

  const ErasureCodeProfile &get_profile() const override {
    return _profile;
  }

  int create_rule(const string &name, CrushWrapper &crush, std::ostream *ss) const override {
    return 0;
  }

  unsigned int get_chunk_count() const override {
    return chunk_count;
  }

  unsigned int get_data_chunk_count() const override {
    return data_chunk_count;
  }

  unsigned int get_coding_chunk_count() const override {
    return 0;
  }

  int get_sub_chunk_count() override {
    return 1;
  }

  unsigned int get_chunk_size(unsigned int stripe_width) const override {
    return 0;
  }

  int minimum_to_decode(const shard_id_set &want_to_read, const shard_id_set &available,
                        shard_id_set &minimum_set,
			shard_id_map<std::vector<std::pair<int, int>>> *minimum_sub_chunks) override {
    shard_id_t parity_shard_index(data_chunk_count);
    for (shard_id_t shard : want_to_read) {
      if (available.contains(shard)) {
        minimum_set.insert(shard);
      } else {
        // Shard is missing.  Recover with every other shard and one parity
        // for each missing shard.
        for (shard_id_t i; i<data_chunk_count; ++i) {
          if (available.contains(i)) {
            minimum_set.insert(i);
          } else {
            minimum_set.insert(parity_shard_index);
            ++parity_shard_index;
          }

          if (int(parity_shard_index) == chunk_count)
            return -EIO; // Cannot recover.
        }
      }
    }

    for (auto &&shard : minimum_set) {
      minimum_sub_chunks->emplace(shard, default_sub_chunk);
    }
    return 0;
  }

  [[deprecated]]
  int minimum_to_decode(const std::set<int> &want_to_read,
    const std::set<int> &available,
    std::map<int, std::vector<std::pair<int, int>>> *minimum) override
  {
    ADD_FAILURE();
    return 0;
  }

  [[deprecated]]
  int minimum_to_decode_with_cost(const std::set<int> &want_to_read,
      const std::map<int, int> &available, std::set<int> *minimum) override {
    ADD_FAILURE();
    return 0;
  }

  int minimum_to_decode_with_cost(const shard_id_set &want_to_read, const shard_id_map<int> &available,
                                shard_id_set *minimum) override {
    return 0;
  }

  int encode(const shard_id_set &want_to_encode, const bufferlist &in, shard_id_map<bufferlist> *encoded) override {
    return 0;
  }

  [[deprecated]]
  int encode(const std::set<int> &want_to_encode, const bufferlist &in
    , std::map<int, bufferlist> *encoded) override
  {
    ADD_FAILURE();
    return 0;
  }

  [[deprecated]]
  int encode_chunks(const std::set<int> &want_to_encode,
                    std::map<int, bufferlist> *encoded) override
  {
    ADD_FAILURE();
    return 0;
  }

  int encode_chunks(const shard_id_map<bufferptr> &in, shard_id_map<bufferptr> &out) override {
    return 0;
  }

  int decode(const shard_id_set &want_to_read, const shard_id_map<bufferlist> &chunks, shard_id_map<bufferlist> *decoded,
	     int chunk_size) override {
    return 0;
  }

  [[deprecated]]
  int decode(const std::set<int> &want_to_read, const std::map<int, bufferlist> &chunks,
    std::map<int, bufferlist> *decoded, int chunk_size) override
  {
    ADD_FAILURE();
    return 0;
  }

  [[deprecated]]
  int decode_chunks(const std::set<int> &want_to_read,
                    const std::map<int, bufferlist> &chunks,
                    std::map<int, bufferlist> *decoded) override {
    ADD_FAILURE();
    return 0;
  }

  int decode_chunks(const shard_id_set &want_to_read,
                    shard_id_map<bufferptr> &in, shard_id_map<bufferptr> &out) override
  {
    if (in.size() < data_chunk_count) {
      ADD_FAILURE();
    }
    uint64_t len = 0;
    for (auto &&[shard, bp] : in) {
      if (len == 0) {
        len = bp.length();
      } else if (len != bp.length()) {
        ADD_FAILURE();
      }
    }
    if (len == 0) {
      ADD_FAILURE();
    }
    if (out.size() == 0) {
      ADD_FAILURE();
    }
    for (auto &&[shard, bp] : out) {
      if (len != bp.length()) {
        ADD_FAILURE();
      }
    }
    return 0;
  }

  const vector<shard_id_t> &get_chunk_mapping() const override {
    return chunk_mapping;
  }

  [[deprecated]]
  int decode_concat(const std::set<int> &want_to_read,
                    const std::map<int, bufferlist> &chunks, bufferlist *decoded) override {
    ADD_FAILURE();
    return 0;
  }

  [[deprecated]]
  int decode_concat(const std::map<int, bufferlist> &chunks,
                    bufferlist *decoded) override {
    ADD_FAILURE();
    return 0;
  }

  size_t get_minimum_granularity() override { return 0; }
  void encode_delta(const bufferptr &old_data, const bufferptr &new_data
    , bufferptr *delta) override {}
  void apply_delta(const shard_id_map<bufferptr> &in
    , shard_id_map<bufferptr> &out) override {}
};

class ECListenerStub : public ECListener {
  OSDMapRef osd_map_ref;
  pg_info_t pg_info;
  set<pg_shard_t> backfill_shards;
  shard_id_set backfill_shard_id_set;
  map<hobject_t, set<pg_shard_t>> missing_loc_shards;
  map<pg_shard_t, pg_missing_t> shard_missing;
  pg_missing_set<false> shard_not_missing_const;
  pg_pool_t pg_pool;
  set<pg_shard_t> acting_recovery_backfill_shards;
  shard_id_set acting_recovery_backfill_shard_id_set;
  map<pg_shard_t, pg_info_t> shard_info;
  PGLog pg_log;
  pg_info_t shard_pg_info;
  std::string dbg_prefix = "stub";

public:
  set<pg_shard_t> acting_shards;

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
    return pg_shard_t();
  }

  void send_message_osd_cluster(vector<std::pair<int, Message *>> &messages, epoch_t from_epoch) override {

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

  bool should_send_op(pg_shard_t peer, const hobject_t &hoid) override {
    return false;
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
  ErasureCodeInterfaceRef ec_impl(new ErasureCodeDummyImpl);
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
  ErasureCodeDummyImpl *ecode = new ErasureCodeDummyImpl();
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
    ECCommon::read_request_t read_request(to_read_list, false, object_size);
    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(to_read_list, false, object_size);

    ASSERT_EQ(read_request,  ref);
  }

  /* Read to every data shard. */
  {
    ECUtil::shard_extent_set_t to_read_list(s.get_k_plus_m());
    hobject_t hoid;

    for (shard_id_t i; i<k; ++i) {
      to_read_list[i].insert(int(i) * 2 * align_size, align_size);
    }

    ECCommon::read_request_t read_request(to_read_list, false, object_size);
    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(to_read_list, false, object_size);
    for (shard_id_t shard_id; shard_id < k; ++shard_id) {
      ref.shard_reads[shard_id].extents = to_read_list[shard_id];
      ref.shard_reads[shard_id].subchunk = ecode->default_sub_chunk;
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

    ECCommon::read_request_t read_request(to_read_list, false, object_size);


    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(to_read_list, false, object_size);
    for (shard_id_t i; i<k; ++i) {
      shard_id_t shard_id(i);
      ref.shard_reads[shard_id].extents = to_read_list[i];
      ref.shard_reads[shard_id].subchunk = ecode->default_sub_chunk;
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
    ECCommon::read_request_t ref(to_read_list, false, object_size);
    ECCommon::read_request_t read_request(to_read_list, false, object_size);
    for (int i=0; i < (int)k; i++) {
      shard_id_t shard_id(i);
      ECCommon::shard_read_t &ref_shard_read = ref.shard_reads[shard_id];
      ref_shard_read.subchunk = ecode->default_sub_chunk;
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

    ECCommon::read_request_t read_request(to_read_list, false, object_size);

    shard_id_t missing_shard(1);
    int parity_shard = k;
    listenerStub.acting_shards.erase(pg_shard_t(int(missing_shard), shard_id_t(missing_shard)));

    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(to_read_list, false, object_size);
    for (shard_id_t i; i<k; ++i) {
      if (i != missing_shard) {
        shard_id_t shard_id(i);
	to_read_list[i].union_of(to_read_list[missing_shard]);
        ref.shard_reads[shard_id].subchunk = ecode->default_sub_chunk;
	ref.shard_reads[shard_id].extents = to_read_list[i];
        ref.shard_reads[shard_id].pg_shard = pg_shard_t(int(i), shard_id);
      } else {
	ECCommon::shard_read_t parity_shard_read;
	parity_shard_read.subchunk = ecode->default_sub_chunk;
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
    ECCommon::read_request_t read_request(to_read_list, false, object_size);
    ECCommon::read_request_t ref(to_read_list, false, object_size);

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
      ref.shard_reads[shard_id_t(i)].subchunk = ecode->default_sub_chunk;
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
    ECCommon::read_request_t read_request(to_read_list, false, object_size);

    pipeline.get_min_avail_to_read_shards(hoid, false, true, read_request);

    ECCommon::read_request_t ref(to_read_list, false, object_size);
    for (unsigned int i=0; i<k+2; i++) {
      ECCommon::shard_read_t shard_read;
      shard_read.subchunk = ecode->default_sub_chunk;
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
    ECCommon::read_request_t read_request(to_read_list, false, object_size);

    shard_id_t missing_shard(1);
    int parity_shard = k;
    std::set<pg_shard_t> error_shards;
    error_shards.emplace(int(missing_shard), shard_id_t(missing_shard));
    // Similar to previous tests with missing shards, but this time, emulate
    // the shard being missing as a result of a bad read.
    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request, error_shards);

    ECCommon::read_request_t ref(to_read_list, false, object_size);
    std::vector<ECCommon::shard_read_t> want_to_read(empty_shard_vector);
    for (shard_id_t i; i<k; ++i) {
      if (i != missing_shard) {
        want_to_read[int(i)].subchunk = ecode->default_sub_chunk;
        want_to_read[int(i)].extents.union_of(to_read_list[missing_shard]);
        want_to_read[int(i)].extents.union_of(to_read_list[i]);
        want_to_read[int(i)].pg_shard = pg_shard_t(int(i), shard_id_t(i));
        ref.shard_reads[shard_id_t(i)] = want_to_read[int(i)];
      } else {
        ECCommon::shard_read_t parity_shard_read;
        parity_shard_read.subchunk = ecode->default_sub_chunk;
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
  ErasureCodeDummyImpl *ecode = new ErasureCodeDummyImpl();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  for (int i = 0; i < nshards; i++) {
    listenerStub.acting_shards.insert(pg_shard_t(i, shard_id_t(i)));
  }

  {
    ECUtil::shard_extent_set_t want_to_read(s.get_k_plus_m());

    ec_align_t to_read(36*1024,10*1024, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECCommon::read_request_t read_request(want_to_read, false, object_size);

    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(want_to_read, false, object_size);
    {
      ECCommon::shard_read_t shard_read;
      shard_read.subchunk = ecode->default_sub_chunk;
      shard_read.extents.insert(20*1024, 4*1024);
      shard_read.pg_shard = pg_shard_t(0, shard_id_t(0));
      ref.shard_reads[shard_id_t(0)] = shard_read;
    }
    {
      ECCommon::shard_read_t shard_read;
      shard_read.subchunk = ecode->default_sub_chunk;
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
    ECCommon::read_request_t read_request(want_to_read, false, object_size);
    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(want_to_read, false, object_size);
    {
      ECCommon::shard_read_t shard_read;
      shard_read.subchunk = ecode->default_sub_chunk;
      shard_read.extents.insert(8*1024, 4*1024);
      shard_read.pg_shard = pg_shard_t(0, shard_id_t(0));
      ref.shard_reads[shard_id_t(0)] = shard_read;
    }
    {
      ECCommon::shard_read_t shard_read;
      shard_read.subchunk = ecode->default_sub_chunk;
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
  ErasureCodeInterfaceRef ec_impl(new ErasureCodeDummyImpl);
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
  ErasureCodeDummyImpl *ecode = new ErasureCodeDummyImpl();
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
    ECCommon::read_request_t read_request(to_read, false, object_size);
    int missing_shard = 0;

    // Mock up a read result.
    ECCommon::read_result_t read_result(&s);
    read_result.errors.emplace(pg_shards[missing_shard], -EIO);

    pipeline.get_remaining_shards(hoid, read_result, read_request, false, false);

    ECCommon::read_request_t ref(to_read, false, object_size);
    int parity_shard = 4;
    for (unsigned int i=0; i<k; i++) {
      ECCommon::shard_read_t shard_read;
      shard_read.subchunk = ecode->default_sub_chunk;
      shard_read.extents.insert(0,4096);
      unsigned int shard_id = i==missing_shard?parity_shard:i;
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
    ECCommon::read_request_t read_request(to_read, false, object_size);
    unsigned int missing_shard = 1;

    // Mock up a read result.
    ECCommon::read_result_t read_result(&s);
    read_result.errors.emplace(pg_shards[missing_shard], -EIO);
    buffer::list bl;
    bl.append_zero(chunk_size/2);
    read_result.buffers_read.insert_in_shard(shard_id_t(0), chunk_size/2, bl);
    read_result.processed_read_requests[shard_id_t(0)].insert(chunk_size/2, bl.length());

    pipeline.get_remaining_shards(hoid, read_result, read_request, false, false);

    // The result should be a read request for the first 4k of shard 0, as that
    // is currently missing.
    ECCommon::read_request_t ref(to_read, false, object_size);
    int parity_shard = 4;
    for (unsigned int i=0; i<k; i++) {
      ECCommon::shard_read_t shard_read;
      shard_read.subchunk = ecode->default_sub_chunk;
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
  ErasureCodeDummyImpl *ecode = new ErasureCodeDummyImpl();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  ECUtil::shard_extent_map_t semap(&s);

  for (shard_id_t i; i<k+m; ++i) {
    bufferlist bl;
    bl.append_zero(i>=k?4096:2048);
    semap.insert_in_shard(i, 12*1024, bl);
  }
  semap.encode(ec_impl, nullptr, 0);
}

TEST(ECCommon, decode)
{
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = 3*align_size;
  const unsigned int k = 3;
  const unsigned int m = 2;

  ECUtil::stripe_info_t s(k, m, swidth, vector<shard_id_t>(0));
  ECListenerStub listenerStub;
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), swidth/k);

  const std::vector<int> chunk_mapping = {}; // no remapping
  ErasureCodeDummyImpl *ecode = new ErasureCodeDummyImpl();
  ecode->data_chunk_count = k;
  ecode->chunk_count = k + m;
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  ECUtil::shard_extent_map_t semap(&s);
  bufferlist bl12k;
  bl12k.append_zero(12288);
  bufferlist bl8k;
  bl8k.append_zero(8192);
  bufferlist bl16k;
  bl16k.append_zero(16384);
  semap.insert_in_shard(shard_id_t(1), 512000, bl12k);
  semap.insert_in_shard(shard_id_t(1), 634880, bl12k);
  semap.insert_in_shard(shard_id_t(2), 512000, bl12k);
  semap.insert_in_shard(shard_id_t(2), 630784, bl16k);
  semap.insert_in_shard(shard_id_t(3), 516096, bl8k);
  semap.insert_in_shard(shard_id_t(3), 634880, bl12k);
  ECUtil::shard_extent_set_t want = semap.get_extent_set();

  want[shard_id_t(0)].insert(516096, 8192);
  want[shard_id_t(0)].insert(634880, 12288);
  want[shard_id_t(4)].insert(516096, 8192);
  want[shard_id_t(4)].insert(634880, 12288);

  ceph_assert(0 == semap.decode(ec_impl, want, 2*1024*1024));
}


TEST(ECCommon, decode2)
{
  const unsigned int k = 4;
  const unsigned int m = 2;
  const uint64_t align_size = EC_ALIGN_SIZE;
  const uint64_t swidth = k*align_size;


  ECUtil::stripe_info_t s(k, m, swidth, vector<shard_id_t>(0));
  ECListenerStub listenerStub;
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), swidth/k);

  const std::vector<int> chunk_mapping = {}; // no remapping
  ErasureCodeDummyImpl *ecode = new ErasureCodeDummyImpl();
  ecode->data_chunk_count = k;
  ecode->chunk_count = k + m;
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  ECUtil::shard_extent_map_t semap(&s);
  bufferlist bl528k;
  bl528k.append_zero(528*1024);
  bufferlist bl524k;
  bl524k.append_zero(524*1024);
  semap.insert_in_shard(shard_id_t(0), 0, bl524k);
  semap.insert_in_shard(shard_id_t(1), 0, bl528k);
  semap.insert_in_shard(shard_id_t(3), 0, bl524k);
  semap.insert_in_shard(shard_id_t(4), 0, bl528k);
  ECUtil::shard_extent_set_t want(k + m);

  //shard_want_to_read={1:[0~540672],2:[0~536576],3:[0~536576],4:[0~540672],5:[0~540672]}
  want[shard_id_t(1)].insert(0, 528*1024);
  want[shard_id_t(2)].insert(0, 524*1024);
  want[shard_id_t(3)].insert(0, 524*1024);
  want[shard_id_t(4)].insert(0, 528*1024);
  want[shard_id_t(5)].insert(0, 528*1024);

  ceph_assert(0 == semap.decode(ec_impl, want, 2104*1024));
}