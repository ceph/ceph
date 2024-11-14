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

using namespace std;

TEST(ECUtil, stripe_info_t)
{
  const uint64_t swidth = 4096;
  const uint64_t ssize = 4;

  ECUtil::stripe_info_t s(ssize, swidth, 0);
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

  // Stripe 1 + 1 chunk for 10 stripes needs to read 11 stripes starting
  // from 1 because there is a partial stripe at the start and end
  ASSERT_EQ(s.chunk_aligned_offset_len_to_chunk(swidth+s.get_chunk_size(), 10*swidth),
	    make_pair(s.get_chunk_size(), 11*s.get_chunk_size()));

  // Stripe 1 + 0 chunks for 10 stripes needs to read 10 stripes starting
  // from 1 because there are no partial stripes
  ASSERT_EQ(s.chunk_aligned_offset_len_to_chunk(swidth, 10*swidth),
	    make_pair(s.get_chunk_size(), 10*s.get_chunk_size()));

  // Stripe 0 + 1 chunk for 10 stripes needs to read 11 stripes starting
  // from 0 because there is a partial stripe at the start and end
  ASSERT_EQ(s.chunk_aligned_offset_len_to_chunk(s.get_chunk_size(), 10*swidth),
	    make_pair<uint64_t>(0, 11*s.get_chunk_size()));

  // Stripe 0 + 1 chunk for (10 stripes + 1 chunk) needs to read 11 stripes
  // starting from 0 because there is a partial stripe at the start and end
  ASSERT_EQ(s.chunk_aligned_offset_len_to_chunk(s.get_chunk_size(),
							  10*swidth + s.get_chunk_size()),
	    make_pair<uint64_t>(0, 11*s.get_chunk_size()));

  // Stripe 0 + 2 chunks for (10 stripes + 2 chunks) needs to read 11 stripes
  // starting from 0 because there is a partial stripe at the start
  ASSERT_EQ(s.chunk_aligned_offset_len_to_chunk(2*s.get_chunk_size(),
    10*swidth + 2*s.get_chunk_size()),
    make_pair<uint64_t>(0, 11*s.get_chunk_size()));

  ASSERT_EQ(s.offset_len_to_stripe_bounds(swidth-10, (uint64_t)20),
            make_pair((uint64_t)0, 2*swidth));
}

TEST(ECUtil, offset_length_is_same_stripe)
{
  const uint64_t swidth = 4096;
  const uint64_t schunk = 1024;
  const uint64_t ssize = 4;

  ECUtil::stripe_info_t s(ssize, swidth, 0);
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), schunk);

  // read nothing at the very beginning
  //   +---+---+---+---+
  //   |  0|   |   |   |
  //   +---+---+---+---+
  //   |   |   |   |   |
  //   +---+---+---+---+
  ASSERT_TRUE(s.offset_length_is_same_stripe(0, 0));

  // read nothing at the stripe end
  //   +---+---+---+---+
  //   |   |   |   |  0|
  //   +---+---+---+---+
  //   |   |   |   |   |
  //   +---+---+---+---+
  ASSERT_TRUE(s.offset_length_is_same_stripe(swidth, 0));

  // read single byte at the stripe end
  //   +---+---+---+---+
  //   |   |   |   | ~1|
  //   +---+---+---+---+
  //   |   |   |   |   |
  //   +---+---+---+---+
  ASSERT_TRUE(s.offset_length_is_same_stripe(swidth - 1, 1));

  // read single stripe
  //   +---+---+---+---+
  //   | 1k| 1k| 1k| 1k|
  //   +---+---+---+---+
  //   |   |   |   |   |
  //   +---+---+---+---+
  ASSERT_TRUE(s.offset_length_is_same_stripe(0, swidth));

  // read single chunk
  //   +---+---+---+---+
  //   | 1k|   |   |   |
  //   +---+---+---+---+
  //   |   |   |   |   |
  //   +---+---+---+---+
  ASSERT_TRUE(s.offset_length_is_same_stripe(0, schunk));

  // read single stripe except its first chunk
  //   +---+---+---+---+
  //   |   | 1k| 1k| 1k|
  //   +---+---+---+---+
  //   |   |   |   |   |
  //   +---+---+---+---+
  ASSERT_TRUE(s.offset_length_is_same_stripe(schunk, swidth - schunk));

  // read two stripes
  //   +---+---+---+---+
  //   | 1k| 1k| 1k| 1k|
  //   +---+---+---+---+
  //   | 1k| 1k| 1k| 1k|
  //   +---+---+---+---+
  ASSERT_FALSE(s.offset_length_is_same_stripe(0, 2*swidth));

  // multistripe read: 1st stripe without 1st byte + 1st byte of 2nd stripe
  //   +-----+---+---+---+
  //   | 1k-1| 1k| 1k| 1k|
  //   +-----+---+---+---+
  //   |    1|   |   |   |
  //   +-----+---+---+---+
  ASSERT_FALSE(s.offset_length_is_same_stripe(1, swidth));
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
  const std::vector<int> chunk_mapping = {}; // no remapping
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

  int minimum_to_decode(const set<int> &want_to_read, const set<int> &available,
			std::map<int, std::vector<std::pair<int, int>>> *minimum) override {
    int parity_shard_index = data_chunk_count;
    for (int shard : want_to_read) {
      if (available.contains(shard)) {
	(*minimum)[shard] = default_sub_chunk;
      } else {
        // Shard is missing.  Recover with every other shard and one parity
        // for each missing shard.
        for (int i=0; i<data_chunk_count; i++) {
          if (available.contains(i))
            (*minimum)[i] = default_sub_chunk;
          else
            (*minimum)[parity_shard_index++] = default_sub_chunk;

          if (parity_shard_index == chunk_count)
            return -EIO; // Cannot recover.
        }
      }
    }
    return 0;
  }

  int minimum_to_decode_with_cost(const set<int> &want_to_read, const map<int, int> &available,
				  std::set<int> *minimum) override {
    return 0;
  }

  int encode(const set<int> &want_to_encode, const bufferlist &in, std::map<int, bufferlist> *encoded) override {
    return 0;
  }

  int encode_chunks(const set<int> &want_to_encode, std::map<int, bufferlist> *encoded) override {
    return 0;
  }

  int decode(const set<int> &want_to_read, const map<int, bufferlist> &chunks, std::map<int, bufferlist> *decoded,
	     int chunk_size) override {
    return 0;
  }

  int decode_chunks(const set<int> &want_to_read, const map<int, bufferlist> &chunks,
		    std::map<int, bufferlist> *decoded) override {
    return 0;
  }

  const vector<int> &get_chunk_mapping() const override {
    return chunk_mapping;
  }

  int decode_concat(const set<int> &want_to_read, const map<int, bufferlist> &chunks, bufferlist *decoded) override {
    return 0;
  }

  int decode_concat(const map<int, bufferlist> &chunks, bufferlist *decoded) override {
    return 0;
  }

};

class ECListenerStub : public ECListener {
  OSDMapRef osd_map_ref;
  pg_info_t pg_info;
  set<pg_shard_t> backfill_shards;
  map<hobject_t, set<pg_shard_t>> missing_loc_shards;
  map<pg_shard_t, pg_missing_t> shard_missing;
  pg_missing_set<false> shard_not_missing_const;
  pg_pool_t pg_pool;
  set<pg_shard_t> acting_recovery_backfill_shards;
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
};

TEST(ECCommon, get_min_want_to_read_shards)
{
  const uint64_t swidth = 4096;
  const uint64_t ssize = 4;
  const uint64_t csize = 1024;

  ECUtil::stripe_info_t s(ssize, swidth, 0);
  ECListenerStub listenerStub;
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), csize);

  const std::vector<int> chunk_mapping = {}; // no remapping
  ErasureCodeInterfaceRef ec_impl(new ErasureCodeDummyImpl);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  std::map<int, extent_set> empty_extent_set_map;

  // read nothing at the very beginning
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(0, 0, 0);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ASSERT_EQ(want_to_read,  empty_extent_set_map);
  }

  // read nothing at the middle (0-sized partial read)
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(2048, 0, 0);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ASSERT_EQ(want_to_read,  empty_extent_set_map);
  }
  // read nothing at the the second stripe (0-sized partial read)
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(swidth, 0, 0);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ASSERT_EQ(want_to_read,  empty_extent_set_map);
  }

  // read not-so-many (< chunk_size) bytes at the middle (partial read)
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(2048, 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;
    ref[2].insert(0, 42);
    ASSERT_EQ(want_to_read, ref);
  }

  // read not-so-many (< chunk_size) bytes after the first stripe.
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(swidth+2048, 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;
    ref[2].insert(csize, 42);
    ASSERT_EQ(want_to_read, ref);
  }

  // read more (> chunk_size) bytes at the middle (partial read)
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(csize, csize + 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;
    ref[1].insert(0, csize);
    ref[2].insert(0, 42);
    ASSERT_EQ(want_to_read, ref);
  }

  // read more (> chunk_size) bytes at the middle (partial read), second stripe
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(swidth + csize, csize + 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;
    ref[1].insert(csize, csize);
    ref[2].insert(csize, 42);
    ASSERT_EQ(want_to_read, ref);
  }

  // full stripe except last chunk
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(0, 3*csize, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;
    ref[0].insert(0, csize);
    ref[1].insert(0, csize);
    ref[2].insert(0, csize);
    ASSERT_EQ(want_to_read, ref);
  }

  // full stripe except last chunk (second stripe)
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(swidth, 3*csize, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;
    ref[0].insert(csize, csize);
    ref[1].insert(csize, csize);
    ref[2].insert(csize, csize);
    ASSERT_EQ(want_to_read, ref);
  }

  // full stripe except 1st chunk
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(csize, swidth - csize, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;
    ref[1].insert(0, csize);
    ref[2].insert(0, csize);
    ref[3].insert(0, csize);
    ASSERT_EQ(want_to_read, ref);
  }

  // full stripe except 1st chunk (second stripe)
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(swidth + csize, swidth - csize, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;
    ref[1].insert(csize, csize);
    ref[2].insert(csize, csize);
    ref[3].insert(csize, csize);
    ASSERT_EQ(want_to_read, ref);
  }

  // large, multi-stripe read starting just after 1st chunk
  // 0XXX
  // XXXX x41
  // X000
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(csize, swidth * 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;
    ref[0].insert(csize, csize*42);
    ref[1].insert(0, csize*42);
    ref[2].insert(0, csize*42);
    ref[3].insert(0, csize*42);
    ASSERT_EQ(want_to_read, ref);
  }

  // large, multi-stripe read starting just after 1st chunk (second stripe)
  // 0XXX
  // XXXX x41
  // X000
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(swidth + csize, swidth * 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;
    ref[0].insert(csize*2, csize*42);
    ref[1].insert(csize, csize*42);
    ref[2].insert(csize, csize*42);
    ref[3].insert(csize, csize*42);
    ASSERT_EQ(want_to_read, ref);
  }

  // large read from the beginning
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(0, swidth * 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;

    ref[0].insert(0, csize*42);
    ref[1].insert(0, csize*42);
    ref[2].insert(0, csize*42);
    ref[3].insert(0, csize*42);
    ASSERT_EQ(want_to_read, ref);
  }

  // large read from the beginning
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(0, swidth * 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;

    ref[0].insert(0, csize*42);
    ref[1].insert(0, csize*42);
    ref[2].insert(0, csize*42);
    ref[3].insert(0, csize*42);
    ASSERT_EQ(want_to_read, ref);
  }

  // large read from the beginning (second stripe)
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(swidth, swidth * 42, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;

    ref[0].insert(csize, csize*42);
    ref[1].insert(csize, csize*42);
    ref[2].insert(csize, csize*42);
    ref[3].insert(csize, csize*42);
    ASSERT_EQ(want_to_read, ref);
  }

  // large read that starts and ends on same shard.
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(swidth, swidth+csize/2, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;

    ref[0].insert(csize, csize+csize/2);
    ref[1].insert(csize, csize);
    ref[2].insert(csize, csize);
    ref[3].insert(csize, csize);
    ASSERT_EQ(want_to_read, ref);
  }

  // large read that starts and ends on last shard
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(swidth-csize, swidth+csize/2, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;

    ref[0].insert(csize, csize);
    ref[1].insert(csize, csize);
    ref[2].insert(csize, csize);
    ref[3].insert(0, csize+csize/2);
    ASSERT_EQ(want_to_read, ref);
  }
  // large read that starts and ends on last shard, partial first shard.
  {
    std::map<int, extent_set> want_to_read;
    ECCommon::ec_align_t to_read(swidth-csize/2, swidth, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    std::map<int, extent_set> ref;

    ref[0].insert(csize, csize);
    ref[1].insert(csize, csize);
    ref[2].insert(csize, csize);
    ref[3].insert(csize/2, csize);
    ASSERT_EQ(want_to_read, ref);
  }
}

TEST(ECCommon, get_min_avail_to_read_shards) {
  const uint64_t page_size = CEPH_PAGE_SIZE;
  const uint64_t swidth = 64*page_size;
  const uint64_t ssize = 4;
  const int nshards = 6;

  std::vector<ECCommon::shard_read_t> empty_shard_vector(ssize);

  bool old_osd_ec_partial_reads_experimental =
    g_ceph_context->_conf->osd_ec_partial_reads_experimental;
  g_ceph_context->_conf->osd_ec_partial_reads_experimental = true;

  ECUtil::stripe_info_t s(ssize, swidth, 0, vector<int>(0));
  ECListenerStub listenerStub;
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), swidth/ssize);

  const std::vector<int> chunk_mapping = {}; // no remapping
  ErasureCodeDummyImpl *ecode = new ErasureCodeDummyImpl();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  for (int i = 0; i < nshards; i++) {
    listenerStub.acting_shards.insert(pg_shard_t(i, shard_id_t(i)));
  }

  // read nothing
  {
    std::map<int, extent_set> want_to_read;
    std::map<int, extent_set> to_read_list;
    hobject_t hoid;
    ECCommon::read_request_t read_request(to_read_list, false);
    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(to_read_list, false);

    ASSERT_EQ(read_request,  ref);
  }

  /* Read to every data shard. */
  {
    std::map<int, extent_set> to_read_list;
    hobject_t hoid;

    for (unsigned int i=0; i<ssize; i++) {
      to_read_list[i].insert(i*2*page_size, page_size);
    }

    ECCommon::read_request_t read_request(to_read_list, false);
    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(to_read_list, false);
    for (unsigned int i=0; i<ssize; i++) {
      ref.shard_reads[pg_shard_t(i, shard_id_t(i))].extents = to_read_list[i];
      ref.shard_reads[pg_shard_t(i, shard_id_t(i))].subchunk = ecode->default_sub_chunk;
    }
    ASSERT_EQ(read_request,  ref);
  }

  /* Read to every data shard. */
  {
    std::map<int, extent_set> to_read_list;
    hobject_t hoid;
    for (unsigned int i=0; i<ssize; i++) {
      to_read_list[i].insert(i*2*page_size, page_size);
    }

    ECCommon::read_request_t read_request(to_read_list, false);


    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(to_read_list, false);
    for (unsigned int i=0; i<ssize; i++) {
      ref.shard_reads[pg_shard_t(i, shard_id_t(i))].extents = to_read_list[i];
      ref.shard_reads[pg_shard_t(i, shard_id_t(i))].subchunk = ecode->default_sub_chunk;
    }

    ASSERT_EQ(read_request,  ref);
  }


  /* Read to every data shard - small read */
  {
    std::map<int, extent_set> to_read_list;
    hobject_t hoid;

    for (int i=0; i < (int)ssize; i++) {
      to_read_list[i].insert(i*2*page_size + i + 1, i+1);
    }
    ECCommon::read_request_t ref(to_read_list, false);
    ECCommon::read_request_t read_request(to_read_list, false);
    for (int i=0; i < (int)ssize; i++) {
      ECCommon::shard_read_t &ref_shard_read = ref.shard_reads[pg_shard_t(i, shard_id_t(i))];
      ref_shard_read.subchunk = ecode->default_sub_chunk;
      ref_shard_read.extents.insert(i*2*page_size, page_size);
    }

    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);
    ASSERT_EQ(read_request,  ref);
  }

  /* Read to every data shard, missing shard. */
  {
    std::map<int, extent_set> to_read_list;
    hobject_t hoid;

    for (unsigned int i=0; i<ssize; i++) {
      to_read_list[i].insert(i*2*page_size, page_size);
    }

    ECCommon::read_request_t read_request(to_read_list, false);

    unsigned int missing_shard = 1;
    int parity_shard = ssize;
    listenerStub.acting_shards.erase(pg_shard_t(missing_shard, shard_id_t(missing_shard)));

    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(to_read_list, false);
    for (unsigned int i=0; i<ssize; i++) {
      if (i != missing_shard) {
	to_read_list[i].union_of(to_read_list[missing_shard]);
        ref.shard_reads[pg_shard_t(i, shard_id_t(i))].subchunk = ecode->default_sub_chunk;
	ref.shard_reads[pg_shard_t(i, shard_id_t(i))].extents = to_read_list[i];
      } else {
	ECCommon::shard_read_t parity_shard_read;
	parity_shard_read.subchunk = ecode->default_sub_chunk;
	parity_shard_read.extents.union_of(to_read_list[i]);
	ref.shard_reads[pg_shard_t(parity_shard, shard_id_t(parity_shard))] = parity_shard_read;
      }
    }

    ASSERT_EQ(read_request,  ref);

    listenerStub.acting_shards.insert(pg_shard_t(1, shard_id_t(1)));
  }


  /* Read to every data shard, missing shard, missing shard is adjacent. */
  {
    std::map<int, extent_set> to_read_list;
    hobject_t hoid;
    unsigned int missing_shard = 1;

    to_read_list[0].insert(0, page_size);
    to_read_list[1].insert(page_size, page_size);
    to_read_list[2].insert(2*page_size, page_size);
    to_read_list[3].insert(3*page_size, page_size);
    ECCommon::read_request_t read_request(to_read_list, false);
    ECCommon::read_request_t ref(to_read_list, false);

    // Populating reference manually to check that adjacent shards get correctly combined.
    ref.shard_reads[pg_shard_t(0, shard_id_t(0))].extents.insert(0, page_size*2);
    ref.shard_reads[pg_shard_t(2, shard_id_t(2))].extents.insert(page_size, page_size*2);
    ref.shard_reads[pg_shard_t(3, shard_id_t(3))].extents.insert(page_size, page_size);
    ref.shard_reads[pg_shard_t(3, shard_id_t(3))].extents.insert(3*page_size, page_size);
    ref.shard_reads[pg_shard_t(4, shard_id_t(4))].extents.insert(page_size, page_size);

    for (unsigned int i=0; i<ssize+1; i++) {
      if (i==missing_shard) {
	continue;
      }
      ref.shard_reads[pg_shard_t(i, shard_id_t(i))].subchunk = ecode->default_sub_chunk;
    }

    listenerStub.acting_shards.erase(pg_shard_t(missing_shard, shard_id_t(missing_shard)));

    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ASSERT_EQ(read_request,  ref);

    listenerStub.acting_shards.insert(pg_shard_t(1, shard_id_t(1)));
  }

  /* Read to every data shard, but with "fast" (redundant) reads */
  {
    std::map<int, extent_set> to_read_list;
    hobject_t hoid;

    extent_set extents_to_read;
    for (unsigned int i=0; i<ssize; i++) {
      to_read_list[i].insert(i*2*page_size, page_size);
      extents_to_read.insert(i*2*page_size, page_size);
    }
    ECCommon::read_request_t read_request(to_read_list, false);

    pipeline.get_min_avail_to_read_shards(hoid, false, true, read_request);

    ECCommon::read_request_t ref(to_read_list, false);
    for (unsigned int i=0; i<ssize+2; i++) {
      ECCommon::shard_read_t shard_read;
      shard_read.subchunk = ecode->default_sub_chunk;
      shard_read.extents = extents_to_read;
      ref.shard_reads[pg_shard_t(i, shard_id_t(i))] = shard_read;
    }

    ASSERT_EQ(read_request,  ref);
  }

  /* Read to every data shard, missing shard. */
  {
    std::map<int, extent_set> to_read_list;
    hobject_t hoid;

    for (unsigned int i=0; i<ssize; i++) {
      to_read_list[i].insert(i*2*page_size, page_size);
    }
    ECCommon::read_request_t read_request(to_read_list, false);

    unsigned int missing_shard = 1;
    int parity_shard = ssize;
    std::set<pg_shard_t> error_shards;
    error_shards.emplace(missing_shard, shard_id_t(missing_shard));
    // Similar to previous tests with missing shards, but this time, emulate
    // the shard being missing as a result of a bad read.
    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request, error_shards);

    ECCommon::read_request_t ref(to_read_list, false);
    std::vector<ECCommon::shard_read_t> want_to_read(empty_shard_vector);
    for (unsigned int i=0; i<ssize; i++) {
      if (i != missing_shard) {
        want_to_read[i].subchunk = ecode->default_sub_chunk;
        want_to_read[i].extents.union_of(to_read_list[missing_shard]);
        want_to_read[i].extents.union_of(to_read_list[i]);
        ref.shard_reads[pg_shard_t(i, shard_id_t(i))] = want_to_read[i];
      } else {
        ECCommon::shard_read_t parity_shard_read;
        parity_shard_read.subchunk = ecode->default_sub_chunk;
        parity_shard_read.extents.union_of(to_read_list[missing_shard]);
        ref.shard_reads[pg_shard_t(parity_shard, shard_id_t(parity_shard))] = parity_shard_read;
      }
    }

    ASSERT_EQ(read_request,  ref);

    listenerStub.acting_shards.insert(pg_shard_t(1, shard_id_t(1)));
  }


  g_ceph_context->_conf->osd_ec_partial_reads_experimental =
    old_osd_ec_partial_reads_experimental;
}

TEST(ECCommon, shard_read_combo_tests)
{
  const uint64_t page_size = CEPH_PAGE_SIZE;
  const uint64_t swidth = 2*page_size;
  const uint64_t ssize = 2;
  const int nshards = 4;
  hobject_t hoid;

  bool old_osd_ec_partial_reads_experimental =
    g_ceph_context->_conf->osd_ec_partial_reads_experimental;
  g_ceph_context->_conf->osd_ec_partial_reads_experimental = true;

  ECUtil::stripe_info_t s(ssize, swidth, 0, vector<int>(0));
  ECListenerStub listenerStub;
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), swidth/ssize);

  const std::vector<int> chunk_mapping = {}; // no remapping
  ErasureCodeDummyImpl *ecode = new ErasureCodeDummyImpl();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  for (int i = 0; i < nshards; i++) {
    listenerStub.acting_shards.insert(pg_shard_t(i, shard_id_t(i)));
  }

  {
    std::map<int, extent_set> want_to_read;

    ECCommon::ec_align_t to_read(36*1024,10*1024, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECCommon::read_request_t read_request(want_to_read, false);

    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(want_to_read, false);
    {
      ECCommon::shard_read_t shard_read;
      shard_read.subchunk = ecode->default_sub_chunk;
      shard_read.extents.insert(20*1024, 4*1024);
      ref.shard_reads[pg_shard_t(0, shard_id_t(0))] = shard_read;
    }
    {
      ECCommon::shard_read_t shard_read;
      shard_read.subchunk = ecode->default_sub_chunk;
      shard_read.extents.insert(16*1024, 8*1024);
      ref.shard_reads[pg_shard_t(1, shard_id_t(1))] = shard_read;
    }

    ASSERT_EQ(read_request,  ref);
  }

  {
    std::map<int, extent_set> want_to_read;

    ECCommon::ec_align_t to_read(12*1024,12*1024, 1);
    pipeline.get_min_want_to_read_shards(to_read, want_to_read);
    ECCommon::read_request_t read_request(want_to_read, false);
    pipeline.get_min_avail_to_read_shards(hoid, false, false, read_request);

    ECCommon::read_request_t ref(want_to_read, false);
    {
      ECCommon::shard_read_t shard_read;
      shard_read.subchunk = ecode->default_sub_chunk;
      shard_read.extents.insert(8*1024, 4*1024);
      ref.shard_reads[pg_shard_t(0, shard_id_t(0))] = shard_read;
    }
    {
      ECCommon::shard_read_t shard_read;
      shard_read.subchunk = ecode->default_sub_chunk;
      shard_read.extents.insert(4*1024, 8*1024);
      ref.shard_reads[pg_shard_t(1, shard_id_t(1))] = shard_read;
    }

    ASSERT_EQ(read_request,  ref);
  }

  g_ceph_context->_conf->osd_ec_partial_reads_experimental =
    old_osd_ec_partial_reads_experimental;
}

TEST(ECCommon, get_min_want_to_read_shards_bug67087)
{
  const uint64_t swidth = 4096;
  const uint64_t ssize = 4;
  const uint64_t csize = 1024;

  ECUtil::stripe_info_t s(ssize, swidth, 0);
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), 1024);

  ECListenerStub listenerStub;
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), csize);

  const std::vector<int> chunk_mapping = {}; // no remapping
  ErasureCodeInterfaceRef ec_impl(new ErasureCodeDummyImpl);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  std::map<int, extent_set> want_to_read;
  ECCommon::ec_align_t to_read1(512,512, 1);
  ECCommon::ec_align_t to_read2(512+16*1024,512, 1);

  std::map<int, extent_set> ref;

  ref[0].insert(512, 512);

  // multitple calls with the same want_to_read can happen during
  // multi-region reads. This will create multiple extents in want_to_read,
  {
    pipeline.get_min_want_to_read_shards(
     to_read1, want_to_read);
    ASSERT_EQ(want_to_read, ref);

    pipeline.get_min_want_to_read_shards(
     to_read2, want_to_read);
    // We have 4 data shards per stripe.
    ref[0].insert(512+4*1024, 512);
  }
}

TEST(ECCommon, get_remaining_shards)
{
  const uint64_t page_size = CEPH_PAGE_SIZE;
  const uint64_t swidth = 64*page_size;
  const uint64_t ssize = 4;
  const int nshards = 6;
  const uint64_t chunk_size = swidth / ssize;

  g_ceph_context->_conf->osd_ec_partial_reads_experimental = true;

  ECUtil::stripe_info_t s(ssize, swidth, 0, vector<int>(0));
  ECListenerStub listenerStub;
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), swidth/ssize);

  const std::vector<int> chunk_mapping = {}; // no remapping
  ErasureCodeDummyImpl *ecode = new ErasureCodeDummyImpl();
  ErasureCodeInterfaceRef ec_impl(ecode);
  ECCommon::ReadPipeline pipeline(g_ceph_context, ec_impl, s, &listenerStub);

  std::vector<ECCommon::shard_read_t> empty_shard_vector(ssize);
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
    std::map<int, extent_set> to_read;
    to_read[0].insert(0, 4096);
    ECCommon::read_request_t read_request(to_read, false);
    int missing_shard = 0;

    // Mock up a read result.
    ECCommon::read_result_t read_result(&s);
    read_result.errors.emplace(pg_shards[missing_shard], -EIO);

    pipeline.get_remaining_shards(hoid, read_result, read_request, false, false);

    ECCommon::read_request_t ref(to_read, false);
    int parity_shard = 4;
    for (unsigned int i=0; i<ssize; i++) {
      ECCommon::shard_read_t shard_read;
      shard_read.subchunk = ecode->default_sub_chunk;
      shard_read.extents.insert(0,4096);
      int shard_id = i==missing_shard?parity_shard:i;
      ref.shard_reads[pg_shard_t(shard_id, shard_id_t(shard_id))] = shard_read;
    }

    ASSERT_EQ(read_request,  ref);
  }

  // Request re-read. There is a page of overlap in what is already read.
  {
    hobject_t hoid;

    std::map<int, extent_set> to_read;
    s.ro_range_to_shard_extent_set(chunk_size/2, chunk_size+page_size, to_read);
    ECCommon::read_request_t read_request(to_read, false);
    int missing_shard = 1;

    // Mock up a read result.
    ECCommon::read_result_t read_result(&s);
    read_result.errors.emplace(pg_shards[missing_shard], -EIO);
    buffer::list bl;
    bl.append_zero(chunk_size/2);
    read_result.buffers_read.insert_in_shard(0, chunk_size/2, bl);

    pipeline.get_remaining_shards(hoid, read_result, read_request, false, false);

    // The result should be a read request for the first 4k of shard 0, as that
    // is currently missing.
    ECCommon::read_request_t ref(to_read, false);
    int parity_shard = 4;
    for (int i=0; i<ssize; i++) {
      ECCommon::shard_read_t shard_read;
      shard_read.subchunk = ecode->default_sub_chunk;
      int shard_id = i==missing_shard?parity_shard:i;
      ref.shard_reads[pg_shard_t(shard_id, shard_id_t(shard_id))] = shard_read;
    }
    ref.shard_reads[pg_shards[0]].extents.insert(0, chunk_size/2);
    ref.shard_reads[pg_shards[2]].extents.insert(0, chunk_size/2+page_size);
    ref.shard_reads[pg_shards[3]].extents.insert(0, chunk_size/2+page_size);
    ref.shard_reads[pg_shards[4]].extents.insert(0, chunk_size/2+page_size);

    ASSERT_EQ(read_request,  ref);
  }
}
