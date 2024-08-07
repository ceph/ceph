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
#include "erasure-code/ErasureCode.h"
#include "osd/osd_types.h"
#include "common/ceph_argparse.h"
#include "common/admin_socket_client.h"

using namespace std;

TEST(ECUtil, stripe_info_t)
{
  const uint64_t swidth = 4096;
  const uint64_t ssize = 4;

  ECUtil::stripe_info_t s(ssize, swidth);
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
  ASSERT_EQ(s.chunk_aligned_offset_len_to_chunk(make_pair(2*s.get_chunk_size(),
							  10*swidth + 2*s.get_chunk_size())),
	    make_pair<uint64_t>(0, 11*s.get_chunk_size()));

  ASSERT_EQ(s.offset_len_to_stripe_bounds(make_pair(swidth-10, (uint64_t)20)),
            make_pair((uint64_t)0, 2*swidth));
}

TEST(ECUtil, offset_length_is_same_stripe)
{
  const uint64_t swidth = 4096;
  const uint64_t schunk = 1024;
  const uint64_t ssize = 4;

  ECUtil::stripe_info_t s(ssize, swidth);
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
	(*minimum)[parity_shard_index++] = default_sub_chunk;
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

  ECUtil::stripe_info_t s(ssize, swidth);
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), 1024);

  const std::vector<int> chunk_mapping = {}; // no remapping

  // read nothing at the very beginning
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      0, 0, s, chunk_mapping, &want_to_read);
    ASSERT_TRUE(want_to_read == std::set<int>{});
  }

  // read nothing at the middle (0-sized partial read)
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      2048, 0, s, chunk_mapping, &want_to_read);
    ASSERT_TRUE(want_to_read == std::set<int>{});
  }

  // read not-so-many (< chunk_size) bytes at the middle (partial read)
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      2048, 42, s, chunk_mapping, &want_to_read);
    ASSERT_TRUE(want_to_read == std::set<int>{2});
  }

  // read more (> chunk_size) bytes at the middle (partial read)
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      1024, 1024+42, s, chunk_mapping, &want_to_read);
    // extra () due to a language / macro limitation
    ASSERT_TRUE(want_to_read == (std::set<int>{1, 2}));
  }

  // full stripe except last chunk
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      0, 3*1024, s, chunk_mapping, &want_to_read);
    // extra () due to a language / macro limitation
    ASSERT_TRUE(want_to_read == (std::set<int>{0, 1, 2}));
  }

  // full stripe except 1st chunk
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      1024, swidth-1024, s, chunk_mapping, &want_to_read);
    // extra () due to a language / macro limitation
    ASSERT_TRUE(want_to_read == (std::set<int>{1, 2, 3}));
  }

  // large, multi-stripe read starting just after 1st chunk
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      1024, swidth*42, s, chunk_mapping, &want_to_read);
    // extra () due to a language / macro limitation
    ASSERT_TRUE(want_to_read == (std::set<int>{0, 1, 2, 3}));
  }

  // large read from the beginning
  {
    std::set<int> want_to_read;
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      0, swidth*42, s, chunk_mapping, &want_to_read);
    // extra () due to a language / macro limitation
    ASSERT_TRUE(want_to_read == (std::set<int>{0, 1, 2, 3}));
  }
}

TEST(ECCommon, get_min_want_to_read_shards_bug67087)
{
  const uint64_t swidth = 4096;
  const uint64_t ssize = 4;

  ECUtil::stripe_info_t s(ssize, swidth);
  ASSERT_EQ(s.get_stripe_width(), swidth);
  ASSERT_EQ(s.get_chunk_size(), 1024);

  const std::vector<int> chunk_mapping = {}; // no remapping

  std::set<int> want_to_read;

  // multitple calls with the same want_to_read can happen during
  // multi-region reads.
  {
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      512, 512, s, chunk_mapping, &want_to_read);
    ASSERT_EQ(want_to_read, std::set<int>{0});
    ECCommon::ReadPipeline::get_min_want_to_read_shards(
      512+16*1024, 512, s, chunk_mapping, &want_to_read);
    ASSERT_EQ(want_to_read, std::set<int>{0});
  }
}
