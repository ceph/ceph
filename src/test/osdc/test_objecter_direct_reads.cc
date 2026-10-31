// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/**
 * What the Objecter does to in-flight EC direct reads, split sub-reads and
 * order-reads-writes ops when the OSDMap changes.
 *
 * The Objecter is initialised with a session per OSD whose connection
 * records the MOSDOps sent on it.  Maps are delivered through
 * handle_osd_map() and replies through ms_fast_dispatch2().
 *
 * Pool 1 is a 2+1 EC pool with 4K chunks and one PG, 1.0:
 *
 *            osd.0      osd.1     osd.2     osd.3
 *   PG 1.0   shard 0    shard 1   shard 2   not in the PG
 *            primary    data      parity
 *
 * With BALANCE_READS, read 0~4K goes to osd.0, read 4K~4K goes to osd.1,
 * and read 0~8K is split into a sub-read to each of them.
 */

#include <algorithm>

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>

#include "common/Cond.h"
#include "global/global_context.h"
#include "include/rados.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDOpReply.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include "osdc/Objecter.h"
#include "test/osd/MockConnection.h"
#include "test/osd/OSDMapTestHelpers.h"

class RecordingConnection : public MockConnection {
public:
  using MockConnection::MockConnection;
  std::vector<MessageRef> sent;

protected:
  int send_msg(MessageRef&& m) override {
    sent.push_back(std::move(m));
    return 0;
  }
};

class TestSplitOpMapChange : public ::testing::Test {
protected:
  static constexpr int num_osds = 4;
  static constexpr int64_t ec_pool_id = 1;
  static constexpr uint64_t chunk_size = 4096;
  static constexpr int primary = 0;
  static constexpr int shard1_osd = 1;
  static constexpr int parity_osd = 2;
  static constexpr int spare_osd = 3;
  const pg_t pgid{0, ec_pool_id};
  const std::vector<int> acting = {primary, shard1_osd, parity_osd};
  boost::asio::io_context ioc;
  MonClient monc{g_ceph_context, ioc};
  std::unique_ptr<Messenger> msgr{
    Messenger::create_client_messenger(g_ceph_context, "client")};
  std::unique_ptr<Objecter> objecter;
  std::map<int, ceph::ref_t<RecordingConnection>> cons;
  C_SaferCond done;

  void SetUp() override
  {
    objecter = std::make_unique<Objecter>(g_ceph_context, msgr.get(), &monc, ioc);
    objecter->init();

    OSDMap map;
    uuid_d fsid;
    fsid.generate_random();
    ceph_assert(map.build_simple(g_ceph_context, 1, fsid, num_osds) == 0);
    // Up but out, so CRUSH maps PG 1.0 nowhere and only the pg_temp set
    // below gives it an acting set.
    for (int i = 0; i < num_osds; i++) {
      map.set_state(i, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    }

    pg_pool_t ec;
    ec.type = pg_pool_t::TYPE_ERASURE;
    ec.size = 3;
    ec.min_size = 2;
    ec.ec_data_shard_count = 2;
    ec.ec_coding_shard_count = 1;
    ec.set_stripe_width(2 * chunk_size);
    ec.set_flag(pg_pool_t::FLAG_EC_OVERWRITES);
    ec.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
    ec.set_flag(pg_pool_t::FLAG_CLIENT_SPLIT_READS);
    ec.set_pg_num(1);
    ec.set_pgp_num(1);
    OSDMapTestHelpers::add_pool(map, ec_pool_id, ec);
    OSDMapTestHelpers::set_pg_acting(map, pgid, acting);

    objecter->start(&map);

    for (int osd = 0; osd < num_osds; osd++) {
      auto s = new Objecter::OSDSession(g_ceph_context, osd, ioc.get_executor());
      cons[osd] = ceph::make_ref<RecordingConnection>(osd);
      s->con = cons[osd];
      s->con->set_priv(RefCountedPtr{s});
      objecter->osd_sessions[osd] = s;
    }
  }

  void TearDown() override
  {
    objecter->shutdown();
    objecter.reset();
  }

  void poll()
  {
    ioc.restart();
    ioc.poll();
  }

  void advance_map(const std::function<void(OSDMap::Incremental&)>& change)
  {
    auto [epoch, fsid] = objecter->with_osdmap([](const OSDMap& o) {
      return std::make_pair(o.get_epoch(), o.get_fsid());
    });
    OSDMap::Incremental inc(epoch + 1);
    inc.fsid = fsid;
    change(inc);
    auto m = ceph::make_message<MOSDMap>(monc.get_fsid(),
                                         CEPH_FEATURES_SUPPORTED_DEFAULT);
    inc.encode(m->incremental_maps[inc.epoch],
               CEPH_FEATURES_SUPPORTED_DEFAULT | CEPH_FEATURE_RESERVED);
    // A paused op, or one left without an OSD, makes the Objecter ask for
    // the next map; wanting it already, as _maybe_request_map() does, stops
    // the unconnected MonClient opening a session.
    monc.sub_want("osdmap", inc.epoch + 1, CEPH_SUBSCRIBE_ONETIME);
    objecter->handle_osd_map(m.get());
    poll();
  }

  void mark_down(int osd)
  {
    advance_map([osd](OSDMap::Incremental& inc) {
      // XORed onto the current state, so this clears UP.
      inc.new_state[osd] = CEPH_OSD_UP;
    });
  }

  void modify_pool(const std::function<void(pg_pool_t&)>& change)
  {
    auto pool = objecter->with_osdmap([](const OSDMap& o) {
      return *o.get_pg_pool(ec_pool_id);
    });
    change(pool);
    advance_map([&pool](OSDMap::Incremental& inc) {
      inc.new_pools[ec_pool_id] = pool;
    });
  }

  void raise_min_size()
  {
    modify_pool([](pg_pool_t& pool) { pool.min_size++; });
  }

  void set_pool_full(bool full)
  {
    modify_pool([full](pg_pool_t& pool) {
      if (full) {
        pool.set_flag(pg_pool_t::FLAG_FULL);
      } else {
        pool.unset_flag(pg_pool_t::FLAG_FULL);
      }
    });
  }

  Objecter::Op *new_read_op(uint64_t off, uint64_t len, int flags)
  {
    osdc_opvec ops(1);
    ops[0].op.op = CEPH_OSD_OP_READ;
    ops[0].op.extent.offset = off;
    ops[0].op.extent.length = len;
    return new Objecter::Op(object_t("obj"), object_locator_t(ec_pool_id),
                            std::move(ops), flags | CEPH_OSD_FLAG_READ,
                            &done, nullptr);
  }

  ceph_tid_t submit_read(uint64_t off, uint64_t len, int flags)
  {
    ceph_tid_t tid = 0;
    objecter->op_submit(new_read_op(off, len, flags), &tid);
    poll();
    return tid;
  }

  ceph_tid_t submit_write()
  {
    osdc_opvec ops(1);
    ops[0].op.op = CEPH_OSD_OP_WRITEFULL;
    ops[0].indata.append("data");
    ops[0].op.extent.length = ops[0].indata.length();
    ceph_tid_t tid = 0;
    objecter->op_submit(new Objecter::Op(object_t("obj"),
                                         object_locator_t(ec_pool_id),
                                         std::move(ops), CEPH_OSD_FLAG_WRITE,
                                         (Context*)nullptr, nullptr),
                        &tid);
    poll();
    return tid;
  }

  shard_id_t shard_of(int osd)
  {
    return shard_id_t(std::ranges::find(acting, osd) - acting.begin());
  }

  std::vector<std::pair<ceph_tid_t, shard_id_t>> sent_to(int osd)
  {
    std::vector<std::pair<ceph_tid_t, shard_id_t>> ops;
    for (auto& m : cons[osd]->sent) {
      ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
      auto op = boost::static_pointer_cast<_mosdop::MOSDOp<osdc_opvec>>(m);
      ops.emplace_back(op->get_tid(), op->get_spg().shard);
    }
    return ops;
  }

  void reply(int osd, const MessageRef& m)
  {
    auto op = boost::static_pointer_cast<_mosdop::MOSDOp<osdc_opvec>>(m);
    auto r = ceph::make_message<MOSDOpReply>();
    r->set_tid(op->get_tid());
    r->set_op_returns(std::vector<pg_log_op_return_item_t>(op->ops.size()));
    r->set_connection(cons[osd]);
    objecter->ms_fast_dispatch2(r);
    poll();
  }

  // The sub-read of a split read to osd.1 is still outstanding when the PG
  // starts a new interval, so the whole read is redriven to the primary.
  void split_read_redriven_on_new_interval(
    const std::function<void()>& new_interval)
  {
    ceph_tid_t parent = submit_read(0, 2 * chunk_size,
                                    CEPH_OSD_FLAG_BALANCE_READS);
    ASSERT_EQ(1u, sent_to(primary).size());
    ASSERT_EQ(1u, sent_to(shard1_osd).size());
    reply(primary, cons[primary]->sent.back());

    // A map that leaves PG 1.0 alone leaves the sub-read alone.
    mark_down(spare_osd);
    EXPECT_EQ(1u, sent_to(shard1_osd).size());
    EXPECT_EQ(1u, sent_to(primary).size());

    new_interval();
    EXPECT_EQ(1u, sent_to(shard1_osd).size());
    ASSERT_EQ(2u, sent_to(primary).size());
    EXPECT_EQ(parent, sent_to(primary).back().first);
    reply(primary, cons[primary]->sent.back());
    EXPECT_EQ(0, done.wait_for(0));
  }

  // The single-chunk read 4K~4K goes directly to osd.1, which holds shard 1.
  // Once the PG starts a new interval, it is redriven to the primary.
  void direct_read_redriven_on_new_interval(
    const std::function<void()>& new_interval, int new_primary)
  {
    ceph_tid_t tid = submit_read(chunk_size, chunk_size,
                                 CEPH_OSD_FLAG_BALANCE_READS);
    ASSERT_EQ(1u, sent_to(shard1_osd).size());
    EXPECT_EQ(std::make_pair(tid, shard_of(shard1_osd)),
              sent_to(shard1_osd).back());

    new_interval();
    EXPECT_EQ(1u, sent_to(shard1_osd).size());
    ASSERT_EQ(1u, sent_to(new_primary).size());
    EXPECT_EQ(std::make_pair(tid, shard_of(new_primary)),
              sent_to(new_primary).back());
    reply(new_primary, cons[new_primary]->sent.back());
    EXPECT_EQ(0, done.wait_for(0));
  }
};

TEST_F(TestSplitOpMapChange, ECSplitReadRedrivesWhenMinSizeChanges)
{
  split_read_redriven_on_new_interval([this] { raise_min_size(); });
}

TEST_F(TestSplitOpMapChange, ECSplitReadRedrivesWhenOtherShardOsdDown)
{
  split_read_redriven_on_new_interval([this] { mark_down(parity_osd); });
}

TEST_F(TestSplitOpMapChange, ECSplitReadRedrivesWhenShardOsdDown)
{
  split_read_redriven_on_new_interval([this] { mark_down(shard1_osd); });
}

TEST_F(TestSplitOpMapChange, ECDirectReadRedrivesWhenPrimaryMoves)
{
  direct_read_redriven_on_new_interval([this] {
    advance_map([this](OSDMap::Incremental& inc) {
      inc.new_primary_temp[pgid] = parity_osd;
    });
  }, parity_osd);
}

TEST_F(TestSplitOpMapChange, ECDirectReadRedrivesWhenMinSizeChanges)
{
  direct_read_redriven_on_new_interval([this] { raise_min_size(); }, primary);
}

// A map can land while _op_submit() has the rwlock dropped to relock for the
// first sub-read of a split read, so the sub-read to osd.1 has a target from
// the older map.  Neither sub-read is sent, and the next change to the PG
// redrives the whole read to the primary.
TEST_F(TestSplitOpMapChange, ECSplitReadNewIntervalDuringSubmit)
{
  g_ceph_context->_conf.set_val_or_die("objecter_debug_inject_relock_delay",
                                       "true");
  objecter->relock_delay = [this] {
    // Only the first sub-read relocks.
    g_ceph_context->_conf.set_val_or_die("objecter_debug_inject_relock_delay",
                                         "false");
    raise_min_size();
  };
  ceph_tid_t parent = submit_read(0, 2 * chunk_size,
                                  CEPH_OSD_FLAG_BALANCE_READS);
  EXPECT_TRUE(sent_to(primary).empty());
  EXPECT_TRUE(sent_to(shard1_osd).empty());

  mark_down(parity_osd);
  EXPECT_TRUE(sent_to(shard1_osd).empty());
  ASSERT_EQ(1u, sent_to(primary).size());
  EXPECT_EQ(parent, sent_to(primary).back().first);
  reply(primary, cons[primary]->sent.back());
  EXPECT_EQ(0, done.wait_for(0));
}

// An ORDER_READS_WRITES read must not overtake a write that a full pool
// pauses.
TEST_F(TestSplitOpMapChange, RWOrderedReadWaitsForWriteOnFullPool)
{
  set_pool_full(true);
  ceph_tid_t write = submit_write();
  ceph_tid_t read = submit_read(0, 1, CEPH_OSD_FLAG_RWORDERED);
  EXPECT_TRUE(sent_to(primary).empty());

  set_pool_full(false);
  ASSERT_EQ(2u, sent_to(primary).size());
  EXPECT_EQ(write, sent_to(primary)[0].first);
  EXPECT_EQ(read, sent_to(primary)[1].first);
}

TEST_F(TestSplitOpMapChange, RWOrderedReadResentAfterWriteWhenPoolFull)
{
  ceph_tid_t write = submit_write();
  ceph_tid_t read = submit_read(0, 1, CEPH_OSD_FLAG_RWORDERED);
  ASSERT_EQ(2u, sent_to(primary).size());

  set_pool_full(true);
  EXPECT_EQ(2u, sent_to(primary).size());

  set_pool_full(false);
  ASSERT_EQ(4u, sent_to(primary).size());
  EXPECT_EQ(write, sent_to(primary)[2].first);
  EXPECT_EQ(read, sent_to(primary)[3].first);
}

// FULL_TRY ops do not respect the full flag, so are not held back.
TEST_F(TestSplitOpMapChange, RWOrderedFullTryReadSentOnFullPool)
{
  set_pool_full(true);
  submit_read(0, 1, CEPH_OSD_FLAG_RWORDERED | CEPH_OSD_FLAG_FULL_TRY);
  EXPECT_EQ(1u, sent_to(primary).size());
}
