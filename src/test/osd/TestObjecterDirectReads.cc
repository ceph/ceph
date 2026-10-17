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
 * What the Objecter does to in-flight EC direct reads and split sub-reads
 * when the OSDMap changes.
 *
 * The Objecter is initialised with a session per OSD whose connection
 * records the MOSDOps sent on it.  Maps are delivered through
 * handle_osd_map() and replies through ms_dispatch2().  PG 1.0 of a 2+1 EC
 * pool has acting [0,1,2]; OSDs 3-5 are spare.
 */

#include <gtest/gtest.h>
#include <thread>

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
  static constexpr int num_osds = 6;
  static constexpr int64_t ec_pool_id = 1;
  static constexpr int primary = 0;
  const std::vector<int> acting = {0, 1, 2};
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
    for (int i = 0; i < num_osds; i++) {
      map.set_state(i, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    }

    pg_pool_t ec;
    ec.type = pg_pool_t::TYPE_ERASURE;
    ec.size = 3;
    ec.min_size = 2;
    ec.ec_data_shard_count = 2;
    ec.ec_coding_shard_count = 1;
    ec.set_stripe_width(2 * 4096);
    ec.set_flag(pg_pool_t::FLAG_EC_OVERWRITES);
    ec.set_flag(pg_pool_t::FLAG_EC_OPTIMIZATIONS);
    ec.set_flag(pg_pool_t::FLAG_CLIENT_SPLIT_READS);
    ec.set_pg_num(1);
    ec.set_pgp_num(1);
    OSDMapTestHelpers::add_pool(map, ec_pool_id, ec);
    OSDMapTestHelpers::set_pg_acting(map, pg_t(0, ec_pool_id), acting);

    objecter->start(&map);

    for (int osd = 0; osd < num_osds; osd++) {
      auto s = new Objecter::OSDSession(g_ceph_context, osd);
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
    // An op left without an OSD makes the Objecter ask for the next map;
    // wanting it already stops the unconnected MonClient opening a session.
    monc.sub_want("osdmap", inc.epoch + 1, CEPH_SUBSCRIBE_ONETIME);
    objecter->handle_osd_map(m.get());
    poll();
  }

  void set_pg_temp(const std::vector<int>& osds)
  {
    advance_map([&osds](OSDMap::Incremental& inc) {
      inc.new_pg_temp[pg_t(0, ec_pool_id)] =
        mempool::osdmap::vector<int32_t>(osds.begin(), osds.end());
    });
  }

  void mark_down(int osd)
  {
    advance_map([osd](OSDMap::Incremental& inc) {
      inc.new_state[osd] = CEPH_OSD_UP;
    });
  }

  void raise_min_size()
  {
    auto pool = objecter->with_osdmap([](const OSDMap& o) {
      return *o.get_pg_pool(ec_pool_id);
    });
    pool.min_size++;
    advance_map([&pool](OSDMap::Incremental& inc) {
      inc.new_pools[ec_pool_id] = pool;
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
    objecter->ms_dispatch2(r);
    poll();
  }

  // A two-chunk read is split into a sub-read to each data shard's OSD.
  // Once the PG starts a new interval, the sub-read still outstanding to the
  // non-primary shard fails and the read is redriven to the primary.
  void split_read_redriven_on_new_interval(
    const std::function<void(int victim)>& new_interval)
  {
    ceph_tid_t parent = submit_read(0, 8192, CEPH_OSD_FLAG_BALANCE_READS);
    ASSERT_EQ(1u, sent_to(primary).size());
    int victim = -1;
    for (int osd : acting) {
      if (osd != primary && !sent_to(osd).empty()) {
        victim = osd;
      }
    }
    ASSERT_NE(-1, victim);
    SCOPED_TRACE("victim osd." + std::to_string(victim));
    for (int osd : acting) {
      if (osd == victim) {
        continue;
      }
      auto sent = cons[osd]->sent;
      for (auto& m : sent) {
        reply(osd, m);
      }
    }

    mark_down(num_osds - 1);
    EXPECT_EQ(1u, sent_to(victim).size());
    EXPECT_EQ(1u, sent_to(primary).size());

    new_interval(victim);
    EXPECT_EQ(1u, sent_to(victim).size());
    ASSERT_EQ(2u, sent_to(primary).size());
    EXPECT_EQ(parent, sent_to(primary).back().first);
    reply(primary, cons[primary]->sent.back());
    EXPECT_EQ(0, done.wait_for(0));
  }

  // A single-chunk read goes directly to osd.1, which holds shard 1.
  void direct_read_redriven_on_new_interval(
    const std::function<void()>& new_interval, int new_primary)
  {
    ceph_tid_t tid = submit_read(4096, 4096, CEPH_OSD_FLAG_BALANCE_READS);
    ASSERT_EQ(1u, sent_to(1).size());
    EXPECT_EQ(std::make_pair(tid, shard_id_t(1)), sent_to(1).back());

    new_interval();
    EXPECT_EQ(1u, sent_to(1).size());
    ASSERT_EQ(1u, sent_to(new_primary).size());
    EXPECT_EQ(std::make_pair(tid, shard_id_t(new_primary)),
              sent_to(new_primary).back());
    reply(new_primary, cons[new_primary]->sent.back());
    EXPECT_EQ(0, done.wait_for(0));
  }
};

TEST_F(TestSplitOpMapChange, ECSplitReadRedrivesWhenMinSizeChanges)
{
  split_read_redriven_on_new_interval([this](int) { raise_min_size(); });
}

TEST_F(TestSplitOpMapChange, ECSplitReadRedrivesWhenOtherShardOsdDown)
{
  split_read_redriven_on_new_interval([this](int victim) {
    mark_down(acting[0] + acting[1] + acting[2] - primary - victim);
  });
}

TEST_F(TestSplitOpMapChange, ECSplitReadRedrivesWhenShardOsdDown)
{
  split_read_redriven_on_new_interval([this](int victim) {
    mark_down(victim);
  });
}

TEST_F(TestSplitOpMapChange, ECDirectReadRedrivesWhenPrimaryMoves)
{
  direct_read_redriven_on_new_interval([this] {
    advance_map([](OSDMap::Incremental& inc) {
      inc.new_primary_temp[pg_t(0, ec_pool_id)] = 2;
    });
  }, 2);
}

TEST_F(TestSplitOpMapChange, ECDirectReadRedrivesWhenMinSizeChanges)
{
  direct_read_redriven_on_new_interval([this] { raise_min_size(); }, primary);
}

// The relock delay makes _op_submit() drop the rwlock for a second after the
// first sub-read has its target, so the map moving every shard lands there.
TEST_F(TestSplitOpMapChange, ECSplitReadShardsMoveDuringSubmit)
{
  const std::vector<int> moved = {3, 4, 5};
  g_ceph_context->_conf.set_val_or_die("objecter_debug_inject_relock_delay",
                                       "true");
  ceph_tid_t parent = 0;
  std::thread submitter([&] {
    objecter->op_submit(new_read_op(0, 8192, CEPH_OSD_FLAG_BALANCE_READS),
                        &parent);
  });
  while (!objecter->is_active()) {
    std::this_thread::yield();
  }
  set_pg_temp(moved);
  submitter.join();
  g_ceph_context->_conf.set_val_or_die("objecter_debug_inject_relock_delay",
                                       "false");
  for (int osd : acting) {
    EXPECT_TRUE(sent_to(osd).empty()) << "osd." << osd;
  }

  mark_down(moved[2]);
  ASSERT_EQ(1u, sent_to(moved[0]).size());
  EXPECT_EQ(std::make_pair(parent, shard_id_t(0)), sent_to(moved[0]).back());
  reply(moved[0], cons[moved[0]]->sent.back());
  EXPECT_EQ(0, done.wait_for(0));
}
