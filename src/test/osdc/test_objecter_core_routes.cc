// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

/**
 * Routing a PG's ops to the connection of the Crimson OSD core owning it
 * (objecter_use_osd_core_hints): when the PG switches connections, and
 * holding its new ops so that it can (objecter_osd_core_switch_grace and
 * objecter_osd_core_switch_max_hold).
 */

#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>

#include "common/perf_counters_collection.h"
#include "global/global_context.h"
#include "include/rados.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
#include "osdc/Objecter.h"
#include "test/osd/MockConnection.h"
#include "test/osd/OSDMapTestHelpers.h"

using namespace std::chrono_literals;

namespace {

/// records the tids of the ops sent on it. Ops may be sent from the
/// Objecter's timer thread (a hold expiring), hence the lock
class RecordingConnection : public MockConnection {
public:
  explicit RecordingConnection(int osd) : MockConnection(osd)
  {
    set_peer_type(CEPH_ENTITY_TYPE_OSD);
  }

  std::vector<ceph_tid_t> tids() const
  {
    std::lock_guard l(lock);
    return sent_tids;
  }

protected:
  int send_msg(MessageRef&& m) override
  {
    if (m->get_type() == CEPH_MSG_OSD_OP) {
      std::lock_guard l(lock);
      sent_tids.push_back(m->get_tid());
    }
    return 0;
  }

private:
  mutable std::mutex lock;
  std::vector<ceph_tid_t> sent_tids;
};

using tids_t = std::vector<ceph_tid_t>;

} // anonymous namespace

class TestObjecterCoreRoutes : public ::testing::Test {
protected:
  static constexpr int osd = 0;
  static constexpr int64_t pool_id = 1;
  /// the core the OSD hints for the (single) PG
  static constexpr uint32_t pg_core = 3;
  /// short, for the tests to wait for them
  static constexpr double grace = 0.2;
  static constexpr double max_hold = 0.6;
  const pg_t pgid{0, pool_id};

  boost::asio::io_context ioc;
  MonClient monc{g_ceph_context, ioc};
  std::unique_ptr<Messenger> msgr{
    Messenger::create_client_messenger(g_ceph_context, "client")};
  std::unique_ptr<Objecter> objecter;
  Objecter::OSDSession *session = nullptr;
  ceph::ref_t<RecordingConnection> main_con;
  ceph::ref_t<RecordingConnection> core_con;
  entity_addrvec_t core_addrs;

  void SetUp() override
  {
    auto& conf = g_ceph_context->_conf;
    // read by the Objecter's constructor
    conf.set_val_or_die("objecter_use_osd_core_hints", "true");
    conf.set_val_or_die("objecter_osd_core_switch_grace",
                        std::to_string(grace));
    conf.set_val_or_die("objecter_osd_core_switch_max_hold",
                        std::to_string(max_hold));
    conf.apply_changes(nullptr);

    objecter = std::make_unique<Objecter>(g_ceph_context, msgr.get(), &monc, ioc);
    objecter->init();

    OSDMap map;
    uuid_d fsid;
    fsid.generate_random();
    ceph_assert(map.build_simple(g_ceph_context, 1, fsid, 1) == 0);
    map.set_state(osd, CEPH_OSD_EXISTS | CEPH_OSD_UP);
    pg_pool_t pool;
    pool.type = pg_pool_t::TYPE_REPLICATED;
    pool.size = 1;
    pool.min_size = 1;
    pool.set_pg_num(1);
    pool.set_pgp_num(1);
    OSDMapTestHelpers::add_pool(map, pool_id, pool);
    OSDMapTestHelpers::set_pg_acting(map, pgid, {osd});
    objecter->start(&map);

    entity_addr_t main_addr;
    main_addr.parse("v2:127.0.0.1:6800/1234");
    entity_addr_t core_addr;
    core_addr.parse("v2:127.0.0.1:6810/1234");
    core_addrs = entity_addrvec_t(core_addr);

    session = new Objecter::OSDSession(g_ceph_context, osd, ioc.get_executor());
    main_con = ceph::make_ref<RecordingConnection>(osd);
    main_con->set_peer_addrs(entity_addrvec_t(main_addr));
    session->con = main_con;
    session->con->set_priv(RefCountedPtr{session});
    // already connected to the core's listener: the hint does not connect
    core_con = ceph::make_ref<RecordingConnection>(osd);
    core_con->set_peer_addrs(core_addrs);
    core_con->set_priv(RefCountedPtr{session});
    session->core_routes.core_cons[pg_core] = core_con;
    objecter->osd_sessions[osd] = session;
  }

  void TearDown() override
  {
    objecter->shutdown();
    objecter.reset();
    auto& conf = g_ceph_context->_conf;
    conf.set_val_or_die("objecter_use_osd_core_hints", "false");
    conf.apply_changes(nullptr);
  }

  void poll()
  {
    ioc.restart();
    ioc.poll();
  }

  /// a read of the object 'name' (all objects map to the single PG)
  ceph_tid_t submit(const std::string& name = "obj")
  {
    osdc_opvec ops(1);
    ops[0].op.op = CEPH_OSD_OP_READ;
    ops[0].op.extent.offset = 0;
    ops[0].op.extent.length = 4096;
    auto op = new Objecter::Op(object_t(name), object_locator_t(pool_id),
                               std::move(ops), CEPH_OSD_FLAG_READ,
                               static_cast<Context*>(nullptr), nullptr);
    ceph_tid_t tid = 0;
    objecter->op_submit(op, &tid);
    poll();
    return tid;
  }

  /// the OSD's reply to tid, on con; with the PG's core hint if 'hint'
  void reply(const ceph::ref_t<RecordingConnection>& con,
             ceph_tid_t tid,
             bool hint = true)
  {
    auto r = ceph::make_message<MOSDOpReply>();
    r->set_tid(tid);
    r->set_op_returns(std::vector<pg_log_op_return_item_t>(1));
    if (hint) {
      r->set_core_hint(osd_core_hint_t(pg_core, core_addrs.front().get_port()));
    }
    r->set_connection(con);
    objecter->ms_fast_dispatch2(r);
    poll();
  }

  /// the PG is due to switch to pg_core, with tids in flight on the main
  /// connection. Returns the tids sent
  tids_t make_busy_pg()
  {
    auto a = submit();
    auto b = submit();
    // the hint, while b is in flight
    reply(main_con, a);
    auto c = submit();
    EXPECT_EQ((tids_t{a, b, c}), main_con->tids());
    EXPECT_TRUE(core_con->tids().empty());
    return {a, b, c};
  }

  static void wait(double seconds)
  {
    std::this_thread::sleep_for(
      std::chrono::duration<double>(seconds));
  }

  /// for the tests that don't expect a hold, however slowly they run
  static void no_holds()
  {
    auto& conf = g_ceph_context->_conf;
    conf.set_val_or_die("objecter_osd_core_switch_grace", "3600");
    conf.apply_changes(nullptr);
  }

  /// wait until the hold ends (on the Objecter's timer thread)
  void wait_hold_ended()
  {
    for (int i = 0; i < 1000 && route_holding(); ++i) {
      wait(0.01);
    }
    ASSERT_FALSE(route_holding());
  }

  bool route_holding()
  {
    std::shared_lock l(session->lock);
    return session->core_routes.pg_routes.at(spg_t(pgid)).holding;
  }

  /// the Objecter's perf counter 'name' (each test has its own Objecter)
  static const PerfCounters::perf_counter_data_any_d& perf(
    const std::string& name)
  {
    const PerfCounters::perf_counter_data_any_d* data = nullptr;
    g_ceph_context->get_perfcounters_collection()->with_counters(
      [&](const PerfCountersCollectionImpl::CounterMap& counters) {
        auto i = counters.find("objecter." + name);
        ceph_assert(i != counters.end());
        data = i->second.data;
      });
    return *data;
  }
  static uint64_t counter(const std::string& name)
  {
    return perf(name).u64;
  }
  /// a time avg counter's count, and its longest increment
  static std::pair<uint64_t, double> time_avg(const std::string& name)
  {
    auto [sum, count, max] = perf(name).read_avg_ex();
    return {count, max / 1e9};
  }
};

// an idle PG switches with its first op after the hint
TEST_F(TestObjecterCoreRoutes, IdlePgSwitches)
{
  no_holds();
  auto a = submit();
  reply(main_con, a);
  auto b = submit();
  EXPECT_EQ((tids_t{a}), main_con->tids());
  EXPECT_EQ((tids_t{b}), core_con->tids());
  EXPECT_FALSE(route_holding());
  EXPECT_EQ(1u, counter("core_route_switch"));
  EXPECT_EQ(0u, counter("op_send_switch_pending"));
  // not delayed
  EXPECT_EQ(0u, time_avg("core_switch_delay").first);
}

// a busy PG keeps its connection during the grace period, and switches
// once its ops drain on their own
TEST_F(TestObjecterCoreRoutes, BusyPgSwitchesWhenDrained)
{
  no_holds();
  auto sent = make_busy_pg();
  auto d = submit();
  EXPECT_EQ(4u, main_con->tids().size());
  EXPECT_FALSE(route_holding());
  for (auto tid : {sent[1], sent[2], d}) {
    reply(main_con, tid);
  }
  auto e = submit();
  EXPECT_EQ((tids_t{e}), core_con->tids());
  EXPECT_EQ(2u, counter("op_send_switch_pending"));  // c and d
  EXPECT_EQ(0u, counter("core_switch_hold"));
  EXPECT_EQ(1u, time_avg("core_switch_delay").first);
}

// after the grace period, the new ops are held until the ops in flight
// are answered; then they are sent, in order, on the core connection
TEST_F(TestObjecterCoreRoutes, HoldUntilDrained)
{
  auto sent = make_busy_pg();
  wait(grace * 1.5);
  auto d = submit();
  auto e = submit();
  EXPECT_TRUE(route_holding());
  EXPECT_EQ(sent, main_con->tids());
  EXPECT_TRUE(core_con->tids().empty());

  reply(main_con, sent[1]);
  EXPECT_TRUE(route_holding());
  EXPECT_TRUE(core_con->tids().empty());
  reply(main_con, sent[2]);
  EXPECT_FALSE(route_holding());
  EXPECT_EQ((tids_t{d, e}), core_con->tids());
  EXPECT_EQ(sent, main_con->tids());

  auto f = submit();
  EXPECT_EQ((tids_t{d, e, f}), core_con->tids());

  EXPECT_EQ(1u, counter("core_switch_hold"));
  EXPECT_EQ(0u, counter("core_switch_hold_expired"));
  EXPECT_EQ(2u, counter("op_switch_held"));
  EXPECT_EQ(1u, time_avg("core_switch_hold_time").first);
  // from c, the first op sent while the PG was due to switch
  auto [delays, longest] = time_avg("core_switch_delay");
  EXPECT_EQ(1u, delays);
  EXPECT_GE(longest, grace);
}

// the safety hatch: if the ops in flight are not answered within
// max_hold, the held ops are sent on the main connection, behind them;
// another hold may start only after another grace period
TEST_F(TestObjecterCoreRoutes, HoldExpires)
{
  auto sent = make_busy_pg();
  wait(grace * 1.5);
  auto d = submit();
  auto e = submit();
  EXPECT_TRUE(route_holding());

  wait_hold_ended();
  auto expected = sent;
  expected.insert(expected.end(), {d, e});
  EXPECT_EQ(expected, main_con->tids());
  EXPECT_TRUE(core_con->tids().empty());

  // the grace period starts again
  auto f = submit();
  EXPECT_FALSE(route_holding());
  expected.push_back(f);
  EXPECT_EQ(expected, main_con->tids());
  wait(grace * 1.5);
  auto g = submit();
  EXPECT_TRUE(route_holding());
  EXPECT_EQ(expected, main_con->tids());

  // this time, the ops drain
  for (auto tid : {sent[1], sent[2], d, e, f}) {
    reply(main_con, tid);
  }
  EXPECT_FALSE(route_holding());
  EXPECT_EQ((tids_t{g}), core_con->tids());

  EXPECT_EQ(2u, counter("core_switch_hold"));
  EXPECT_EQ(1u, counter("core_switch_hold_expired"));
  EXPECT_EQ(0u, counter("core_switch_hold_expired_unanswered"));
  auto [holds, longest_hold] = time_avg("core_switch_hold_time");
  EXPECT_EQ(2u, holds);
  EXPECT_GE(longest_hold, max_hold);
  // a single switch, over both holds
  auto [delays, longest] = time_avg("core_switch_delay");
  EXPECT_EQ(1u, delays);
  EXPECT_GE(longest, 2 * grace + max_hold);
}

// the reset of the core connection ends the hold: the held ops are sent
// on the main connection, in order
TEST_F(TestObjecterCoreRoutes, CoreConResetEndsHold)
{
  auto sent = make_busy_pg();
  wait(grace * 1.5);
  auto d = submit();
  auto e = submit();
  EXPECT_TRUE(route_holding());

  objecter->ms_handle_reset(core_con.get());
  poll();
  EXPECT_FALSE(route_holding());
  auto expected = sent;
  expected.insert(expected.end(), {d, e});
  EXPECT_EQ(expected, main_con->tids());
  EXPECT_TRUE(core_con->tids().empty());
  EXPECT_EQ(1u, counter("core_switch_hold"));
  EXPECT_EQ(0u, counter("core_switch_hold_expired"));
  EXPECT_EQ(0u, time_avg("core_switch_delay").first);
}

// a held op paused meanwhile is not sent when the hold ends, and is no
// longer held: it waits to be resent when unpaused, as any paused op
TEST_F(TestObjecterCoreRoutes, PausedOpNotHeldAfterHold)
{
  auto sent = make_busy_pg();
  wait(grace * 1.5);
  auto d = submit();
  auto e = submit();
  EXPECT_TRUE(route_holding());
  Objecter::Op* paused_op = nullptr;
  {
    std::unique_lock l(session->lock);
    paused_op = session->ops.at(d);
    EXPECT_TRUE(paused_op->core_route.switch_held);
    paused_op->target.paused = true;
  }

  reply(main_con, sent[1]);
  reply(main_con, sent[2]);
  EXPECT_FALSE(route_holding());
  EXPECT_EQ((tids_t{e}), core_con->tids());
  EXPECT_EQ(sent, main_con->tids());
  std::shared_lock l(session->lock);
  EXPECT_FALSE(paused_op->core_route.switch_held);
  EXPECT_FALSE(session->ops.at(e)->core_route.switch_held);
}

// an op canceled after being sent keeps its PG from switching until it is
// answered
TEST_F(TestObjecterCoreRoutes, CanceledOpAnswered)
{
  no_holds();
  auto a = submit();
  auto b = submit();
  reply(main_con, a);
  // b times out: the OSD may still execute it, on the main connection
  objecter->op_cancel(b, -ETIMEDOUT);
  poll();
  auto c = submit();
  EXPECT_EQ((tids_t{a, b, c}), main_con->tids());
  reply(main_con, c);
  // c is answered, but b is not
  auto d = submit();
  EXPECT_EQ((tids_t{a, b, c, d}), main_con->tids());
  reply(main_con, d);
  // b's answer arrives, after its cancellation
  reply(main_con, b);
  auto e = submit();
  EXPECT_EQ((tids_t{e}), core_con->tids());

  EXPECT_EQ(1u, counter("op_unanswered"));
  EXPECT_EQ(1u, counter("op_unanswered_answered"));
  // c and d: nothing in flight, only b unanswered
  EXPECT_EQ(2u, counter("op_send_switch_unanswered"));
  EXPECT_EQ(1u, counter("core_switch_delayed_unanswered"));
  EXPECT_EQ(1u, time_avg("core_switch_delay").first);
}

// ... or until it is old enough for its answer not to be expected anymore
TEST_F(TestObjecterCoreRoutes, CanceledOpForgotten)
{
  no_holds();
  auto a = submit();
  auto b = submit();
  reply(main_con, a);
  objecter->op_cancel(b, -ETIMEDOUT);
  poll();
  auto c = submit();
  reply(main_con, c);
  EXPECT_EQ((tids_t{a, b, c}), main_con->tids());
  wait(max_hold * 1.5);
  auto d = submit();
  EXPECT_EQ((tids_t{d}), core_con->tids());

  EXPECT_EQ(1u, counter("op_unanswered"));
  EXPECT_EQ(1u, counter("op_unanswered_forgotten"));
  EXPECT_EQ(0u, counter("op_unanswered_answered"));
  EXPECT_EQ(1u, counter("core_switch_delayed_unanswered"));
}

// a canceled op is not waited for once its connection is reset
TEST_F(TestObjecterCoreRoutes, CanceledOpOnResetCon)
{
  no_holds();
  // switch the PG to the core connection first
  auto a = submit();
  reply(main_con, a);
  auto b = submit();
  auto c = submit();
  EXPECT_EQ((tids_t{b, c}), core_con->tids());
  reply(core_con, b);
  objecter->op_cancel(c, -ETIMEDOUT);
  poll();
  {
    std::shared_lock l(session->lock);
    EXPECT_EQ(1u, session->core_routes.unanswered.size());
  }
  objecter->ms_handle_reset(core_con.get());
  poll();
  {
    std::shared_lock l(session->lock);
    EXPECT_TRUE(session->core_routes.unanswered.empty());
    EXPECT_FALSE(session->core_routes.pg_routes.at(spg_t(pgid)).busy());
  }
  EXPECT_EQ(1u, counter("op_unanswered"));
  EXPECT_EQ(1u, counter("op_unanswered_reset"));
}
