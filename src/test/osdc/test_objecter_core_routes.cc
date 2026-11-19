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
 * (objecter_use_osd_core_hints): when the PG switches connections.
 */

#include <algorithm>
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

/// records the tids of the ops sent on it
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
    open_core_con(true);
    objecter->osd_sessions[osd] = session;
  }

  /// core_con is opened, for a PG of its own (not the tests' one), and, if
  /// 'connect', connected
  void open_core_con(bool connect)
  {
    std::unique_lock l(session->lock);
    osdc::core_route::HostBlocks blocks;
    const auto out = session->core_routes.follow_hint(
      spg_t(pg_t(1, pool_id)),
      osd_core_hint_t(pg_core, core_addrs.front().get_port()), main_con,
      ceph::coarse_mono_clock::now(), blocks,
      [this](const entity_addr_t&) -> ConnectionRef { return core_con; },
      [this](const osdc::core_route::op_visitor_t& visit) {
        for (auto& [tid, op] : session->ops) {
          visit(op->target.actual_pgid, op->core_route);
        }
      },
      osdc::core_route::config_t{.enabled = true});
    ceph_assert(out.opened == core_con);
    if (connect) {
      ceph_assert(session->core_routes.admit_core_con(core_con, blocks));
    }
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
  auto a = submit();
  reply(main_con, a);
  auto b = submit();
  EXPECT_EQ((tids_t{a}), main_con->tids());
  EXPECT_EQ((tids_t{b}), core_con->tids());
  EXPECT_EQ(1u, counter("core_route_switch"));
  EXPECT_EQ(0u, counter("op_send_switch_pending"));
  // not delayed
  EXPECT_EQ(0u, time_avg("core_switch_delay").first);
}

// a core connection is not routed to until it connects (see
// Objecter::ms_handle_connect())
TEST_F(TestObjecterCoreRoutes, NotRoutedUntilConnected)
{
  {
    // opened again, not connected yet
    std::unique_lock l(session->lock);
    osdc::core_route::HostBlocks blocks;
    ASSERT_TRUE(session->core_routes.drop_core_con(
      core_con, osdc::core_route::drop_why_t::reset,
      ceph::coarse_mono_clock::now(), blocks));
  }
  open_core_con(false);
  auto a = submit();
  reply(main_con, a);
  auto b = submit();
  EXPECT_EQ((tids_t{a, b}), main_con->tids());
  EXPECT_EQ(0u, counter("op_send_switch_pending"));

  objecter->ms_handle_connect(core_con.get());
  reply(main_con, b);
  auto c = submit();
  EXPECT_EQ((tids_t{c}), core_con->tids());
  EXPECT_EQ(1u, counter("core_route_switch"));
}

// a busy PG keeps its connection while its ops are in flight, and switches
// once they drain
TEST_F(TestObjecterCoreRoutes, BusyPgSwitchesWhenDrained)
{
  auto sent = make_busy_pg();
  auto d = submit();
  EXPECT_EQ(4u, main_con->tids().size());
  for (auto tid : {sent[1], sent[2], d}) {
    reply(main_con, tid);
  }
  auto e = submit();
  EXPECT_EQ((tids_t{e}), core_con->tids());
  EXPECT_EQ(2u, counter("op_send_switch_pending"));  // c and d
  EXPECT_EQ(1u, time_avg("core_switch_delay").first);
}
