// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <Python.h>

#include <cassert>

#include "common/async/context_pool.h"
#include "common/TrackedOp.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "gtest/gtest.h"
#include "messages/MPGStats.h"
#include "mgr/ClusterState.h"
#include "mgr/DaemonState.h"
#include "mgr/MgrClient.h"
#include "mon/MgrMap.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "osdc/Objecter.h"

#define dout_subsys ceph_subsys_client

namespace bs = boost::system;
namespace ca = ceph::async;

class ClusterStateTestHelper : public ClusterState {
public:
  ClusterStateTestHelper(
      MonClient* mc_,
      Objecter* objecter_,
      const MgrMap& mgrmap_) :
    ClusterState(mc_, objecter_, mgrmap_)
  {}

  const PGMap::Incremental&
  test_get_pending_inc() const
  {
    return pending_inc;
  }

  const std::map<int64_t, unsigned>&
  test_get_existing_pools() const
  {
    return existing_pools;
  }

  const PGMap&
  test_get_pg_map() const
  {
    return pg_map;
  }

  Objecter*
  test_get_objecter() const
  {
    return objecter;
  }
};

// A Connection test double that records every message MgrClient emits instead
// of putting it on the wire.  Connection::send_message()/send_message2() all
// funnel through the protected send_msg(), so this single override captures
// every send path MgrClient can take.
class RecordingConnection : public Connection {
public:
  std::vector<MessageRef> sent;

  explicit RecordingConnection(CephContext* cct_)
    : Connection(cct_, nullptr)
  {
    set_features(CEPH_FEATURES_ALL);
  }

  bool is_connected() override { return true; }
  void send_keepalive() override {}
  void mark_down() override {}
  void mark_disposable() override {}
  void shutdown() override {}
  entity_addr_t get_peer_socket_addr() const override { return entity_addr_t(); }

protected:
  int send_msg(MessageRef&& m) override {
    sent.push_back(std::move(m));
    return 0;
  }
};

// Exposes MgrClient's protected session/map internals so tests can drive the
// live-session code paths without a running mgr daemon.  Mirrors the
// ClusterStateTestHelper pattern above.
class MgrClientTestHelper : public MgrClient {
public:
  MgrClientTestHelper(CephContext* cct_, Messenger* msgr_, MonMap* monmap_)
    : MgrClient(cct_, msgr_, monmap_)
  {}

  // Establish a live session backed by a recording connection, bypassing the
  // messenger / MgrMap-availability path that reconnect() needs in production.
  RecordingConnection*
  inject_session()
  {
    std::lock_guard l(lock);
    session.reset(new MgrSessionState());
    session->con = ceph::make_ref<RecordingConnection>(cct);
    return static_cast<RecordingConnection*>(session->con.get());
  }

  void
  test_set_map(const MgrMap& m)
  {
    std::lock_guard l(lock);
    map = m;
  }

  bool
  test_has_session()
  {
    std::lock_guard l(lock);
    return static_cast<bool>(session);
  }

  RecordingConnection*
  test_recording_con()
  {
    std::lock_guard l(lock);
    return session ? static_cast<RecordingConnection*>(session->con.get())
                   : nullptr;
  }

  uint32_t
  test_stats_period()
  {
    std::lock_guard l(lock);
    return stats_period;
  }

  uint32_t
  test_stats_threshold()
  {
    std::lock_guard l(lock);
    return stats_threshold;
  }

  size_t
  test_command_table_size()
  {
    std::lock_guard l(lock);
    return command_table.get_commands().size();
  }

  Context*
  test_report_callback()
  {
    std::lock_guard l(lock);
    return report_callback;
  }

  bool
  test_timer_cancel_event(Context* c)
  {
    std::lock_guard l(lock);
    return timer.cancel_event(c);
  }
};

class TestMgr : public ::testing::Test {
public:
  static void
  SetUpTestSuite()
  {
    if (!cct) {
      std::vector<const char*> args = {"unittest_mgr"};
      cct = global_init(
          nullptr, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
          CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

      cct->_conf.set_val("auth_client_required", "none");
      cct->_conf.set_val("auth_cluster_required", "none");
      cct->_conf.set_val("auth_service_required", "none");
      cct->_conf.apply_changes(nullptr);

      common_init_finish(cct.get());
    }
  }

  void
  SetUp() override
  {
    icp = std::make_unique<ca::io_context_pool>(1);
    mc = std::make_unique<MonClient>(cct.get(), *icp);
    messenger.reset(
        Messenger::create_client_messenger(cct.get(), "unittest_mgr"));
    messenger->start();
    objecter =
        std::make_unique<Objecter>(cct.get(), messenger.get(), mc.get(), *icp);

    ceph_assert(objecter != nullptr);
    objecter->set_client_incarnation(0);
    objecter->init();

    cs = std::make_unique<ClusterStateTestHelper>(
        mc.get(), objecter.get(), mgr_map);
  }

  void
  TearDown() override
  {
    // Shut down every test-owned client before the messengers they ride on.
    // Clients are owned by the fixture rather than the test body so that a fatal
    // gtest assertion -- which returns from the test body before its explicit
    // shutdown() -- cannot leave a client initialized; ~SafeTimer and
    // ~CommandTable both assert, and an uncaught abort there takes down the whole
    // test executable instead of failing a single test.  shutdown() is
    // idempotent and safe on an un-initialized client, so a second call after a
    // test's own shutdown() is harmless.
    for (auto& c : clients) {
      c->shutdown();
    }
    clients.clear();

    // Restore any suite-shared config values a test overrode.  cct is shared
    // across the whole suite, so a leaked override would bleed into later tests.
    for (auto it = conf_restores.rbegin(); it != conf_restores.rend(); ++it) {
      cct->_conf.set_val(it->first, it->second);
    }
    if (!conf_restores.empty()) {
      cct->_conf.apply_changes(nullptr);
      conf_restores.clear();
    }

    ceph_assert(objecter != nullptr);
    ceph_assert(mc != nullptr);
    objecter->shutdown();
    mc->shutdown();
    messenger->shutdown();
    messenger->wait();

    // Secondary messengers a test created through make_messenger().
    for (auto& m : extra_messengers) {
      m->shutdown();
      m->wait();
    }
    extra_messengers.clear();

    cs.reset();
    objecter.reset();
    mc.reset();
    messenger.reset();
    icp.reset();
  }

  // Construct a fixture-owned MgrClient (or a secondary-messenger variant) whose
  // shutdown is guaranteed by TearDown().  Does not init(); the caller drives the
  // init()/shutdown() lifecycle, and TearDown() backstops cleanup on every exit
  // path.
  MgrClient&
  make_client(Messenger* msgr_ = nullptr)
  {
    auto c = std::make_unique<MgrClient>(
        cct.get(), msgr_ ? msgr_ : messenger.get(), &monmap);
    MgrClient& ref = *c;
    clients.push_back(std::move(c));
    return ref;
  }

  // Same, but returns the MgrClientTestHelper subclass so tests can reach the
  // protected session/map internals.
  MgrClientTestHelper&
  make_helper_client(Messenger* msgr_ = nullptr)
  {
    auto c = std::make_unique<MgrClientTestHelper>(
        cct.get(), msgr_ ? msgr_ : messenger.get(), &monmap);
    MgrClientTestHelper& ref = *c;
    clients.push_back(std::move(c));
    return ref;
  }

  // A started secondary messenger owned by the fixture; shut down and waited on
  // in TearDown() so it is cleaned up even when a test exits early.
  Messenger&
  make_messenger(const char* name)
  {
    std::unique_ptr<Messenger> m(
        Messenger::create_client_messenger(cct.get(), name));
    m->start();
    Messenger& ref = *m;
    extra_messengers.push_back(std::move(m));
    return ref;
  }

  // Override a suite-shared config value for the duration of the test; the
  // original is captured and restored by TearDown() even on an early exit.
  void
  override_conf(const std::string& key, const std::string& val)
  {
    std::string orig;
    cct->_conf.get_val(key, &orig);
    conf_restores.emplace_back(key, orig);
    cct->_conf.set_val(key, val);
    cct->_conf.apply_changes(nullptr);
  }

protected:
  static inline boost::intrusive_ptr<CephContext> cct;
  std::unique_ptr<ClusterStateTestHelper> cs;
  std::unique_ptr<ca::io_context_pool> icp;
  std::unique_ptr<Messenger> messenger;
  std::unique_ptr<Objecter> objecter;
  std::unique_ptr<MonClient> mc;
  MgrMap mgr_map;
  OSDMap osd_map;
  MonMap monmap;
  std::vector<std::unique_ptr<MgrClient>> clients;
  std::vector<std::unique_ptr<Messenger>> extra_messengers;
  std::vector<std::pair<std::string, std::string>> conf_restores;
};

class ClusterStateTest : public TestMgr {
public:
  ceph::ref_t<DeviceState> device;

  void
  SetUp() override
  {
    TestMgr::SetUp();

    //Setup pools and notify osdmap
    pool.set_pg_num(2);
    osd_inc.new_pool_max = 1;
    osd_inc.new_pools[1] = pool;
    osd_map.apply_incremental(osd_inc);

    cs->with_osdmap_and_pgmap([&](const OSDMap& old_map, const PGMap& pg_map) {
      cs->notify_osdmap(osd_map);
    });

    stats->pool_stat[1] = store_statfs_t{};
    stats->set_src(entity_name_t::OSD(0));
    stats->osd_stat.seq = 1;
    pgstat.state = PG_STATE_ACTIVE;
    pgstat.reported_epoch = 1;
    pgstat.reported_seq = 2;
  }

  void
  ingest_and_pginc()
  {
    cs->ingest_pgstats(stats);
    p_inc = cs->test_get_pending_inc();
  }

protected:
  const mempool::osdmap::map<int64_t, pg_pool_t>& pools = osd_map.get_pools();
  OSDMap::Incremental osd_inc = OSDMap::Incremental(osd_map.get_epoch() + 1);
  ceph::ref_t<MPGStats> stats = ceph::make_ref<MPGStats>();
  PGMap::Incremental p_inc;
  pg_stat_t pgstat;
  pg_pool_t pool;
};

class DeviceStateTest : public ::testing::Test {
public:
  ceph::ref_t<DeviceState> device;

  void
  SetUp() override
  {
    device = ceph::make_ref<DeviceState>("test_device_111");
  }
};

struct PythonEnv : public ::testing::Environment {
  void
  SetUp() override
  {
    Py_Initialize();
  }

  void
  TearDown() override
  {
    Py_Finalize();
  }
};

class MgrOpRequestTestHelper : public ::testing::Test {
public:
  static inline boost::intrusive_ptr<CephContext> cct;
  std::unique_ptr<OpTracker> tracker;

  static void SetUpTestSuite() {
    if (!cct) {
      std::vector<const char*> args = {"unittest_mgr_mgroprequest"};
      cct = global_init(
          nullptr, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
          CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
      common_init_finish(cct.get());
    }
  }

  void SetUp() override {
    tracker = std::make_unique<OpTracker>(cct.get(), true, 1);
  }

  void TearDown() override {
    tracker.reset();
  }
};
