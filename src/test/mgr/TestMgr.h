// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <Python.h>

#include <cassert>

#include "common/async/context_pool.h"
#include "common/Finisher.h"
#include "common/LogClient.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "gtest/gtest.h"
#include "messages/MPGStats.h"
#include "messages/MMgrReport.h"
#include "mgr/ClusterState.h"
#include "mgr/DaemonServer.h"
#include "mgr/DaemonState.h"
#include "mgr/PyModuleRegistry.h"
#include "mgr/ThreadMonitor.h"
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
    ceph_assert(objecter != nullptr);
    ceph_assert(mc != nullptr);
    objecter->shutdown();
    mc->shutdown();
    messenger->shutdown();
    messenger->wait();

    cs.reset();
    objecter.reset();
    mc.reset();
    messenger.reset();
    icp.reset();
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

// Mgr itself requires full daemon init (monc, objecter, etc.) before it is
// usable; this helper only scaffolds construction/teardown.
class MgrTestHelper : public TestMgr {
public:
  LogChannelRef clog;
  LogChannelRef audit_clog;
  std::unique_ptr<PyModuleRegistry> py_registry;

  void SetUp() override {
    TestMgr::SetUp();
    clog = std::make_shared<LogChannel>(cct.get(), nullptr, "cluster");
    audit_clog = std::make_shared<LogChannel>(cct.get(), nullptr, "audit");
    py_registry = std::make_unique<PyModuleRegistry>(clog);
  }

  void TearDown() override {
    py_registry.reset();
    audit_clog.reset();
    clog.reset();
    TestMgr::TearDown();
  }
};

class DaemonServerTestHelper : public TestMgr {
public:
  LogChannelRef clog;
  LogChannelRef audit_clog;
  std::unique_ptr<PyModuleRegistry> py_registry;
  std::unique_ptr<DaemonStateIndex> daemon_state_index;
  std::unique_ptr<Finisher> finisher;
  std::unique_ptr<DaemonServer> daemon_server;

  void SetUp() override {
    TestMgr::SetUp();
    clog = std::make_shared<LogChannel>(cct.get(), nullptr, "cluster");
    audit_clog = std::make_shared<LogChannel>(cct.get(), nullptr, "audit");
    daemon_state_index = std::make_unique<DaemonStateIndex>();
    py_registry = std::make_unique<PyModuleRegistry>(clog);
    finisher = std::make_unique<Finisher>(cct.get(), "test_finisher", "test_fin");
    finisher->start();
    daemon_server = std::make_unique<DaemonServer>(
        mc.get(),
        *finisher,
        *daemon_state_index,
        *cs,
        *py_registry,
        clog,
        audit_clog);
  }

  void TearDown() override {
    daemon_server.reset();
    py_registry.reset();
    if (finisher) {
      finisher->stop();
      finisher.reset();
    }
    daemon_state_index.reset();
    audit_clog.reset();
    clog.reset();
    TestMgr::TearDown();
  }
};

class DaemonPerfCountersTestHelper : public ::testing::Test {
public:
  PerfCounterTypes types;
  std::unique_ptr<DaemonPerfCounters> perf_counters;
  
  void SetUp() override {
    perf_counters = std::make_unique<DaemonPerfCounters>(types);
  }

  void TearDown() override {
    perf_counters.reset();
    types.clear();
  }
};

class MockMetricListener : public MetricListener {
public:
  int update_count = 0;
  
  void handle_query_updated() override {
    update_count++;
  }
};

class MetricCollectorTestHelper : public ::testing::Test {
public:
  static inline boost::intrusive_ptr<CephContext> cct;
  
  static void SetUpTestSuite() {
    if (!cct) {
      std::vector<const char*> args = {"unittest_metriccollector"};
      cct = global_init(
          nullptr, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
          CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
      common_init_finish(cct.get());
    }
  }
};

class ThreadMonitorTestHelper : public ::testing::Test {
public:
  static inline boost::intrusive_ptr<CephContext> cct;
  std::unique_ptr<ThreadMonitor> thread_monitor;
  
  static void SetUpTestSuite() {
    if (!cct) {
      std::vector<const char*> args = {"unittest_threadmonitor"};
      cct = global_init(
          nullptr, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
          CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
      common_init_finish(cct.get());
    }
  }
  
  void SetUp() override {
    thread_monitor = std::make_unique<ThreadMonitor>(cct.get());
  }
  
  void TearDown() override {
    thread_monitor.reset();
  }
};

class PyModuleRegistryTestHelper : public ::testing::Test {
public:
  static inline boost::intrusive_ptr<CephContext> cct;
  LogChannelRef clog;
  std::unique_ptr<PyModuleRegistry> registry;
  
  static void SetUpTestSuite() {
    if (!cct) {
      std::vector<const char*> args = {"unittest_pymoduleregistry"};
      cct = global_init(
          nullptr, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
          CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
      common_init_finish(cct.get());
    }
  }
  
  void SetUp() override {
    clog = std::make_shared<LogChannel>(cct.get(), nullptr, "cluster");
    registry = std::make_unique<PyModuleRegistry>(clog);
  }
  
  void TearDown() override {
    registry.reset();
    clog.reset();
  }
};

class PyOSDMapTest : public ::testing::Test {
public:
  OSDMap osd_map;
  
  void SetUp() override {
    osd_map.set_epoch(1);
  }
};

class MgrStandbyTestHelper : public TestMgr {
public:
  LogChannelRef clog;
  std::unique_ptr<PyModuleRegistry> py_registry;
  
  void SetUp() override {
    TestMgr::SetUp();
    clog = std::make_shared<LogChannel>(cct.get(), nullptr, "cluster");
    py_registry = std::make_unique<PyModuleRegistry>(clog);
  }
  
  void TearDown() override {
    py_registry.reset();
    clog.reset();
    TestMgr::TearDown();
  }
};

// StandbyPyModules tests share the same fixture as MgrStandby.
using StandbyPyModulesTestHelper = MgrStandbyTestHelper;

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


