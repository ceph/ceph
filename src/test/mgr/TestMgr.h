// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <Python.h>

#include <cassert>

#include "common/async/context_pool.h"
#include "global/global_context.h"
#include "gtest/gtest.h"
#include "messages/MPGStats.h"
#include "mgr/ClusterState.h"
#include "mgr/DaemonState.h"
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
