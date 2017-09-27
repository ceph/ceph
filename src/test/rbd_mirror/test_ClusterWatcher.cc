// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "include/rados/librados.hpp"
#include "common/Cond.h"
#include "common/errno.h"
#include "common/Mutex.h"
#include "librbd/internal.h"
#include "librbd/api/Mirror.h"
#include "tools/rbd_mirror/ClusterWatcher.h"
#include "tools/rbd_mirror/ServiceDaemon.h"
#include "tools/rbd_mirror/types.h"
#include "test/rbd_mirror/test_fixture.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"
#include <boost/scope_exit.hpp>
#include <iostream>
#include <map>
#include <memory>
#include <set>

using rbd::mirror::ClusterWatcher;
using rbd::mirror::peer_t;
using rbd::mirror::pool_config_t;
using rbd::mirror::RadosRef;
using std::map;
using std::set;
using std::string;

void register_test_cluster_watcher() {
}

class TestClusterWatcher : public ::rbd::mirror::TestFixture {
public:

  TestClusterWatcher() : m_lock("TestClusterWatcherLock")
  {
    m_cluster = std::make_shared<librados::Rados>();
    EXPECT_EQ("", connect_cluster_pp(*m_cluster));
  }

  ~TestClusterWatcher() override {
    m_cluster->wait_for_latest_osdmap();
    for (auto& pool : m_pools) {
      EXPECT_EQ(0, m_cluster->pool_delete(pool.c_str()));
    }
  }

  void SetUp() override {
    TestFixture::SetUp();
    m_service_daemon.reset(new rbd::mirror::ServiceDaemon<>(g_ceph_context,
                                                            m_cluster,
                                                            m_threads));
    m_cluster_watcher.reset(new ClusterWatcher(m_cluster, m_lock,
                                               m_service_daemon.get()));
  }

  void TearDown() override {
    m_service_daemon.reset();
    m_cluster_watcher.reset();
    TestFixture::TearDown();
  }

  void create_pool(bool enable_mirroring, const peer_t &peer,
                   string *uuid = nullptr, string *name=nullptr) {
    string pool_name = get_temp_pool_name("test-rbd-mirror-");
    ASSERT_EQ(0, m_cluster->pool_create(pool_name.c_str()));

    int64_t pool_id = m_cluster->pool_lookup(pool_name.c_str());
    ASSERT_GE(pool_id, 0);

    librados::IoCtx ioctx;
    ASSERT_EQ(0, m_cluster->ioctx_create2(pool_id, ioctx));
    ioctx.application_enable("rbd", true);

    m_pools.insert(pool_name);
    if (enable_mirroring) {
      ASSERT_EQ(0, librbd::api::Mirror<>::mode_set(ioctx,
                                                   RBD_MIRROR_MODE_POOL));

      std::string gen_uuid;
      ASSERT_EQ(0, librbd::api::Mirror<>::peer_add(ioctx,
						   uuid != nullptr ? uuid :
                                                                     &gen_uuid,
						   peer.cluster_name,
						   peer.client_name));
      m_pool_configs[pool_id] = pool_config_t{pool_name, "", {peer}};
    }
    if (name != nullptr) {
      *name = pool_name;
    }
  }

  void delete_pool(const string &name) {
    int64_t pool_id = m_cluster->pool_lookup(name.c_str());
    ASSERT_GE(pool_id, 0);
    if (m_pool_configs.find(pool_id) != m_pool_configs.end()) {
      m_pool_configs.erase(pool_id);
    }
    m_pools.erase(name);
    ASSERT_EQ(0, m_cluster->pool_delete(name.c_str()));
  }

  void create_cache_pool(const string &base_pool, string *cache_pool_name) {
    bufferlist inbl;
    *cache_pool_name = get_temp_pool_name("test-rbd-mirror-");
    ASSERT_EQ(0, m_cluster->pool_create(cache_pool_name->c_str()));

    ASSERT_EQ(0, m_cluster->mon_command(
      "{\"prefix\": \"osd tier add\", \"pool\": \"" + base_pool +
      "\", \"tierpool\": \"" + *cache_pool_name +
      "\", \"force_nonempty\": \"--force-nonempty\" }",
      inbl, NULL, NULL));
    ASSERT_EQ(0, m_cluster->mon_command(
      "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + base_pool +
      "\", \"overlaypool\": \"" + *cache_pool_name + "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, m_cluster->mon_command(
      "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + *cache_pool_name +
      "\", \"mode\": \"writeback\"}",
      inbl, NULL, NULL));
    m_cluster->wait_for_latest_osdmap();
  }

  void remove_cache_pool(const string &base_pool, const string &cache_pool) {
    bufferlist inbl;
    // tear down tiers
    ASSERT_EQ(0, m_cluster->mon_command(
      "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + base_pool +
      "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, m_cluster->mon_command(
      "{\"prefix\": \"osd tier remove\", \"pool\": \"" + base_pool +
      "\", \"tierpool\": \"" + cache_pool + "\"}",
      inbl, NULL, NULL));
    m_cluster->wait_for_latest_osdmap();
    m_cluster->pool_delete(cache_pool.c_str());
  }

  void check_configs() {
    m_cluster_watcher->refresh_pools();
    Mutex::Locker l(m_lock);
    ASSERT_EQ(m_pool_configs, m_cluster_watcher->get_pool_configs());
  }

  RadosRef m_cluster;
  Mutex m_lock;
  unique_ptr<rbd::mirror::ServiceDaemon<>> m_service_daemon;
  unique_ptr<ClusterWatcher> m_cluster_watcher;

  set<string> m_pools;
  ClusterWatcher::PoolConfigs m_pool_configs;
};

TEST_F(TestClusterWatcher, NoPools) {
  check_configs();
}

TEST_F(TestClusterWatcher, NoMirroredPools) {
  check_configs();
  create_pool(false, peer_t());
  check_configs();
  create_pool(false, peer_t());
  check_configs();
  create_pool(false, peer_t());
  check_configs();
}

TEST_F(TestClusterWatcher, ReplicatedPools) {
  peer_t site1("", "site1", "mirror1");
  peer_t site2("", "site2", "mirror2");
  string first_pool, last_pool;
  check_configs();
  create_pool(true, site1, &site1.uuid, &first_pool);
  check_configs();
  create_pool(false, peer_t());
  check_configs();
  create_pool(false, peer_t());
  check_configs();
  create_pool(false, peer_t());
  check_configs();
  create_pool(true, site2, &site2.uuid);
  check_configs();
  create_pool(true, site2, &site2.uuid);
  check_configs();
  create_pool(true, site2, &site2.uuid, &last_pool);
  check_configs();
  delete_pool(first_pool);
  check_configs();
  delete_pool(last_pool);
  check_configs();
}

TEST_F(TestClusterWatcher, CachePools) {
  peer_t site1("", "site1", "mirror1");
  string base1, base2, cache1, cache2;
  create_pool(true, site1, &site1.uuid, &base1);
  check_configs();

  create_cache_pool(base1, &cache1);
  BOOST_SCOPE_EXIT( base1, cache1, this_ ) {
    this_->remove_cache_pool(base1, cache1);
  } BOOST_SCOPE_EXIT_END;
  check_configs();

  create_pool(false, peer_t(), nullptr, &base2);
  create_cache_pool(base2, &cache2);
  BOOST_SCOPE_EXIT( base2, cache2, this_ ) {
    this_->remove_cache_pool(base2, cache2);
  } BOOST_SCOPE_EXIT_END;
  check_configs();
}
