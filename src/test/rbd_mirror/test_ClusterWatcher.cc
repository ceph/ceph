// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "include/rados/librados.hpp"
#include "common/Cond.h"
#include "common/errno.h"
#include "common/ceph_mutex.h"
#include "librbd/internal.h"
#include "librbd/api/Mirror.h"
#include "tools/rbd_mirror/ClusterWatcher.h"
#include "tools/rbd_mirror/ServiceDaemon.h"
#include "tools/rbd_mirror/Types.h"
#include "test/rbd_mirror/test_fixture.h"
#include "test/librados/test_cxx.h"
#include "test/librbd/test_support.h"
#include "gtest/gtest.h"
#include <boost/scope_exit.hpp>
#include <iostream>
#include <map>
#include <memory>
#include <set>

using rbd::mirror::ClusterWatcher;
using rbd::mirror::PeerSpec;
using rbd::mirror::RadosRef;
using std::map;
using std::set;
using std::string;

void register_test_cluster_watcher() {
}

class TestClusterWatcher : public ::rbd::mirror::TestFixture {
public:

  TestClusterWatcher() {
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

  void create_pool(bool enable_mirroring, const PeerSpec &peer,
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
      ASSERT_EQ(0, librbd::api::Mirror<>::peer_site_add(
                     ioctx, uuid != nullptr ? uuid : &gen_uuid,
                     RBD_MIRROR_PEER_DIRECTION_RX_TX,
                     peer.cluster_name, peer.client_name));
      m_pool_peers[pool_id].insert(peer);
    }
    if (name != nullptr) {
      *name = pool_name;
    }
  }

  void delete_pool(const string &name, const PeerSpec &peer) {
    int64_t pool_id = m_cluster->pool_lookup(name.c_str());
    ASSERT_GE(pool_id, 0);
    if (m_pool_peers.find(pool_id) != m_pool_peers.end()) {
      m_pool_peers[pool_id].erase(peer);
      if (m_pool_peers[pool_id].empty()) {
	m_pool_peers.erase(pool_id);
      }
    }
    m_pools.erase(name);
    ASSERT_EQ(0, m_cluster->pool_delete(name.c_str()));
  }

  void set_peer_config_key(const std::string& pool_name,
                           const PeerSpec &peer) {
    int64_t pool_id = m_cluster->pool_lookup(pool_name.c_str());
    ASSERT_GE(pool_id, 0);

    std::string json =
      "{"
        "\\\"mon_host\\\": \\\"" + peer.mon_host + "\\\", "
        "\\\"key\\\": \\\"" + peer.key + "\\\""
      "}";

    bufferlist in_bl;
    ASSERT_EQ(0, m_cluster->mon_command(
      "{"
        "\"prefix\": \"config-key set\","
        "\"key\": \"" RBD_MIRROR_PEER_CONFIG_KEY_PREFIX + stringify(pool_id) +
          "/" + peer.uuid + "\","
        "\"val\": \"" + json + "\"" +
      "}", in_bl, nullptr, nullptr));
  }

  void check_peers() {
    m_cluster_watcher->refresh_pools();
    std::lock_guard l{m_lock};
    ASSERT_EQ(m_pool_peers, m_cluster_watcher->get_pool_peers());
  }

  RadosRef m_cluster;
  ceph::mutex m_lock = ceph::make_mutex("TestClusterWatcherLock");
  std::unique_ptr<rbd::mirror::ServiceDaemon<>> m_service_daemon;
  std::unique_ptr<ClusterWatcher> m_cluster_watcher;

  set<string> m_pools;
  ClusterWatcher::PoolPeers m_pool_peers;
};

TEST_F(TestClusterWatcher, NoPools) {
  check_peers();
}

TEST_F(TestClusterWatcher, NoMirroredPools) {
  check_peers();
  create_pool(false, PeerSpec());
  check_peers();
  create_pool(false, PeerSpec());
  check_peers();
  create_pool(false, PeerSpec());
  check_peers();
}

TEST_F(TestClusterWatcher, ReplicatedPools) {
  PeerSpec site1("", "site1", "mirror1");
  PeerSpec site2("", "site2", "mirror2");
  string first_pool, last_pool;
  check_peers();
  create_pool(true, site1, &site1.uuid, &first_pool);
  check_peers();
  create_pool(false, PeerSpec());
  check_peers();
  create_pool(false, PeerSpec());
  check_peers();
  create_pool(false, PeerSpec());
  check_peers();
  create_pool(true, site2, &site2.uuid);
  check_peers();
  create_pool(true, site2, &site2.uuid);
  check_peers();
  create_pool(true, site2, &site2.uuid, &last_pool);
  check_peers();
  delete_pool(first_pool, site1);
  check_peers();
  delete_pool(last_pool, site2);
  check_peers();
}

TEST_F(TestClusterWatcher, ConfigKey) {
  REQUIRE(!is_librados_test_stub(*m_cluster));

  std::string pool_name;
  check_peers();

  PeerSpec site1("", "site1", "mirror1");
  create_pool(true, site1, &site1.uuid, &pool_name);
  check_peers();

  PeerSpec site2("", "site2", "mirror2");
  site2.mon_host = "abc";
  site2.key = "xyz";
  create_pool(false, site2, &site2.uuid);
  set_peer_config_key(pool_name, site2);

  check_peers();
}

TEST_F(TestClusterWatcher, SiteName) {
  REQUIRE(!is_librados_test_stub(*m_cluster));

  std::string site_name;
  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.mirror_site_name_get(*m_cluster, &site_name));

  m_cluster_watcher->refresh_pools();

  std::lock_guard l{m_lock};
  ASSERT_EQ(site_name, m_cluster_watcher->get_site_name());
}
