// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.hpp"
#include "librbd/internal.h"
#include "librbd/Utils.h"
#include "librbd/api/Mirror.h"
#include "test/librbd/test_support.h"
#include "test/rbd_mirror/test_fixture.h"
#include "tools/rbd_mirror/LeaderWatcher.h"
#include "tools/rbd_mirror/Threads.h"
#include "common/Cond.h"

#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"

using librbd::util::unique_lock_name;
using rbd::mirror::LeaderWatcher;

void register_test_leader_watcher() {
}

class TestLeaderWatcher : public ::rbd::mirror::TestFixture {
public:
  class Listener : public rbd::mirror::leader_watcher::Listener {
  public:
    Listener()
      : m_test_lock(unique_lock_name("LeaderWatcher::m_test_lock", this)) {
    }

    void on_acquire(int r, Context *ctx) {
      Mutex::Locker locker(m_test_lock);
      m_on_acquire_r = r;
      m_on_acquire = ctx;
    }

    void on_release(int r, Context *ctx) {
      Mutex::Locker locker(m_test_lock);
      m_on_release_r = r;
      m_on_release = ctx;
    }

    int acquire_count() const {
      Mutex::Locker locker(m_test_lock);
      return m_acquire_count;
    }

    int release_count() const {
      Mutex::Locker locker(m_test_lock);
      return m_release_count;
    }

    void post_acquire_handler(Context *on_finish) override {
      Mutex::Locker locker(m_test_lock);
      m_acquire_count++;
      on_finish->complete(m_on_acquire_r);
      m_on_acquire_r = 0;
      if (m_on_acquire != nullptr) {
        m_on_acquire->complete(0);
        m_on_acquire = nullptr;
      }
    }

    void pre_release_handler(Context *on_finish) override {
      Mutex::Locker locker(m_test_lock);
      m_release_count++;
      on_finish->complete(m_on_release_r);
      m_on_release_r = 0;
      if (m_on_release != nullptr) {
        m_on_release->complete(0);
        m_on_release = nullptr;
      }
    }

    void update_leader_handler(const std::string &leader_instance_id) override {
    }

    void handle_instances_added(const InstanceIds& instance_ids) override {
    }
    void handle_instances_removed(const InstanceIds& instance_ids) override {
    }

  private:
    mutable Mutex m_test_lock;
    int m_acquire_count = 0;
    int m_release_count = 0;
    int m_on_acquire_r = 0;
    int m_on_release_r = 0;
    Context *m_on_acquire = nullptr;
    Context *m_on_release = nullptr;
  };

  struct Connection {
    librados::Rados cluster;
    librados::IoCtx io_ctx;
  };

  std::list<std::unique_ptr<Connection> > m_connections;

  void SetUp() override {
    TestFixture::SetUp();
    EXPECT_EQ(0, librbd::api::Mirror<>::mode_set(m_local_io_ctx,
                                                 RBD_MIRROR_MODE_POOL));

    if (is_librados_test_stub(*_rados)) {
      // speed testing up a little
      EXPECT_EQ(0, _rados->conf_set("rbd_mirror_leader_heartbeat_interval",
                                    "1"));
    }
  }

  librados::IoCtx &create_connection(bool no_heartbeats = false) {
    m_connections.push_back(std::unique_ptr<Connection>(new Connection()));
    Connection *c = m_connections.back().get();

    EXPECT_EQ("", connect_cluster_pp(c->cluster));
    if (no_heartbeats) {
      EXPECT_EQ(0, c->cluster.conf_set("rbd_mirror_leader_heartbeat_interval",
                                       "3600"));
    } else if (is_librados_test_stub(*_rados)) {
      EXPECT_EQ(0, c->cluster.conf_set("rbd_mirror_leader_heartbeat_interval",
                                       "1"));
    }
    EXPECT_EQ(0, c->cluster.ioctx_create(_local_pool_name.c_str(), c->io_ctx));

    return c->io_ctx;
  }
};

TEST_F(TestLeaderWatcher, InitShutdown)
{
  Listener listener;
  LeaderWatcher<> leader_watcher(m_threads, m_local_io_ctx, &listener);

  C_SaferCond on_init_acquire;
  listener.on_acquire(0, &on_init_acquire);
  ASSERT_EQ(0, leader_watcher.init());
  ASSERT_EQ(0, on_init_acquire.wait());
  ASSERT_TRUE(leader_watcher.is_leader());

  leader_watcher.shut_down();
  ASSERT_EQ(1, listener.acquire_count());
  ASSERT_EQ(1, listener.release_count());
  ASSERT_FALSE(leader_watcher.is_leader());
}

TEST_F(TestLeaderWatcher, Release)
{
  Listener listener;
  LeaderWatcher<> leader_watcher(m_threads, m_local_io_ctx, &listener);

  C_SaferCond on_init_acquire;
  listener.on_acquire(0, &on_init_acquire);
  ASSERT_EQ(0, leader_watcher.init());
  ASSERT_EQ(0, on_init_acquire.wait());
  ASSERT_TRUE(leader_watcher.is_leader());

  C_SaferCond on_release;
  C_SaferCond on_acquire;
  listener.on_release(0, &on_release);
  listener.on_acquire(0, &on_acquire);
  leader_watcher.release_leader();
  ASSERT_EQ(0, on_release.wait());
  ASSERT_FALSE(leader_watcher.is_leader());

  // wait for lock re-acquired due to no another locker
  ASSERT_EQ(0, on_acquire.wait());
  ASSERT_TRUE(leader_watcher.is_leader());

  C_SaferCond on_release2;
  listener.on_release(0, &on_release2);
  leader_watcher.release_leader();
  ASSERT_EQ(0, on_release2.wait());

  leader_watcher.shut_down();
  ASSERT_EQ(2, listener.acquire_count());
  ASSERT_EQ(2, listener.release_count());
}

TEST_F(TestLeaderWatcher, ListenerError)
{
  Listener listener;
  LeaderWatcher<> leader_watcher(m_threads, m_local_io_ctx, &listener);

  // make listener return error on acquire
  C_SaferCond on_init_acquire, on_init_release;
  listener.on_acquire(-EINVAL, &on_init_acquire);
  listener.on_release(0, &on_init_release);
  ASSERT_EQ(0, leader_watcher.init());
  ASSERT_EQ(0, on_init_acquire.wait());
  ASSERT_EQ(0, on_init_release.wait());
  ASSERT_FALSE(leader_watcher.is_leader());

  // wait for lock re-acquired due to no another locker
  C_SaferCond on_acquire;
  listener.on_acquire(0, &on_acquire);
  ASSERT_EQ(0, on_acquire.wait());
  ASSERT_TRUE(leader_watcher.is_leader());

  // make listener return error on release
  C_SaferCond on_release;
  listener.on_release(-EINVAL, &on_release);
  leader_watcher.release_leader();
  ASSERT_EQ(0, on_release.wait());
  ASSERT_FALSE(leader_watcher.is_leader());

  leader_watcher.shut_down();
  ASSERT_EQ(2, listener.acquire_count());
  ASSERT_EQ(2, listener.release_count());
  ASSERT_FALSE(leader_watcher.is_leader());
}

TEST_F(TestLeaderWatcher, Two)
{
  Listener listener1;
  LeaderWatcher<> leader_watcher1(m_threads, create_connection(), &listener1);

  C_SaferCond on_init_acquire;
  listener1.on_acquire(0, &on_init_acquire);
  ASSERT_EQ(0, leader_watcher1.init());
  ASSERT_EQ(0, on_init_acquire.wait());

  Listener listener2;
  LeaderWatcher<> leader_watcher2(m_threads, create_connection(), &listener2);

  ASSERT_EQ(0, leader_watcher2.init());
  ASSERT_TRUE(leader_watcher1.is_leader());
  ASSERT_FALSE(leader_watcher2.is_leader());

  C_SaferCond on_release;
  C_SaferCond on_acquire;
  listener1.on_release(0, &on_release);
  listener2.on_acquire(0, &on_acquire);
  leader_watcher1.release_leader();
  ASSERT_EQ(0, on_release.wait());
  ASSERT_FALSE(leader_watcher1.is_leader());

  // wait for lock acquired by another watcher
  ASSERT_EQ(0, on_acquire.wait());
  ASSERT_TRUE(leader_watcher2.is_leader());

  leader_watcher1.shut_down();
  leader_watcher2.shut_down();

  ASSERT_EQ(1, listener1.acquire_count());
  ASSERT_EQ(1, listener1.release_count());
  ASSERT_EQ(1, listener2.acquire_count());
  ASSERT_EQ(1, listener2.release_count());
}

TEST_F(TestLeaderWatcher, Break)
{
  Listener listener1, listener2;
  LeaderWatcher<> leader_watcher1(m_threads,
                                  create_connection(true /* no heartbeats */),
                                  &listener1);
  LeaderWatcher<> leader_watcher2(m_threads, create_connection(), &listener2);

  C_SaferCond on_init_acquire;
  listener1.on_acquire(0, &on_init_acquire);
  ASSERT_EQ(0, leader_watcher1.init());
  ASSERT_EQ(0, on_init_acquire.wait());

  C_SaferCond on_acquire;
  listener2.on_acquire(0, &on_acquire);
  ASSERT_EQ(0, leader_watcher2.init());
  ASSERT_FALSE(leader_watcher2.is_leader());

  // wait for lock broken due to no heartbeats and re-acquired
  ASSERT_EQ(0, on_acquire.wait());
  ASSERT_TRUE(leader_watcher2.is_leader());

  leader_watcher1.shut_down();
  leader_watcher2.shut_down();
}

TEST_F(TestLeaderWatcher, Stress)
{
  const int WATCHERS_COUNT = 20;
  std::list<LeaderWatcher<> *> leader_watchers;
  Listener listener;

  for (int i = 0; i < WATCHERS_COUNT; i++) {
    auto leader_watcher =
      new LeaderWatcher<>(m_threads, create_connection(), &listener);
    leader_watchers.push_back(leader_watcher);
  }

  C_SaferCond on_init_acquire;
  listener.on_acquire(0, &on_init_acquire);
  for (auto &leader_watcher : leader_watchers) {
    ASSERT_EQ(0, leader_watcher->init());
  }
  ASSERT_EQ(0, on_init_acquire.wait());

  while (true) {
    C_SaferCond on_acquire;
    listener.on_acquire(0, &on_acquire);
    std::unique_ptr<LeaderWatcher<> > leader_watcher;
    for (auto it = leader_watchers.begin(); it != leader_watchers.end(); ) {
      if ((*it)->is_leader()) {
        ASSERT_FALSE(leader_watcher);
        leader_watcher.reset(*it);
        it = leader_watchers.erase(it);
      } else {
        it++;
      }
    }

    ASSERT_TRUE(leader_watcher);
    leader_watcher->shut_down();
    if (leader_watchers.empty()) {
      break;
    }
    ASSERT_EQ(0, on_acquire.wait());
  }
}
