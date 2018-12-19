// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_client.h"
#include "test/rbd_mirror/test_fixture.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/Instances.h"
#include "tools/rbd_mirror/Threads.h"
#include "common/Cond.h"

#include "test/librados/test.h"
#include "gtest/gtest.h"
#include <vector>

using rbd::mirror::InstanceWatcher;
using rbd::mirror::Instances;

void register_test_instances() {
}

class TestInstances : public ::rbd::mirror::TestFixture {
public:
  struct Listener : public rbd::mirror::instances::Listener {
    std::mutex lock;

    struct Instance {
      uint32_t count = 0;
      std::set<std::string> ids;
      C_SaferCond ctx;
    };

    Instance add;
    Instance remove;

    void handle(const InstanceIds& instance_ids, Instance* instance) {
      std::unique_lock<std::mutex> locker(lock);
      for (auto& instance_id : instance_ids) {
        ceph_assert(instance->count > 0);
        --instance->count;

        instance->ids.insert(instance_id);
        if (instance->count == 0) {
          instance->ctx.complete(0);
        }
      }
    }

    void handle_added(const InstanceIds& instance_ids) override {
      handle(instance_ids, &add);
    }

    void handle_removed(const InstanceIds& instance_ids) override {
      handle(instance_ids, &remove);
    }
  };

  virtual void SetUp() {
    TestFixture::SetUp();
    m_local_io_ctx.remove(RBD_MIRROR_LEADER);
    EXPECT_EQ(0, m_local_io_ctx.create(RBD_MIRROR_LEADER, true));

    m_instance_id = stringify(m_local_io_ctx.get_instance_id());
  }

  Listener m_listener;
  std::string m_instance_id;
};

TEST_F(TestInstances, InitShutdown)
{
  m_listener.add.count = 1;
  Instances<> instances(m_threads, m_local_io_ctx, m_instance_id, m_listener);

  std::string instance_id = "instance_id";
  ASSERT_EQ(0, librbd::cls_client::mirror_instances_add(&m_local_io_ctx,
                                                        instance_id));

  C_SaferCond on_init;
  instances.init(&on_init);
  ASSERT_EQ(0, on_init.wait());

  ASSERT_LT(0U, m_listener.add.count);
  instances.unblock_listener();

  ASSERT_EQ(0, m_listener.add.ctx.wait());
  ASSERT_EQ(std::set<std::string>({instance_id}), m_listener.add.ids);

  C_SaferCond on_shut_down;
  instances.shut_down(&on_shut_down);
  ASSERT_EQ(0, on_shut_down.wait());
}

TEST_F(TestInstances, InitEnoent)
{
  Instances<> instances(m_threads, m_local_io_ctx, m_instance_id, m_listener);

  m_local_io_ctx.remove(RBD_MIRROR_LEADER);

  C_SaferCond on_init;
  instances.init(&on_init);
  ASSERT_EQ(0, on_init.wait());

  C_SaferCond on_shut_down;
  instances.shut_down(&on_shut_down);
  ASSERT_EQ(0, on_shut_down.wait());
}

TEST_F(TestInstances, NotifyRemove)
{
  // speed testing up a little
  EXPECT_EQ(0, _rados->conf_set("rbd_mirror_leader_heartbeat_interval", "1"));
  EXPECT_EQ(0, _rados->conf_set("rbd_mirror_leader_max_missed_heartbeats",
                                "2"));
  EXPECT_EQ(0, _rados->conf_set("rbd_mirror_leader_max_acquire_attempts_before_break",
                                "0"));

  m_listener.add.count = 2;
  m_listener.remove.count = 1;
  Instances<> instances(m_threads, m_local_io_ctx, m_instance_id, m_listener);

  std::string instance_id1 = "instance_id1";
  std::string instance_id2 = "instance_id2";

  ASSERT_EQ(0, librbd::cls_client::mirror_instances_add(&m_local_io_ctx,
                                                        instance_id1));


  C_SaferCond on_init;
  instances.init(&on_init);
  ASSERT_EQ(0, on_init.wait());

  instances.acked({instance_id1, instance_id2});

  ASSERT_LT(0U, m_listener.add.count);
  instances.unblock_listener();

  ASSERT_EQ(0, m_listener.add.ctx.wait());
  ASSERT_EQ(std::set<std::string>({instance_id1, instance_id2}),
            m_listener.add.ids);

  std::vector<std::string> instance_ids;
  for (int i = 0; i < 100; i++) {
    instances.acked({instance_id1});
    if (m_listener.remove.count > 0) {
      usleep(250000);
    }
  }

  instances.acked({instance_id1});
  ASSERT_EQ(0, m_listener.remove.ctx.wait());
  ASSERT_EQ(std::set<std::string>({instance_id2}),
           m_listener.remove.ids);

  C_SaferCond on_get;
  instances.acked({instance_id1});
  InstanceWatcher<>::get_instances(m_local_io_ctx, &instance_ids, &on_get);
  EXPECT_EQ(0, on_get.wait());
  EXPECT_EQ(1U, instance_ids.size());
  ASSERT_EQ(instance_ids[0], instance_id1);

  C_SaferCond on_shut_down;
  instances.shut_down(&on_shut_down);
  ASSERT_EQ(0, on_shut_down.wait());
}
