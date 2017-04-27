// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_client.h"
#include "test/rbd_mirror/test_fixture.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/Instances.h"
#include "tools/rbd_mirror/Threads.h"

#include "test/librados/test.h"
#include "gtest/gtest.h"

using rbd::mirror::InstanceWatcher;
using rbd::mirror::Instances;

void register_test_instances() {
}

class TestInstances : public ::rbd::mirror::TestFixture {
public:
  virtual void SetUp() {
    TestFixture::SetUp();
    m_local_io_ctx.remove(RBD_MIRROR_LEADER);
    EXPECT_EQ(0, m_local_io_ctx.create(RBD_MIRROR_LEADER, true));
  }
};

TEST_F(TestInstances, InitShutdown)
{
  Instances<> instances(m_threads, m_local_io_ctx);

  std::string instance_id = "instance_id";
  ASSERT_EQ(0, librbd::cls_client::mirror_instances_add(&m_local_io_ctx,
                                                        instance_id));

  C_SaferCond on_init;
  instances.init(&on_init);
  ASSERT_EQ(0, on_init.wait());

  C_SaferCond on_shut_down;
  instances.shut_down(&on_shut_down);
  ASSERT_EQ(0, on_shut_down.wait());
}

TEST_F(TestInstances, InitEnoent)
{
  Instances<> instances(m_threads, m_local_io_ctx);

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

  Instances<> instances(m_threads, m_local_io_ctx);

  std::string instance_id1 = "instance_id1";
  std::string instance_id2 = "instance_id2";

  ASSERT_EQ(0, librbd::cls_client::mirror_instances_add(&m_local_io_ctx,
                                                        instance_id1));
  ASSERT_EQ(0, librbd::cls_client::mirror_instances_add(&m_local_io_ctx,
                                                        instance_id2));

  C_SaferCond on_init;
  instances.init(&on_init);
  ASSERT_EQ(0, on_init.wait());

  std::vector<std::string> instance_ids;

  for (int i = 0; i < 10; i++) {
    instances.notify(instance_id1);
    sleep(1);
    C_SaferCond on_get;
    InstanceWatcher<>::get_instances(m_local_io_ctx, &instance_ids, &on_get);
    EXPECT_EQ(0, on_get.wait());
    if (instance_ids.size() <= 1U) {
      break;
    }
  }

  ASSERT_EQ(1U, instance_ids.size());
  ASSERT_EQ(instance_ids[0], instance_id1);

  C_SaferCond on_shut_down;
  instances.shut_down(&on_shut_down);
  ASSERT_EQ(0, on_shut_down.wait());
}
