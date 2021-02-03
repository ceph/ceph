// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "cls/rbd/cls_rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"
#include "librbd/internal.h"
#include "test/rbd_mirror/test_fixture.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/Threads.h"
#include "common/Cond.h"

#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"

using rbd::mirror::InstanceWatcher;

void register_test_instance_watcher() {
}

class TestInstanceWatcher : public ::rbd::mirror::TestFixture {
public:
  std::string m_instance_id;
  std::string m_oid;

  void SetUp() override {
    TestFixture::SetUp();
    m_local_io_ctx.remove(RBD_MIRROR_LEADER);
    EXPECT_EQ(0, m_local_io_ctx.create(RBD_MIRROR_LEADER, true));

    m_instance_id = stringify(m_local_io_ctx.get_instance_id());
    m_oid = RBD_MIRROR_INSTANCE_PREFIX + m_instance_id;
  }

  void get_instances(std::vector<std::string> *instance_ids) {
    instance_ids->clear();
    C_SaferCond on_get;
    InstanceWatcher<>::get_instances(m_local_io_ctx, instance_ids, &on_get);
    EXPECT_EQ(0, on_get.wait());
  }
};

TEST_F(TestInstanceWatcher, InitShutdown)
{
  InstanceWatcher<> instance_watcher(m_local_io_ctx, *m_threads->asio_engine,
                                     nullptr, nullptr, m_instance_id);
  std::vector<std::string> instance_ids;
  get_instances(&instance_ids);
  ASSERT_EQ(0U, instance_ids.size());

  uint64_t size;
  ASSERT_EQ(-ENOENT, m_local_io_ctx.stat(m_oid, &size, nullptr));

  // Init
  ASSERT_EQ(0, instance_watcher.init());

  get_instances(&instance_ids);
  ASSERT_EQ(1U, instance_ids.size());
  ASSERT_EQ(m_instance_id, instance_ids[0]);

  ASSERT_EQ(0, m_local_io_ctx.stat(m_oid, &size, nullptr));
  std::list<obj_watch_t> watchers;
  ASSERT_EQ(0, m_local_io_ctx.list_watchers(m_oid, &watchers));
  ASSERT_EQ(1U, watchers.size());
  ASSERT_EQ(m_instance_id, stringify(watchers.begin()->watcher_id));

  get_instances(&instance_ids);
  ASSERT_EQ(1U, instance_ids.size());

  // Shutdown
  instance_watcher.shut_down();

  ASSERT_EQ(-ENOENT, m_local_io_ctx.stat(m_oid, &size, nullptr));
  get_instances(&instance_ids);
  ASSERT_EQ(0U, instance_ids.size());
}

TEST_F(TestInstanceWatcher, Remove)
{
  std::string instance_id = "instance_id";
  std::string oid = RBD_MIRROR_INSTANCE_PREFIX + instance_id;

  std::vector<std::string> instance_ids;
  get_instances(&instance_ids);
  ASSERT_EQ(0U, instance_ids.size());

  uint64_t size;
  ASSERT_EQ(-ENOENT, m_local_io_ctx.stat(oid, &size, nullptr));

  librados::Rados cluster;
  librados::IoCtx io_ctx;
  ASSERT_EQ("", connect_cluster_pp(cluster));
  ASSERT_EQ(0, cluster.ioctx_create(_local_pool_name.c_str(), io_ctx));
  InstanceWatcher<> instance_watcher(m_local_io_ctx, *m_threads->asio_engine,
                                     nullptr, nullptr, "instance_id");
  // Init
  ASSERT_EQ(0, instance_watcher.init());

  get_instances(&instance_ids);
  ASSERT_EQ(1U, instance_ids.size());
  ASSERT_EQ(instance_id, instance_ids[0]);

  ASSERT_EQ(0, m_local_io_ctx.stat(oid, &size, nullptr));
  std::list<obj_watch_t> watchers;
  ASSERT_EQ(0, m_local_io_ctx.list_watchers(oid, &watchers));
  ASSERT_EQ(1U, watchers.size());

  // Remove
  C_SaferCond on_remove;
  InstanceWatcher<>::remove_instance(m_local_io_ctx, *m_threads->asio_engine,
                                     "instance_id", &on_remove);
  ASSERT_EQ(0, on_remove.wait());

  ASSERT_EQ(-ENOENT, m_local_io_ctx.stat(oid, &size, nullptr));
  get_instances(&instance_ids);
  ASSERT_EQ(0U, instance_ids.size());

  // Shutdown
  instance_watcher.shut_down();

  ASSERT_EQ(-ENOENT, m_local_io_ctx.stat(m_oid, &size, nullptr));
  get_instances(&instance_ids);
  ASSERT_EQ(0U, instance_ids.size());

  // Remove NOENT
  C_SaferCond on_remove_noent;
  InstanceWatcher<>::remove_instance(m_local_io_ctx, *m_threads->asio_engine,
                                     instance_id, &on_remove_noent);
  ASSERT_EQ(0, on_remove_noent.wait());
}
