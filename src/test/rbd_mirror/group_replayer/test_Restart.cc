// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/Cond.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/PoolMetaCache.h"
#include "tools/rbd_mirror/group_replayer/Replayer.h"
#include <chrono>
#include <thread>

namespace rbd::mirror::group_replayer {

class TestGroupReplayerRestart : public TestMockFixture {
public:
  struct Listener : GroupCtx::Listener {
    void handle_notification() override {}
  } listener;
  PoolMetaCache pool_meta_cache{g_ceph_context};
  GroupCtx group_ctx;
  std::list<std::pair<librados::IoCtx, ImageReplayer<librbd::ImageCtx>*>> images;
  Replayer<>* replayer = nullptr;

  void TearDown() override {
    if (replayer != nullptr) {
      C_SaferCond ctx;
      replayer->shut_down(&ctx);
      EXPECT_EQ(0, ctx.wait());
      replayer->destroy();
    }
    TestMockFixture::TearDown();
  }
};

// Test sequence: Restart first sync after saving a CREATED local snapshot but
// before its copy is complete and the group is enabled. Use an empty group to
// isolate group-state recovery from image creation. Finish enabling the group.
TEST_F(TestGroupReplayerRestart, RestoreGroupStateAfterCreatedSnapshot) {
  const auto group_id = get_temp_image_name();
  const auto header = librbd::util::group_header_name(group_id);
  const auto global_id = "global-" + group_id;
  for (auto io_ctx : {m_local_io_ctx, m_remote_io_ctx}) {
    ASSERT_EQ(0, io_ctx.create(header, true));
    ASSERT_EQ(0, librbd::cls_client::group_dir_add(
      &io_ctx, RBD_GROUP_DIRECTORY, group_id, group_id));
    ASSERT_EQ(0, librbd::cls_client::mirror_group_set(
      &io_ctx, group_id, {global_id, cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                         cls::rbd::MIRROR_GROUP_STATE_CREATING}));
  }
  ASSERT_EQ(0, librbd::cls_client::mirror_group_set(
    &m_remote_io_ctx, group_id,
    {global_id, cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
     cls::rbd::MIRROR_GROUP_STATE_ENABLED}));
  cls::rbd::GroupSnapshot local{
    "group-snap", cls::rbd::GroupSnapshotNamespaceMirror{
      cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote-uuid",
      "group-snap", cls::rbd::MIRROR_GROUP_SNAPSHOT_INCOMPLETE},
    "local-snap", cls::rbd::GROUP_SNAPSHOT_STATE_CREATING};
  auto remote = local;
  remote.snapshot_namespace = cls::rbd::GroupSnapshotNamespaceMirror{
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"remote-peer"}, {}, {},
    cls::rbd::MIRROR_GROUP_SNAPSHOT_COMPLETE};
  for (auto entry : {std::make_pair(m_local_io_ctx, local),
                     std::make_pair(m_remote_io_ctx, remote)}) {
    ASSERT_EQ(0, librbd::cls_client::group_snap_set(
      &entry.first, header, entry.second));
    entry.second.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATED;
    ASSERT_EQ(0, librbd::cls_client::group_snap_set(
      &entry.first, header, entry.second));
  }
  std::string fsid;
  librados::Rados rados(m_remote_io_ctx);
  ASSERT_EQ(0, rados.cluster_fsid(&fsid));
  pool_meta_cache.set_remote_pool_meta(fsid, m_remote_io_ctx.get_id(),
                                     {"remote-uuid", "remote-peer"});
  group_ctx.name = group_id;
  group_ctx.listener = &listener;
  replayer = Replayer<>::create(
    m_threads, m_local_io_ctx, m_remote_io_ctx, global_id, "local-uuid",
    &pool_meta_cache, group_id, group_id, &group_ctx, nullptr, &images);
  C_SaferCond ctx;
  replayer->init(&ctx);
  ASSERT_EQ(0, ctx.wait());
  cls::rbd::MirrorGroup group;
  for (unsigned i = 0; i < 50; ++i) {
    ASSERT_EQ(0, librbd::cls_client::mirror_group_get(
      &m_local_io_ctx, group_id, &group));
    if (group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  EXPECT_EQ(cls::rbd::MIRROR_GROUP_STATE_ENABLED, group.state);
}

} // namespace rbd::mirror::group_replayer
