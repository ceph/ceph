// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/Cond.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/InstanceReplayer.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/PoolMetaCache.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/group_replayer/Replayer.h"
#include "tools/rbd_mirror/group_replayer/BootstrapRequest.h"
#include "tools/rbd_mirror/group_replayer/GroupStateBuilder.h"
#include "tools/rbd_mirror/group_replayer/RemoveLocalGroupRequest.h"
#include <chrono>
#include <thread>

namespace rbd::mirror::group_replayer {

class TestGroupReplayer : public TestMockFixture {
public:
  struct Listener : GroupCtx::Listener {
    void handle_notification() override {}
  } listener;
  PoolMetaCache pool_meta_cache{g_ceph_context};
  GroupCtx group_ctx;
  std::list<std::pair<librados::IoCtx, ImageReplayer<librbd::ImageCtx>*>> images;
  Replayer<>* replayer = nullptr;
  InstanceReplayer<>* instance_replayer = nullptr;
  InstanceWatcher<>* instance_watcher = nullptr;

  void check_interrupted_group_removal(bool resync,
                                       bool metadata_error = false);

  void TearDown() override {
    if (replayer != nullptr) {
      C_SaferCond ctx;
      replayer->shut_down(&ctx);
      EXPECT_EQ(0, ctx.wait());
      replayer->destroy();
    }
    if (instance_watcher != nullptr) {
      instance_watcher->destroy();
    }
    if (instance_replayer != nullptr) {
      instance_replayer->destroy();
    }
    TestMockFixture::TearDown();
  }
};

// Test sequence: Restart with a CREATING mirror group snapshot whose image
// snap-ids were not saved. One image copy is complete and the other is pending.
// Keep only the first image in live membership. Remove both image snapshots
// before removing the group snapshot, and preserve an unrelated image snapshot.
TEST_F(TestGroupReplayer, PruneCreatingMirrorSnapshot) {
  const auto group_id = get_temp_image_name();
  const auto header = librbd::util::group_header_name(group_id);
  ASSERT_EQ(0, m_local_io_ctx.create(header, true));
  ASSERT_EQ(0, m_remote_io_ctx.create(header, true));
  ASSERT_EQ(0, librbd::cls_client::mirror_group_set(
    &m_local_io_ctx, group_id, {"global-group",
      cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
      cls::rbd::MIRROR_GROUP_STATE_CREATING}));
  ASSERT_EQ(0, librbd::cls_client::group_dir_add(
    &m_remote_io_ctx, RBD_GROUP_DIRECTORY, group_id, group_id));
  group_ctx.name = group_id;
  group_ctx.listener = &listener;

  cls::rbd::GroupSnapshot local{
    "group-snap", cls::rbd::GroupSnapshotNamespaceMirror{
      cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote-uuid",
      "group-snap", cls::rbd::MIRROR_GROUP_SNAPSHOT_INCOMPLETE},
    "local-snap", cls::rbd::GROUP_SNAPSHOT_STATE_CREATING};
  for (bool complete : {true, false}) {
    auto image_id = get_temp_image_name();
    auto image_header = librbd::util::header_name(image_id);
    ASSERT_EQ(0, librbd::cls_client::create_image(
      &m_local_io_ctx, image_header, 4096, 12, 0, image_id, -1));
    ASSERT_EQ(0, librbd::cls_client::dir_add_image(
      &m_local_io_ctx, RBD_DIRECTORY, image_id, image_id));
    cls::rbd::MirrorSnapshotNamespace ns{
      cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote-uuid", 12};
    ns.group_spec = {group_id, m_local_io_ctx.get_id()};
    librados::ObjectWriteOperation op;
    ns.group_snap_id = "older-group-snap";
    librbd::cls_client::snapshot_add(&op, 14, "older-image-snap", ns);
    ns.group_snap_id = local.id;
    librbd::cls_client::snapshot_add(&op, 15, "image-snap", ns);
    librbd::cls_client::mirror_image_snapshot_set_copy_progress(
      &op, 15, complete, 0);
    ASSERT_EQ(0, m_local_io_ctx.operate(image_header, &op));
    local.snaps.push_back({m_local_io_ctx.get_id(), image_id, CEPH_NOSNAP});
    if (complete) {
      ASSERT_EQ(0, librbd::cls_client::group_image_set(
        &m_local_io_ctx, header,
        {image_id, m_local_io_ctx.get_id(),
         cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED}));
    }
  }
  ASSERT_EQ(0, librbd::cls_client::group_snap_set(
    &m_local_io_ctx, header, local));
  std::string fsid;
  librados::Rados rados(m_remote_io_ctx);
  ASSERT_EQ(0, rados.cluster_fsid(&fsid));
  pool_meta_cache.set_remote_pool_meta(fsid, m_remote_io_ctx.get_id(),
                                     {"remote-uuid", "remote-peer"});
  instance_replayer = InstanceReplayer<>::create(
    m_local_io_ctx, "local-uuid", m_threads, nullptr, nullptr, nullptr,
    &pool_meta_cache);
  instance_watcher = InstanceWatcher<>::create(
    m_local_io_ctx, *m_threads->asio_engine, instance_replayer, nullptr);
  replayer = Replayer<>::create(m_threads, m_local_io_ctx, m_remote_io_ctx,
    "global-group", "local-uuid", &pool_meta_cache, group_id, group_id,
    &group_ctx, instance_watcher, &images);
  C_SaferCond ctx;
  replayer->init(&ctx);
  ASSERT_EQ(0, ctx.wait());

  auto wait_for_removal = [&] {
    for (unsigned i = 0; i < 200; ++i) {
      cls::rbd::GroupSnapshot snap;
      if (librbd::cls_client::group_snap_get_by_id(
            &m_local_io_ctx, header, local.id, &snap) == -ENOENT) {
        return true;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
  };
  ASSERT_TRUE(wait_for_removal());
  for (const auto& snap : local.snaps) {
    cls::rbd::SnapshotInfo info;
    EXPECT_EQ(-ENOENT, librbd::cls_client::snapshot_get(
      &m_local_io_ctx, librbd::util::header_name(snap.image_id), 15, &info));
    ASSERT_EQ(0, librbd::cls_client::snapshot_get(
      &m_local_io_ctx, librbd::util::header_name(snap.image_id), 14, &info));
  }
}

void TestGroupReplayer::check_interrupted_group_removal(
    bool resync, bool metadata_error) {
  const auto group_id = get_temp_image_name();
  const auto header = librbd::util::group_header_name(group_id);
  const auto global_group_id = "global-" + group_id;
  for (auto io_ctx : {m_local_io_ctx, m_remote_io_ctx}) {
    ASSERT_EQ(0, io_ctx.create(header, true));
    ASSERT_EQ(0, librbd::cls_client::group_dir_add(
      &io_ctx, RBD_GROUP_DIRECTORY, group_id, group_id));
    ASSERT_EQ(0, librbd::cls_client::mirror_group_set(
      &io_ctx, group_id, {global_group_id, cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                         cls::rbd::MIRROR_GROUP_STATE_CREATING}));
  }
  ASSERT_EQ(0, librbd::cls_client::mirror_group_set(
    &m_remote_io_ctx, group_id,
    {global_group_id, cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
     cls::rbd::MIRROR_GROUP_STATE_ENABLED}));
  cls::rbd::GroupSnapshot remote{
    "remote-snap", cls::rbd::GroupSnapshotNamespaceMirror{
      cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {}, {}, {},
      cls::rbd::MIRROR_GROUP_SNAPSHOT_INCOMPLETE},
    "remote-snap", cls::rbd::GROUP_SNAPSHOT_STATE_CREATING};
  ASSERT_EQ(0, librbd::cls_client::group_snap_set(
    &m_remote_io_ctx, header, remote));
  remote.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATED;
  cls::rbd::set_mirror_group_snapshot_complete(remote);
  ASSERT_EQ(0, librbd::cls_client::group_snap_set(
    &m_remote_io_ctx, header, remote));
  if (resync) {
    bufferlist value;
    value.append("true");
    ASSERT_EQ(0, librbd::cls_client::metadata_set(
      &m_local_io_ctx, header, {{RBD_GROUP_RESYNC, value}}));
  }

  using namespace testing;
  auto& io_ctx = get_mock_io_ctx(m_local_io_ctx);
  EXPECT_CALL(io_ctx, exec_internal(_, _, _, _, _, _, _, _))
    .WillRepeatedly(DoDefault());
  EXPECT_CALL(io_ctx, exec_internal(header, _, StrEq("rbd"),
                                  StrEq("group_snap_list"), _, _, _, _))
    .WillOnce(Return(-EIO)).RetiresOnSaturation();
  C_SaferCond interrupted;
  auto remove = RemoveLocalGroupRequest<>::create(
    m_local_io_ctx, global_group_id, resync, m_threads->work_queue,
    &interrupted);
  remove->send();
  ASSERT_EQ(-EIO, interrupted.wait());

  if (metadata_error) {
    EXPECT_CALL(io_ctx, exec_internal(header, _, StrEq("rbd"),
                                    StrEq("metadata_get"), _, _, _, _))
      .WillOnce(Return(-EIO)).RetiresOnSaturation();
  }
  // A replacement GroupReplayer starts with no in-memory resync flag.
  bool resync_requested = false;
  GroupStateBuilder<>* state_builder = nullptr;
  C_SaferCond ctx;
  auto req = BootstrapRequest<>::create(
    m_threads, m_local_io_ctx, m_remote_io_ctx, global_group_id, "local-uuid",
    "remote-peer", nullptr, nullptr, nullptr, nullptr, &pool_meta_cache,
    &resync_requested, &group_ctx, &images, &state_builder, &ctx);
  req->send();
  int r = ctx.wait();
  req->put();
  std::unique_ptr<GroupStateBuilder<>> state(state_builder);
  EXPECT_EQ(metadata_error ? -EIO : -ENOENT, r);
  EXPECT_EQ(resync && !metadata_error, resync_requested);
  std::string id;
  EXPECT_EQ(metadata_error ? 0 : -ENOENT, librbd::cls_client::dir_get_id(
    &m_local_io_ctx, RBD_GROUP_DIRECTORY, group_id, &id));
}

// Test sequence: Interrupt removal after DISABLING and the resync flag are
// saved. Start a new bootstrap with its in-memory flag cleared. Finish removal
// and restore that flag so GroupReplayer knows to recreate the secondary.
TEST_F(TestGroupReplayer, ResumeInterruptedResync) {
  check_interrupted_group_removal(true);
}

// Test sequence: Resume DISABLING cleanup without a saved resync flag while
// the remote group still exists. Finish removal without requesting recreation.
TEST_F(TestGroupReplayer, ResumeInterruptedRemovalWithoutResync) {
  check_interrupted_group_removal(false);
}

// Test sequence: Resume interrupted resync, but fail reading its saved flag.
// Return the I/O error and keep the local group for a later retry.
TEST_F(TestGroupReplayer, InterruptedResyncMetadataReadError) {
  check_interrupted_group_removal(true, true);
}

} // namespace rbd::mirror::group_replayer
