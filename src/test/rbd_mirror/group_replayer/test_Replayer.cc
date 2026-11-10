// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "test/rbd_mirror/test_mock_fixture.h"
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

  void check_user_snapshot_resync(bool missing_mirror_metadata);

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

void TestGroupReplayer::check_user_snapshot_resync(bool missing_mirror_metadata) {
  const auto group_id = get_temp_image_name();
  const auto header = librbd::util::group_header_name(group_id);
  const auto global_group_id = "global-" + group_id;
  const auto current_image = "img1-" + group_id;
  const auto former_image = "img2-" + group_id;
  for (auto io_ctx : {m_local_io_ctx, m_remote_io_ctx}) {
    ASSERT_EQ(0, io_ctx.create(header, true));
    ASSERT_EQ(0, librbd::cls_client::group_dir_add(
      &io_ctx, RBD_GROUP_DIRECTORY, group_id, group_id));
    ASSERT_EQ(0, librbd::cls_client::mirror_group_set(
      &io_ctx, group_id, {global_group_id, cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
                         cls::rbd::MIRROR_GROUP_STATE_ENABLED}));
    ASSERT_EQ(0, librbd::cls_client::group_image_set(
      &io_ctx, header, {current_image, io_ctx.get_id(),
                       cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED}));
    ASSERT_EQ(0, librbd::cls_client::mirror_image_set(
      &io_ctx, current_image, {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
        "global-" + current_image,
        cls::rbd::MIRROR_IMAGE_TYPE_GROUP, cls::rbd::MIRROR_IMAGE_STATE_ENABLED}));
    cls::rbd::GroupSnapshot user{
      "user-snap", cls::rbd::GroupSnapshotNamespaceUser{}, "snap1",
      cls::rbd::GROUP_SNAPSHOT_STATE_CREATING};
    user.snaps = {{io_ctx.get_id(), current_image, 10},
                  {io_ctx.get_id(), former_image, 11}};
    ASSERT_EQ(0, librbd::cls_client::group_snap_set(&io_ctx, header, user));
    user.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATED;
    ASSERT_EQ(0, librbd::cls_client::group_snap_set(&io_ctx, header, user));
    cls::rbd::GroupSnapshot mirror{
      "mirror-snap", cls::rbd::GroupSnapshotNamespaceMirror{
        io_ctx.get_id() == m_local_io_ctx.get_id() ?
          cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY :
          cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY,
        {}, "remote-uuid", "mirror-snap",
        cls::rbd::MIRROR_GROUP_SNAPSHOT_INCOMPLETE},
      "mirror", cls::rbd::GROUP_SNAPSHOT_STATE_CREATING};
    mirror.snaps = {{io_ctx.get_id(), current_image, 12}};
    ASSERT_EQ(0, librbd::cls_client::group_snap_set(&io_ctx, header, mirror));
    mirror.state = cls::rbd::GROUP_SNAPSHOT_STATE_CREATED;
    cls::rbd::set_mirror_group_snapshot_complete(mirror);
    ASSERT_EQ(0, librbd::cls_client::group_snap_set(&io_ctx, header, mirror));
  }
  if (!missing_mirror_metadata) {
    ASSERT_EQ(0, librbd::cls_client::mirror_image_set(
      &m_remote_io_ctx, former_image,
      {cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT, "global-" + former_image,
       cls::rbd::MIRROR_IMAGE_TYPE_STANDALONE,
       cls::rbd::MIRROR_IMAGE_STATE_ENABLED}));
  }
  const auto image_header = librbd::util::header_name(former_image);
  ASSERT_EQ(0, librbd::cls_client::create_image(
    &m_local_io_ctx, image_header, 4096, 12, 0, former_image, -1));
  librados::ObjectWriteOperation op;
  librbd::cls_client::snapshot_add(&op, 11, "saved-image-snap",
    cls::rbd::ImageSnapshotNamespaceGroup{
      m_local_io_ctx.get_id(), group_id, "user-snap"});
  ASSERT_EQ(0, m_local_io_ctx.operate(image_header, &op));
  bufferlist value;
  value.append("true");
  ASSERT_EQ(0, librbd::cls_client::metadata_set(
    &m_local_io_ctx, header, {{RBD_GROUP_RESYNC, value}}));

  bool resync = false;
  GroupStateBuilder<>* state_builder = nullptr;
  C_SaferCond ctx;
  auto req = BootstrapRequest<>::create(
    m_threads, m_local_io_ctx, m_remote_io_ctx, global_group_id, "local-uuid",
    nullptr, nullptr, nullptr, nullptr, &pool_meta_cache, &resync,
    &group_ctx, &images, &state_builder, &ctx);
  req->send();
  int r = ctx.wait();
  req->put();
  std::unique_ptr<GroupStateBuilder<>> state(state_builder);
  ASSERT_EQ(-EBUSY, r);
  cls::rbd::GroupSnapshot user;
  ASSERT_EQ(0, librbd::cls_client::group_snap_get_by_id(
    &m_local_io_ctx, header, "user-snap", &user));
  ASSERT_EQ(2U, user.snaps.size());
  cls::rbd::SnapshotInfo image_snap;
  ASSERT_EQ(0, librbd::cls_client::snapshot_get(
    &m_local_io_ctx, image_header, 11, &image_snap));
  cls::rbd::MirrorGroup mirror_group;
  ASSERT_EQ(0, librbd::cls_client::mirror_group_get(
    &m_local_io_ctx, group_id, &mirror_group));
  ASSERT_EQ(cls::rbd::MIRROR_GROUP_STATE_ENABLED, mirror_group.state);
}

// Test sequence: Resync a group whose retained user snapshot includes a
// detached image with mirror metadata already removed. Fail with -EBUSY before
// removing the local group or either saved snapshot.
TEST_F(TestGroupReplayer, ResyncRejectsDisabledUserSnapshotMember) {
  check_user_snapshot_resync(true);
}

// Test sequence: Resync after relocation while a retained user snapshot still
// references an image now mirrored independently. Preserve that image snapshot
// and report -EBUSY instead of deleting it and waiting for a group replayer.
TEST_F(TestGroupReplayer, ResyncRejectsStandaloneUserSnapshotMember) {
  check_user_snapshot_resync(false);
}

} // namespace rbd::mirror::group_replayer
