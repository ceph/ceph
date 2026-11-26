// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/Cond.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/group_replayer/RemoveLocalGroupRequest.h"

namespace rbd {
namespace mirror {
namespace group_replayer {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::Return;
using ::testing::StrEq;

class TestGroupReplayerRemoveLocalGroupRequest : public TestMockFixture {
public:
  const std::string group_id = get_temp_image_name();
  const std::string group_name = group_id;
  const std::string global_id = "global-" + group_id;

  void create_group(cls::rbd::MirrorGroupState state) {
    ASSERT_EQ(0, m_local_io_ctx.create(
      librbd::util::group_header_name(group_id), true));
    ASSERT_EQ(0, librbd::cls_client::group_dir_add(
      &m_local_io_ctx, RBD_GROUP_DIRECTORY, group_name, group_id));
    cls::rbd::MirrorGroup group{global_id, cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
      cls::rbd::MIRROR_GROUP_STATE_CREATING};
    ASSERT_EQ(0, librbd::cls_client::mirror_group_set(
      &m_local_io_ctx, group_id, group));
    if (state != group.state) {
      group.state = state;
      ASSERT_EQ(0, librbd::cls_client::mirror_group_set(
        &m_local_io_ctx, group_id, group));
    }
  }

  int remove_group() {
    C_SaferCond ctx;
    auto req = RemoveLocalGroupRequest<librbd::ImageCtx>::create(
      m_local_io_ctx, global_id, false, m_threads->work_queue, &ctx);
    req->send();
    return ctx.wait();
  }
};

// Test sequence: Model primary disable with no role snapshots or image metadata.
// With missing or stale removal intent, keep the group and its members.
TEST_F(TestGroupReplayerRemoveLocalGroupRequest, PreserveUnknownDisablingGroup) {
  create_group(cls::rbd::MIRROR_GROUP_STATE_DISABLING);
  cls::rbd::GroupImageStatus member{
    "image-id", m_local_io_ctx.get_id(), cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED};
  ASSERT_EQ(0, librbd::cls_client::group_image_set(
    &m_local_io_ctx, librbd::util::group_header_name(group_id), member));

  ASSERT_EQ(-EAGAIN, remove_group());
  bufferlist stale_intent;
  stale_intent.append("another-global-group-id");
  ASSERT_EQ(0, librbd::cls_client::metadata_set(&m_local_io_ctx, RBD_MIRRORING,
    {{".rbd_mirror.group_remove." + group_id, stale_intent}}));
  ASSERT_EQ(-EAGAIN, remove_group());
  std::string id;
  ASSERT_EQ(0, librbd::cls_client::dir_get_id(
    &m_local_io_ctx, RBD_GROUP_DIRECTORY, group_name, &id));
  ASSERT_EQ(group_id, id);
  std::vector<cls::rbd::GroupImageStatus> members;
  ASSERT_EQ(0, librbd::cls_client::group_image_list(
    &m_local_io_ctx, librbd::util::group_header_name(group_id), {}, 10, &members));
  ASSERT_EQ(1U, members.size());
  ASSERT_EQ("image-id", members.front().spec.image_id);
}

// Test sequence: Interrupt replica removal after DISABLING is saved.
// A new request must finish cleanup using the saved intent and remove it too.
TEST_F(TestGroupReplayerRemoveLocalGroupRequest, ResumeReplicaRemoval) {
  create_group(cls::rbd::MIRROR_GROUP_STATE_CREATING);
  EXPECT_CALL(get_mock_io_ctx(m_local_io_ctx),
              exec_internal(_, _, _, _, _, _, _, _)).WillRepeatedly(DoDefault());
  EXPECT_CALL(get_mock_io_ctx(m_local_io_ctx),
              exec_internal(librbd::util::group_header_name(group_id), _,
                            StrEq("rbd"), StrEq("group_snap_list"), _, _, _, _))
    .WillOnce(Return(-EIO)).RetiresOnSaturation();
  ASSERT_EQ(-EIO, remove_group());

  const std::string key = ".rbd_mirror.group_remove." + group_id;
  std::string intent;
  ASSERT_EQ(0, librbd::cls_client::metadata_get(
    &m_local_io_ctx, RBD_MIRRORING, key, &intent));
  ASSERT_EQ(global_id, intent);
  ASSERT_EQ(0, remove_group());
  std::string id;
  ASSERT_EQ(-ENOENT, librbd::cls_client::dir_get_id(
    &m_local_io_ctx, RBD_GROUP_DIRECTORY, group_name, &id));
  ASSERT_EQ(-ENOENT, librbd::cls_client::metadata_get(
    &m_local_io_ctx, RBD_MIRRORING, key, &intent));
}

} // namespace group_replayer
} // namespace mirror
} // namespace rbd
