// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "common/bit_vector.hpp"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "librbd/image/DetachChildRequest.h"
#include "librbd/mirror/snapshot/RemoveImageStateRequest.h"
#include "librbd/operation/SnapshotRemoveRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {
namespace image {

template <>
class DetachChildRequest<MockImageCtx> {
public:
  static DetachChildRequest *s_instance;
  static DetachChildRequest *create(MockImageCtx &image_ctx,
                                    Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context *on_finish = nullptr;

  DetachChildRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

DetachChildRequest<MockImageCtx> *DetachChildRequest<MockImageCtx>::s_instance;

} // namespace image

namespace mirror {
namespace snapshot {

template<>
class RemoveImageStateRequest<MockImageCtx> {
public:
  static RemoveImageStateRequest *s_instance;
  Context *on_finish = nullptr;

  static RemoveImageStateRequest *create(MockImageCtx *image_ctx,
                                         uint64_t snap_id,
                                         Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  RemoveImageStateRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

RemoveImageStateRequest<MockImageCtx> *RemoveImageStateRequest<MockImageCtx>::s_instance;

} // namespace snapshot
} // namespace mirror
} // namespace librbd

// template definitions
#include "librbd/operation/SnapshotRemoveRequest.cc"

namespace librbd {
namespace operation {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockOperationSnapshotRemoveRequest : public TestMockFixture {
public:
  typedef SnapshotRemoveRequest<MockImageCtx> MockSnapshotRemoveRequest;
  typedef image::DetachChildRequest<MockImageCtx> MockDetachChildRequest;
  typedef mirror::snapshot::RemoveImageStateRequest<MockImageCtx> MockRemoveImageStateRequest;

  int create_snapshot(const char *snap_name) {
    librbd::ImageCtx *ictx;
    int r = open_image(m_image_name, &ictx);
    if (r < 0) {
      return r;
    }

    r = snap_create(*ictx, snap_name);
    if (r < 0) {
      return r;
    }

    r = snap_protect(*ictx, snap_name);
    if (r < 0) {
      return r;
    }
    close_image(ictx);
    return 0;
  }

  void expect_snapshot_trash_add(MockImageCtx &mock_image_ctx, int r) {
    if (mock_image_ctx.old_format) {
      return;
    }

    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                               StrEq("snapshot_trash_add"),
                               _, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_snapshot_get(MockImageCtx &mock_image_ctx,
                           const cls::rbd::SnapshotInfo& snap_info, int r) {
    if (mock_image_ctx.old_format) {
      return;
    }

    using ceph::encode;
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                     StrEq("snapshot_get"), _, _, _, _))
      .WillOnce(WithArg<5>(Invoke([snap_info, r](bufferlist* bl) {
                             encode(snap_info, *bl);
                             return r;
                           })));
  }

  void expect_children_list(MockImageCtx &mock_image_ctx,
                            const cls::rbd::ChildImageSpecs& child_images, int r) {
    if (mock_image_ctx.old_format) {
      return;
    }

    using ceph::encode;
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                     StrEq("children_list"), _, _, _, _))
      .WillOnce(WithArg<5>(Invoke([child_images, r](bufferlist* bl) {
                             encode(child_images, *bl);
                             return r;
                           })));
  }

  void expect_detach_stale_child(MockImageCtx &mock_image_ctx, int r) {
    auto& parent_spec = mock_image_ctx.parent_md.spec;

    bufferlist bl;
    encode(parent_spec.snap_id, bl);
    encode(cls::rbd::ChildImageSpec{mock_image_ctx.md_ctx.get_id(), "",
                                    mock_image_ctx.id}, bl);
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(util::header_name(parent_spec.image_id),
                     _, StrEq("rbd"), StrEq("child_detach"), ContentsEqual(bl),
                     _, _, _))
      .WillOnce(Return(r));
  }

  void expect_object_map_snap_remove(MockImageCtx &mock_image_ctx, int r) {
    if (mock_image_ctx.object_map != nullptr) {
      EXPECT_CALL(*mock_image_ctx.object_map, snapshot_remove(_, _))
                    .WillOnce(WithArg<1>(CompleteContext(
                      r, mock_image_ctx.image_ctx->op_work_queue)));
    }
  }

  void expect_remove_image_state(
      MockImageCtx &mock_image_ctx,
      MockRemoveImageStateRequest &mock_remove_image_state_request, int r) {
    EXPECT_CALL(mock_remove_image_state_request, send())
      .WillOnce(FinishRequest(&mock_remove_image_state_request, r,
                              &mock_image_ctx));
  }

  void expect_get_parent_spec(MockImageCtx &mock_image_ctx, int r) {
    if (mock_image_ctx.old_format) {
      return;
    }

    auto &expect = EXPECT_CALL(mock_image_ctx, get_parent_spec(_, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      auto &parent_spec = mock_image_ctx.snap_info.rbegin()->second.parent.spec;
      expect.WillOnce(DoAll(SetArgPointee<1>(parent_spec),
                            Return(0)));
    }
  }

  void expect_detach_child(MockImageCtx &mock_image_ctx,
                           MockDetachChildRequest& mock_request, int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(FinishRequest(&mock_request, r, &mock_image_ctx));
  }

  void expect_snap_remove(MockImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                               StrEq(mock_image_ctx.old_format ? "snap_remove" :
                                                                  "snapshot_remove"),
                                _, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_rm_snap(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, rm_snap(_, _, _)).Times(1);
  }

  void expect_release_snap_id(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.data_ctx),
                                selfmanaged_snap_remove(_))
                                  .WillOnce(DoDefault());
  }

};

TEST_F(TestMockOperationSnapshotRemoveRequest, Success) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_snapshot_trash_add(mock_image_ctx, 0);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_snapshot_get(mock_image_ctx,
                      {snap_id, {cls::rbd::UserSnapshotNamespace{}},
                       "snap1", 123, {}, 0}, 0);

  expect_get_parent_spec(mock_image_ctx, 0);
  expect_object_map_snap_remove(mock_image_ctx, 0);
  expect_release_snap_id(mock_image_ctx);
  expect_snap_remove(mock_image_ctx, 0);
  expect_rm_snap(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(), "snap1",
    snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, SuccessCloneParent) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_snapshot_trash_add(mock_image_ctx, 0);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_snapshot_get(mock_image_ctx,
                      {snap_id, {cls::rbd::UserSnapshotNamespace{}},
                       "snap1", 123, {}, 1}, 0);

  const cls::rbd::ChildImageSpecs child_images;
  expect_children_list(mock_image_ctx, child_images, 0);
  expect_get_parent_spec(mock_image_ctx, 0);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(), "snap1",
    snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, SuccessTrash) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_snapshot_trash_add(mock_image_ctx, 0);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_snapshot_get(mock_image_ctx,
                      {snap_id,
                       {cls::rbd::TrashSnapshotNamespace{
                          cls::rbd::SNAPSHOT_NAMESPACE_TYPE_USER, "snap1"}},
                       "snap1", 123, {}, 0}, 0);

  expect_get_parent_spec(mock_image_ctx, 0);
  expect_object_map_snap_remove(mock_image_ctx, 0);
  expect_release_snap_id(mock_image_ctx);
  expect_snap_remove(mock_image_ctx, 0);
  expect_rm_snap(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(), "snap1",
    snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, FlattenedCloneRemovesChild) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);
  REQUIRE(!is_feature_enabled(RBD_FEATURE_DEEP_FLATTEN))

  ASSERT_EQ(0, create_snapshot("snap1"));

  int order = 22;
  uint64_t features;
  ASSERT_TRUE(::get_features(&features));
  std::string clone_name = get_temp_image_name();
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
                             clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(clone_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));

  librbd::NoOpProgressContext prog_ctx;
  ASSERT_EQ(0, flatten(*ictx, prog_ctx));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_snapshot_trash_add(mock_image_ctx, 0);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_snapshot_get(mock_image_ctx,
                      {snap_id, {cls::rbd::UserSnapshotNamespace{}},
                       "snap1", 123, {}, 0}, 0);

  expect_get_parent_spec(mock_image_ctx, 0);

  MockDetachChildRequest mock_detach_child_request;
  expect_detach_child(mock_image_ctx, mock_detach_child_request, -ENOENT);

  expect_object_map_snap_remove(mock_image_ctx, 0);

  expect_release_snap_id(mock_image_ctx);
  expect_snap_remove(mock_image_ctx, 0);
  expect_rm_snap(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(), "snap1",
    snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, TrashCloneParent) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  NoOpProgressContext prog_ctx;
  ASSERT_EQ(0, ictx->operations->snap_create(
                 {cls::rbd::TrashSnapshotNamespace{}}, "snap1", 0, prog_ctx));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_snapshot_get(mock_image_ctx,
                      {snap_id, {cls::rbd::TrashSnapshotNamespace{}},
                       "snap1", 123, {}, 1}, 0);
  const cls::rbd::ChildImageSpecs child_images;
  expect_children_list(mock_image_ctx, child_images, 0);
  expect_get_parent_spec(mock_image_ctx, 0);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::TrashSnapshotNamespace{}, "snap1",
    snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EBUSY, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, MirrorSnapshot) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_snapshot_trash_add(mock_image_ctx, 0);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_snapshot_get(mock_image_ctx,
                      {snap_id, {cls::rbd::MirrorSnapshotNamespace{}},
                       "mirror", 123, {}, 0}, 0);

  expect_get_parent_spec(mock_image_ctx, 0);
  expect_object_map_snap_remove(mock_image_ctx, 0);
  MockRemoveImageStateRequest mock_remove_image_state_request;
  expect_remove_image_state(mock_image_ctx, mock_remove_image_state_request, 0);
  expect_release_snap_id(mock_image_ctx);
  expect_snap_remove(mock_image_ctx, 0);
  expect_rm_snap(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::MirrorSnapshotNamespace(),
    "mirror", snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, SnapshotTrashAddNotSupported) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_snapshot_trash_add(mock_image_ctx, -EOPNOTSUPP);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_get_parent_spec(mock_image_ctx, 0);
  expect_object_map_snap_remove(mock_image_ctx, 0);
  expect_release_snap_id(mock_image_ctx);
  expect_snap_remove(mock_image_ctx, 0);
  expect_rm_snap(mock_image_ctx);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(), "snap1",
    snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(0, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, SnapshotTrashAddError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_snapshot_trash_add(mock_image_ctx, -EINVAL);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(), "snap1",
    snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, SnapshotGetError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_snapshot_trash_add(mock_image_ctx, 0);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_snapshot_get(mock_image_ctx,
                      {snap_id, {cls::rbd::UserSnapshotNamespace{}},
                       "snap1", 123, {}, 0}, -EOPNOTSUPP);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(), "snap1",
    snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EOPNOTSUPP, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, ObjectMapSnapRemoveError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);
  MockObjectMap mock_object_map;
  mock_image_ctx.object_map = &mock_object_map;

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_snapshot_trash_add(mock_image_ctx, 0);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_snapshot_get(mock_image_ctx,
                      {snap_id, {cls::rbd::UserSnapshotNamespace{}},
                       "snap1", 123, {}, 0}, 0);

  expect_get_parent_spec(mock_image_ctx, 0);

  expect_object_map_snap_remove(mock_image_ctx, -EINVAL);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(), "snap1",
    snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, RemoveChildParentError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_snapshot_trash_add(mock_image_ctx, 0);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_snapshot_get(mock_image_ctx,
                      {snap_id, {cls::rbd::UserSnapshotNamespace{}},
                       "snap1", 123, {}, 0}, 0);

  expect_get_parent_spec(mock_image_ctx, -ENOENT);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(), "snap1",
    snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-ENOENT, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, RemoveChildError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snapshot("snap1"));

  int order = 22;
  uint64_t features;
  ASSERT_TRUE(::get_features(&features));
  std::string clone_name = get_temp_image_name();
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
                             clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(clone_name, &ictx));
  if (ictx->test_features(RBD_FEATURE_DEEP_FLATTEN)) {
    std::cout << "SKIPPING" << std::endl;
    return SUCCEED();
  }

  ASSERT_EQ(0, snap_create(*ictx, "snap1"));

  librbd::NoOpProgressContext prog_ctx;
  ASSERT_EQ(0, flatten(*ictx, prog_ctx));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_op_work_queue(mock_image_ctx);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_get_parent_spec(mock_image_ctx, 0);

  MockDetachChildRequest mock_detach_child_request;
  expect_detach_child(mock_image_ctx, mock_detach_child_request, -EINVAL);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(), "snap1",
    snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, RemoveSnapError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_snapshot_trash_add(mock_image_ctx, 0);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_snapshot_get(mock_image_ctx,
                      {snap_id, {cls::rbd::UserSnapshotNamespace{}},
                       "snap1", 123, {}, 0}, 0);

  expect_get_parent_spec(mock_image_ctx, 0);
  expect_object_map_snap_remove(mock_image_ctx, 0);
  expect_release_snap_id(mock_image_ctx);
  expect_snap_remove(mock_image_ctx, -ENOENT);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(), "snap1",
    snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-ENOENT, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, MissingSnap) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  uint64_t snap_id = 456;

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(), "snap1",
    snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-ENOENT, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, ListChildrenError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_snapshot_trash_add(mock_image_ctx, 0);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_snapshot_get(mock_image_ctx,
                      {snap_id, {cls::rbd::UserSnapshotNamespace{}},
                       "snap1", 123, {}, 1}, 0);
  const cls::rbd::ChildImageSpecs child_images;
  expect_children_list(mock_image_ctx, child_images, -EINVAL);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(), "snap1",
    snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

TEST_F(TestMockOperationSnapshotRemoveRequest, DetachStaleChildError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  ASSERT_EQ(0, create_snapshot("snap1"));

  int order = 22;
  uint64_t features;
  ASSERT_TRUE(::get_features(&features));
  std::string clone_name = get_temp_image_name();
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap1", m_ioctx,
                             clone_name.c_str(), features, &order, 0, 0));

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(clone_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap1"));
  ASSERT_EQ(0, ictx->state->refresh_if_required());

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  ::testing::InSequence seq;
  expect_snapshot_trash_add(mock_image_ctx, 0);

  uint64_t snap_id = ictx->snap_info.rbegin()->first;
  expect_snapshot_get(mock_image_ctx,
                      {snap_id, {cls::rbd::UserSnapshotNamespace{}},
                       "snap1", 123, {}, 1}, 0);
  const cls::rbd::ChildImageSpecs child_images;
  expect_children_list(mock_image_ctx, child_images, -EINVAL);

  C_SaferCond cond_ctx;
  MockSnapshotRemoveRequest *req = new MockSnapshotRemoveRequest(
    mock_image_ctx, &cond_ctx, cls::rbd::UserSnapshotNamespace(), "snap1",
    snap_id);
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    req->send();
  }
  ASSERT_EQ(-EINVAL, cond_ctx.wait());
}

} // namespace operation
} // namespace librbd
