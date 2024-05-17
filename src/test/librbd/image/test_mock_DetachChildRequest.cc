// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockContextWQ.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/image/TypeTraits.h"
#include "librbd/image/DetachChildRequest.h"
#include "librbd/trash/RemoveRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  static MockTestImageCtx* s_instance;
  static MockTestImageCtx* create(const std::string &image_name,
                                  const std::string &image_id,
                                  const char *snap, librados::IoCtx& p,
                                  bool read_only) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
    s_instance = this;
  }
};

MockTestImageCtx* MockTestImageCtx::s_instance = nullptr;

} // anonymous namespace

namespace image {

template <>
struct TypeTraits<MockTestImageCtx> {
  typedef librbd::MockContextWQ ContextWQ;
};

} // namespace image

namespace trash {

template <>
class RemoveRequest<MockTestImageCtx> {
private:
  typedef ::librbd::image::TypeTraits<MockTestImageCtx> TypeTraits;
  typedef typename TypeTraits::ContextWQ ContextWQ;
public:
  static RemoveRequest *s_instance;
  static RemoveRequest *create(librados::IoCtx &ioctx,
                               MockTestImageCtx *image_ctx,
                               ContextWQ *op_work_queue, bool force,
                               ProgressContext &prog_ctx, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context *on_finish = nullptr;

  RemoveRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

RemoveRequest<MockTestImageCtx> *RemoveRequest<MockTestImageCtx>::s_instance;

} // namespace trash
} // namespace librbd

// template definitions
#include "librbd/image/DetachChildRequest.cc"

namespace librbd {
namespace image {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockImageDetachChildRequest : public TestMockFixture {
public:
  typedef DetachChildRequest<MockTestImageCtx> MockDetachChildRequest;
  typedef trash::RemoveRequest<MockTestImageCtx> MockTrashRemoveRequest;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &image_ctx));
  }

  void expect_test_op_features(MockTestImageCtx& mock_image_ctx, bool enabled) {
    EXPECT_CALL(mock_image_ctx,
                test_op_features(RBD_OPERATION_FEATURE_CLONE_CHILD))
      .WillOnce(Return(enabled));
  }

  void expect_create_ioctx(MockImageCtx &mock_image_ctx,
                           librados::MockTestMemIoCtxImpl **io_ctx_impl) {
    *io_ctx_impl = &get_mock_io_ctx(mock_image_ctx.md_ctx);
    auto rados_client = (*io_ctx_impl)->get_mock_rados_client();

    EXPECT_CALL(*rados_client, create_ioctx(_, _))
      .WillOnce(DoAll(GetReference(*io_ctx_impl), Return(*io_ctx_impl)));
  }

  void expect_child_detach(MockImageCtx &mock_image_ctx,
                           librados::MockTestMemIoCtxImpl &mock_io_ctx_impl,
                           int r) {
    auto& parent_spec = mock_image_ctx.parent_md.spec;

    bufferlist bl;
    encode(parent_spec.snap_id, bl);
    encode(cls::rbd::ChildImageSpec{mock_image_ctx.md_ctx.get_id(), "",
                                    mock_image_ctx.id}, bl);

    EXPECT_CALL(mock_io_ctx_impl,
                exec(util::header_name(parent_spec.image_id),
                     _, StrEq("rbd"), StrEq("child_detach"), ContentsEqual(bl),
                     _, _, _))
      .WillOnce(Return(r));
  }

  void expect_remove_child(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(RBD_CHILDREN, _, StrEq("rbd"), StrEq("remove_child"), _,
                     _, _, _))
      .WillOnce(Return(r));
  }

  void expect_snapshot_get(MockImageCtx &mock_image_ctx,
                           librados::MockTestMemIoCtxImpl &mock_io_ctx_impl,
                           const std::string& parent_header_name,
                           const cls::rbd::SnapshotInfo& snap_info, int r) {

    using ceph::encode;
    EXPECT_CALL(mock_io_ctx_impl,
                exec(parent_header_name, _, StrEq("rbd"),
                     StrEq("snapshot_get"), _, _, _, _))
      .WillOnce(WithArg<5>(Invoke([snap_info, r](bufferlist* bl) {
                             encode(snap_info, *bl);
                             return r;
                           })));
  }

  void expect_open(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.state, open(true, _))
      .WillOnce(WithArg<1>(Invoke([this, &mock_image_ctx, r](Context* ctx) {
                             EXPECT_EQ(0U, mock_image_ctx.read_only_mask &
                                             IMAGE_READ_ONLY_FLAG_NON_PRIMARY);
                             image_ctx->op_work_queue->queue(ctx, r);
                           })));
    if (r == 0) {
      EXPECT_CALL(mock_image_ctx, test_features(_))
        .WillOnce(Return(false));
    }
  }

  void expect_close(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.state, close(_))
      .WillOnce(Invoke([this, r](Context* ctx) {
                  image_ctx->op_work_queue->queue(ctx, r);
                }));
  }

  void expect_snap_remove(MockImageCtx &mock_image_ctx,
                          const std::string &snap_name, int r) {
    EXPECT_CALL(*mock_image_ctx.operations,
                snap_remove({cls::rbd::TrashSnapshotNamespace{}},
                            StrEq(snap_name), _))
      .WillOnce(WithArg<2>(Invoke([this, r](Context *ctx) {
                             image_ctx->op_work_queue->queue(ctx, r);
                           })));
  }

  void expect_trash_get(MockImageCtx &mock_image_ctx,
                        librados::MockTestMemIoCtxImpl &mock_io_ctx_impl,
                        const cls::rbd::TrashImageSpec& trash_spec,
                        int r) {
    using ceph::encode;
    EXPECT_CALL(mock_io_ctx_impl,
                exec(RBD_TRASH, _, StrEq("rbd"),
                     StrEq("trash_get"), _, _, _, _))
      .WillOnce(WithArg<5>(Invoke([trash_spec, r](bufferlist* bl) {
                             encode(trash_spec, *bl);
                             return r;
                           })));
  }

  void expect_trash_remove(MockTrashRemoveRequest& mock_trash_remove_request,
                           int r) {
    EXPECT_CALL(mock_trash_remove_request, send())
      .WillOnce(Invoke([&mock_trash_remove_request, r]() {
          mock_trash_remove_request.on_finish->complete(r);
        }));
  }

  librbd::ImageCtx *image_ctx;
};

TEST_F(TestMockImageDetachChildRequest, SuccessV1) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  mock_image_ctx.parent_md.spec = {m_ioctx.get_id(), "", "parent id", 234};

  InSequence seq;
  expect_test_op_features(mock_image_ctx, false);
  expect_remove_child(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = MockDetachChildRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDetachChildRequest, SuccessV2) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  mock_image_ctx.parent_md.spec = {m_ioctx.get_id(), "", "parent id", 234};

  InSequence seq;
  expect_test_op_features(mock_image_ctx, true);

  librados::MockTestMemIoCtxImpl *mock_io_ctx_impl;
  expect_create_ioctx(mock_image_ctx, &mock_io_ctx_impl);
  expect_child_detach(mock_image_ctx, *mock_io_ctx_impl, 0);
  expect_snapshot_get(mock_image_ctx, *mock_io_ctx_impl,
                      "rbd_header.parent id",
                      {234, {cls::rbd::UserSnapshotNamespace{}},
                       "snap1", 123, {}, 0}, 0);

  C_SaferCond ctx;
  auto req = MockDetachChildRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDetachChildRequest, TrashedSnapshotSuccess) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  mock_image_ctx.parent_md.spec = {m_ioctx.get_id(), "", "parent id", 234};

  InSequence seq;
  expect_test_op_features(mock_image_ctx, true);

  librados::MockTestMemIoCtxImpl *mock_io_ctx_impl;
  expect_create_ioctx(mock_image_ctx, &mock_io_ctx_impl);
  expect_child_detach(mock_image_ctx, *mock_io_ctx_impl, 0);
  expect_snapshot_get(mock_image_ctx, *mock_io_ctx_impl,
                      "rbd_header.parent id",
                      {234, {cls::rbd::TrashSnapshotNamespace{}},
                       "snap1", 123, {}, 0}, 0);
  expect_open(mock_image_ctx, 0);
  expect_snap_remove(mock_image_ctx, "snap1", 0);
  const cls::rbd::TrashImageSpec trash_spec;
  expect_trash_get(mock_image_ctx, *mock_io_ctx_impl, trash_spec, -ENOENT);
  expect_close(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = MockDetachChildRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDetachChildRequest, ParentAutoRemove) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  mock_image_ctx.parent_md.spec = {m_ioctx.get_id(), "", "parent id", 234};

  InSequence seq;
  expect_test_op_features(mock_image_ctx, true);

  librados::MockTestMemIoCtxImpl *mock_io_ctx_impl;
  expect_create_ioctx(mock_image_ctx, &mock_io_ctx_impl);
  expect_child_detach(mock_image_ctx, *mock_io_ctx_impl, 0);
  expect_snapshot_get(mock_image_ctx, *mock_io_ctx_impl,
                      "rbd_header.parent id",
                      {234, {cls::rbd::TrashSnapshotNamespace{}},
                       "snap1", 123, {}, 0}, 0);
  expect_open(mock_image_ctx, 0);
  expect_snap_remove(mock_image_ctx, "snap1", 0);
  const cls::rbd::TrashImageSpec trash_spec =
      {cls::rbd::TRASH_IMAGE_SOURCE_USER_PARENT, "parent", {}, {}};

  expect_trash_get(mock_image_ctx, *mock_io_ctx_impl, trash_spec, 0);
  MockTrashRemoveRequest mock_trash_remove_request;
  expect_trash_remove(mock_trash_remove_request, 0);

  C_SaferCond ctx;
  auto req = MockDetachChildRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDetachChildRequest, TrashedSnapshotInUse) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  mock_image_ctx.parent_md.spec = {m_ioctx.get_id(), "", "parent id", 234};

  InSequence seq;
  expect_test_op_features(mock_image_ctx, true);

  librados::MockTestMemIoCtxImpl *mock_io_ctx_impl;
  expect_create_ioctx(mock_image_ctx, &mock_io_ctx_impl);
  expect_child_detach(mock_image_ctx, *mock_io_ctx_impl, 0);
  expect_snapshot_get(mock_image_ctx, *mock_io_ctx_impl,
                      "rbd_header.parent id",
                      {234, {cls::rbd::TrashSnapshotNamespace{}},
                       "snap1", 123, {}, 1}, 0);

  C_SaferCond ctx;
  auto req = MockDetachChildRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDetachChildRequest, TrashedSnapshotSnapshotGetError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  mock_image_ctx.parent_md.spec = {m_ioctx.get_id(), "", "parent id", 234};

  InSequence seq;
  expect_test_op_features(mock_image_ctx, true);

  librados::MockTestMemIoCtxImpl *mock_io_ctx_impl;
  expect_create_ioctx(mock_image_ctx, &mock_io_ctx_impl);
  expect_child_detach(mock_image_ctx, *mock_io_ctx_impl, 0);
  expect_snapshot_get(mock_image_ctx, *mock_io_ctx_impl,
                      "rbd_header.parent id",
                      {234, {cls::rbd::TrashSnapshotNamespace{}},
                       "snap1", 123, {}, 0}, -EINVAL);

  C_SaferCond ctx;
  auto req = MockDetachChildRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDetachChildRequest, TrashedSnapshotOpenParentError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  mock_image_ctx.parent_md.spec = {m_ioctx.get_id(), "", "parent id", 234};

  InSequence seq;
  expect_test_op_features(mock_image_ctx, true);

  librados::MockTestMemIoCtxImpl *mock_io_ctx_impl;
  expect_create_ioctx(mock_image_ctx, &mock_io_ctx_impl);
  expect_child_detach(mock_image_ctx, *mock_io_ctx_impl, 0);
  expect_snapshot_get(mock_image_ctx, *mock_io_ctx_impl,
                      "rbd_header.parent id",
                      {234, {cls::rbd::TrashSnapshotNamespace{}},
                       "snap1", 123, {}, 0}, 0);
  expect_open(mock_image_ctx, -EPERM);

  C_SaferCond ctx;
  auto req = MockDetachChildRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDetachChildRequest, TrashedSnapshotRemoveError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  mock_image_ctx.parent_md.spec = {m_ioctx.get_id(), "", "parent id", 234};

  InSequence seq;
  expect_test_op_features(mock_image_ctx, true);

  librados::MockTestMemIoCtxImpl *mock_io_ctx_impl;
  expect_create_ioctx(mock_image_ctx, &mock_io_ctx_impl);
  expect_child_detach(mock_image_ctx, *mock_io_ctx_impl, 0);
  expect_snapshot_get(mock_image_ctx, *mock_io_ctx_impl,
                      "rbd_header.parent id",
                      {234, {cls::rbd::TrashSnapshotNamespace{}},
                       "snap1", 123, {}, 0}, 0);
  expect_open(mock_image_ctx, 0);
  expect_snap_remove(mock_image_ctx, "snap1", -EPERM);
  expect_close(mock_image_ctx, -EPERM);

  C_SaferCond ctx;
  auto req = MockDetachChildRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDetachChildRequest, ParentDNE) {
  MockTestImageCtx mock_image_ctx(*image_ctx);
  expect_op_work_queue(mock_image_ctx);

  C_SaferCond ctx;
  auto req = MockDetachChildRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDetachChildRequest, ChildDetachError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  mock_image_ctx.parent_md.spec = {m_ioctx.get_id(), "", "parent id", 234};

  InSequence seq;
  expect_test_op_features(mock_image_ctx, true);

  librados::MockTestMemIoCtxImpl *mock_io_ctx_impl;
  expect_create_ioctx(mock_image_ctx, &mock_io_ctx_impl);
  expect_child_detach(mock_image_ctx, *mock_io_ctx_impl, -EPERM);

  C_SaferCond ctx;
  auto req = MockDetachChildRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockImageDetachChildRequest, RemoveChildError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  mock_image_ctx.parent_md.spec = {m_ioctx.get_id(), "", "parent id", 234};

  InSequence seq;
  expect_test_op_features(mock_image_ctx, false);
  expect_remove_child(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = MockDetachChildRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace image
} // namespace librbd
