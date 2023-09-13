// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockContextWQ.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/image/AttachChildRequest.h"
#include "librbd/image/RefreshRequest.h"
#include "librbd/internal.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace image {

template <>
struct RefreshRequest<MockTestImageCtx> {
  Context* on_finish = nullptr;
  static RefreshRequest* s_instance;
  static RefreshRequest* create(MockTestImageCtx &image_ctx,
                                bool acquiring_lock, bool skip_open_parent,
                                Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  RefreshRequest() {
    s_instance = this;
  }
};

RefreshRequest<MockTestImageCtx>* RefreshRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace image

} // namespace librbd

// template definitions
#include "librbd/image/AttachChildRequest.cc"

namespace librbd {
namespace image {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockImageAttachChildRequest : public TestMockFixture {
public:
  typedef AttachChildRequest<MockTestImageCtx> MockAttachChildRequest;
  typedef RefreshRequest<MockTestImageCtx> MockRefreshRequest;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &image_ctx));
    NoOpProgressContext prog_ctx;
    ASSERT_EQ(0, image_ctx->operations->snap_create(
                   cls::rbd::UserSnapshotNamespace{}, "snap", 0, prog_ctx));
    if (is_feature_enabled(RBD_FEATURE_LAYERING)) {
      ASSERT_EQ(0, image_ctx->operations->snap_protect(
                     cls::rbd::UserSnapshotNamespace{}, "snap"));

      uint64_t snap_id = image_ctx->snap_ids[
        {cls::rbd::UserSnapshotNamespace{}, "snap"}];
      ASSERT_NE(CEPH_NOSNAP, snap_id);

      C_SaferCond ctx;
      image_ctx->state->snap_set(snap_id, &ctx);
      ASSERT_EQ(0, ctx.wait());
    }
  }

  void expect_add_child(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(RBD_CHILDREN, _, StrEq("rbd"), StrEq("add_child"), _, _, _,
                     _))
      .WillOnce(Return(r));
  }

  void expect_refresh(MockRefreshRequest& mock_refresh_request, int r) {
    EXPECT_CALL(mock_refresh_request, send())
      .WillOnce(Invoke([this, &mock_refresh_request, r]() {
                  image_ctx->op_work_queue->queue(mock_refresh_request.on_finish, r);
                }));
  }

  void expect_is_snap_protected(MockImageCtx &mock_image_ctx, bool is_protected,
                                int r) {
    EXPECT_CALL(mock_image_ctx, is_snap_protected(_, _))
      .WillOnce(WithArg<1>(Invoke([is_protected, r](bool* is_prot) {
                             *is_prot = is_protected;
                             return r;
                           })));
  }

  void expect_op_features_set(MockImageCtx &mock_image_ctx, int r) {
    bufferlist bl;
    encode(static_cast<uint64_t>(RBD_OPERATION_FEATURE_CLONE_CHILD), bl);
    encode(static_cast<uint64_t>(RBD_OPERATION_FEATURE_CLONE_CHILD), bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(util::header_name(mock_image_ctx.id), _, StrEq("rbd"),
                     StrEq("op_features_set"), ContentsEqual(bl), _, _, _))
      .WillOnce(Return(r));
  }

  void expect_child_attach(MockImageCtx &mock_image_ctx, int r) {
    bufferlist bl;
    encode(mock_image_ctx.snap_id, bl);
    encode(cls::rbd::ChildImageSpec{m_ioctx.get_id(), "", mock_image_ctx.id},
           bl);

    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                     StrEq("child_attach"), ContentsEqual(bl), _, _, _))
      .WillOnce(Return(r));
  }

  librbd::ImageCtx *image_ctx;
};

TEST_F(TestMockImageAttachChildRequest, SuccessV1) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;

  expect_add_child(mock_image_ctx, 0);

  MockRefreshRequest mock_refresh_request;
  expect_refresh(mock_refresh_request, 0);
  expect_is_snap_protected(mock_image_ctx, true, 0);

  C_SaferCond ctx;
  auto req = MockAttachChildRequest::create(&mock_image_ctx, &mock_image_ctx,
                                            image_ctx->snap_id, nullptr, 0, 1,
                                            &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageAttachChildRequest, SuccessV2) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;

  expect_op_features_set(mock_image_ctx, 0);
  expect_child_attach(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = MockAttachChildRequest::create(&mock_image_ctx, &mock_image_ctx,
                                            image_ctx->snap_id, nullptr, 0, 2,
                                            &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageAttachChildRequest, AddChildError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;

  expect_add_child(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = MockAttachChildRequest::create(&mock_image_ctx, &mock_image_ctx,
                                            image_ctx->snap_id, nullptr, 0, 1,
                                            &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageAttachChildRequest, RefreshError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;

  expect_add_child(mock_image_ctx, 0);

  MockRefreshRequest mock_refresh_request;
  expect_refresh(mock_refresh_request, -EINVAL);

  C_SaferCond ctx;
  auto req = MockAttachChildRequest::create(&mock_image_ctx, &mock_image_ctx,
                                            image_ctx->snap_id, nullptr, 0, 1,
                                            &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageAttachChildRequest, ValidateProtectedFailed) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;

  expect_add_child(mock_image_ctx, 0);

  MockRefreshRequest mock_refresh_request;
  expect_refresh(mock_refresh_request, 0);
  expect_is_snap_protected(mock_image_ctx, false, 0);

  C_SaferCond ctx;
  auto req = MockAttachChildRequest::create(&mock_image_ctx, &mock_image_ctx,
                                            image_ctx->snap_id, nullptr, 0, 1,
                                            &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageAttachChildRequest, SetCloneError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;

  expect_op_features_set(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = MockAttachChildRequest::create(&mock_image_ctx, &mock_image_ctx,
                                            image_ctx->snap_id, nullptr, 0, 2,
                                            &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageAttachChildRequest, AttachChildError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;

  expect_op_features_set(mock_image_ctx, 0);
  expect_child_attach(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = MockAttachChildRequest::create(&mock_image_ctx, &mock_image_ctx,
                                            image_ctx->snap_id, nullptr, 0, 2,
                                            &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace image
} // namespace librbd
