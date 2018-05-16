// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockContextWQ.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/image/DetachChildRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

// template definitions
#include "librbd/image/DetachChildRequest.cc"

namespace librbd {
namespace image {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;

class TestMockImageDetachChildRequest : public TestMockFixture {
public:
  typedef DetachChildRequest<MockTestImageCtx> MockDetachChildRequest;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &image_ctx));
  }

  void expect_test_op_features(MockTestImageCtx& mock_image_ctx, bool enabled) {
    EXPECT_CALL(mock_image_ctx,
                test_op_features(RBD_OPERATION_FEATURE_CLONE_CHILD))
      .WillOnce(Return(enabled));
  }

  void expect_child_detach(MockImageCtx &mock_image_ctx, int r) {
    auto& parent_spec = mock_image_ctx.parent_md.spec;

    bufferlist bl;
    encode(parent_spec.snap_id, bl);
    encode(cls::rbd::ChildImageSpec{mock_image_ctx.md_ctx.get_id(),
                                    mock_image_ctx.id}, bl);

    auto& io_ctx_impl = get_mock_io_ctx(mock_image_ctx.md_ctx);
    auto rados_client = io_ctx_impl.get_mock_rados_client();

    EXPECT_CALL(*rados_client, create_ioctx(_, _))
      .WillOnce(DoAll(GetReference(&io_ctx_impl),
                      Return(&io_ctx_impl)));

    EXPECT_CALL(io_ctx_impl,
                exec(util::header_name(parent_spec.image_id),
                     _, StrEq("rbd"), StrEq("child_detach"), ContentsEqual(bl),
                     _, _))
      .WillOnce(Return(r));
  }

  void expect_remove_child(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(RBD_CHILDREN, _, StrEq("rbd"), StrEq("remove_child"), _,
                     _, _))
      .WillOnce(Return(r));
  }

  librbd::ImageCtx *image_ctx;
};

TEST_F(TestMockImageDetachChildRequest, SuccessV1) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  mock_image_ctx.parent_md.spec = {m_ioctx.get_id(), "parent id", 234};

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
  mock_image_ctx.parent_md.spec = {m_ioctx.get_id(), "parent id", 234};

  InSequence seq;
  expect_test_op_features(mock_image_ctx, true);
  expect_child_detach(mock_image_ctx, 0);

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
  mock_image_ctx.parent_md.spec = {m_ioctx.get_id(), "parent id", 234};

  InSequence seq;
  expect_test_op_features(mock_image_ctx, true);
  expect_child_detach(mock_image_ctx, -EPERM);

  C_SaferCond ctx;
  auto req = MockDetachChildRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockImageDetachChildRequest, RemoveChildError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);
  mock_image_ctx.parent_md.spec = {m_ioctx.get_id(), "parent id", 234};

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
