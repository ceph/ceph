// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockContextWQ.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "librbd/image/AttachParentRequest.h"
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
#include "librbd/image/AttachParentRequest.cc"

namespace librbd {
namespace image {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;

class TestMockImageAttachParentRequest : public TestMockFixture {
public:
  typedef AttachParentRequest<MockTestImageCtx> MockAttachParentRequest;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &image_ctx));
  }

  void expect_parent_attach(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                     StrEq("parent_attach"), _, _, _))
      .WillOnce(Return(r));
  }

  void expect_set_parent(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                     StrEq("set_parent"), _, _, _))
      .WillOnce(Return(r));
  }

  librbd::ImageCtx *image_ctx;
};

TEST_F(TestMockImageAttachParentRequest, ParentAttachSuccess) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;
  expect_parent_attach(mock_image_ctx, 0);

  cls::rbd::ParentImageSpec parent_image_spec{
    1, "ns", "image id", 123};

  C_SaferCond ctx;
  auto req = MockAttachParentRequest::create(mock_image_ctx, parent_image_spec,
                                             234, false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageAttachParentRequest, SetParentSuccess) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;
  expect_parent_attach(mock_image_ctx, -EOPNOTSUPP);
  expect_set_parent(mock_image_ctx, 0);

  cls::rbd::ParentImageSpec parent_image_spec{
    1, "", "image id", 123};

  C_SaferCond ctx;
  auto req = MockAttachParentRequest::create(mock_image_ctx, parent_image_spec,
                                             234, false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageAttachParentRequest, ParentAttachError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;
  expect_parent_attach(mock_image_ctx, -EPERM);

  cls::rbd::ParentImageSpec parent_image_spec{
    1, "", "image id", 123};

  C_SaferCond ctx;
  auto req = MockAttachParentRequest::create(mock_image_ctx, parent_image_spec,
                                             234, false, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockImageAttachParentRequest, SetParentError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;
  expect_parent_attach(mock_image_ctx, -EOPNOTSUPP);
  expect_set_parent(mock_image_ctx, -EINVAL);

  cls::rbd::ParentImageSpec parent_image_spec{
    1, "", "image id", 123};

  C_SaferCond ctx;
  auto req = MockAttachParentRequest::create(mock_image_ctx, parent_image_spec,
                                             234, false, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageAttachParentRequest, NamespaceUnsupported) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;
  expect_parent_attach(mock_image_ctx, -EOPNOTSUPP);

  cls::rbd::ParentImageSpec parent_image_spec{
    1, "ns", "image id", 123};

  C_SaferCond ctx;
  auto req = MockAttachParentRequest::create(mock_image_ctx, parent_image_spec,
                                             234, false, &ctx);
  req->send();
  ASSERT_EQ(-EXDEV, ctx.wait());
}

} // namespace image
} // namespace librbd
