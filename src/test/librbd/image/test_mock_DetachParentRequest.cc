// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockContextWQ.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "librbd/image/DetachParentRequest.h"
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
#include "librbd/image/DetachParentRequest.cc"

namespace librbd {
namespace image {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;

class TestMockImageDetachParentRequest : public TestMockFixture {
public:
  typedef DetachParentRequest<MockTestImageCtx> MockDetachParentRequest;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &image_ctx));
  }

  void expect_parent_detach(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                     StrEq("parent_detach"), _, _, _, _))
      .WillOnce(Return(r));
  }

  void expect_remove_parent(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                     StrEq("remove_parent"), _, _, _, _))
      .WillOnce(Return(r));
  }

  librbd::ImageCtx *image_ctx;
};

TEST_F(TestMockImageDetachParentRequest, ParentDetachSuccess) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;
  expect_parent_detach(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = MockDetachParentRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDetachParentRequest, RemoveParentSuccess) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;
  expect_parent_detach(mock_image_ctx, -EOPNOTSUPP);
  expect_remove_parent(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto req = MockDetachParentRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDetachParentRequest, ParentDNE) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;
  expect_parent_detach(mock_image_ctx, -ENOENT);

  C_SaferCond ctx;
  auto req = MockDetachParentRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageDetachParentRequest, ParentDetachError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;
  expect_parent_detach(mock_image_ctx, -EPERM);

  C_SaferCond ctx;
  auto req = MockDetachParentRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockImageDetachParentRequest, RemoveParentError) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  MockTestImageCtx mock_image_ctx(*image_ctx);

  InSequence seq;
  expect_parent_detach(mock_image_ctx, -EOPNOTSUPP);
  expect_remove_parent(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto req = MockDetachParentRequest::create(mock_image_ctx, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace image
} // namespace librbd
