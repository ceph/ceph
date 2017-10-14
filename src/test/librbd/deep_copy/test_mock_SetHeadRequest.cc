// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librados_test_stub/LibradosTestStub.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "osdc/Striper.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "librbd/deep_copy/SetHeadRequest.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

// template definitions
#include "librbd/deep_copy/SetHeadRequest.cc"
template class librbd::deep_copy::SetHeadRequest<librbd::MockTestImageCtx>;

namespace librbd {
namespace deep_copy {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::InvokeWithoutArgs;
using ::testing::Return;
using ::testing::ReturnNew;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockDeepCopySetHeadRequest : public TestMockFixture {
public:
  typedef SetHeadRequest<librbd::MockTestImageCtx> MockSetHeadRequest;

  librbd::ImageCtx *m_image_ctx;
  ThreadPool *m_thread_pool;
  ContextWQ *m_work_queue;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_image_ctx));

    librbd::ImageCtx::get_thread_pool_instance(m_image_ctx->cct, &m_thread_pool,
                                               &m_work_queue);
  }

  void expect_start_op(librbd::MockExclusiveLock &mock_exclusive_lock) {
    EXPECT_CALL(mock_exclusive_lock, start_op()).WillOnce(
      ReturnNew<FunctionContext>([](int) {}));
  }

  void expect_test_features(librbd::MockTestImageCtx &mock_image_ctx,
                            uint64_t features, bool enabled) {
    EXPECT_CALL(mock_image_ctx, test_features(features))
                  .WillOnce(Return(enabled));
  }

  void expect_set_size(librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"), StrEq("set_size"), _, _, _))
                  .WillOnce(Return(r));
  }

  void expect_remove_parent(librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"), StrEq("remove_parent"), _, _, _))
                  .WillOnce(Return(r));
  }

  void expect_set_parent(librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"), StrEq("set_parent"), _, _, _))
                  .WillOnce(Return(r));
  }

  MockSetHeadRequest *create_request(
      librbd::MockTestImageCtx &mock_local_image_ctx, uint64_t size,
      const librbd::ParentSpec &parent_spec, uint64_t parent_overlap,
      Context *on_finish) {
    return new MockSetHeadRequest(&mock_local_image_ctx, size, parent_spec,
                                  parent_overlap, on_finish);
  }
};

TEST_F(TestMockDeepCopySetHeadRequest, Resize) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_start_op(mock_exclusive_lock);
  expect_set_size(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto request = create_request(mock_image_ctx, 123, {}, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopySetHeadRequest, ResizeError) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_start_op(mock_exclusive_lock);
  expect_set_size(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto request = create_request(mock_image_ctx, 123, {}, 0, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopySetHeadRequest, RemoveParent) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  mock_image_ctx.parent_md.spec.pool_id = 213;

  InSequence seq;
  expect_start_op(mock_exclusive_lock);
  expect_remove_parent(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto request = create_request(mock_image_ctx, m_image_ctx->size, {}, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopySetHeadRequest, RemoveParentError) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  mock_image_ctx.parent_md.spec.pool_id = 213;

  InSequence seq;
  expect_start_op(mock_exclusive_lock);
  expect_remove_parent(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  auto request = create_request(mock_image_ctx, m_image_ctx->size, {}, 0, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopySetHeadRequest, RemoveSetParent) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  mock_image_ctx.parent_md.spec.pool_id = 213;

  InSequence seq;
  expect_start_op(mock_exclusive_lock);
  expect_remove_parent(mock_image_ctx, 0);
  expect_start_op(mock_exclusive_lock);
  expect_set_parent(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto request = create_request(mock_image_ctx, m_image_ctx->size,
                                {123, "test", 0}, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopySetHeadRequest, SetParentSpec) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_start_op(mock_exclusive_lock);
  expect_set_parent(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto request = create_request(mock_image_ctx, m_image_ctx->size,
                                {123, "test", 0}, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopySetHeadRequest, SetParentOverlap) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  mock_image_ctx.parent_md.spec = {123, "test", 0};

  InSequence seq;
  expect_start_op(mock_exclusive_lock);
  expect_set_parent(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto request = create_request(mock_image_ctx, m_image_ctx->size,
                                mock_image_ctx.parent_md.spec, 123, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopySetHeadRequest, SetParentError) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_start_op(mock_exclusive_lock);
  expect_set_parent(mock_image_ctx, -ESTALE);

  C_SaferCond ctx;
  auto request = create_request(mock_image_ctx, m_image_ctx->size,
                                {123, "test", 0}, 0, &ctx);
  request->send();
  ASSERT_EQ(-ESTALE, ctx.wait());
}

} // namespace deep_copy
} // namespace librbd
