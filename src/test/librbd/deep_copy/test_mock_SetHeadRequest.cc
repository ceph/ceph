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
#include "librbd/image/AttachParentRequest.h"
#include "librbd/image/DetachParentRequest.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace image {

template <>
struct AttachParentRequest<MockTestImageCtx> {
  Context* on_finish = nullptr;
  static AttachParentRequest* s_instance;
  static AttachParentRequest* create(MockTestImageCtx&,
                                     const cls::rbd::ParentImageSpec& pspec,
                                     uint64_t parent_overlap, bool reattach,
                                     Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  AttachParentRequest() {
    s_instance = this;
  }
};

AttachParentRequest<MockTestImageCtx>* AttachParentRequest<MockTestImageCtx>::s_instance = nullptr;

template <>
class DetachParentRequest<MockTestImageCtx> {
public:
  static DetachParentRequest *s_instance;
  static DetachParentRequest *create(MockTestImageCtx &image_ctx,
                                     Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context *on_finish = nullptr;

  DetachParentRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

DetachParentRequest<MockTestImageCtx> *DetachParentRequest<MockTestImageCtx>::s_instance;

} // namespace image
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
  typedef image::AttachParentRequest<MockTestImageCtx> MockAttachParentRequest;
  typedef image::DetachParentRequest<MockTestImageCtx> MockDetachParentRequest;

  librbd::ImageCtx *m_image_ctx;
  asio::ContextWQ *m_work_queue;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_image_ctx));

    librbd::ImageCtx::get_work_queue(m_image_ctx->cct, &m_work_queue);
  }

  void expect_start_op(librbd::MockExclusiveLock &mock_exclusive_lock) {
    EXPECT_CALL(mock_exclusive_lock, start_op(_)).WillOnce(Return(new LambdaContext([](int){})));
  }

  void expect_test_features(librbd::MockTestImageCtx &mock_image_ctx,
                            uint64_t features, bool enabled) {
    EXPECT_CALL(mock_image_ctx, test_features(features))
                  .WillOnce(Return(enabled));
  }

  void expect_set_size(librbd::MockTestImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                     StrEq("set_size"), _, _, _, _))
                  .WillOnce(Return(r));
  }

  void expect_detach_parent(MockImageCtx &mock_image_ctx,
                            MockDetachParentRequest& mock_request, int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(FinishRequest(&mock_request, r, &mock_image_ctx));
  }

  void expect_attach_parent(MockImageCtx &mock_image_ctx,
                            MockAttachParentRequest& mock_request, int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(FinishRequest(&mock_request, r, &mock_image_ctx));
  }

  MockSetHeadRequest *create_request(
      librbd::MockTestImageCtx &mock_local_image_ctx, uint64_t size,
      const cls::rbd::ParentImageSpec &parent_spec, uint64_t parent_overlap,
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
  MockDetachParentRequest mock_detach_parent;
  expect_detach_parent(mock_image_ctx, mock_detach_parent, 0);

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
  MockDetachParentRequest mock_detach_parent;
  expect_detach_parent(mock_image_ctx, mock_detach_parent, -EINVAL);

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
  MockDetachParentRequest mock_detach_parent;
  expect_detach_parent(mock_image_ctx, mock_detach_parent, 0);
  expect_start_op(mock_exclusive_lock);
  MockAttachParentRequest mock_attach_parent;
  expect_attach_parent(mock_image_ctx, mock_attach_parent, 0);

  C_SaferCond ctx;
  auto request = create_request(mock_image_ctx, m_image_ctx->size,
                                {123, "", "test", 0}, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopySetHeadRequest, SetParentSpec) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_start_op(mock_exclusive_lock);
  MockAttachParentRequest mock_attach_parent;
  expect_attach_parent(mock_image_ctx, mock_attach_parent, 0);

  C_SaferCond ctx;
  auto request = create_request(mock_image_ctx, m_image_ctx->size,
                                {123, "", "test", 0}, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopySetHeadRequest, SetParentOverlap) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  mock_image_ctx.parent_md.spec = {123, "", "test", 0};
  mock_image_ctx.parent_md.overlap = m_image_ctx->size;

  InSequence seq;
  expect_start_op(mock_exclusive_lock);
  expect_set_size(mock_image_ctx, 0);

  C_SaferCond ctx;
  auto request = create_request(mock_image_ctx, 123,
                                mock_image_ctx.parent_md.spec, 123, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(123U, mock_image_ctx.parent_md.overlap);
}

TEST_F(TestMockDeepCopySetHeadRequest, SetParentError) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  InSequence seq;
  expect_start_op(mock_exclusive_lock);
  MockAttachParentRequest mock_attach_parent;
  expect_attach_parent(mock_image_ctx, mock_attach_parent, -ESTALE);

  C_SaferCond ctx;
  auto request = create_request(mock_image_ctx, m_image_ctx->size,
                                {123, "", "test", 0}, 0, &ctx);
  request->send();
  ASSERT_EQ(-ESTALE, ctx.wait());
}

} // namespace deep_copy
} // namespace librbd
