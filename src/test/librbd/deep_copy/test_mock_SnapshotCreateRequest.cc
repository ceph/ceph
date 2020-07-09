// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librados_test_stub/LibradosTestStub.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "osdc/Striper.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "librbd/deep_copy/SetHeadRequest.h"
#include "librbd/deep_copy/SnapshotCreateRequest.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace deep_copy {

template <>
class SetHeadRequest<librbd::MockTestImageCtx> {
public:
  static SetHeadRequest* s_instance;
  Context *on_finish;

  static SetHeadRequest* create(librbd::MockTestImageCtx *image_ctx,
                                uint64_t size,
                                const cls::rbd::ParentImageSpec &parent_spec,
                                uint64_t parent_overlap, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  SetHeadRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

SetHeadRequest<librbd::MockTestImageCtx>* SetHeadRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace deep_copy
} // namespace librbd

// template definitions
#include "librbd/deep_copy/SnapshotCreateRequest.cc"
template class librbd::deep_copy::SnapshotCreateRequest<librbd::MockTestImageCtx>;

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

class TestMockDeepCopySnapshotCreateRequest : public TestMockFixture {
public:
  typedef SetHeadRequest<librbd::MockTestImageCtx> MockSetHeadRequest;
  typedef SnapshotCreateRequest<librbd::MockTestImageCtx> MockSnapshotCreateRequest;

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

  void expect_set_head(MockSetHeadRequest &mock_set_head_request, int r) {
    EXPECT_CALL(mock_set_head_request, send())
      .WillOnce(Invoke([&mock_set_head_request, r]() {
            mock_set_head_request.on_finish->complete(r);
          }));
  }

  void expect_snap_create(librbd::MockTestImageCtx &mock_image_ctx,
                          const std::string &snap_name, uint64_t snap_id, int r) {
    uint64_t flags = SNAP_CREATE_FLAG_SKIP_OBJECT_MAP |
                     SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE;
    EXPECT_CALL(*mock_image_ctx.operations,
                execute_snap_create(_, StrEq(snap_name), _, 0, flags, _))
                  .WillOnce(DoAll(InvokeWithoutArgs([&mock_image_ctx, snap_id, snap_name]() {
                                    inject_snap(mock_image_ctx, snap_id, snap_name);
                                  }),
                                  WithArg<2>(Invoke([this, r](Context *ctx) {
                                    m_work_queue->queue(ctx, r);
                                  }))));
  }

  void expect_object_map_resize(librbd::MockTestImageCtx &mock_image_ctx,
                                librados::snap_t snap_id, int r) {
    std::string oid(librbd::ObjectMap<>::object_map_name(mock_image_ctx.id,
                                                         snap_id));
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(oid, _, StrEq("rbd"), StrEq("object_map_resize"), _, _, _,
                     _))
                  .WillOnce(Return(r));
  }

  static void inject_snap(librbd::MockTestImageCtx &mock_image_ctx,
                   uint64_t snap_id, const std::string &snap_name) {
    mock_image_ctx.snap_ids[{cls::rbd::UserSnapshotNamespace(),
			     snap_name}] = snap_id;
  }

  MockSnapshotCreateRequest *create_request(librbd::MockTestImageCtx &mock_local_image_ctx,
                                            const std::string &snap_name,
					    const cls::rbd::SnapshotNamespace &snap_namespace,
                                            uint64_t size,
                                            const cls::rbd::ParentImageSpec &spec,
                                            uint64_t parent_overlap,
                                            Context *on_finish) {
    return new MockSnapshotCreateRequest(&mock_local_image_ctx, snap_name, snap_namespace, size,
                                         spec, parent_overlap, on_finish);
  }
};

TEST_F(TestMockDeepCopySnapshotCreateRequest, SnapCreate) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  MockSetHeadRequest mock_set_head_request;

  InSequence seq;
  expect_set_head(mock_set_head_request, 0);
  expect_start_op(mock_exclusive_lock);
  expect_snap_create(mock_image_ctx, "snap1", 10, 0);
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, false);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_image_ctx,
                                                      "snap1",
						      cls::rbd::UserSnapshotNamespace(),
                                                      m_image_ctx->size,
                                                      {}, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopySnapshotCreateRequest, SetHeadError) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  MockSetHeadRequest mock_set_head_request;

  InSequence seq;
  expect_set_head(mock_set_head_request, -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_image_ctx,
                                                      "snap1",
						      cls::rbd::UserSnapshotNamespace(),
						      123, {}, 0,
                                                      &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopySnapshotCreateRequest, SnapCreateError) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  MockSetHeadRequest mock_set_head_request;

  InSequence seq;
  expect_set_head(mock_set_head_request, 0);
  expect_start_op(mock_exclusive_lock);
  expect_snap_create(mock_image_ctx, "snap1", 10, -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_image_ctx,
                                                      "snap1",
						      cls::rbd::UserSnapshotNamespace(),
                                                      m_image_ctx->size,
                                                      {}, 0, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockDeepCopySnapshotCreateRequest, ResizeObjectMap) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  MockSetHeadRequest mock_set_head_request;

  InSequence seq;
  expect_set_head(mock_set_head_request, 0);
  expect_start_op(mock_exclusive_lock);
  expect_snap_create(mock_image_ctx, "snap1", 10, 0);
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, true);
  expect_start_op(mock_exclusive_lock);
  expect_object_map_resize(mock_image_ctx, 10, 0);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_image_ctx,
                                                      "snap1",
						      cls::rbd::UserSnapshotNamespace(),
                                                      m_image_ctx->size,
                                                      {}, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockDeepCopySnapshotCreateRequest, ResizeObjectMapError) {
  librbd::MockTestImageCtx mock_image_ctx(*m_image_ctx);
  librbd::MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  MockSetHeadRequest mock_set_head_request;

  InSequence seq;
  expect_set_head(mock_set_head_request, 0);
  expect_start_op(mock_exclusive_lock);
  expect_snap_create(mock_image_ctx, "snap1", 10, 0);
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, true);
  expect_start_op(mock_exclusive_lock);
  expect_object_map_resize(mock_image_ctx, 10, -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_image_ctx,
                                                      "snap1",
						      cls::rbd::UserSnapshotNamespace(),
                                                      m_image_ctx->size,
                                                      {}, 0, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace deep_copy
} // namespace librbd
