// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/librados_test_stub/LibradosTestStub.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "osdc/Striper.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "tools/rbd_mirror/image_sync/SnapshotCreateRequest.h"
#include "tools/rbd_mirror/Threads.h"

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
#include "tools/rbd_mirror/image_sync/SnapshotCreateRequest.cc"
template class rbd::mirror::image_sync::SnapshotCreateRequest<librbd::MockTestImageCtx>;

namespace rbd {
namespace mirror {
namespace image_sync {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::InvokeWithoutArgs;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockImageSyncSnapshotCreateRequest : public TestMockFixture {
public:
  typedef SnapshotCreateRequest<librbd::MockTestImageCtx> MockSnapshotCreateRequest;

  virtual void SetUp() {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
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

  void expect_snap_create(librbd::MockTestImageCtx &mock_image_ctx,
                          const std::string &snap_name, uint64_t snap_id, int r) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_snap_create(StrEq(snap_name), _, 0, true))
                  .WillOnce(DoAll(InvokeWithoutArgs([&mock_image_ctx, snap_id, snap_name]() {
                                    inject_snap(mock_image_ctx, snap_id, snap_name);
                                  }),
                                  WithArg<1>(Invoke([this, r](Context *ctx) {
                                    m_threads->work_queue->queue(ctx, r);
                                  }))));
  }

  void expect_object_map_resize(librbd::MockTestImageCtx &mock_image_ctx,
                                librados::snap_t snap_id, int r) {
    std::string oid(librbd::ObjectMap<>::object_map_name(mock_image_ctx.id,
                                                         snap_id));
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(oid, _, StrEq("rbd"), StrEq("object_map_resize"), _, _, _))
                  .WillOnce(Return(r));
  }

  static void inject_snap(librbd::MockTestImageCtx &mock_image_ctx,
                   uint64_t snap_id, const std::string &snap_name) {
    mock_image_ctx.snap_ids[snap_name] = snap_id;
  }

  MockSnapshotCreateRequest *create_request(librbd::MockTestImageCtx &mock_local_image_ctx,
                                            const std::string &snap_name,
                                            uint64_t size,
                                            const librbd::parent_spec &spec,
                                            uint64_t parent_overlap,
                                            Context *on_finish) {
    return new MockSnapshotCreateRequest(&mock_local_image_ctx, snap_name, size,
                                         spec, parent_overlap, on_finish);
  }

  librbd::ImageCtx *m_local_image_ctx;
};

TEST_F(TestMockImageSyncSnapshotCreateRequest, Resize) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  InSequence seq;
  expect_set_size(mock_local_image_ctx, 0);
  expect_snap_create(mock_local_image_ctx, "snap1", 10, 0);
  expect_test_features(mock_local_image_ctx, RBD_FEATURE_OBJECT_MAP, false);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1", 123, {}, 0,
                                                      &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCreateRequest, ResizeError) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  InSequence seq;
  expect_set_size(mock_local_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1", 123, {}, 0,
                                                      &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCreateRequest, RemoveParent) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.parent_md.spec.pool_id = 213;

  InSequence seq;
  expect_remove_parent(mock_local_image_ctx, 0);
  expect_snap_create(mock_local_image_ctx, "snap1", 10, 0);
  expect_test_features(mock_local_image_ctx, RBD_FEATURE_OBJECT_MAP, false);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1",
                                                      m_local_image_ctx->size,
                                                      {}, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCreateRequest, RemoveParentError) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.parent_md.spec.pool_id = 213;

  InSequence seq;
  expect_remove_parent(mock_local_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1",
                                                      m_local_image_ctx->size,
                                                      {}, 0, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCreateRequest, RemoveSetParent) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.parent_md.spec.pool_id = 213;

  InSequence seq;
  expect_remove_parent(mock_local_image_ctx, 0);
  expect_set_parent(mock_local_image_ctx, 0);
  expect_snap_create(mock_local_image_ctx, "snap1", 10, 0);
  expect_test_features(mock_local_image_ctx, RBD_FEATURE_OBJECT_MAP, false);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1",
                                                      m_local_image_ctx->size,
                                                      {123, "test", 0}, 0,
                                                      &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCreateRequest, SetParentSpec) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  InSequence seq;
  expect_set_parent(mock_local_image_ctx, 0);
  expect_snap_create(mock_local_image_ctx, "snap1", 10, 0);
  expect_test_features(mock_local_image_ctx, RBD_FEATURE_OBJECT_MAP, false);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1",
                                                      m_local_image_ctx->size,
                                                      {123, "test", 0}, 0,
                                                      &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCreateRequest, SetParentOverlap) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  mock_local_image_ctx.parent_md.spec = {123, "test", 0};

  InSequence seq;
  expect_set_parent(mock_local_image_ctx, 0);
  expect_snap_create(mock_local_image_ctx, "snap1", 10, 0);
  expect_test_features(mock_local_image_ctx, RBD_FEATURE_OBJECT_MAP, false);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1",
                                                      m_local_image_ctx->size,
                                                      mock_local_image_ctx.parent_md.spec,
                                                      123, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCreateRequest, SetParentError) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  InSequence seq;
  expect_set_parent(mock_local_image_ctx, -ESTALE);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1",
                                                      m_local_image_ctx->size,
                                                      {123, "test", 0}, 0,
                                                      &ctx);
  request->send();
  ASSERT_EQ(-ESTALE, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCreateRequest, SnapCreate) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  InSequence seq;
  expect_snap_create(mock_local_image_ctx, "snap1", 10, 0);
  expect_test_features(mock_local_image_ctx, RBD_FEATURE_OBJECT_MAP, false);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1",
                                                      m_local_image_ctx->size,
                                                      {}, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCreateRequest, SnapCreateError) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  InSequence seq;
  expect_snap_create(mock_local_image_ctx, "snap1", 10, -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1",
                                                      m_local_image_ctx->size,
                                                      {}, 0, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCreateRequest, ResizeObjectMap) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  InSequence seq;
  expect_snap_create(mock_local_image_ctx, "snap1", 10, 0);
  expect_test_features(mock_local_image_ctx, RBD_FEATURE_OBJECT_MAP, true);
  expect_object_map_resize(mock_local_image_ctx, 10, 0);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1",
                                                      m_local_image_ctx->size,
                                                      {}, 0, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCreateRequest, ResizeObjectMapError) {
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  InSequence seq;
  expect_snap_create(mock_local_image_ctx, "snap1", 10, 0);
  expect_test_features(mock_local_image_ctx, RBD_FEATURE_OBJECT_MAP, true);
  expect_object_map_resize(mock_local_image_ctx, 10, -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1",
                                                      m_local_image_ctx->size,
                                                      {}, 0, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd
