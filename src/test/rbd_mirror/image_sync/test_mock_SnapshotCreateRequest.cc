// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/librados_test_stub/LibradosTestStub.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "tools/rbd_mirror/image_sync/SnapshotCreateRequest.h"
#include "tools/rbd_mirror/Threads.h"

// template definitions
#include "tools/rbd_mirror/image_sync/SnapshotCreateRequest.cc"
template class rbd::mirror::image_sync::SnapshotCreateRequest<librbd::MockImageCtx>;

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
  typedef SnapshotCreateRequest<librbd::MockImageCtx> MockSnapshotCreateRequest;

  virtual void SetUp() {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
  }

  void expect_set_size(librbd::MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"), StrEq("set_size"), _, _, _))
                  .WillOnce(Return(r));
  }

  void expect_snap_create(librbd::MockImageCtx &mock_image_ctx,
                          const std::string &snap_name, uint64_t snap_id, int r) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_snap_create(StrEq(snap_name), _, 0))
                  .WillOnce(DoAll(InvokeWithoutArgs([&mock_image_ctx, snap_id, snap_name]() {
                                    inject_snap(mock_image_ctx, snap_id, snap_name);
                                  }),
                                  WithArg<1>(Invoke([this, r](Context *ctx) {
                                    m_threads->work_queue->queue(ctx, r);
                                  }))));
  }

  static void inject_snap(librbd::MockImageCtx &mock_image_ctx,
                   uint64_t snap_id, const std::string &snap_name) {
    mock_image_ctx.snap_ids[snap_name] = snap_id;
  }

  MockSnapshotCreateRequest *create_request(librbd::MockImageCtx &mock_local_image_ctx,
                                            const std::string &snap_name,
                                            uint64_t size, Context *on_finish) {
    return new MockSnapshotCreateRequest(&mock_local_image_ctx, snap_name, size,
                                         on_finish);
  }

  librbd::ImageCtx *m_local_image_ctx;
};

TEST_F(TestMockImageSyncSnapshotCreateRequest, Resize) {
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);

  InSequence seq;
  expect_set_size(mock_local_image_ctx, 0);
  expect_snap_create(mock_local_image_ctx, "snap1", 10, 0);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1", 123,
                                                      &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCreateRequest, ResizeError) {
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);

  InSequence seq;
  expect_set_size(mock_local_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1", 123,
                                                      &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCreateRequest, SnapCreate) {
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);

  InSequence seq;
  expect_snap_create(mock_local_image_ctx, "snap1", 10, 0);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1",
                                                      m_local_image_ctx->size,
                                                      &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCreateRequest, SnapCreateError) {
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);

  InSequence seq;
  expect_snap_create(mock_local_image_ctx, "snap1", 10, -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCreateRequest *request = create_request(mock_local_image_ctx,
                                                      "snap1",
                                                      m_local_image_ctx->size,
                                                      &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd
