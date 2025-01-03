// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/mock/image_sync/MockSyncPointHandler.h"
#include "tools/rbd_mirror/image_sync/SyncPointCreateRequest.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

} // namespace librbd

// template definitions
#include "tools/rbd_mirror/image_sync/SyncPointCreateRequest.cc"

namespace rbd {
namespace mirror {
namespace image_sync {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;

class TestMockImageSyncSyncPointCreateRequest : public TestMockFixture {
public:
  typedef SyncPointCreateRequest<librbd::MockTestImageCtx> MockSyncPointCreateRequest;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));
  }

  void expect_get_snap_seqs(MockSyncPointHandler& mock_sync_point_handler) {
    EXPECT_CALL(mock_sync_point_handler, get_snap_seqs())
      .WillRepeatedly(Return(librbd::SnapSeqs{}));
  }

  void expect_get_sync_points(MockSyncPointHandler& mock_sync_point_handler) {
    EXPECT_CALL(mock_sync_point_handler, get_sync_points())
      .WillRepeatedly(Invoke([this]() {
                        return m_sync_points;
                      }));
  }

  void expect_update_sync_points(MockSyncPointHandler& mock_sync_point_handler,
                                 int r) {
    EXPECT_CALL(mock_sync_point_handler, update_sync_points(_, _, false, _))
      .WillOnce(DoAll(WithArg<1>(Invoke([this, r](const SyncPoints& sync_points) {
                                   if (r >= 0) {
                                     m_sync_points = sync_points;
                                   }
                                 })),
                      WithArg<3>(CompleteContext(r))));
  }

  void expect_image_refresh(librbd::MockTestImageCtx &mock_remote_image_ctx, int r) {
    EXPECT_CALL(*mock_remote_image_ctx.state, refresh(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_snap_create(librbd::MockTestImageCtx &mock_remote_image_ctx, int r) {
    EXPECT_CALL(*mock_remote_image_ctx.operations, snap_create(_, _, _, _, _))
      .WillOnce(WithArg<4>(CompleteContext(r)));
  }

  MockSyncPointCreateRequest *create_request(librbd::MockTestImageCtx &mock_remote_image_ctx,
                                             MockSyncPointHandler& mock_sync_point_handler,
                                             Context *ctx) {
    return new MockSyncPointCreateRequest(&mock_remote_image_ctx, "uuid",
                                          &mock_sync_point_handler, ctx);
  }

  librbd::ImageCtx *m_remote_image_ctx;
  SyncPoints m_sync_points;
};

TEST_F(TestMockImageSyncSyncPointCreateRequest, Success) {
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_update_sync_points(mock_sync_point_handler, 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_snap_create(mock_remote_image_ctx, 0);
  expect_image_refresh(mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockSyncPointCreateRequest *req = create_request(mock_remote_image_ctx,
                                                   mock_sync_point_handler,
                                                   &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(1U, m_sync_points.size());
}

TEST_F(TestMockImageSyncSyncPointCreateRequest, ResyncSuccess) {
  m_sync_points.emplace_front(cls::rbd::UserSnapshotNamespace(), "start snap",
                              "", boost::none);
  auto sync_point = m_sync_points.front();

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_update_sync_points(mock_sync_point_handler, 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_snap_create(mock_remote_image_ctx, 0);
  expect_image_refresh(mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockSyncPointCreateRequest *req = create_request(mock_remote_image_ctx,
                                                   mock_sync_point_handler,
                                                   &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(2U, m_sync_points.size());
  ASSERT_EQ(sync_point, m_sync_points.front());
  ASSERT_EQ("start snap", m_sync_points.back().from_snap_name);
}

TEST_F(TestMockImageSyncSyncPointCreateRequest, SnapshotExists) {
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_update_sync_points(mock_sync_point_handler, 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_snap_create(mock_remote_image_ctx, -EEXIST);
  expect_update_sync_points(mock_sync_point_handler, 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_snap_create(mock_remote_image_ctx, 0);
  expect_image_refresh(mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockSyncPointCreateRequest *req = create_request(mock_remote_image_ctx,
                                                   mock_sync_point_handler,
                                                   &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(1U, m_sync_points.size());
}

TEST_F(TestMockImageSyncSyncPointCreateRequest, ClientUpdateError) {
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_update_sync_points(mock_sync_point_handler, -EINVAL);

  C_SaferCond ctx;
  MockSyncPointCreateRequest *req = create_request(mock_remote_image_ctx,
                                                   mock_sync_point_handler,
                                                   &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());

  ASSERT_TRUE(m_sync_points.empty());
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd
