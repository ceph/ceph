// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/mock/image_sync/MockSyncPointHandler.h"
#include "tools/rbd_mirror/image_sync/SyncPointPruneRequest.h"

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
#include "tools/rbd_mirror/image_sync/SyncPointPruneRequest.cc"
template class rbd::mirror::image_sync::SyncPointPruneRequest<librbd::MockTestImageCtx>;

namespace rbd {
namespace mirror {
namespace image_sync {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockImageSyncSyncPointPruneRequest : public TestMockFixture {
public:
  typedef SyncPointPruneRequest<librbd::MockTestImageCtx> MockSyncPointPruneRequest;

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
                                 bool complete, int r) {
    EXPECT_CALL(mock_sync_point_handler, update_sync_points(_, _, complete, _))
      .WillOnce(DoAll(WithArg<1>(Invoke([this, r](const SyncPoints& sync_points) {
                                   if (r >= 0) {
                                     m_sync_points = sync_points;
                                   }
                                 })),
                      WithArg<3>(CompleteContext(r))));
  }

  void expect_get_snap_id(librbd::MockTestImageCtx &mock_remote_image_ctx,
                          const std::string &snap_name, uint64_t snap_id) {
    EXPECT_CALL(mock_remote_image_ctx, get_snap_id(_, StrEq(snap_name)))
      .WillOnce(Return(snap_id));
  }

  void expect_image_refresh(librbd::MockTestImageCtx &mock_remote_image_ctx, int r) {
    EXPECT_CALL(*mock_remote_image_ctx.state, refresh(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_snap_remove(librbd::MockTestImageCtx &mock_remote_image_ctx,
                          const std::string &snap_name, int r) {
    EXPECT_CALL(*mock_remote_image_ctx.operations, snap_remove(_, StrEq(snap_name), _))
      .WillOnce(WithArg<2>(CompleteContext(r)));
  }

  MockSyncPointPruneRequest *create_request(librbd::MockTestImageCtx &mock_remote_image_ctx,
                                            MockSyncPointHandler& mock_sync_point_handler,
                                            bool sync_complete, Context *ctx) {
    return new MockSyncPointPruneRequest(&mock_remote_image_ctx, sync_complete,
                                         &mock_sync_point_handler, ctx);
  }

  librbd::ImageCtx *m_remote_image_ctx;
  SyncPoints m_sync_points;
};

TEST_F(TestMockImageSyncSyncPointPruneRequest, SyncInProgressSuccess) {
  m_sync_points.emplace_front(cls::rbd::UserSnapshotNamespace(), "snap1",
			      "", boost::none);
  auto sync_points = m_sync_points;

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_get_snap_id(mock_remote_image_ctx, "snap1", 123);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_sync_points(mock_sync_point_handler, false, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_sync_point_handler,
                                                  false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(sync_points, m_sync_points);
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, RestartedSyncInProgressSuccess) {
  m_sync_points.emplace_front(cls::rbd::UserSnapshotNamespace(), "snap2",
                              "snap1", boost::none);
  m_sync_points.emplace_front(cls::rbd::UserSnapshotNamespace(), "snap1", "",
                              boost::none);
  auto sync_points = m_sync_points;

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_get_snap_id(mock_remote_image_ctx, "snap1", 123);
  expect_snap_remove(mock_remote_image_ctx, "snap2", 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_sync_points(mock_sync_point_handler, false, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_sync_point_handler,
                                                  false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  sync_points.pop_back();
  ASSERT_EQ(sync_points, m_sync_points);
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, SyncInProgressMissingSnapSuccess) {
  m_sync_points.emplace_front(cls::rbd::UserSnapshotNamespace(), "snap2",
                              "snap1", boost::none);
  m_sync_points.emplace_front(cls::rbd::UserSnapshotNamespace(), "snap1", "",
                              boost::none);

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_get_snap_id(mock_remote_image_ctx, "snap1", CEPH_NOSNAP);
  expect_snap_remove(mock_remote_image_ctx, "snap2", 0);
  expect_snap_remove(mock_remote_image_ctx, "snap1", 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_sync_points(mock_sync_point_handler, false, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_sync_point_handler,
                                                  false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(SyncPoints{}, m_sync_points);
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, SyncInProgressUnexpectedFromSnapSuccess) {
  m_sync_points.emplace_front(cls::rbd::UserSnapshotNamespace(), "snap2",
                              "snap1", boost::none);

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_get_snap_id(mock_remote_image_ctx, "snap2", 124);
  expect_snap_remove(mock_remote_image_ctx, "snap2", 0);
  expect_snap_remove(mock_remote_image_ctx, "snap1", 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_sync_points(mock_sync_point_handler, false, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_sync_point_handler,
                                                  false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(SyncPoints(), m_sync_points);
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, SyncCompleteSuccess) {
  m_sync_points.emplace_front(cls::rbd::UserSnapshotNamespace(), "snap1",
                              "", boost::none);

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_snap_remove(mock_remote_image_ctx, "snap1", 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_sync_points(mock_sync_point_handler, true, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_sync_point_handler,
                                                  true, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(m_sync_points.empty());
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, RestartedSyncCompleteSuccess) {
  m_sync_points.emplace_front(cls::rbd::UserSnapshotNamespace(), "snap2",
                              "snap1", boost::none);
  m_sync_points.emplace_front(cls::rbd::UserSnapshotNamespace(), "snap1",
                              "", boost::none);
  auto sync_points = m_sync_points;

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_sync_points(mock_sync_point_handler, true, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_sync_point_handler,
                                                  true, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
  sync_points.pop_front();
  ASSERT_EQ(sync_points, m_sync_points);
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, RestartedCatchUpSyncCompleteSuccess) {
  m_sync_points.emplace_front(cls::rbd::UserSnapshotNamespace(), "snap3",
                              "snap2", boost::none);
  m_sync_points.emplace_front(cls::rbd::UserSnapshotNamespace(), "snap2",
                              "snap1", boost::none);
  auto sync_points = m_sync_points;

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_snap_remove(mock_remote_image_ctx, "snap1", 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_sync_points(mock_sync_point_handler, true, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_sync_point_handler,
                                                  true, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
  sync_points.pop_front();
  ASSERT_EQ(sync_points, m_sync_points);
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, SnapshotDNE) {
  m_sync_points.emplace_front(cls::rbd::UserSnapshotNamespace(), "snap1",
                              "", boost::none);

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_snap_remove(mock_remote_image_ctx, "snap1", -ENOENT);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_sync_points(mock_sync_point_handler, true, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_sync_point_handler,
                                                  true, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(m_sync_points.empty());
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, ClientUpdateError) {
  m_sync_points.emplace_front(cls::rbd::UserSnapshotNamespace(), "snap2",
                              "snap1", boost::none);
  m_sync_points.emplace_front(cls::rbd::UserSnapshotNamespace(), "snap1",
                              "", boost::none);
  auto sync_points = m_sync_points;

  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_sync_points(mock_sync_point_handler, true, -EINVAL);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_sync_point_handler,
                                                  true, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());

  ASSERT_EQ(sync_points, m_sync_points);
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd
