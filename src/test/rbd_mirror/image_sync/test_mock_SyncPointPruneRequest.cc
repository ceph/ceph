// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "tools/rbd_mirror/image_sync/SyncPointPruneRequest.h"

namespace librbd {
namespace journal {

template <>
struct TypeTraits<librbd::MockImageCtx> {
  typedef ::journal::MockJournaler Journaler;
};

} // namespace journal
} // namespace librbd

// template definitions
#include "tools/rbd_mirror/image_sync/SyncPointPruneRequest.cc"
template class rbd::mirror::image_sync::SyncPointPruneRequest<librbd::MockImageCtx>;

namespace rbd {
namespace mirror {
namespace image_sync {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockImageSyncSyncPointPruneRequest : public TestMockFixture {
public:
  typedef SyncPointPruneRequest<librbd::MockImageCtx> MockSyncPointPruneRequest;

  virtual void SetUp() {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));
  }

  void expect_update_client(journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, update_client(_, _))
      .WillOnce(WithArg<1>(CompleteContext(r)));
  }

  void expect_get_snap_id(librbd::MockImageCtx &mock_remote_image_ctx,
                          const std::string &snap_name, uint64_t snap_id) {
    EXPECT_CALL(mock_remote_image_ctx, get_snap_id(StrEq(snap_name)))
      .WillOnce(Return(snap_id));
  }

  void expect_image_refresh(librbd::MockImageCtx &mock_remote_image_ctx, int r) {
    EXPECT_CALL(*mock_remote_image_ctx.state, refresh(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_snap_remove(librbd::MockImageCtx &mock_remote_image_ctx,
                          const std::string &snap_name, int r) {
    EXPECT_CALL(*mock_remote_image_ctx.operations, snap_remove(StrEq(snap_name), _))
      .WillOnce(WithArg<1>(CompleteContext(r)));
  }

  MockSyncPointPruneRequest *create_request(librbd::MockImageCtx &mock_remote_image_ctx,
                                            journal::MockJournaler &mock_journaler,
                                            bool sync_complete, Context *ctx) {
    return new MockSyncPointPruneRequest(&mock_remote_image_ctx, sync_complete,
                                         &mock_journaler, &m_client_meta, ctx);
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::journal::MirrorPeerClientMeta m_client_meta;
};

TEST_F(TestMockImageSyncSyncPointPruneRequest, SyncInProgressSuccess) {
  librbd::journal::MirrorPeerClientMeta client_meta;
  client_meta.sync_points.emplace_front("snap1", boost::none);
  m_client_meta = client_meta;

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_get_snap_id(mock_remote_image_ctx, "snap1", 123);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_journaler, false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(client_meta, m_client_meta);
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, RestartedSyncInProgressSuccess) {
  librbd::journal::MirrorPeerClientMeta client_meta;
  client_meta.sync_points.emplace_front("snap2", "snap1", boost::none);
  client_meta.sync_points.emplace_front("snap1", boost::none);
  m_client_meta = client_meta;

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_get_snap_id(mock_remote_image_ctx, "snap1", 123);
  expect_snap_remove(mock_remote_image_ctx, "snap2", 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_journaler, false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  client_meta.sync_points.pop_back();
  ASSERT_EQ(client_meta, m_client_meta);
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, SyncInProgressMissingSnapSuccess) {
  librbd::journal::MirrorPeerClientMeta client_meta;
  client_meta.sync_points.emplace_front("snap2", "snap1", boost::none);
  client_meta.sync_points.emplace_front("snap1", boost::none);
  m_client_meta = client_meta;

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_get_snap_id(mock_remote_image_ctx, "snap1", CEPH_NOSNAP);
  expect_snap_remove(mock_remote_image_ctx, "snap2", 0);
  expect_snap_remove(mock_remote_image_ctx, "snap1", 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_journaler, false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  client_meta.sync_points.clear();
  ASSERT_EQ(client_meta, m_client_meta);
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, SyncInProgressUnexpectedFromSnapSuccess) {
  librbd::journal::MirrorPeerClientMeta client_meta;
  client_meta.sync_points.emplace_front("snap2", "snap1", boost::none);
  m_client_meta = client_meta;

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_get_snap_id(mock_remote_image_ctx, "snap2", 124);
  expect_snap_remove(mock_remote_image_ctx, "snap2", 0);
  expect_snap_remove(mock_remote_image_ctx, "snap1", 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_journaler, false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  client_meta.sync_points.clear();
  ASSERT_EQ(client_meta, m_client_meta);
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, SyncCompleteSuccess) {
  librbd::journal::MirrorPeerClientMeta client_meta;
  client_meta.sync_points.emplace_front("snap1", boost::none);
  m_client_meta = client_meta;
  ASSERT_EQ(librbd::journal::MIRROR_PEER_STATE_SYNCING, m_client_meta.state);

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_snap_remove(mock_remote_image_ctx, "snap1", 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_journaler, true, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(m_client_meta.sync_points.empty());
  ASSERT_EQ(librbd::journal::MIRROR_PEER_STATE_REPLAYING, m_client_meta.state);
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, RestartedSyncCompleteSuccess) {
  librbd::journal::MirrorPeerClientMeta client_meta;
  client_meta.sync_points.emplace_front("snap2", "snap1", boost::none);
  client_meta.sync_points.emplace_front("snap1", boost::none);
  m_client_meta = client_meta;

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_journaler, true, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
  client_meta.sync_points.pop_front();
  ASSERT_EQ(client_meta, m_client_meta);
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, RestartedCatchUpSyncCompleteSuccess) {
  librbd::journal::MirrorPeerClientMeta client_meta;
  client_meta.sync_points.emplace_front("snap3", "snap2", boost::none);
  client_meta.sync_points.emplace_front("snap2", "snap1", boost::none);
  m_client_meta = client_meta;

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_snap_remove(mock_remote_image_ctx, "snap1", 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_journaler, true, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
  client_meta.sync_points.pop_front();
  ASSERT_EQ(client_meta, m_client_meta);
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, SnapshotDNE) {
  librbd::journal::MirrorPeerClientMeta client_meta;
  client_meta.sync_points.emplace_front("snap1", boost::none);
  m_client_meta = client_meta;

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_snap_remove(mock_remote_image_ctx, "snap1", -ENOENT);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_journaler, true, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(m_client_meta.sync_points.empty());
}

TEST_F(TestMockImageSyncSyncPointPruneRequest, ClientUpdateError) {
  librbd::journal::MirrorPeerClientMeta client_meta;
  client_meta.sync_points.emplace_front("snap2", "snap1", boost::none);
  client_meta.sync_points.emplace_front("snap1", boost::none);
  m_client_meta = client_meta;

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_update_client(mock_journaler, -EINVAL);

  C_SaferCond ctx;
  MockSyncPointPruneRequest *req = create_request(mock_remote_image_ctx,
                                                  mock_journaler, true, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());

  ASSERT_EQ(client_meta, m_client_meta);
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd
