// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "librbd/journal/Types.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/mock/MockJournaler.h"
#include "tools/rbd_mirror/image_sync/SyncPointCreateRequest.h"

// template definitions
#include "tools/rbd_mirror/image_sync/SyncPointCreateRequest.cc"
template class rbd::mirror::image_sync::SyncPointCreateRequest<librbd::MockImageCtx>;

namespace rbd {
namespace mirror {
namespace image_sync {

using ::testing::_;
using ::testing::InSequence;
using ::testing::WithArg;

class TestMockImageSyncSyncPointCreateRequest : public TestMockFixture {
public:
  typedef SyncPointCreateRequest<librbd::MockImageCtx> MockSyncPointCreateRequest;

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

  void expect_image_refresh(librbd::MockImageCtx &mock_remote_image_ctx, int r) {
    EXPECT_CALL(*mock_remote_image_ctx.state, refresh(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_snap_create(librbd::MockImageCtx &mock_remote_image_ctx, int r) {
    EXPECT_CALL(*mock_remote_image_ctx.operations, snap_create(_, _))
      .WillOnce(WithArg<1>(CompleteContext(r)));
  }

  MockSyncPointCreateRequest *create_request(librbd::MockImageCtx &mock_remote_image_ctx,
                                             journal::MockJournaler &mock_journaler,
                                             Context *ctx) {
    return new MockSyncPointCreateRequest(&mock_remote_image_ctx, "uuid",
                                          &mock_journaler, &m_client_meta, ctx);
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::journal::MirrorPeerClientMeta m_client_meta;
};

TEST_F(TestMockImageSyncSyncPointCreateRequest, Success) {
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_update_client(mock_journaler, 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_snap_create(mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockSyncPointCreateRequest *req = create_request(mock_remote_image_ctx,
                                                   mock_journaler, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(1U, m_client_meta.sync_points.size());
}

TEST_F(TestMockImageSyncSyncPointCreateRequest, ResyncSuccess) {
  m_client_meta.sync_points.emplace_front("start snap", "", boost::none);
  auto sync_point = m_client_meta.sync_points.front();

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_update_client(mock_journaler, 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_snap_create(mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockSyncPointCreateRequest *req = create_request(mock_remote_image_ctx,
                                                   mock_journaler, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(2U, m_client_meta.sync_points.size());
  ASSERT_EQ(sync_point, m_client_meta.sync_points.front());
  ASSERT_EQ("start snap", m_client_meta.sync_points.back().from_snap_name);
}

TEST_F(TestMockImageSyncSyncPointCreateRequest, SnapshotExists) {
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_update_client(mock_journaler, 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_snap_create(mock_remote_image_ctx, -EEXIST);
  expect_update_client(mock_journaler, 0);
  expect_image_refresh(mock_remote_image_ctx, 0);
  expect_snap_create(mock_remote_image_ctx, 0);

  C_SaferCond ctx;
  MockSyncPointCreateRequest *req = create_request(mock_remote_image_ctx,
                                                   mock_journaler, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(1U, m_client_meta.sync_points.size());
}

TEST_F(TestMockImageSyncSyncPointCreateRequest, ClientUpdateError) {
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_update_client(mock_journaler, -EINVAL);

  C_SaferCond ctx;
  MockSyncPointCreateRequest *req = create_request(mock_remote_image_ctx,
                                                   mock_journaler, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());

  ASSERT_TRUE(m_client_meta.sync_points.empty());
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd
