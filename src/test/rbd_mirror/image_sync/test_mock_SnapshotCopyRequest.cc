// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/mock/MockJournaler.h"
#include "tools/rbd_mirror/image_sync/SnapshotCopyRequest.h"
#include "tools/rbd_mirror/Threads.h"

// template definitions
#include "tools/rbd_mirror/image_sync/SnapshotCopyRequest.cc"
template class rbd::mirror::image_sync::SnapshotCopyRequest<librbd::MockImageCtx>;

namespace rbd {
namespace mirror {
namespace image_sync {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::InvokeWithoutArgs;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockImageSyncSnapshotCopyRequest : public TestMockFixture {
public:
  typedef SnapshotCopyRequest<librbd::MockImageCtx> MockSnapshotCopyRequest;

  virtual void SetUp() {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));

    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));

    m_threads = new rbd::mirror::Threads(reinterpret_cast<CephContext*>(
      m_local_io_ctx.cct()));
  }

  virtual void TearDown() {
    delete m_threads;
    TestMockFixture::TearDown();
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

  void expect_snap_remove(librbd::MockImageCtx &mock_image_ctx,
                          const std::string &snap_name, int r) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_snap_remove(StrEq(snap_name), _))
                  .WillOnce(WithArg<1>(Invoke([this, r](Context *ctx) {
                              m_threads->work_queue->queue(ctx, r);
                            })));
  }

  void expect_update_client(journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, update_client(_, _))
                  .WillOnce(WithArg<1>(CompleteContext(r)));
  }

  static void inject_snap(librbd::MockImageCtx &mock_image_ctx,
                   uint64_t snap_id, const std::string &snap_name) {
    mock_image_ctx.snap_ids[snap_name] = snap_id;
  }

  MockSnapshotCopyRequest *create_request(librbd::MockImageCtx &mock_remote_image_ctx,
                                          librbd::MockImageCtx &mock_local_image_ctx,
                                          journal::MockJournaler &mock_journaler,
                                          Context *on_finish) {
    return new MockSnapshotCopyRequest(&mock_local_image_ctx,
                                       &mock_remote_image_ctx, &m_snap_map,
                                       &mock_journaler, &m_client_meta,
                                       on_finish);
  }

  int create_snap(librbd::ImageCtx *image_ctx, const std::string &snap_name) {
    int r = image_ctx->operations->snap_create(snap_name.c_str());
    if (r < 0) {
      return r;
    }

    r = image_ctx->state->refresh();
    if (r < 0) {
      return r;
    }
    return 0;
  }

  void validate_snap_seqs(const librbd::journal::MirrorPeerClientMeta::SnapSeqs &snap_seqs) {
    ASSERT_EQ(snap_seqs, m_client_meta.snap_seqs);
  }

  void validate_snap_map(const MockSnapshotCopyRequest::SnapMap &snap_map) {
    ASSERT_EQ(snap_map, m_snap_map);
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::ImageCtx *m_local_image_ctx;

  MockSnapshotCopyRequest::SnapMap m_snap_map;
  librbd::journal::MirrorPeerClientMeta m_client_meta;

  rbd::mirror::Threads *m_threads = nullptr;
};

TEST_F(TestMockImageSyncSnapshotCopyRequest, Empty) {
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_remote_image_ctx,
                                                    mock_local_image_ctx,
                                                    mock_journaler, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  validate_snap_map({});
  validate_snap_seqs({});
}

TEST_F(TestMockImageSyncSnapshotCopyRequest, UpdateClientError) {
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_update_client(mock_journaler, -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_remote_image_ctx,
                                                    mock_local_image_ctx,
                                                    mock_journaler, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCopyRequest, SnapCreate) {
  ASSERT_EQ(0, create_snap(m_remote_image_ctx, "snap1"));
  ASSERT_EQ(0, create_snap(m_remote_image_ctx, "snap2"));

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_snap_create(mock_local_image_ctx, "snap1", 12, 0);
  expect_snap_create(mock_local_image_ctx, "snap2", 14, 0);
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_remote_image_ctx,
                                                    mock_local_image_ctx,
                                                    mock_journaler, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  uint64_t snap_id1 = m_remote_image_ctx->snap_ids["snap1"];
  uint64_t snap_id2 = m_remote_image_ctx->snap_ids["snap2"];
  validate_snap_map({{snap_id1, {12}}, {snap_id2, {14, 12}}});
  validate_snap_seqs({{snap_id1, 12}, {snap_id2, 14}});
}

TEST_F(TestMockImageSyncSnapshotCopyRequest, SnapCreateError) {
  ASSERT_EQ(0, create_snap(m_remote_image_ctx, "snap1"));

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_snap_create(mock_local_image_ctx, "snap1", 12, -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_remote_image_ctx,
                                                    mock_local_image_ctx,
                                                    mock_journaler, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockImageSyncSnapshotCopyRequest, SnapRemove) {
  ASSERT_EQ(0, create_snap(m_remote_image_ctx, "snap1"));
  ASSERT_EQ(0, create_snap(m_local_image_ctx, "snap1"));

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_snap_remove(mock_local_image_ctx, "snap1", 0);
  expect_snap_create(mock_local_image_ctx, "snap1", 12, 0);
  expect_update_client(mock_journaler, 0);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_remote_image_ctx,
                                                    mock_local_image_ctx,
                                                    mock_journaler, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());

  uint64_t snap_id1 = m_remote_image_ctx->snap_ids["snap1"];
  validate_snap_map({{snap_id1, {12}}});
  validate_snap_seqs({{snap_id1, 12}});
}

TEST_F(TestMockImageSyncSnapshotCopyRequest, SnapRemoveError) {
  ASSERT_EQ(0, create_snap(m_local_image_ctx, "snap1"));

  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;

  InSequence seq;
  expect_snap_remove(mock_local_image_ctx, "snap1", -EINVAL);

  C_SaferCond ctx;
  MockSnapshotCopyRequest *request = create_request(mock_remote_image_ctx,
                                                    mock_local_image_ctx,
                                                    mock_journaler, &ctx);
  request->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd
