// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockObjectMap.h"
#include "tools/rbd_mirror/ImageSync.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_sync/ImageCopyRequest.h"
#include "tools/rbd_mirror/image_sync/SnapshotCopyRequest.h"
#include "tools/rbd_mirror/image_sync/SyncPointCreateRequest.h"
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
#include "tools/rbd_mirror/ImageSync.cc"
template class rbd::mirror::ImageSync<librbd::MockImageCtx>;

namespace rbd {
namespace mirror {

namespace image_sync {

template <>
class ImageCopyRequest<librbd::MockImageCtx> {
public:
  static ImageCopyRequest* s_instance;
  Context *on_finish;

  static ImageCopyRequest* create(librbd::MockImageCtx *local_image_ctx,
                                  librbd::MockImageCtx *remote_image_ctx,
                                  SafeTimer *timer, Mutex *timer_lock,
                                  journal::MockJournaler *journaler,
                                  librbd::journal::MirrorPeerClientMeta *client_meta,
                                  librbd::journal::MirrorPeerSyncPoint *sync_point,
                                  Context *on_finish,
                                  rbd::mirror::ProgressContext *progress_ctx = nullptr) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  ImageCopyRequest() {
    s_instance = this;
  }

  void put() {
  }

  void get() {
  }

  MOCK_METHOD0(cancel, void());
  MOCK_METHOD0(send, void());
};

template <>
class SnapshotCopyRequest<librbd::MockImageCtx> {
public:
  static SnapshotCopyRequest* s_instance;
  Context *on_finish;

  static SnapshotCopyRequest* create(librbd::MockImageCtx *local_image_ctx,
                                     librbd::MockImageCtx *remote_image_ctx,
                                     SnapshotCopyRequest<librbd::ImageCtx>::SnapMap *snap_map,
                                     journal::MockJournaler *journaler,
                                     librbd::journal::MirrorPeerClientMeta *client_meta,
                                     ContextWQ *work_queue,
                                     Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  SnapshotCopyRequest() {
    s_instance = this;
  }

  void put() {
  }

  void get() {
  }

  MOCK_METHOD0(send, void());
  MOCK_METHOD0(cancel, void());
};

template <>
class SyncPointCreateRequest<librbd::MockImageCtx> {
public:
  static SyncPointCreateRequest *s_instance;
  Context *on_finish;

  static SyncPointCreateRequest* create(librbd::MockImageCtx *remote_image_ctx,
                                        const std::string &mirror_uuid,
                                        journal::MockJournaler *journaler,
                                        librbd::journal::MirrorPeerClientMeta *client_meta,
                                        Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  SyncPointCreateRequest() {
    s_instance = this;
  }
  MOCK_METHOD0(send, void());
};

template <>
class SyncPointPruneRequest<librbd::MockImageCtx> {
public:
  static SyncPointPruneRequest *s_instance;
  Context *on_finish;
  bool sync_complete;

  static SyncPointPruneRequest* create(librbd::MockImageCtx *remote_image_ctx,
                                       bool sync_complete,
                                       journal::MockJournaler *journaler,
                                       librbd::journal::MirrorPeerClientMeta *client_meta,
                                       Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    s_instance->sync_complete = sync_complete;
    return s_instance;
  }

  SyncPointPruneRequest() {
    s_instance = this;
  }
  MOCK_METHOD0(send, void());
};

ImageCopyRequest<librbd::MockImageCtx>* ImageCopyRequest<librbd::MockImageCtx>::s_instance = nullptr;
SnapshotCopyRequest<librbd::MockImageCtx>* SnapshotCopyRequest<librbd::MockImageCtx>::s_instance = nullptr;
SyncPointCreateRequest<librbd::MockImageCtx>* SyncPointCreateRequest<librbd::MockImageCtx>::s_instance = nullptr;
SyncPointPruneRequest<librbd::MockImageCtx>* SyncPointPruneRequest<librbd::MockImageCtx>::s_instance = nullptr;

} // namespace image_sync

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;
using ::testing::InvokeWithoutArgs;

class TestMockImageSync : public TestMockFixture {
public:
  typedef ImageSync<librbd::MockImageCtx> MockImageSync;
  typedef image_sync::ImageCopyRequest<librbd::MockImageCtx> MockImageCopyRequest;
  typedef image_sync::SnapshotCopyRequest<librbd::MockImageCtx> MockSnapshotCopyRequest;
  typedef image_sync::SyncPointCreateRequest<librbd::MockImageCtx> MockSyncPointCreateRequest;
  typedef image_sync::SyncPointPruneRequest<librbd::MockImageCtx> MockSyncPointPruneRequest;

  virtual void SetUp() {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));

    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
  }

  void expect_create_sync_point(librbd::MockImageCtx &mock_local_image_ctx,
                                MockSyncPointCreateRequest &mock_sync_point_create_request,
                                int r) {
    EXPECT_CALL(mock_sync_point_create_request, send())
      .WillOnce(Invoke([this, &mock_local_image_ctx, &mock_sync_point_create_request, r]() {
          if (r == 0) {
            mock_local_image_ctx.snap_ids["snap1"] = 123;
            m_client_meta.sync_points.emplace_back("snap1", boost::none);
          }
          m_threads->work_queue->queue(mock_sync_point_create_request.on_finish, r);
        }));
  }

  void expect_copy_snapshots(MockSnapshotCopyRequest &mock_snapshot_copy_request, int r) {
    EXPECT_CALL(mock_snapshot_copy_request, send())
      .WillOnce(Invoke([this, &mock_snapshot_copy_request, r]() {
          m_threads->work_queue->queue(mock_snapshot_copy_request.on_finish, r);
        }));
  }

  void expect_copy_image(MockImageCopyRequest &mock_image_copy_request, int r) {
    EXPECT_CALL(mock_image_copy_request, send())
      .WillOnce(Invoke([this, &mock_image_copy_request, r]() {
          m_threads->work_queue->queue(mock_image_copy_request.on_finish, r);
        }));
  }

  void expect_rollback_object_map(librbd::MockObjectMap &mock_object_map, int r) {
    if ((m_local_image_ctx->features & RBD_FEATURE_OBJECT_MAP) != 0) {
      EXPECT_CALL(mock_object_map, rollback(_, _))
        .WillOnce(WithArg<1>(Invoke([this, r](Context *ctx) {
            m_threads->work_queue->queue(ctx, r);
          })));
    }
  }

  void expect_create_object_map(librbd::MockImageCtx &mock_image_ctx,
                                librbd::MockObjectMap *mock_object_map) {
    EXPECT_CALL(mock_image_ctx, create_object_map(CEPH_NOSNAP))
      .WillOnce(Return(mock_object_map));
  }

  void expect_open_object_map(librbd::MockImageCtx &mock_image_ctx,
                              librbd::MockObjectMap &mock_object_map) {
    EXPECT_CALL(mock_object_map, open(_))
      .WillOnce(Invoke([this](Context *ctx) {
          m_threads->work_queue->queue(ctx, 0);
        }));
  }

  void expect_prune_sync_point(MockSyncPointPruneRequest &mock_sync_point_prune_request,
                               bool sync_complete, int r) {
    EXPECT_CALL(mock_sync_point_prune_request, send())
      .WillOnce(Invoke([this, &mock_sync_point_prune_request, sync_complete, r]() {
          ASSERT_EQ(sync_complete, mock_sync_point_prune_request.sync_complete);
          if (r == 0 && !m_client_meta.sync_points.empty()) {
            if (sync_complete) {
              m_client_meta.sync_points.pop_front();
            } else {
              while (m_client_meta.sync_points.size() > 1) {
                m_client_meta.sync_points.pop_back();
              }
            }
          }
          m_threads->work_queue->queue(mock_sync_point_prune_request.on_finish, r);
        }));
  }

  MockImageSync *create_request(librbd::MockImageCtx &mock_remote_image_ctx,
                                librbd::MockImageCtx &mock_local_image_ctx,
                                journal::MockJournaler &mock_journaler,
                                Context *ctx) {
    return new MockImageSync(&mock_local_image_ctx, &mock_remote_image_ctx,
                             m_threads->timer, &m_threads->timer_lock,
                             "mirror-uuid", &mock_journaler, &m_client_meta,
                             m_threads->work_queue, ctx);
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::ImageCtx *m_local_image_ctx;
  librbd::journal::MirrorPeerClientMeta m_client_meta;
};

TEST_F(TestMockImageSync, SimpleSync) {
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;
  MockImageCopyRequest mock_image_copy_request;
  MockSnapshotCopyRequest mock_snapshot_copy_request;
  MockSyncPointCreateRequest mock_sync_point_create_request;
  MockSyncPointPruneRequest mock_sync_point_prune_request;

  librbd::MockObjectMap *mock_object_map = new librbd::MockObjectMap();
  mock_local_image_ctx.object_map = mock_object_map;
  expect_test_features(mock_local_image_ctx);

  InSequence seq;
  expect_create_sync_point(mock_local_image_ctx, mock_sync_point_create_request, 0);
  expect_copy_snapshots(mock_snapshot_copy_request, 0);
  expect_copy_image(mock_image_copy_request, 0);
  expect_rollback_object_map(*mock_object_map, 0);
  expect_create_object_map(mock_local_image_ctx, mock_object_map);
  expect_open_object_map(mock_local_image_ctx, *mock_object_map);
  expect_prune_sync_point(mock_sync_point_prune_request, true, 0);

  C_SaferCond ctx;
  MockImageSync *request = create_request(mock_remote_image_ctx,
                                          mock_local_image_ctx,
                                          mock_journaler, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSync, RestartSync) {
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;
  MockImageCopyRequest mock_image_copy_request;
  MockSnapshotCopyRequest mock_snapshot_copy_request;
  MockSyncPointCreateRequest mock_sync_point_create_request;
  MockSyncPointPruneRequest mock_sync_point_prune_request;

  m_client_meta.sync_points = {{"snap1", boost::none},
                               {"snap2", "snap1", boost::none}};
  mock_local_image_ctx.snap_ids["snap1"] = 123;
  mock_local_image_ctx.snap_ids["snap2"] = 234;

  librbd::MockObjectMap *mock_object_map = new librbd::MockObjectMap();
  mock_local_image_ctx.object_map = mock_object_map;
  expect_test_features(mock_local_image_ctx);

  InSequence seq;
  expect_prune_sync_point(mock_sync_point_prune_request, false, 0);
  expect_copy_snapshots(mock_snapshot_copy_request, 0);
  expect_copy_image(mock_image_copy_request, 0);
  expect_rollback_object_map(*mock_object_map, 0);
  expect_create_object_map(mock_local_image_ctx, mock_object_map);
  expect_open_object_map(mock_local_image_ctx, *mock_object_map);
  expect_prune_sync_point(mock_sync_point_prune_request, true, 0);

  C_SaferCond ctx;
  MockImageSync *request = create_request(mock_remote_image_ctx,
                                          mock_local_image_ctx,
                                          mock_journaler, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSync, CancelImageCopy) {
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;
  MockImageCopyRequest mock_image_copy_request;
  MockSnapshotCopyRequest mock_snapshot_copy_request;
  MockSyncPointCreateRequest mock_sync_point_create_request;
  MockSyncPointPruneRequest mock_sync_point_prune_request;

  m_client_meta.sync_points = {{"snap1", boost::none}};

  InSequence seq;
  expect_prune_sync_point(mock_sync_point_prune_request, false, 0);
  expect_copy_snapshots(mock_snapshot_copy_request, 0);

  C_SaferCond image_copy_ctx;
  EXPECT_CALL(mock_image_copy_request, send())
    .WillOnce(Invoke([&image_copy_ctx]() {
        image_copy_ctx.complete(0);
      }));
  EXPECT_CALL(mock_image_copy_request, cancel());

  C_SaferCond ctx;
  MockImageSync *request = create_request(mock_remote_image_ctx,
                                          mock_local_image_ctx,
                                          mock_journaler, &ctx);
  request->get();
  request->send();

  // cancel the image copy once it starts
  ASSERT_EQ(0, image_copy_ctx.wait());
  request->cancel();
  request->put();
  m_threads->work_queue->queue(mock_image_copy_request.on_finish, 0);

  ASSERT_EQ(-ECANCELED, ctx.wait());
}

TEST_F(TestMockImageSync, CancelAfterCopySnapshots) {
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;
  MockSnapshotCopyRequest mock_snapshot_copy_request;
  MockSyncPointCreateRequest mock_sync_point_create_request;

  librbd::MockObjectMap *mock_object_map = new librbd::MockObjectMap();
  mock_local_image_ctx.object_map = mock_object_map;
  expect_test_features(mock_local_image_ctx);

  C_SaferCond ctx;
  MockImageSync *request = create_request(mock_remote_image_ctx,
                                          mock_local_image_ctx,
                                          mock_journaler, &ctx);
  InSequence seq;
  expect_create_sync_point(mock_local_image_ctx, mock_sync_point_create_request, 0);
  EXPECT_CALL(mock_snapshot_copy_request, send())
    .WillOnce((DoAll(InvokeWithoutArgs([request]() {
	      request->cancel();
	    }),
	  Invoke([this, &mock_snapshot_copy_request]() {
	      m_threads->work_queue->queue(mock_snapshot_copy_request.on_finish, 0);
	    }))));
  EXPECT_CALL(mock_snapshot_copy_request, cancel());

  request->send();
  ASSERT_EQ(-ECANCELED, ctx.wait());
}

TEST_F(TestMockImageSync, CancelAfterCopyImage) {
  librbd::MockImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockImageCtx mock_local_image_ctx(*m_local_image_ctx);
  journal::MockJournaler mock_journaler;
  MockImageCopyRequest mock_image_copy_request;
  MockSnapshotCopyRequest mock_snapshot_copy_request;
  MockSyncPointCreateRequest mock_sync_point_create_request;
  MockSyncPointPruneRequest mock_sync_point_prune_request;

  librbd::MockObjectMap *mock_object_map = new librbd::MockObjectMap();
  mock_local_image_ctx.object_map = mock_object_map;
  expect_test_features(mock_local_image_ctx);

  C_SaferCond ctx;
  MockImageSync *request = create_request(mock_remote_image_ctx,
                                          mock_local_image_ctx,
                                          mock_journaler, &ctx);
  InSequence seq;
  expect_create_sync_point(mock_local_image_ctx, mock_sync_point_create_request, 0);
  expect_copy_snapshots(mock_snapshot_copy_request, 0);
  EXPECT_CALL(mock_image_copy_request, send())
    .WillOnce((DoAll(InvokeWithoutArgs([request]() {
	      request->cancel();
	    }),
	  Invoke([this, &mock_image_copy_request]() {
	      m_threads->work_queue->queue(mock_image_copy_request.on_finish, 0);
	    }))));
  EXPECT_CALL(mock_image_copy_request, cancel());

  request->send();
  ASSERT_EQ(-ECANCELED, ctx.wait());
}

} // namespace mirror
} // namespace rbd
