// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "librbd/DeepCopyRequest.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/mock/image_sync/MockSyncPointHandler.h"
#include "tools/rbd_mirror/ImageSync.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_sync/SyncPointCreateRequest.h"
#include "tools/rbd_mirror/image_sync/SyncPointPruneRequest.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

template <>
class DeepCopyRequest<librbd::MockTestImageCtx> {
public:
  static DeepCopyRequest* s_instance;
  Context *on_finish;

  static DeepCopyRequest* create(
      librbd::MockTestImageCtx *src_image_ctx,
      librbd::MockTestImageCtx *dst_image_ctx,
      librados::snap_t src_snap_id_start, librados::snap_t src_snap_id_end,
      librados::snap_t dst_snap_id_start, bool flatten,
      const librbd::deep_copy::ObjectNumber &object_number,
      librbd::asio::ContextWQ *work_queue, SnapSeqs *snap_seqs,
      deep_copy::Handler *handler, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  DeepCopyRequest() {
    s_instance = this;
  }

  void put() {
  }

  void get() {
  }

  MOCK_METHOD0(cancel, void());
  MOCK_METHOD0(send, void());
};

DeepCopyRequest<librbd::MockTestImageCtx>* DeepCopyRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace librbd

// template definitions
#include "tools/rbd_mirror/ImageSync.cc"

namespace rbd {
namespace mirror {

template <>
struct Threads<librbd::MockTestImageCtx> {
  ceph::mutex &timer_lock;
  SafeTimer *timer;
  librbd::asio::ContextWQ *work_queue;

  Threads(Threads<librbd::ImageCtx> *threads)
    : timer_lock(threads->timer_lock), timer(threads->timer),
      work_queue(threads->work_queue) {
  }
};

template<>
struct InstanceWatcher<librbd::MockTestImageCtx> {
  MOCK_METHOD2(notify_sync_request, void(const std::string, Context *));
  MOCK_METHOD1(cancel_sync_request, bool(const std::string &));
  MOCK_METHOD1(notify_sync_complete, void(const std::string &));
};

namespace image_sync {

template <>
class SyncPointCreateRequest<librbd::MockTestImageCtx> {
public:
  static SyncPointCreateRequest *s_instance;
  Context *on_finish;

  static SyncPointCreateRequest* create(librbd::MockTestImageCtx *remote_image_ctx,
                                        const std::string &mirror_uuid,
                                        image_sync::SyncPointHandler* sync_point_handler,
                                        Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  SyncPointCreateRequest() {
    s_instance = this;
  }
  MOCK_METHOD0(send, void());
};

template <>
class SyncPointPruneRequest<librbd::MockTestImageCtx> {
public:
  static SyncPointPruneRequest *s_instance;
  Context *on_finish;
  bool sync_complete;

  static SyncPointPruneRequest* create(librbd::MockTestImageCtx *remote_image_ctx,
                                       bool sync_complete,
                                       image_sync::SyncPointHandler* sync_point_handler,
                                       Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    s_instance->sync_complete = sync_complete;
    return s_instance;
  }

  SyncPointPruneRequest() {
    s_instance = this;
  }
  MOCK_METHOD0(send, void());
};

SyncPointCreateRequest<librbd::MockTestImageCtx>* SyncPointCreateRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
SyncPointPruneRequest<librbd::MockTestImageCtx>* SyncPointPruneRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace image_sync

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;
using ::testing::InvokeWithoutArgs;

class TestMockImageSync : public TestMockFixture {
public:
  typedef Threads<librbd::MockTestImageCtx> MockThreads;
  typedef ImageSync<librbd::MockTestImageCtx> MockImageSync;
  typedef InstanceWatcher<librbd::MockTestImageCtx> MockInstanceWatcher;
  typedef image_sync::SyncPointCreateRequest<librbd::MockTestImageCtx> MockSyncPointCreateRequest;
  typedef image_sync::SyncPointPruneRequest<librbd::MockTestImageCtx> MockSyncPointPruneRequest;
  typedef image_sync::MockSyncPointHandler MockSyncPointHandler;
  typedef librbd::DeepCopyRequest<librbd::MockTestImageCtx> MockImageCopyRequest;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));

    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
  }

  void expect_get_snap_id(librbd::MockTestImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, get_snap_id(_, _))
      .WillOnce(Return(123));
  }

  void expect_notify_sync_request(MockInstanceWatcher &mock_instance_watcher,
                                  const std::string &sync_id, int r) {
    EXPECT_CALL(mock_instance_watcher, notify_sync_request(sync_id, _))
      .WillOnce(Invoke([this, r](const std::string &, Context *on_sync_start) {
            m_threads->work_queue->queue(on_sync_start, r);
          }));
  }

  void expect_cancel_sync_request(MockInstanceWatcher &mock_instance_watcher,
                                  const std::string &sync_id, bool canceled) {
    EXPECT_CALL(mock_instance_watcher, cancel_sync_request(sync_id))
      .WillOnce(Return(canceled));
  }

  void expect_notify_sync_complete(MockInstanceWatcher &mock_instance_watcher,
                                   const std::string &sync_id) {
    EXPECT_CALL(mock_instance_watcher, notify_sync_complete(sync_id));
  }

  void expect_create_sync_point(librbd::MockTestImageCtx &mock_local_image_ctx,
                                MockSyncPointCreateRequest &mock_sync_point_create_request,
                                int r) {
    EXPECT_CALL(mock_sync_point_create_request, send())
      .WillOnce(Invoke([this, &mock_local_image_ctx, &mock_sync_point_create_request, r]() {
          if (r == 0) {
            mock_local_image_ctx.snap_ids[{cls::rbd::UserSnapshotNamespace(),
					   "snap1"}] = 123;
            m_sync_points.emplace_back(cls::rbd::UserSnapshotNamespace(),
                                       "snap1", "", boost::none);
          }
          m_threads->work_queue->queue(mock_sync_point_create_request.on_finish, r);
        }));
  }

  void expect_copy_image(MockImageCopyRequest &mock_image_copy_request, int r) {
    EXPECT_CALL(mock_image_copy_request, send())
      .WillOnce(Invoke([this, &mock_image_copy_request, r]() {
          m_threads->work_queue->queue(mock_image_copy_request.on_finish, r);
        }));
  }

  void expect_flush_sync_point(MockSyncPointHandler& mock_sync_point_handler,
                               int r) {
    EXPECT_CALL(mock_sync_point_handler, update_sync_points(_, _, false, _))
      .WillOnce(WithArg<3>(CompleteContext(r)));
  }

  void expect_prune_sync_point(MockSyncPointPruneRequest &mock_sync_point_prune_request,
                               bool sync_complete, int r) {
    EXPECT_CALL(mock_sync_point_prune_request, send())
      .WillOnce(Invoke([this, &mock_sync_point_prune_request, sync_complete, r]() {
          ASSERT_EQ(sync_complete, mock_sync_point_prune_request.sync_complete);
          if (r == 0 && !m_sync_points.empty()) {
            if (sync_complete) {
              m_sync_points.pop_front();
            } else {
              while (m_sync_points.size() > 1) {
                m_sync_points.pop_back();
              }
            }
          }
          m_threads->work_queue->queue(mock_sync_point_prune_request.on_finish, r);
        }));
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

  MockImageSync *create_request(MockThreads& mock_threads,
                                librbd::MockTestImageCtx &mock_remote_image_ctx,
                                librbd::MockTestImageCtx &mock_local_image_ctx,
                                MockSyncPointHandler& mock_sync_point_handler,
                                MockInstanceWatcher &mock_instance_watcher,
                                Context *ctx) {
    return new MockImageSync(&mock_threads, &mock_local_image_ctx,
                             &mock_remote_image_ctx,
                             "mirror-uuid", &mock_sync_point_handler,
                             &mock_instance_watcher, nullptr, ctx);
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::ImageCtx *m_local_image_ctx;

  image_sync::SyncPoints m_sync_points;
};

TEST_F(TestMockImageSync, SimpleSync) {
  MockThreads mock_threads(m_threads);
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;
  MockInstanceWatcher mock_instance_watcher;
  MockImageCopyRequest mock_image_copy_request;
  MockSyncPointCreateRequest mock_sync_point_create_request;
  MockSyncPointPruneRequest mock_sync_point_prune_request;

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_notify_sync_request(mock_instance_watcher, mock_local_image_ctx.id, 0);
  expect_create_sync_point(mock_local_image_ctx, mock_sync_point_create_request, 0);
  expect_get_snap_id(mock_remote_image_ctx);
  expect_copy_image(mock_image_copy_request, 0);
  expect_flush_sync_point(mock_sync_point_handler, 0);
  expect_prune_sync_point(mock_sync_point_prune_request, true, 0);
  expect_notify_sync_complete(mock_instance_watcher, mock_local_image_ctx.id);

  C_SaferCond ctx;
  MockImageSync *request = create_request(mock_threads, mock_remote_image_ctx,
                                          mock_local_image_ctx,
                                          mock_sync_point_handler,
                                          mock_instance_watcher, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSync, RestartSync) {
  MockThreads mock_threads(m_threads);
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;
  MockInstanceWatcher mock_instance_watcher;
  MockImageCopyRequest mock_image_copy_request;
  MockSyncPointCreateRequest mock_sync_point_create_request;
  MockSyncPointPruneRequest mock_sync_point_prune_request;

  m_sync_points = {{cls::rbd::UserSnapshotNamespace(), "snap1", "", boost::none},
                   {cls::rbd::UserSnapshotNamespace(), "snap2", "snap1", boost::none}};
  mock_local_image_ctx.snap_ids[{cls::rbd::UserSnapshotNamespace(), "snap1"}] = 123;
  mock_local_image_ctx.snap_ids[{cls::rbd::UserSnapshotNamespace(), "snap2"}] = 234;

  expect_test_features(mock_local_image_ctx);
  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_notify_sync_request(mock_instance_watcher, mock_local_image_ctx.id, 0);
  expect_prune_sync_point(mock_sync_point_prune_request, false, 0);
  expect_get_snap_id(mock_remote_image_ctx);
  expect_copy_image(mock_image_copy_request, 0);
  expect_flush_sync_point(mock_sync_point_handler, 0);
  expect_prune_sync_point(mock_sync_point_prune_request, true, 0);
  expect_notify_sync_complete(mock_instance_watcher, mock_local_image_ctx.id);

  C_SaferCond ctx;
  MockImageSync *request = create_request(mock_threads, mock_remote_image_ctx,
                                          mock_local_image_ctx,
                                          mock_sync_point_handler,
                                          mock_instance_watcher, &ctx);
  request->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageSync, CancelNotifySyncRequest) {
  MockThreads mock_threads(m_threads);
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;
  MockInstanceWatcher mock_instance_watcher;

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  Context *on_sync_start = nullptr;
  C_SaferCond notify_sync_ctx;
  EXPECT_CALL(mock_instance_watcher,
              notify_sync_request(mock_local_image_ctx.id, _))
    .WillOnce(Invoke([&on_sync_start, &notify_sync_ctx](
                         const std::string &, Context *ctx) {
                       on_sync_start = ctx;
                       notify_sync_ctx.complete(0);
                     }));
  EXPECT_CALL(mock_instance_watcher,
              cancel_sync_request(mock_local_image_ctx.id))
    .WillOnce(Invoke([&on_sync_start](const std::string &) {
          EXPECT_NE(nullptr, on_sync_start);
          on_sync_start->complete(-ECANCELED);
          return true;
        }));

  C_SaferCond ctx;
  MockImageSync *request = create_request(mock_threads, mock_remote_image_ctx,
                                          mock_local_image_ctx,
                                          mock_sync_point_handler,
                                          mock_instance_watcher, &ctx);
  request->get();
  request->send();

  // cancel the notify sync request once it starts
  ASSERT_EQ(0, notify_sync_ctx.wait());
  request->cancel();
  request->put();

  ASSERT_EQ(-ECANCELED, ctx.wait());
}

TEST_F(TestMockImageSync, CancelImageCopy) {
  MockThreads mock_threads(m_threads);
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;
  MockInstanceWatcher mock_instance_watcher;
  MockImageCopyRequest mock_image_copy_request;
  MockSyncPointCreateRequest mock_sync_point_create_request;
  MockSyncPointPruneRequest mock_sync_point_prune_request;

  m_sync_points = {{cls::rbd::UserSnapshotNamespace(), "snap1", "", boost::none}};
  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_notify_sync_request(mock_instance_watcher, mock_local_image_ctx.id, 0);
  expect_prune_sync_point(mock_sync_point_prune_request, false, 0);
  expect_get_snap_id(mock_remote_image_ctx);

  C_SaferCond image_copy_ctx;
  EXPECT_CALL(mock_image_copy_request, send())
    .WillOnce(Invoke([&image_copy_ctx]() {
        image_copy_ctx.complete(0);
      }));
  expect_cancel_sync_request(mock_instance_watcher, mock_local_image_ctx.id,
                             false);
  EXPECT_CALL(mock_image_copy_request, cancel());
  expect_notify_sync_complete(mock_instance_watcher, mock_local_image_ctx.id);

  C_SaferCond ctx;
  MockImageSync *request = create_request(mock_threads, mock_remote_image_ctx,
                                          mock_local_image_ctx,
                                          mock_sync_point_handler,
                                          mock_instance_watcher, &ctx);
  request->get();
  request->send();

  // cancel the image copy once it starts
  ASSERT_EQ(0, image_copy_ctx.wait());
  request->cancel();
  request->put();
  m_threads->work_queue->queue(mock_image_copy_request.on_finish, 0);

  ASSERT_EQ(-ECANCELED, ctx.wait());
}

TEST_F(TestMockImageSync, CancelAfterCopyImage) {
  MockThreads mock_threads(m_threads);
  librbd::MockTestImageCtx mock_remote_image_ctx(*m_remote_image_ctx);
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);
  MockSyncPointHandler mock_sync_point_handler;
  MockInstanceWatcher mock_instance_watcher;
  MockImageCopyRequest mock_image_copy_request;
  MockSyncPointCreateRequest mock_sync_point_create_request;
  MockSyncPointPruneRequest mock_sync_point_prune_request;

  C_SaferCond ctx;
  MockImageSync *request = create_request(mock_threads, mock_remote_image_ctx,
                                          mock_local_image_ctx,
                                          mock_sync_point_handler,
                                          mock_instance_watcher, &ctx);

  expect_get_snap_seqs(mock_sync_point_handler);
  expect_get_sync_points(mock_sync_point_handler);

  InSequence seq;
  expect_notify_sync_request(mock_instance_watcher, mock_local_image_ctx.id, 0);
  expect_create_sync_point(mock_local_image_ctx, mock_sync_point_create_request, 0);
  expect_get_snap_id(mock_remote_image_ctx);
  EXPECT_CALL(mock_image_copy_request, send())
    .WillOnce((DoAll(InvokeWithoutArgs([request]() {
	      request->cancel();
	    }),
	  Invoke([this, &mock_image_copy_request]() {
	      m_threads->work_queue->queue(mock_image_copy_request.on_finish, 0);
	    }))));
  expect_cancel_sync_request(mock_instance_watcher, mock_local_image_ctx.id,
                             false);
  EXPECT_CALL(mock_image_copy_request, cancel());
  expect_notify_sync_complete(mock_instance_watcher, mock_local_image_ctx.id);

  request->send();
  ASSERT_EQ(-ECANCELED, ctx.wait());
}

} // namespace mirror
} // namespace rbd
