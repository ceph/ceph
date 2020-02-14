// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/deep_copy/ImageCopyRequest.h"
#include "librbd/deep_copy/SnapshotCopyRequest.h"
#include "librbd/mirror/snapshot/CreateNonPrimaryRequest.h"
#include "librbd/mirror/snapshot/GetImageStateRequest.h"
#include "librbd/mirror/snapshot/UnlinkPeerRequest.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/ReplayerListener.h"
#include "tools/rbd_mirror/image_replayer/Utils.h"
#include "tools/rbd_mirror/image_replayer/snapshot/Replayer.h"
#include "tools/rbd_mirror/image_replayer/snapshot/StateBuilder.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/mock/MockContextWQ.h"
#include "test/rbd_mirror/mock/MockSafeTimer.h"

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
struct ImageCopyRequest<MockTestImageCtx> {
  uint64_t src_snap_id_start;
  uint64_t src_snap_id_end;
  uint64_t dst_snap_id_start;
  librbd::deep_copy::ObjectNumber object_number;
  librbd::SnapSeqs snap_seqs;

  static ImageCopyRequest* s_instance;
  static ImageCopyRequest* create(MockTestImageCtx *src_image_ctx,
                                  MockTestImageCtx *dst_image_ctx,
                                  librados::snap_t src_snap_id_start,
                                  librados::snap_t src_snap_id_end,
                                  librados::snap_t dst_snap_id_start,
                                  bool flatten,
                                  const ObjectNumber &object_number,
                                  const SnapSeqs &snap_seqs,
                                  ProgressContext *prog_ctx,
                                  Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->src_snap_id_start = src_snap_id_start;
    s_instance->src_snap_id_end = src_snap_id_end;
    s_instance->dst_snap_id_start = dst_snap_id_start;
    s_instance->object_number = object_number;
    s_instance->snap_seqs = snap_seqs;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context* on_finish = nullptr;

  ImageCopyRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template <>
struct SnapshotCopyRequest<MockTestImageCtx> {
  librados::snap_t src_snap_id_start;
  librados::snap_t src_snap_id_end;
  librados::snap_t dst_snap_id_start;
  SnapSeqs* snap_seqs = nullptr;

  static SnapshotCopyRequest* s_instance;
  static SnapshotCopyRequest* create(MockTestImageCtx *src_image_ctx,
                                     MockTestImageCtx *dst_image_ctx,
                                     librados::snap_t src_snap_id_start,
                                     librados::snap_t src_snap_id_end,
                                     librados::snap_t dst_snap_id_start,
                                     bool flatten,
                                     ::MockContextWQ *work_queue,
                                     SnapSeqs *snap_seqs,
                                     Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->src_snap_id_start = src_snap_id_start;
    s_instance->src_snap_id_end = src_snap_id_end;
    s_instance->dst_snap_id_start = dst_snap_id_start;
    s_instance->snap_seqs = snap_seqs;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context* on_finish = nullptr;

  SnapshotCopyRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

ImageCopyRequest<MockTestImageCtx>* ImageCopyRequest<MockTestImageCtx>::s_instance = nullptr;
SnapshotCopyRequest<MockTestImageCtx>* SnapshotCopyRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace deep_copy

namespace mirror {
namespace snapshot {

template <>
struct CreateNonPrimaryRequest<MockTestImageCtx> {
  bool demoted = false;
  std::string primary_mirror_uuid;
  uint64_t primary_snap_id;
  SnapSeqs snap_seqs;
  uint64_t* snap_id = nullptr;

  static CreateNonPrimaryRequest* s_instance;
  static CreateNonPrimaryRequest* create(MockTestImageCtx *image_ctx,
                                         bool demoted,
                                         const std::string &primary_mirror_uuid,
                                         uint64_t primary_snap_id,
                                         const SnapSeqs& snap_seqs,
                                         const ImageState &image_state,
                                         uint64_t *snap_id,
                                         Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->demoted = demoted;
    s_instance->primary_mirror_uuid = primary_mirror_uuid;
    s_instance->primary_snap_id = primary_snap_id;
    s_instance->snap_seqs = snap_seqs;
    s_instance->snap_id = snap_id;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context* on_finish = nullptr;

  CreateNonPrimaryRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template <>
struct GetImageStateRequest<MockTestImageCtx> {
  uint64_t snap_id = CEPH_NOSNAP;

  static GetImageStateRequest* s_instance;
  static GetImageStateRequest* create(MockTestImageCtx *image_ctx,
                                      uint64_t snap_id,
                                      ImageState *image_state,
                                      Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->snap_id = snap_id;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context* on_finish = nullptr;

  GetImageStateRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template <>
struct UnlinkPeerRequest<MockTestImageCtx> {
  uint64_t snap_id;
  std::string mirror_peer_uuid;

  static UnlinkPeerRequest* s_instance;
  static UnlinkPeerRequest*create (MockTestImageCtx *image_ctx,
                                   uint64_t snap_id,
                                   const std::string &mirror_peer_uuid,
                                   Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->snap_id = snap_id;
    s_instance->mirror_peer_uuid = mirror_peer_uuid;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  Context* on_finish = nullptr;

  UnlinkPeerRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

CreateNonPrimaryRequest<MockTestImageCtx>* CreateNonPrimaryRequest<MockTestImageCtx>::s_instance = nullptr;
GetImageStateRequest<MockTestImageCtx>* GetImageStateRequest<MockTestImageCtx>::s_instance = nullptr;
UnlinkPeerRequest<MockTestImageCtx>* UnlinkPeerRequest<MockTestImageCtx>::s_instance = nullptr;

} // namespace snapshot
} // namespace mirror
} // namespace librbd

namespace rbd {
namespace mirror {

template <>
struct Threads<librbd::MockTestImageCtx> {
  MockSafeTimer *timer;
  ceph::mutex &timer_lock;

  MockContextWQ *work_queue;

  Threads(Threads<librbd::ImageCtx>* threads)
    : timer(new MockSafeTimer()),
      timer_lock(threads->timer_lock),
      work_queue(new MockContextWQ()) {
  }
  ~Threads() {
    delete timer;
    delete work_queue;
  }
};

namespace {

struct MockReplayerListener : public image_replayer::ReplayerListener {
  MOCK_METHOD0(handle_notification, void());
};

} // anonymous namespace

namespace image_replayer {

template<>
struct CloseImageRequest<librbd::MockTestImageCtx> {
  static CloseImageRequest* s_instance;
  librbd::MockTestImageCtx **image_ctx = nullptr;
  Context *on_finish = nullptr;

  static CloseImageRequest* create(librbd::MockTestImageCtx **image_ctx,
                                   Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->image_ctx = image_ctx;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  CloseImageRequest() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }

  ~CloseImageRequest() {
    ceph_assert(s_instance == this);
    s_instance = nullptr;
  }

  MOCK_METHOD0(send, void());
};

CloseImageRequest<librbd::MockTestImageCtx>* CloseImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

namespace snapshot {

template<>
struct StateBuilder<librbd::MockTestImageCtx> {
  StateBuilder(librbd::MockTestImageCtx& local_image_ctx,
               librbd::MockTestImageCtx& remote_image_ctx)
    : local_image_ctx(&local_image_ctx),
      remote_image_ctx(&remote_image_ctx) {
  }

  librbd::MockTestImageCtx* local_image_ctx;
  librbd::MockTestImageCtx* remote_image_ctx;

  std::string remote_mirror_uuid = "remote mirror uuid";
};

} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

#include "tools/rbd_mirror/image_replayer/snapshot/Replayer.cc"

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace snapshot {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockImageReplayerSnapshotReplayer : public TestMockFixture {
public:
  typedef Replayer<librbd::MockTestImageCtx> MockReplayer;
  typedef StateBuilder<librbd::MockTestImageCtx> MockStateBuilder;
  typedef Threads<librbd::MockTestImageCtx> MockThreads;
  typedef CloseImageRequest<librbd::MockTestImageCtx> MockCloseImageRequest;
  typedef librbd::deep_copy::ImageCopyRequest<librbd::MockTestImageCtx> MockImageCopyRequest;
  typedef librbd::deep_copy::SnapshotCopyRequest<librbd::MockTestImageCtx> MockSnapshotCopyRequest;
  typedef librbd::mirror::snapshot::CreateNonPrimaryRequest<librbd::MockTestImageCtx> MockCreateNonPrimaryRequest;
  typedef librbd::mirror::snapshot::GetImageStateRequest<librbd::MockTestImageCtx> MockGetImageStateRequest;
  typedef librbd::mirror::snapshot::UnlinkPeerRequest<librbd::MockTestImageCtx> MockUnlinkPeerRequest;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));

    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name,
                              m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name,
                            &m_remote_image_ctx));
  }

  void expect_work_queue_repeatedly(MockThreads &mock_threads) {
    EXPECT_CALL(*mock_threads.work_queue, queue(_, _))
      .WillRepeatedly(Invoke([this](Context *ctx, int r) {
          m_threads->work_queue->queue(ctx, r);
        }));
  }

  void expect_add_event_after_repeatedly(MockThreads &mock_threads) {
    EXPECT_CALL(*mock_threads.timer, add_event_after(_, _))
      .WillRepeatedly(
        DoAll(Invoke([this](double seconds, Context *ctx) {
                       m_threads->timer->add_event_after(seconds, ctx);
                     }),
          ReturnArg<1>()));
    EXPECT_CALL(*mock_threads.timer, cancel_event(_))
      .WillRepeatedly(
        Invoke([this](Context *ctx) {
          return m_threads->timer->cancel_event(ctx);
        }));
  }

  void expect_register_update_watcher(librbd::MockTestImageCtx& mock_image_ctx,
                                      librbd::UpdateWatchCtx** update_watch_ctx,
                                      uint64_t watch_handle, int r) {
    EXPECT_CALL(*mock_image_ctx.state, register_update_watcher(_, _))
      .WillOnce(Invoke([update_watch_ctx, watch_handle, r]
                       (librbd::UpdateWatchCtx* ctx, uint64_t* handle) {
          if (r >= 0) {
            *update_watch_ctx = ctx;
            *handle = watch_handle;
          }
          return r;
        }));
  }

  void expect_unregister_update_watcher(librbd::MockTestImageCtx& mock_image_ctx,
                                        uint64_t watch_handle, int r) {
    EXPECT_CALL(*mock_image_ctx.state, unregister_update_watcher(watch_handle, _))
      .WillOnce(WithArg<1>(Invoke([this, r](Context* ctx) {
          m_threads->work_queue->queue(ctx, r);
        })));
  }

  void expect_is_refresh_required(librbd::MockTestImageCtx& mock_image_ctx,
                                  bool is_required) {
    EXPECT_CALL(*mock_image_ctx.state, is_refresh_required())
      .WillOnce(Return(is_required));
  }

  void expect_refresh(librbd::MockTestImageCtx& mock_image_ctx,
                      const std::map<uint64_t, librbd::SnapInfo>& snaps,
                      int r) {
    EXPECT_CALL(*mock_image_ctx.state, refresh(_))
      .WillOnce(Invoke([this, &mock_image_ctx, snaps, r](Context* ctx) {
        mock_image_ctx.snap_info = snaps;
        m_threads->work_queue->queue(ctx, r);
      }));
  }

  void expect_notify_update(librbd::MockTestImageCtx& mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, notify_update(_))
      .WillOnce(Invoke([this](Context* ctx) {
        m_threads->work_queue->queue(ctx, 0);
      }));
  }

  void expect_snapshot_copy(MockSnapshotCopyRequest& mock_snapshot_copy_request,
                            uint64_t src_snap_id_start,
                            uint64_t src_snap_id_end,
                            uint64_t dst_snap_id_start,
                            const librbd::SnapSeqs& snap_seqs, int r) {
    EXPECT_CALL(mock_snapshot_copy_request, send())
      .WillOnce(Invoke([this, &req=mock_snapshot_copy_request,
                        src_snap_id_start, src_snap_id_end, dst_snap_id_start,
                        snap_seqs, r]() {
        ASSERT_EQ(src_snap_id_start, req.src_snap_id_start);
        ASSERT_EQ(src_snap_id_end, req.src_snap_id_end);
        ASSERT_EQ(dst_snap_id_start, req.dst_snap_id_start);
        *req.snap_seqs = snap_seqs;
        m_threads->work_queue->queue(req.on_finish, r);
      }));
  }

  void expect_get_image_state(MockGetImageStateRequest& mock_get_image_state_request,
                              uint64_t snap_id, int r) {
    EXPECT_CALL(mock_get_image_state_request, send())
      .WillOnce(Invoke([this, &req=mock_get_image_state_request, snap_id, r]() {
        ASSERT_EQ(snap_id, req.snap_id);
        m_threads->work_queue->queue(req.on_finish, r);
      }));
  }

  void expect_create_non_primary_request(MockCreateNonPrimaryRequest& mock_create_non_primary_request,
                                         bool demoted,
                                         const std::string& primary_mirror_uuid,
                                         uint64_t primary_snap_id,
                                         const librbd::SnapSeqs& snap_seqs,
                                         uint64_t snap_id, int r) {
    EXPECT_CALL(mock_create_non_primary_request, send())
      .WillOnce(Invoke([this, &req=mock_create_non_primary_request, demoted,
                        primary_mirror_uuid, primary_snap_id, snap_seqs,
                        snap_id, r]() {
        ASSERT_EQ(demoted, req.demoted);
        ASSERT_EQ(primary_mirror_uuid, req.primary_mirror_uuid);
        ASSERT_EQ(primary_snap_id, req.primary_snap_id);
        ASSERT_EQ(snap_seqs, req.snap_seqs);
        *req.snap_id = snap_id;
        m_threads->work_queue->queue(req.on_finish, r);
      }));
  }

  void expect_image_copy(MockImageCopyRequest& mock_image_copy_request,
                         uint64_t src_snap_id_start, uint64_t src_snap_id_end,
                         uint64_t dst_snap_id_start,
                         const librbd::deep_copy::ObjectNumber& object_number,
                         const librbd::SnapSeqs& snap_seqs, int r) {
    EXPECT_CALL(mock_image_copy_request, send())
      .WillOnce(Invoke([this, &req=mock_image_copy_request, src_snap_id_start,
                        src_snap_id_end, dst_snap_id_start, object_number,
                        snap_seqs, r]() {
        ASSERT_EQ(src_snap_id_start, req.src_snap_id_start);
        ASSERT_EQ(src_snap_id_end, req.src_snap_id_end);
        ASSERT_EQ(dst_snap_id_start, req.dst_snap_id_start);
        ASSERT_EQ(object_number, req.object_number);
        ASSERT_EQ(snap_seqs, req.snap_seqs);
        m_threads->work_queue->queue(req.on_finish, r);
      }));
  }

  void expect_unlink_peer(MockUnlinkPeerRequest& mock_unlink_peer_request,
                          uint64_t snap_id, const std::string& mirror_peer_uuid,
                          int r) {
    EXPECT_CALL(mock_unlink_peer_request, send())
      .WillOnce(Invoke([this, &req=mock_unlink_peer_request, snap_id,
                        mirror_peer_uuid, r]() {
        ASSERT_EQ(snap_id, req.snap_id);
        ASSERT_EQ(mirror_peer_uuid, req.mirror_peer_uuid);
        m_threads->work_queue->queue(req.on_finish, r);
      }));
  }

  void expect_mirror_image_snapshot_set_copy_progress(
      librbd::MockTestImageCtx& mock_test_image_ctx, uint64_t snap_id,
      bool completed, uint64_t last_copied_object, int r) {
    bufferlist bl;
    encode(snap_id, bl);
    encode(completed, bl);
    encode(last_copied_object, bl);

    EXPECT_CALL(get_mock_io_ctx(mock_test_image_ctx.md_ctx),
                exec(mock_test_image_ctx.header_oid, _, StrEq("rbd"),
                     StrEq("mirror_image_snapshot_set_copy_progress"),
                     ContentsEqual(bl), _, _))
      .WillOnce(Return(r));
  }

  void expect_send(MockCloseImageRequest &mock_close_image_request, int r) {
    EXPECT_CALL(mock_close_image_request, send())
      .WillOnce(Invoke([this, &mock_close_image_request, r]() {
            *mock_close_image_request.image_ctx = nullptr;
            m_threads->work_queue->queue(mock_close_image_request.on_finish, r);
          }));
  }

  void expect_notification(MockThreads& mock_threads,
                           MockReplayerListener& mock_replayer_listener) {
    EXPECT_CALL(mock_replayer_listener, handle_notification())
      .WillRepeatedly(Invoke([this]() {
          std::unique_lock locker{m_lock};
          ++m_notifications;
          m_cond.notify_all();
        }));
  }

  int wait_for_notification(uint32_t count) {
    std::unique_lock locker{m_lock};
    for (uint32_t idx = 0; idx < count; ++idx) {
      while (m_notifications == 0) {
        if (m_cond.wait_for(locker, 10s) == std::cv_status::timeout) {
          return -ETIMEDOUT;
        }
      }
      --m_notifications;
    }
    return 0;
  }

  int init_entry_replayer(MockReplayer& mock_replayer,
                          MockThreads& mock_threads,
                          librbd::MockTestImageCtx& mock_local_image_ctx,
                          librbd::MockTestImageCtx& mock_remote_image_ctx,
                          MockReplayerListener& mock_replayer_listener,
                          librbd::UpdateWatchCtx** update_watch_ctx) {
    expect_register_update_watcher(mock_remote_image_ctx, update_watch_ctx, 123,
                                   0);
    expect_is_refresh_required(mock_local_image_ctx, false);
    expect_is_refresh_required(mock_remote_image_ctx, false);

    C_SaferCond init_ctx;
    mock_replayer.init(&init_ctx);
    int r = init_ctx.wait();
    if (r < 0) {
      return r;
    }

    return wait_for_notification(2);
  }

  int shut_down_entry_replayer(MockReplayer& mock_replayer,
                               MockThreads& mock_threads,
                               librbd::MockTestImageCtx& mock_remote_image_ctx) {
    expect_unregister_update_watcher(mock_remote_image_ctx, 123, 0);

    C_SaferCond shutdown_ctx;
    mock_replayer.shut_down(&shutdown_ctx);
    return shutdown_ctx.wait();
  }

  librbd::ImageCtx* m_local_image_ctx = nullptr;
  librbd::ImageCtx* m_remote_image_ctx = nullptr;

  PoolMetaCache m_pool_meta_cache{g_ceph_context};

  ceph::mutex m_lock = ceph::make_mutex(
    "TestMockImageReplayerSnapshotReplayer");
  ceph::condition_variable m_cond;
  uint32_t m_notifications = 0;
};

TEST_F(TestMockImageReplayerSnapshotReplayer, InitShutDown) {
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx};
  librbd::MockTestImageCtx mock_remote_image_ctx{*m_remote_image_ctx};

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);

  MockReplayerListener mock_replayer_listener;
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_image_ctx);
  MockReplayer mock_replayer{&mock_threads, "local mirror uuid",
                             &m_pool_meta_cache, &mock_state_builder,
                             &mock_replayer_listener};
  m_pool_meta_cache.set_remote_pool_meta(
    m_remote_io_ctx.get_id(),
    {"remote mirror uuid", "remote mirror peer uuid"});

  librbd::UpdateWatchCtx* update_watch_ctx = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_local_image_ctx,
                                   mock_remote_image_ctx,
                                   mock_replayer_listener,
                                   &update_watch_ctx));
  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_remote_image_ctx));
}

TEST_F(TestMockImageReplayerSnapshotReplayer, SyncSnapshot) {
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx};
  librbd::MockTestImageCtx mock_remote_image_ctx{*m_remote_image_ctx};

  // it should sync two snapshots and skip two (user and mirror w/o matching
  // peer uuid)
  mock_remote_image_ctx.snap_info = {
    {1U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"remote mirror peer uuid"},
       "", 0U},
     0, {}, 0, 0, {}}},
    {2U, librbd::SnapInfo{"snap2", cls::rbd::UserSnapshotNamespace{},
     0, {}, 0, 0, {}}},
    {3U, librbd::SnapInfo{"snap3", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {""},
       "", 0U},
     0, {}, 0, 0, {}}},
    {4U, librbd::SnapInfo{"snap4", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"remote mirror peer uuid"},
       "", 0U},
     0, {}, 0, 0, {}}}};

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);

  MockReplayerListener mock_replayer_listener;
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_image_ctx);
  MockReplayer mock_replayer{&mock_threads, "local mirror uuid",
                             &m_pool_meta_cache, &mock_state_builder,
                             &mock_replayer_listener};
  m_pool_meta_cache.set_remote_pool_meta(
    m_remote_io_ctx.get_id(),
    {"remote mirror uuid", "remote mirror peer uuid"});

  librbd::UpdateWatchCtx* update_watch_ctx = nullptr;

  // init
  expect_register_update_watcher(mock_remote_image_ctx, &update_watch_ctx, 123,
                                 0);

  // sync snap1
  expect_is_refresh_required(mock_local_image_ctx, false);
  expect_is_refresh_required(mock_remote_image_ctx, false);
  MockSnapshotCopyRequest mock_snapshot_copy_request;
  expect_snapshot_copy(mock_snapshot_copy_request, 0, 1, 0, {{1, CEPH_NOSNAP}},
                       0);
  MockGetImageStateRequest mock_get_image_state_request;
  expect_get_image_state(mock_get_image_state_request, 1, 0);
  MockCreateNonPrimaryRequest mock_create_non_primary_request;
  expect_create_non_primary_request(mock_create_non_primary_request,
                                    false, "remote mirror uuid", 1,
                                    {{1, CEPH_NOSNAP}}, 11, 0);
  MockImageCopyRequest mock_image_copy_request;
  expect_image_copy(mock_image_copy_request, 0, 1, 0, {},
                    {{1, CEPH_NOSNAP}}, 0);
  expect_mirror_image_snapshot_set_copy_progress(
    mock_local_image_ctx, 11, true, 0, 0);
  expect_notify_update(mock_local_image_ctx);

  // sync snap4
  expect_is_refresh_required(mock_local_image_ctx, true);
  expect_refresh(
    mock_local_image_ctx, {
      {11U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
         cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote mirror uuid",
         1, true, 0, {}},
       0, {}, 0, 0, {}}},
    }, 0);
  expect_is_refresh_required(mock_remote_image_ctx, false);
  expect_snapshot_copy(mock_snapshot_copy_request, 1, 4, 11,
                       {{1, 11}, {4, CEPH_NOSNAP}}, 0);
  expect_get_image_state(mock_get_image_state_request, 4, 0);
  expect_create_non_primary_request(mock_create_non_primary_request,
                                    false, "remote mirror uuid", 4,
                                    {{1, 11}, {4, CEPH_NOSNAP}}, 14, 0);
  expect_image_copy(mock_image_copy_request, 1, 4, 11, {},
                    {{1, 11}, {4, CEPH_NOSNAP}}, 0);
  MockUnlinkPeerRequest mock_unlink_peer_request;
  expect_mirror_image_snapshot_set_copy_progress(
    mock_local_image_ctx, 14, true, 0, 0);
  expect_notify_update(mock_local_image_ctx);
  expect_unlink_peer(mock_unlink_peer_request, 1, "remote mirror peer uuid",
                     0);

  // idle
  expect_is_refresh_required(mock_local_image_ctx, true);
  expect_refresh(
    mock_local_image_ctx, {
      {11U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
         cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote mirror uuid",
         1, true, 0, {}},
       0, {}, 0, 0, {}}},
      {14U, librbd::SnapInfo{"snap4", cls::rbd::MirrorSnapshotNamespace{
         cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote mirror uuid",
         4, true, 0, {}},
       0, {}, 0, 0, {}}},
    }, 0);
  expect_is_refresh_required(mock_remote_image_ctx, false);

  // fire init
  C_SaferCond init_ctx;
  mock_replayer.init(&init_ctx);
  ASSERT_EQ(0, init_ctx.wait());

  // wait for sync to complete
  ASSERT_EQ(0, wait_for_notification(4));

  // shut down
  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_remote_image_ctx));
}

TEST_F(TestMockImageReplayerSnapshotReplayer, InterruptedSync) {
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx};
  librbd::MockTestImageCtx mock_remote_image_ctx{*m_remote_image_ctx};

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);

  MockReplayerListener mock_replayer_listener;
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_image_ctx);
  MockReplayer mock_replayer{&mock_threads, "local mirror uuid",
                             &m_pool_meta_cache, &mock_state_builder,
                             &mock_replayer_listener};
  m_pool_meta_cache.set_remote_pool_meta(
    m_remote_io_ctx.get_id(),
    {"remote mirror uuid", "remote mirror peer uuid"});

  librbd::UpdateWatchCtx* update_watch_ctx = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_local_image_ctx,
                                   mock_remote_image_ctx,
                                   mock_replayer_listener,
                                   &update_watch_ctx));

  // inject a incomplete sync snapshot
  mock_remote_image_ctx.snap_info = {
    {1U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"remote mirror peer uuid"},
       "", 0U},
     0, {}, 0, 0, {}}}};
  mock_local_image_ctx.snap_info = {
    {11U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote mirror uuid",
       1, false, 123, {{1, CEPH_NOSNAP}}},
     0, {}, 0, 0, {}}}};

  // re-sync snap1
  expect_is_refresh_required(mock_local_image_ctx, false);
  expect_is_refresh_required(mock_remote_image_ctx, false);
  MockImageCopyRequest mock_image_copy_request;
  expect_image_copy(mock_image_copy_request, 0, 1, 0,
                    librbd::deep_copy::ObjectNumber{123U},
                    {{1, CEPH_NOSNAP}}, 0);
  expect_mirror_image_snapshot_set_copy_progress(
    mock_local_image_ctx, 11, true, 123, 0);
  expect_notify_update(mock_local_image_ctx);

  // idle
  expect_is_refresh_required(mock_local_image_ctx, true);
  expect_refresh(
    mock_local_image_ctx, {
      {11U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
         cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote mirror uuid",
         1, true, 0, {}},
       0, {}, 0, 0, {}}},
    }, 0);
  expect_is_refresh_required(mock_remote_image_ctx, false);

  // wake-up replayer
  update_watch_ctx->handle_notify();

  // wait for sync to complete
  ASSERT_EQ(0, wait_for_notification(2));

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_remote_image_ctx));
}

TEST_F(TestMockImageReplayerSnapshotReplayer, RemoteImageDemoted) {
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx};
  librbd::MockTestImageCtx mock_remote_image_ctx{*m_remote_image_ctx};

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);

  MockReplayerListener mock_replayer_listener;
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_image_ctx);
  MockReplayer mock_replayer{&mock_threads, "local mirror uuid",
                             &m_pool_meta_cache, &mock_state_builder,
                             &mock_replayer_listener};
  m_pool_meta_cache.set_remote_pool_meta(
    m_remote_io_ctx.get_id(),
    {"remote mirror uuid", "remote mirror peer uuid"});

  librbd::UpdateWatchCtx* update_watch_ctx = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_local_image_ctx,
                                   mock_remote_image_ctx,
                                   mock_replayer_listener,
                                   &update_watch_ctx));

  // inject a demotion snapshot
  mock_remote_image_ctx.snap_info = {
    {1U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED,
       {"remote mirror peer uuid"}, "", 0U},
     0, {}, 0, 0, {}}}};

  // sync snap1
  expect_is_refresh_required(mock_local_image_ctx, false);
  expect_is_refresh_required(mock_remote_image_ctx, false);
  MockSnapshotCopyRequest mock_snapshot_copy_request;
  expect_snapshot_copy(mock_snapshot_copy_request, 0, 1, 0, {{1, CEPH_NOSNAP}},
                       0);
  MockGetImageStateRequest mock_get_image_state_request;
  expect_get_image_state(mock_get_image_state_request, 1, 0);
  MockCreateNonPrimaryRequest mock_create_non_primary_request;
  expect_create_non_primary_request(mock_create_non_primary_request,
                                    true, "remote mirror uuid", 1,
                                    {{1, CEPH_NOSNAP}}, 11, 0);
  MockImageCopyRequest mock_image_copy_request;
  expect_image_copy(mock_image_copy_request, 0, 1, 0, {},
                    {{1, CEPH_NOSNAP}}, 0);
  expect_mirror_image_snapshot_set_copy_progress(
    mock_local_image_ctx, 11, true, 0, 0);
  expect_notify_update(mock_local_image_ctx);

  // idle
  expect_is_refresh_required(mock_local_image_ctx, true);
  expect_refresh(
    mock_local_image_ctx, {
      {11U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
         cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote mirror uuid",
         1, true, 0, {}},
       0, {}, 0, 0, {}}},
    }, 0);
  expect_is_refresh_required(mock_remote_image_ctx, false);

  // wake-up replayer
  update_watch_ctx->handle_notify();

  // wait for sync to complete and expect replay complete
  ASSERT_EQ(0, wait_for_notification(2));
  ASSERT_FALSE(mock_replayer.is_replaying());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_remote_image_ctx));
}

TEST_F(TestMockImageReplayerSnapshotReplayer, LocalImagePromoted) {
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx};
  librbd::MockTestImageCtx mock_remote_image_ctx{*m_remote_image_ctx};

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);

  MockReplayerListener mock_replayer_listener;
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_image_ctx);
  MockReplayer mock_replayer{&mock_threads, "local mirror uuid",
                             &m_pool_meta_cache, &mock_state_builder,
                             &mock_replayer_listener};
  m_pool_meta_cache.set_remote_pool_meta(
    m_remote_io_ctx.get_id(),
    {"remote mirror uuid", "remote mirror peer uuid"});

  librbd::UpdateWatchCtx* update_watch_ctx = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_local_image_ctx,
                                   mock_remote_image_ctx,
                                   mock_replayer_listener,
                                   &update_watch_ctx));

  // inject a promotion snapshot
  mock_local_image_ctx.snap_info = {
    {1U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY,
       {"remote mirror peer uuid"}, "", 0U, true, 0, {}},
     0, {}, 0, 0, {}}}};

  // idle
  expect_is_refresh_required(mock_local_image_ctx, false);
  expect_is_refresh_required(mock_remote_image_ctx, false);

  // wake-up replayer
  update_watch_ctx->handle_notify();

  // wait for sync to complete and expect replay complete
  ASSERT_EQ(0, wait_for_notification(1));
  ASSERT_FALSE(mock_replayer.is_replaying());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_remote_image_ctx));
}

TEST_F(TestMockImageReplayerSnapshotReplayer, RegisterUpdateWatcherError) {
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx};
  librbd::MockTestImageCtx mock_remote_image_ctx{*m_remote_image_ctx};

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_image_ctx);
  MockReplayerListener mock_replayer_listener;
  MockReplayer mock_replayer{&mock_threads, "local mirror uuid",
                             &m_pool_meta_cache, &mock_state_builder,
                             &mock_replayer_listener};
  m_pool_meta_cache.set_remote_pool_meta(
    m_remote_io_ctx.get_id(),
    {"remote mirror uuid", "remote mirror peer uuid"});

  // init
  librbd::UpdateWatchCtx* update_watch_ctx = nullptr;
  expect_register_update_watcher(mock_remote_image_ctx, &update_watch_ctx, 123,
                                 -EINVAL);

  // fire init
  C_SaferCond init_ctx;
  mock_replayer.init(&init_ctx);
  ASSERT_EQ(-EINVAL, init_ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotReplayer, UnregisterUpdateWatcherError) {
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx};
  librbd::MockTestImageCtx mock_remote_image_ctx{*m_remote_image_ctx};

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);

  MockReplayerListener mock_replayer_listener;
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_image_ctx);
  MockReplayer mock_replayer{&mock_threads, "local mirror uuid",
                             &m_pool_meta_cache, &mock_state_builder,
                             &mock_replayer_listener};
  m_pool_meta_cache.set_remote_pool_meta(
    m_remote_io_ctx.get_id(),
    {"remote mirror uuid", "remote mirror peer uuid"});

  librbd::UpdateWatchCtx* update_watch_ctx = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_local_image_ctx,
                                   mock_remote_image_ctx,
                                   mock_replayer_listener,
                                   &update_watch_ctx));


  // shut down
  expect_unregister_update_watcher(mock_remote_image_ctx, 123, -EINVAL);

  C_SaferCond shutdown_ctx;
  mock_replayer.shut_down(&shutdown_ctx);
  ASSERT_EQ(-EINVAL, shutdown_ctx.wait());
}

TEST_F(TestMockImageReplayerSnapshotReplayer, RefreshLocalImageError) {
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx};
  librbd::MockTestImageCtx mock_remote_image_ctx{*m_remote_image_ctx};

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);

  MockReplayerListener mock_replayer_listener;
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_image_ctx);
  MockReplayer mock_replayer{&mock_threads, "local mirror uuid",
                             &m_pool_meta_cache, &mock_state_builder,
                             &mock_replayer_listener};
  m_pool_meta_cache.set_remote_pool_meta(
    m_remote_io_ctx.get_id(),
    {"remote mirror uuid", "remote mirror peer uuid"});

  librbd::UpdateWatchCtx* update_watch_ctx = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_local_image_ctx,
                                   mock_remote_image_ctx,
                                   mock_replayer_listener,
                                   &update_watch_ctx));

  // sync
  expect_is_refresh_required(mock_local_image_ctx, true);
  expect_refresh(mock_local_image_ctx, {}, -EINVAL);

  // wake-up replayer
  update_watch_ctx->handle_notify();

  // wait for sync to complete and expect replay complete
  ASSERT_EQ(0, wait_for_notification(1));
  ASSERT_FALSE(mock_replayer.is_replaying());
  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_remote_image_ctx));
}

TEST_F(TestMockImageReplayerSnapshotReplayer, RefreshRemoteImageError) {
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx};
  librbd::MockTestImageCtx mock_remote_image_ctx{*m_remote_image_ctx};

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);

  MockReplayerListener mock_replayer_listener;
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_image_ctx);
  MockReplayer mock_replayer{&mock_threads, "local mirror uuid",
                             &m_pool_meta_cache, &mock_state_builder,
                             &mock_replayer_listener};
  m_pool_meta_cache.set_remote_pool_meta(
    m_remote_io_ctx.get_id(),
    {"remote mirror uuid", "remote mirror peer uuid"});

  librbd::UpdateWatchCtx* update_watch_ctx = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_local_image_ctx,
                                   mock_remote_image_ctx,
                                   mock_replayer_listener,
                                   &update_watch_ctx));

  // sync
  expect_is_refresh_required(mock_local_image_ctx, false);
  expect_is_refresh_required(mock_remote_image_ctx, true);
  expect_refresh(mock_remote_image_ctx, {}, -EINVAL);

  // wake-up replayer
  update_watch_ctx->handle_notify();

  // wait for sync to complete and expect replay complete
  ASSERT_EQ(0, wait_for_notification(1));
  ASSERT_FALSE(mock_replayer.is_replaying());
  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_remote_image_ctx));
}

TEST_F(TestMockImageReplayerSnapshotReplayer, CopySnapshotsError) {
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx};
  librbd::MockTestImageCtx mock_remote_image_ctx{*m_remote_image_ctx};

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);

  MockReplayerListener mock_replayer_listener;
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_image_ctx);
  MockReplayer mock_replayer{&mock_threads, "local mirror uuid",
                             &m_pool_meta_cache, &mock_state_builder,
                             &mock_replayer_listener};
  m_pool_meta_cache.set_remote_pool_meta(
    m_remote_io_ctx.get_id(),
    {"remote mirror uuid", "remote mirror peer uuid"});

  librbd::UpdateWatchCtx* update_watch_ctx = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_local_image_ctx,
                                   mock_remote_image_ctx,
                                   mock_replayer_listener,
                                   &update_watch_ctx));

  // inject snapshot
  mock_remote_image_ctx.snap_info = {
    {1U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"remote mirror peer uuid"}, "",
       0U},
     0, {}, 0, 0, {}}}};

  // sync snap1
  expect_is_refresh_required(mock_local_image_ctx, false);
  expect_is_refresh_required(mock_remote_image_ctx, false);
  MockSnapshotCopyRequest mock_snapshot_copy_request;
  expect_snapshot_copy(mock_snapshot_copy_request, 0, 1, 0, {{1, CEPH_NOSNAP}},
                       -EINVAL);

  // wake-up replayer
  update_watch_ctx->handle_notify();

  // wait for sync to complete and expect replay complete
  ASSERT_EQ(0, wait_for_notification(1));
  ASSERT_FALSE(mock_replayer.is_replaying());
  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_remote_image_ctx));
}

TEST_F(TestMockImageReplayerSnapshotReplayer, GetImageStateError) {
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx};
  librbd::MockTestImageCtx mock_remote_image_ctx{*m_remote_image_ctx};

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);

  MockReplayerListener mock_replayer_listener;
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_image_ctx);
  MockReplayer mock_replayer{&mock_threads, "local mirror uuid",
                             &m_pool_meta_cache, &mock_state_builder,
                             &mock_replayer_listener};
  m_pool_meta_cache.set_remote_pool_meta(
    m_remote_io_ctx.get_id(),
    {"remote mirror uuid", "remote mirror peer uuid"});

  librbd::UpdateWatchCtx* update_watch_ctx = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_local_image_ctx,
                                   mock_remote_image_ctx,
                                   mock_replayer_listener,
                                   &update_watch_ctx));

  // inject snapshot
  mock_remote_image_ctx.snap_info = {
    {1U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"remote mirror peer uuid"}, "",
       0U},
     0, {}, 0, 0, {}}}};

  // sync snap1
  expect_is_refresh_required(mock_local_image_ctx, false);
  expect_is_refresh_required(mock_remote_image_ctx, false);
  MockSnapshotCopyRequest mock_snapshot_copy_request;
  expect_snapshot_copy(mock_snapshot_copy_request, 0, 1, 0, {{1, CEPH_NOSNAP}},
                       0);
  MockGetImageStateRequest mock_get_image_state_request;
  expect_get_image_state(mock_get_image_state_request, 1, -EINVAL);

  // wake-up replayer
  update_watch_ctx->handle_notify();

  // wait for sync to complete and expect replay complete
  ASSERT_EQ(0, wait_for_notification(1));
  ASSERT_FALSE(mock_replayer.is_replaying());
  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_remote_image_ctx));
}

TEST_F(TestMockImageReplayerSnapshotReplayer, CreateNonPrimarySnapshotError) {
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx};
  librbd::MockTestImageCtx mock_remote_image_ctx{*m_remote_image_ctx};

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);

  MockReplayerListener mock_replayer_listener;
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_image_ctx);
  MockReplayer mock_replayer{&mock_threads, "local mirror uuid",
                             &m_pool_meta_cache, &mock_state_builder,
                             &mock_replayer_listener};
  m_pool_meta_cache.set_remote_pool_meta(
    m_remote_io_ctx.get_id(),
    {"remote mirror uuid", "remote mirror peer uuid"});

  librbd::UpdateWatchCtx* update_watch_ctx = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_local_image_ctx,
                                   mock_remote_image_ctx,
                                   mock_replayer_listener,
                                   &update_watch_ctx));

  // inject snapshot
  mock_remote_image_ctx.snap_info = {
    {1U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"remote mirror peer uuid"}, "",
       0U},
     0, {}, 0, 0, {}}}};

  // sync snap1
  expect_is_refresh_required(mock_local_image_ctx, false);
  expect_is_refresh_required(mock_remote_image_ctx, false);
  MockSnapshotCopyRequest mock_snapshot_copy_request;
  expect_snapshot_copy(mock_snapshot_copy_request, 0, 1, 0, {{1, CEPH_NOSNAP}},
                       0);
  MockGetImageStateRequest mock_get_image_state_request;
  expect_get_image_state(mock_get_image_state_request, 1, 0);
  MockCreateNonPrimaryRequest mock_create_non_primary_request;
  expect_create_non_primary_request(mock_create_non_primary_request,
                                    false, "remote mirror uuid", 1,
                                    {{1, CEPH_NOSNAP}}, 11, -EINVAL);

  // wake-up replayer
  update_watch_ctx->handle_notify();

  // wait for sync to complete and expect replay complete
  ASSERT_EQ(0, wait_for_notification(1));
  ASSERT_FALSE(mock_replayer.is_replaying());
  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_remote_image_ctx));
}

TEST_F(TestMockImageReplayerSnapshotReplayer, CopyImageError) {
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx};
  librbd::MockTestImageCtx mock_remote_image_ctx{*m_remote_image_ctx};

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);

  MockReplayerListener mock_replayer_listener;
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_image_ctx);
  MockReplayer mock_replayer{&mock_threads, "local mirror uuid",
                             &m_pool_meta_cache, &mock_state_builder,
                             &mock_replayer_listener};
  m_pool_meta_cache.set_remote_pool_meta(
    m_remote_io_ctx.get_id(),
    {"remote mirror uuid", "remote mirror peer uuid"});

  librbd::UpdateWatchCtx* update_watch_ctx = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_local_image_ctx,
                                   mock_remote_image_ctx,
                                   mock_replayer_listener,
                                   &update_watch_ctx));

  // inject snapshot
  mock_remote_image_ctx.snap_info = {
    {1U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"remote mirror peer uuid"}, "",
       0U},
     0, {}, 0, 0, {}}}};

  // sync snap1
  expect_is_refresh_required(mock_local_image_ctx, false);
  expect_is_refresh_required(mock_remote_image_ctx, false);
  MockSnapshotCopyRequest mock_snapshot_copy_request;
  expect_snapshot_copy(mock_snapshot_copy_request, 0, 1, 0, {{1, CEPH_NOSNAP}},
                       0);
  MockGetImageStateRequest mock_get_image_state_request;
  expect_get_image_state(mock_get_image_state_request, 1, 0);
  MockCreateNonPrimaryRequest mock_create_non_primary_request;
  expect_create_non_primary_request(mock_create_non_primary_request,
                                    false, "remote mirror uuid", 1,
                                    {{1, CEPH_NOSNAP}}, 11, 0);
  MockImageCopyRequest mock_image_copy_request;
  expect_image_copy(mock_image_copy_request, 0, 1, 0, {},
                    {{1, CEPH_NOSNAP}}, -EINVAL);

  // wake-up replayer
  update_watch_ctx->handle_notify();

  // wait for sync to complete and expect replay complete
  ASSERT_EQ(0, wait_for_notification(1));
  ASSERT_FALSE(mock_replayer.is_replaying());
  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_remote_image_ctx));
}

TEST_F(TestMockImageReplayerSnapshotReplayer, UpdateNonPrimarySnapshotError) {
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx};
  librbd::MockTestImageCtx mock_remote_image_ctx{*m_remote_image_ctx};

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);

  MockReplayerListener mock_replayer_listener;
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_image_ctx);
  MockReplayer mock_replayer{&mock_threads, "local mirror uuid",
                             &m_pool_meta_cache, &mock_state_builder,
                             &mock_replayer_listener};
  m_pool_meta_cache.set_remote_pool_meta(
    m_remote_io_ctx.get_id(),
    {"remote mirror uuid", "remote mirror peer uuid"});

  librbd::UpdateWatchCtx* update_watch_ctx = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_local_image_ctx,
                                   mock_remote_image_ctx,
                                   mock_replayer_listener,
                                   &update_watch_ctx));

  // inject snapshot
  mock_remote_image_ctx.snap_info = {
    {1U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"remote mirror peer uuid"}, "",
       0U},
     0, {}, 0, 0, {}}}};

  // sync snap1
  expect_is_refresh_required(mock_local_image_ctx, false);
  expect_is_refresh_required(mock_remote_image_ctx, false);
  MockSnapshotCopyRequest mock_snapshot_copy_request;
  expect_snapshot_copy(mock_snapshot_copy_request, 0, 1, 0, {{1, CEPH_NOSNAP}},
                       0);
  MockGetImageStateRequest mock_get_image_state_request;
  expect_get_image_state(mock_get_image_state_request, 1, 0);
  MockCreateNonPrimaryRequest mock_create_non_primary_request;
  expect_create_non_primary_request(mock_create_non_primary_request,
                                    false, "remote mirror uuid", 1,
                                    {{1, CEPH_NOSNAP}}, 11, 0);
  MockImageCopyRequest mock_image_copy_request;
  expect_image_copy(mock_image_copy_request, 0, 1, 0, {},
                    {{1, CEPH_NOSNAP}}, 0);
  expect_mirror_image_snapshot_set_copy_progress(
    mock_local_image_ctx, 11, true, 0, -EINVAL);

  // wake-up replayer
  update_watch_ctx->handle_notify();

  // wait for sync to complete and expect replay complete
  ASSERT_EQ(0, wait_for_notification(1));
  ASSERT_FALSE(mock_replayer.is_replaying());
  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_remote_image_ctx));
}

TEST_F(TestMockImageReplayerSnapshotReplayer, UnlinkPeerError) {
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx};
  librbd::MockTestImageCtx mock_remote_image_ctx{*m_remote_image_ctx};

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);

  MockReplayerListener mock_replayer_listener;
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_image_ctx);
  MockReplayer mock_replayer{&mock_threads, "local mirror uuid",
                             &m_pool_meta_cache, &mock_state_builder,
                             &mock_replayer_listener};
  m_pool_meta_cache.set_remote_pool_meta(
    m_remote_io_ctx.get_id(),
    {"remote mirror uuid", "remote mirror peer uuid"});

  librbd::UpdateWatchCtx* update_watch_ctx = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_local_image_ctx,
                                   mock_remote_image_ctx,
                                   mock_replayer_listener,
                                   &update_watch_ctx));

  // inject snapshot
  mock_remote_image_ctx.snap_info = {
    {1U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"remote mirror peer uuid"}, "",
       0U},
     0, {}, 0, 0, {}}},
    {2U, librbd::SnapInfo{"snap2", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"remote mirror peer uuid"},
       "", 0U},
     0, {}, 0, 0, {}}}};
  mock_local_image_ctx.snap_info = {
    {11U, librbd::SnapInfo{"snap1", cls::rbd::MirrorSnapshotNamespace{
       cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "remote mirror uuid",
       1, true, 0, {}},
     0, {}, 0, 0, {}}}};

  // sync snap2
  expect_is_refresh_required(mock_local_image_ctx, false);
  expect_is_refresh_required(mock_remote_image_ctx, false);
  MockSnapshotCopyRequest mock_snapshot_copy_request;
  expect_snapshot_copy(mock_snapshot_copy_request, 1, 2, 11, {{2, CEPH_NOSNAP}},
                       0);
  MockGetImageStateRequest mock_get_image_state_request;
  expect_get_image_state(mock_get_image_state_request, 2, 0);
  MockCreateNonPrimaryRequest mock_create_non_primary_request;
  expect_create_non_primary_request(mock_create_non_primary_request,
                                    false, "remote mirror uuid", 2,
                                    {{2, CEPH_NOSNAP}}, 12, 0);
  MockImageCopyRequest mock_image_copy_request;
  expect_image_copy(mock_image_copy_request, 1, 2, 11, {},
                    {{2, CEPH_NOSNAP}}, 0);
  expect_mirror_image_snapshot_set_copy_progress(
    mock_local_image_ctx, 12, true, 0, 0);
  expect_notify_update(mock_local_image_ctx);
  MockUnlinkPeerRequest mock_unlink_peer_request;
  expect_unlink_peer(mock_unlink_peer_request, 1, "remote mirror peer uuid",
                     -EINVAL);

  // wake-up replayer
  update_watch_ctx->handle_notify();

  // wait for sync to complete and expect replay complete
  ASSERT_EQ(0, wait_for_notification(1));
  ASSERT_FALSE(mock_replayer.is_replaying());
  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_remote_image_ctx));
}

} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd
