// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/rbd_mirror/mock/MockContextWQ.h"
#include "test/rbd_mirror/mock/MockSafeTimer.h"
#include "tools/rbd_mirror/ImageReplayer.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/InstanceReplayer.h"
#include "tools/rbd_mirror/ServiceDaemon.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/Types.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

} // namespace librbd

namespace rbd {
namespace mirror {

template <>
struct Threads<librbd::MockTestImageCtx> {
  MockSafeTimer *timer;
  ceph::mutex &timer_lock;
  ceph::condition_variable timer_cond;

  MockContextWQ *work_queue;

  Threads(Threads<librbd::ImageCtx> *threads)
    : timer(new MockSafeTimer()),
      timer_lock(threads->timer_lock),
      work_queue(new MockContextWQ()) {
  }
  ~Threads() {
    delete timer;
    delete work_queue;
  }
};

template<>
struct ServiceDaemon<librbd::MockTestImageCtx> {
  MOCK_METHOD4(add_or_update_namespace_attribute,
               void(int64_t, const std::string&, const std::string&,
                    const service_daemon::AttributeValue&));
};

template<>
struct InstanceWatcher<librbd::MockTestImageCtx> {
};

template<>
struct ImageReplayer<librbd::MockTestImageCtx> {
  typedef std::set<Peer<librbd::MockTestImageCtx>> Peers;

  static ImageReplayer* s_instance;
  std::string global_image_id;

  static ImageReplayer *create(
      librados::IoCtx &local_io_ctx, const std::string &local_mirror_uuid,
      const std::string &global_image_id,
      Threads<librbd::MockTestImageCtx> *threads,
      InstanceWatcher<librbd::MockTestImageCtx> *instance_watcher,
      MirrorStatusUpdater<librbd::MockTestImageCtx>* local_status_updater,
      journal::CacheManagerHandler *cache_manager_handler,
      PoolMetaCache<librbd::MockTestImageCtx>* pool_meta_cache,
      Peers peers) {
    ceph_assert(s_instance != nullptr);
    s_instance->global_image_id = global_image_id;
    return s_instance;
  }

  ImageReplayer() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }

  virtual ~ImageReplayer() {
    ceph_assert(s_instance == this);
    s_instance = nullptr;
  }

  MOCK_METHOD0(destroy, void());
  MOCK_METHOD2(start, void(Context *, bool));
  MOCK_METHOD2(stop, void(Context *, bool));
  MOCK_METHOD1(restart, void(Context*));
  MOCK_METHOD0(flush, void());
  MOCK_METHOD1(print_status, void(Formatter *));
  MOCK_METHOD1(add_peer, void(const Peer<librbd::MockTestImageCtx>& peer));
  MOCK_METHOD0(get_global_image_id, const std::string &());
  MOCK_METHOD0(get_local_image_id, const std::string &());
  MOCK_METHOD0(is_running, bool());
  MOCK_METHOD0(is_stopped, bool());
  MOCK_METHOD0(is_blocklisted, bool());

  MOCK_CONST_METHOD0(is_finished, bool());
  MOCK_METHOD1(set_finished, void(bool));

  MOCK_CONST_METHOD0(get_health_state, image_replayer::HealthState());
};

ImageReplayer<librbd::MockTestImageCtx>* ImageReplayer<librbd::MockTestImageCtx>::s_instance = nullptr;

template<>
struct MirrorStatusUpdater<librbd::MockTestImageCtx> {
};

} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/InstanceReplayer.cc"

namespace rbd {
namespace mirror {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::ReturnRef;
using ::testing::WithArg;

class TestMockInstanceReplayer : public TestMockFixture {
public:
  typedef Threads<librbd::MockTestImageCtx> MockThreads;
  typedef ImageReplayer<librbd::MockTestImageCtx> MockImageReplayer;
  typedef InstanceReplayer<librbd::MockTestImageCtx> MockInstanceReplayer;
  typedef InstanceWatcher<librbd::MockTestImageCtx> MockInstanceWatcher;
  typedef MirrorStatusUpdater<librbd::MockTestImageCtx> MockMirrorStatusUpdater;
  typedef ServiceDaemon<librbd::MockTestImageCtx> MockServiceDaemon;

  void expect_work_queue(MockThreads &mock_threads) {
    EXPECT_CALL(*mock_threads.work_queue, queue(_, _))
      .WillOnce(Invoke([this](Context *ctx, int r) {
          m_threads->work_queue->queue(ctx, r);
        }));
  }

  void expect_add_event_after(MockThreads &mock_threads,
                              Context** timer_ctx = nullptr) {
    EXPECT_CALL(*mock_threads.timer, add_event_after(_, _))
      .WillOnce(DoAll(
        WithArg<1>(Invoke([this, &mock_threads, timer_ctx](Context *ctx) {
          ceph_assert(ceph_mutex_is_locked(mock_threads.timer_lock));
          if (timer_ctx != nullptr) {
            *timer_ctx = ctx;
            mock_threads.timer_cond.notify_one();
          } else {
            m_threads->work_queue->queue(
              new LambdaContext([&mock_threads, ctx](int) {
                std::lock_guard timer_lock{mock_threads.timer_lock};
                ctx->complete(0);
              }), 0);
          }
        })),
        ReturnArg<1>()));
  }

  void expect_cancel_event(MockThreads &mock_threads, bool canceled) {
    EXPECT_CALL(*mock_threads.timer, cancel_event(_))
      .WillOnce(Return(canceled));
  }
};

TEST_F(TestMockInstanceReplayer, AcquireReleaseImage) {
  MockThreads mock_threads(m_threads);
  MockServiceDaemon mock_service_daemon;
  MockMirrorStatusUpdater mock_status_updater;
  MockInstanceWatcher mock_instance_watcher;
  MockImageReplayer mock_image_replayer;
  MockInstanceReplayer instance_replayer(
      m_local_io_ctx, "local_mirror_uuid",
      &mock_threads, &mock_service_daemon, &mock_status_updater, nullptr,
      nullptr);
  std::string global_image_id("global_image_id");

  EXPECT_CALL(mock_image_replayer, get_global_image_id())
    .WillRepeatedly(ReturnRef(global_image_id));

  InSequence seq;
  expect_work_queue(mock_threads);
  Context *timer_ctx = nullptr;
  expect_add_event_after(mock_threads, &timer_ctx);
  instance_replayer.init();
  instance_replayer.add_peer({"peer_uuid", m_remote_io_ctx, {}, nullptr});

  // Acquire

  C_SaferCond on_acquire;
  EXPECT_CALL(mock_image_replayer, is_stopped()).WillOnce(Return(true));
  EXPECT_CALL(mock_image_replayer, is_blocklisted()).WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, is_finished()).WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, start(_, false))
    .WillOnce(CompleteContext(0));
  expect_work_queue(mock_threads);

  instance_replayer.acquire_image(&mock_instance_watcher, global_image_id,
                                  &on_acquire);
  ASSERT_EQ(0, on_acquire.wait());

  // Release

  C_SaferCond on_release;

  EXPECT_CALL(mock_image_replayer, is_stopped())
    .WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, is_running())
    .WillOnce(Return(false));
  expect_work_queue(mock_threads);
  expect_add_event_after(mock_threads);
  expect_work_queue(mock_threads);
  EXPECT_CALL(mock_image_replayer, is_stopped())
    .WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, is_running())
    .WillOnce(Return(true));
  EXPECT_CALL(mock_image_replayer, stop(_, false))
    .WillOnce(CompleteContext(0));
  expect_work_queue(mock_threads);
  EXPECT_CALL(mock_image_replayer, is_stopped())
    .WillOnce(Return(true));
  expect_work_queue(mock_threads);
  EXPECT_CALL(mock_image_replayer, destroy());

  instance_replayer.release_image("global_image_id", &on_release);
  ASSERT_EQ(0, on_release.wait());

  expect_work_queue(mock_threads);
  expect_cancel_event(mock_threads, true);
  expect_work_queue(mock_threads);
  instance_replayer.shut_down();
  ASSERT_TRUE(timer_ctx != nullptr);
  delete timer_ctx;
}

TEST_F(TestMockInstanceReplayer, RemoveFinishedImage) {
  MockThreads mock_threads(m_threads);
  MockServiceDaemon mock_service_daemon;
  MockMirrorStatusUpdater mock_status_updater;
  MockInstanceWatcher mock_instance_watcher;
  MockImageReplayer mock_image_replayer;
  MockInstanceReplayer instance_replayer(
      m_local_io_ctx, "local_mirror_uuid",
      &mock_threads, &mock_service_daemon, &mock_status_updater, nullptr,
      nullptr);
  std::string global_image_id("global_image_id");

  EXPECT_CALL(mock_image_replayer, get_global_image_id())
    .WillRepeatedly(ReturnRef(global_image_id));

  InSequence seq;
  expect_work_queue(mock_threads);
  Context *timer_ctx1 = nullptr;
  expect_add_event_after(mock_threads, &timer_ctx1);
  instance_replayer.init();
  instance_replayer.add_peer({"peer_uuid", m_remote_io_ctx, {}, nullptr});

  // Acquire

  C_SaferCond on_acquire;
  EXPECT_CALL(mock_image_replayer, is_stopped()).WillOnce(Return(true));
  EXPECT_CALL(mock_image_replayer, is_blocklisted()).WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, is_finished()).WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, start(_, false))
    .WillOnce(CompleteContext(0));
  expect_work_queue(mock_threads);

  instance_replayer.acquire_image(&mock_instance_watcher, global_image_id,
                                  &on_acquire);
  ASSERT_EQ(0, on_acquire.wait());

  // periodic start timer
  Context *timer_ctx2 = nullptr;
  expect_add_event_after(mock_threads, &timer_ctx2);

  Context *start_image_replayers_ctx = nullptr;
  EXPECT_CALL(*mock_threads.work_queue, queue(_, 0))
    .WillOnce(Invoke([&start_image_replayers_ctx](Context *ctx, int r) {
                start_image_replayers_ctx = ctx;
              }));

  ASSERT_TRUE(timer_ctx1 != nullptr);
  {
    std::lock_guard timer_locker{mock_threads.timer_lock};
    timer_ctx1->complete(0);
  }

  // remove finished image replayer
  EXPECT_CALL(mock_image_replayer, get_health_state()).WillOnce(
    Return(image_replayer::HEALTH_STATE_OK));
  EXPECT_CALL(mock_image_replayer, is_stopped()).WillOnce(Return(true));
  EXPECT_CALL(mock_image_replayer, is_blocklisted()).WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, is_finished()).WillOnce(Return(true));
  EXPECT_CALL(mock_image_replayer, destroy());
  expect_work_queue(mock_threads);
  EXPECT_CALL(mock_service_daemon,
              add_or_update_namespace_attribute(_, _, _, _)).Times(3);

  ASSERT_TRUE(start_image_replayers_ctx != nullptr);
  start_image_replayers_ctx->complete(0);

  // shut down
  expect_work_queue(mock_threads);
  expect_cancel_event(mock_threads, true);
  expect_work_queue(mock_threads);
  instance_replayer.shut_down();
  ASSERT_TRUE(timer_ctx2 != nullptr);
  delete timer_ctx2;
}

TEST_F(TestMockInstanceReplayer, Reacquire) {
  MockThreads mock_threads(m_threads);
  MockServiceDaemon mock_service_daemon;
  MockMirrorStatusUpdater mock_status_updater;
  MockInstanceWatcher mock_instance_watcher;
  MockImageReplayer mock_image_replayer;
  MockInstanceReplayer instance_replayer(
      m_local_io_ctx, "local_mirror_uuid",
      &mock_threads, &mock_service_daemon, &mock_status_updater, nullptr,
      nullptr);
  std::string global_image_id("global_image_id");

  EXPECT_CALL(mock_image_replayer, get_global_image_id())
    .WillRepeatedly(ReturnRef(global_image_id));

  InSequence seq;
  expect_work_queue(mock_threads);
  Context *timer_ctx = nullptr;
  expect_add_event_after(mock_threads, &timer_ctx);
  instance_replayer.init();
  instance_replayer.add_peer({"peer_uuid", m_remote_io_ctx, {}, nullptr});

  // Acquire

  EXPECT_CALL(mock_image_replayer, is_stopped()).WillOnce(Return(true));
  EXPECT_CALL(mock_image_replayer, is_blocklisted()).WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, is_finished()).WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, start(_, false))
    .WillOnce(CompleteContext(0));
  expect_work_queue(mock_threads);

  C_SaferCond on_acquire1;
  instance_replayer.acquire_image(&mock_instance_watcher, global_image_id,
                                  &on_acquire1);
  ASSERT_EQ(0, on_acquire1.wait());

  // Re-acquire
  EXPECT_CALL(mock_image_replayer, set_finished(false));
  EXPECT_CALL(mock_image_replayer, restart(_))
    .WillOnce(CompleteContext(0));
  expect_work_queue(mock_threads);

  C_SaferCond on_acquire2;
  instance_replayer.acquire_image(&mock_instance_watcher, global_image_id,
                                  &on_acquire2);
  ASSERT_EQ(0, on_acquire2.wait());

  expect_work_queue(mock_threads);
  expect_cancel_event(mock_threads, true);
  EXPECT_CALL(mock_image_replayer, is_stopped()).WillOnce(Return(true));
  expect_work_queue(mock_threads);
  expect_work_queue(mock_threads);
  EXPECT_CALL(mock_image_replayer, is_stopped()).WillOnce(Return(true));
  EXPECT_CALL(mock_image_replayer, destroy());
  instance_replayer.shut_down();
  ASSERT_TRUE(timer_ctx != nullptr);
  delete timer_ctx;
}

} // namespace mirror
} // namespace rbd
