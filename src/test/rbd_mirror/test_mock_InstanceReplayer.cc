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
  Mutex &timer_lock;
  Cond timer_cond;

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
  MOCK_METHOD3(add_or_update_attribute,
               void(int64_t, const std::string&,
                    const service_daemon::AttributeValue&));
};

template<>
struct InstanceWatcher<librbd::MockTestImageCtx> {
};

template<>
struct ImageReplayer<librbd::MockTestImageCtx> {
  static ImageReplayer* s_instance;
  std::string global_image_id;

  static ImageReplayer *create(
    Threads<librbd::MockTestImageCtx> *threads,
    InstanceWatcher<librbd::MockTestImageCtx> *instance_watcher,
    RadosRef local, const std::string &local_mirror_uuid, int64_t local_pool_id,
    const std::string &global_image_id) {
    assert(s_instance != nullptr);
    s_instance->global_image_id = global_image_id;
    return s_instance;
  }

  ImageReplayer() {
    assert(s_instance == nullptr);
    s_instance = this;
  }

  virtual ~ImageReplayer() {
    assert(s_instance == this);
    s_instance = nullptr;
  }

  MOCK_METHOD0(destroy, void());
  MOCK_METHOD2(start, void(Context *, bool));
  MOCK_METHOD2(stop, void(Context *, bool));
  MOCK_METHOD0(restart, void());
  MOCK_METHOD0(flush, void());
  MOCK_METHOD2(print_status, void(Formatter *, stringstream *));
  MOCK_METHOD2(add_peer, void(const std::string &, librados::IoCtx &));
  MOCK_METHOD0(get_global_image_id, const std::string &());
  MOCK_METHOD0(get_local_image_id, const std::string &());
  MOCK_METHOD0(is_running, bool());
  MOCK_METHOD0(is_stopped, bool());
  MOCK_METHOD0(is_blacklisted, bool());

  MOCK_CONST_METHOD0(is_finished, bool());
  MOCK_METHOD1(set_finished, void(bool));

  MOCK_CONST_METHOD0(get_health_state, image_replayer::HealthState());
};

ImageReplayer<librbd::MockTestImageCtx>* ImageReplayer<librbd::MockTestImageCtx>::s_instance = nullptr;

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
          assert(mock_threads.timer_lock.is_locked());
          if (timer_ctx != nullptr) {
            *timer_ctx = ctx;
            mock_threads.timer_cond.SignalOne();
          } else {
            m_threads->work_queue->queue(
              new FunctionContext([&mock_threads, ctx](int) {
                Mutex::Locker timer_lock(mock_threads.timer_lock);
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
  MockInstanceWatcher mock_instance_watcher;
  MockImageReplayer mock_image_replayer;
  MockInstanceReplayer instance_replayer(
    &mock_threads, &mock_service_daemon,
    rbd::mirror::RadosRef(new librados::Rados(m_local_io_ctx)),
    "local_mirror_uuid", m_local_io_ctx.get_id());
  std::string global_image_id("global_image_id");

  EXPECT_CALL(mock_image_replayer, get_global_image_id())
    .WillRepeatedly(ReturnRef(global_image_id));

  InSequence seq;
  expect_work_queue(mock_threads);
  Context *timer_ctx = nullptr;
  expect_add_event_after(mock_threads, &timer_ctx);
  instance_replayer.init();
  instance_replayer.add_peer("peer_uuid", m_remote_io_ctx);

  // Acquire

  C_SaferCond on_acquire;
  EXPECT_CALL(mock_image_replayer, add_peer("peer_uuid", _));
  EXPECT_CALL(mock_image_replayer, set_finished(false));
  EXPECT_CALL(mock_image_replayer, is_stopped()).WillOnce(Return(true));
  EXPECT_CALL(mock_image_replayer, is_blacklisted()).WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, is_finished()).WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, start(nullptr, false));
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
  MockInstanceWatcher mock_instance_watcher;
  MockImageReplayer mock_image_replayer;
  MockInstanceReplayer instance_replayer(
    &mock_threads, &mock_service_daemon,
    rbd::mirror::RadosRef(new librados::Rados(m_local_io_ctx)),
    "local_mirror_uuid", m_local_io_ctx.get_id());
  std::string global_image_id("global_image_id");

  EXPECT_CALL(mock_image_replayer, get_global_image_id())
    .WillRepeatedly(ReturnRef(global_image_id));

  InSequence seq;
  expect_work_queue(mock_threads);
  Context *timer_ctx1 = nullptr;
  expect_add_event_after(mock_threads, &timer_ctx1);
  instance_replayer.init();
  instance_replayer.add_peer("peer_uuid", m_remote_io_ctx);

  // Acquire

  C_SaferCond on_acquire;
  EXPECT_CALL(mock_image_replayer, add_peer("peer_uuid", _));
  EXPECT_CALL(mock_image_replayer, set_finished(false));
  EXPECT_CALL(mock_image_replayer, is_stopped()).WillOnce(Return(true));
  EXPECT_CALL(mock_image_replayer, is_blacklisted()).WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, is_finished()).WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, start(nullptr, false));
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
    Mutex::Locker timer_locker(mock_threads.timer_lock);
    timer_ctx1->complete(0);
  }

  // remove finished image replayer
  EXPECT_CALL(mock_image_replayer, get_health_state()).WillOnce(
    Return(image_replayer::HEALTH_STATE_OK));
  EXPECT_CALL(mock_image_replayer, is_stopped()).WillOnce(Return(true));
  EXPECT_CALL(mock_image_replayer, is_blacklisted()).WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, is_finished()).WillOnce(Return(true));
  EXPECT_CALL(mock_image_replayer, destroy());
  EXPECT_CALL(mock_service_daemon,add_or_update_attribute(_, _, _)).Times(3);

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
} // namespace mirror
} // namespace rbd
