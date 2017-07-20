// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/test_mock_fixture.h"
#include "tools/rbd_mirror/ImageDeleter.h"
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
  Mutex &timer_lock;
  SafeTimer *timer;
  ContextWQ *work_queue;

  Threads(Threads<librbd::ImageCtx> *threads)
    : timer_lock(threads->timer_lock), timer(threads->timer),
      work_queue(threads->work_queue) {
  }
};

template <>
struct ImageDeleter<librbd::MockTestImageCtx> {
  MOCK_METHOD4(schedule_image_delete, void(RadosRef, int64_t,
                                           const std::string&, bool));
  MOCK_METHOD4(wait_for_scheduled_deletion,
               void(int64_t, const std::string&, Context*, bool));
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
    ImageDeleter<librbd::MockTestImageCtx>* image_deleter,
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
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::WithArg;

class TestMockInstanceReplayer : public TestMockFixture {
public:
  typedef ImageDeleter<librbd::MockTestImageCtx> MockImageDeleter;
  typedef ImageReplayer<librbd::MockTestImageCtx> MockImageReplayer;
  typedef InstanceReplayer<librbd::MockTestImageCtx> MockInstanceReplayer;
  typedef InstanceWatcher<librbd::MockTestImageCtx> MockInstanceWatcher;
  typedef ServiceDaemon<librbd::MockTestImageCtx> MockServiceDaemon;
  typedef Threads<librbd::MockTestImageCtx> MockThreads;

  void SetUp() override {
    TestMockFixture::SetUp();

    m_mock_threads = new MockThreads(m_threads);
  }

  void TearDown() override {
    delete m_mock_threads;
    TestMockFixture::TearDown();
  }

  void expect_wait_for_scheduled_deletion(MockImageDeleter& mock_image_deleter,
                                          const std::string& global_image_id,
                                          int r) {
    EXPECT_CALL(mock_image_deleter,
                wait_for_scheduled_deletion(_, global_image_id, _, false))
      .WillOnce(WithArg<2>(Invoke([this, r](Context *ctx) {
                             m_threads->work_queue->queue(ctx, r);
                           })));
  }

  MockThreads *m_mock_threads;
};

TEST_F(TestMockInstanceReplayer, AcquireReleaseImage) {
  MockServiceDaemon mock_service_daemon;
  MockImageDeleter mock_image_deleter;
  MockInstanceWatcher mock_instance_watcher;
  MockImageReplayer mock_image_replayer;
  MockInstanceReplayer instance_replayer(
    m_mock_threads, &mock_service_daemon, &mock_image_deleter,
    rbd::mirror::RadosRef(new librados::Rados(m_local_io_ctx)),
    "local_mirror_uuid", m_local_io_ctx.get_id());

  std::string global_image_id("global_image_id");

  EXPECT_CALL(mock_image_replayer, get_global_image_id())
    .WillRepeatedly(ReturnRef(global_image_id));
  EXPECT_CALL(mock_image_replayer, is_blacklisted())
    .WillRepeatedly(Return(false));

  expect_wait_for_scheduled_deletion(mock_image_deleter, "global_image_id", 0);

  InSequence seq;

  instance_replayer.init();
  instance_replayer.add_peer("peer_uuid", m_remote_io_ctx);

  // Acquire

  C_SaferCond on_acquire;

  EXPECT_CALL(mock_image_replayer, add_peer("peer_uuid", _));
  EXPECT_CALL(mock_image_replayer, is_stopped())
    .WillOnce(Return(true));
  EXPECT_CALL(mock_image_replayer, start(nullptr, false));

  instance_replayer.acquire_image(&mock_instance_watcher, global_image_id,
                                  "remote_mirror_uuid", "remote_image_id",
                                  &on_acquire);
  ASSERT_EQ(0, on_acquire.wait());

  // Release

  C_SaferCond on_release;

  EXPECT_CALL(mock_image_replayer, is_stopped())
    .WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, is_running())
    .WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, is_stopped())
    .WillOnce(Return(false));
  EXPECT_CALL(mock_image_replayer, is_running())
    .WillOnce(Return(true));
  EXPECT_CALL(mock_image_replayer, stop(_, false))
    .WillOnce(CompleteContext(0));
  EXPECT_CALL(mock_image_replayer, is_stopped())
    .WillOnce(Return(true));
  EXPECT_CALL(mock_image_replayer, destroy());

  instance_replayer.release_image("global_image_id", "remote_mirror_uuid",
                                  "remote_image_id", false, &on_release);
  ASSERT_EQ(0, on_release.wait());

  instance_replayer.shut_down();
}

} // namespace mirror
} // namespace rbd
