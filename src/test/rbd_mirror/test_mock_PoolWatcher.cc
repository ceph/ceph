// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/mock/MockContextWQ.h"
#include "test/rbd_mirror/mock/MockSafeTimer.h"
#include "librbd/MirroringWatcher.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/PoolWatcher.h"
#include "tools/rbd_mirror/pool_watcher/RefreshImagesRequest.h"
#include "include/stringify.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

struct MockMirroringWatcher {
  static MockMirroringWatcher *s_instance;
  static MockMirroringWatcher &get_instance() {
    ceph_assert(s_instance != nullptr);
    return *s_instance;
  }

  MockMirroringWatcher() {
    s_instance = this;
  }

  MOCK_CONST_METHOD0(is_unregistered, bool());
  MOCK_METHOD1(register_watch, void(Context*));
  MOCK_METHOD1(unregister_watch, void(Context*));

  MOCK_CONST_METHOD0(get_oid, std::string());
};

template <>
struct MirroringWatcher<MockTestImageCtx> {
  static MirroringWatcher *s_instance;

  MirroringWatcher(librados::IoCtx &io_ctx, ::MockContextWQ *work_queue) {
    s_instance = this;
  }
  virtual ~MirroringWatcher() {
  }

  static MirroringWatcher<MockTestImageCtx> &get_instance() {
    ceph_assert(s_instance != nullptr);
    return *s_instance;
  }

  virtual void handle_rewatch_complete(int r) = 0;

  virtual void handle_mode_updated(cls::rbd::MirrorMode mirror_mode) = 0;
  virtual void handle_image_updated(cls::rbd::MirrorImageState state,
                                    const std::string &remote_image_id,
                                    const std::string &global_image_id) = 0;

  bool is_unregistered() const {
    return MockMirroringWatcher::get_instance().is_unregistered();
  }
  void register_watch(Context *ctx) {
    MockMirroringWatcher::get_instance().register_watch(ctx);
  }
  void unregister_watch(Context *ctx) {
    MockMirroringWatcher::get_instance().unregister_watch(ctx);
  }
  std::string get_oid() const {
    return MockMirroringWatcher::get_instance().get_oid();
  }
};

MockMirroringWatcher *MockMirroringWatcher::s_instance = nullptr;
MirroringWatcher<MockTestImageCtx> *MirroringWatcher<MockTestImageCtx>::s_instance = nullptr;

} // namespace librbd

namespace rbd {
namespace mirror {

template <>
struct Threads<librbd::MockTestImageCtx> {
  MockSafeTimer *timer;
  Mutex &timer_lock;

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

namespace pool_watcher {

template <>
struct RefreshImagesRequest<librbd::MockTestImageCtx> {
  ImageIds *image_ids = nullptr;
  Context *on_finish = nullptr;
  static RefreshImagesRequest *s_instance;
  static RefreshImagesRequest *create(librados::IoCtx &io_ctx,
                                      ImageIds *image_ids,
                                      Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->image_ids = image_ids;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  MOCK_METHOD0(send, void());

  RefreshImagesRequest() {
    s_instance = this;
  }
};

RefreshImagesRequest<librbd::MockTestImageCtx> *RefreshImagesRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace pool_watcher

} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/PoolWatcher.cc"

namespace rbd {
namespace mirror {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::StrEq;
using ::testing::WithArg;
using ::testing::WithoutArgs;

class TestMockPoolWatcher : public TestMockFixture {
public:
  typedef PoolWatcher<librbd::MockTestImageCtx> MockPoolWatcher;
  typedef Threads<librbd::MockTestImageCtx> MockThreads;
  typedef pool_watcher::RefreshImagesRequest<librbd::MockTestImageCtx> MockRefreshImagesRequest;
  typedef librbd::MockMirroringWatcher MockMirroringWatcher;
  typedef librbd::MirroringWatcher<librbd::MockTestImageCtx> MirroringWatcher;

  struct MockListener : pool_watcher::Listener {
    TestMockPoolWatcher *test;

    MockListener(TestMockPoolWatcher *test) : test(test) {
    }

    MOCK_METHOD3(mock_handle_update, void(const std::string &, const ImageIds &,
                                          const ImageIds &));
    void handle_update(const std::string &mirror_uuid,
                       ImageIds &&added_image_ids,
                       ImageIds &&removed_image_ids) override {
      mock_handle_update(mirror_uuid, added_image_ids, removed_image_ids);
    }
  };

  TestMockPoolWatcher() : m_lock("TestMockPoolWatcher::m_lock") {
  }

  void expect_work_queue(MockThreads &mock_threads) {
    EXPECT_CALL(*mock_threads.work_queue, queue(_, _))
      .WillRepeatedly(Invoke([this](Context *ctx, int r) {
          m_threads->work_queue->queue(ctx, r);
        }));
  }

  void expect_mirroring_watcher_is_unregistered(MockMirroringWatcher &mock_mirroring_watcher,
                                                bool unregistered) {
    EXPECT_CALL(mock_mirroring_watcher, is_unregistered())
      .WillOnce(Return(unregistered));
  }

  void expect_mirroring_watcher_register(MockMirroringWatcher &mock_mirroring_watcher,
                                         int r) {
    EXPECT_CALL(mock_mirroring_watcher, register_watch(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_mirroring_watcher_unregister(MockMirroringWatcher &mock_mirroring_watcher,
                                         int r) {
    EXPECT_CALL(mock_mirroring_watcher, unregister_watch(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_refresh_images(MockRefreshImagesRequest &request,
                             const ImageIds &image_ids, int r) {
    EXPECT_CALL(request, send())
      .WillOnce(Invoke([&request, image_ids, r]() {
          *request.image_ids = image_ids;
          request.on_finish->complete(r);
        }));
  }

  void expect_listener_handle_update(MockListener &mock_listener,
                                     const std::string &mirror_uuid,
                                     const ImageIds &added_image_ids,
                                     const ImageIds &removed_image_ids) {
    EXPECT_CALL(mock_listener, mock_handle_update(mirror_uuid, added_image_ids,
                                                  removed_image_ids))
      .WillOnce(WithoutArgs(Invoke([this]() {
          Mutex::Locker locker(m_lock);
          ++m_update_count;
          m_cond.Signal();
        })));
  }

  void expect_mirror_uuid_get(librados::IoCtx &io_ctx,
                              const std::string &uuid, int r) {
    bufferlist out_bl;
    encode(uuid, out_bl);

    EXPECT_CALL(get_mock_io_ctx(io_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_uuid_get"),
                     _, _, _))
      .WillOnce(DoAll(WithArg<5>(Invoke([out_bl](bufferlist *bl) {
                          *bl = out_bl;
                        })),
                      Return(r)));
  }

  void expect_timer_add_event(MockThreads &mock_threads) {
    EXPECT_CALL(*mock_threads.timer, add_event_after(_, _))
      .WillOnce(DoAll(WithArg<1>(Invoke([this](Context *ctx) {
                        auto wrapped_ctx =
			  new FunctionContext([this, ctx](int r) {
			      Mutex::Locker timer_locker(m_threads->timer_lock);
			      ctx->complete(r);
			    });
			m_threads->work_queue->queue(wrapped_ctx, 0);
                      })),
                      ReturnArg<1>()));
  }

  int when_shut_down(MockPoolWatcher &mock_pool_watcher) {
    C_SaferCond ctx;
    mock_pool_watcher.shut_down(&ctx);
    return ctx.wait();
  }

  bool wait_for_update(uint32_t count) {
    Mutex::Locker locker(m_lock);
    while (m_update_count < count) {
      if (m_cond.WaitInterval(m_lock, utime_t(10, 0)) != 0) {
        break;
      }
    }
    if (m_update_count < count) {
      return false;
    }

    m_update_count -= count;
    return true;
  }

  Mutex m_lock;
  Cond m_cond;
  uint32_t m_update_count = 0;
};

TEST_F(TestMockPoolWatcher, EmptyPool) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  MockRefreshImagesRequest mock_refresh_images_request;
  expect_refresh_images(mock_refresh_images_request, {}, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);

  MockListener mock_listener(this);
  expect_listener_handle_update(mock_listener, "remote uuid", {}, {});

  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());

  ASSERT_TRUE(wait_for_update(1));
  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, NonEmptyPool) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  ImageIds image_ids{
    {"global id 1", "remote id 1"},
    {"global id 2", "remote id 2"}};
  MockRefreshImagesRequest mock_refresh_images_request;
  expect_refresh_images(mock_refresh_images_request, image_ids, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);

  MockListener mock_listener(this);
  expect_listener_handle_update(mock_listener, "remote uuid", image_ids, {});

  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());

  ASSERT_TRUE(wait_for_update(1));
  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, NotifyDuringRefresh) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  ImageIds image_ids{
    {"global id 1", "remote id 1"},
    {"global id 2", "remote id 2"}};
  MockRefreshImagesRequest mock_refresh_images_request;
  bool refresh_sent = false;
  EXPECT_CALL(mock_refresh_images_request, send())
    .WillOnce(Invoke([this, &mock_refresh_images_request, &image_ids,
                      &refresh_sent]() {
       *mock_refresh_images_request.image_ids = image_ids;

        Mutex::Locker locker(m_lock);
        refresh_sent = true;
        m_cond.Signal();
      }));

  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);

  MockListener mock_listener(this);
  image_ids = {
    {"global id 1", "remote id 1a"},
    {"global id 3", "remote id 3"}};
  expect_listener_handle_update(mock_listener, "remote uuid", image_ids, {});

  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  mock_pool_watcher.init(nullptr);

  {
    Mutex::Locker locker(m_lock);
    while (!refresh_sent) {
      m_cond.Wait(m_lock);
    }
  }

  MirroringWatcher::get_instance().handle_image_updated(
    cls::rbd::MIRROR_IMAGE_STATE_DISABLING, "remote id 2", "global id 2");
  MirroringWatcher::get_instance().handle_image_updated(
    cls::rbd::MIRROR_IMAGE_STATE_ENABLED, "remote id 1a", "global id 1");
  MirroringWatcher::get_instance().handle_image_updated(
    cls::rbd::MIRROR_IMAGE_STATE_ENABLED, "remote id 3", "global id 3");

  mock_refresh_images_request.on_finish->complete(0);
  ASSERT_TRUE(wait_for_update(1));

  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, Notify) {
  MockThreads mock_threads(m_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  ImageIds image_ids{
    {"global id 1", "remote id 1"},
    {"global id 2", "remote id 2"}};
  MockRefreshImagesRequest mock_refresh_images_request;
  expect_refresh_images(mock_refresh_images_request, image_ids, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);

  EXPECT_CALL(*mock_threads.work_queue, queue(_, _))
    .WillOnce(Invoke([this](Context *ctx, int r) {
        m_threads->work_queue->queue(ctx, r);
      }));

  MockListener mock_listener(this);
  expect_listener_handle_update(mock_listener, "remote uuid", image_ids, {});

  Context *notify_ctx = nullptr;
  EXPECT_CALL(*mock_threads.work_queue, queue(_, _))
    .WillOnce(Invoke([this, &notify_ctx](Context *ctx, int r) {
        Mutex::Locker locker(m_lock);
        ASSERT_EQ(nullptr, notify_ctx);
        notify_ctx = ctx;
        m_cond.Signal();
      }));
  expect_listener_handle_update(
    mock_listener, "remote uuid",
    {{"global id 1", "remote id 1a"}, {"global id 3", "remote id 3"}},
    {{"global id 1", "remote id 1"}, {"global id 2", "remote id 2"}});

  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(wait_for_update(1));

  C_SaferCond flush_ctx;
  m_threads->work_queue->queue(&flush_ctx, 0);
  ASSERT_EQ(0, flush_ctx.wait());

  MirroringWatcher::get_instance().handle_image_updated(
    cls::rbd::MIRROR_IMAGE_STATE_DISABLING, "remote id 2", "global id 2");
  MirroringWatcher::get_instance().handle_image_updated(
    cls::rbd::MIRROR_IMAGE_STATE_DISABLED, "remote id 2", "global id 2");
  MirroringWatcher::get_instance().handle_image_updated(
    cls::rbd::MIRROR_IMAGE_STATE_ENABLED, "remote id 1a", "global id 1");
  MirroringWatcher::get_instance().handle_image_updated(
    cls::rbd::MIRROR_IMAGE_STATE_ENABLED, "remote id 3", "global id 3");
  notify_ctx->complete(0);

  ASSERT_TRUE(wait_for_update(1));

  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, RegisterWatcherBlacklist) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, -EBLACKLISTED);

  MockListener mock_listener(this);
  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(-EBLACKLISTED, ctx.wait());
  ASSERT_TRUE(mock_pool_watcher.is_blacklisted());

  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, RegisterWatcherMissing) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, -ENOENT);
  expect_timer_add_event(mock_threads);

  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  MockRefreshImagesRequest mock_refresh_images_request;
  expect_refresh_images(mock_refresh_images_request, {}, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);

  MockListener mock_listener(this);
  expect_listener_handle_update(mock_listener, "remote uuid", {}, {});

  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(-ENOENT, ctx.wait());

  ASSERT_TRUE(wait_for_update(1));
  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, RegisterWatcherError) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, -EINVAL);
  expect_timer_add_event(mock_threads);

  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  MockRefreshImagesRequest mock_refresh_images_request;
  expect_refresh_images(mock_refresh_images_request, {}, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);

  MockListener mock_listener(this);
  expect_listener_handle_update(mock_listener, "remote uuid", {}, {});

  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());

  ASSERT_TRUE(wait_for_update(1));
  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, RefreshBlacklist) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  MockRefreshImagesRequest mock_refresh_images_request;
  expect_refresh_images(mock_refresh_images_request, {}, -EBLACKLISTED);

  MockListener mock_listener(this);
  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(-EBLACKLISTED, ctx.wait());
  ASSERT_TRUE(mock_pool_watcher.is_blacklisted());

  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, RefreshMissing) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  MockRefreshImagesRequest mock_refresh_images_request;
  expect_refresh_images(mock_refresh_images_request, {}, -ENOENT);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);

  MockListener mock_listener(this);
  expect_listener_handle_update(mock_listener, "remote uuid", {}, {});

  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());

  ASSERT_TRUE(wait_for_update(1));
  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, RefreshError) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  MockRefreshImagesRequest mock_refresh_images_request;
  expect_refresh_images(mock_refresh_images_request, {}, -EINVAL);
  expect_timer_add_event(mock_threads);

  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, false);
  expect_refresh_images(mock_refresh_images_request, {}, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);

  MockListener mock_listener(this);
  expect_listener_handle_update(mock_listener, "remote uuid", {}, {});

  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());

  ASSERT_TRUE(wait_for_update(1));
  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, GetMirrorUuidBlacklist) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  MockRefreshImagesRequest mock_refresh_images_request;
  expect_refresh_images(mock_refresh_images_request, {}, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", -EBLACKLISTED);

  MockListener mock_listener(this);
  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(-EBLACKLISTED, ctx.wait());
  ASSERT_TRUE(mock_pool_watcher.is_blacklisted());

  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, GetMirrorUuidMissing) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  MockRefreshImagesRequest mock_refresh_images_request;
  expect_refresh_images(mock_refresh_images_request, {}, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "", -ENOENT);
  expect_timer_add_event(mock_threads);

  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, false);
  expect_refresh_images(mock_refresh_images_request, {}, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);

  MockListener mock_listener(this);
  expect_listener_handle_update(mock_listener, "remote uuid", {}, {});

  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(-ENOENT, ctx.wait());

  ASSERT_TRUE(wait_for_update(1));
  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, GetMirrorUuidError) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  MockRefreshImagesRequest mock_refresh_images_request;
  expect_refresh_images(mock_refresh_images_request, {}, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", -EINVAL);
  expect_timer_add_event(mock_threads);

  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, false);
  expect_refresh_images(mock_refresh_images_request, {}, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);

  MockListener mock_listener(this);
  expect_listener_handle_update(mock_listener, "remote uuid", {}, {});

  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());

  ASSERT_TRUE(wait_for_update(1));
  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, Rewatch) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  MockRefreshImagesRequest mock_refresh_images_request;
  expect_refresh_images(mock_refresh_images_request, {}, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);

  MockListener mock_listener(this);
  expect_listener_handle_update(mock_listener, "remote uuid", {}, {});

  expect_timer_add_event(mock_threads);
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, false);
  expect_refresh_images(mock_refresh_images_request, {{"global id", "image id"}}, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);
  expect_listener_handle_update(mock_listener, "remote uuid",
                                {{"global id", "image id"}}, {});

  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(wait_for_update(1));

  MirroringWatcher::get_instance().handle_rewatch_complete(0);
  ASSERT_TRUE(wait_for_update(1));

  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, RewatchBlacklist) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  MockRefreshImagesRequest mock_refresh_images_request;
  expect_refresh_images(mock_refresh_images_request, {}, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);

  MockListener mock_listener(this);
  expect_listener_handle_update(mock_listener, "remote uuid", {}, {});

  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(wait_for_update(1));

  MirroringWatcher::get_instance().handle_rewatch_complete(-EBLACKLISTED);
  ASSERT_TRUE(mock_pool_watcher.is_blacklisted());

  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, RewatchError) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  MockRefreshImagesRequest mock_refresh_images_request;
  expect_refresh_images(mock_refresh_images_request, {}, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);

  MockListener mock_listener(this);
  expect_listener_handle_update(mock_listener, "remote uuid", {}, {});

  expect_timer_add_event(mock_threads);
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, false);
  expect_refresh_images(mock_refresh_images_request, {{"global id", "image id"}}, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);
  expect_listener_handle_update(mock_listener, "remote uuid",
                                {{"global id", "image id"}}, {});

  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(wait_for_update(1));

  MirroringWatcher::get_instance().handle_rewatch_complete(-EINVAL);
  ASSERT_TRUE(wait_for_update(1));

  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, DeferredRefresh) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  MockRefreshImagesRequest mock_refresh_images_request;

  EXPECT_CALL(mock_refresh_images_request, send())
    .WillOnce(Invoke([&mock_refresh_images_request]() {
        *mock_refresh_images_request.image_ids = {};
        MirroringWatcher::get_instance().handle_rewatch_complete(0);
        mock_refresh_images_request.on_finish->complete(0);
        }));
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);
  expect_timer_add_event(mock_threads);

  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, false);
  expect_refresh_images(mock_refresh_images_request, {}, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);

  MockListener mock_listener(this);
  expect_listener_handle_update(mock_listener, "remote uuid", {}, {});

  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());
  ASSERT_TRUE(wait_for_update(1));

  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

TEST_F(TestMockPoolWatcher, MirrorUuidUpdated) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  MockMirroringWatcher mock_mirroring_watcher;
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, true);
  expect_mirroring_watcher_register(mock_mirroring_watcher, 0);

  ImageIds image_ids{
    {"global id 1", "remote id 1"},
    {"global id 2", "remote id 2"}};
  MockRefreshImagesRequest mock_refresh_images_request;
  expect_refresh_images(mock_refresh_images_request, image_ids, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "remote uuid", 0);

  MockListener mock_listener(this);
  expect_listener_handle_update(mock_listener, "remote uuid", image_ids, {});

  MockPoolWatcher mock_pool_watcher(&mock_threads, m_remote_io_ctx,
                                    mock_listener);
  C_SaferCond ctx;
  mock_pool_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());

  ASSERT_TRUE(wait_for_update(1));

  expect_timer_add_event(mock_threads);
  ImageIds new_image_ids{
    {"global id 1", "remote id 1"}};
  expect_mirroring_watcher_is_unregistered(mock_mirroring_watcher, false);
  expect_refresh_images(mock_refresh_images_request, new_image_ids, 0);
  expect_mirror_uuid_get(m_remote_io_ctx, "updated uuid", 0);
  expect_listener_handle_update(mock_listener, "remote uuid", {}, image_ids);
  expect_listener_handle_update(mock_listener, "updated uuid", new_image_ids,
                                {});

  MirroringWatcher::get_instance().handle_rewatch_complete(0);
  ASSERT_TRUE(wait_for_update(2));

  expect_mirroring_watcher_unregister(mock_mirroring_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_pool_watcher));
}

} // namespace mirror
} // namespace rbd
