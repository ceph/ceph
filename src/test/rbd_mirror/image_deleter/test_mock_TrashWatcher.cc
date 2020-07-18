// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/mock/MockContextWQ.h"
#include "test/rbd_mirror/mock/MockSafeTimer.h"
#include "librbd/TrashWatcher.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_deleter/TrashWatcher.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

struct MockTrashWatcher {
  static MockTrashWatcher *s_instance;
  static MockTrashWatcher &get_instance() {
    ceph_assert(s_instance != nullptr);
    return *s_instance;
  }

  MockTrashWatcher() {
    s_instance = this;
  }

  MOCK_CONST_METHOD0(is_unregistered, bool());
  MOCK_METHOD1(register_watch, void(Context*));
  MOCK_METHOD1(unregister_watch, void(Context*));
};

template <>
struct TrashWatcher<MockTestImageCtx> {
  static TrashWatcher *s_instance;

  TrashWatcher(librados::IoCtx &io_ctx, ::MockContextWQ *work_queue) {
    s_instance = this;
  }
  virtual ~TrashWatcher() {
  }

  static TrashWatcher<MockTestImageCtx> &get_instance() {
    ceph_assert(s_instance != nullptr);
    return *s_instance;
  }

  virtual void handle_rewatch_complete(int r) = 0;

  virtual void handle_image_added(const std::string &image_id,
                                  const cls::rbd::TrashImageSpec& spec) = 0;
  virtual void handle_image_removed(const std::string &image_id) = 0;

  bool is_unregistered() const {
    return MockTrashWatcher::get_instance().is_unregistered();
  }
  void register_watch(Context *ctx) {
    MockTrashWatcher::get_instance().register_watch(ctx);
  }
  void unregister_watch(Context *ctx) {
    MockTrashWatcher::get_instance().unregister_watch(ctx);
  }
};

MockTrashWatcher *MockTrashWatcher::s_instance = nullptr;
TrashWatcher<MockTestImageCtx> *TrashWatcher<MockTestImageCtx>::s_instance = nullptr;

} // namespace librbd

namespace rbd {
namespace mirror {

template <>
struct Threads<librbd::MockTestImageCtx> {
  MockSafeTimer *timer;
  ceph::mutex &timer_lock;

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

} // namespace mirror
} // namespace rbd

#include "tools/rbd_mirror/image_deleter/TrashWatcher.cc"

namespace rbd {
namespace mirror {
namespace image_deleter {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockImageDeleterTrashWatcher : public TestMockFixture {
public:
  typedef TrashWatcher<librbd::MockTestImageCtx> MockTrashWatcher;
  typedef Threads<librbd::MockTestImageCtx> MockThreads;
  typedef librbd::MockTrashWatcher MockLibrbdTrashWatcher;
  typedef librbd::TrashWatcher<librbd::MockTestImageCtx> LibrbdTrashWatcher;

  struct MockListener : TrashListener {
    MOCK_METHOD2(handle_trash_image, void(const std::string&,
					  const ceph::real_clock::time_point&));
  };

  void expect_work_queue(MockThreads &mock_threads) {
    EXPECT_CALL(*mock_threads.work_queue, queue(_, _))
      .WillRepeatedly(Invoke([this](Context *ctx, int r) {
          m_threads->work_queue->queue(ctx, r);
        }));
  }

  void expect_trash_watcher_is_unregistered(MockLibrbdTrashWatcher &mock_trash_watcher,
                                            bool unregistered) {
    EXPECT_CALL(mock_trash_watcher, is_unregistered())
      .WillOnce(Return(unregistered));
  }

  void expect_trash_watcher_register(MockLibrbdTrashWatcher &mock_trash_watcher,
                                     int r) {
    EXPECT_CALL(mock_trash_watcher, register_watch(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_trash_watcher_unregister(MockLibrbdTrashWatcher &mock_trash_watcher,
                                       int r) {
    EXPECT_CALL(mock_trash_watcher, unregister_watch(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_create_trash(librados::IoCtx &io_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(io_ctx), create(RBD_TRASH, false, _))
      .WillOnce(Return(r));
  }

  void expect_trash_list(librados::IoCtx &io_ctx,
                         const std::string& last_image_id,
                         std::map<std::string, cls::rbd::TrashImageSpec>&& images,
                         int r) {
    bufferlist bl;
    encode(last_image_id, bl);
    encode(static_cast<size_t>(1024), bl);

    bufferlist out_bl;
    encode(images, out_bl);

    EXPECT_CALL(get_mock_io_ctx(io_ctx),
                exec(RBD_TRASH, _, StrEq("rbd"), StrEq("trash_list"),
                     ContentsEqual(bl), _, _, _))
      .WillOnce(DoAll(WithArg<5>(Invoke([out_bl](bufferlist *bl) {
                          *bl = out_bl;
                        })),
                      Return(r)));
  }

  void expect_timer_add_event(MockThreads &mock_threads) {
    EXPECT_CALL(*mock_threads.timer, add_event_after(_, _))
      .WillOnce(DoAll(WithArg<1>(Invoke([this](Context *ctx) {
                        auto wrapped_ctx =
			  new LambdaContext([this, ctx](int r) {
			      std::lock_guard timer_locker{m_threads->timer_lock};
			      ctx->complete(r);
			    });
			m_threads->work_queue->queue(wrapped_ctx, 0);
                      })),
                      ReturnArg<1>()));
  }

  void expect_handle_trash_image(MockListener& mock_listener,
                                 const std::string& global_image_id) {
    EXPECT_CALL(mock_listener, handle_trash_image(global_image_id, _));
  }

  int when_shut_down(MockTrashWatcher &mock_trash_watcher) {
    C_SaferCond ctx;
    mock_trash_watcher.shut_down(&ctx);
    return ctx.wait();
  }

};

TEST_F(TestMockImageDeleterTrashWatcher, EmptyPool) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  expect_create_trash(m_local_io_ctx, 0);

  MockLibrbdTrashWatcher mock_librbd_trash_watcher;
  expect_trash_watcher_is_unregistered(mock_librbd_trash_watcher, true);
  expect_trash_watcher_register(mock_librbd_trash_watcher, 0);
  expect_trash_list(m_local_io_ctx, "", {}, 0);

  MockListener mock_listener;
  MockTrashWatcher mock_trash_watcher(m_local_io_ctx, &mock_threads,
                                      mock_listener);
  C_SaferCond ctx;
  mock_trash_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());

  expect_trash_watcher_unregister(mock_librbd_trash_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_trash_watcher));
}

TEST_F(TestMockImageDeleterTrashWatcher, NonEmptyPool) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  MockListener mock_listener;
  expect_handle_trash_image(mock_listener, "image0");

  InSequence seq;
  expect_create_trash(m_local_io_ctx, 0);

  MockLibrbdTrashWatcher mock_librbd_trash_watcher;
  expect_trash_watcher_is_unregistered(mock_librbd_trash_watcher, true);
  expect_trash_watcher_register(mock_librbd_trash_watcher, 0);

  std::map<std::string, cls::rbd::TrashImageSpec> images;
  images["image0"] = {cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING, "name", {}, {}};
  for (auto idx = 1; idx < 1024; ++idx) {
    images["image" + stringify(idx)] = {};
  }
  expect_trash_list(m_local_io_ctx, "", std::move(images), 0);

  images.clear();
  for (auto idx = 1024; idx < 2000; ++idx) {
    images["image" + stringify(idx)] = {};
  }
  expect_trash_list(m_local_io_ctx, "image999", std::move(images), 0);

  MockTrashWatcher mock_trash_watcher(m_local_io_ctx, &mock_threads,
                                      mock_listener);
  C_SaferCond ctx;
  mock_trash_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());
  m_threads->work_queue->drain();

  expect_trash_watcher_unregister(mock_librbd_trash_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_trash_watcher));
}

TEST_F(TestMockImageDeleterTrashWatcher, Notify) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  MockListener mock_listener;
  expect_handle_trash_image(mock_listener, "image1");

  InSequence seq;
  expect_create_trash(m_local_io_ctx, 0);

  MockLibrbdTrashWatcher mock_librbd_trash_watcher;
  expect_trash_watcher_is_unregistered(mock_librbd_trash_watcher, true);
  expect_trash_watcher_register(mock_librbd_trash_watcher, 0);
  expect_trash_list(m_local_io_ctx, "", {}, 0);

  MockTrashWatcher mock_trash_watcher(m_local_io_ctx, &mock_threads,
                                      mock_listener);
  C_SaferCond ctx;
  mock_trash_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());

  LibrbdTrashWatcher::get_instance().handle_image_added(
    "image1", {cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING, "name", {}, {}});
  m_threads->work_queue->drain();

  expect_trash_watcher_unregister(mock_librbd_trash_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_trash_watcher));
}

TEST_F(TestMockImageDeleterTrashWatcher, CreateBlacklist) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  expect_create_trash(m_local_io_ctx, -EBLACKLISTED);

  MockListener mock_listener;
  MockTrashWatcher mock_trash_watcher(m_local_io_ctx, &mock_threads,
                                      mock_listener);
  C_SaferCond ctx;
  mock_trash_watcher.init(&ctx);
  ASSERT_EQ(-EBLACKLISTED, ctx.wait());

  MockLibrbdTrashWatcher mock_librbd_trash_watcher;
  expect_trash_watcher_unregister(mock_librbd_trash_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_trash_watcher));
}

TEST_F(TestMockImageDeleterTrashWatcher, CreateDNE) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  expect_create_trash(m_local_io_ctx, -ENOENT);

  MockListener mock_listener;
  MockTrashWatcher mock_trash_watcher(m_local_io_ctx, &mock_threads,
                                      mock_listener);
  C_SaferCond ctx;
  mock_trash_watcher.init(&ctx);
  ASSERT_EQ(-ENOENT, ctx.wait());

  MockLibrbdTrashWatcher mock_librbd_trash_watcher;
  expect_trash_watcher_unregister(mock_librbd_trash_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_trash_watcher));
}

TEST_F(TestMockImageDeleterTrashWatcher, CreateError) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  expect_create_trash(m_local_io_ctx, -EINVAL);

  expect_timer_add_event(mock_threads);
  expect_create_trash(m_local_io_ctx, 0);

  MockLibrbdTrashWatcher mock_librbd_trash_watcher;
  expect_trash_watcher_is_unregistered(mock_librbd_trash_watcher, true);
  expect_trash_watcher_register(mock_librbd_trash_watcher, 0);

  MockListener mock_listener;
  MockTrashWatcher mock_trash_watcher(m_local_io_ctx, &mock_threads,
                                      mock_listener);
  C_SaferCond ctx;
  mock_trash_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());

  expect_trash_watcher_unregister(mock_librbd_trash_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_trash_watcher));
}

TEST_F(TestMockImageDeleterTrashWatcher, RegisterWatcherBlacklist) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  expect_create_trash(m_local_io_ctx, 0);

  MockLibrbdTrashWatcher mock_librbd_trash_watcher;
  expect_trash_watcher_is_unregistered(mock_librbd_trash_watcher, true);
  expect_trash_watcher_register(mock_librbd_trash_watcher, -EBLACKLISTED);

  MockListener mock_listener;
  MockTrashWatcher mock_trash_watcher(m_local_io_ctx, &mock_threads,
                                      mock_listener);
  C_SaferCond ctx;
  mock_trash_watcher.init(&ctx);
  ASSERT_EQ(-EBLACKLISTED, ctx.wait());

  expect_trash_watcher_unregister(mock_librbd_trash_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_trash_watcher));
}

TEST_F(TestMockImageDeleterTrashWatcher, RegisterWatcherError) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  expect_create_trash(m_local_io_ctx, 0);

  MockLibrbdTrashWatcher mock_librbd_trash_watcher;
  expect_trash_watcher_is_unregistered(mock_librbd_trash_watcher, true);
  expect_trash_watcher_register(mock_librbd_trash_watcher, -EINVAL);
  expect_timer_add_event(mock_threads);

  expect_create_trash(m_local_io_ctx, 0);

  expect_trash_watcher_is_unregistered(mock_librbd_trash_watcher, true);
  expect_trash_watcher_register(mock_librbd_trash_watcher, 0);

  MockListener mock_listener;
  MockTrashWatcher mock_trash_watcher(m_local_io_ctx, &mock_threads,
                                      mock_listener);
  C_SaferCond ctx;
  mock_trash_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());

  expect_trash_watcher_unregister(mock_librbd_trash_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_trash_watcher));
}

TEST_F(TestMockImageDeleterTrashWatcher, TrashListBlacklist) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  expect_create_trash(m_local_io_ctx, 0);

  MockLibrbdTrashWatcher mock_librbd_trash_watcher;
  expect_trash_watcher_is_unregistered(mock_librbd_trash_watcher, true);
  expect_trash_watcher_register(mock_librbd_trash_watcher, 0);
  expect_trash_list(m_local_io_ctx, "", {}, -EBLACKLISTED);

  MockListener mock_listener;
  MockTrashWatcher mock_trash_watcher(m_local_io_ctx, &mock_threads,
                                      mock_listener);
  C_SaferCond ctx;
  mock_trash_watcher.init(&ctx);
  ASSERT_EQ(-EBLACKLISTED, ctx.wait());

  expect_trash_watcher_unregister(mock_librbd_trash_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_trash_watcher));
}

TEST_F(TestMockImageDeleterTrashWatcher, TrashListError) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  expect_create_trash(m_local_io_ctx, 0);

  MockLibrbdTrashWatcher mock_librbd_trash_watcher;
  expect_trash_watcher_is_unregistered(mock_librbd_trash_watcher, true);
  expect_trash_watcher_register(mock_librbd_trash_watcher, 0);
  expect_trash_list(m_local_io_ctx, "", {}, -EINVAL);

  expect_timer_add_event(mock_threads);
  expect_create_trash(m_local_io_ctx, 0);

  expect_trash_watcher_is_unregistered(mock_librbd_trash_watcher, false);
  expect_trash_list(m_local_io_ctx, "", {}, 0);

  MockListener mock_listener;
  MockTrashWatcher mock_trash_watcher(m_local_io_ctx, &mock_threads,
                                      mock_listener);
  C_SaferCond ctx;
  mock_trash_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());

  expect_trash_watcher_unregister(mock_librbd_trash_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_trash_watcher));
}

TEST_F(TestMockImageDeleterTrashWatcher, Rewatch) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  expect_create_trash(m_local_io_ctx, 0);

  MockLibrbdTrashWatcher mock_librbd_trash_watcher;
  expect_trash_watcher_is_unregistered(mock_librbd_trash_watcher, true);
  expect_trash_watcher_register(mock_librbd_trash_watcher, 0);
  expect_trash_list(m_local_io_ctx, "", {}, 0);

  MockListener mock_listener;
  MockTrashWatcher mock_trash_watcher(m_local_io_ctx, &mock_threads,
                                      mock_listener);
  C_SaferCond ctx;
  mock_trash_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());

  expect_timer_add_event(mock_threads);
  expect_create_trash(m_local_io_ctx, 0);

  expect_trash_watcher_is_unregistered(mock_librbd_trash_watcher, false);
  expect_trash_list(m_local_io_ctx, "", {}, 0);
  LibrbdTrashWatcher::get_instance().handle_rewatch_complete(0);
  m_threads->work_queue->drain();

  expect_trash_watcher_unregister(mock_librbd_trash_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_trash_watcher));
}

TEST_F(TestMockImageDeleterTrashWatcher, RewatchBlacklist) {
  MockThreads mock_threads(m_threads);
  expect_work_queue(mock_threads);

  InSequence seq;
  expect_create_trash(m_local_io_ctx, 0);

  MockLibrbdTrashWatcher mock_librbd_trash_watcher;
  expect_trash_watcher_is_unregistered(mock_librbd_trash_watcher, true);
  expect_trash_watcher_register(mock_librbd_trash_watcher, 0);
  expect_trash_list(m_local_io_ctx, "", {}, 0);

  MockListener mock_listener;
  MockTrashWatcher mock_trash_watcher(m_local_io_ctx, &mock_threads,
                                      mock_listener);
  C_SaferCond ctx;
  mock_trash_watcher.init(&ctx);
  ASSERT_EQ(0, ctx.wait());

  LibrbdTrashWatcher::get_instance().handle_rewatch_complete(-EBLACKLISTED);
  m_threads->work_queue->drain();

  expect_trash_watcher_unregister(mock_librbd_trash_watcher, 0);
  ASSERT_EQ(0, when_shut_down(mock_trash_watcher));
}

} // namespace image_deleter
} // namespace mirror
} // namespace rbd
