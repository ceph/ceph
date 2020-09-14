// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/stringify.h"
#include "tools/rbd_mirror/MirrorStatusUpdater.h"
#include "tools/rbd_mirror/MirrorStatusWatcher.h"
#include "tools/rbd_mirror/Threads.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/mock/MockContextWQ.h"
#include "test/rbd_mirror/mock/MockSafeTimer.h"
#include <map>
#include <string>
#include <utility>

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
struct MirrorStatusWatcher<librbd::MockTestImageCtx> {
  static MirrorStatusWatcher* s_instance;
  static MirrorStatusWatcher* create(librados::IoCtx& io_ctx,
                                     MockContextWQ* mock_context_wq) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  MOCK_METHOD1(init, void(Context*));
  MOCK_METHOD1(shut_down, void(Context*));

  MirrorStatusWatcher() {
    s_instance = this;
  }
};

MirrorStatusWatcher<librbd::MockTestImageCtx>* MirrorStatusWatcher<librbd::MockTestImageCtx>::s_instance = nullptr;

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

#include "tools/rbd_mirror/MirrorStatusUpdater.cc"

namespace rbd {
namespace mirror {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::StrEq;
using ::testing::Return;
using ::testing::WithArg;

class TestMockMirrorStatusUpdater : public TestMockFixture {
public:
  typedef MirrorStatusUpdater<librbd::MockTestImageCtx> MockMirrorStatusUpdater;
  typedef MirrorStatusWatcher<librbd::MockTestImageCtx> MockMirrorStatusWatcher;
  typedef Threads<librbd::MockTestImageCtx> MockThreads;

  typedef std::map<std::string, cls::rbd::MirrorImageSiteStatus>
      MirrorImageSiteStatuses;

  void SetUp() override {
    TestMockFixture::SetUp();

    m_mock_local_io_ctx = &get_mock_io_ctx(m_local_io_ctx);
    m_mock_threads = new MockThreads(m_threads);
  }

  void TearDown() override {
    delete m_mock_threads;
    TestMockFixture::TearDown();
  }

  void expect_timer_add_event(Context** timer_event) {
    EXPECT_CALL(*m_mock_threads->timer, add_event_after(_, _))
      .WillOnce(WithArg<1>(Invoke([timer_event](Context *ctx) {
          *timer_event = ctx;
          return ctx;
        })));
  }

  void expect_timer_cancel_event() {
    EXPECT_CALL(*m_mock_threads->timer, cancel_event(_))
      .WillOnce(Invoke([](Context* ctx) {
          delete ctx;
          return false;
        }));
  }

  void expect_work_queue(bool async) {
    EXPECT_CALL(*m_mock_threads->work_queue, queue(_, _))
      .WillOnce(Invoke([this, async](Context *ctx, int r) {
          if (async) {
            m_threads->work_queue->queue(ctx, r);
          } else {
            ctx->complete(r);
          }
        }));
  }

  void expect_mirror_status_watcher_init(
      MockMirrorStatusWatcher& mock_mirror_status_watcher, int r) {
    EXPECT_CALL(*mock_mirror_status_watcher.s_instance, init(_))
      .WillOnce(Invoke([this, r](Context* ctx) {
          m_threads->work_queue->queue(ctx, r);
        }));
  }

  void expect_mirror_status_watcher_shut_down(
      MockMirrorStatusWatcher& mock_mirror_status_watcher, int r) {
    EXPECT_CALL(*mock_mirror_status_watcher.s_instance, shut_down(_))
      .WillOnce(Invoke([this, r](Context* ctx) {
          m_threads->work_queue->queue(ctx, r);
        }));
  }

  void expect_mirror_status_update(
      const std::string& global_image_id,
      const cls::rbd::MirrorImageSiteStatus& mirror_image_status, int r) {
    EXPECT_CALL(*m_mock_local_io_ctx,
                exec(RBD_MIRRORING, _, StrEq("rbd"),
                     StrEq("mirror_image_status_set"), _, _, _, _))
      .WillOnce(WithArg<4>(Invoke(
        [r, global_image_id, mirror_image_status](bufferlist& in_bl) {
          auto bl_it = in_bl.cbegin();
          std::string decode_global_image_id;
          decode(decode_global_image_id, bl_it);
          EXPECT_EQ(global_image_id, decode_global_image_id);

          cls::rbd::MirrorImageSiteStatus decode_mirror_image_status;
          decode(decode_mirror_image_status, bl_it);
          EXPECT_EQ(mirror_image_status, decode_mirror_image_status);
          return r;
        })));
  }

  void expect_mirror_status_update(
      const MirrorImageSiteStatuses& mirror_image_site_statuses,
      const std::string& mirror_uuid, int r) {
    EXPECT_CALL(*m_mock_local_io_ctx, aio_operate(_, _, _, _, _))
      .WillOnce(Invoke([this](auto&&... args) {
          int r = m_mock_local_io_ctx->do_aio_operate(decltype(args)(args)...);
          m_mock_local_io_ctx->aio_flush();
          return r;
        }));

    for (auto [global_image_id, mirror_image_status] :
           mirror_image_site_statuses) {
      mirror_image_status.mirror_uuid = mirror_uuid;
      expect_mirror_status_update(global_image_id, mirror_image_status, r);
      if (r < 0) {
        break;
      }
    }
  }

  void fire_timer_event(Context** timer_event,
                        Context** update_task) {
    expect_timer_add_event(timer_event);

    // timer queues the update task
    EXPECT_CALL(*m_mock_threads->work_queue, queue(_, _))
      .WillOnce(WithArg<0>(Invoke([update_task](Context* ctx) mutable {
          *update_task = ctx;
        })));

    // fire the timer task
    {
      std::lock_guard timer_locker{m_mock_threads->timer_lock};
      ceph_assert(*timer_event != nullptr);
      (*timer_event)->complete(0);
    }
  }

  void init_mirror_status_updater(
      MockMirrorStatusUpdater& mock_mirror_status_updater,
      MockMirrorStatusWatcher& mock_mirror_status_watcher,
      Context** timer_event) {
    expect_timer_add_event(timer_event);
    expect_mirror_status_watcher_init(mock_mirror_status_watcher, 0);
    expect_work_queue(true);

    C_SaferCond ctx;
    mock_mirror_status_updater.init(&ctx);
    ASSERT_EQ(0, ctx.wait());
  }

  void shut_down_mirror_status_updater(
      MockMirrorStatusUpdater& mock_mirror_status_updater,
      MockMirrorStatusWatcher& mock_mirror_status_watcher) {
    expect_timer_cancel_event();
    expect_mirror_status_watcher_shut_down(mock_mirror_status_watcher, 0);
    expect_work_queue(true);

    C_SaferCond ctx;
    mock_mirror_status_updater.shut_down(&ctx);
    ASSERT_EQ(0, ctx.wait());
  }

  librados::MockTestMemIoCtxImpl* m_mock_local_io_ctx = nullptr;
  MockThreads* m_mock_threads = nullptr;
};

TEST_F(TestMockMirrorStatusUpdater, InitShutDown) {
  MockMirrorStatusUpdater mock_mirror_status_updater(m_local_io_ctx,
                                                     m_mock_threads, "");
  MockMirrorStatusWatcher* mock_mirror_status_watcher =
    new MockMirrorStatusWatcher();

  Context* timer_event = nullptr;
  init_mirror_status_updater(mock_mirror_status_updater,
                             *mock_mirror_status_watcher, &timer_event);

  shut_down_mirror_status_updater(mock_mirror_status_updater,
                                  *mock_mirror_status_watcher);
}

TEST_F(TestMockMirrorStatusUpdater, InitStatusWatcherError) {
  MockMirrorStatusUpdater mock_mirror_status_updater(m_local_io_ctx,
                                                     m_mock_threads, "");
  MockMirrorStatusWatcher* mock_mirror_status_watcher =
    new MockMirrorStatusWatcher();

  Context* timer_event = nullptr;
  expect_timer_add_event(&timer_event);
  expect_mirror_status_watcher_init(*mock_mirror_status_watcher, -EINVAL);
  expect_timer_cancel_event();
  expect_work_queue(true);

  C_SaferCond ctx;
  mock_mirror_status_updater.init(&ctx);
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockMirrorStatusUpdater, ShutDownStatusWatcherError) {
  MockMirrorStatusUpdater mock_mirror_status_updater(m_local_io_ctx,
                                                     m_mock_threads, "");
  MockMirrorStatusWatcher* mock_mirror_status_watcher =
    new MockMirrorStatusWatcher();

  Context* timer_event = nullptr;
  init_mirror_status_updater(mock_mirror_status_updater,
                             *mock_mirror_status_watcher, &timer_event);

  C_SaferCond on_shutdown;
  expect_timer_cancel_event();
  expect_mirror_status_watcher_shut_down(*mock_mirror_status_watcher, -EINVAL);
  expect_work_queue(true);
  mock_mirror_status_updater.shut_down(&on_shutdown);

  ASSERT_EQ(-EINVAL, on_shutdown.wait());
}

TEST_F(TestMockMirrorStatusUpdater, SmallBatch) {
  MockMirrorStatusUpdater mock_mirror_status_updater(m_local_io_ctx,
                                                     m_mock_threads, "");
  MockMirrorStatusWatcher* mock_mirror_status_watcher =
    new MockMirrorStatusWatcher();

  InSequence seq;

  Context* timer_event = nullptr;
  init_mirror_status_updater(mock_mirror_status_updater,
                             *mock_mirror_status_watcher, &timer_event);

  MirrorImageSiteStatuses mirror_image_site_statuses;
  for (auto i = 0; i < 100; ++i) {
    auto pair = mirror_image_site_statuses.emplace(
      stringify(i), cls::rbd::MirrorImageSiteStatus{});
    mock_mirror_status_updater.set_mirror_image_status(pair.first->first,
                                                       pair.first->second,
                                                       false);
  }

  Context* update_task = nullptr;
  fire_timer_event(&timer_event, &update_task);

  expect_mirror_status_update(mirror_image_site_statuses, "", 0);
  update_task->complete(0);

  shut_down_mirror_status_updater(mock_mirror_status_updater,
                                  *mock_mirror_status_watcher);
}

TEST_F(TestMockMirrorStatusUpdater, LargeBatch) {
  MockMirrorStatusUpdater mock_mirror_status_updater(m_local_io_ctx,
                                                     m_mock_threads, "");
  MockMirrorStatusWatcher* mock_mirror_status_watcher =
    new MockMirrorStatusWatcher();

  InSequence seq;

  Context* timer_event = nullptr;
  init_mirror_status_updater(mock_mirror_status_updater,
                             *mock_mirror_status_watcher, &timer_event);

  MirrorImageSiteStatuses mirror_image_site_statuses;
  for (auto i = 0; i < 200; ++i) {
    auto pair = mirror_image_site_statuses.emplace(
      stringify(i), cls::rbd::MirrorImageSiteStatus{});
    mock_mirror_status_updater.set_mirror_image_status(pair.first->first,
                                                       pair.first->second,
                                                       false);
  }

  auto it_1 = mirror_image_site_statuses.begin();
  auto it_2 = mirror_image_site_statuses.begin();
  std::advance(it_2, 100);
  MirrorImageSiteStatuses mirror_image_site_statuses_1{it_1, it_2};

  it_1 = it_2;
  std::advance(it_2, 100);
  MirrorImageSiteStatuses mirror_image_site_statuses_2{it_1, it_2};

  Context* update_task = nullptr;
  fire_timer_event(&timer_event, &update_task);

  expect_mirror_status_update(mirror_image_site_statuses_1, "", 0);
  expect_mirror_status_update(mirror_image_site_statuses_2, "", 0);
  update_task->complete(0);

  shut_down_mirror_status_updater(mock_mirror_status_updater,
                                  *mock_mirror_status_watcher);
}

TEST_F(TestMockMirrorStatusUpdater, OverwriteStatus) {
  MockMirrorStatusUpdater mock_mirror_status_updater(m_local_io_ctx,
                                                     m_mock_threads, "");
  MockMirrorStatusWatcher* mock_mirror_status_watcher =
    new MockMirrorStatusWatcher();

  InSequence seq;

  Context* timer_event = nullptr;
  init_mirror_status_updater(mock_mirror_status_updater,
                             *mock_mirror_status_watcher, &timer_event);

  mock_mirror_status_updater.set_mirror_image_status("1", {}, false);
  mock_mirror_status_updater.set_mirror_image_status(
    "1", {"", cls::rbd::MIRROR_IMAGE_STATUS_STATE_REPLAYING, "description"},
    false);

  Context* update_task = nullptr;
  fire_timer_event(&timer_event, &update_task);

  expect_mirror_status_update(
    {{"1", cls::rbd::MirrorImageSiteStatus{
        "", cls::rbd::MIRROR_IMAGE_STATUS_STATE_REPLAYING, "description"}}},
    "", 0);
  update_task->complete(0);

  shut_down_mirror_status_updater(mock_mirror_status_updater,
                                  *mock_mirror_status_watcher);
}

TEST_F(TestMockMirrorStatusUpdater, OverwriteStatusInFlight) {
  MockMirrorStatusUpdater mock_mirror_status_updater(m_local_io_ctx,
                                                     m_mock_threads, "");
  MockMirrorStatusWatcher* mock_mirror_status_watcher =
    new MockMirrorStatusWatcher();

  InSequence seq;

  Context* timer_event = nullptr;
  init_mirror_status_updater(mock_mirror_status_updater,
                             *mock_mirror_status_watcher, &timer_event);

  mock_mirror_status_updater.set_mirror_image_status("1", {}, false);

  Context* update_task = nullptr;
  fire_timer_event(&timer_event, &update_task);

  EXPECT_CALL(*m_mock_local_io_ctx, aio_operate(_, _, _, _, _))
    .WillOnce(Invoke([this, &mock_mirror_status_updater](auto&&... args) {
        mock_mirror_status_updater.set_mirror_image_status(
          "1", {"", cls::rbd::MIRROR_IMAGE_STATUS_STATE_REPLAYING,
                "description"},
          true);

        int r = m_mock_local_io_ctx->do_aio_operate(decltype(args)(args)...);
        m_mock_local_io_ctx->aio_flush();
        return r;
      }));
  expect_mirror_status_update("1", cls::rbd::MirrorImageSiteStatus{}, 0);
  expect_work_queue(false);
  expect_mirror_status_update(
    {{"1", cls::rbd::MirrorImageSiteStatus{
        "", cls::rbd::MIRROR_IMAGE_STATUS_STATE_REPLAYING, "description"}}},
    "", 0);

  update_task->complete(0);

  shut_down_mirror_status_updater(mock_mirror_status_updater,
                                  *mock_mirror_status_watcher);
}

TEST_F(TestMockMirrorStatusUpdater, ImmediateUpdate) {
  MockMirrorStatusUpdater mock_mirror_status_updater(m_local_io_ctx,
                                                     m_mock_threads, "");
  MockMirrorStatusWatcher* mock_mirror_status_watcher =
    new MockMirrorStatusWatcher();

  InSequence seq;

  Context* timer_event = nullptr;
  init_mirror_status_updater(mock_mirror_status_updater,
                             *mock_mirror_status_watcher, &timer_event);

  expect_work_queue(false);
  expect_mirror_status_update({{"1", cls::rbd::MirrorImageSiteStatus{}}},
                              "", 0);
  mock_mirror_status_updater.set_mirror_image_status("1", {}, true);

  shut_down_mirror_status_updater(mock_mirror_status_updater,
                                  *mock_mirror_status_watcher);
}

TEST_F(TestMockMirrorStatusUpdater, RemoveIdleStatus) {
  MockMirrorStatusUpdater mock_mirror_status_updater(m_local_io_ctx,
                                                     m_mock_threads, "");
  MockMirrorStatusWatcher* mock_mirror_status_watcher =
    new MockMirrorStatusWatcher();

  InSequence seq;

  Context* timer_event = nullptr;
  init_mirror_status_updater(mock_mirror_status_updater,
                             *mock_mirror_status_watcher, &timer_event);

  mock_mirror_status_updater.set_mirror_image_status("1", {}, false);

  C_SaferCond ctx;
  expect_work_queue(true);
  mock_mirror_status_updater.remove_mirror_image_status("1", &ctx);
  ASSERT_EQ(0, ctx.wait());

  shut_down_mirror_status_updater(mock_mirror_status_updater,
                                  *mock_mirror_status_watcher);
}

TEST_F(TestMockMirrorStatusUpdater, RemoveInFlightStatus) {
  MockMirrorStatusUpdater mock_mirror_status_updater(m_local_io_ctx,
                                                     m_mock_threads, "");
  MockMirrorStatusWatcher* mock_mirror_status_watcher =
    new MockMirrorStatusWatcher();

  InSequence seq;

  Context* timer_event = nullptr;
  init_mirror_status_updater(mock_mirror_status_updater,
                             *mock_mirror_status_watcher, &timer_event);

  mock_mirror_status_updater.set_mirror_image_status("1", {}, false);

  Context* update_task = nullptr;
  fire_timer_event(&timer_event, &update_task);

  C_SaferCond on_removed;
  EXPECT_CALL(*m_mock_local_io_ctx, aio_operate(_, _, _, _, _))
    .WillOnce(Invoke(
      [this, &mock_mirror_status_updater, &on_removed](auto&&... args) {
        mock_mirror_status_updater.remove_mirror_image_status("1", &on_removed);

        int r = m_mock_local_io_ctx->do_aio_operate(decltype(args)(args)...);
        m_mock_local_io_ctx->aio_flush();
        return r;
      }));
  update_task->complete(0);
  ASSERT_EQ(0, on_removed.wait());

  shut_down_mirror_status_updater(mock_mirror_status_updater,
                                  *mock_mirror_status_watcher);
}

TEST_F(TestMockMirrorStatusUpdater, ShutDownWhileUpdating) {
  MockMirrorStatusUpdater mock_mirror_status_updater(m_local_io_ctx,
                                                     m_mock_threads, "");
  MockMirrorStatusWatcher* mock_mirror_status_watcher =
    new MockMirrorStatusWatcher();

  InSequence seq;

  Context* timer_event = nullptr;
  init_mirror_status_updater(mock_mirror_status_updater,
                             *mock_mirror_status_watcher, &timer_event);

  mock_mirror_status_updater.set_mirror_image_status("1", {}, false);

  Context* update_task = nullptr;
  fire_timer_event(&timer_event, &update_task);

  C_SaferCond on_shutdown;
  EXPECT_CALL(*m_mock_local_io_ctx, aio_operate(_, _, _, _, _))
    .WillOnce(Invoke(
      [this, &mock_mirror_status_updater, &on_shutdown](auto&&... args) {
        mock_mirror_status_updater.shut_down(&on_shutdown);
        m_threads->work_queue->drain();

        int r = m_mock_local_io_ctx->do_aio_operate(decltype(args)(args)...);
        m_mock_local_io_ctx->aio_flush();
        return r;
      }));

  expect_timer_cancel_event();
  expect_mirror_status_watcher_shut_down(*mock_mirror_status_watcher, 0);

  update_task->complete(0);
  ASSERT_EQ(0, on_shutdown.wait());
}

TEST_F(TestMockMirrorStatusUpdater, MirrorPeerSitePing) {
  MockMirrorStatusUpdater mock_mirror_status_updater(m_local_io_ctx,
                                                     m_mock_threads,
                                                     "mirror uuid");
  MockMirrorStatusWatcher* mock_mirror_status_watcher =
    new MockMirrorStatusWatcher();

  InSequence seq;

  Context* timer_event = nullptr;
  init_mirror_status_updater(mock_mirror_status_updater,
                             *mock_mirror_status_watcher, &timer_event);

  MirrorImageSiteStatuses mirror_image_site_statuses;
  for (auto i = 0; i < 100; ++i) {
    auto pair = mirror_image_site_statuses.emplace(
      stringify(i), cls::rbd::MirrorImageSiteStatus{});
    mock_mirror_status_updater.set_mirror_image_status(pair.first->first,
                                                       pair.first->second,
                                                       false);
  }

  Context* update_task = nullptr;
  fire_timer_event(&timer_event, &update_task);

  expect_mirror_status_update(mirror_image_site_statuses, "mirror uuid", 0);
  update_task->complete(0);

  shut_down_mirror_status_updater(mock_mirror_status_updater,
                                  *mock_mirror_status_watcher);
}

} // namespace mirror
} // namespace rbd
