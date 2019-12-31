// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/journal/cls_journal_types.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "tools/rbd_mirror/ImageDeleter.h"
#include "tools/rbd_mirror/ImageReplayer.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/MirrorStatusUpdater.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/BootstrapRequest.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/Replayer.h"
#include "tools/rbd_mirror/image_replayer/ReplayerListener.h"
#include "tools/rbd_mirror/image_replayer/Utils.h"
#include "tools/rbd_mirror/image_replayer/journal/Replayer.h"
#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/mock/MockContextWQ.h"
#include "test/rbd_mirror/mock/MockSafeTimer.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace journal {

template <>
struct TypeTraits<MockTestImageCtx> {
  typedef ::journal::MockJournalerProxy Journaler;
};

} // namespace journal
} // namespace librbd

namespace rbd {
namespace mirror {

template <>
struct ImageDeleter<librbd::MockTestImageCtx> {
  static ImageDeleter* s_instance;

  static void trash_move(librados::IoCtx& local_io_ctx,
                         const std::string& global_image_id, bool resync,
                         MockContextWQ* work_queue, Context* on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->trash_move(global_image_id, resync, on_finish);
  }

  MOCK_METHOD3(trash_move, void(const std::string&, bool, Context*));

  ImageDeleter() {
    s_instance = this;
  }
};

ImageDeleter<librbd::MockTestImageCtx>* ImageDeleter<librbd::MockTestImageCtx>::s_instance = nullptr;

template <>
struct MirrorStatusUpdater<librbd::MockTestImageCtx> {

  MOCK_METHOD1(exists, bool(const std::string&));
  MOCK_METHOD3(set_mirror_image_status,
               void(const std::string&, const cls::rbd::MirrorImageSiteStatus&,
                    bool));
  MOCK_METHOD2(remove_mirror_image_status, void(const std::string&, Context*));
};

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

template<>
class InstanceWatcher<librbd::MockTestImageCtx> {
};

namespace image_replayer {

template<>
struct BootstrapRequest<librbd::MockTestImageCtx> {
  static BootstrapRequest* s_instance;
  librbd::MockTestImageCtx **image_ctx = nullptr;
  std::string* local_image_id = nullptr;
  bool *do_resync = nullptr;
  Context *on_finish = nullptr;

  static BootstrapRequest* create(
      Threads<librbd::MockTestImageCtx>* threads,
      librados::IoCtx &local_io_ctx,
      librados::IoCtx &remote_io_ctx,
      rbd::mirror::InstanceWatcher<librbd::MockTestImageCtx> *instance_watcher,
      const std::string &remote_image_id,
      const std::string &global_image_id,
      const std::string &local_mirror_uuid,
      ::journal::CacheManagerHandler *cache_manager_handler,
      rbd::mirror::ProgressContext *progress_ctx,
      librbd::MockTestImageCtx **local_image_ctx,
      std::string* local_image_id,
      std::string* remote_mirror_uuid,
      ::journal::MockJournalerProxy** remote_journaler,
      bool *do_resync, Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->image_ctx = local_image_ctx;
    s_instance->local_image_id = local_image_id;
    s_instance->do_resync = do_resync;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  BootstrapRequest() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }

  ~BootstrapRequest() {
    ceph_assert(s_instance == this);
    s_instance = nullptr;
  }

  void put() {
  }

  void get() {
  }

  std::string get_local_image_name() const {
    return "local image name";
  }

  inline bool is_syncing() const {
    return false;
  }

  MOCK_METHOD0(send, void());
  MOCK_METHOD0(cancel, void());
};

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

BootstrapRequest<librbd::MockTestImageCtx>* BootstrapRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
CloseImageRequest<librbd::MockTestImageCtx>* CloseImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

namespace journal {

template <>
struct Replayer<librbd::MockTestImageCtx> : public image_replayer::Replayer {
  static Replayer* s_instance;
  librbd::MockTestImageCtx** local_image_ctx;
  image_replayer::ReplayerListener* replayer_listener;

  static Replayer* create(librbd::MockTestImageCtx** local_image_ctx,
                          ::journal::MockJournalerProxy* remote_journaler,
                          const std::string& local_mirror_uuid,
                          const std::string& remote_mirror_uuid,
                          image_replayer::ReplayerListener* replayer_listener,
                          Threads<librbd::MockTestImageCtx>* threads) {
    ceph_assert(s_instance != nullptr);
    ceph_assert(local_image_ctx != nullptr);
    s_instance->local_image_ctx = local_image_ctx;
    s_instance->replayer_listener = replayer_listener;
    return s_instance;
  }

  Replayer() {
    s_instance = this;
  }

  MOCK_METHOD0(destroy, void());

  MOCK_METHOD1(init, void(Context*));
  MOCK_METHOD1(shut_down, void(Context*));
  MOCK_METHOD1(flush, void(Context*));

  MOCK_METHOD2(get_replay_status, bool(std::string*, Context*));

  MOCK_CONST_METHOD0(is_replaying, bool());
  MOCK_CONST_METHOD0(is_resync_requested, bool());
  MOCK_CONST_METHOD0(get_error_code, int());
  MOCK_CONST_METHOD0(get_error_description, std::string());
};

Replayer<librbd::MockTestImageCtx>* Replayer<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/ImageReplayer.cc"

namespace rbd {
namespace mirror {

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::MatcherCast;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SetArgPointee;
using ::testing::WithArg;

class TestMockImageReplayer : public TestMockFixture {
public:
  typedef Threads<librbd::MockTestImageCtx> MockThreads;
  typedef ImageDeleter<librbd::MockTestImageCtx> MockImageDeleter;
  typedef MirrorStatusUpdater<librbd::MockTestImageCtx> MockMirrorStatusUpdater;
  typedef image_replayer::BootstrapRequest<librbd::MockTestImageCtx> MockBootstrapRequest;
  typedef image_replayer::CloseImageRequest<librbd::MockTestImageCtx> MockCloseImageRequest;
  typedef image_replayer::journal::Replayer<librbd::MockTestImageCtx> MockJournalReplayer;
  typedef ImageReplayer<librbd::MockTestImageCtx> MockImageReplayer;
  typedef InstanceWatcher<librbd::MockTestImageCtx> MockInstanceWatcher;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_remote_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_remote_io_ctx, m_image_name, &m_remote_image_ctx));
  }

  void TearDown() override {
    delete m_image_replayer;

    TestMockFixture::TearDown();
  }

  void create_local_image() {
    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
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

  void expect_trash_move(MockImageDeleter& mock_image_deleter,
                         const std::string& global_image_id,
                         bool ignore_orphan, int r) {
    EXPECT_CALL(mock_image_deleter,
                trash_move(global_image_id, ignore_orphan, _))
      .WillOnce(WithArg<2>(Invoke([this, r](Context* ctx) {
                             m_threads->work_queue->queue(ctx, r);
                           })));
  }

  bufferlist encode_tag_data(const librbd::journal::TagData &tag_data) {
    bufferlist bl;
    encode(tag_data, bl);
    return bl;
  }

  void expect_send(MockBootstrapRequest &mock_bootstrap_request,
                   librbd::MockTestImageCtx &mock_local_image_ctx,
                   bool do_resync, int r) {
    EXPECT_CALL(mock_bootstrap_request, send())
      .WillOnce(Invoke([&mock_bootstrap_request, &mock_local_image_ctx,
                        do_resync, r]() {
            if (r == 0) {
              *mock_bootstrap_request.image_ctx = &mock_local_image_ctx;
              *mock_bootstrap_request.do_resync = do_resync;
            } else if (r == -ENOLINK) {
              *mock_bootstrap_request.local_image_id = mock_local_image_ctx.id;
            }
            mock_bootstrap_request.on_finish->complete(r);
          }));
  }

  void expect_init(MockJournalReplayer& mock_journal_replayer, int r) {
    EXPECT_CALL(mock_journal_replayer, init(_))
      .WillOnce(Invoke([this, &mock_journal_replayer, r](Context* ctx) {
                  if (r < 0) {
                    *mock_journal_replayer.local_image_ctx = nullptr;
                  }
                  m_threads->work_queue->queue(ctx, r);
                }));
  }

  void expect_shut_down(MockJournalReplayer& mock_journal_replayer, int r) {
    EXPECT_CALL(mock_journal_replayer, shut_down(_))
      .WillOnce(Invoke([this, &mock_journal_replayer, r](Context* ctx) {
                  *mock_journal_replayer.local_image_ctx = nullptr;
                  m_threads->work_queue->queue(ctx, r);
                }));
    EXPECT_CALL(mock_journal_replayer, destroy());
  }

  void expect_get_replay_status(MockJournalReplayer& mock_journal_replayer) {
    EXPECT_CALL(mock_journal_replayer, get_replay_status(_, _))
      .WillRepeatedly(DoAll(WithArg<1>(CompleteContext(-EEXIST)),
                            Return(true)));
  }

  void expect_send(MockCloseImageRequest &mock_close_image_request, int r) {
    EXPECT_CALL(mock_close_image_request, send())
      .WillOnce(Invoke([&mock_close_image_request, r]() {
            *mock_close_image_request.image_ctx = nullptr;
            mock_close_image_request.on_finish->complete(r);
          }));
  }

  void expect_set_mirror_image_status_repeatedly() {
    EXPECT_CALL(m_local_status_updater, set_mirror_image_status(_, _, _))
      .WillRepeatedly(Invoke([](auto, auto, auto){}));
    EXPECT_CALL(m_remote_status_updater, set_mirror_image_status(_, _, _))
      .WillRepeatedly(Invoke([](auto, auto, auto){}));
  }

  void expect_mirror_image_status_exists(bool exists) {
    EXPECT_CALL(m_local_status_updater, exists(_))
      .WillOnce(Return(exists));
    EXPECT_CALL(m_remote_status_updater, exists(_))
      .WillOnce(Return(exists));
  }

  void create_image_replayer(MockThreads &mock_threads) {
    m_image_replayer = new MockImageReplayer(
        m_local_io_ctx, "local_mirror_uuid", "global image id",
        &mock_threads, &m_instance_watcher, &m_local_status_updater, nullptr);
    m_image_replayer->add_peer("peer_uuid", m_remote_io_ctx,
                               &m_remote_status_updater);
  }

  void wait_for_stopped() {
    for (int i = 0; i < 10000; i++) {
      if (m_image_replayer->is_stopped()) {
        break;
      }
      usleep(1000);
    }
    ASSERT_TRUE(m_image_replayer->is_stopped());
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::ImageCtx *m_local_image_ctx = nullptr;
  MockInstanceWatcher m_instance_watcher;
  MockMirrorStatusUpdater m_local_status_updater;
  MockMirrorStatusUpdater m_remote_status_updater;
  MockImageReplayer *m_image_replayer = nullptr;
};

TEST_F(TestMockImageReplayer, StartStop) {
  // START

  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockBootstrapRequest mock_bootstrap_request;
  MockJournalReplayer mock_journal_replayer;

  expect_get_replay_status(mock_journal_replayer);
  expect_set_mirror_image_status_repeatedly();

  InSequence seq;
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);
  expect_init(mock_journal_replayer, 0);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());
  ASSERT_EQ(image_replayer::HEALTH_STATE_OK,
            m_image_replayer->get_health_state());

  // STOP
  expect_shut_down(mock_journal_replayer, 0);
  expect_mirror_image_status_exists(false);

  C_SaferCond stop_ctx;
  m_image_replayer->stop(&stop_ctx);
  ASSERT_EQ(0, stop_ctx.wait());
  ASSERT_EQ(image_replayer::HEALTH_STATE_OK,
            m_image_replayer->get_health_state());
}

TEST_F(TestMockImageReplayer, LocalImagePrimary) {
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;

  expect_set_mirror_image_status_repeatedly();

  InSequence seq;

  MockBootstrapRequest mock_bootstrap_request;
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, -ENOMSG);

  expect_mirror_image_status_exists(false);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, BootstrapRemoteDeleted) {
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;

  expect_set_mirror_image_status_repeatedly();

  InSequence seq;

  MockBootstrapRequest mock_bootstrap_request;
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, -ENOLINK);

  expect_trash_move(mock_image_deleter, "global image id", false, 0);
  expect_mirror_image_status_exists(false);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, BootstrapResyncRequested) {
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;

  expect_set_mirror_image_status_repeatedly();

  InSequence seq;

  MockBootstrapRequest mock_bootstrap_request;
  expect_send(mock_bootstrap_request, mock_local_image_ctx, true, 0);

  MockCloseImageRequest mock_close_image_request;
  expect_send(mock_close_image_request, 0);

  expect_trash_move(mock_image_deleter, "global image id", true, 0);
  expect_mirror_image_status_exists(false);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, BootstrapError) {
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockBootstrapRequest mock_bootstrap_request;

  expect_set_mirror_image_status_repeatedly();

  InSequence seq;
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, -EINVAL);

  expect_mirror_image_status_exists(false);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(-EINVAL, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, BootstrapCancel) {
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;

  expect_set_mirror_image_status_repeatedly();

  InSequence seq;

  create_image_replayer(mock_threads);

  MockBootstrapRequest mock_bootstrap_request;
  EXPECT_CALL(mock_bootstrap_request, send())
    .WillOnce(Invoke([this, &mock_bootstrap_request]() {
        m_image_replayer->stop();
        mock_bootstrap_request.on_finish->complete(-ECANCELED);
      }));
  EXPECT_CALL(mock_bootstrap_request, cancel());

  expect_mirror_image_status_exists(false);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(-ECANCELED, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, StopError) {
  // START

  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockBootstrapRequest mock_bootstrap_request;
  MockJournalReplayer mock_journal_replayer;

  expect_get_replay_status(mock_journal_replayer);
  expect_set_mirror_image_status_repeatedly();

  InSequence seq;
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);
  expect_init(mock_journal_replayer, 0);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());

  // STOP (errors are ignored)

  expect_shut_down(mock_journal_replayer, -EINVAL);
  expect_mirror_image_status_exists(false);

  C_SaferCond stop_ctx;
  m_image_replayer->stop(&stop_ctx);
  ASSERT_EQ(0, stop_ctx.wait());
}

TEST_F(TestMockImageReplayer, ReplayerError) {
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockBootstrapRequest mock_bootstrap_request;
  MockJournalReplayer mock_journal_replayer;

  expect_set_mirror_image_status_repeatedly();

  InSequence seq;
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);
  expect_init(mock_journal_replayer, -EINVAL);
  EXPECT_CALL(mock_journal_replayer, get_error_description())
    .WillOnce(Return("FAIL"));
  EXPECT_CALL(mock_journal_replayer, destroy());

  expect_mirror_image_status_exists(false);
  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(-EINVAL, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, ReplayerResync) {
  // START
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockBootstrapRequest mock_bootstrap_request;
  MockJournalReplayer mock_journal_replayer;

  expect_get_replay_status(mock_journal_replayer);
  expect_set_mirror_image_status_repeatedly();

  InSequence seq;
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);
  expect_init(mock_journal_replayer, 0);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());

  // NOTIFY
  EXPECT_CALL(mock_journal_replayer, is_resync_requested())
    .WillOnce(Return(true));
  expect_shut_down(mock_journal_replayer, 0);
  expect_trash_move(mock_image_deleter, "global image id", true, 0);
  expect_mirror_image_status_exists(false);
  mock_journal_replayer.replayer_listener->handle_notification();
  ASSERT_FALSE(m_image_replayer->is_running());

  wait_for_stopped();
}

TEST_F(TestMockImageReplayer, ReplayerInterrupted) {
  // START
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockBootstrapRequest mock_bootstrap_request;
  MockJournalReplayer mock_journal_replayer;

  expect_get_replay_status(mock_journal_replayer);
  expect_set_mirror_image_status_repeatedly();

  InSequence seq;
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);
  expect_init(mock_journal_replayer, 0);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());

  // NOTIFY
  EXPECT_CALL(mock_journal_replayer, is_resync_requested())
    .WillOnce(Return(false));
  EXPECT_CALL(mock_journal_replayer, is_replaying())
    .WillOnce(Return(false));
  EXPECT_CALL(mock_journal_replayer, get_error_code())
    .WillOnce(Return(-EINVAL));
  EXPECT_CALL(mock_journal_replayer, get_error_description())
    .WillOnce(Return("INVALID"));
  expect_shut_down(mock_journal_replayer, 0);
  expect_mirror_image_status_exists(false);
  mock_journal_replayer.replayer_listener->handle_notification();
  ASSERT_FALSE(m_image_replayer->is_running());

  wait_for_stopped();
}

TEST_F(TestMockImageReplayer, ReplayerRenamed) {
  // START
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockBootstrapRequest mock_bootstrap_request;
  MockJournalReplayer mock_journal_replayer;

  expect_get_replay_status(mock_journal_replayer);
  expect_set_mirror_image_status_repeatedly();

  InSequence seq;
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);
  expect_init(mock_journal_replayer, 0);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());

  // NOTIFY
  EXPECT_CALL(mock_journal_replayer, is_resync_requested())
    .WillOnce(Return(false));
  EXPECT_CALL(mock_journal_replayer, is_replaying())
    .WillOnce(Return(true));
  mock_local_image_ctx.name = "NEW NAME";
  mock_journal_replayer.replayer_listener->handle_notification();

  // STOP
  expect_shut_down(mock_journal_replayer, 0);
  expect_mirror_image_status_exists(false);

  C_SaferCond stop_ctx;
  m_image_replayer->stop(&stop_ctx);
  ASSERT_EQ(0, stop_ctx.wait());

  auto image_spec = image_replayer::util::compute_image_spec(
    m_local_io_ctx, "NEW NAME");
  ASSERT_EQ(image_spec, m_image_replayer->get_name());
}

} // namespace mirror
} // namespace rbd
