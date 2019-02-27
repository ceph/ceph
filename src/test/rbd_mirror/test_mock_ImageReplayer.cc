// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/journal/cls_journal_types.h"
#include "librbd/journal/Replay.h"
#include "librbd/journal/Types.h"
#include "tools/rbd_mirror/ImageDeleter.h"
#include "tools/rbd_mirror/ImageReplayer.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/BootstrapRequest.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/EventPreprocessor.h"
#include "tools/rbd_mirror/image_replayer/PrepareLocalImageRequest.h"
#include "tools/rbd_mirror/image_replayer/PrepareRemoteImageRequest.h"
#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"
#include "test/rbd_mirror/mock/MockContextWQ.h"
#include "test/rbd_mirror/mock/MockSafeTimer.h"

namespace librbd {

namespace {

struct MockTestJournal;

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
  MockTestJournal *journal = nullptr;
};

struct MockTestJournal : public MockJournal {
  MOCK_METHOD2(start_external_replay, void(journal::Replay<MockTestImageCtx> **,
                                           Context *on_start));
  MOCK_METHOD0(stop_external_replay, void());
};

} // anonymous namespace

namespace journal {

template<>
struct Replay<MockTestImageCtx> {
  MOCK_METHOD2(decode, int(bufferlist::const_iterator *, EventEntry *));
  MOCK_METHOD3(process, void(const EventEntry &, Context *, Context *));
  MOCK_METHOD1(flush, void(Context*));
  MOCK_METHOD2(shut_down, void(bool, Context*));
};

template <>
struct TypeTraits<MockTestImageCtx> {
  typedef ::journal::MockJournalerProxy Journaler;
  typedef ::journal::MockReplayEntryProxy ReplayEntry;
};

struct MirrorPeerClientMeta;

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

template<>
class InstanceWatcher<librbd::MockTestImageCtx> {
};

namespace image_replayer {

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

template<>
struct PrepareLocalImageRequest<librbd::MockTestImageCtx> {
  static PrepareLocalImageRequest* s_instance;
  std::string *local_image_id = nullptr;
  std::string *local_image_name = nullptr;
  std::string *tag_owner = nullptr;
  Context *on_finish = nullptr;

  static PrepareLocalImageRequest* create(librados::IoCtx &,
                                          const std::string &global_image_id,
                                          std::string *local_image_id,
                                          std::string *local_image_name,
                                          std::string *tag_owner,
                                          MockContextWQ *work_queue,
                                          Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->local_image_id = local_image_id;
    s_instance->local_image_name = local_image_name;
    s_instance->tag_owner = tag_owner;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  PrepareLocalImageRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template<>
struct PrepareRemoteImageRequest<librbd::MockTestImageCtx> {
  static PrepareRemoteImageRequest* s_instance;
  std::string *remote_mirror_uuid = nullptr;
  std::string *remote_image_id = nullptr;
  cls::journal::ClientState *client_state;
  ::journal::MockJournalerProxy **remote_journaler = nullptr;
  librbd::journal::MirrorPeerClientMeta *client_meta = nullptr;
  Context *on_finish = nullptr;

  static PrepareRemoteImageRequest* create(Threads<librbd::MockTestImageCtx> *threads,
                                           librados::IoCtx &,
                                           const std::string &global_image_id,
                                           const std::string &local_mirror_uuid,
                                           const std::string &local_image_id,
                                           const journal::Settings &settings,
                                           std::string *remote_mirror_uuid,
                                           std::string *remote_image_id,
                                           ::journal::MockJournalerProxy **remote_journaler,
                                           cls::journal::ClientState *client_state,
                                           librbd::journal::MirrorPeerClientMeta *client_meta,
                                           Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->remote_mirror_uuid = remote_mirror_uuid;
    s_instance->remote_image_id = remote_image_id;
    s_instance->remote_journaler = remote_journaler;
    s_instance->client_state = client_state;
    s_instance->client_meta = client_meta;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  PrepareRemoteImageRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template<>
struct BootstrapRequest<librbd::MockTestImageCtx> {
  static BootstrapRequest* s_instance;
  librbd::MockTestImageCtx **image_ctx = nullptr;
  Context *on_finish = nullptr;
  bool *do_resync = nullptr;

  static BootstrapRequest* create(
      Threads<librbd::MockTestImageCtx>* threads,
      librados::IoCtx &local_io_ctx, librados::IoCtx &remote_io_ctx,
      rbd::mirror::InstanceWatcher<librbd::MockTestImageCtx> *instance_watcher,
      librbd::MockTestImageCtx **local_image_ctx,
      const std::string &local_image_name, const std::string &remote_image_id,
      const std::string &global_image_id,
      const std::string &local_mirror_uuid,
      const std::string &remote_mirror_uuid,
      ::journal::MockJournalerProxy *journaler,
      cls::journal::ClientState *client_state,
      librbd::journal::MirrorPeerClientMeta *client_meta,
      Context *on_finish, bool *do_resync,
      rbd::mirror::ProgressContext *progress_ctx = nullptr) {
    ceph_assert(s_instance != nullptr);
    s_instance->image_ctx = local_image_ctx;
    s_instance->on_finish = on_finish;
    s_instance->do_resync = do_resync;
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

template<>
struct EventPreprocessor<librbd::MockTestImageCtx> {
  static EventPreprocessor *s_instance;

  static EventPreprocessor *create(librbd::MockTestImageCtx &local_image_ctx,
                                   ::journal::MockJournalerProxy &remote_journaler,
                                   const std::string &local_mirror_uuid,
                                   librbd::journal::MirrorPeerClientMeta *client_meta,
                                   MockContextWQ *work_queue) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  static void destroy(EventPreprocessor* processor) {
  }

  EventPreprocessor() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }

  ~EventPreprocessor() {
    ceph_assert(s_instance == this);
    s_instance = nullptr;
  }

  MOCK_METHOD1(is_required, bool(const librbd::journal::EventEntry &));
  MOCK_METHOD2(preprocess, void(librbd::journal::EventEntry *, Context *));
};

template<>
struct ReplayStatusFormatter<librbd::MockTestImageCtx> {
  static ReplayStatusFormatter* s_instance;

  static ReplayStatusFormatter* create(::journal::MockJournalerProxy *journaler,
                                       const std::string &mirror_uuid) {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  static void destroy(ReplayStatusFormatter* formatter) {
  }

  ReplayStatusFormatter() {
    ceph_assert(s_instance == nullptr);
    s_instance = this;
  }

  ~ReplayStatusFormatter() {
    ceph_assert(s_instance == this);
    s_instance = nullptr;
  }

  MOCK_METHOD2(get_or_send_update, bool(std::string *description, Context *on_finish));
};

BootstrapRequest<librbd::MockTestImageCtx>* BootstrapRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
CloseImageRequest<librbd::MockTestImageCtx>* CloseImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
EventPreprocessor<librbd::MockTestImageCtx>* EventPreprocessor<librbd::MockTestImageCtx>::s_instance = nullptr;
PrepareLocalImageRequest<librbd::MockTestImageCtx>* PrepareLocalImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
PrepareRemoteImageRequest<librbd::MockTestImageCtx>* PrepareRemoteImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
ReplayStatusFormatter<librbd::MockTestImageCtx>* ReplayStatusFormatter<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

// template definitions
#include "tools/rbd_mirror/ImageReplayer.cc"

namespace rbd {
namespace mirror {

class TestMockImageReplayer : public TestMockFixture {
public:
  typedef Threads<librbd::MockTestImageCtx> MockThreads;
  typedef ImageDeleter<librbd::MockTestImageCtx> MockImageDeleter;
  typedef BootstrapRequest<librbd::MockTestImageCtx> MockBootstrapRequest;
  typedef CloseImageRequest<librbd::MockTestImageCtx> MockCloseImageRequest;
  typedef EventPreprocessor<librbd::MockTestImageCtx> MockEventPreprocessor;
  typedef PrepareLocalImageRequest<librbd::MockTestImageCtx> MockPrepareLocalImageRequest;
  typedef PrepareRemoteImageRequest<librbd::MockTestImageCtx> MockPrepareRemoteImageRequest;
  typedef ReplayStatusFormatter<librbd::MockTestImageCtx> MockReplayStatusFormatter;
  typedef librbd::journal::Replay<librbd::MockTestImageCtx> MockReplay;
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

  void expect_get_or_send_update(
    MockReplayStatusFormatter &mock_replay_status_formatter) {
    EXPECT_CALL(mock_replay_status_formatter, get_or_send_update(_, _))
      .WillRepeatedly(DoAll(WithArg<1>(CompleteContext(-EEXIST)),
                            Return(true)));
  }

  void expect_send(MockPrepareLocalImageRequest &mock_request,
                   const std::string &local_image_id,
                   const std::string &local_image_name,
                   const std::string &tag_owner,
                   int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(Invoke([&mock_request, local_image_id, local_image_name, tag_owner, r]() {
          if (r == 0) {
            *mock_request.local_image_id = local_image_id;
            *mock_request.local_image_name = local_image_name;
            *mock_request.tag_owner = tag_owner;
          }
          mock_request.on_finish->complete(r);
        }));
  }

  void expect_send(MockPrepareRemoteImageRequest& mock_request,
                   const std::string& mirror_uuid, const std::string& image_id,
                   int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(Invoke([&mock_request, image_id, mirror_uuid, r]() {
                  if (r >= 0) {
                    *mock_request.remote_journaler = new ::journal::MockJournalerProxy();
                  }

                  *mock_request.remote_mirror_uuid = mirror_uuid;
                  *mock_request.remote_image_id = image_id;
                  mock_request.on_finish->complete(r);
                }));
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
            }
            mock_bootstrap_request.on_finish->complete(r);
          }));
  }

  void expect_start_external_replay(librbd::MockTestJournal &mock_journal,
                                    MockReplay *mock_replay, int r) {
    EXPECT_CALL(mock_journal, start_external_replay(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(mock_replay),
                      WithArg<1>(CompleteContext(r))));
  }

  void expect_init(::journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, init(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_get_cached_client(::journal::MockJournaler &mock_journaler,
                                int r) {
    librbd::journal::ImageClientMeta image_client_meta;
    image_client_meta.tag_class = 0;

    librbd::journal::ClientData client_data;
    client_data.client_meta = image_client_meta;

    cls::journal::Client client;
    encode(client_data, client.data);

    EXPECT_CALL(mock_journaler, get_cached_client("local_mirror_uuid", _))
      .WillOnce(DoAll(SetArgPointee<1>(client),
                      Return(r)));
  }

  void expect_stop_replay(::journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, stop_replay(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_flush(MockReplay &mock_replay, int r) {
    EXPECT_CALL(mock_replay, flush(_)).WillOnce(CompleteContext(r));
  }

  void expect_shut_down(MockReplay &mock_replay, bool cancel_ops, int r) {
    EXPECT_CALL(mock_replay, shut_down(cancel_ops, _))
      .WillOnce(WithArg<1>(CompleteContext(r)));
  }

  void expect_shut_down(journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, shut_down(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_send(MockCloseImageRequest &mock_close_image_request, int r) {
    EXPECT_CALL(mock_close_image_request, send())
      .WillOnce(Invoke([&mock_close_image_request, r]() {
            *mock_close_image_request.image_ctx = nullptr;
            mock_close_image_request.on_finish->complete(r);
          }));
  }

  void expect_get_commit_tid_in_debug(
    ::journal::MockReplayEntry &mock_replay_entry) {
    // It is used in debug messages and depends on debug level
    EXPECT_CALL(mock_replay_entry, get_commit_tid())
    .Times(AtLeast(0))
      .WillRepeatedly(Return(0));
  }

  void expect_get_tag_tid_in_debug(librbd::MockTestJournal &mock_journal) {
    // It is used in debug messages and depends on debug level
    EXPECT_CALL(mock_journal, get_tag_tid()).Times(AtLeast(0))
      .WillRepeatedly(Return(0));
  }

  void expect_committed(::journal::MockReplayEntry &mock_replay_entry,
                        ::journal::MockJournaler &mock_journaler, int times) {
    EXPECT_CALL(mock_replay_entry, get_data()).Times(times);
    EXPECT_CALL(mock_journaler, committed(
                  MatcherCast<const ::journal::MockReplayEntryProxy&>(_)))
      .Times(times);
  }

  void expect_try_pop_front(::journal::MockJournaler &mock_journaler,
                            uint64_t replay_tag_tid, bool entries_available) {
    EXPECT_CALL(mock_journaler, try_pop_front(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(::journal::MockReplayEntryProxy()),
                      SetArgPointee<1>(replay_tag_tid),
                      Return(entries_available)));
  }

  void expect_try_pop_front_return_no_entries(
    ::journal::MockJournaler &mock_journaler, Context *on_finish) {
    EXPECT_CALL(mock_journaler, try_pop_front(_, _))
      .WillOnce(DoAll(Invoke([on_finish](::journal::MockReplayEntryProxy *e,
					 uint64_t *t) {
			       on_finish->complete(0);
			     }),
		      Return(false)));
  }

  void expect_get_tag(::journal::MockJournaler &mock_journaler,
                      const cls::journal::Tag &tag, int r) {
    EXPECT_CALL(mock_journaler, get_tag(_, _, _))
      .WillOnce(DoAll(SetArgPointee<1>(tag),
                      WithArg<2>(CompleteContext(r))));
  }

  void expect_allocate_tag(librbd::MockTestJournal &mock_journal, int r) {
    EXPECT_CALL(mock_journal, allocate_tag(_, _, _))
      .WillOnce(WithArg<2>(CompleteContext(r)));
  }

  void expect_preprocess(MockEventPreprocessor &mock_event_preprocessor,
                         bool required, int r) {
    EXPECT_CALL(mock_event_preprocessor, is_required(_))
      .WillOnce(Return(required));
    if (required) {
      EXPECT_CALL(mock_event_preprocessor, preprocess(_, _))
        .WillOnce(WithArg<1>(CompleteContext(r)));
    }
  }

  void expect_process(MockReplay &mock_replay,
                      int on_ready_r, int on_commit_r) {
    EXPECT_CALL(mock_replay, process(_, _, _))
      .WillOnce(DoAll(WithArg<1>(CompleteContext(on_ready_r)),
                      WithArg<2>(CompleteContext(on_commit_r))));
  }

  void create_image_replayer(MockThreads &mock_threads) {
    m_image_replayer = new MockImageReplayer(
      &mock_threads, &m_instance_watcher,
      rbd::mirror::RadosRef(new librados::Rados(m_local_io_ctx)),
      "local_mirror_uuid", m_local_io_ctx.get_id(), "global image id");
    m_image_replayer->add_peer("peer_uuid", m_remote_io_ctx);
  }

  librbd::ImageCtx *m_remote_image_ctx;
  librbd::ImageCtx *m_local_image_ctx = nullptr;
  MockInstanceWatcher m_instance_watcher;
  MockImageReplayer *m_image_replayer = nullptr;
};

TEST_F(TestMockImageReplayer, StartStop) {
  // START

  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockTestJournal mock_local_journal;
  mock_local_image_ctx.journal = &mock_local_journal;

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  MockBootstrapRequest mock_bootstrap_request;
  MockReplay mock_local_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;

  expect_get_or_send_update(mock_replay_status_formatter);

  InSequence seq;
  expect_send(mock_prepare_local_image_request, mock_local_image_ctx.id,
              mock_local_image_ctx.name, "remote mirror uuid", 0);
  expect_send(mock_prepare_remote_image_request, "remote mirror uuid",
              m_remote_image_ctx->id, 0);
  EXPECT_CALL(mock_remote_journaler, construct());
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);

  EXPECT_CALL(mock_local_journal, add_listener(_));

  expect_init(mock_remote_journaler, 0);

  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, 0);

  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);

  EXPECT_CALL(mock_remote_journaler, start_live_replay(_, _));

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());
  ASSERT_EQ(image_replayer::HEALTH_STATE_OK,
            m_image_replayer->get_health_state());

  // STOP

  MockCloseImageRequest mock_close_local_image_request;

  expect_shut_down(mock_local_replay, true, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_send(mock_close_local_image_request, 0);

  expect_stop_replay(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, 0);

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
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  MockReplayStatusFormatter mock_replay_status_formatter;

  expect_get_or_send_update(mock_replay_status_formatter);

  InSequence seq;
  expect_send(mock_prepare_local_image_request, mock_local_image_ctx.id,
              mock_local_image_ctx.name, "", 0);
  expect_send(mock_prepare_remote_image_request, "remote mirror uuid",
              "remote image id", 0);
  EXPECT_CALL(mock_remote_journaler, construct());
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, 0);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, LocalImageDNE) {
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  MockBootstrapRequest mock_bootstrap_request;
  MockReplayStatusFormatter mock_replay_status_formatter;

  expect_get_or_send_update(mock_replay_status_formatter);

  InSequence seq;
  expect_send(mock_prepare_local_image_request, "", "", "", -ENOENT);
  expect_send(mock_prepare_remote_image_request, "remote mirror uuid",
              m_remote_image_ctx->id, 0);
  EXPECT_CALL(mock_remote_journaler, construct());
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, -EREMOTEIO);

  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, 0);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(-EREMOTEIO, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, PrepareLocalImageError) {
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockReplayStatusFormatter mock_replay_status_formatter;

  expect_get_or_send_update(mock_replay_status_formatter);

  InSequence seq;
  expect_send(mock_prepare_local_image_request, mock_local_image_ctx.id,
              mock_local_image_ctx.name, "remote mirror uuid", -EINVAL);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(-EINVAL, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, GetRemoteImageIdDNE) {
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  MockReplayStatusFormatter mock_replay_status_formatter;

  expect_get_or_send_update(mock_replay_status_formatter);

  InSequence seq;
  expect_send(mock_prepare_local_image_request, mock_local_image_ctx.id,
              mock_local_image_ctx.name, "remote mirror uuid", 0);
  expect_send(mock_prepare_remote_image_request, "remote mirror uuid",
              "", -ENOENT);
  expect_trash_move(mock_image_deleter, "global image id", false, 0);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, GetRemoteImageIdNonLinkedDNE) {
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  MockReplayStatusFormatter mock_replay_status_formatter;

  expect_get_or_send_update(mock_replay_status_formatter);

  InSequence seq;
  expect_send(mock_prepare_local_image_request, mock_local_image_ctx.id,
              mock_local_image_ctx.name, "some other mirror uuid", 0);
  expect_send(mock_prepare_remote_image_request, "remote mirror uuid",
              "", -ENOENT);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(-ENOENT, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, GetRemoteImageIdError) {
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  MockReplayStatusFormatter mock_replay_status_formatter;

  expect_get_or_send_update(mock_replay_status_formatter);

  InSequence seq;
  expect_send(mock_prepare_local_image_request, mock_local_image_ctx.id,
              mock_local_image_ctx.name, "remote mirror uuid", 0);
  expect_send(mock_prepare_remote_image_request, "remote mirror uuid",
              m_remote_image_ctx->id, -EINVAL);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(-EINVAL, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, BootstrapError) {
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  MockBootstrapRequest mock_bootstrap_request;
  MockReplayStatusFormatter mock_replay_status_formatter;

  expect_get_or_send_update(mock_replay_status_formatter);

  InSequence seq;
  expect_send(mock_prepare_local_image_request, mock_local_image_ctx.id,
              mock_local_image_ctx.name, "remote mirror uuid", 0);
  expect_send(mock_prepare_remote_image_request, "remote mirror uuid",
              m_remote_image_ctx->id, 0);
  EXPECT_CALL(mock_remote_journaler, construct());
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, -EINVAL);

  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, 0);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(-EINVAL, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, StopBeforeBootstrap) {
  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  MockReplayStatusFormatter mock_replay_status_formatter;

  expect_get_or_send_update(mock_replay_status_formatter);

  InSequence seq;
  expect_send(mock_prepare_local_image_request, mock_local_image_ctx.id,
              mock_local_image_ctx.name, "remote mirror uuid", 0);
  expect_send(mock_prepare_remote_image_request, "remote mirror uuid",
              m_remote_image_ctx->id, 0);
  EXPECT_CALL(mock_remote_journaler, construct())
    .WillOnce(Invoke([this]() {
                m_image_replayer->stop(nullptr, true);
              }));

  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, 0);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(-ECANCELED, start_ctx.wait());
}

TEST_F(TestMockImageReplayer, StartExternalReplayError) {
  // START

  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockTestJournal mock_local_journal;
  mock_local_image_ctx.journal = &mock_local_journal;

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  MockBootstrapRequest mock_bootstrap_request;
  MockReplay mock_local_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;

  expect_get_or_send_update(mock_replay_status_formatter);

  InSequence seq;
  expect_send(mock_prepare_local_image_request, mock_local_image_ctx.id,
              mock_local_image_ctx.name, "remote mirror uuid", 0);
  expect_send(mock_prepare_remote_image_request, "remote mirror uuid",
              m_remote_image_ctx->id, 0);
  EXPECT_CALL(mock_remote_journaler, construct());
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);

  EXPECT_CALL(mock_local_journal, add_listener(_));

  expect_init(mock_remote_journaler, 0);

  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, 0);

  expect_start_external_replay(mock_local_journal, nullptr, -EINVAL);

  MockCloseImageRequest mock_close_local_image_request;
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  expect_send(mock_close_local_image_request, 0);

  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, 0);

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(-EINVAL, start_ctx.wait());
  ASSERT_EQ(image_replayer::HEALTH_STATE_ERROR,
            m_image_replayer->get_health_state());
}

TEST_F(TestMockImageReplayer, StopError) {
  // START

  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockTestJournal mock_local_journal;
  mock_local_image_ctx.journal = &mock_local_journal;

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  MockBootstrapRequest mock_bootstrap_request;
  MockReplay mock_local_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;

  expect_get_or_send_update(mock_replay_status_formatter);

  InSequence seq;
  expect_send(mock_prepare_local_image_request, mock_local_image_ctx.id,
              mock_local_image_ctx.name, "remote mirror uuid", 0);
  expect_send(mock_prepare_remote_image_request, "remote mirror uuid",
              m_remote_image_ctx->id, 0);
  EXPECT_CALL(mock_remote_journaler, construct());
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);

  EXPECT_CALL(mock_local_journal, add_listener(_));

  expect_init(mock_remote_journaler, 0);

  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, 0);

  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);

  EXPECT_CALL(mock_remote_journaler, start_live_replay(_, _));

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());

  // STOP (errors are ignored)

  MockCloseImageRequest mock_close_local_image_request;

  expect_shut_down(mock_local_replay, true, -EINVAL);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_send(mock_close_local_image_request, -EINVAL);

  expect_stop_replay(mock_remote_journaler, -EINVAL);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, -EINVAL);

  C_SaferCond stop_ctx;
  m_image_replayer->stop(&stop_ctx);
  ASSERT_EQ(0, stop_ctx.wait());
}

TEST_F(TestMockImageReplayer, Replay) {
  // START

  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockTestJournal mock_local_journal;
  mock_local_image_ctx.journal = &mock_local_journal;

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  MockBootstrapRequest mock_bootstrap_request;
  MockReplay mock_local_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  ::journal::MockReplayEntry mock_replay_entry;

  expect_get_or_send_update(mock_replay_status_formatter);
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_get_tag_tid_in_debug(mock_local_journal);
  expect_committed(mock_replay_entry, mock_remote_journaler, 2);

  InSequence seq;
  expect_send(mock_prepare_local_image_request, mock_local_image_ctx.id,
              mock_local_image_ctx.name, "remote mirror uuid", 0);
  expect_send(mock_prepare_remote_image_request, "remote mirror uuid",
              m_remote_image_ctx->id, 0);
  EXPECT_CALL(mock_remote_journaler, construct());
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);

  EXPECT_CALL(mock_local_journal, add_listener(_));

  expect_init(mock_remote_journaler, 0);

  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, 0);

  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);

  EXPECT_CALL(mock_remote_journaler, start_live_replay(_, _));

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());

  // REPLAY

  cls::journal::Tag tag =
    {1, 0, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                            librbd::Journal<>::LOCAL_MIRROR_UUID,
                            true, 0, 0})};

  expect_try_pop_front(mock_remote_journaler, tag.tid, true);

  // replay_flush
  expect_shut_down(mock_local_replay, false, 0);
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);
  expect_get_tag(mock_remote_journaler, tag, 0);
  expect_allocate_tag(mock_local_journal, 0);

  // process
  EXPECT_CALL(mock_replay_entry, get_data());
  EXPECT_CALL(mock_local_replay, decode(_, _))
    .WillOnce(Return(0));
  expect_preprocess(mock_event_preprocessor, false, 0);
  expect_process(mock_local_replay, 0, 0);

  // the next event with preprocess
  expect_try_pop_front(mock_remote_journaler, tag.tid, true);
  EXPECT_CALL(mock_replay_entry, get_data());
  EXPECT_CALL(mock_local_replay, decode(_, _))
    .WillOnce(Return(0));
  expect_preprocess(mock_event_preprocessor, true, 0);
  expect_process(mock_local_replay, 0, 0);

  // attempt to process the next event
  C_SaferCond replay_ctx;
  expect_try_pop_front_return_no_entries(mock_remote_journaler, &replay_ctx);

  // fire
  m_image_replayer->handle_replay_ready();
  ASSERT_EQ(0, replay_ctx.wait());

  // STOP

  MockCloseImageRequest mock_close_local_image_request;
  expect_shut_down(mock_local_replay, true, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_send(mock_close_local_image_request, 0);

  expect_stop_replay(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, 0);

  C_SaferCond stop_ctx;
  m_image_replayer->stop(&stop_ctx);
  ASSERT_EQ(0, stop_ctx.wait());
}

TEST_F(TestMockImageReplayer, DecodeError) {
  // START

  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockTestJournal mock_local_journal;
  mock_local_image_ctx.journal = &mock_local_journal;

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  MockBootstrapRequest mock_bootstrap_request;
  MockReplay mock_local_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  ::journal::MockReplayEntry mock_replay_entry;

  expect_get_or_send_update(mock_replay_status_formatter);
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_get_tag_tid_in_debug(mock_local_journal);

  InSequence seq;
  expect_send(mock_prepare_local_image_request, mock_local_image_ctx.id,
              mock_local_image_ctx.name, "remote mirror uuid", 0);
  expect_send(mock_prepare_remote_image_request, "remote mirror uuid",
              m_remote_image_ctx->id, 0);
  EXPECT_CALL(mock_remote_journaler, construct());
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);

  EXPECT_CALL(mock_local_journal, add_listener(_));

  expect_init(mock_remote_journaler, 0);

  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, 0);

  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);

  EXPECT_CALL(mock_remote_journaler, start_live_replay(_, _));

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());

  // REPLAY

  cls::journal::Tag tag =
    {1, 0, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                            librbd::Journal<>::LOCAL_MIRROR_UUID,
                            true, 0, 0})};

  expect_try_pop_front(mock_remote_journaler, tag.tid, true);

  // replay_flush
  expect_shut_down(mock_local_replay, false, 0);
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);
  expect_get_tag(mock_remote_journaler, tag, 0);
  expect_allocate_tag(mock_local_journal, 0);

  // process
  EXPECT_CALL(mock_replay_entry, get_data());
  EXPECT_CALL(mock_local_replay, decode(_, _))
    .WillOnce(Return(-EINVAL));

  // stop on error
  expect_shut_down(mock_local_replay, true, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());

  MockCloseImageRequest mock_close_local_image_request;
  C_SaferCond close_ctx;
  EXPECT_CALL(mock_close_local_image_request, send())
    .WillOnce(Invoke([&mock_close_local_image_request, &close_ctx]() {
	  *mock_close_local_image_request.image_ctx = nullptr;
	  mock_close_local_image_request.on_finish->complete(0);
	  close_ctx.complete(0);
	}));

  expect_stop_replay(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, 0);

  // fire
  m_image_replayer->handle_replay_ready();
  ASSERT_EQ(0, close_ctx.wait());

  while (!m_image_replayer->is_stopped()) {
    usleep(1000);
  }
}

TEST_F(TestMockImageReplayer, DelayedReplay) {

  // START

  create_local_image();
  librbd::MockTestImageCtx mock_local_image_ctx(*m_local_image_ctx);

  librbd::MockTestJournal mock_local_journal;
  mock_local_image_ctx.journal = &mock_local_journal;

  journal::MockJournaler mock_remote_journaler;
  MockThreads mock_threads(m_threads);
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);

  MockImageDeleter mock_image_deleter;
  MockPrepareLocalImageRequest mock_prepare_local_image_request;
  MockPrepareRemoteImageRequest mock_prepare_remote_image_request;
  MockBootstrapRequest mock_bootstrap_request;
  MockReplay mock_local_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  ::journal::MockReplayEntry mock_replay_entry;

  expect_get_or_send_update(mock_replay_status_formatter);
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_get_tag_tid_in_debug(mock_local_journal);
  expect_committed(mock_replay_entry, mock_remote_journaler, 1);

  InSequence seq;
  expect_send(mock_prepare_local_image_request, mock_local_image_ctx.id,
              mock_local_image_ctx.name, "remote mirror uuid", 0);
  expect_send(mock_prepare_remote_image_request, "remote mirror uuid",
              m_remote_image_ctx->id, 0);
  EXPECT_CALL(mock_remote_journaler, construct());
  expect_send(mock_bootstrap_request, mock_local_image_ctx, false, 0);

  EXPECT_CALL(mock_local_journal, add_listener(_));

  expect_init(mock_remote_journaler, 0);

  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, 0);

  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);

  EXPECT_CALL(mock_remote_journaler, start_live_replay(_, _));

  create_image_replayer(mock_threads);

  C_SaferCond start_ctx;
  m_image_replayer->start(&start_ctx);
  ASSERT_EQ(0, start_ctx.wait());

  // REPLAY

  cls::journal::Tag tag =
    {1, 0, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                            librbd::Journal<>::LOCAL_MIRROR_UUID,
                            true, 0, 0})};

  expect_try_pop_front(mock_remote_journaler, tag.tid, true);

  // replay_flush
  expect_shut_down(mock_local_replay, false, 0);
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, &mock_local_replay, 0);
  expect_get_tag(mock_remote_journaler, tag, 0);
  expect_allocate_tag(mock_local_journal, 0);

  // process with delay
  EXPECT_CALL(mock_replay_entry, get_data());
  librbd::journal::EventEntry event_entry(
    librbd::journal::AioDiscardEvent(123, 345, 0), ceph_clock_now());
  EXPECT_CALL(mock_local_replay, decode(_, _))
    .WillOnce(DoAll(SetArgPointee<1>(event_entry),
                    Return(0)));
  expect_preprocess(mock_event_preprocessor, false, 0);
  expect_process(mock_local_replay, 0, 0);

  // attempt to process the next event
  C_SaferCond replay_ctx;
  expect_try_pop_front_return_no_entries(mock_remote_journaler, &replay_ctx);

  // fire
  mock_local_image_ctx.mirroring_replay_delay = 2;
  m_image_replayer->handle_replay_ready();
  ASSERT_EQ(0, replay_ctx.wait());

  // add a pending (delayed) entry before stop
  expect_try_pop_front(mock_remote_journaler, tag.tid, true);
  EXPECT_CALL(mock_replay_entry, get_data());
  C_SaferCond decode_ctx;
  EXPECT_CALL(mock_local_replay, decode(_, _))
    .WillOnce(DoAll(Invoke([&decode_ctx](bufferlist::const_iterator* it,
                                         librbd::journal::EventEntry *e) {
                             decode_ctx.complete(0);
                           }),
                    Return(0)));

  mock_local_image_ctx.mirroring_replay_delay = 10;
  m_image_replayer->handle_replay_ready();
  ASSERT_EQ(0, decode_ctx.wait());

  // STOP

  MockCloseImageRequest mock_close_local_image_request;

  expect_shut_down(mock_local_replay, true, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_send(mock_close_local_image_request, 0);

  expect_stop_replay(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));
  expect_shut_down(mock_remote_journaler, 0);

  C_SaferCond stop_ctx;
  m_image_replayer->stop(&stop_ctx);
  ASSERT_EQ(0, stop_ctx.wait());
}


} // namespace mirror
} // namespace rbd
