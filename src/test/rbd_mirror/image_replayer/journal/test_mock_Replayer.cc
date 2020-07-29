// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/image_replayer/CloseImageRequest.h"
#include "tools/rbd_mirror/image_replayer/ReplayerListener.h"
#include "tools/rbd_mirror/image_replayer/Utils.h"
#include "tools/rbd_mirror/image_replayer/journal/Replayer.h"
#include "tools/rbd_mirror/image_replayer/journal/EventPreprocessor.h"
#include "tools/rbd_mirror/image_replayer/journal/ReplayStatusFormatter.h"
#include "tools/rbd_mirror/image_replayer/journal/StateBuilder.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/rbd_mirror/mock/MockContextWQ.h"
#include "test/rbd_mirror/mock/MockSafeTimer.h"
#include <boost/intrusive_ptr.hpp>

namespace librbd {

namespace {

struct MockTestJournal;

struct MockTestImageCtx : public librbd::MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx &image_ctx,
                            MockTestJournal& mock_test_journal)
    : librbd::MockImageCtx(image_ctx), journal(&mock_test_journal) {
  }

  MockTestJournal* journal = nullptr;
};

struct MockTestJournal : public MockJournal {
  MOCK_METHOD2(start_external_replay, void(journal::Replay<MockTestImageCtx> **,
                                           Context *on_start));
  MOCK_METHOD0(stop_external_replay, void());
};

} // anonymous namespace

namespace journal {

template <>
struct TypeTraits<librbd::MockTestImageCtx> {
  typedef ::journal::MockJournaler Journaler;
  typedef ::journal::MockReplayEntryProxy ReplayEntry;
};

template<>
struct Replay<MockTestImageCtx> {
  MOCK_METHOD2(decode, int(bufferlist::const_iterator *, EventEntry *));
  MOCK_METHOD3(process, void(const EventEntry &, Context *, Context *));
  MOCK_METHOD1(flush, void(Context*));
  MOCK_METHOD2(shut_down, void(bool, Context*));
};

} // namespace journal
} // namespace librbd

namespace boost {

template<>
struct intrusive_ptr<librbd::MockTestJournal> {
  intrusive_ptr() {
  }
  intrusive_ptr(librbd::MockTestJournal* mock_test_journal)
    : mock_test_journal(mock_test_journal) {
  }

  librbd::MockTestJournal* operator->() {
    return mock_test_journal;
  }

  void reset() {
    mock_test_journal = nullptr;
  }

  const librbd::MockTestJournal* get() const {
    return mock_test_journal;
  }

  template<typename T>
  bool operator==(T* t) const {
    return (mock_test_journal == t);
  }

  librbd::MockTestJournal* mock_test_journal = nullptr;
};

} // namespace boost

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

namespace journal {

template <>
struct EventPreprocessor<librbd::MockTestImageCtx> {
  static EventPreprocessor *s_instance;

  static EventPreprocessor *create(librbd::MockTestImageCtx &local_image_ctx,
                                   ::journal::MockJournaler &remote_journaler,
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

  static ReplayStatusFormatter* create(::journal::MockJournaler *journaler,
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

  MOCK_METHOD1(handle_entry_processed, void(uint64_t));
  MOCK_METHOD2(get_or_send_update, bool(std::string *description, Context *on_finish));
};

template<>
struct StateBuilder<librbd::MockTestImageCtx> {
  StateBuilder(librbd::MockTestImageCtx& local_image_ctx,
               ::journal::MockJournaler& remote_journaler,
               const librbd::journal::MirrorPeerClientMeta& remote_client_meta)
    : local_image_ctx(&local_image_ctx),
      remote_journaler(&remote_journaler),
      remote_client_meta(remote_client_meta) {
  }

  librbd::MockTestImageCtx* local_image_ctx;
  std::string remote_mirror_uuid = "remote mirror uuid";
  ::journal::MockJournaler* remote_journaler = nullptr;
  librbd::journal::MirrorPeerClientMeta remote_client_meta;
};

EventPreprocessor<librbd::MockTestImageCtx>* EventPreprocessor<librbd::MockTestImageCtx>::s_instance = nullptr;
ReplayStatusFormatter<librbd::MockTestImageCtx>* ReplayStatusFormatter<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

#include "tools/rbd_mirror/image_replayer/journal/Replayer.cc"

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace journal {

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::MatcherCast;
using ::testing::Return;
using ::testing::ReturnArg;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::WithArg;

class TestMockImageReplayerJournalReplayer : public TestMockFixture {
public:
  typedef Replayer<librbd::MockTestImageCtx> MockReplayer;
  typedef EventPreprocessor<librbd::MockTestImageCtx> MockEventPreprocessor;
  typedef ReplayStatusFormatter<librbd::MockTestImageCtx> MockReplayStatusFormatter;
  typedef StateBuilder<librbd::MockTestImageCtx> MockStateBuilder;
  typedef Threads<librbd::MockTestImageCtx> MockThreads;
  typedef CloseImageRequest<librbd::MockTestImageCtx> MockCloseImageRequest;
  typedef librbd::journal::Replay<librbd::MockTestImageCtx> MockReplay;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::RBD rbd;
    ASSERT_EQ(0, create_image(rbd, m_local_io_ctx, m_image_name, m_image_size));
    ASSERT_EQ(0, open_image(m_local_io_ctx, m_image_name, &m_local_image_ctx));
  }

  bufferlist encode_tag_data(const librbd::journal::TagData &tag_data) {
    bufferlist bl;
    encode(tag_data, bl);
    return bl;
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

  void expect_init(::journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, init(_))
      .WillOnce(CompleteContext(m_threads->work_queue, r));
  }

  void expect_stop_replay(::journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, stop_replay(_))
      .WillOnce(CompleteContext(r));
  }

  void expect_shut_down(MockReplay &mock_replay, bool cancel_ops, int r) {
    EXPECT_CALL(mock_replay, shut_down(cancel_ops, _))
      .WillOnce(WithArg<1>(CompleteContext(m_threads->work_queue, r)));
  }

  void expect_get_cached_client(::journal::MockJournaler &mock_journaler,
                                const std::string& client_id,
                                const cls::journal::Client& client,
                                const librbd::journal::ClientMeta& client_meta,
                                int r) {
    librbd::journal::ClientData client_data;
    client_data.client_meta = client_meta;

    cls::journal::Client client_copy{client};
    encode(client_data, client_copy.data);

    EXPECT_CALL(mock_journaler, get_cached_client(client_id, _))
      .WillOnce(DoAll(SetArgPointee<1>(client_copy),
                      Return(r)));
  }

  void expect_start_external_replay(librbd::MockTestJournal &mock_journal,
                                    MockReplay *mock_replay, int r) {
    EXPECT_CALL(mock_journal, start_external_replay(_, _))
      .WillOnce(DoAll(SetArgPointee<0>(mock_replay),
                      WithArg<1>(CompleteContext(m_threads->work_queue, r))));
  }

  void expect_is_tag_owner(librbd::MockTestJournal &mock_journal,
                           bool is_owner) {
    EXPECT_CALL(mock_journal, is_tag_owner()).WillOnce(Return(is_owner));
  }

  void expect_is_resync_requested(librbd::MockTestJournal &mock_journal,
                                  int r, bool resync_requested) {
    EXPECT_CALL(mock_journal, is_resync_requested(_)).WillOnce(
      DoAll(SetArgPointee<0>(resync_requested),
            Return(r)));
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

  void expect_flush(MockReplay& mock_replay, int r) {
    EXPECT_CALL(mock_replay, flush(_))
      .WillOnce(CompleteContext(m_threads->work_queue, r));
  }

  void expect_flush_commit_position(::journal::MockJournaler& mock_journal,
                                    int r) {
    EXPECT_CALL(mock_journal, flush_commit_position(_))
      .WillOnce(CompleteContext(m_threads->work_queue, r));
  }

  void expect_get_tag_data(librbd::MockTestJournal& mock_local_journal,
                           const librbd::journal::TagData& tag_data) {
    EXPECT_CALL(mock_local_journal, get_tag_data())
      .WillOnce(Return(tag_data));
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
      .WillOnce(Invoke([this]() {
          std::unique_lock locker{m_lock};
          m_notified = true;
          m_cond.notify_all();
        }));
  }

  int wait_for_notification() {
    std::unique_lock locker{m_lock};
    while (!m_notified) {
      if (m_cond.wait_for(locker, 10s) == std::cv_status::timeout) {
        return -ETIMEDOUT;
      }
    }
    m_notified = false;
    return 0;
  }

  void expect_local_journal_add_listener(
      librbd::MockTestJournal& mock_local_journal,
      librbd::journal::Listener** local_journal_listener) {
    EXPECT_CALL(mock_local_journal, add_listener(_))
      .WillOnce(SaveArg<0>(local_journal_listener));
    expect_is_tag_owner(mock_local_journal, false);
    expect_is_resync_requested(mock_local_journal, 0, false);
  }

  int init_entry_replayer(MockReplayer& mock_replayer,
                          MockThreads& mock_threads,
                          MockReplayerListener& mock_replayer_listener,
                          librbd::MockTestJournal& mock_local_journal,
                          ::journal::MockJournaler& mock_remote_journaler,
                          MockReplay& mock_local_journal_replay,
                          librbd::journal::Listener** local_journal_listener,
                          ::journal::ReplayHandler** remote_replay_handler,
                          ::journal::JournalMetadataListener** remote_journal_listener) {
    expect_init(mock_remote_journaler, 0);
    EXPECT_CALL(mock_remote_journaler, add_listener(_))
      .WillOnce(SaveArg<0>(remote_journal_listener));
    expect_get_cached_client(mock_remote_journaler, "local mirror uuid", {},
                             {librbd::journal::MirrorPeerClientMeta{}}, 0);
    expect_start_external_replay(mock_local_journal, &mock_local_journal_replay,
                                 0);
    expect_local_journal_add_listener(mock_local_journal,
                                      local_journal_listener);
    EXPECT_CALL(mock_remote_journaler, start_live_replay(_, _))
      .WillOnce(SaveArg<0>(remote_replay_handler));
    expect_notification(mock_threads, mock_replayer_listener);

    C_SaferCond init_ctx;
    mock_replayer.init(&init_ctx);
    int r = init_ctx.wait();
    if (r < 0) {
      return r;
    }

    return wait_for_notification();
  }

  int shut_down_entry_replayer(MockReplayer& mock_replayer,
                               MockThreads& mock_threads,
                               librbd::MockTestJournal& mock_local_journal,
                               ::journal::MockJournaler& mock_remote_journaler,
                               MockReplay& mock_local_journal_replay) {
    expect_shut_down(mock_local_journal_replay, true, 0);
    EXPECT_CALL(mock_local_journal, remove_listener(_));
    EXPECT_CALL(mock_local_journal, stop_external_replay());
    MockCloseImageRequest mock_close_image_request;
    expect_send(mock_close_image_request, 0);
    expect_stop_replay(mock_remote_journaler, 0);
    EXPECT_CALL(mock_remote_journaler, remove_listener(_));

    C_SaferCond shutdown_ctx;
    mock_replayer.shut_down(&shutdown_ctx);
    return shutdown_ctx.wait();
  }

  librbd::ImageCtx* m_local_image_ctx = nullptr;

  ceph::mutex m_lock = ceph::make_mutex(
    "TestMockImageReplayerJournalReplayer");
  ceph::condition_variable m_cond;
  bool m_notified = false;
};

TEST_F(TestMockImageReplayerJournalReplayer, InitShutDown) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

TEST_F(TestMockImageReplayerJournalReplayer, InitRemoteJournalerError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  expect_init(mock_remote_journaler, -EINVAL);
  MockCloseImageRequest mock_close_image_request;
  expect_send(mock_close_image_request, 0);

  C_SaferCond init_ctx;
  mock_replayer.init(&init_ctx);
  ASSERT_EQ(-EINVAL, init_ctx.wait());
}

TEST_F(TestMockImageReplayerJournalReplayer, InitRemoteJournalerGetClientError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  expect_init(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, "local mirror uuid", {},
                           {librbd::journal::MirrorPeerClientMeta{}}, -EINVAL);
  MockCloseImageRequest mock_close_image_request;
  expect_send(mock_close_image_request, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));

  C_SaferCond init_ctx;
  mock_replayer.init(&init_ctx);
  ASSERT_EQ(-EINVAL, init_ctx.wait());
}

TEST_F(TestMockImageReplayerJournalReplayer, InitNoLocalJournal) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};

  mock_local_image_ctx.journal = nullptr;
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;
  expect_init(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, "local mirror uuid", {},
                           {librbd::journal::MirrorPeerClientMeta{}}, 0);

  MockCloseImageRequest mock_close_image_request;
  expect_send(mock_close_image_request, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));

  C_SaferCond init_ctx;
  mock_replayer.init(&init_ctx);
  ASSERT_EQ(-EINVAL, init_ctx.wait());
}

TEST_F(TestMockImageReplayerJournalReplayer, InitLocalJournalStartExternalReplayError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  expect_init(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, "local mirror uuid", {},
                           {librbd::journal::MirrorPeerClientMeta{}}, 0);
  expect_start_external_replay(mock_local_journal, nullptr, -EINVAL);
  MockCloseImageRequest mock_close_image_request;
  expect_send(mock_close_image_request, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));

  C_SaferCond init_ctx;
  mock_replayer.init(&init_ctx);
  ASSERT_EQ(-EINVAL, init_ctx.wait());
}

TEST_F(TestMockImageReplayerJournalReplayer, InitIsPromoted) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  expect_init(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, "local mirror uuid", {},
                           {librbd::journal::MirrorPeerClientMeta{}}, 0);
  MockReplay mock_local_journal_replay;
  expect_start_external_replay(mock_local_journal, &mock_local_journal_replay,
                               0);
  EXPECT_CALL(mock_local_journal, add_listener(_));
  expect_is_tag_owner(mock_local_journal, true);
  expect_notification(mock_threads, mock_replayer_listener);

  C_SaferCond init_ctx;
  mock_replayer.init(&init_ctx);
  ASSERT_EQ(0, init_ctx.wait());
  ASSERT_EQ(0, wait_for_notification());

  expect_shut_down(mock_local_journal_replay, true, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  MockCloseImageRequest mock_close_image_request;
  expect_send(mock_close_image_request, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));

  C_SaferCond shutdown_ctx;
  mock_replayer.shut_down(&shutdown_ctx);
  ASSERT_EQ(0, shutdown_ctx.wait());
}

TEST_F(TestMockImageReplayerJournalReplayer, InitDisconnected) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  mock_local_image_ctx.config.set_val("rbd_mirroring_resync_after_disconnect",
                                      "false");

  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  expect_init(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, "local mirror uuid",
                           {{}, {}, {},
                            cls::journal::CLIENT_STATE_DISCONNECTED},
                           {librbd::journal::MirrorPeerClientMeta{
                              mock_local_image_ctx.id}}, 0);
  MockCloseImageRequest mock_close_image_request;
  expect_send(mock_close_image_request, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));

  C_SaferCond init_ctx;
  mock_replayer.init(&init_ctx);
  ASSERT_EQ(-ENOTCONN, init_ctx.wait());
  ASSERT_FALSE(mock_replayer.is_resync_requested());
}

TEST_F(TestMockImageReplayerJournalReplayer, InitDisconnectedResync) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  mock_local_image_ctx.config.set_val("rbd_mirroring_resync_after_disconnect",
                                      "true");

  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  expect_init(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, "local mirror uuid",
                           {{}, {}, {},
                            cls::journal::CLIENT_STATE_DISCONNECTED},
                           {librbd::journal::MirrorPeerClientMeta{
                              mock_local_image_ctx.id}}, 0);
  MockCloseImageRequest mock_close_image_request;
  expect_send(mock_close_image_request, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));

  C_SaferCond init_ctx;
  mock_replayer.init(&init_ctx);
  ASSERT_EQ(-ENOTCONN, init_ctx.wait());
  ASSERT_TRUE(mock_replayer.is_resync_requested());
}

TEST_F(TestMockImageReplayerJournalReplayer, InitResyncRequested) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  expect_init(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, "local mirror uuid", {},
                           {librbd::journal::MirrorPeerClientMeta{}}, 0);
  MockReplay mock_local_journal_replay;
  expect_start_external_replay(mock_local_journal, &mock_local_journal_replay,
                               0);
  EXPECT_CALL(mock_local_journal, add_listener(_));
  expect_is_tag_owner(mock_local_journal, false);
  expect_is_resync_requested(mock_local_journal, 0, true);
  expect_notification(mock_threads, mock_replayer_listener);

  C_SaferCond init_ctx;
  mock_replayer.init(&init_ctx);
  ASSERT_EQ(0, init_ctx.wait());
  ASSERT_EQ(0, wait_for_notification());

  expect_shut_down(mock_local_journal_replay, true, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  MockCloseImageRequest mock_close_image_request;
  expect_send(mock_close_image_request, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));

  C_SaferCond shutdown_ctx;
  mock_replayer.shut_down(&shutdown_ctx);
  ASSERT_EQ(0, shutdown_ctx.wait());
}

TEST_F(TestMockImageReplayerJournalReplayer, InitResyncRequestedError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  expect_init(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, add_listener(_));
  expect_get_cached_client(mock_remote_journaler, "local mirror uuid", {},
                           {librbd::journal::MirrorPeerClientMeta{}}, 0);
  MockReplay mock_local_journal_replay;
  expect_start_external_replay(mock_local_journal, &mock_local_journal_replay,
                               0);
  EXPECT_CALL(mock_local_journal, add_listener(_));
  expect_is_tag_owner(mock_local_journal, false);
  expect_is_resync_requested(mock_local_journal, -EINVAL, false);
  expect_notification(mock_threads, mock_replayer_listener);

  C_SaferCond init_ctx;
  mock_replayer.init(&init_ctx);
  ASSERT_EQ(0, init_ctx.wait());
  ASSERT_EQ(0, wait_for_notification());
  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  expect_shut_down(mock_local_journal_replay, true, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  MockCloseImageRequest mock_close_image_request;
  expect_send(mock_close_image_request, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));

  C_SaferCond shutdown_ctx;
  mock_replayer.shut_down(&shutdown_ctx);
  ASSERT_EQ(0, shutdown_ctx.wait());
}

TEST_F(TestMockImageReplayerJournalReplayer, ShutDownLocalJournalReplayError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  expect_shut_down(mock_local_journal_replay, true, -EINVAL);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  MockCloseImageRequest mock_close_image_request;
  expect_send(mock_close_image_request, 0);
  expect_stop_replay(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));

  C_SaferCond shutdown_ctx;
  mock_replayer.shut_down(&shutdown_ctx);
  ASSERT_EQ(-EINVAL, shutdown_ctx.wait());
}

TEST_F(TestMockImageReplayerJournalReplayer, CloseLocalImageError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  expect_shut_down(mock_local_journal_replay, true, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  MockCloseImageRequest mock_close_image_request;
  expect_send(mock_close_image_request, -EINVAL);
  expect_stop_replay(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));

  C_SaferCond shutdown_ctx;
  mock_replayer.shut_down(&shutdown_ctx);
  ASSERT_EQ(-EINVAL, shutdown_ctx.wait());
}

TEST_F(TestMockImageReplayerJournalReplayer, StopRemoteJournalerError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  expect_shut_down(mock_local_journal_replay, true, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  MockCloseImageRequest mock_close_image_request;
  expect_send(mock_close_image_request, 0);
  expect_stop_replay(mock_remote_journaler, -EPERM);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));

  C_SaferCond shutdown_ctx;
  mock_replayer.shut_down(&shutdown_ctx);
  ASSERT_EQ(-EPERM, shutdown_ctx.wait());
}

TEST_F(TestMockImageReplayerJournalReplayer, Replay) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  ::journal::MockReplayEntry mock_replay_entry;
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_get_tag_tid_in_debug(mock_local_journal);
  expect_committed(mock_replay_entry, mock_remote_journaler, 2);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  cls::journal::Tag tag =
    {1, 0, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                            librbd::Journal<>::LOCAL_MIRROR_UUID,
                            true, 0, 0})};

  expect_try_pop_front(mock_remote_journaler, tag.tid, true);

  // replay_flush
  expect_shut_down(mock_local_journal_replay, false, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, &mock_local_journal_replay,
                               0);
  expect_local_journal_add_listener(mock_local_journal,
                                    &local_journal_listener);
  expect_get_tag(mock_remote_journaler, tag, 0);
  expect_allocate_tag(mock_local_journal, 0);

  // process
  EXPECT_CALL(mock_local_journal_replay, decode(_, _)).WillOnce(Return(0));
  expect_preprocess(mock_event_preprocessor, false, 0);
  expect_process(mock_local_journal_replay, 0, 0);
  EXPECT_CALL(mock_replay_status_formatter, handle_entry_processed(_));

  // the next event with preprocess
  expect_try_pop_front(mock_remote_journaler, tag.tid, true);
  EXPECT_CALL(mock_local_journal_replay, decode(_, _)).WillOnce(Return(0));
  expect_preprocess(mock_event_preprocessor, true, 0);
  expect_process(mock_local_journal_replay, 0, 0);
  EXPECT_CALL(mock_replay_status_formatter, handle_entry_processed(_));

  // attempt to process the next event
  C_SaferCond replay_ctx;
  expect_try_pop_front_return_no_entries(mock_remote_journaler, &replay_ctx);

  // fire
  remote_replay_handler->handle_entries_available();
  ASSERT_EQ(0, replay_ctx.wait());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

TEST_F(TestMockImageReplayerJournalReplayer, DecodeError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  ::journal::MockReplayEntry mock_replay_entry;
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_get_tag_tid_in_debug(mock_local_journal);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  cls::journal::Tag tag =
    {1, 0, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                            librbd::Journal<>::LOCAL_MIRROR_UUID,
                            true, 0, 0})};

  expect_try_pop_front(mock_remote_journaler, tag.tid, true);

  // replay_flush
  expect_shut_down(mock_local_journal_replay, false, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, &mock_local_journal_replay,
                               0);
  expect_local_journal_add_listener(mock_local_journal,
                                    &local_journal_listener);
  expect_get_tag(mock_remote_journaler, tag, 0);
  expect_allocate_tag(mock_local_journal, 0);

  // process
  EXPECT_CALL(mock_replay_entry, get_data());
  EXPECT_CALL(mock_local_journal_replay, decode(_, _))
    .WillOnce(Return(-EINVAL));
  expect_notification(mock_threads, mock_replayer_listener);

  // fire
  remote_replay_handler->handle_entries_available();
  wait_for_notification();

  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

TEST_F(TestMockImageReplayerJournalReplayer, DelayedReplay) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  ::journal::MockReplayEntry mock_replay_entry;
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_get_tag_tid_in_debug(mock_local_journal);
  expect_committed(mock_replay_entry, mock_remote_journaler, 1);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  cls::journal::Tag tag =
    {1, 0, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                            librbd::Journal<>::LOCAL_MIRROR_UUID,
                            true, 0, 0})};

  expect_try_pop_front(mock_remote_journaler, tag.tid, true);

  // replay_flush
  expect_shut_down(mock_local_journal_replay, false, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, &mock_local_journal_replay,
                               0);
  expect_local_journal_add_listener(mock_local_journal,
                                    &local_journal_listener);
  expect_get_tag(mock_remote_journaler, tag, 0);
  expect_allocate_tag(mock_local_journal, 0);

  // process with delay
  EXPECT_CALL(mock_replay_entry, get_data());
  librbd::journal::EventEntry event_entry(
    librbd::journal::AioDiscardEvent(123, 345, 0), ceph_clock_now());
  EXPECT_CALL(mock_local_journal_replay, decode(_, _))
    .WillOnce(DoAll(SetArgPointee<1>(event_entry),
                    Return(0)));

  Context* delayed_task_ctx = nullptr;
  EXPECT_CALL(*mock_threads.timer, add_event_after(_, _))
    .WillOnce(
      DoAll(Invoke([this, &delayed_task_ctx](double seconds, Context *ctx) {
                std::unique_lock locker{m_lock};
                delayed_task_ctx = ctx;
                m_cond.notify_all();
            }),
            ReturnArg<1>()));
  expect_preprocess(mock_event_preprocessor, false, 0);
  expect_process(mock_local_journal_replay, 0, 0);
  EXPECT_CALL(mock_replay_status_formatter, handle_entry_processed(_));

  // attempt to process the next event
  C_SaferCond replay_ctx;
  expect_try_pop_front_return_no_entries(mock_remote_journaler, &replay_ctx);

  // fire
  mock_local_image_ctx.mirroring_replay_delay = 600;
  remote_replay_handler->handle_entries_available();
  {
    std::unique_lock locker{m_lock};
    while (delayed_task_ctx == nullptr) {
      if (m_cond.wait_for(locker, 10s) == std::cv_status::timeout) {
        FAIL() << "timed out waiting for task";
        break;
      }
    }
  }
  {
    std::unique_lock timer_locker{mock_threads.timer_lock};
    delayed_task_ctx->complete(0);
  }
  ASSERT_EQ(0, replay_ctx.wait());

  // add a pending (delayed) entry before stop
  expect_try_pop_front(mock_remote_journaler, tag.tid, true);
  C_SaferCond decode_ctx;
  EXPECT_CALL(mock_local_journal_replay, decode(_, _))
    .WillOnce(DoAll(Invoke([&decode_ctx](bufferlist::const_iterator* it,
                                         librbd::journal::EventEntry *e) {
                             decode_ctx.complete(0);
                           }),
                    Return(0)));

  remote_replay_handler->handle_entries_available();
  ASSERT_EQ(0, decode_ctx.wait());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

TEST_F(TestMockImageReplayerJournalReplayer, ReplayNoMemoryError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  expect_notification(mock_threads, mock_replayer_listener);
  remote_replay_handler->handle_complete(-ENOMEM);

  wait_for_notification();
  ASSERT_EQ(false, mock_replayer.is_replaying());
  ASSERT_EQ(-ENOMEM, mock_replayer.get_error_code());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

TEST_F(TestMockImageReplayerJournalReplayer, LocalJournalForcePromoted) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  expect_notification(mock_threads, mock_replayer_listener);
  local_journal_listener->handle_promoted();
  wait_for_notification();

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

TEST_F(TestMockImageReplayerJournalReplayer, LocalJournalResyncRequested) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  expect_notification(mock_threads, mock_replayer_listener);
  local_journal_listener->handle_resync();
  wait_for_notification();

  ASSERT_TRUE(mock_replayer.is_resync_requested());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

TEST_F(TestMockImageReplayerJournalReplayer, RemoteJournalDisconnected) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  mock_local_image_ctx.config.set_val("rbd_mirroring_resync_after_disconnect",
                                      "true");

  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  expect_get_cached_client(mock_remote_journaler, "local mirror uuid",
                           {{}, {}, {},
                            cls::journal::CLIENT_STATE_DISCONNECTED},
                           {librbd::journal::MirrorPeerClientMeta{
                              mock_local_image_ctx.id}}, 0);
  expect_notification(mock_threads, mock_replayer_listener);

  remote_journaler_listener->handle_update(nullptr);
  wait_for_notification();

  ASSERT_EQ(-ENOTCONN, mock_replayer.get_error_code());
  ASSERT_FALSE(mock_replayer.is_replaying());
  ASSERT_TRUE(mock_replayer.is_resync_requested());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

TEST_F(TestMockImageReplayerJournalReplayer, Flush) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  expect_flush(mock_local_journal_replay, 0);
  expect_flush_commit_position(mock_remote_journaler, 0);

  C_SaferCond ctx;
  mock_replayer.flush(&ctx);
  ASSERT_EQ(0, ctx.wait());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

TEST_F(TestMockImageReplayerJournalReplayer, FlushError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  expect_flush(mock_local_journal_replay, -EINVAL);

  C_SaferCond ctx;
  mock_replayer.flush(&ctx);
  ASSERT_EQ(-EINVAL, ctx.wait());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

TEST_F(TestMockImageReplayerJournalReplayer, FlushCommitPositionError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  expect_flush(mock_local_journal_replay, 0);
  expect_flush_commit_position(mock_remote_journaler, -EINVAL);

  C_SaferCond ctx;
  mock_replayer.flush(&ctx);
  ASSERT_EQ(-EINVAL, ctx.wait());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}


TEST_F(TestMockImageReplayerJournalReplayer, ReplayFlushShutDownError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  ::journal::MockReplayEntry mock_replay_entry;
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  expect_try_pop_front(mock_remote_journaler, 1, true);
  expect_shut_down(mock_local_journal_replay, false, -EINVAL);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_notification(mock_threads, mock_replayer_listener);
  remote_replay_handler->handle_entries_available();

  wait_for_notification();
  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  MockCloseImageRequest mock_close_image_request;
  expect_send(mock_close_image_request, 0);
  expect_stop_replay(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));

  C_SaferCond shutdown_ctx;
  mock_replayer.shut_down(&shutdown_ctx);
  ASSERT_EQ(0, shutdown_ctx.wait());
}

TEST_F(TestMockImageReplayerJournalReplayer, ReplayFlushStartError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  ::journal::MockReplayEntry mock_replay_entry;
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  expect_try_pop_front(mock_remote_journaler, 1, true);
  expect_shut_down(mock_local_journal_replay, false, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, nullptr, -EINVAL);
  expect_notification(mock_threads, mock_replayer_listener);
  remote_replay_handler->handle_entries_available();

  wait_for_notification();
  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  MockCloseImageRequest mock_close_image_request;
  expect_send(mock_close_image_request, 0);
  expect_stop_replay(mock_remote_journaler, 0);
  EXPECT_CALL(mock_remote_journaler, remove_listener(_));

  C_SaferCond shutdown_ctx;
  mock_replayer.shut_down(&shutdown_ctx);
  ASSERT_EQ(0, shutdown_ctx.wait());
}

TEST_F(TestMockImageReplayerJournalReplayer, GetTagError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  ::journal::MockReplayEntry mock_replay_entry;
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_work_queue_repeatedly(mock_threads);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  cls::journal::Tag tag =
    {1, 0, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                            librbd::Journal<>::LOCAL_MIRROR_UUID,
                            true, 0, 0})};
  expect_try_pop_front(mock_remote_journaler, tag.tid, true);
  expect_shut_down(mock_local_journal_replay, false, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, &mock_local_journal_replay,
                               0);
  expect_local_journal_add_listener(mock_local_journal,
                                    &local_journal_listener);
  expect_get_tag(mock_remote_journaler, tag, -EINVAL);
  expect_notification(mock_threads, mock_replayer_listener);
  remote_replay_handler->handle_entries_available();

  wait_for_notification();
  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

TEST_F(TestMockImageReplayerJournalReplayer, AllocateTagDemotion) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  ::journal::MockReplayEntry mock_replay_entry;
  expect_work_queue_repeatedly(mock_threads);
  expect_notification(mock_threads, mock_replayer_listener);
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_get_tag_tid_in_debug(mock_local_journal);
  expect_committed(mock_replay_entry, mock_remote_journaler, 1);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  cls::journal::Tag tag =
    {1, 0, encode_tag_data({librbd::Journal<>::ORPHAN_MIRROR_UUID,
                            librbd::Journal<>::LOCAL_MIRROR_UUID,
                            true, 0, 0})};

  expect_try_pop_front(mock_remote_journaler, tag.tid, true);
  expect_shut_down(mock_local_journal_replay, false, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, &mock_local_journal_replay,
                               0);
  expect_local_journal_add_listener(mock_local_journal,
                                    &local_journal_listener);
  expect_get_tag(mock_remote_journaler, tag, 0);
  expect_get_tag_data(mock_local_journal, {});
  expect_allocate_tag(mock_local_journal, 0);
  EXPECT_CALL(mock_local_journal_replay, decode(_, _)).WillOnce(Return(0));
  expect_preprocess(mock_event_preprocessor, false, 0);
  expect_process(mock_local_journal_replay, 0, 0);
  EXPECT_CALL(mock_replay_status_formatter, handle_entry_processed(_));

  remote_replay_handler->handle_entries_available();
  wait_for_notification();
  ASSERT_FALSE(mock_replayer.is_replaying());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

TEST_F(TestMockImageReplayerJournalReplayer, AllocateTagError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  ::journal::MockReplayEntry mock_replay_entry;
  expect_work_queue_repeatedly(mock_threads);
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_get_tag_tid_in_debug(mock_local_journal);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  cls::journal::Tag tag =
    {1, 0, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                            librbd::Journal<>::LOCAL_MIRROR_UUID,
                            true, 0, 0})};

  expect_try_pop_front(mock_remote_journaler, tag.tid, true);
  expect_shut_down(mock_local_journal_replay, false, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, &mock_local_journal_replay,
                               0);
  expect_local_journal_add_listener(mock_local_journal,
                                    &local_journal_listener);
  expect_get_tag(mock_remote_journaler, tag, 0);
  expect_allocate_tag(mock_local_journal, -EINVAL);
  expect_notification(mock_threads, mock_replayer_listener);
  remote_replay_handler->handle_entries_available();

  wait_for_notification();
  ASSERT_FALSE(mock_replayer.is_replaying());
  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

TEST_F(TestMockImageReplayerJournalReplayer, PreprocessError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  ::journal::MockReplayEntry mock_replay_entry;
  expect_work_queue_repeatedly(mock_threads);
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_get_tag_tid_in_debug(mock_local_journal);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  cls::journal::Tag tag =
    {1, 0, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                            librbd::Journal<>::LOCAL_MIRROR_UUID,
                            true, 0, 0})};

  expect_try_pop_front(mock_remote_journaler, tag.tid, true);
  expect_shut_down(mock_local_journal_replay, false, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, &mock_local_journal_replay,
                               0);
  expect_local_journal_add_listener(mock_local_journal,
                                    &local_journal_listener);
  expect_get_tag(mock_remote_journaler, tag, 0);
  expect_allocate_tag(mock_local_journal, 0);
  EXPECT_CALL(mock_replay_entry, get_data());
  EXPECT_CALL(mock_local_journal_replay, decode(_, _)).WillOnce(Return(0));
  expect_preprocess(mock_event_preprocessor, true, -EINVAL);

  expect_notification(mock_threads, mock_replayer_listener);
  remote_replay_handler->handle_entries_available();

  wait_for_notification();
  ASSERT_FALSE(mock_replayer.is_replaying());
  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

TEST_F(TestMockImageReplayerJournalReplayer, ProcessError) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  ::journal::MockReplayEntry mock_replay_entry;
  expect_work_queue_repeatedly(mock_threads);
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_get_tag_tid_in_debug(mock_local_journal);
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  cls::journal::Tag tag =
    {1, 0, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                            librbd::Journal<>::LOCAL_MIRROR_UUID,
                            true, 0, 0})};

  expect_try_pop_front(mock_remote_journaler, tag.tid, true);
  expect_shut_down(mock_local_journal_replay, false, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, &mock_local_journal_replay,
                               0);
  expect_local_journal_add_listener(mock_local_journal,
                                    &local_journal_listener);
  expect_get_tag(mock_remote_journaler, tag, 0);
  expect_allocate_tag(mock_local_journal, 0);
  EXPECT_CALL(mock_replay_entry, get_data());
  EXPECT_CALL(mock_local_journal_replay, decode(_, _)).WillOnce(Return(0));
  expect_preprocess(mock_event_preprocessor, false, 0);
  expect_process(mock_local_journal_replay, 0, -EINVAL);
  EXPECT_CALL(mock_replay_status_formatter, handle_entry_processed(_));

  // attempt to process the next event
  C_SaferCond replay_ctx;
  expect_try_pop_front_return_no_entries(mock_remote_journaler, &replay_ctx);
  remote_replay_handler->handle_entries_available();

  wait_for_notification();
  ASSERT_FALSE(mock_replayer.is_replaying());
  ASSERT_EQ(-EINVAL, mock_replayer.get_error_code());

  ASSERT_EQ(0, replay_ctx.wait());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

TEST_F(TestMockImageReplayerJournalReplayer, ImageNameUpdated) {
  librbd::MockTestJournal mock_local_journal;
  librbd::MockTestImageCtx mock_local_image_ctx{*m_local_image_ctx,
                                                mock_local_journal};
  ::journal::MockJournaler mock_remote_journaler;
  MockReplayerListener mock_replayer_listener;
  MockThreads mock_threads{m_threads};
  MockStateBuilder mock_state_builder(mock_local_image_ctx,
                                      mock_remote_journaler,
                                      {});
  MockReplayer mock_replayer{
    &mock_threads, "local mirror uuid", &mock_state_builder,
    &mock_replayer_listener};

  ::journal::MockReplayEntry mock_replay_entry;
  expect_work_queue_repeatedly(mock_threads);
  expect_add_event_after_repeatedly(mock_threads);
  expect_get_commit_tid_in_debug(mock_replay_entry);
  expect_get_tag_tid_in_debug(mock_local_journal);
  expect_committed(mock_replay_entry, mock_remote_journaler, 1);
  expect_notification(mock_threads, mock_replayer_listener);

  InSequence seq;

  MockReplay mock_local_journal_replay;
  MockEventPreprocessor mock_event_preprocessor;
  MockReplayStatusFormatter mock_replay_status_formatter;
  librbd::journal::Listener* local_journal_listener = nullptr;
  ::journal::ReplayHandler* remote_replay_handler = nullptr;
  ::journal::JournalMetadataListener* remote_journaler_listener = nullptr;
  ASSERT_EQ(0, init_entry_replayer(mock_replayer, mock_threads,
                                   mock_replayer_listener, mock_local_journal,
                                   mock_remote_journaler,
                                   mock_local_journal_replay,
                                   &local_journal_listener,
                                   &remote_replay_handler,
                                   &remote_journaler_listener));

  mock_local_image_ctx.name = "NEW NAME";
  cls::journal::Tag tag =
    {1, 0, encode_tag_data({librbd::Journal<>::LOCAL_MIRROR_UUID,
                            librbd::Journal<>::LOCAL_MIRROR_UUID,
                            true, 0, 0})};

  expect_try_pop_front(mock_remote_journaler, tag.tid, true);
  expect_shut_down(mock_local_journal_replay, false, 0);
  EXPECT_CALL(mock_local_journal, remove_listener(_));
  EXPECT_CALL(mock_local_journal, stop_external_replay());
  expect_start_external_replay(mock_local_journal, &mock_local_journal_replay,
                               0);
  expect_local_journal_add_listener(mock_local_journal,
                                    &local_journal_listener);
  expect_get_tag(mock_remote_journaler, tag, 0);
  expect_allocate_tag(mock_local_journal, 0);
  EXPECT_CALL(mock_local_journal_replay, decode(_, _)).WillOnce(Return(0));
  expect_preprocess(mock_event_preprocessor, false, 0);
  expect_process(mock_local_journal_replay, 0, 0);
  EXPECT_CALL(mock_replay_status_formatter, handle_entry_processed(_));

  // attempt to process the next event
  C_SaferCond replay_ctx;
  expect_try_pop_front_return_no_entries(mock_remote_journaler, &replay_ctx);

  remote_replay_handler->handle_entries_available();
  wait_for_notification();

  auto image_spec = util::compute_image_spec(m_local_io_ctx, "NEW NAME");
  ASSERT_EQ(image_spec, mock_replayer.get_image_spec());

  ASSERT_EQ(0, replay_ctx.wait());
  ASSERT_TRUE(mock_replayer.is_replaying());

  ASSERT_EQ(0, shut_down_entry_replayer(mock_replayer, mock_threads,
                                        mock_local_journal,
                                        mock_remote_journaler,
                                        mock_local_journal_replay));
}

} // namespace journal
} // namespace image_replayer
} // namespace mirror
} // namespace rbd
