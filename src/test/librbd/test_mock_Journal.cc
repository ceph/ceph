// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/journal/mock/MockJournaler.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournalPolicy.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "cls/journal/cls_journal_types.h"
#include "journal/Journaler.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioObjectRequest.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/journal/Replay.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <functional>
#include <list>
#include <boost/scope_exit.hpp>

#define dout_subsys ceph_subsys_rbd

namespace librbd {

namespace {

struct MockJournalImageCtx : public MockImageCtx {
  MockJournalImageCtx(librbd::ImageCtx& image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace journal {

template <>
struct TypeTraits<MockJournalImageCtx> {
  typedef ::journal::MockJournalerProxy Journaler;
  typedef ::journal::MockFutureProxy  Future;
  typedef ::journal::MockReplayEntryProxy ReplayEntry;
};

struct MockReplay {
  static MockReplay *s_instance;
  static MockReplay &get_instance() {
    assert(s_instance != nullptr);
    return *s_instance;
  }

  MockReplay() {
    s_instance = this;
  }

  MOCK_METHOD2(shut_down, void(bool cancel_ops, Context *));
  MOCK_METHOD2(decode, int(bufferlist::iterator*, EventEntry *));
  MOCK_METHOD3(process, void(const EventEntry&, Context *, Context *));
  MOCK_METHOD2(replay_op_ready, void(uint64_t, Context *));
};

template <>
class Replay<MockJournalImageCtx> {
public:
  static Replay *create(MockJournalImageCtx &image_ctx) {
    return new Replay();
  }

  void shut_down(bool cancel_ops, Context *on_finish) {
    MockReplay::get_instance().shut_down(cancel_ops, on_finish);
  }

  int decode(bufferlist::iterator *it, EventEntry *event_entry) {
    return MockReplay::get_instance().decode(it, event_entry);
  }

  void process(const EventEntry& event_entry, Context *on_ready,
               Context *on_commit) {
    MockReplay::get_instance().process(event_entry, on_ready, on_commit);
  }

  void replay_op_ready(uint64_t op_tid, Context *on_resume) {
    MockReplay::get_instance().replay_op_ready(op_tid, on_resume);
  }
};

MockReplay *MockReplay::s_instance = nullptr;

} // namespace journal
} // namespace librbd

// template definitions
#include "librbd/Journal.cc"
template class librbd::Journal<librbd::MockJournalImageCtx>;

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::InvokeWithoutArgs;
using ::testing::MatcherCast;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::SetArgPointee;
using ::testing::WithArg;
using namespace std::placeholders;

ACTION_P2(StartReplay, wq, ctx) {
  wq->queue(ctx, 0);
}

namespace librbd {

class TestMockJournal : public TestMockFixture {
public:
  typedef journal::MockReplay MockJournalReplay;
  typedef Journal<MockJournalImageCtx> MockJournal;

  typedef std::function<void(::journal::ReplayHandler*)> ReplayAction;
  typedef std::list<Context *> Contexts;

  TestMockJournal() : m_lock("lock") {
  }

  ~TestMockJournal() {
    assert(m_commit_contexts.empty());
  }

  Mutex m_lock;
  Cond m_cond;
  Contexts m_commit_contexts;

  struct C_ReplayAction : public Context {
    ::journal::ReplayHandler **replay_handler;
    ReplayAction replay_action;

    C_ReplayAction(::journal::ReplayHandler **replay_handler,
                   const ReplayAction &replay_action)
      : replay_handler(replay_handler), replay_action(replay_action) {
    }
    virtual void finish(int r) {
      if (replay_action) {
        replay_action(*replay_handler);
      }
    }
  };

  void expect_construct_journaler(::journal::MockJournaler &mock_journaler) {
    EXPECT_CALL(mock_journaler, construct());
  }

  void expect_init_journaler(::journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, init(_))
                  .WillOnce(CompleteContext(r, NULL));
  }

  void expect_shut_down_journaler(::journal::MockJournaler &mock_journaler) {
    EXPECT_CALL(mock_journaler, remove_listener(_));
    EXPECT_CALL(mock_journaler, shut_down(_))
                  .WillOnce(CompleteContext(0, NULL));
  }

  void expect_get_max_append_size(::journal::MockJournaler &mock_journaler,
                                  uint32_t max_size) {
    EXPECT_CALL(mock_journaler, get_max_append_size())
                  .WillOnce(Return(max_size));
  }

  void expect_get_journaler_cached_client(::journal::MockJournaler &mock_journaler, int r) {
    journal::ImageClientMeta image_client_meta;
    image_client_meta.tag_class = 0;
    expect_get_journaler_cached_client(mock_journaler, image_client_meta, r);
  }

  void expect_get_journaler_cached_client(::journal::MockJournaler &mock_journaler,
                                          const journal::ImageClientMeta &client_meta,
                                          int r) {
    journal::ClientData client_data;
    client_data.client_meta = client_meta;

    cls::journal::Client client;
    ::encode(client_data, client.data);

    EXPECT_CALL(mock_journaler, get_cached_client("", _))
                  .WillOnce(DoAll(SetArgPointee<1>(client),
                                  Return(r)));
  }

  void expect_get_journaler_tags(MockImageCtx &mock_image_ctx,
                                 ::journal::MockJournaler &mock_journaler,
                                 bool primary, int r) {
    journal::TagData tag_data;
    if (!primary) {
      tag_data.mirror_uuid = "remote mirror uuid";
    }

    bufferlist tag_data_bl;
    ::encode(tag_data, tag_data_bl);

    ::journal::Journaler::Tags tags = {{0, 0, {}}, {1, 0, tag_data_bl}};
    EXPECT_CALL(mock_journaler, get_tags(0, _, _))
                  .WillOnce(DoAll(SetArgPointee<1>(tags),
                                  WithArg<2>(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue))));
    EXPECT_CALL(mock_journaler, add_listener(_))
                  .WillOnce(SaveArg<0>(&m_listener));
  }

  void expect_get_journaler_tags(MockImageCtx &mock_image_ctx,
                                 ::journal::MockJournaler &mock_journaler,
                                 uint64_t start_after_tag_tid,
                                 ::journal::Journaler::Tags &&tags, int r) {
    EXPECT_CALL(mock_journaler, get_tags(start_after_tag_tid, 0, _, _))
                  .WillOnce(DoAll(SetArgPointee<2>(tags),
                                  WithArg<3>(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue))));
  }

  void expect_start_replay(MockJournalImageCtx &mock_image_ctx,
                           ::journal::MockJournaler &mock_journaler,
                           const ReplayAction &action) {
    EXPECT_CALL(mock_journaler, start_replay(_))
                 .WillOnce(DoAll(SaveArg<0>(&m_replay_handler),
                           StartReplay(mock_image_ctx.image_ctx->op_work_queue,
                                       new C_ReplayAction(&m_replay_handler,
                                                          action))));
  }

  void expect_stop_replay(::journal::MockJournaler &mock_journaler) {
    EXPECT_CALL(mock_journaler, stop_replay(_))
                  .WillOnce(CompleteContext(0, NULL));
  }

  void expect_shut_down_replay(MockJournalImageCtx &mock_image_ctx,
                               MockJournalReplay &mock_journal_replay, int r,
                               bool cancel_ops = false) {
    EXPECT_CALL(mock_journal_replay, shut_down(cancel_ops, _))
                  .WillOnce(WithArg<1>(Invoke([this, &mock_image_ctx, r](Context *on_flush) {
                    this->commit_replay(mock_image_ctx, on_flush, r);})));
  }

  void expect_get_data(::journal::MockReplayEntry &mock_replay_entry) {
    EXPECT_CALL(mock_replay_entry, get_data())
                  .WillOnce(Return(bufferlist()));
  }

  void expect_try_pop_front(MockJournalImageCtx &mock_image_ctx,
                            ::journal::MockJournaler &mock_journaler,
                            bool entries_available,
                            ::journal::MockReplayEntry &mock_replay_entry,
                            const ReplayAction &action = {}) {
    EXPECT_CALL(mock_journaler, try_pop_front(_))
                  .WillOnce(DoAll(SetArgPointee<0>(::journal::MockReplayEntryProxy()),
                                  StartReplay(mock_image_ctx.image_ctx->op_work_queue,
                                              new C_ReplayAction(&m_replay_handler,
                                                                 action)),
                                  Return(entries_available)));
    if (entries_available) {
      expect_get_data(mock_replay_entry);
    }
  }

  void expect_replay_process(MockJournalReplay &mock_journal_replay) {
    EXPECT_CALL(mock_journal_replay, decode(_, _))
                  .WillOnce(Return(0));
    EXPECT_CALL(mock_journal_replay, process(_, _, _))
                  .WillOnce(DoAll(WithArg<1>(CompleteContext(0, NULL)),
                                  WithArg<2>(Invoke(this, &TestMockJournal::save_commit_context))));
  }

  void expect_start_append(::journal::MockJournaler &mock_journaler) {
    EXPECT_CALL(mock_journaler, start_append(_, _, _));
  }

  void expect_stop_append(::journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, stop_append(_))
                  .WillOnce(CompleteContext(r, NULL));
  }

  void expect_committed(::journal::MockJournaler &mock_journaler,
                        size_t events) {
    EXPECT_CALL(mock_journaler, committed(MatcherCast<const ::journal::MockReplayEntryProxy&>(_)))
                  .Times(events);
  }

  void expect_append_journaler(::journal::MockJournaler &mock_journaler) {
    EXPECT_CALL(mock_journaler, append(_, _))
                  .WillOnce(Return(::journal::MockFutureProxy()));
  }

  void expect_wait_future(::journal::MockFuture &mock_future,
                          Context **on_safe) {
    EXPECT_CALL(mock_future, wait(_))
                  .WillOnce(SaveArg<0>(on_safe));
  }

  void expect_future_committed(::journal::MockJournaler &mock_journaler) {
    EXPECT_CALL(mock_journaler, committed(MatcherCast<const ::journal::MockFutureProxy&>(_)));
  }

  void expect_future_is_valid(::journal::MockFuture &mock_future) {
    EXPECT_CALL(mock_future, is_valid()).WillOnce(Return(false));
  }

  void expect_flush_commit_position(::journal::MockJournaler &mock_journaler) {
    EXPECT_CALL(mock_journaler, flush_commit_position(_))
                  .WillOnce(CompleteContext(0, NULL));
  }

  int when_open(MockJournal &mock_journal) {
    C_SaferCond ctx;
    mock_journal.open(&ctx);
    return ctx.wait();
  }

  int when_close(MockJournal &mock_journal) {
    C_SaferCond ctx;
    mock_journal.close(&ctx);
    return ctx.wait();
  }

  uint64_t when_append_write_event(MockJournalImageCtx &mock_image_ctx,
                                   MockJournal &mock_journal, uint64_t length) {
    bufferlist bl;
    bl.append_zero(length);

    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    return mock_journal.append_write_event(0, length, bl, {}, false);
  }

  uint64_t when_append_io_event(MockJournalImageCtx &mock_image_ctx,
                                MockJournal &mock_journal,
                                AioObjectRequest<> *object_request = nullptr) {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    MockJournal::AioObjectRequests object_requests;
    if (object_request != nullptr) {
      object_requests.push_back(object_request);
    }
    return mock_journal.append_io_event(
      journal::EventEntry{journal::AioFlushEvent{}}, object_requests, 0, 0,
      false);
  }

  void save_commit_context(Context *ctx) {
    Mutex::Locker locker(m_lock);
    m_commit_contexts.push_back(ctx);
    m_cond.Signal();
  }

  void wake_up() {
    Mutex::Locker locker(m_lock);
    m_cond.Signal();
  }

  void commit_replay(MockJournalImageCtx &mock_image_ctx, Context *on_flush,
                     int r) {
    Contexts commit_contexts;
    std::swap(commit_contexts, m_commit_contexts);

    derr << "SHUT DOWN REPLAY START" << dendl;
    for (auto ctx : commit_contexts) {
      mock_image_ctx.image_ctx->op_work_queue->queue(ctx, r);
    }

    on_flush = new FunctionContext([on_flush](int r) {
        derr << "FLUSH START" << dendl;
        on_flush->complete(r);
        derr << "FLUSH FINISH" << dendl;
      });
    mock_image_ctx.image_ctx->op_work_queue->queue(on_flush, 0);
    derr << "SHUT DOWN REPLAY FINISH" << dendl;
  }

  void open_journal(MockJournalImageCtx &mock_image_ctx,
                    MockJournal &mock_journal,
                    ::journal::MockJournaler &mock_journaler,
                    bool primary = true) {
    expect_op_work_queue(mock_image_ctx);

    InSequence seq;
    expect_construct_journaler(mock_journaler);
    expect_init_journaler(mock_journaler, 0);
    expect_get_max_append_size(mock_journaler, 1 << 16);
    expect_get_journaler_cached_client(mock_journaler, 0);
    expect_get_journaler_tags(mock_image_ctx, mock_journaler, primary, 0);
    expect_start_replay(
      mock_image_ctx, mock_journaler,
      std::bind(&invoke_replay_complete, _1, 0));

    MockJournalReplay mock_journal_replay;
    expect_stop_replay(mock_journaler);
    expect_shut_down_replay(mock_image_ctx, mock_journal_replay, 0);
    expect_committed(mock_journaler, 0);
    expect_start_append(mock_journaler);
    ASSERT_EQ(0, when_open(mock_journal));
  }

  void close_journal(MockJournal &mock_journal,
                     ::journal::MockJournaler &mock_journaler) {
    expect_stop_append(mock_journaler, 0);
    ASSERT_EQ(0, when_close(mock_journal));
  }

  static void invoke_replay_ready(::journal::ReplayHandler *handler) {
    handler->handle_entries_available();
  }

  static void invoke_replay_complete(::journal::ReplayHandler *handler, int r) {
    handler->handle_complete(r);
  }

  ::journal::ReplayHandler *m_replay_handler = nullptr;
  ::journal::JournalMetadataListener *m_listener = nullptr;
};

TEST_F(TestMockJournal, StateTransitions) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_max_append_size(mock_journaler, 1 << 16);
  expect_get_journaler_cached_client(mock_journaler, 0);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, true, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler,
    std::bind(&invoke_replay_ready, _1));

  ::journal::MockReplayEntry mock_replay_entry;
  MockJournalReplay mock_journal_replay;
  expect_try_pop_front(mock_image_ctx, mock_journaler, true, mock_replay_entry);
  expect_replay_process(mock_journal_replay);
  expect_try_pop_front(mock_image_ctx, mock_journaler, true, mock_replay_entry);
  expect_replay_process(mock_journal_replay);
  expect_try_pop_front(mock_image_ctx, mock_journaler, false, mock_replay_entry,
                       std::bind(&invoke_replay_ready, _1));
  expect_try_pop_front(mock_image_ctx, mock_journaler, true, mock_replay_entry);
  expect_replay_process(mock_journal_replay);
  expect_try_pop_front(mock_image_ctx, mock_journaler, false, mock_replay_entry,
                       std::bind(&invoke_replay_complete, _1, 0));

  expect_stop_replay(mock_journaler);
  expect_shut_down_replay(mock_image_ctx, mock_journal_replay, 0);
  expect_committed(mock_journaler, 3);

  expect_start_append(mock_journaler);

  ASSERT_EQ(0, when_open(mock_journal));

  expect_stop_append(mock_journaler, 0);
  expect_shut_down_journaler(mock_journaler);
  ASSERT_EQ(0, when_close(mock_journal));
}

TEST_F(TestMockJournal, InitError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, -EINVAL);
  expect_shut_down_journaler(mock_journaler);
  ASSERT_EQ(-EINVAL, when_open(mock_journal));
}

TEST_F(TestMockJournal, GetCachedClientError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_max_append_size(mock_journaler, 1 << 16);
  expect_get_journaler_cached_client(mock_journaler, -ENOENT);
  expect_shut_down_journaler(mock_journaler);
  ASSERT_EQ(-ENOENT, when_open(mock_journal));
}

TEST_F(TestMockJournal, GetTagsError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_max_append_size(mock_journaler, 1 << 16);
  expect_get_journaler_cached_client(mock_journaler, 0);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, true, -EBADMSG);
  expect_shut_down_journaler(mock_journaler);
  ASSERT_EQ(-EBADMSG, when_open(mock_journal));
}

TEST_F(TestMockJournal, ReplayCompleteError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_max_append_size(mock_journaler, 1 << 16);
  expect_get_journaler_cached_client(mock_journaler, 0);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, true, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler,
    std::bind(&invoke_replay_complete, _1, -EINVAL));

  MockJournalReplay mock_journal_replay;
  expect_stop_replay(mock_journaler);
  expect_shut_down_replay(mock_image_ctx, mock_journal_replay, 0, true);
  expect_shut_down_journaler(mock_journaler);

  // replay failure should result in replay-restart
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_max_append_size(mock_journaler, 1 << 16);
  expect_get_journaler_cached_client(mock_journaler, 0);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, true, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler,
    std::bind(&invoke_replay_complete, _1, 0));

  expect_stop_replay(mock_journaler);
  expect_shut_down_replay(mock_image_ctx, mock_journal_replay, 0);
  expect_start_append(mock_journaler);
  ASSERT_EQ(0, when_open(mock_journal));

  expect_stop_append(mock_journaler, 0);
  expect_shut_down_journaler(mock_journaler);
  ASSERT_EQ(0, when_close(mock_journal));
}

TEST_F(TestMockJournal, FlushReplayError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_max_append_size(mock_journaler, 1 << 16);
  expect_get_journaler_cached_client(mock_journaler, 0);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, true, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler,
    std::bind(&invoke_replay_ready, _1));

  ::journal::MockReplayEntry mock_replay_entry;
  MockJournalReplay mock_journal_replay;
  expect_try_pop_front(mock_image_ctx, mock_journaler, true, mock_replay_entry);
  expect_replay_process(mock_journal_replay);
  expect_try_pop_front(mock_image_ctx, mock_journaler, false, mock_replay_entry,
                       std::bind(&invoke_replay_complete, _1, 0));
  expect_stop_replay(mock_journaler);
  expect_shut_down_replay(mock_image_ctx, mock_journal_replay, -EINVAL);
  expect_shut_down_journaler(mock_journaler);

  // replay flush failure should result in replay-restart
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_max_append_size(mock_journaler, 1 << 16);
  expect_get_journaler_cached_client(mock_journaler, 0);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, true, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler,
    std::bind(&invoke_replay_complete, _1, 0));

  expect_stop_replay(mock_journaler);
  expect_shut_down_replay(mock_image_ctx, mock_journal_replay, 0);
  expect_start_append(mock_journaler);
  ASSERT_EQ(0, when_open(mock_journal));

  expect_stop_append(mock_journaler, 0);
  expect_shut_down_journaler(mock_journaler);
  ASSERT_EQ(0, when_close(mock_journal));
}

TEST_F(TestMockJournal, CorruptEntry) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_max_append_size(mock_journaler, 1 << 16);
  expect_get_journaler_cached_client(mock_journaler, 0);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, true, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler,
    std::bind(&invoke_replay_ready, _1));

  ::journal::MockReplayEntry mock_replay_entry;
  MockJournalReplay mock_journal_replay;
  expect_try_pop_front(mock_image_ctx, mock_journaler, true, mock_replay_entry);
  EXPECT_CALL(mock_journal_replay, decode(_, _)).WillOnce(Return(-EBADMSG));
  expect_stop_replay(mock_journaler);
  expect_shut_down_replay(mock_image_ctx, mock_journal_replay, 0, true);
  expect_shut_down_journaler(mock_journaler);

  // replay failure should result in replay-restart
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_max_append_size(mock_journaler, 1 << 16);
  expect_get_journaler_cached_client(mock_journaler, 0);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, true, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler,
    std::bind(&invoke_replay_complete, _1, 0));
  expect_stop_replay(mock_journaler);
  expect_shut_down_replay(mock_image_ctx, mock_journal_replay, 0);
  expect_start_append(mock_journaler);
  ASSERT_EQ(0, when_open(mock_journal));

  expect_stop_append(mock_journaler, -EINVAL);
  expect_shut_down_journaler(mock_journaler);
  ASSERT_EQ(-EINVAL, when_close(mock_journal));
}

TEST_F(TestMockJournal, StopError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_max_append_size(mock_journaler, 1 << 16);
  expect_get_journaler_cached_client(mock_journaler, 0);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, true, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler,
    std::bind(&invoke_replay_complete, _1, 0));

  MockJournalReplay mock_journal_replay;
  expect_stop_replay(mock_journaler);
  expect_shut_down_replay(mock_image_ctx, mock_journal_replay, 0);
  expect_start_append(mock_journaler);
  ASSERT_EQ(0, when_open(mock_journal));

  expect_stop_append(mock_journaler, -EINVAL);
  expect_shut_down_journaler(mock_journaler);
  ASSERT_EQ(-EINVAL, when_close(mock_journal));
}

TEST_F(TestMockJournal, ReplayOnDiskPreFlushError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_max_append_size(mock_journaler, 1 << 16);
  expect_get_journaler_cached_client(mock_journaler, 0);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, true, 0);

  expect_start_replay(
    mock_image_ctx, mock_journaler,
    std::bind(&invoke_replay_ready, _1));

  ::journal::MockReplayEntry mock_replay_entry;
  MockJournalReplay mock_journal_replay;
  expect_try_pop_front(mock_image_ctx, mock_journaler, true, mock_replay_entry);

  EXPECT_CALL(mock_journal_replay, decode(_, _))
                .WillOnce(Return(0));
  Context *on_ready;
  EXPECT_CALL(mock_journal_replay, process(_, _, _))
                .WillOnce(DoAll(SaveArg<1>(&on_ready),
                                WithArg<2>(Invoke(this, &TestMockJournal::save_commit_context))));

  expect_try_pop_front(mock_image_ctx, mock_journaler, false,
                       mock_replay_entry);
  expect_stop_replay(mock_journaler);
  expect_shut_down_replay(mock_image_ctx, mock_journal_replay, 0, true);
  expect_shut_down_journaler(mock_journaler);

  // replay write-to-disk failure should result in replay-restart
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_max_append_size(mock_journaler, 1 << 16);
  expect_get_journaler_cached_client(mock_journaler, 0);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, true, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler, {
      std::bind(&invoke_replay_complete, _1, 0)
    });

  expect_stop_replay(mock_journaler);
  expect_shut_down_replay(mock_image_ctx, mock_journal_replay, 0);
  expect_start_append(mock_journaler);

  C_SaferCond ctx;
  mock_journal.open(&ctx);

  // wait for the process callback
  {
    Mutex::Locker locker(m_lock);
    while (m_commit_contexts.empty()) {
      m_cond.Wait(m_lock);
    }
  }
  on_ready->complete(0);

  // inject RADOS error in the middle of replay
  Context *on_safe = m_commit_contexts.front();
  m_commit_contexts.clear();
  on_safe->complete(-EINVAL);

  // flag the replay as complete
  m_replay_handler->handle_complete(0);

  ASSERT_EQ(0, ctx.wait());

  expect_stop_append(mock_journaler, 0);
  expect_shut_down_journaler(mock_journaler);
  ASSERT_EQ(0, when_close(mock_journal));
}

TEST_F(TestMockJournal, ReplayOnDiskPostFlushError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_max_append_size(mock_journaler, 1 << 16);
  expect_get_journaler_cached_client(mock_journaler, 0);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, true, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler,
    std::bind(&invoke_replay_ready, _1));

  ::journal::MockReplayEntry mock_replay_entry;
  MockJournalReplay mock_journal_replay;
  expect_try_pop_front(mock_image_ctx, mock_journaler, true, mock_replay_entry);
  expect_replay_process(mock_journal_replay);
  expect_try_pop_front(mock_image_ctx, mock_journaler, false, mock_replay_entry,
                       std::bind(&invoke_replay_complete, _1, 0));
  expect_stop_replay(mock_journaler);

  Context *on_flush = nullptr;
  EXPECT_CALL(mock_journal_replay, shut_down(false, _))
    .WillOnce(DoAll(SaveArg<1>(&on_flush),
                    InvokeWithoutArgs(this, &TestMockJournal::wake_up)));

  // replay write-to-disk failure should result in replay-restart
  expect_shut_down_journaler(mock_journaler);
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_get_max_append_size(mock_journaler, 1 << 16);
  expect_get_journaler_cached_client(mock_journaler, 0);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, true, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler,
    std::bind(&invoke_replay_complete, _1, 0));

  expect_stop_replay(mock_journaler);
  expect_shut_down_replay(mock_image_ctx, mock_journal_replay, 0);
  expect_start_append(mock_journaler);

  C_SaferCond ctx;
  mock_journal.open(&ctx);

  // proceed with the flush
  {
    // wait for on_flush callback
    Mutex::Locker locker(m_lock);
    while (on_flush == nullptr) {
      m_cond.Wait(m_lock);
    }
  }

  {
    // wait for the on_safe process callback
    Mutex::Locker locker(m_lock);
    while (m_commit_contexts.empty()) {
      m_cond.Wait(m_lock);
    }
  }
  m_commit_contexts.front()->complete(-EINVAL);
  m_commit_contexts.clear();
  on_flush->complete(0);

  ASSERT_EQ(0, ctx.wait());

  expect_stop_append(mock_journaler, 0);
  expect_shut_down_journaler(mock_journaler);
  ASSERT_EQ(0, when_close(mock_journal));
}

TEST_F(TestMockJournal, EventAndIOCommitOrder) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  ::journal::MockJournaler mock_journaler;
  open_journal(mock_image_ctx, mock_journal, mock_journaler);
  BOOST_SCOPE_EXIT_ALL(&) {
    close_journal(mock_journal, mock_journaler);
  };

  ::journal::MockFuture mock_future;
  Context *on_journal_safe1;
  expect_append_journaler(mock_journaler);
  expect_wait_future(mock_future, &on_journal_safe1);
  ASSERT_EQ(1U, when_append_io_event(mock_image_ctx, mock_journal));
  mock_journal.get_work_queue()->drain();

  Context *on_journal_safe2;
  expect_append_journaler(mock_journaler);
  expect_wait_future(mock_future, &on_journal_safe2);
  ASSERT_EQ(2U, when_append_io_event(mock_image_ctx, mock_journal));
  mock_journal.get_work_queue()->drain();

  // commit journal event followed by IO event (standard)
  on_journal_safe1->complete(0);
  ictx->op_work_queue->drain();
  expect_future_committed(mock_journaler);
  mock_journal.commit_io_event(1U, 0);

  // commit IO event followed by journal event (cache overwrite)
  mock_journal.commit_io_event(2U, 0);
  expect_future_committed(mock_journaler);

  C_SaferCond event_ctx;
  mock_journal.wait_event(2U, &event_ctx);
  on_journal_safe2->complete(0);
  ictx->op_work_queue->drain();
  ASSERT_EQ(0, event_ctx.wait());

  expect_shut_down_journaler(mock_journaler);
}

TEST_F(TestMockJournal, AppendWriteEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  ::journal::MockJournaler mock_journaler;
  open_journal(mock_image_ctx, mock_journal, mock_journaler);
  BOOST_SCOPE_EXIT_ALL(&) {
    close_journal(mock_journal, mock_journaler);
  };

  InSequence seq;

  ::journal::MockFuture mock_future;
  Context *on_journal_safe = nullptr;
  expect_append_journaler(mock_journaler);
  expect_append_journaler(mock_journaler);
  expect_append_journaler(mock_journaler);
  expect_wait_future(mock_future, &on_journal_safe);
  ASSERT_EQ(1U, when_append_write_event(mock_image_ctx, mock_journal, 1 << 17));
  mock_journal.get_work_queue()->drain();

  on_journal_safe->complete(0);
  C_SaferCond event_ctx;
  mock_journal.wait_event(1U, &event_ctx);
  ASSERT_EQ(0, event_ctx.wait());

  expect_future_committed(mock_journaler);
  expect_future_committed(mock_journaler);
  expect_future_committed(mock_journaler);
  mock_journal.commit_io_event(1U, 0);
  ictx->op_work_queue->drain();

  expect_shut_down_journaler(mock_journaler);
}

TEST_F(TestMockJournal, EventCommitError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  ::journal::MockJournaler mock_journaler;
  open_journal(mock_image_ctx, mock_journal, mock_journaler);
  BOOST_SCOPE_EXIT_ALL(&) {
    close_journal(mock_journal, mock_journaler);
  };

  C_SaferCond object_request_ctx;
  AioObjectRemove *object_request = new AioObjectRemove(
    ictx, "oid", 0, {}, &object_request_ctx);

  ::journal::MockFuture mock_future;
  Context *on_journal_safe;
  expect_append_journaler(mock_journaler);
  expect_wait_future(mock_future, &on_journal_safe);
  ASSERT_EQ(1U, when_append_io_event(mock_image_ctx, mock_journal,
                                     object_request));
  mock_journal.get_work_queue()->drain();

  // commit the event in the journal w/o waiting writeback
  expect_future_committed(mock_journaler);
  on_journal_safe->complete(-EINVAL);
  ASSERT_EQ(-EINVAL, object_request_ctx.wait());

  // cache should receive the error after attempting writeback
  expect_future_is_valid(mock_future);
  C_SaferCond flush_ctx;
  mock_journal.flush_event(1U, &flush_ctx);
  ASSERT_EQ(-EINVAL, flush_ctx.wait());

  expect_shut_down_journaler(mock_journaler);
}

TEST_F(TestMockJournal, EventCommitErrorWithPendingWriteback) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  ::journal::MockJournaler mock_journaler;
  open_journal(mock_image_ctx, mock_journal, mock_journaler);
  BOOST_SCOPE_EXIT_ALL(&) {
    close_journal(mock_journal, mock_journaler);
  };

  C_SaferCond object_request_ctx;
  AioObjectRemove *object_request = new AioObjectRemove(
    ictx, "oid", 0, {}, &object_request_ctx);

  ::journal::MockFuture mock_future;
  Context *on_journal_safe;
  expect_append_journaler(mock_journaler);
  expect_wait_future(mock_future, &on_journal_safe);
  ASSERT_EQ(1U, when_append_io_event(mock_image_ctx, mock_journal,
                                     object_request));
  mock_journal.get_work_queue()->drain();

  expect_future_is_valid(mock_future);
  C_SaferCond flush_ctx;
  mock_journal.flush_event(1U, &flush_ctx);

  // commit the event in the journal w/ waiting cache writeback
  expect_future_committed(mock_journaler);
  on_journal_safe->complete(-EINVAL);
  ASSERT_EQ(-EINVAL, object_request_ctx.wait());

  // cache should receive the error if waiting
  ASSERT_EQ(-EINVAL, flush_ctx.wait());

  expect_shut_down_journaler(mock_journaler);
}

TEST_F(TestMockJournal, IOCommitError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  ::journal::MockJournaler mock_journaler;
  open_journal(mock_image_ctx, mock_journal, mock_journaler);
  BOOST_SCOPE_EXIT_ALL(&) {
    close_journal(mock_journal, mock_journaler);
  };

  ::journal::MockFuture mock_future;
  Context *on_journal_safe;
  expect_append_journaler(mock_journaler);
  expect_wait_future(mock_future, &on_journal_safe);
  ASSERT_EQ(1U, when_append_io_event(mock_image_ctx, mock_journal));
  mock_journal.get_work_queue()->drain();

  // failed IO remains uncommitted in journal
  on_journal_safe->complete(0);
  ictx->op_work_queue->drain();
  mock_journal.commit_io_event(1U, -EINVAL);

  expect_shut_down_journaler(mock_journaler);
}

TEST_F(TestMockJournal, FlushCommitPosition) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  ::journal::MockJournaler mock_journaler;
  open_journal(mock_image_ctx, mock_journal, mock_journaler);
  BOOST_SCOPE_EXIT_ALL(&) {
    close_journal(mock_journal, mock_journaler);
  };

  expect_flush_commit_position(mock_journaler);
  C_SaferCond ctx;
  mock_journal.flush_commit_position(&ctx);
  ASSERT_EQ(0, ctx.wait());

  expect_shut_down_journaler(mock_journaler);
}

TEST_F(TestMockJournal, ExternalReplay) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  ::journal::MockJournaler mock_journaler;
  open_journal(mock_image_ctx, mock_journal, mock_journaler);
  BOOST_SCOPE_EXIT_ALL(&) {
    close_journal(mock_journal, mock_journaler);
  };

  InSequence seq;
  expect_stop_append(mock_journaler, 0);
  expect_start_append(mock_journaler);
  expect_shut_down_journaler(mock_journaler);

  C_SaferCond start_ctx;

  journal::Replay<MockJournalImageCtx> *journal_replay = nullptr;
  mock_journal.start_external_replay(&journal_replay, &start_ctx);
  ASSERT_EQ(0, start_ctx.wait());

  mock_journal.stop_external_replay();
}

TEST_F(TestMockJournal, ExternalReplayFailure) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  ::journal::MockJournaler mock_journaler;
  open_journal(mock_image_ctx, mock_journal, mock_journaler);
  BOOST_SCOPE_EXIT_ALL(&) {
    close_journal(mock_journal, mock_journaler);
  };

  InSequence seq;
  expect_stop_append(mock_journaler, -EINVAL);
  expect_start_append(mock_journaler);
  expect_shut_down_journaler(mock_journaler);

  C_SaferCond start_ctx;

  journal::Replay<MockJournalImageCtx> *journal_replay = nullptr;
  mock_journal.start_external_replay(&journal_replay, &start_ctx);
  ASSERT_EQ(-EINVAL, start_ctx.wait());
}

TEST_F(TestMockJournal, AppendDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  MockJournalPolicy mock_journal_policy;

  ::journal::MockJournaler mock_journaler;
  open_journal(mock_image_ctx, mock_journal, mock_journaler);
  BOOST_SCOPE_EXIT_ALL(&) {
    close_journal(mock_journal, mock_journaler);
  };

  InSequence seq;
  RWLock::RLocker snap_locker(mock_image_ctx.snap_lock);
  EXPECT_CALL(mock_image_ctx, get_journal_policy()).WillOnce(
    Return(ictx->get_journal_policy()));
  ASSERT_TRUE(mock_journal.is_journal_appending());

  EXPECT_CALL(mock_image_ctx, get_journal_policy()).WillOnce(
    Return(&mock_journal_policy));
  EXPECT_CALL(mock_journal_policy, append_disabled()).WillOnce(Return(true));
  ASSERT_FALSE(mock_journal.is_journal_appending());

  expect_shut_down_journaler(mock_journaler);
}

TEST_F(TestMockJournal, CloseListenerEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  ::journal::MockJournaler mock_journaler;
  open_journal(mock_image_ctx, mock_journal, mock_journaler);

  struct Listener : public journal::Listener {
    C_SaferCond ctx;
    virtual void handle_close() {
      ctx.complete(0);
    }
    virtual void handle_resync() {
      ADD_FAILURE() << "unexpected resync request";
    }
    virtual void handle_promoted() {
      ADD_FAILURE() << "unexpected promotion event";
    }
  } listener;
  mock_journal.add_listener(&listener);

  expect_shut_down_journaler(mock_journaler);
  close_journal(mock_journal, mock_journaler);

  ASSERT_EQ(0, listener.ctx.wait());
  mock_journal.remove_listener(&listener);
}

TEST_F(TestMockJournal, ResyncRequested) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  ::journal::MockJournaler mock_journaler;
  open_journal(mock_image_ctx, mock_journal, mock_journaler, false);

  struct Listener : public journal::Listener {
    C_SaferCond ctx;
    virtual void handle_close() {
      ADD_FAILURE() << "unexpected close action";
    }
    virtual void handle_resync() {
      ctx.complete(0);
    }
    virtual void handle_promoted() {
      ADD_FAILURE() << "unexpected promotion event";
    }
  } listener;
  mock_journal.add_listener(&listener);

  BOOST_SCOPE_EXIT_ALL(&) {
    mock_journal.remove_listener(&listener);
    close_journal(mock_journal, mock_journaler);
  };

  InSequence seq;

  journal::TagData tag_data;
  tag_data.mirror_uuid == Journal<>::LOCAL_MIRROR_UUID;

  bufferlist tag_data_bl;
  ::encode(tag_data, tag_data_bl);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, 0,
                            {{0, 0, tag_data_bl}}, 0);

  journal::ImageClientMeta image_client_meta;
  image_client_meta.tag_class = 0;
  image_client_meta.resync_requested = true;
  expect_get_journaler_cached_client(mock_journaler, image_client_meta, 0);
  expect_shut_down_journaler(mock_journaler);

  m_listener->handle_update(nullptr);
  ASSERT_EQ(0, listener.ctx.wait());
}

TEST_F(TestMockJournal, ForcePromoted) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockJournalImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  ::journal::MockJournaler mock_journaler;
  open_journal(mock_image_ctx, mock_journal, mock_journaler, false);

  struct Listener : public journal::Listener {
    C_SaferCond ctx;
    virtual void handle_close() {
      ADD_FAILURE() << "unexpected close action";
    }
    virtual void handle_resync() {
      ADD_FAILURE() << "unexpected resync event";
    }
    virtual void handle_promoted() {
      ctx.complete(0);
    }
  } listener;
  mock_journal.add_listener(&listener);

  BOOST_SCOPE_EXIT_ALL(&) {
    mock_journal.remove_listener(&listener);
    close_journal(mock_journal, mock_journaler);
  };

  InSequence seq;

  journal::TagData tag_data;
  tag_data.mirror_uuid == Journal<>::LOCAL_MIRROR_UUID;

  bufferlist tag_data_bl;
  ::encode(tag_data, tag_data_bl);
  expect_get_journaler_tags(mock_image_ctx, mock_journaler, 0,
                            {{100, 0, tag_data_bl}}, 0);

  journal::ImageClientMeta image_client_meta;
  image_client_meta.tag_class = 0;
  expect_get_journaler_cached_client(mock_journaler, image_client_meta, 0);
  expect_shut_down_journaler(mock_journaler);

  m_listener->handle_update(nullptr);
  ASSERT_EQ(0, listener.ctx.wait());
}

} // namespace librbd
