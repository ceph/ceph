// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/journal/Replay.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <functional>
#include <list>

namespace journal {

struct MockFuture {
  static MockFuture *s_instance;
  static MockFuture &get_instance() {
    assert(s_instance != nullptr);
    return *s_instance;
  }

  MockFuture() {
    s_instance = this;
  }

  MOCK_CONST_METHOD0(is_valid, bool());
  MOCK_METHOD1(flush, void(Context *));
  MOCK_METHOD1(wait, void(Context *));
};

struct MockFutureProxy {
  bool is_valid() const {
    return MockFuture::get_instance().is_valid();
  }

  void flush(Context *on_safe) {
    MockFuture::get_instance().flush(on_safe);
  }

  void wait(Context *on_safe) {
    MockFuture::get_instance().wait(on_safe);
  }
};

struct MockReplayEntry {
  static MockReplayEntry *s_instance;
  static MockReplayEntry &get_instance() {
    assert(s_instance != nullptr);
    return *s_instance;
  }

  MockReplayEntry() {
    s_instance = this;
  }

  MOCK_METHOD0(get_data, bufferlist());
};

struct MockReplayEntryProxy {
  bufferlist get_data() {
    return MockReplayEntry::get_instance().get_data();
  }
};

struct MockJournaler {
  static MockJournaler *s_instance;
  static MockJournaler &get_instance() {
    assert(s_instance != nullptr);
    return *s_instance;
  }

  MockJournaler() {
    s_instance = this;
  }

  MOCK_METHOD0(construct, void());

  MOCK_METHOD3(get_metadata, void(uint8_t *order, uint8_t *splay_width,
                                  int64_t *pool_id));
  MOCK_METHOD1(init, void(Context*));

  MOCK_METHOD1(start_replay, void(::journal::ReplayHandler *replay_handler));
  MOCK_METHOD1(try_pop_front, bool(MockReplayEntryProxy *replay_entry));
  MOCK_METHOD0(stop_replay, void());

  MOCK_METHOD3(start_append, void(int flush_interval, uint64_t flush_bytes,
                                  double flush_age));
  MOCK_METHOD2(append, MockFutureProxy(const std::string &tag,
                                       const bufferlist &bl));
  MOCK_METHOD1(flush, void(Context *on_safe));
  MOCK_METHOD1(stop_append, void(Context *on_safe));

  MOCK_METHOD1(committed, void(const MockReplayEntryProxy &replay_entry));
  MOCK_METHOD1(committed, void(const MockFutureProxy &future));
};

struct MockJournalerProxy {
  template <typename IoCtxT>
  MockJournalerProxy(IoCtxT &header_ioctx, const std::string &,
                     const std::string &, double) {
    MockJournaler::get_instance().construct();
  }

  int exists(bool *header_exists) const {
    return -EINVAL;
  }
  int create(uint8_t order, uint8_t splay_width, int64_t pool_id) {
    return -EINVAL;
  }
  int remove(bool force) {
    return -EINVAL;
  }
  int register_client(const std::string &description) {
    return -EINVAL;
  }

  void get_metadata(uint8_t *order, uint8_t *splay_width, int64_t *pool_id) {
    MockJournaler::get_instance().get_metadata(order, splay_width, pool_id);
  }

  void init(Context *on_finish) {
    MockJournaler::get_instance().init(on_finish);
  }

  void start_replay(::journal::ReplayHandler *replay_handler) {
    MockJournaler::get_instance().start_replay(replay_handler);
  }

  bool try_pop_front(MockReplayEntryProxy *replay_entry) {
    return MockJournaler::get_instance().try_pop_front(replay_entry);
  }

  void stop_replay() {
    MockJournaler::get_instance().stop_replay();
  }

  void start_append(int flush_interval, uint64_t flush_bytes, double flush_age) {
    MockJournaler::get_instance().start_append(flush_interval, flush_bytes,
                                               flush_age);
  }

  MockFutureProxy append(const std::string &tag, const bufferlist &bl) {
    return MockJournaler::get_instance().append(tag, bl);
  }

  void flush(Context *on_safe) {
    MockJournaler::get_instance().flush(on_safe);
  }

  void stop_append(Context *on_safe) {
    MockJournaler::get_instance().stop_append(on_safe);
  }

  void committed(const MockReplayEntryProxy &replay_entry) {
    MockJournaler::get_instance().committed(replay_entry);
  }

  void committed(const MockFutureProxy &future) {
    MockJournaler::get_instance().committed(future);
  }
};

MockFuture *MockFuture::s_instance = nullptr;
MockReplayEntry *MockReplayEntry::s_instance = nullptr;
MockJournaler *MockJournaler::s_instance = nullptr;

} // namespace journal

namespace librbd {
namespace journal {

template <>
struct TypeTraits<MockImageCtx> {
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

  MOCK_METHOD1(flush, void(Context *));
  MOCK_METHOD2(process, int(bufferlist::iterator, Context *));
};

template <>
class Replay<MockImageCtx> {
public:
  static Replay *create(MockImageCtx &image_ctx) {
    return new Replay();
  }

  void flush(Context *on_finish) {
    MockReplay::get_instance().flush(on_finish);
  }

  int process(bufferlist::iterator it, Context *on_commit) {
    return MockReplay::get_instance().process(it, on_commit);
  }
};

MockReplay *MockReplay::s_instance = nullptr;

} // namespace journal
} // namespace librbd

// template definitions
#include "librbd/Journal.cc"
template class librbd::Journal<librbd::MockImageCtx>;

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::MatcherCast;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::WithArg;
using namespace std::placeholders;

ACTION_P2(StartReplay, wq, ctx) {
  ctx->replay_handler = arg0;
  wq->queue(ctx, 0);
}

namespace librbd {

class TestMockJournal : public TestMockFixture {
public:
  typedef journal::MockReplay MockJournalReplay;
  typedef Journal<MockImageCtx> MockJournal;

  typedef std::function<void(::journal::ReplayHandler*)> ReplayAction;
  typedef std::list<ReplayAction> ReplayActions;
  typedef std::list<Context *> Contexts;

  Contexts m_commit_contexts;
  struct C_StartReplay : public Context {
    ReplayActions replay_actions;
    ::journal::ReplayHandler *replay_handler;

    C_StartReplay(const ReplayActions &replay_actions)
      : replay_actions(replay_actions), replay_handler(nullptr) {
    }
    virtual void finish(int r) {
      for (auto &action : replay_actions) {
        action(replay_handler);
      }
    }
  };

  ~TestMockJournal() {
    assert(m_commit_contexts.empty());
  }

  void expect_construct_journaler(::journal::MockJournaler &mock_journaler) {
    EXPECT_CALL(mock_journaler, construct());
  }

  void expect_init_journaler(::journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, init(_))
                  .WillOnce(CompleteContext(r, NULL));

  }

  void expect_start_replay(MockImageCtx &mock_image_ctx,
                           ::journal::MockJournaler &mock_journaler,
                           const ReplayActions &actions) {
    EXPECT_CALL(mock_journaler, start_replay(_))
                 .WillOnce(StartReplay(mock_image_ctx.image_ctx->op_work_queue,
                                       new C_StartReplay(actions)));
  }

  void expect_stop_replay(::journal::MockJournaler &mock_journaler) {
    EXPECT_CALL(mock_journaler, stop_replay());
  }

  void expect_flush_replay(MockJournalReplay &mock_journal_replay, int r) {
    EXPECT_CALL(mock_journal_replay, flush(_))
                  .WillOnce(CompleteContext(r, NULL));
  }

  void expect_get_data(::journal::MockReplayEntry &mock_replay_entry) {
    EXPECT_CALL(mock_replay_entry, get_data())
                  .WillOnce(Return(bufferlist()));
  }

  void expect_try_pop_front(::journal::MockJournaler &mock_journaler,
                            bool entries_available,
                            ::journal::MockReplayEntry &mock_replay_entry) {
    EXPECT_CALL(mock_journaler, try_pop_front(_))
                  .WillOnce(DoAll(SetArgPointee<0>(::journal::MockReplayEntryProxy()),
                                  Return(entries_available)));
    if (entries_available) {
      expect_get_data(mock_replay_entry);
    }
  }

  void expect_replay_process(MockJournalReplay &mock_journal_replay, int r) {
    auto &expect = EXPECT_CALL(mock_journal_replay, process(_, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoAll(WithArg<1>(Invoke(this, &TestMockJournal::save_commit_context)),
                            Return(0)));
    }
  }

  void expect_start_append(::journal::MockJournaler &mock_journaler) {
    EXPECT_CALL(mock_journaler, start_append(_, _, _));
  }

  void expect_stop_append(::journal::MockJournaler &mock_journaler, int r) {
    EXPECT_CALL(mock_journaler, stop_append(_))
                  .WillOnce(CompleteContext(r, NULL));
  }

  void expect_committed(::journal::MockJournaler &mock_journaler) {
    EXPECT_CALL(mock_journaler, committed(MatcherCast<const ::journal::MockReplayEntryProxy&>(_)))
                  .Times(m_commit_contexts.size());
  }

  int when_open(MockJournal &mock_journal) {
    C_SaferCond ctx;
    mock_journal.open(&ctx);
    return ctx.wait();
  }

  void when_commit_replay(int r) {
    for (auto ctx : m_commit_contexts) {
      ctx->complete(r);
    }
    m_commit_contexts.clear();
  }

  int when_close(MockJournal &mock_journal) {
    C_SaferCond ctx;
    mock_journal.close(&ctx);
    return ctx.wait();
  }

  void save_commit_context(Context *ctx) {
    m_commit_contexts.push_back(ctx);
  }

  static void invoke_replay_ready(::journal::ReplayHandler *handler) {
    handler->handle_entries_available();
  }

  static void invoke_replay_complete(::journal::ReplayHandler *handler, int r) {
    handler->handle_complete(r);
  }
};

TEST_F(TestMockJournal, StateTransitions) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler, {
      std::bind(&invoke_replay_ready, _1),
      std::bind(&invoke_replay_ready, _1),
      std::bind(&invoke_replay_complete, _1, 0)
    });

  ::journal::MockReplayEntry mock_replay_entry;
  MockJournalReplay mock_journal_replay;
  expect_try_pop_front(mock_journaler, true, mock_replay_entry);
  expect_replay_process(mock_journal_replay, 0);
  expect_try_pop_front(mock_journaler, true, mock_replay_entry);
  expect_replay_process(mock_journal_replay, 0);
  expect_try_pop_front(mock_journaler, false, mock_replay_entry);
  expect_try_pop_front(mock_journaler, true, mock_replay_entry);
  expect_replay_process(mock_journal_replay, 0);
  expect_try_pop_front(mock_journaler, false, mock_replay_entry);

  expect_stop_replay(mock_journaler);
  expect_flush_replay(mock_journal_replay, 0);

  expect_start_append(mock_journaler);

  ASSERT_EQ(0, when_open(mock_journal));

  expect_committed(mock_journaler);
  when_commit_replay(0);

  expect_stop_append(mock_journaler, 0);
  ASSERT_EQ(0, when_close(mock_journal));
}

TEST_F(TestMockJournal, InitError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, -EINVAL);
  ASSERT_EQ(-EINVAL, when_open(mock_journal));
}

TEST_F(TestMockJournal, ReplayProcessError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler, {
      std::bind(&invoke_replay_ready, _1)
    });

  ::journal::MockReplayEntry mock_replay_entry;
  MockJournalReplay mock_journal_replay;
  expect_try_pop_front(mock_journaler, true, mock_replay_entry);
  expect_replay_process(mock_journal_replay, -EINVAL);

  expect_stop_replay(mock_journaler);
  expect_flush_replay(mock_journal_replay, 0);

  // replay failure should result in replay-restart
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler, {
      std::bind(&invoke_replay_complete, _1, 0)
    });

  expect_stop_replay(mock_journaler);
  expect_flush_replay(mock_journal_replay, 0);
  expect_start_append(mock_journaler);
  ASSERT_EQ(0, when_open(mock_journal));

  expect_stop_append(mock_journaler, 0);
  ASSERT_EQ(0, when_close(mock_journal));
}

TEST_F(TestMockJournal, ReplayCompleteError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler, {
      std::bind(&invoke_replay_complete, _1, -EINVAL)
    });

  MockJournalReplay mock_journal_replay;
  expect_stop_replay(mock_journaler);
  expect_flush_replay(mock_journal_replay, 0);

  // replay failure should result in replay-restart
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler, {
      std::bind(&invoke_replay_complete, _1, 0)
    });

  expect_stop_replay(mock_journaler);
  expect_flush_replay(mock_journal_replay, 0);
  expect_start_append(mock_journaler);
  ASSERT_EQ(0, when_open(mock_journal));

  expect_stop_append(mock_journaler, 0);
  ASSERT_EQ(0, when_close(mock_journal));
}

TEST_F(TestMockJournal, FlushReplayError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler, {
      std::bind(&invoke_replay_complete, _1, 0)
    });

  MockJournalReplay mock_journal_replay;
  expect_stop_replay(mock_journaler);
  expect_flush_replay(mock_journal_replay, -EINVAL);

  // replay flush failure should result in replay-restart
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler, {
      std::bind(&invoke_replay_complete, _1, 0)
    });

  expect_stop_replay(mock_journaler);
  expect_flush_replay(mock_journal_replay, 0);
  expect_start_append(mock_journaler);
  ASSERT_EQ(0, when_open(mock_journal));

  expect_stop_append(mock_journaler, 0);
  ASSERT_EQ(0, when_close(mock_journal));
}

TEST_F(TestMockJournal, StopError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ::journal::MockJournaler mock_journaler;
  expect_construct_journaler(mock_journaler);
  expect_init_journaler(mock_journaler, 0);
  expect_start_replay(
    mock_image_ctx, mock_journaler, {
      std::bind(&invoke_replay_complete, _1, 0)
    });

  MockJournalReplay mock_journal_replay;
  expect_stop_replay(mock_journaler);
  expect_flush_replay(mock_journal_replay, 0);
  expect_start_append(mock_journaler);
  ASSERT_EQ(0, when_open(mock_journal));

  expect_stop_append(mock_journaler, -EINVAL);
  ASSERT_EQ(-EINVAL, when_close(mock_journal));
}




} // namespace librbd
