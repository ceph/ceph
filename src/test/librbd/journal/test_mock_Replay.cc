// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "librbd/AioImageRequest.h"
#include "librbd/journal/Entries.h"
#include "librbd/journal/Replay.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <boost/scope_exit.hpp>

namespace librbd {

template <>
struct AioImageRequest<MockImageCtx> {
  static AioImageRequest *s_instance;

  MOCK_METHOD5(aio_write, void(AioCompletion *c, uint64_t off, size_t len,
                               const char *buf, int op_flags));
  static void aio_write(MockImageCtx *ictx, AioCompletion *c, uint64_t off,
                        size_t len, const char *buf, int op_flags) {
    assert(s_instance != nullptr);
    s_instance->aio_write(c, off, len, buf, op_flags);
  }

  MOCK_METHOD3(aio_discard, void(AioCompletion *c, uint64_t off, uint64_t len));
  static void aio_discard(MockImageCtx *ictx, AioCompletion *c, uint64_t off,
                          uint64_t len) {
    assert(s_instance != nullptr);
    s_instance->aio_discard(c, off, len);
  }

  MOCK_METHOD1(aio_flush, void(AioCompletion *c));
  static void aio_flush(MockImageCtx *ictx, AioCompletion *c) {
    assert(s_instance != nullptr);
    s_instance->aio_flush(c);
  }

  AioImageRequest() {
    s_instance = this;
  }
};

AioImageRequest<MockImageCtx> *AioImageRequest<MockImageCtx>::s_instance = nullptr;

}

// template definitions
#include "librbd/journal/Replay.cc"
template class librbd::journal::Replay<librbd::MockImageCtx>;

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::WithArgs;

MATCHER_P(CStrEq, str, "") {
  return (strncmp(arg, str, strlen(str)) == 0);
}

ACTION_P2(CompleteAioCompletion, r, image_ctx) {
  CephContext *cct = image_ctx->cct;
  image_ctx->op_work_queue->queue(new FunctionContext([cct, arg0](int r) {
      arg0->get();
      arg0->set_request_count(cct, 1);
      arg0->complete_request(cct, r);
    }), r);
}

namespace librbd {
namespace journal {

class TestMockJournalReplay : public TestMockFixture {
public:
  typedef AioImageRequest<MockImageCtx> MockAioImageRequest;
  typedef Replay<MockImageCtx> MockJournalReplay;

  void expect_aio_discard(MockAioImageRequest &mock_aio_image_request,
                          AioCompletion **aio_comp, uint64_t off,
                          uint64_t len) {
    EXPECT_CALL(mock_aio_image_request, aio_discard(_, off, len))
                  .WillOnce(SaveArg<0>(aio_comp));
  }

  void expect_aio_flush(MockAioImageRequest &mock_aio_image_request,
                        AioCompletion **aio_comp) {
    EXPECT_CALL(mock_aio_image_request, aio_flush(_))
                  .WillOnce(SaveArg<0>(aio_comp));
  }

  void expect_aio_flush(MockImageCtx &mock_image_ctx,
                        MockAioImageRequest &mock_aio_image_request, int r) {
    EXPECT_CALL(mock_aio_image_request, aio_flush(_))
                  .WillOnce(CompleteAioCompletion(r, mock_image_ctx.image_ctx));
  }

  void expect_aio_write(MockAioImageRequest &mock_aio_image_request,
                        AioCompletion **aio_comp, uint64_t off,
                        uint64_t len, const char *data) {
    EXPECT_CALL(mock_aio_image_request,
                aio_write(_, off, len, CStrEq(data), _))
                  .WillOnce(SaveArg<0>(aio_comp));
  }

  void expect_flatten(MockImageCtx &mock_image_ctx, Context **on_finish) {
    EXPECT_CALL(*mock_image_ctx.operations, flatten(_, _))
                  .WillOnce(SaveArg<1>(on_finish));
  }

  void expect_rename(MockImageCtx &mock_image_ctx, Context **on_finish,
                     const char *image_name) {
    EXPECT_CALL(*mock_image_ctx.operations, rename(CStrEq(image_name), _))
                  .WillOnce(SaveArg<1>(on_finish));
  }

  void expect_resize(MockImageCtx &mock_image_ctx, Context **on_finish,
                     uint64_t size, uint64_t op_tid) {
    EXPECT_CALL(*mock_image_ctx.operations, resize(size, _, _, op_tid))
                  .WillOnce(SaveArg<2>(on_finish));
  }

  void expect_snap_create(MockImageCtx &mock_image_ctx,
                          Context **on_finish, const char *snap_name,
                          uint64_t op_tid) {
    EXPECT_CALL(*mock_image_ctx.operations, snap_create(CStrEq(snap_name), _,
                                                        op_tid))
                  .WillOnce(SaveArg<1>(on_finish));
  }

  void expect_snap_remove(MockImageCtx &mock_image_ctx,
                          Context **on_finish, const char *snap_name) {
    EXPECT_CALL(*mock_image_ctx.operations, snap_remove(CStrEq(snap_name), _))
                  .WillOnce(SaveArg<1>(on_finish));
  }

  void expect_snap_rename(MockImageCtx &mock_image_ctx,
                          Context **on_finish, uint64_t snap_id,
                          const char *snap_name) {
    EXPECT_CALL(*mock_image_ctx.operations, snap_rename(snap_id, CStrEq(snap_name), _))
                  .WillOnce(SaveArg<2>(on_finish));
  }

  void expect_snap_protect(MockImageCtx &mock_image_ctx,
                           Context **on_finish, const char *snap_name) {
    EXPECT_CALL(*mock_image_ctx.operations, snap_protect(CStrEq(snap_name), _))
                  .WillOnce(SaveArg<1>(on_finish));
  }

  void expect_snap_unprotect(MockImageCtx &mock_image_ctx,
                             Context **on_finish, const char *snap_name) {
    EXPECT_CALL(*mock_image_ctx.operations, snap_unprotect(CStrEq(snap_name), _))
                  .WillOnce(SaveArg<1>(on_finish));
  }

  void expect_snap_rollback(MockImageCtx &mock_image_ctx,
                            Context **on_finish, const char *snap_name) {
    EXPECT_CALL(*mock_image_ctx.operations, snap_rollback(CStrEq(snap_name), _, _))
                  .WillOnce(SaveArg<2>(on_finish));
  }

  void when_process(MockJournalReplay &mock_journal_replay,
                    EventEntry &&event_entry, Context *on_ready,
                    Context *on_safe) {
    bufferlist bl;
    ::encode(event_entry, bl);

    bufferlist::iterator it = bl.begin();
    when_process(mock_journal_replay, &it, on_ready, on_safe);
  }

  void when_process(MockJournalReplay &mock_journal_replay,
                    bufferlist::iterator *it, Context *on_ready,
                    Context *on_safe) {
    mock_journal_replay.process(it, on_ready, on_safe);
  }

  void when_complete(MockImageCtx &mock_image_ctx, AioCompletion *aio_comp,
                     int r) {
    aio_comp->get();
    aio_comp->set_request_count(mock_image_ctx.cct, 1);
    aio_comp->complete_request(mock_image_ctx.cct, r);
  }

  int when_flush(MockJournalReplay &mock_journal_replay) {
    C_SaferCond ctx;
    mock_journal_replay.flush(&ctx);
    return ctx.wait();
  }

  void when_replay_op_ready(MockJournalReplay &mock_journal_replay,
                            uint64_t op_tid, Context *on_resume) {
    mock_journal_replay.replay_op_ready(op_tid, on_resume);
  }

  bufferlist to_bl(const std::string &str) {
    bufferlist bl;
    bl.append(str);
    return bl;
  }
};

TEST_F(TestMockJournalReplay, AioDiscard) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockAioImageRequest mock_aio_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  AioCompletion *aio_comp;
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  expect_aio_discard(mock_aio_image_request, &aio_comp, 123, 456);
  when_process(mock_journal_replay,
               EventEntry{AioDiscardEvent(123, 456)},
               &on_ready, &on_safe);

  when_complete(mock_image_ctx, aio_comp, 0);
  ASSERT_EQ(0, on_ready.wait());

  expect_aio_flush(mock_image_ctx, mock_aio_image_request, 0);
  ASSERT_EQ(0, when_flush(mock_journal_replay));
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, AioWrite) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockAioImageRequest mock_aio_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  AioCompletion *aio_comp;
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  expect_aio_write(mock_aio_image_request, &aio_comp, 123, 456, "test");
  when_process(mock_journal_replay,
               EventEntry{AioWriteEvent(123, 456, to_bl("test"))},
               &on_ready, &on_safe);

  when_complete(mock_image_ctx, aio_comp, 0);
  ASSERT_EQ(0, on_ready.wait());

  expect_aio_flush(mock_image_ctx, mock_aio_image_request, 0);
  ASSERT_EQ(0, when_flush(mock_journal_replay));
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, AioFlush) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockAioImageRequest mock_aio_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  AioCompletion *aio_comp;
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  expect_aio_flush(mock_aio_image_request, &aio_comp);
  when_process(mock_journal_replay, EventEntry{AioFlushEvent()},
               &on_ready, &on_safe);

  when_complete(mock_image_ctx, aio_comp, 0);
  ASSERT_EQ(0, on_safe.wait());

  ASSERT_EQ(0, when_flush(mock_journal_replay));
  ASSERT_EQ(0, on_ready.wait());
}

TEST_F(TestMockJournalReplay, IOError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockAioImageRequest mock_aio_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  AioCompletion *aio_comp;
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  expect_aio_discard(mock_aio_image_request, &aio_comp, 123, 456);
  when_process(mock_journal_replay,
               EventEntry{AioDiscardEvent(123, 456)},
               &on_ready, &on_safe);

  when_complete(mock_image_ctx, aio_comp, -EINVAL);
  ASSERT_EQ(-EINVAL, on_safe.wait());

  expect_aio_flush(mock_image_ctx, mock_aio_image_request, 0);
  ASSERT_EQ(0, when_flush(mock_journal_replay));
  ASSERT_EQ(0, on_ready.wait());
}

TEST_F(TestMockJournalReplay, SoftFlushIO) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockAioImageRequest mock_aio_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  const size_t io_count = 32;
  C_SaferCond on_safes[io_count];
  for (size_t i = 0; i < io_count; ++i) {
    AioCompletion *aio_comp;
    AioCompletion *flush_comp = nullptr;
    C_SaferCond on_ready;
    expect_aio_discard(mock_aio_image_request, &aio_comp, 123, 456);
    if (i == io_count - 1) {
      expect_aio_flush(mock_aio_image_request, &flush_comp);
    }
    when_process(mock_journal_replay,
                 EventEntry{AioDiscardEvent(123, 456)},
                 &on_ready, &on_safes[i]);
    ASSERT_EQ(0, on_ready.wait());

    when_complete(mock_image_ctx, aio_comp, 0);
    if (flush_comp != nullptr) {
      when_complete(mock_image_ctx, flush_comp, 0);
    }
  }
  for (auto &on_safe : on_safes) {
    ASSERT_EQ(0, on_safe.wait());
  }

  ASSERT_EQ(0, when_flush(mock_journal_replay));
}

TEST_F(TestMockJournalReplay, PauseIO) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockAioImageRequest mock_aio_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  const size_t io_count = 64;
  std::list<AioCompletion *> flush_comps;
  C_SaferCond on_safes[io_count];
  for (size_t i = 0; i < io_count; ++i) {
    AioCompletion *aio_comp;
    C_SaferCond on_ready;
    expect_aio_write(mock_aio_image_request, &aio_comp, 123, 456, "test");
    if ((i + 1) % 32 == 0) {
      flush_comps.push_back(nullptr);
      expect_aio_flush(mock_aio_image_request, &flush_comps.back());
    }
    when_process(mock_journal_replay,
                 EventEntry{AioWriteEvent(123, 456, to_bl("test"))},
                 &on_ready, &on_safes[i]);
    if (i < io_count - 1) {
      ASSERT_EQ(0, on_ready.wait());
    }

    when_complete(mock_image_ctx, aio_comp, 0);
    if (i == io_count - 1) {
      for (auto flush_comp : flush_comps) {
        when_complete(mock_image_ctx, flush_comp, 0);
        ASSERT_EQ(0, on_ready.wait());
      }
    }
  }
  for (auto &on_safe : on_safes) {
    ASSERT_EQ(0, on_safe.wait());
  }

  ASSERT_EQ(0, when_flush(mock_journal_replay));
}

TEST_F(TestMockJournalReplay, MissingOpFinishEvent) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  when_process(mock_journal_replay, EventEntry{SnapRemoveEvent(123, "snap")},
               &on_ready, &on_safe);

  ASSERT_EQ(0, on_ready.wait());

  ASSERT_EQ(0, when_flush(mock_journal_replay));
  ASSERT_EQ(-ERESTART, on_safe.wait());
}

TEST_F(TestMockJournalReplay, MissingCompleteOpFinishEvent) {
  // TODO
}

TEST_F(TestMockJournalReplay, UnknownOpFinishEvent) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_ready, &on_safe);

  ASSERT_EQ(0, on_safe.wait());
  ASSERT_EQ(0, on_ready.wait());
}

TEST_F(TestMockJournalReplay, OpEventError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_snap_remove(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay, EventEntry{SnapRemoveEvent(123, "snap")},
               &on_start_ready, &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  on_finish->complete(-EINVAL);
  ASSERT_EQ(-EINVAL, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(-EINVAL, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapCreateEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_snap_create(mock_image_ctx, &on_finish, "snap", 123);

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay, EventEntry{SnapCreateEvent(123, "snap")},
               &on_start_ready, &on_start_safe);

  C_SaferCond on_resume;
  when_replay_op_ready(mock_journal_replay, 123, &on_resume);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  ASSERT_EQ(0, on_resume.wait());
  on_finish->complete(0);

  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapRemoveEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_snap_remove(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay, EventEntry{SnapRemoveEvent(123, "snap")},
               &on_start_ready, &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapRenameEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_snap_rename(mock_image_ctx, &on_finish, 234, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
               EventEntry{SnapRenameEvent(123, 234, "snap")},
               &on_start_ready, &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapProtectEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_snap_protect(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay, EventEntry{SnapProtectEvent(123, "snap")},
               &on_start_ready, &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapUnprotectEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_snap_unprotect(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay, EventEntry{SnapUnprotectEvent(123, "snap")},
               &on_start_ready, &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapRollbackEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_snap_rollback(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay, EventEntry{SnapRollbackEvent(123, "snap")},
               &on_start_ready, &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, RenameEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_rename(mock_image_ctx, &on_finish, "image");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay, EventEntry{RenameEvent(123, "image")},
               &on_start_ready, &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, ResizeEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_resize(mock_image_ctx, &on_finish, 234, 123);

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay, EventEntry{ResizeEvent(123, 234)},
               &on_start_ready, &on_start_safe);

  C_SaferCond on_resume;
  when_replay_op_ready(mock_journal_replay, 123, &on_resume);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  ASSERT_EQ(0, on_resume.wait());
  on_finish->complete(0);

  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, FlattenEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_flatten(mock_image_ctx, &on_finish);

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay, EventEntry{FlattenEvent(123)},
               &on_start_ready, &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, UnknownEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  bufferlist bl;
  ENCODE_START(1, 1, bl);
  ::encode(static_cast<uint32_t>(-1), bl);
  ENCODE_FINISH(bl);

  bufferlist::iterator it = bl.begin();
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  when_process(mock_journal_replay, &it, &on_ready, &on_safe);

  ASSERT_EQ(0, on_safe.wait());
  ASSERT_EQ(0, on_ready.wait());
}

} // namespace journal
} // namespace librbd
