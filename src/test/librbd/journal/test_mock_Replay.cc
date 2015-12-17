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
                     uint64_t size) {
    EXPECT_CALL(*mock_image_ctx.operations, resize(size, _, _))
                  .WillOnce(SaveArg<2>(on_finish));
  }

  void expect_snap_create(MockImageCtx &mock_image_ctx,
                          Context **on_finish, const char *snap_name) {
    EXPECT_CALL(*mock_image_ctx.operations, snap_create(CStrEq(snap_name), _))
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
                    EventEntry &&event_entry, Context *on_safe) {
    bufferlist bl;
    ::encode(event_entry, bl);

    bufferlist::iterator it = bl.begin();
    mock_journal_replay.process(it, on_safe);
  }

  void when_complete(MockImageCtx &mock_image_ctx, AioCompletion *aio_comp,
                     int r) {
    aio_comp->get();
    aio_comp->set_request_count(mock_image_ctx.cct, 1);
    aio_comp->complete_request(mock_image_ctx.cct, r);
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

  InSequence seq;
  AioCompletion *aio_comp;
  C_SaferCond on_safe;
  expect_aio_discard(mock_aio_image_request, &aio_comp, 123, 456);
  when_process(mock_journal_replay,
               EventEntry{AioDiscardEvent(123, 456)},
               &on_safe);

  when_complete(mock_image_ctx, aio_comp, 0);
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, AioWrite) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockAioImageRequest mock_aio_image_request;

  InSequence seq;
  AioCompletion *aio_comp;
  C_SaferCond on_safe;
  expect_aio_write(mock_aio_image_request, &aio_comp, 123, 456, "test");
  when_process(mock_journal_replay,
               EventEntry{AioWriteEvent(123, 456, to_bl("test"))},
               &on_safe);

  when_complete(mock_image_ctx, aio_comp, 0);
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, AioFlush) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockAioImageRequest mock_aio_image_request;

  InSequence seq;
  AioCompletion *aio_comp;
  C_SaferCond on_safe;
  expect_aio_flush(mock_aio_image_request, &aio_comp);
  when_process(mock_journal_replay, EventEntry{AioFlushEvent()}, &on_safe);

  when_complete(mock_image_ctx, aio_comp, 0);
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, IOError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockAioImageRequest mock_aio_image_request;

  InSequence seq;
  AioCompletion *aio_comp;
  C_SaferCond on_safe;
  expect_aio_discard(mock_aio_image_request, &aio_comp, 123, 456);
  when_process(mock_journal_replay,
               EventEntry{AioDiscardEvent(123, 456)},
               &on_safe);

  when_complete(mock_image_ctx, aio_comp, -EINVAL);
  ASSERT_EQ(-EINVAL, on_safe.wait());
}

TEST_F(TestMockJournalReplay, SynchronousUntilFlush) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);
  // TODO
}

TEST_F(TestMockJournalReplay, SnapCreateEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_snap_create(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_safe;
  when_process(mock_journal_replay, EventEntry{SnapCreateEvent(123, "snap")},
               &on_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapRemoveEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_snap_remove(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_safe;
  when_process(mock_journal_replay, EventEntry{SnapRemoveEvent(123, "snap")},
               &on_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapRenameEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_snap_rename(mock_image_ctx, &on_finish, 234, "snap");

  C_SaferCond on_safe;
  when_process(mock_journal_replay,
               EventEntry{SnapRenameEvent(123, 234, "snap")},
               &on_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapProtectEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_snap_protect(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_safe;
  when_process(mock_journal_replay, EventEntry{SnapProtectEvent(123, "snap")},
               &on_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapUnprotectEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_snap_unprotect(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_safe;
  when_process(mock_journal_replay, EventEntry{SnapUnprotectEvent(123, "snap")},
               &on_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapRollbackEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_snap_rollback(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_safe;
  when_process(mock_journal_replay, EventEntry{SnapRollbackEvent(123, "snap")},
               &on_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, RenameEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_rename(mock_image_ctx, &on_finish, "image");

  C_SaferCond on_safe;
  when_process(mock_journal_replay, EventEntry{RenameEvent(123, "image")},
               &on_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, ResizeEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_resize(mock_image_ctx, &on_finish, 234);

  C_SaferCond on_safe;
  when_process(mock_journal_replay, EventEntry{ResizeEvent(123, 234)},
               &on_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, FlattenEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockJournalReplay mock_journal_replay(mock_image_ctx);

  InSequence seq;
  Context *on_finish;
  expect_flatten(mock_image_ctx, &on_finish);

  C_SaferCond on_safe;
  when_process(mock_journal_replay, EventEntry{FlattenEvent(123)},
               &on_safe);

  on_finish->complete(0);
  ASSERT_EQ(0, on_safe.wait());
}

} // namespace journal
} // namespace librbd
