// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/journal/Replay.h"
#include "librbd/journal/Types.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <boost/scope_exit.hpp>

namespace librbd {

namespace {

struct MockReplayImageCtx : public MockImageCtx {
  explicit MockReplayImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace io {

template <>
struct ImageRequest<MockReplayImageCtx> {
  static ImageRequest *s_instance;

  MOCK_METHOD4(aio_write, void(AioCompletion *c, const Extents &image_extents,
                               const bufferlist &bl, int op_flags));
  static void aio_write(MockReplayImageCtx *ictx, AioCompletion *c,
                        Extents &&image_extents, bufferlist &&bl,
                        int op_flags, const ZTracer::Trace &parent_trace) {
    ceph_assert(s_instance != nullptr);
    s_instance->aio_write(c, image_extents, bl, op_flags);
  }

  MOCK_METHOD3(aio_discard, void(AioCompletion *c, const Extents& image_extents,
                                 uint32_t discard_granularity_bytes));
  static void aio_discard(MockReplayImageCtx *ictx, AioCompletion *c,
                          Extents&& image_extents,
                          uint32_t discard_granularity_bytes,
                          const ZTracer::Trace &parent_trace) {
    ceph_assert(s_instance != nullptr);
    s_instance->aio_discard(c, image_extents, discard_granularity_bytes);
  }

  MOCK_METHOD1(aio_flush, void(AioCompletion *c));
  static void aio_flush(MockReplayImageCtx *ictx, AioCompletion *c,
                        FlushSource, const ZTracer::Trace &parent_trace) {
    ceph_assert(s_instance != nullptr);
    s_instance->aio_flush(c);
  }

  MOCK_METHOD4(aio_writesame, void(AioCompletion *c,
                                   const Extents& image_extents,
                                   const bufferlist &bl, int op_flags));
  static void aio_writesame(MockReplayImageCtx *ictx, AioCompletion *c,
                            Extents&& image_extents, bufferlist &&bl,
                            int op_flags, const ZTracer::Trace &parent_trace) {
    ceph_assert(s_instance != nullptr);
    s_instance->aio_writesame(c, image_extents, bl, op_flags);
  }

  MOCK_METHOD6(aio_compare_and_write, void(AioCompletion *c, const Extents &image_extents,
                                           const bufferlist &cmp_bl, const bufferlist &bl,
                                           uint64_t *mismatch_offset, int op_flags));
  static void aio_compare_and_write(MockReplayImageCtx *ictx, AioCompletion *c,
                                    Extents &&image_extents, bufferlist &&cmp_bl,
                                    bufferlist &&bl, uint64_t *mismatch_offset,
                                    int op_flags, const ZTracer::Trace &parent_trace) {
    ceph_assert(s_instance != nullptr);
    s_instance->aio_compare_and_write(c, image_extents, cmp_bl, bl,
                                      mismatch_offset, op_flags);
  }

  ImageRequest() {
    s_instance = this;
  }
};

ImageRequest<MockReplayImageCtx> *ImageRequest<MockReplayImageCtx>::s_instance = nullptr;

} // namespace io

namespace util {

inline ImageCtx *get_image_ctx(librbd::MockReplayImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util

} // namespace librbd

// template definitions
#include "librbd/journal/Replay.cc"
template class librbd::journal::Replay<librbd::MockReplayImageCtx>;

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::StrEq;
using ::testing::WithArgs;

MATCHER_P(BufferlistEqual, str, "") {
  bufferlist bl(arg);
  return (strncmp(bl.c_str(), str, strlen(str)) == 0);
}

MATCHER_P(CStrEq, str, "") {
  return (strncmp(arg, str, strlen(str)) == 0);
}

ACTION_P2(NotifyInvoke, lock, cond) {
  std::lock_guard locker{*lock};
  cond->notify_all();
}

ACTION_P2(CompleteAioCompletion, r, image_ctx) {
  image_ctx->op_work_queue->queue(new LambdaContext([this, arg0](int r) {
      arg0->get();
      arg0->init_time(image_ctx, librbd::io::AIO_TYPE_NONE);
      arg0->set_request_count(1);
      arg0->complete_request(r);
    }), r);
}

namespace librbd {
namespace journal {

class TestMockJournalReplay : public TestMockFixture {
public:
  typedef io::ImageRequest<MockReplayImageCtx> MockIoImageRequest;
  typedef Replay<MockReplayImageCtx> MockJournalReplay;

  TestMockJournalReplay() = default;

  void expect_accept_ops(MockExclusiveLock &mock_exclusive_lock, bool accept) {
    EXPECT_CALL(mock_exclusive_lock, accept_ops()).WillRepeatedly(
      Return(accept));
  }

  void expect_aio_discard(MockIoImageRequest &mock_io_image_request,
                          io::AioCompletion **aio_comp, uint64_t off,
                          uint64_t len, uint32_t discard_granularity_bytes) {
    EXPECT_CALL(mock_io_image_request, aio_discard(_, io::Extents{{off, len}},
                                                   discard_granularity_bytes))
                  .WillOnce(SaveArg<0>(aio_comp));
  }

  void expect_aio_flush(MockIoImageRequest &mock_io_image_request,
                        io::AioCompletion **aio_comp) {
    EXPECT_CALL(mock_io_image_request, aio_flush(_))
                  .WillOnce(SaveArg<0>(aio_comp));
  }

  void expect_aio_flush(MockReplayImageCtx &mock_image_ctx,
                        MockIoImageRequest &mock_io_image_request, int r) {
    EXPECT_CALL(mock_io_image_request, aio_flush(_))
                  .WillOnce(CompleteAioCompletion(r, mock_image_ctx.image_ctx));
  }

  void expect_aio_write(MockIoImageRequest &mock_io_image_request,
                        io::AioCompletion **aio_comp, uint64_t off,
                        uint64_t len, const char *data) {
    EXPECT_CALL(mock_io_image_request,
                aio_write(_, io::Extents{{off, len}}, BufferlistEqual(data), _))
                  .WillOnce(SaveArg<0>(aio_comp));
  }

  void expect_aio_writesame(MockIoImageRequest &mock_io_image_request,
                            io::AioCompletion **aio_comp, uint64_t off,
                            uint64_t len, const char *data) {
    EXPECT_CALL(mock_io_image_request,
                aio_writesame(_, io::Extents{{off, len}},
                              BufferlistEqual(data), _))
                  .WillOnce(SaveArg<0>(aio_comp));
  }

  void expect_aio_compare_and_write(MockIoImageRequest &mock_io_image_request,
                                    io::AioCompletion **aio_comp, uint64_t off,
                                    uint64_t len, const char *cmp_data,
                                    const char *data,
                                    uint64_t *mismatch_offset) {
    EXPECT_CALL(mock_io_image_request,
                aio_compare_and_write(_, io::Extents{{off, len}},
                                      BufferlistEqual(cmp_data),
                                      BufferlistEqual(data),
                                      mismatch_offset, _))
                  .WillOnce(SaveArg<0>(aio_comp));
  }

  void expect_flatten(MockReplayImageCtx &mock_image_ctx, Context **on_finish) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_flatten(_, _))
                  .WillOnce(DoAll(SaveArg<1>(on_finish),
                                  NotifyInvoke(&m_invoke_lock, &m_invoke_cond)));
  }

  void expect_rename(MockReplayImageCtx &mock_image_ctx, Context **on_finish,
                     const char *image_name) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_rename(StrEq(image_name), _))
                  .WillOnce(DoAll(SaveArg<1>(on_finish),
                                  NotifyInvoke(&m_invoke_lock, &m_invoke_cond)));
  }

  void expect_resize(MockReplayImageCtx &mock_image_ctx, Context **on_finish,
                     uint64_t size, uint64_t op_tid) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_resize(size, _, _, _, op_tid))
                  .WillOnce(DoAll(SaveArg<3>(on_finish),
                                  NotifyInvoke(&m_invoke_lock, &m_invoke_cond)));
  }

  void expect_snap_create(MockReplayImageCtx &mock_image_ctx,
                          Context **on_finish, const char *snap_name,
                          uint64_t op_tid) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_snap_create(_, StrEq(snap_name), _,
                                                                op_tid, false, _))
                  .WillOnce(DoAll(SaveArg<2>(on_finish),
                                  NotifyInvoke(&m_invoke_lock, &m_invoke_cond)));
  }

  void expect_snap_remove(MockReplayImageCtx &mock_image_ctx,
                          Context **on_finish, const char *snap_name) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_snap_remove(_, StrEq(snap_name), _))
                  .WillOnce(DoAll(SaveArg<2>(on_finish),
                                  NotifyInvoke(&m_invoke_lock, &m_invoke_cond)));
  }

  void expect_snap_rename(MockReplayImageCtx &mock_image_ctx,
                          Context **on_finish, uint64_t snap_id,
                          const char *snap_name) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_snap_rename(snap_id, StrEq(snap_name), _))
                  .WillOnce(DoAll(SaveArg<2>(on_finish),
                                  NotifyInvoke(&m_invoke_lock, &m_invoke_cond)));
  }

  void expect_snap_protect(MockReplayImageCtx &mock_image_ctx,
                           Context **on_finish, const char *snap_name) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_snap_protect(_, StrEq(snap_name), _))
                  .WillOnce(DoAll(SaveArg<2>(on_finish),
                                  NotifyInvoke(&m_invoke_lock, &m_invoke_cond)));
  }

  void expect_snap_unprotect(MockReplayImageCtx &mock_image_ctx,
                             Context **on_finish, const char *snap_name) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_snap_unprotect(_, StrEq(snap_name), _))
                  .WillOnce(DoAll(SaveArg<2>(on_finish),
                                  NotifyInvoke(&m_invoke_lock, &m_invoke_cond)));
  }

  void expect_snap_rollback(MockReplayImageCtx &mock_image_ctx,
                            Context **on_finish, const char *snap_name) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_snap_rollback(_, StrEq(snap_name), _, _))
                  .WillOnce(DoAll(SaveArg<3>(on_finish),
                                  NotifyInvoke(&m_invoke_lock, &m_invoke_cond)));
  }

  void expect_update_features(MockReplayImageCtx &mock_image_ctx, Context **on_finish,
                              uint64_t features, bool enabled, uint64_t op_tid) {
      EXPECT_CALL(*mock_image_ctx.operations, execute_update_features(features, enabled, _, op_tid))
                  .WillOnce(DoAll(SaveArg<2>(on_finish),
                                  NotifyInvoke(&m_invoke_lock, &m_invoke_cond)));
  }

  void expect_metadata_set(MockReplayImageCtx &mock_image_ctx,
                           Context **on_finish, const char *key,
                           const char *value) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_metadata_set(StrEq(key),
                                                                 StrEq(value), _))
                  .WillOnce(DoAll(SaveArg<2>(on_finish),
                                  NotifyInvoke(&m_invoke_lock, &m_invoke_cond)));
  }

  void expect_metadata_remove(MockReplayImageCtx &mock_image_ctx,
                              Context **on_finish, const char *key) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_metadata_remove(StrEq(key), _))
                  .WillOnce(DoAll(SaveArg<1>(on_finish),
                                  NotifyInvoke(&m_invoke_lock, &m_invoke_cond)));
  }

  void expect_refresh_image(MockReplayImageCtx &mock_image_ctx, bool required,
                            int r) {
    EXPECT_CALL(*mock_image_ctx.state, is_refresh_required())
                  .WillOnce(Return(required));
    if (required) {
      EXPECT_CALL(*mock_image_ctx.state, refresh(_))
                    .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
    }
  }

  void when_process(MockJournalReplay &mock_journal_replay,
                    EventEntry &&event_entry, Context *on_ready,
                    Context *on_safe) {
    bufferlist bl;
    encode(event_entry, bl);

    auto it = bl.cbegin();
    when_process(mock_journal_replay, &it, on_ready, on_safe);
  }

  void when_process(MockJournalReplay &mock_journal_replay,
                    bufferlist::const_iterator *it, Context *on_ready,
                    Context *on_safe) {
    EventEntry event_entry;
    int r = mock_journal_replay.decode(it, &event_entry);
    ASSERT_EQ(0, r);

    mock_journal_replay.process(event_entry, on_ready, on_safe);
  }

  void when_complete(MockReplayImageCtx &mock_image_ctx,
                     io::AioCompletion *aio_comp, int r) {
    aio_comp->get();
    aio_comp->init_time(mock_image_ctx.image_ctx, librbd::io::AIO_TYPE_NONE);
    aio_comp->set_request_count(1);
    aio_comp->complete_request(r);
  }

  int when_flush(MockJournalReplay &mock_journal_replay) {
    C_SaferCond ctx;
    mock_journal_replay.flush(&ctx);
    return ctx.wait();
  }

  int when_shut_down(MockJournalReplay &mock_journal_replay, bool cancel_ops) {
    C_SaferCond ctx;
    mock_journal_replay.shut_down(cancel_ops, &ctx);
    return ctx.wait();
  }

  void when_replay_op_ready(MockJournalReplay &mock_journal_replay,
                            uint64_t op_tid, Context *on_resume) {
    mock_journal_replay.replay_op_ready(op_tid, on_resume);
  }

  void wait_for_op_invoked(Context **on_finish, int r) {
    {
      std::unique_lock locker{m_invoke_lock};      
      m_invoke_cond.wait(locker, [on_finish] { return *on_finish != nullptr; });
    }
    (*on_finish)->complete(r);
  }

  bufferlist to_bl(const std::string &str) {
    bufferlist bl;
    bl.append(str);
    return bl;
  }

  ceph::mutex m_invoke_lock = ceph::make_mutex("m_invoke_lock");
  ceph::condition_variable m_invoke_cond;
};

TEST_F(TestMockJournalReplay, AioDiscard) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockIoImageRequest mock_io_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  io::AioCompletion *aio_comp;
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  expect_aio_discard(mock_io_image_request, &aio_comp, 123, 456,
                     ictx->discard_granularity_bytes);
  when_process(mock_journal_replay,
               EventEntry{AioDiscardEvent(123, 456,
                                          ictx->discard_granularity_bytes)},
               &on_ready, &on_safe);

  when_complete(mock_image_ctx, aio_comp, 0);
  ASSERT_EQ(0, on_ready.wait());

  expect_aio_flush(mock_image_ctx, mock_io_image_request, 0);
  ASSERT_EQ(0, when_shut_down(mock_journal_replay, false));
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, AioWrite) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockIoImageRequest mock_io_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  io::AioCompletion *aio_comp;
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  expect_aio_write(mock_io_image_request, &aio_comp, 123, 456, "test");
  when_process(mock_journal_replay,
               EventEntry{AioWriteEvent(123, 456, to_bl("test"))},
               &on_ready, &on_safe);

  when_complete(mock_image_ctx, aio_comp, 0);
  ASSERT_EQ(0, on_ready.wait());

  expect_aio_flush(mock_image_ctx, mock_io_image_request, 0);
  ASSERT_EQ(0, when_shut_down(mock_journal_replay, false));
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, AioFlush) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockIoImageRequest mock_io_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  io::AioCompletion *aio_comp;
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  expect_aio_flush(mock_io_image_request, &aio_comp);
  when_process(mock_journal_replay, EventEntry{AioFlushEvent()},
               &on_ready, &on_safe);

  when_complete(mock_image_ctx, aio_comp, 0);
  ASSERT_EQ(0, on_safe.wait());

  ASSERT_EQ(0, when_shut_down(mock_journal_replay, false));
  ASSERT_EQ(0, on_ready.wait());
}

TEST_F(TestMockJournalReplay, AioWriteSame) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockIoImageRequest mock_io_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  io::AioCompletion *aio_comp;
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  expect_aio_writesame(mock_io_image_request, &aio_comp, 123, 456, "333");
  when_process(mock_journal_replay,
               EventEntry{AioWriteSameEvent(123, 456, to_bl("333"))},
               &on_ready, &on_safe);

  when_complete(mock_image_ctx, aio_comp, 0);
  ASSERT_EQ(0, on_ready.wait());

  expect_aio_flush(mock_image_ctx, mock_io_image_request, 0);
  ASSERT_EQ(0, when_shut_down(mock_journal_replay, false));
  ASSERT_EQ(0, on_safe.wait());
}


TEST_F(TestMockJournalReplay, AioCompareAndWrite) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_write_journal_replay(mock_image_ctx);
  MockJournalReplay mock_compare_and_write_journal_replay(mock_image_ctx);
  MockJournalReplay mock_mis_compare_and_write_journal_replay(mock_image_ctx);
  MockIoImageRequest mock_io_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  io::AioCompletion *aio_comp;
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  expect_aio_write(mock_io_image_request, &aio_comp, 512, 512, "test");
  when_process(mock_write_journal_replay,
               EventEntry{AioWriteEvent(512, 512, to_bl("test"))},
               &on_ready, &on_safe);

  when_complete(mock_image_ctx, aio_comp, 0);
  ASSERT_EQ(0, on_ready.wait());

  expect_aio_flush(mock_image_ctx, mock_io_image_request, 0);
  ASSERT_EQ(0, when_shut_down(mock_write_journal_replay, false));
  ASSERT_EQ(0, on_safe.wait());

  expect_aio_compare_and_write(mock_io_image_request, &aio_comp,
                               512, 512, "test", "test", nullptr);
  when_process(mock_compare_and_write_journal_replay,
               EventEntry{AioCompareAndWriteEvent(512, 512, to_bl("test"),
               to_bl("test"))}, &on_ready, &on_safe);

  when_complete(mock_image_ctx, aio_comp, 0);
  ASSERT_EQ(0, on_ready.wait());

  expect_aio_flush(mock_image_ctx, mock_io_image_request, 0);
  ASSERT_EQ(0, when_shut_down(mock_compare_and_write_journal_replay, false));
  ASSERT_EQ(0, on_safe.wait());

  expect_aio_compare_and_write(mock_io_image_request, &aio_comp,
                               512, 512, "111", "test", nullptr);
  when_process(mock_mis_compare_and_write_journal_replay,
               EventEntry{AioCompareAndWriteEvent(512, 512, to_bl("111"),
               to_bl("test"))}, &on_ready, &on_safe);

  when_complete(mock_image_ctx, aio_comp, 0);
  ASSERT_EQ(0, on_ready.wait());

  expect_aio_flush(mock_image_ctx, mock_io_image_request, 0);
  ASSERT_EQ(0, when_shut_down(mock_mis_compare_and_write_journal_replay, false));
  ASSERT_EQ(0, on_safe.wait());

}

TEST_F(TestMockJournalReplay, IOError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockIoImageRequest mock_io_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  io::AioCompletion *aio_comp;
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  expect_aio_discard(mock_io_image_request, &aio_comp, 123, 456,
                     ictx->discard_granularity_bytes);
  when_process(mock_journal_replay,
               EventEntry{AioDiscardEvent(123, 456,
                                          ictx->discard_granularity_bytes)},
               &on_ready, &on_safe);

  when_complete(mock_image_ctx, aio_comp, -EINVAL);
  ASSERT_EQ(-EINVAL, on_safe.wait());

  expect_aio_flush(mock_image_ctx, mock_io_image_request, 0);
  ASSERT_EQ(0, when_shut_down(mock_journal_replay, false));
  ASSERT_EQ(0, on_ready.wait());
}

TEST_F(TestMockJournalReplay, SoftFlushIO) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockIoImageRequest mock_io_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  const size_t io_count = 32;
  C_SaferCond on_safes[io_count];
  for (size_t i = 0; i < io_count; ++i) {
    io::AioCompletion *aio_comp;
    io::AioCompletion *flush_comp = nullptr;
    C_SaferCond on_ready;
    expect_aio_discard(mock_io_image_request, &aio_comp, 123, 456,
                       ictx->discard_granularity_bytes);
    if (i == io_count - 1) {
      expect_aio_flush(mock_io_image_request, &flush_comp);
    }
    when_process(mock_journal_replay,
                 EventEntry{AioDiscardEvent(123, 456,
                                            ictx->discard_granularity_bytes)},
                 &on_ready, &on_safes[i]);
    when_complete(mock_image_ctx, aio_comp, 0);
    ASSERT_EQ(0, on_ready.wait());

    if (flush_comp != nullptr) {
      when_complete(mock_image_ctx, flush_comp, 0);
    }
  }
  for (auto &on_safe : on_safes) {
    ASSERT_EQ(0, on_safe.wait());
  }

  ASSERT_EQ(0, when_shut_down(mock_journal_replay, false));
}

TEST_F(TestMockJournalReplay, PauseIO) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockIoImageRequest mock_io_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  const size_t io_count = 64;
  std::list<io::AioCompletion *> flush_comps;
  C_SaferCond on_safes[io_count];
  for (size_t i = 0; i < io_count; ++i) {
    io::AioCompletion *aio_comp;
    C_SaferCond on_ready;
    expect_aio_write(mock_io_image_request, &aio_comp, 123, 456, "test");
    if ((i + 1) % 32 == 0) {
      flush_comps.push_back(nullptr);
      expect_aio_flush(mock_io_image_request, &flush_comps.back());
    }
    when_process(mock_journal_replay,
                 EventEntry{AioWriteEvent(123, 456, to_bl("test"))},
                 &on_ready, &on_safes[i]);
    when_complete(mock_image_ctx, aio_comp, 0);
    if (i < io_count - 1) {
      ASSERT_EQ(0, on_ready.wait());
    } else {
      for (auto flush_comp : flush_comps) {
        when_complete(mock_image_ctx, flush_comp, 0);
      }
      ASSERT_EQ(0, on_ready.wait());
    }
  }
  for (auto &on_safe : on_safes) {
    ASSERT_EQ(0, on_safe.wait());
  }

  ASSERT_EQ(0, when_shut_down(mock_journal_replay, false));
}

TEST_F(TestMockJournalReplay, Flush) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockIoImageRequest mock_io_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  io::AioCompletion *aio_comp = nullptr;
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  expect_aio_discard(mock_io_image_request, &aio_comp, 123, 456,
                     ictx->discard_granularity_bytes);
  when_process(mock_journal_replay,
               EventEntry{AioDiscardEvent(123, 456,
                                          ictx->discard_granularity_bytes)},
               &on_ready, &on_safe);

  when_complete(mock_image_ctx, aio_comp, 0);
  ASSERT_EQ(0, on_ready.wait());

  expect_aio_flush(mock_image_ctx, mock_io_image_request, 0);
  ASSERT_EQ(0, when_flush(mock_journal_replay));
  ASSERT_EQ(0, on_safe.wait());
}

TEST_F(TestMockJournalReplay, OpFinishError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapRemoveEvent(123,
					  cls::rbd::UserSnapshotNamespace(),
					  "snap")},
               &on_start_ready,
	       &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, -EIO)},
               &on_finish_ready, &on_finish_safe);

  ASSERT_EQ(-EIO, on_start_safe.wait());
  ASSERT_EQ(-EIO, on_finish_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
}

TEST_F(TestMockJournalReplay, BlockedOpFinishError) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
  expect_snap_create(mock_image_ctx, &on_finish, "snap", 123);

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapCreateEvent(123,
					  cls::rbd::UserSnapshotNamespace(),
					  "snap")},
               &on_start_ready,
	       &on_start_safe);

  C_SaferCond on_resume;
  when_replay_op_ready(mock_journal_replay, 123, &on_resume);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, -EBADMSG)},
               &on_finish_ready, &on_finish_safe);

  ASSERT_EQ(-EBADMSG, on_resume.wait());
  wait_for_op_invoked(&on_finish, -ESTALE);

  ASSERT_EQ(-ESTALE, on_start_safe.wait());
  ASSERT_EQ(-ESTALE, on_finish_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
}

TEST_F(TestMockJournalReplay, MissingOpFinishEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  EXPECT_CALL(*mock_image_ctx.state, is_refresh_required())
                .WillRepeatedly(Return(false));

  InSequence seq;
  Context *on_snap_create_finish = nullptr;
  expect_snap_create(mock_image_ctx, &on_snap_create_finish, "snap", 123);

  Context *on_snap_remove_finish = nullptr;
  expect_snap_remove(mock_image_ctx, &on_snap_remove_finish, "snap");

  C_SaferCond on_snap_remove_ready;
  C_SaferCond on_snap_remove_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapRemoveEvent(122,
					  cls::rbd::UserSnapshotNamespace(),
					  "snap")},
               &on_snap_remove_ready,
	       &on_snap_remove_safe);
  ictx->op_work_queue->drain();
  ASSERT_EQ(0, on_snap_remove_ready.wait());

  C_SaferCond on_snap_create_ready;
  C_SaferCond on_snap_create_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapCreateEvent(123,
					  cls::rbd::UserSnapshotNamespace(),
					  "snap")},
               &on_snap_create_ready,
	       &on_snap_create_safe);
  ictx->op_work_queue->drain();

  C_SaferCond on_shut_down;
  mock_journal_replay.shut_down(false, &on_shut_down);

  wait_for_op_invoked(&on_snap_remove_finish, 0);
  ASSERT_EQ(0, on_snap_remove_safe.wait());

  C_SaferCond on_snap_create_resume;
  when_replay_op_ready(mock_journal_replay, 123, &on_snap_create_resume);
  ASSERT_EQ(0, on_snap_create_resume.wait());

  wait_for_op_invoked(&on_snap_create_finish, 0);
  ASSERT_EQ(0, on_snap_create_ready.wait());
  ASSERT_EQ(0, on_snap_create_safe.wait());

  ASSERT_EQ(0, on_shut_down.wait());
}

TEST_F(TestMockJournalReplay, MissingOpFinishEventCancelOps) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_snap_create_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
  expect_snap_create(mock_image_ctx, &on_snap_create_finish, "snap", 123);

  C_SaferCond on_snap_remove_ready;
  C_SaferCond on_snap_remove_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapRemoveEvent(122,
					  cls::rbd::UserSnapshotNamespace(),
					  "snap")},
               &on_snap_remove_ready,
	       &on_snap_remove_safe);
  ictx->op_work_queue->drain();
  ASSERT_EQ(0, on_snap_remove_ready.wait());

  C_SaferCond on_snap_create_ready;
  C_SaferCond on_snap_create_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapCreateEvent(123,
					  cls::rbd::UserSnapshotNamespace(),
					  "snap")},
               &on_snap_create_ready,
	       &on_snap_create_safe);
  ictx->op_work_queue->drain();

  C_SaferCond on_resume;
  when_replay_op_ready(mock_journal_replay, 123, &on_resume);
  ASSERT_EQ(0, on_snap_create_ready.wait());

  C_SaferCond on_shut_down;
  mock_journal_replay.shut_down(true, &on_shut_down);

  ASSERT_EQ(-ERESTART, on_resume.wait());
  on_snap_create_finish->complete(-ERESTART);
  ASSERT_EQ(-ERESTART, on_snap_create_safe.wait());

  ASSERT_EQ(-ERESTART, on_snap_remove_safe.wait());
  ASSERT_EQ(0, on_shut_down.wait());
}

TEST_F(TestMockJournalReplay, UnknownOpFinishEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

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

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
  expect_snap_remove(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapRemoveEvent(123,
					  cls::rbd::UserSnapshotNamespace(),
					  "snap")},
               &on_start_ready,
	       &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  wait_for_op_invoked(&on_finish, -EINVAL);
  ASSERT_EQ(-EINVAL, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(-EINVAL, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapCreateEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
  expect_snap_create(mock_image_ctx, &on_finish, "snap", 123);

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapCreateEvent(123,
					  cls::rbd::UserSnapshotNamespace(),
					  "snap")},
               &on_start_ready,
	       &on_start_safe);

  C_SaferCond on_resume;
  when_replay_op_ready(mock_journal_replay, 123, &on_resume);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  ASSERT_EQ(0, on_resume.wait());
  wait_for_op_invoked(&on_finish, 0);

  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapCreateEventExists) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
  expect_snap_create(mock_image_ctx, &on_finish, "snap", 123);

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapCreateEvent(123,
					  cls::rbd::UserSnapshotNamespace(),
					  "snap")},
               &on_start_ready,
	       &on_start_safe);

  wait_for_op_invoked(&on_finish, -EEXIST);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapRemoveEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
  expect_snap_remove(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapRemoveEvent(123,
					  cls::rbd::UserSnapshotNamespace(),
					  "snap")},
               &on_start_ready,
	       &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  wait_for_op_invoked(&on_finish, 0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapRemoveEventDNE) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
  expect_snap_remove(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapRemoveEvent(123,
					  cls::rbd::UserSnapshotNamespace(),
					  "snap")},
               &on_start_ready,
	       &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  wait_for_op_invoked(&on_finish, -ENOENT);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapRenameEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
  expect_snap_rename(mock_image_ctx, &on_finish, 234, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
               EventEntry{SnapRenameEvent(123, 234, "snap1", "snap")},
               &on_start_ready, &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  wait_for_op_invoked(&on_finish, 0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapRenameEventExists) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
  expect_snap_rename(mock_image_ctx, &on_finish, 234, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
               EventEntry{SnapRenameEvent(123, 234, "snap1", "snap")},
               &on_start_ready, &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  wait_for_op_invoked(&on_finish, -EEXIST);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapProtectEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
  expect_snap_protect(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapProtectEvent(123,
					   cls::rbd::UserSnapshotNamespace(),
					   "snap")},
               &on_start_ready,
	       &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  wait_for_op_invoked(&on_finish, 0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapProtectEventBusy) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
  expect_snap_protect(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapProtectEvent(123,
					   cls::rbd::UserSnapshotNamespace(),
					   "snap")},
               &on_start_ready,
	       &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  wait_for_op_invoked(&on_finish, -EBUSY);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapUnprotectEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
  expect_snap_unprotect(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapUnprotectEvent(123,
					     cls::rbd::UserSnapshotNamespace(),
					     "snap")},
               &on_start_ready,
	       &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  wait_for_op_invoked(&on_finish, 0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapUnprotectOpFinishBusy) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapUnprotectEvent(123,
					     cls::rbd::UserSnapshotNamespace(),
					     "snap")},
               &on_start_ready,
	       &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  // aborts the snap unprotect op if image had children
  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, -EBUSY)},
               &on_finish_ready, &on_finish_safe);

  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
}

TEST_F(TestMockJournalReplay, SnapUnprotectEventInvalid) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
  expect_snap_unprotect(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapUnprotectEvent(123,
					     cls::rbd::UserSnapshotNamespace(),
					     "snap")},
               &on_start_ready,
	       &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  wait_for_op_invoked(&on_finish, -EINVAL);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, SnapRollbackEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
  expect_snap_rollback(mock_image_ctx, &on_finish, "snap");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
	       EventEntry{SnapRollbackEvent(123,
					    cls::rbd::UserSnapshotNamespace(),
					    "snap")},
               &on_start_ready,
	       &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  wait_for_op_invoked(&on_finish, 0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, RenameEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
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

  wait_for_op_invoked(&on_finish, 0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, RenameEventExists) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
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

  wait_for_op_invoked(&on_finish, -EEXIST);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, ResizeEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
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
  wait_for_op_invoked(&on_finish, 0);

  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, FlattenEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
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

  wait_for_op_invoked(&on_finish, 0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, FlattenEventInvalid) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
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

  wait_for_op_invoked(&on_finish, -EINVAL);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, UpdateFeaturesEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  uint64_t features = RBD_FEATURE_OBJECT_MAP | RBD_FEATURE_FAST_DIFF;
  bool enabled = !ictx->test_features(features);

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, false, 0);
  expect_update_features(mock_image_ctx, &on_finish, features, enabled, 123);

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay,
               EventEntry{UpdateFeaturesEvent(123, features, enabled)},
               &on_start_ready, &on_start_safe);

  C_SaferCond on_resume;
  when_replay_op_ready(mock_journal_replay, 123, &on_resume);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  ASSERT_EQ(0, on_resume.wait());
  wait_for_op_invoked(&on_finish, 0);

  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, MetadataSetEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_metadata_set(mock_image_ctx, &on_finish, "key", "value");
  expect_refresh_image(mock_image_ctx, false, 0);

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay, EventEntry{MetadataSetEvent(123, "key", "value")},
               &on_start_ready, &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  wait_for_op_invoked(&on_finish, 0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, MetadataRemoveEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_metadata_remove(mock_image_ctx, &on_finish, "key");
  expect_refresh_image(mock_image_ctx, false, 0);

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay, EventEntry{MetadataRemoveEvent(123, "key")},
               &on_start_ready, &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  wait_for_op_invoked(&on_finish, 0);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, MetadataRemoveEventDNE) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_metadata_remove(mock_image_ctx, &on_finish, "key");

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay, EventEntry{MetadataRemoveEvent(123, "key")},
               &on_start_ready, &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  wait_for_op_invoked(&on_finish, -ENOENT);
  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, UnknownEvent) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  bufferlist bl;
  ENCODE_START(1, 1, bl);
  encode(static_cast<uint32_t>(-1), bl);
  ENCODE_FINISH(bl);

  auto it = bl.cbegin();
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  when_process(mock_journal_replay, &it, &on_ready, &on_safe);

  ASSERT_EQ(0, on_safe.wait());
  ASSERT_EQ(0, on_ready.wait());
}

TEST_F(TestMockJournalReplay, RefreshImageBeforeOpStart) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  Context *on_finish = nullptr;
  expect_refresh_image(mock_image_ctx, true, 0);
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
  wait_for_op_invoked(&on_finish, 0);

  ASSERT_EQ(0, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(0, on_finish_safe.wait());
}

TEST_F(TestMockJournalReplay, FlushEventAfterShutDown) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockIoImageRequest mock_io_image_request;
  expect_op_work_queue(mock_image_ctx);

  ASSERT_EQ(0, when_shut_down(mock_journal_replay, false));

  C_SaferCond on_ready;
  C_SaferCond on_safe;
  when_process(mock_journal_replay, EventEntry{AioFlushEvent()},
               &on_ready, &on_safe);
  ASSERT_EQ(0, on_ready.wait());
  ASSERT_EQ(-ESHUTDOWN, on_safe.wait());
}

TEST_F(TestMockJournalReplay, ModifyEventAfterShutDown) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockIoImageRequest mock_io_image_request;
  expect_op_work_queue(mock_image_ctx);

  ASSERT_EQ(0, when_shut_down(mock_journal_replay, false));

  C_SaferCond on_ready;
  C_SaferCond on_safe;
  when_process(mock_journal_replay,
               EventEntry{AioWriteEvent(123, 456, to_bl("test"))},
               &on_ready, &on_safe);
  ASSERT_EQ(0, on_ready.wait());
  ASSERT_EQ(-ESHUTDOWN, on_safe.wait());
}

TEST_F(TestMockJournalReplay, OpEventAfterShutDown) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, true);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockIoImageRequest mock_io_image_request;
  expect_op_work_queue(mock_image_ctx);

  ASSERT_EQ(0, when_shut_down(mock_journal_replay, false));

  C_SaferCond on_ready;
  C_SaferCond on_safe;
  when_process(mock_journal_replay, EventEntry{RenameEvent(123, "image")},
               &on_ready, &on_safe);
  ASSERT_EQ(0, on_ready.wait());
  ASSERT_EQ(-ESHUTDOWN, on_safe.wait());
}

TEST_F(TestMockJournalReplay, LockLostBeforeProcess) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, false);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  MockIoImageRequest mock_io_image_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  C_SaferCond on_ready;
  C_SaferCond on_safe;
  when_process(mock_journal_replay, EventEntry{AioFlushEvent()},
               &on_ready, &on_safe);
  ASSERT_EQ(0, on_ready.wait());
  ASSERT_EQ(-ECANCELED, on_safe.wait());

  ASSERT_EQ(0, when_shut_down(mock_journal_replay, false));
}

TEST_F(TestMockJournalReplay, LockLostBeforeExecuteOp) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockReplayImageCtx mock_image_ctx(*ictx);

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  expect_accept_ops(mock_exclusive_lock, false);

  MockJournalReplay mock_journal_replay(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  EXPECT_CALL(mock_exclusive_lock, accept_ops()).WillOnce(Return(true));
  EXPECT_CALL(mock_exclusive_lock, accept_ops()).WillOnce(Return(true));
  expect_refresh_image(mock_image_ctx, false, 0);

  C_SaferCond on_start_ready;
  C_SaferCond on_start_safe;
  when_process(mock_journal_replay, EventEntry{RenameEvent(123, "image")},
               &on_start_ready, &on_start_safe);
  ASSERT_EQ(0, on_start_ready.wait());

  C_SaferCond on_finish_ready;
  C_SaferCond on_finish_safe;
  when_process(mock_journal_replay, EventEntry{OpFinishEvent(123, 0)},
               &on_finish_ready, &on_finish_safe);

  ASSERT_EQ(-ECANCELED, on_start_safe.wait());
  ASSERT_EQ(0, on_finish_ready.wait());
  ASSERT_EQ(-ECANCELED, on_finish_safe.wait());
}

} // namespace journal
} // namespace librbd
