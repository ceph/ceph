// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"
#include "test/librbd/mock/cache/MockImageCache.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ObjectDispatchSpec.h"

namespace librbd {
namespace {

struct MockTestImageCtx;

struct MockTestJournal : public MockJournal {
  MOCK_METHOD4(append_write_event, uint64_t(uint64_t, size_t,
                                            const bufferlist &, bool));
  MOCK_METHOD5(append_io_event_mock, uint64_t(const journal::EventEntry&,
                                              uint64_t, size_t, bool, int));
  uint64_t append_io_event(journal::EventEntry &&event_entry,
                           uint64_t offset, size_t length,
                           bool flush_entry, int filter_ret_val) {
    // googlemock doesn't support move semantics
    return append_io_event_mock(event_entry, offset, length, flush_entry,
                                filter_ret_val);
  }

  MOCK_METHOD2(commit_io_event, void(uint64_t, int));
};

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }

  MockTestJournal* journal;
};

} // anonymous namespace

namespace util {

inline ImageCtx *get_image_ctx(MockTestImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util
} // namespace librbd

#include "librbd/io/ImageRequest.cc"

namespace librbd {
namespace io {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;
using ::testing::WithoutArgs;
using ::testing::Exactly;

struct TestMockIoImageRequest : public TestMockFixture {
  typedef ImageRequest<librbd::MockTestImageCtx> MockImageRequest;
  typedef ImageReadRequest<librbd::MockTestImageCtx> MockImageReadRequest;
  typedef ImageWriteRequest<librbd::MockTestImageCtx> MockImageWriteRequest;
  typedef ImageDiscardRequest<librbd::MockTestImageCtx> MockImageDiscardRequest;
  typedef ImageFlushRequest<librbd::MockTestImageCtx> MockImageFlushRequest;
  typedef ImageWriteSameRequest<librbd::MockTestImageCtx> MockImageWriteSameRequest;
  typedef ImageCompareAndWriteRequest<librbd::MockTestImageCtx> MockImageCompareAndWriteRequest;

  void expect_is_journal_appending(MockTestJournal &mock_journal, bool appending) {
    EXPECT_CALL(mock_journal, is_journal_appending())
      .WillOnce(Return(appending));
  }

  void expect_get_modify_timestamp(MockTestImageCtx &mock_image_ctx,
                                   bool needs_update) {
    if (needs_update) {
      mock_image_ctx.mtime_update_interval = 5;
      EXPECT_CALL(mock_image_ctx, get_modify_timestamp())
        .WillOnce(Return(ceph_clock_now() - utime_t(10,0)));
    } else {
      mock_image_ctx.mtime_update_interval = 600;
      EXPECT_CALL(mock_image_ctx, get_modify_timestamp())
        .WillOnce(Return(ceph_clock_now()));
    }
  }

  void expect_object_discard_request(MockTestImageCtx &mock_image_ctx,
                                     uint64_t object_no, uint64_t offset,
                                     uint32_t length, int r) {
    EXPECT_CALL(*mock_image_ctx.io_object_dispatcher, send(_))
      .WillOnce(Invoke([&mock_image_ctx, object_no, offset, length, r]
                (ObjectDispatchSpec* spec) {
                  auto* discard_spec = boost::get<ObjectDispatchSpec::DiscardRequest>(&spec->request);
                  ASSERT_TRUE(discard_spec != nullptr);
                  ASSERT_EQ(object_no, discard_spec->object_no);
                  ASSERT_EQ(offset, discard_spec->object_off);
                  ASSERT_EQ(length, discard_spec->object_len);

                  spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
                  mock_image_ctx.image_ctx->op_work_queue->queue(&spec->dispatcher_ctx, r);
                }));
  }

  void expect_object_request_send(MockTestImageCtx &mock_image_ctx,
                                  int r) {
    EXPECT_CALL(*mock_image_ctx.io_object_dispatcher, send(_))
      .WillOnce(Invoke([&mock_image_ctx, r](ObjectDispatchSpec* spec) {
                  spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
                  mock_image_ctx.image_ctx->op_work_queue->queue(&spec->dispatcher_ctx, r);
                }));
  }

  void expect_flush_async_operations(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(mock_image_ctx, flush_async_operations(_))
      .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }
};

TEST_F(TestMockIoImageRequest, AioWriteModifyTimestamp) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockTestJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  mock_image_ctx.mtime_update_interval = 5;

  utime_t dummy = ceph_clock_now();
  dummy -= utime_t(10,0);

  EXPECT_CALL(mock_image_ctx, get_modify_timestamp())
      .Times(Exactly(3))
      .WillOnce(Return(dummy))
      .WillOnce(Return(dummy))
      .WillOnce(Return(dummy + utime_t(10,0)));

  EXPECT_CALL(mock_image_ctx, set_modify_timestamp(_))
      .Times(Exactly(1));

  InSequence seq;
  expect_is_journal_appending(mock_journal, false);
  expect_object_request_send(mock_image_ctx, 0);

  C_SaferCond aio_comp_ctx_1, aio_comp_ctx_2;
  AioCompletion *aio_comp_1 = AioCompletion::create_and_start(
    &aio_comp_ctx_1, ictx, AIO_TYPE_WRITE);

  AioCompletion *aio_comp_2 = AioCompletion::create_and_start(
    &aio_comp_ctx_2, ictx, AIO_TYPE_WRITE);

  bufferlist bl;
  bl.append("1");
  MockImageWriteRequest mock_aio_image_write_1(mock_image_ctx, aio_comp_1,
                                             {{0, 1}}, std::move(bl), 0, {});
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_aio_image_write_1.send();
  }
  ASSERT_EQ(0, aio_comp_ctx_1.wait());

  expect_is_journal_appending(mock_journal, false);
  expect_object_request_send(mock_image_ctx, 0);

  bl.append("1");
  MockImageWriteRequest mock_aio_image_write_2(mock_image_ctx, aio_comp_2,
                                             {{0, 1}}, std::move(bl), 0, {});
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_aio_image_write_2.send();
  }
  ASSERT_EQ(0, aio_comp_ctx_2.wait());
}

TEST_F(TestMockIoImageRequest, AioReadAccessTimestamp) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockTestJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  mock_image_ctx.atime_update_interval = 5;

  utime_t dummy = ceph_clock_now();
  dummy -= utime_t(10,0);

  EXPECT_CALL(mock_image_ctx, get_access_timestamp())
      .Times(Exactly(3))
      .WillOnce(Return(dummy))
      .WillOnce(Return(dummy))
      .WillOnce(Return(dummy + utime_t(10,0)));

  EXPECT_CALL(mock_image_ctx, set_access_timestamp(_))
      .Times(Exactly(1));

  InSequence seq;
  expect_object_request_send(mock_image_ctx, 0);

  C_SaferCond aio_comp_ctx_1, aio_comp_ctx_2;
  AioCompletion *aio_comp_1 = AioCompletion::create_and_start(
    &aio_comp_ctx_1, ictx, AIO_TYPE_READ);


  ReadResult rr;
  MockImageReadRequest mock_aio_image_read_1(mock_image_ctx, aio_comp_1,
                                             {{0, 1}}, std::move(rr), 0, {});
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_aio_image_read_1.send();
  }
  ASSERT_EQ(1, aio_comp_ctx_1.wait());

  AioCompletion *aio_comp_2 = AioCompletion::create_and_start(
    &aio_comp_ctx_2, ictx, AIO_TYPE_READ);
  expect_object_request_send(mock_image_ctx, 0);

  MockImageReadRequest mock_aio_image_read_2(mock_image_ctx, aio_comp_2,
                                             {{0, 1}}, std::move(rr), 0, {});
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_aio_image_read_2.send();
  }
  ASSERT_EQ(1, aio_comp_ctx_2.wait());
}

TEST_F(TestMockIoImageRequest, PartialDiscard) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->discard_granularity_bytes = 0;

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.journal = nullptr;

  InSequence seq;
  expect_get_modify_timestamp(mock_image_ctx, false);
  expect_object_discard_request(mock_image_ctx, 0, 16, 63, 0);
  expect_object_discard_request(mock_image_ctx, 0, 84, 100, 0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_DISCARD);
  MockImageDiscardRequest mock_aio_image_discard(
    mock_image_ctx, aio_comp, {{16, 63}, {84, 100}},
    ictx->discard_granularity_bytes, {});
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_aio_image_discard.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

TEST_F(TestMockIoImageRequest, TailDiscard) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, resize(ictx, ictx->layout.object_size));
  ictx->discard_granularity_bytes = 2 * ictx->layout.object_size;

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.journal = nullptr;

  InSequence seq;
  expect_get_modify_timestamp(mock_image_ctx, false);
  expect_object_discard_request(
    mock_image_ctx, 0, ictx->layout.object_size - 1024, 1024, 0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_DISCARD);
  MockImageDiscardRequest mock_aio_image_discard(
    mock_image_ctx, aio_comp,
    {{ictx->layout.object_size - 1024, 1024}},
    ictx->discard_granularity_bytes, {});
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_aio_image_discard.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

TEST_F(TestMockIoImageRequest, DiscardGranularity) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, resize(ictx, ictx->layout.object_size));
  ictx->discard_granularity_bytes = 32;

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.journal = nullptr;

  InSequence seq;
  expect_get_modify_timestamp(mock_image_ctx, false);
  expect_object_discard_request(mock_image_ctx, 0, 32, 32, 0);
  expect_object_discard_request(mock_image_ctx, 0, 96, 64, 0);
  expect_object_discard_request(
    mock_image_ctx, 0, ictx->layout.object_size - 32, 32, 0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_DISCARD);
  MockImageDiscardRequest mock_aio_image_discard(
    mock_image_ctx, aio_comp,
    {{16, 63}, {96, 31}, {84, 100}, {ictx->layout.object_size - 33, 33}},
    ictx->discard_granularity_bytes, {});
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_aio_image_discard.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

TEST_F(TestMockIoImageRequest, AioWriteJournalAppendDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockTestJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  InSequence seq;
  expect_get_modify_timestamp(mock_image_ctx, false);
  expect_is_journal_appending(mock_journal, false);
  expect_object_request_send(mock_image_ctx, 0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_WRITE);

  bufferlist bl;
  bl.append("1");
  MockImageWriteRequest mock_aio_image_write(mock_image_ctx, aio_comp,
                                             {{0, 1}}, std::move(bl), 0, {});
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_aio_image_write.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

TEST_F(TestMockIoImageRequest, AioDiscardJournalAppendDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->discard_granularity_bytes = 0;

  MockTestImageCtx mock_image_ctx(*ictx);
  MockTestJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  InSequence seq;
  expect_get_modify_timestamp(mock_image_ctx, false);
  expect_is_journal_appending(mock_journal, false);
  expect_object_request_send(mock_image_ctx, 0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_DISCARD);
  MockImageDiscardRequest mock_aio_image_discard(
    mock_image_ctx, aio_comp, {{0, 1}}, ictx->discard_granularity_bytes, {});
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_aio_image_discard.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

TEST_F(TestMockIoImageRequest, AioFlushJournalAppendDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockTestJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_is_journal_appending(mock_journal, false);
  expect_flush_async_operations(mock_image_ctx, 0);
  expect_object_request_send(mock_image_ctx, 0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_FLUSH);
  MockImageFlushRequest mock_aio_image_flush(mock_image_ctx, aio_comp,
                                             FLUSH_SOURCE_USER, {});
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_aio_image_flush.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

TEST_F(TestMockIoImageRequest, AioWriteSameJournalAppendDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockTestJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  InSequence seq;
  expect_get_modify_timestamp(mock_image_ctx, false);
  expect_is_journal_appending(mock_journal, false);
  expect_object_request_send(mock_image_ctx, 0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_WRITESAME);

  bufferlist bl;
  bl.append("1");
  MockImageWriteSameRequest mock_aio_image_writesame(mock_image_ctx, aio_comp,
                                                     {{0, 1}}, std::move(bl), 0,
                                                     {});
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_aio_image_writesame.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

TEST_F(TestMockIoImageRequest, AioCompareAndWriteJournalAppendDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockTestJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  InSequence seq;
  expect_get_modify_timestamp(mock_image_ctx, false);
  expect_is_journal_appending(mock_journal, false);
  expect_object_request_send(mock_image_ctx, 0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_COMPARE_AND_WRITE);

  bufferlist cmp_bl;
  cmp_bl.append("1");
  bufferlist write_bl;
  write_bl.append("1");
  uint64_t mismatch_offset;
  MockImageCompareAndWriteRequest mock_aio_image_write(mock_image_ctx, aio_comp,
                                                       {{0, 1}}, std::move(cmp_bl),
                                                       std::move(write_bl),
                                                       &mismatch_offset,
                                                       0, {});
  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_aio_image_write.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

} // namespace io
} // namespace librbd
