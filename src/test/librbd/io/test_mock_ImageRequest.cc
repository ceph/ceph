// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"
#include "test/librbd/mock/cache/MockImageCache.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/Utils.h"

namespace librbd {
namespace {

struct MockTestImageCtx;

struct MockTestJournal : public MockJournal {
  MOCK_METHOD3(append_write_event, uint64_t(const io::Extents&,
                                            const bufferlist &, bool));
  MOCK_METHOD3(append_write_same_event, uint64_t(const io::Extents&,
                                                 const bufferlist &, bool));
  MOCK_METHOD5(append_compare_and_write_event, uint64_t(uint64_t, size_t,
                                                        const bufferlist &,
                                                        const bufferlist &,
                                                        bool));
  MOCK_METHOD3(append_discard_event, uint64_t(const io::Extents&, uint32_t, bool));
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

namespace util {

template <>
void area_to_object_extents(MockTestImageCtx* image_ctx, uint64_t offset,
                            uint64_t length, ImageArea area,
                            uint64_t buffer_offset,
                            striper::LightweightObjectExtents* object_extents) {
  Striper::file_to_extents(image_ctx->cct, &image_ctx->layout, offset, length,
                           0, buffer_offset, object_extents);
}

template <>
std::pair<Extents, ImageArea> object_to_area_extents(
    MockTestImageCtx* image_ctx, uint64_t object_no,
    const Extents& object_extents) {
  Extents extents;
  for (auto [off, len] : object_extents) {
    Striper::extent_to_file(image_ctx->cct, &image_ctx->layout, object_no, off,
                            len, extents);
  }
  return {std::move(extents), ImageArea::DATA};
}

} // namespace util

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
  typedef ImageListSnapsRequest<librbd::MockTestImageCtx> MockImageListSnapsRequest;

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

  void expect_journal_append_discard_event(MockTestJournal &mock_journal,
                                           uint64_t journal_tid,
                                           const io::Extents& extents) {
    EXPECT_CALL(mock_journal, append_discard_event(extents, _, _))
      .WillOnce(Return(journal_tid));
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

  void expect_object_list_snaps_request(MockTestImageCtx &mock_image_ctx,
                                        uint64_t object_no,
                                        const SnapshotDelta& snap_delta,
                                        int r) {
    EXPECT_CALL(*mock_image_ctx.io_object_dispatcher, send(_))
      .WillOnce(
        Invoke([&mock_image_ctx, object_no, snap_delta, r]
               (ObjectDispatchSpec* spec) {
                  auto request = boost::get<
                    librbd::io::ObjectDispatchSpec::ListSnapsRequest>(
                      &spec->request);
                  ASSERT_TRUE(request != nullptr);
                  ASSERT_EQ(object_no, request->object_no);

                  *request->snapshot_delta = snap_delta;
                  spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
                  mock_image_ctx.image_ctx->op_work_queue->queue(&spec->dispatcher_ctx, r);
                }));
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
  MockImageWriteRequest mock_aio_image_write_1(
    mock_image_ctx, aio_comp_1, {{0, 1}}, ImageArea::DATA, std::move(bl),
    0, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    mock_aio_image_write_1.send();
  }
  ASSERT_EQ(0, aio_comp_ctx_1.wait());

  expect_is_journal_appending(mock_journal, false);
  expect_object_request_send(mock_image_ctx, 0);

  bl.append("1");
  MockImageWriteRequest mock_aio_image_write_2(
    mock_image_ctx, aio_comp_2, {{0, 1}}, ImageArea::DATA, std::move(bl),
    0, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
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
  MockImageReadRequest mock_aio_image_read_1(
    mock_image_ctx, aio_comp_1, {{0, 1}}, ImageArea::DATA, std::move(rr),
    mock_image_ctx.get_data_io_context(), 0, 0, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    mock_aio_image_read_1.send();
  }
  ASSERT_EQ(1, aio_comp_ctx_1.wait());

  AioCompletion *aio_comp_2 = AioCompletion::create_and_start(
    &aio_comp_ctx_2, ictx, AIO_TYPE_READ);
  expect_object_request_send(mock_image_ctx, 0);

  MockImageReadRequest mock_aio_image_read_2(
    mock_image_ctx, aio_comp_2, {{0, 1}}, ImageArea::DATA, std::move(rr),
    mock_image_ctx.get_data_io_context(), 0, 0, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
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
    mock_image_ctx, aio_comp, {{16, 63}, {84, 100}}, ImageArea::DATA,
    ictx->discard_granularity_bytes, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
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
    {{ictx->layout.object_size - 1024, 1024}}, ImageArea::DATA,
    ictx->discard_granularity_bytes, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
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
    ImageArea::DATA, ictx->discard_granularity_bytes, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    mock_aio_image_discard.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

TEST_F(TestMockIoImageRequest, PartialDiscardJournalAppendEnabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->discard_granularity_bytes = 0;

  MockTestImageCtx mock_image_ctx(*ictx);
  MockTestJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  InSequence seq;
  expect_get_modify_timestamp(mock_image_ctx, false);
  expect_is_journal_appending(mock_journal, true);
  expect_journal_append_discard_event(mock_journal, 0,
                                      {{16, 63}, {84, 100}});
  expect_object_discard_request(mock_image_ctx, 0, 16, 63, 0);
  expect_object_discard_request(mock_image_ctx, 0, 84, 100, 0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_DISCARD);
  MockImageDiscardRequest mock_aio_image_discard(
    mock_image_ctx, aio_comp, {{16, 63}, {84, 100}}, ImageArea::DATA,
    ictx->discard_granularity_bytes, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    mock_aio_image_discard.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

TEST_F(TestMockIoImageRequest, TailDiscardJournalAppendEnabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, resize(ictx, ictx->layout.object_size));
  ictx->discard_granularity_bytes = 2 * ictx->layout.object_size;

  MockTestImageCtx mock_image_ctx(*ictx);
  MockTestJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  InSequence seq;
  expect_get_modify_timestamp(mock_image_ctx, false);
  expect_is_journal_appending(mock_journal, true);
  expect_journal_append_discard_event(
    mock_journal, 0, {{ictx->layout.object_size - 1024, 1024}});
  expect_object_discard_request(
    mock_image_ctx, 0, ictx->layout.object_size - 1024, 1024, 0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_DISCARD);
  MockImageDiscardRequest mock_aio_image_discard(
    mock_image_ctx, aio_comp,
    {{ictx->layout.object_size - 1024, 1024}}, ImageArea::DATA,
    ictx->discard_granularity_bytes, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    mock_aio_image_discard.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

TEST_F(TestMockIoImageRequest, PruneRequiredDiscardJournalAppendEnabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->discard_granularity_bytes = 32;

  MockTestImageCtx mock_image_ctx(*ictx);
  MockTestJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  InSequence seq;
  expect_get_modify_timestamp(mock_image_ctx, false);
  expect_is_journal_appending(mock_journal, true);
  EXPECT_CALL(mock_journal, append_discard_event(_, _, _)).Times(0);
  EXPECT_CALL(*mock_image_ctx.io_object_dispatcher, send(_)).Times(0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_DISCARD);
  MockImageDiscardRequest mock_aio_image_discard(
    mock_image_ctx, aio_comp, {{96, 31}}, ImageArea::DATA,
    ictx->discard_granularity_bytes, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    mock_aio_image_discard.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

TEST_F(TestMockIoImageRequest, LengthModifiedDiscardJournalAppendEnabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ictx->discard_granularity_bytes = 32;

  MockTestImageCtx mock_image_ctx(*ictx);
  MockTestJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  InSequence seq;
  expect_get_modify_timestamp(mock_image_ctx, false);
  expect_is_journal_appending(mock_journal, true);
  expect_journal_append_discard_event(mock_journal, 0, {{32, 32}});
  expect_object_discard_request(mock_image_ctx, 0, 32, 32, 0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_DISCARD);
  MockImageDiscardRequest mock_aio_image_discard(
    mock_image_ctx, aio_comp, {{16, 63}}, ImageArea::DATA,
    ictx->discard_granularity_bytes, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    mock_aio_image_discard.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

TEST_F(TestMockIoImageRequest, DiscardGranularityJournalAppendEnabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, resize(ictx, ictx->layout.object_size));
  ictx->discard_granularity_bytes = 32;

  MockTestImageCtx mock_image_ctx(*ictx);
  MockTestJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  InSequence seq;
  expect_get_modify_timestamp(mock_image_ctx, false);
  expect_is_journal_appending(mock_journal, true);
  expect_journal_append_discard_event(
    mock_journal, 0,
    {{32, 32}, {96, 64}, {ictx->layout.object_size - 32, 32}});
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
    ImageArea::DATA, ictx->discard_granularity_bytes, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
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
  MockImageWriteRequest mock_aio_image_write(
    mock_image_ctx, aio_comp, {{0, 1}}, ImageArea::DATA, std::move(bl), 0, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
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
    mock_image_ctx, aio_comp, {{0, 1}}, ImageArea::DATA,
    ictx->discard_granularity_bytes, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
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
  expect_object_request_send(mock_image_ctx, 0);

  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_FLUSH);
  MockImageFlushRequest mock_aio_image_flush(mock_image_ctx, aio_comp,
                                             FLUSH_SOURCE_USER, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
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
  MockImageWriteSameRequest mock_aio_image_writesame(
    mock_image_ctx, aio_comp, {{0, 1}}, ImageArea::DATA, std::move(bl), 0, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
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
  MockImageCompareAndWriteRequest mock_aio_image_write(
    mock_image_ctx, aio_comp, {{0, 1}}, ImageArea::DATA,
    std::move(cmp_bl), std::move(write_bl), &mismatch_offset, 0, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    mock_aio_image_write.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());
}

TEST_F(TestMockIoImageRequest, ListSnaps) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  mock_image_ctx.layout.object_size = 16384;
  mock_image_ctx.layout.stripe_unit = 4096;
  mock_image_ctx.layout.stripe_count = 2;

  InSequence seq;

  SnapshotDelta object_snapshot_delta;
  object_snapshot_delta[{5,6}].insert(
    0, 1024, {SPARSE_EXTENT_STATE_DATA, 1024});
  object_snapshot_delta[{5,5}].insert(
    4096, 4096, {SPARSE_EXTENT_STATE_ZEROED, 4096});
  expect_object_list_snaps_request(mock_image_ctx, 0, object_snapshot_delta, 0);
  object_snapshot_delta = {};
  object_snapshot_delta[{5,6}].insert(
    1024, 3072, {SPARSE_EXTENT_STATE_DATA, 3072});
  object_snapshot_delta[{5,5}].insert(
    2048, 2048, {SPARSE_EXTENT_STATE_ZEROED, 2048});
  expect_object_list_snaps_request(mock_image_ctx, 1, object_snapshot_delta, 0);

  SnapshotDelta snapshot_delta;
  C_SaferCond aio_comp_ctx;
  AioCompletion *aio_comp = AioCompletion::create_and_start(
    &aio_comp_ctx, ictx, AIO_TYPE_GENERIC);
  MockImageListSnapsRequest mock_image_list_snaps_request(
    mock_image_ctx, aio_comp, {{0, 16384}, {16384, 16384}}, ImageArea::DATA,
    {0, CEPH_NOSNAP}, 0, &snapshot_delta, {});
  {
    std::shared_lock owner_locker{mock_image_ctx.owner_lock};
    mock_image_list_snaps_request.send();
  }
  ASSERT_EQ(0, aio_comp_ctx.wait());

  SnapshotDelta expected_snapshot_delta;
  expected_snapshot_delta[{5,6}].insert(
    0, 1024, {SPARSE_EXTENT_STATE_DATA, 1024});
  expected_snapshot_delta[{5,6}].insert(
    5120, 3072, {SPARSE_EXTENT_STATE_DATA, 3072});
  expected_snapshot_delta[{5,5}].insert(
    6144, 6144, {SPARSE_EXTENT_STATE_ZEROED, 6144});
  ASSERT_EQ(expected_snapshot_delta, snapshot_delta);
}

} // namespace io
} // namespace librbd
