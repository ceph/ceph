// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockSafeTimer.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "include/rbd/librbd.hpp"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/SimpleSchedulerObjectDispatch.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace io {

template <>
struct TypeTraits<MockTestImageCtx> {
  typedef ::MockSafeTimer SafeTimer;
};

template <>
struct FlushTracker<MockTestImageCtx> {
  FlushTracker(MockTestImageCtx*) {
  }

  void shut_down() {
  }

  void flush(Context*) {
  }

  void start_io(uint64_t) {
  }

  void finish_io(uint64_t) {
  }

};

} // namespace io
} // namespace librbd

#include "librbd/io/SimpleSchedulerObjectDispatch.cc"

namespace librbd {
namespace io {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;

struct TestMockIoSimpleSchedulerObjectDispatch : public TestMockFixture {
  typedef SimpleSchedulerObjectDispatch<librbd::MockTestImageCtx> MockSimpleSchedulerObjectDispatch;

  MockSafeTimer m_mock_timer;
  ceph::mutex m_mock_timer_lock =
    ceph::make_mutex("TestMockIoSimpleSchedulerObjectDispatch::Mutex");

  TestMockIoSimpleSchedulerObjectDispatch() {
    MockTestImageCtx::set_timer_instance(&m_mock_timer, &m_mock_timer_lock);
    EXPECT_EQ(0, _rados.conf_set("rbd_io_scheduler_simple_max_delay", "1"));
  }

  void expect_get_object_name(MockTestImageCtx &mock_image_ctx,
                              uint64_t object_no) {
    EXPECT_CALL(mock_image_ctx, get_object_name(object_no))
      .WillRepeatedly(Return(
          mock_image_ctx.image_ctx->get_object_name(object_no)));
  }

  void expect_dispatch_delayed_requests(MockTestImageCtx &mock_image_ctx,
                                        int r) {
    EXPECT_CALL(*mock_image_ctx.io_object_dispatcher, send(_))
      .WillOnce(Invoke([&mock_image_ctx, r](ObjectDispatchSpec* spec) {
                  spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
                  mock_image_ctx.image_ctx->op_work_queue->queue(
                      &spec->dispatcher_ctx, r);
                }));
  }

  void expect_cancel_timer_task(Context *timer_task) {
      EXPECT_CALL(m_mock_timer, cancel_event(timer_task))
        .WillOnce(Invoke([](Context *timer_task) {
                    delete timer_task;
                    return true;
                  }));
  }

  void expect_add_timer_task(Context **timer_task) {
    EXPECT_CALL(m_mock_timer, add_event_at(_, _))
      .WillOnce(Invoke([timer_task](ceph::real_clock::time_point, Context *task) {
                  *timer_task = task;
                  return task;
                }));
  }

  void expect_schedule_dispatch_delayed_requests(Context *current_task,
                                                 Context **new_task) {
    if (current_task != nullptr) {
      expect_cancel_timer_task(current_task);
    }
    if (new_task != nullptr) {
      expect_add_timer_task(new_task);
    }
  }

  void run_timer_task(Context *timer_task) {
    std::lock_guard timer_locker{m_mock_timer_lock};
    timer_task->complete(0);
  }
};

TEST_F(TestMockIoSimpleSchedulerObjectDispatch, Read) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockSimpleSchedulerObjectDispatch
      mock_simple_scheduler_object_dispatch(&mock_image_ctx);

  C_SaferCond cond;
  Context *on_finish = &cond;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.read(
      0, 0, 4096, CEPH_NOSNAP, 0, {}, nullptr, nullptr, nullptr, nullptr,
      &on_finish, nullptr));
  ASSERT_EQ(on_finish, &cond); // not modified
  on_finish->complete(0);
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestMockIoSimpleSchedulerObjectDispatch, Discard) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockSimpleSchedulerObjectDispatch
      mock_simple_scheduler_object_dispatch(&mock_image_ctx);

  C_SaferCond cond;
  Context *on_finish = &cond;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.discard(
      0, 0, 4096, mock_image_ctx.snapc, 0, {}, nullptr, nullptr, nullptr,
      &on_finish, nullptr));
  ASSERT_NE(on_finish, &cond);
  on_finish->complete(0);
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestMockIoSimpleSchedulerObjectDispatch, Write) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockSimpleSchedulerObjectDispatch
      mock_simple_scheduler_object_dispatch(&mock_image_ctx);

  ceph::bufferlist data;
  data.append("X");
  int object_dispatch_flags = 0;
  C_SaferCond cond;
  Context *on_finish = &cond;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.write(
      0, 0, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, nullptr, &on_finish, nullptr));
  ASSERT_NE(on_finish, &cond);
  on_finish->complete(0);
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestMockIoSimpleSchedulerObjectDispatch, WriteSame) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockSimpleSchedulerObjectDispatch
      mock_simple_scheduler_object_dispatch(&mock_image_ctx);

  io::LightweightBufferExtents buffer_extents;
  ceph::bufferlist data;
  C_SaferCond cond;
  Context *on_finish = &cond;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.write_same(
      0, 0, 4096, std::move(buffer_extents), std::move(data),
      mock_image_ctx.snapc, 0, {}, nullptr, nullptr, nullptr, &on_finish,
      nullptr));
  ASSERT_NE(on_finish, &cond);
  on_finish->complete(0);
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestMockIoSimpleSchedulerObjectDispatch, CompareAndWrite) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockSimpleSchedulerObjectDispatch
      mock_simple_scheduler_object_dispatch(&mock_image_ctx);

  ceph::bufferlist cmp_data;
  ceph::bufferlist write_data;
  C_SaferCond cond;
  Context *on_finish = &cond;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.compare_and_write(
      0, 0, std::move(cmp_data), std::move(write_data), mock_image_ctx.snapc, 0,
      {}, nullptr, nullptr, nullptr, nullptr, &on_finish, nullptr));
  ASSERT_NE(on_finish, &cond);
  on_finish->complete(0);
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestMockIoSimpleSchedulerObjectDispatch, Flush) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockSimpleSchedulerObjectDispatch
      mock_simple_scheduler_object_dispatch(&mock_image_ctx);

  io::DispatchResult dispatch_result;
  C_SaferCond cond;
  Context *on_finish = &cond;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.flush(
      FLUSH_SOURCE_USER, {}, nullptr, &dispatch_result, &on_finish, nullptr));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_EQ(on_finish, &cond); // not modified
  on_finish->complete(0);
  ASSERT_EQ(0, cond.wait());
}

TEST_F(TestMockIoSimpleSchedulerObjectDispatch, WriteDelayed) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockSimpleSchedulerObjectDispatch
      mock_simple_scheduler_object_dispatch(&mock_image_ctx);

  expect_get_object_name(mock_image_ctx, 0);

  InSequence seq;

  ceph::bufferlist data;
  data.append("X");
  int object_dispatch_flags = 0;
  C_SaferCond cond1;
  Context *on_finish1 = &cond1;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.write(
      0, 0, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, nullptr, &on_finish1, nullptr));
  ASSERT_NE(on_finish1, &cond1);

  Context *timer_task = nullptr;
  expect_schedule_dispatch_delayed_requests(nullptr, &timer_task);

  io::DispatchResult dispatch_result;
  C_SaferCond cond2;
  Context *on_finish2 = &cond2;
  C_SaferCond on_dispatched;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.write(
      0, 0, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish2,
      &on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_NE(on_finish2, &cond2);
  ASSERT_NE(timer_task, nullptr);

  expect_dispatch_delayed_requests(mock_image_ctx, 0);
  expect_schedule_dispatch_delayed_requests(timer_task, nullptr);

  on_finish1->complete(0);
  ASSERT_EQ(0, cond1.wait());
  ASSERT_EQ(0, on_dispatched.wait());
  on_finish2->complete(0);
  ASSERT_EQ(0, cond2.wait());
}

TEST_F(TestMockIoSimpleSchedulerObjectDispatch, WriteDelayedFlush) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockSimpleSchedulerObjectDispatch
      mock_simple_scheduler_object_dispatch(&mock_image_ctx);

  expect_get_object_name(mock_image_ctx, 0);

  InSequence seq;

  ceph::bufferlist data;
  data.append("X");
  int object_dispatch_flags = 0;
  C_SaferCond cond1;
  Context *on_finish1 = &cond1;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.write(
      0, 0, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, nullptr, &on_finish1, nullptr));
  ASSERT_NE(on_finish1, &cond1);

  Context *timer_task = nullptr;
  expect_schedule_dispatch_delayed_requests(nullptr, &timer_task);

  io::DispatchResult dispatch_result;
  C_SaferCond cond2;
  Context *on_finish2 = &cond2;
  C_SaferCond on_dispatched;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.write(
      0, 0, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish2,
      &on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_NE(on_finish2, &cond2);
  ASSERT_NE(timer_task, nullptr);

  expect_dispatch_delayed_requests(mock_image_ctx, 0);
  expect_schedule_dispatch_delayed_requests(timer_task, nullptr);

  C_SaferCond cond3;
  Context *on_finish3 = &cond3;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.flush(
      FLUSH_SOURCE_USER, {}, nullptr, &dispatch_result, &on_finish3, nullptr));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_EQ(on_finish3, &cond3);

  on_finish1->complete(0);
  ASSERT_EQ(0, cond1.wait());
  ASSERT_EQ(0, on_dispatched.wait());
  on_finish2->complete(0);
  ASSERT_EQ(0, cond2.wait());
  on_finish3->complete(0);
  ASSERT_EQ(0, cond3.wait());
}

TEST_F(TestMockIoSimpleSchedulerObjectDispatch, WriteMerged) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockSimpleSchedulerObjectDispatch
      mock_simple_scheduler_object_dispatch(&mock_image_ctx);

  expect_get_object_name(mock_image_ctx, 0);

  InSequence seq;

  ceph::bufferlist data;
  data.append("X");
  int object_dispatch_flags = 0;
  C_SaferCond cond1;
  Context *on_finish1 = &cond1;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.write(
      0, 0, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, nullptr, &on_finish1, nullptr));
  ASSERT_NE(on_finish1, &cond1);

  Context *timer_task = nullptr;
  expect_schedule_dispatch_delayed_requests(nullptr, &timer_task);

  uint64_t object_off = 20;
  data.clear();
  data.append(std::string(10, 'A'));
  io::DispatchResult dispatch_result;
  C_SaferCond cond2;
  Context *on_finish2 = &cond2;
  C_SaferCond on_dispatched2;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.write(
      0, object_off, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish2,
      &on_dispatched2));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_NE(on_finish2, &cond2);
  ASSERT_NE(timer_task, nullptr);

  object_off = 0;
  data.clear();
  data.append(std::string(10, 'B'));
  C_SaferCond cond3;
  Context *on_finish3 = &cond3;
  C_SaferCond on_dispatched3;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.write(
      0, object_off, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish3,
      &on_dispatched3));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_NE(on_finish3, &cond3);

  object_off = 10;
  data.clear();
  data.append(std::string(10, 'C'));
  C_SaferCond cond4;
  Context *on_finish4 = &cond4;
  C_SaferCond on_dispatched4;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.write(
      0, object_off, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish4,
      &on_dispatched4));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_NE(on_finish4, &cond4);

  object_off = 30;
  data.clear();
  data.append(std::string(10, 'D'));
  C_SaferCond cond5;
  Context *on_finish5 = &cond5;
  C_SaferCond on_dispatched5;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.write(
      0, object_off, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish5,
      &on_dispatched5));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_NE(on_finish5, &cond5);

  object_off = 50;
  data.clear();
  data.append(std::string(10, 'E'));
  C_SaferCond cond6;
  Context *on_finish6 = &cond6;
  C_SaferCond on_dispatched6;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.write(
      0, object_off, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish6,
      &on_dispatched6));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_NE(on_finish6, &cond6);

  // expect two requests dispatched:
  // 0~40 (merged 0~10, 10~10, 20~10, 30~10) and 50~10
  expect_dispatch_delayed_requests(mock_image_ctx, 0);
  expect_dispatch_delayed_requests(mock_image_ctx, 0);
  expect_schedule_dispatch_delayed_requests(timer_task, nullptr);

  on_finish1->complete(0);
  ASSERT_EQ(0, cond1.wait());
  ASSERT_EQ(0, on_dispatched2.wait());
  ASSERT_EQ(0, on_dispatched3.wait());
  ASSERT_EQ(0, on_dispatched4.wait());
  ASSERT_EQ(0, on_dispatched5.wait());
  ASSERT_EQ(0, on_dispatched6.wait());
  on_finish2->complete(0);
  on_finish3->complete(0);
  on_finish4->complete(0);
  on_finish5->complete(0);
  on_finish6->complete(0);
  ASSERT_EQ(0, cond2.wait());
  ASSERT_EQ(0, cond3.wait());
  ASSERT_EQ(0, cond4.wait());
  ASSERT_EQ(0, cond5.wait());
  ASSERT_EQ(0, cond6.wait());
}

TEST_F(TestMockIoSimpleSchedulerObjectDispatch, WriteNonSequential) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockSimpleSchedulerObjectDispatch
      mock_simple_scheduler_object_dispatch(&mock_image_ctx);

  expect_get_object_name(mock_image_ctx, 0);

  InSequence seq;

  ceph::bufferlist data;
  int object_dispatch_flags = 0;
  C_SaferCond cond1;
  Context *on_finish1 = &cond1;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.write(
      0, 0, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, nullptr, &on_finish1, nullptr));
  ASSERT_NE(on_finish1, &cond1);

  Context *timer_task = nullptr;
  expect_schedule_dispatch_delayed_requests(nullptr, &timer_task);

  uint64_t object_off = 0;
  data.clear();
  data.append(std::string(10, 'X'));
  io::DispatchResult dispatch_result;
  C_SaferCond cond2;
  Context *on_finish2 = &cond2;
  C_SaferCond on_dispatched2;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.write(
      0, object_off, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish2,
      &on_dispatched2));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_NE(on_finish2, &cond2);
  ASSERT_NE(timer_task, nullptr);

  expect_dispatch_delayed_requests(mock_image_ctx, 0);
  expect_schedule_dispatch_delayed_requests(timer_task, nullptr);

  object_off = 5;
  data.clear();
  data.append(std::string(10, 'Y'));
  C_SaferCond cond3;
  Context *on_finish3 = &cond3;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.write(
      0, object_off, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish3, nullptr));
  ASSERT_NE(on_finish3, &cond3);

  on_finish1->complete(0);
  ASSERT_EQ(0, cond1.wait());
  ASSERT_EQ(0, on_dispatched2.wait());
  on_finish2->complete(0);
  ASSERT_EQ(0, cond2.wait());
  on_finish3->complete(0);
  ASSERT_EQ(0, cond3.wait());
}

TEST_F(TestMockIoSimpleSchedulerObjectDispatch, Mixed) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockSimpleSchedulerObjectDispatch
      mock_simple_scheduler_object_dispatch(&mock_image_ctx);

  expect_get_object_name(mock_image_ctx, 0);

  InSequence seq;

  // write (1) 0~0 (in-flight)
  // will wrap on_finish with dispatch_seq=1 to dispatch future delayed writes
  ceph::bufferlist data;
  int object_dispatch_flags = 0;
  C_SaferCond cond1;
  Context *on_finish1 = &cond1;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.write(
      0, 0, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, nullptr, &on_finish1, nullptr));
  ASSERT_NE(on_finish1, &cond1);

  // write (2) 0~10 (delayed)
  // will wait for write (1) to finish or a non-seq io comes
  Context *timer_task = nullptr;
  expect_schedule_dispatch_delayed_requests(nullptr, &timer_task);
  uint64_t object_off = 0;
  data.clear();
  data.append(std::string(10, 'A'));
  io::DispatchResult dispatch_result;
  C_SaferCond cond2;
  Context *on_finish2 = &cond2;
  C_SaferCond on_dispatched2;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.write(
      0, object_off, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish2,
      &on_dispatched2));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_NE(on_finish2, &cond2);
  ASSERT_NE(timer_task, nullptr);

  // write (3) 10~10 (delayed)
  // will be merged with write (2)
  object_off = 10;
  data.clear();
  data.append(std::string(10, 'B'));
  C_SaferCond cond3;
  Context *on_finish3 = &cond3;
  C_SaferCond on_dispatched3;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.write(
      0, object_off, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish3,
      &on_dispatched3));
  ASSERT_NE(on_finish3, &cond3);

  // discard (1) (non-seq io)
  // will dispatch the delayed writes (2) and (3) and wrap on_finish
  // with dispatch_seq=2 to dispatch future delayed writes
  expect_dispatch_delayed_requests(mock_image_ctx, 0);
  expect_schedule_dispatch_delayed_requests(timer_task, nullptr);
  C_SaferCond cond4;
  Context *on_finish4 = &cond4;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.discard(
      0, 4096, 4096, mock_image_ctx.snapc, 0, {}, nullptr, nullptr, nullptr,
      &on_finish4, nullptr));
  ASSERT_NE(on_finish4, &cond4);
  ASSERT_EQ(0, on_dispatched2.wait());
  ASSERT_EQ(0, on_dispatched3.wait());

  // write (4) 20~10 (delayed)
  // will wait for discard (1) to finish or a non-seq io comes
  timer_task = nullptr;
  expect_schedule_dispatch_delayed_requests(nullptr, &timer_task);
  object_off = 20;
  data.clear();
  data.append(std::string(10, 'C'));
  C_SaferCond cond5;
  Context *on_finish5 = &cond5;
  C_SaferCond on_dispatched5;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.write(
      0, object_off, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish5,
      &on_dispatched5));
  ASSERT_NE(on_finish5, &cond5);
  ASSERT_NE(timer_task, nullptr);

  // discard (2) (non-seq io)
  // will dispatch the delayed write (4) and wrap on_finish with dispatch_seq=3
  // to dispatch future delayed writes
  expect_dispatch_delayed_requests(mock_image_ctx, 0);
  expect_schedule_dispatch_delayed_requests(timer_task, nullptr);
  C_SaferCond cond6;
  Context *on_finish6 = &cond6;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.discard(
      0, 4096, 4096, mock_image_ctx.snapc, 0, {}, nullptr, nullptr, nullptr,
      &on_finish6, nullptr));
  ASSERT_NE(on_finish6, &cond6);
  ASSERT_EQ(0, on_dispatched5.wait());

  // write (5) 30~10 (delayed)
  // will wait for discard (2) to finish or a non-seq io comes
  timer_task = nullptr;
  expect_schedule_dispatch_delayed_requests(nullptr, &timer_task);
  object_off = 30;
  data.clear();
  data.append(std::string(10, 'D'));
  C_SaferCond cond7;
  Context *on_finish7 = &cond7;
  C_SaferCond on_dispatched7;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.write(
      0, object_off, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish7,
      &on_dispatched7));
  ASSERT_NE(on_finish7, &cond7);
  ASSERT_NE(timer_task, nullptr);

  // write (1) finishes
  // on_finish wrapper will skip dispatch delayed write (5)
  // due to dispatch_seq(1) < m_dispatch_seq(3)
  on_finish1->complete(0);
  ASSERT_EQ(0, cond1.wait());

  // writes (2) and (3) finish ("dispatch delayed" is not called)
  on_finish2->complete(0);
  on_finish3->complete(0);
  ASSERT_EQ(0, cond2.wait());
  ASSERT_EQ(0, cond3.wait());

  // discard (1) finishes
  // on_finish wrapper will skip dispatch delayed write (5)
  // due to dispatch_seq(2) < m_dispatch_seq(3)
  on_finish4->complete(0);
  ASSERT_EQ(0, cond4.wait());

  // writes (4) finishes ("dispatch delayed" is not called)
  on_finish5->complete(0);
  ASSERT_EQ(0, cond5.wait());

  // discard (2) finishes
  // on_finish wrapper will dispatch the delayed write (5)
  // due to dispatch_seq(3) == m_dispatch_seq(3)
  expect_dispatch_delayed_requests(mock_image_ctx, 0);
  expect_schedule_dispatch_delayed_requests(timer_task, nullptr);
  on_finish6->complete(0);
  ASSERT_EQ(0, cond6.wait());
  ASSERT_EQ(0, on_dispatched7.wait());

  // write (5) finishes ("dispatch delayed" is not called)
  on_finish7->complete(0);
  ASSERT_EQ(0, cond7.wait());
}

TEST_F(TestMockIoSimpleSchedulerObjectDispatch, DispatchQueue) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockSimpleSchedulerObjectDispatch
      mock_simple_scheduler_object_dispatch(&mock_image_ctx);

  expect_get_object_name(mock_image_ctx, 0);
  expect_get_object_name(mock_image_ctx, 1);

  InSequence seq;

  // send 2 writes to object 0

  uint64_t object_no = 0;
  ceph::bufferlist data;
  int object_dispatch_flags = 0;
  C_SaferCond cond1;
  Context *on_finish1 = &cond1;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.write(
      object_no, 0, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, nullptr, &on_finish1, nullptr));
  ASSERT_NE(on_finish1, &cond1);

  Context *timer_task = nullptr;
  expect_schedule_dispatch_delayed_requests(nullptr, &timer_task);

  data.clear();
  io::DispatchResult dispatch_result;
  C_SaferCond cond2;
  Context *on_finish2 = &cond2;
  C_SaferCond on_dispatched2;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.write(
      object_no, 0, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish2,
      &on_dispatched2));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_NE(on_finish2, &cond2);
  ASSERT_NE(timer_task, nullptr);

  // send 2 writes to object 1

  object_no = 1;
  data.clear();
  C_SaferCond cond3;
  Context *on_finish3 = &cond3;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.write(
      object_no, 0, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, nullptr, &on_finish3, nullptr));
  ASSERT_NE(on_finish3, &cond3);

  data.clear();
  C_SaferCond cond4;
  Context *on_finish4 = &cond4;
  C_SaferCond on_dispatched4;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.write(
      object_no, 0, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish4,
      &on_dispatched4));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_NE(on_finish4, &cond4);

  // finish write (1) to object 0
  expect_dispatch_delayed_requests(mock_image_ctx, 0);
  expect_schedule_dispatch_delayed_requests(timer_task, &timer_task);
  on_finish1->complete(0);
  ASSERT_EQ(0, cond1.wait());
  ASSERT_EQ(0, on_dispatched2.wait());

  // finish write (2) to object 0
  on_finish2->complete(0);
  ASSERT_EQ(0, cond2.wait());

  // finish write (1) to object 1
  expect_dispatch_delayed_requests(mock_image_ctx, 0);
  expect_schedule_dispatch_delayed_requests(timer_task, nullptr);
  on_finish3->complete(0);
  ASSERT_EQ(0, cond3.wait());
  ASSERT_EQ(0, on_dispatched4.wait());

  // finish write (2) to object 1
  on_finish4->complete(0);
  ASSERT_EQ(0, cond4.wait());
}

TEST_F(TestMockIoSimpleSchedulerObjectDispatch, Timer) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockSimpleSchedulerObjectDispatch
      mock_simple_scheduler_object_dispatch(&mock_image_ctx);

  expect_get_object_name(mock_image_ctx, 0);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  ceph::bufferlist data;
  int object_dispatch_flags = 0;
  C_SaferCond cond1;
  Context *on_finish1 = &cond1;
  ASSERT_FALSE(mock_simple_scheduler_object_dispatch.write(
      0, 0, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, nullptr, &on_finish1, nullptr));
  ASSERT_NE(on_finish1, &cond1);

  Context *timer_task = nullptr;
  expect_schedule_dispatch_delayed_requests(nullptr, &timer_task);

  data.clear();
  io::DispatchResult dispatch_result;
  C_SaferCond cond2;
  Context *on_finish2 = &cond2;
  C_SaferCond on_dispatched;
  ASSERT_TRUE(mock_simple_scheduler_object_dispatch.write(
      0, 0, std::move(data), mock_image_ctx.snapc, 0, {},
      &object_dispatch_flags, nullptr, &dispatch_result, &on_finish2,
      &on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_NE(on_finish2, &cond2);
  ASSERT_NE(timer_task, nullptr);

  expect_dispatch_delayed_requests(mock_image_ctx, 0);

  run_timer_task(timer_task);
  ASSERT_EQ(0, on_dispatched.wait());

  on_finish1->complete(0);
  ASSERT_EQ(0, cond1.wait());
  on_finish2->complete(0);
  ASSERT_EQ(0, cond2.wait());
}

} // namespace io
} // namespace librbd
