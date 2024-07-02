// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "include/rbd/librbd.hpp"
#include "librbd/cache/WriteAroundObjectDispatch.h"
#include "librbd/io/ObjectDispatchSpec.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

struct MockContext : public C_SaferCond  {
  MOCK_METHOD1(complete, void(int));
  MOCK_METHOD1(finish, void(int));

  void do_complete(int r) {
    C_SaferCond::complete(r);
  }
};

} // anonymous namespace
} // namespace librbd

#include "librbd/cache/WriteAroundObjectDispatch.cc"

namespace librbd {
namespace cache {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Invoke;

struct TestMockCacheWriteAroundObjectDispatch : public TestMockFixture {
  typedef WriteAroundObjectDispatch<librbd::MockTestImageCtx> MockWriteAroundObjectDispatch;

  void expect_op_work_queue(MockTestImageCtx& mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.op_work_queue, queue(_, _))
      .WillRepeatedly(Invoke([](Context* ctx, int r) {
                        ctx->complete(r);
                      }));
  }

  void expect_context_complete(MockContext& mock_context, int r) {
    EXPECT_CALL(mock_context, complete(r))
      .WillOnce(Invoke([&mock_context](int r) {
                  mock_context.do_complete(r);
                }));
  }
};

TEST_F(TestMockCacheWriteAroundObjectDispatch, WriteThrough) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockWriteAroundObjectDispatch object_dispatch(&mock_image_ctx, 0, false);

  InSequence seq;

  bufferlist data;
  data.append(std::string(4096, '1'));

  io::DispatchResult dispatch_result;
  MockContext finish_ctx;
  MockContext dispatch_ctx;
  Context* finish_ctx_ptr = &finish_ctx;
  ASSERT_FALSE(object_dispatch.write(0, 0, std::move(data), {}, 0, 0,
                                     std::nullopt, {}, nullptr, nullptr,
                                     &dispatch_result, &finish_ctx_ptr,
                                     &dispatch_ctx));
  ASSERT_EQ(finish_ctx_ptr, &finish_ctx);
}

TEST_F(TestMockCacheWriteAroundObjectDispatch, WriteThroughUntilFlushed) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockWriteAroundObjectDispatch object_dispatch(&mock_image_ctx, 4096, true);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  bufferlist data;
  data.append(std::string(4096, '1'));

  io::DispatchResult dispatch_result;
  MockContext finish_ctx;
  MockContext dispatch_ctx;
  Context* finish_ctx_ptr = &finish_ctx;
  ASSERT_FALSE(object_dispatch.write(0, 0, std::move(data), {}, 0, 0,
                                     std::nullopt, {}, nullptr, nullptr,
                                     &dispatch_result, &finish_ctx_ptr,
                                     &dispatch_ctx));
  ASSERT_EQ(finish_ctx_ptr, &finish_ctx);

  ASSERT_FALSE(object_dispatch.flush(io::FLUSH_SOURCE_USER, {}, nullptr,
                                     &dispatch_result, &finish_ctx_ptr,
                                     &dispatch_ctx));

  expect_context_complete(dispatch_ctx, 0);
  expect_context_complete(finish_ctx, 0);

  ASSERT_TRUE(object_dispatch.write(0, 0, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr,
                                    &dispatch_ctx));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr, &finish_ctx);
  ASSERT_EQ(0, dispatch_ctx.wait());
  ASSERT_EQ(0, finish_ctx.wait());
  finish_ctx_ptr->complete(0);
}

TEST_F(TestMockCacheWriteAroundObjectDispatch, DispatchIO) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockWriteAroundObjectDispatch object_dispatch(&mock_image_ctx, 4096, false);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  bufferlist data;
  data.append(std::string(4096, '1'));

  io::DispatchResult dispatch_result;
  MockContext finish_ctx;
  MockContext dispatch_ctx;
  Context* finish_ctx_ptr = &finish_ctx;

  expect_context_complete(dispatch_ctx, 0);
  expect_context_complete(finish_ctx, 0);

  ASSERT_TRUE(object_dispatch.write(0, 0, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr,
                                    &dispatch_ctx));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr, &finish_ctx);

  ASSERT_EQ(0, dispatch_ctx.wait());
  ASSERT_EQ(0, finish_ctx.wait());
  finish_ctx_ptr->complete(0);
}

TEST_F(TestMockCacheWriteAroundObjectDispatch, BlockedIO) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockWriteAroundObjectDispatch object_dispatch(&mock_image_ctx, 16384, false);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  bufferlist data;
  data.append(std::string(4096, '1'));

  io::DispatchResult dispatch_result;
  MockContext finish_ctx1;
  MockContext dispatch_ctx1;
  Context* finish_ctx_ptr1 = &finish_ctx1;

  expect_context_complete(dispatch_ctx1, 0);
  expect_context_complete(finish_ctx1, 0);

  ASSERT_TRUE(object_dispatch.write(0, 0, std::move(data), {}, 0,  0,
                                    std::nullopt,{}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr1,
                                    &dispatch_ctx1));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr1, &finish_ctx1);

  MockContext finish_ctx2;
  MockContext dispatch_ctx2;
  Context* finish_ctx_ptr2 = &finish_ctx2;

  expect_context_complete(dispatch_ctx2, 0);
  expect_context_complete(finish_ctx2, 0);

  ASSERT_TRUE(object_dispatch.write(0, 4096, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr2,
                                    &dispatch_ctx2));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr2, &finish_ctx2);

  MockContext finish_ctx3;
  MockContext dispatch_ctx3;
  Context* finish_ctx_ptr3 = &finish_ctx3;

  ASSERT_TRUE(object_dispatch.write(0, 1024, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr3,
                                    &dispatch_ctx3));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr3, &finish_ctx3);

  ASSERT_EQ(0, dispatch_ctx1.wait());
  ASSERT_EQ(0, dispatch_ctx2.wait());
  ASSERT_EQ(0, finish_ctx1.wait());
  ASSERT_EQ(0, finish_ctx2.wait());
  finish_ctx_ptr2->complete(0);

  expect_context_complete(dispatch_ctx3, 0);
  expect_context_complete(finish_ctx3, 0);
  finish_ctx_ptr1->complete(0);

  ASSERT_EQ(0, dispatch_ctx3.wait());
  finish_ctx_ptr3->complete(0);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheWriteAroundObjectDispatch, QueuedIO) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockWriteAroundObjectDispatch object_dispatch(&mock_image_ctx, 4095, false);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  bufferlist data;
  data.append(std::string(4096, '1'));

  io::DispatchResult dispatch_result;
  MockContext finish_ctx1;
  MockContext dispatch_ctx1;
  Context* finish_ctx_ptr1 = &finish_ctx1;

  expect_context_complete(dispatch_ctx1, 0);
  expect_context_complete(finish_ctx1, 0);

  ASSERT_TRUE(object_dispatch.write(0, 0, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr1,
                                    &dispatch_ctx1));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr1, &finish_ctx1);

  MockContext finish_ctx2;
  MockContext dispatch_ctx2;
  Context* finish_ctx_ptr2 = &finish_ctx2;

  ASSERT_TRUE(object_dispatch.write(0, 8192, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr2,
                                    &dispatch_ctx2));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr2, &finish_ctx2);
  ASSERT_EQ(0, dispatch_ctx1.wait());

  expect_context_complete(dispatch_ctx2, 0);
  expect_context_complete(finish_ctx2, 0);
  finish_ctx_ptr1->complete(0);

  ASSERT_EQ(0, finish_ctx1.wait());
  ASSERT_EQ(0, dispatch_ctx2.wait());
  ASSERT_EQ(0, finish_ctx2.wait());
  finish_ctx_ptr2->complete(0);
}

TEST_F(TestMockCacheWriteAroundObjectDispatch, BlockedAndQueuedIO) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockWriteAroundObjectDispatch object_dispatch(&mock_image_ctx, 8196, false);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  bufferlist data;
  data.append(std::string(4096, '1'));

  io::DispatchResult dispatch_result;
  MockContext finish_ctx1;
  MockContext dispatch_ctx1;
  Context* finish_ctx_ptr1 = &finish_ctx1;

  expect_context_complete(dispatch_ctx1, 0);
  expect_context_complete(finish_ctx1, 0);

  ASSERT_TRUE(object_dispatch.write(0, 0, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr1,
                                    &dispatch_ctx1));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr1, &finish_ctx1);

  MockContext finish_ctx2;
  MockContext dispatch_ctx2;
  Context* finish_ctx_ptr2 = &finish_ctx2;

  expect_context_complete(dispatch_ctx2, 0);
  expect_context_complete(finish_ctx2, 0);

  ASSERT_TRUE(object_dispatch.write(0, 4096, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr2,
                                    &dispatch_ctx2));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr2, &finish_ctx2);

  MockContext finish_ctx3;
  MockContext dispatch_ctx3;
  Context* finish_ctx_ptr3 = &finish_ctx3;

  ASSERT_TRUE(object_dispatch.write(0, 0, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr3,
                                    &dispatch_ctx3));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr3, &finish_ctx3);

  ASSERT_EQ(0, dispatch_ctx1.wait());
  ASSERT_EQ(0, dispatch_ctx2.wait());
  ASSERT_EQ(0, finish_ctx1.wait());
  ASSERT_EQ(0, finish_ctx2.wait());
  finish_ctx_ptr2->complete(0);

  expect_context_complete(dispatch_ctx3, 0);
  expect_context_complete(finish_ctx3, 0);
  finish_ctx_ptr1->complete(0);

  ASSERT_EQ(0, dispatch_ctx3.wait());
  ASSERT_EQ(0, finish_ctx3.wait());
  finish_ctx_ptr3->complete(0);
}

TEST_F(TestMockCacheWriteAroundObjectDispatch, Flush) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockWriteAroundObjectDispatch object_dispatch(&mock_image_ctx, 4096, true);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  io::DispatchResult dispatch_result;
  MockContext finish_ctx;
  MockContext dispatch_ctx;
  Context* finish_ctx_ptr = &finish_ctx;
  ASSERT_FALSE(object_dispatch.flush(io::FLUSH_SOURCE_USER, {}, nullptr,
                                     &dispatch_result, &finish_ctx_ptr,
                                     &dispatch_ctx));
  ASSERT_EQ(finish_ctx_ptr, &finish_ctx);
}

TEST_F(TestMockCacheWriteAroundObjectDispatch, FlushQueuedOnInFlightIO) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockWriteAroundObjectDispatch object_dispatch(&mock_image_ctx, 4096, false);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  bufferlist data;
  data.append(std::string(4096, '1'));

  io::DispatchResult dispatch_result;
  MockContext finish_ctx1;
  MockContext dispatch_ctx1;
  Context* finish_ctx_ptr1 = &finish_ctx1;

  expect_context_complete(dispatch_ctx1, 0);
  expect_context_complete(finish_ctx1, 0);

  ASSERT_TRUE(object_dispatch.write(0, 0, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr1,
                                    &dispatch_ctx1));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr1, &finish_ctx1);
  ASSERT_EQ(0, dispatch_ctx1.wait());

  MockContext finish_ctx2;
  MockContext dispatch_ctx2;
  Context* finish_ctx_ptr2 = &finish_ctx2;
  ASSERT_FALSE(object_dispatch.flush(io::FLUSH_SOURCE_USER, {}, nullptr,
                                     &dispatch_result, &finish_ctx_ptr2,
                                     &dispatch_ctx2));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr2, &finish_ctx2);

  expect_context_complete(finish_ctx2, 0);
  finish_ctx_ptr1->complete(0);
  ASSERT_EQ(0, finish_ctx1.wait());

  finish_ctx_ptr2->complete(0);
  ASSERT_EQ(0, finish_ctx2.wait());
}

TEST_F(TestMockCacheWriteAroundObjectDispatch, FlushQueuedOnQueuedIO) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockWriteAroundObjectDispatch object_dispatch(&mock_image_ctx, 4096, false);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  bufferlist data;
  data.append(std::string(4096, '1'));

  io::DispatchResult dispatch_result;
  MockContext finish_ctx1;
  MockContext dispatch_ctx1;
  Context* finish_ctx_ptr1 = &finish_ctx1;

  expect_context_complete(dispatch_ctx1, 0);
  expect_context_complete(finish_ctx1, 0);

  ASSERT_TRUE(object_dispatch.write(0, 0, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr1,
                                    &dispatch_ctx1));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr1, &finish_ctx1);
  ASSERT_EQ(0, dispatch_ctx1.wait());

  MockContext finish_ctx2;
  MockContext dispatch_ctx2;
  Context* finish_ctx_ptr2 = &finish_ctx2;

  ASSERT_TRUE(object_dispatch.write(0, 8192, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr2,
                                    &dispatch_ctx2));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr2, &finish_ctx2);
  ASSERT_EQ(0, dispatch_ctx1.wait());

  MockContext finish_ctx3;
  MockContext dispatch_ctx3;
  Context* finish_ctx_ptr3 = &finish_ctx3;
  ASSERT_TRUE(object_dispatch.flush(io::FLUSH_SOURCE_USER, {}, nullptr,
                                    &dispatch_result, &finish_ctx_ptr3,
                                    &dispatch_ctx3));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr3, &finish_ctx3);

  expect_context_complete(dispatch_ctx2, 0);
  expect_context_complete(finish_ctx2, 0);
  expect_context_complete(dispatch_ctx3, 0);
  finish_ctx_ptr1->complete(0);

  ASSERT_EQ(0, finish_ctx1.wait());
  ASSERT_EQ(0, dispatch_ctx2.wait());
  ASSERT_EQ(0, finish_ctx2.wait());

  expect_context_complete(finish_ctx3, 0);
  finish_ctx_ptr2->complete(0);

  finish_ctx_ptr3->complete(0);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheWriteAroundObjectDispatch, FlushError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockWriteAroundObjectDispatch object_dispatch(&mock_image_ctx, 4096, false);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  bufferlist data;
  data.append(std::string(4096, '1'));

  io::DispatchResult dispatch_result;
  MockContext finish_ctx1;
  MockContext dispatch_ctx1;
  Context* finish_ctx_ptr1 = &finish_ctx1;

  expect_context_complete(dispatch_ctx1, 0);
  expect_context_complete(finish_ctx1, 0);

  ASSERT_TRUE(object_dispatch.write(0, 0, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr1,
                                    &dispatch_ctx1));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr1, &finish_ctx1);
  ASSERT_EQ(0, dispatch_ctx1.wait());
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContext finish_ctx2;
  MockContext dispatch_ctx2;
  Context* finish_ctx_ptr2 = &finish_ctx2;
  ASSERT_FALSE(object_dispatch.flush(io::FLUSH_SOURCE_USER, {}, nullptr,
                                     &dispatch_result, &finish_ctx_ptr2,
                                     &dispatch_ctx2));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr2, &finish_ctx2);

  expect_context_complete(finish_ctx2, -EPERM);
  finish_ctx_ptr1->complete(-EPERM);
  finish_ctx_ptr2->complete(0);
  ASSERT_EQ(-EPERM, finish_ctx2.wait());
}

TEST_F(TestMockCacheWriteAroundObjectDispatch, UnoptimizedIO) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockWriteAroundObjectDispatch object_dispatch(&mock_image_ctx, 16384, false);

  InSequence seq;

  bufferlist data;
  data.append(std::string(4096, '1'));

  io::DispatchResult dispatch_result;
  MockContext finish_ctx;
  MockContext dispatch_ctx;
  Context* finish_ctx_ptr = &finish_ctx;

  ASSERT_FALSE(object_dispatch.compare_and_write(0, 0, std::move(data),
                                                 std::move(data), {}, 0, {},
                                                 nullptr, nullptr, nullptr,
                                                 &dispatch_result,
                                                 &finish_ctx_ptr,
                                                 &dispatch_ctx));
  ASSERT_EQ(finish_ctx_ptr, &finish_ctx);
}

TEST_F(TestMockCacheWriteAroundObjectDispatch, UnoptimizedIOInFlightIO) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockWriteAroundObjectDispatch object_dispatch(&mock_image_ctx, 16384, false);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  bufferlist data;
  data.append(std::string(4096, '1'));

  io::DispatchResult dispatch_result;
  MockContext finish_ctx1;
  MockContext dispatch_ctx1;
  Context* finish_ctx_ptr1 = &finish_ctx1;

  expect_context_complete(dispatch_ctx1, 0);
  expect_context_complete(finish_ctx1, 0);

  ASSERT_TRUE(object_dispatch.write(0, 0, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr1,
                                    &dispatch_ctx1));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr1, &finish_ctx1);
  ASSERT_EQ(0, dispatch_ctx1.wait());
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContext finish_ctx2;
  MockContext dispatch_ctx2;
  Context* finish_ctx_ptr2 = &finish_ctx2;
  ASSERT_TRUE(object_dispatch.compare_and_write(0, 0, std::move(data),
                                                std::move(data), {}, 0, {},
                                                nullptr, nullptr, nullptr,
                                                &dispatch_result,
                                                &finish_ctx_ptr2,
                                                &dispatch_ctx2));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_EQ(finish_ctx_ptr2, &finish_ctx2);

  expect_context_complete(dispatch_ctx2, 0);
  finish_ctx_ptr1->complete(0);
  ASSERT_EQ(0, dispatch_ctx2.wait());
}

TEST_F(TestMockCacheWriteAroundObjectDispatch, UnoptimizedIOBlockedIO) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockWriteAroundObjectDispatch object_dispatch(&mock_image_ctx, 4096, false);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;

  bufferlist data;
  data.append(std::string(4096, '1'));

  io::DispatchResult dispatch_result;
  MockContext finish_ctx1;
  MockContext dispatch_ctx1;
  Context* finish_ctx_ptr1 = &finish_ctx1;

  expect_context_complete(dispatch_ctx1, 0);
  expect_context_complete(finish_ctx1, 0);

  ASSERT_TRUE(object_dispatch.write(0, 0, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr1,
                                    &dispatch_ctx1));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr1, &finish_ctx1);
  ASSERT_EQ(0, dispatch_ctx1.wait());
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContext finish_ctx2;
  MockContext dispatch_ctx2;
  Context* finish_ctx_ptr2 = &finish_ctx2;
  ASSERT_TRUE(object_dispatch.write(0, 4096, std::move(data), {}, 0, 0,
                                    std::nullopt, {}, nullptr, nullptr,
                                    &dispatch_result, &finish_ctx_ptr2,
                                    &dispatch_ctx2));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_NE(finish_ctx_ptr2, &finish_ctx2);

  MockContext finish_ctx3;
  MockContext dispatch_ctx3;
  Context* finish_ctx_ptr3 = &finish_ctx3;
  ASSERT_TRUE(object_dispatch.compare_and_write(0, 0, std::move(data),
                                                std::move(data), {}, 0, {},
                                                nullptr, nullptr, nullptr,
                                                &dispatch_result,
                                                &finish_ctx_ptr3,
                                                &dispatch_ctx3));
  ASSERT_EQ(io::DISPATCH_RESULT_CONTINUE, dispatch_result);
  ASSERT_EQ(finish_ctx_ptr3, &finish_ctx3);

  expect_context_complete(dispatch_ctx3, 0);
  expect_context_complete(dispatch_ctx2, 0);
  expect_context_complete(finish_ctx2, 0);
  finish_ctx_ptr1->complete(0);
  ASSERT_EQ(0, dispatch_ctx3.wait());
  ASSERT_EQ(0, dispatch_ctx2.wait());
  ASSERT_EQ(0, finish_ctx2.wait());
  finish_ctx_ptr2->complete(0);
}

TEST_F(TestMockCacheWriteAroundObjectDispatch, WriteFUA) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockWriteAroundObjectDispatch object_dispatch(&mock_image_ctx, 16384, false);

  InSequence seq;

  bufferlist data;
  data.append(std::string(4096, '1'));

  io::DispatchResult dispatch_result;
  MockContext finish_ctx;
  MockContext dispatch_ctx;
  Context* finish_ctx_ptr = &finish_ctx;
  ASSERT_FALSE(object_dispatch.write(0, 0, std::move(data), {},
                                     LIBRADOS_OP_FLAG_FADVISE_FUA, 0,
                                     std::nullopt, {}, nullptr, nullptr,
                                     &dispatch_result, &finish_ctx_ptr,
                                     &dispatch_ctx));
  ASSERT_EQ(finish_ctx_ptr, &finish_ctx);
}

TEST_F(TestMockCacheWriteAroundObjectDispatch, WriteSameFUA) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockWriteAroundObjectDispatch object_dispatch(&mock_image_ctx, 16384, false);

  InSequence seq;

  bufferlist data;
  data.append(std::string(512, '1'));

  io::DispatchResult dispatch_result;
  MockContext finish_ctx;
  MockContext dispatch_ctx;
  Context* finish_ctx_ptr = &finish_ctx;
  ASSERT_FALSE(object_dispatch.write_same(0, 0, 8192, {{0, 8192}},
                                          std::move(data), {},
                                          LIBRADOS_OP_FLAG_FADVISE_FUA, {},
                                          nullptr, nullptr, &dispatch_result,
                                          &finish_ctx_ptr, &dispatch_ctx));
  ASSERT_EQ(finish_ctx_ptr, &finish_ctx);
}

} // namespace cache
} // namespace librbd
