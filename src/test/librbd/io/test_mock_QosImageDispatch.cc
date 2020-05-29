// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/exclusive_lock/MockPolicy.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/io/ImageRequest.h"

namespace librbd {
namespace io {

TEST_F(TestMockIoImageRequestWQ, QosNoLimit) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  MockImageDispatchSpec mock_queued_image_request;
  expect_was_throttled(mock_queued_image_request, false);
  expect_set_throttled(mock_queued_image_request);

  InSequence seq;
  MockImageRequestWQ mock_image_request_wq(&mock_image_ctx, "io", 60, nullptr);

  mock_image_request_wq.apply_qos_limit(IMAGE_DISPATCH_FLAG_QOS_BPS_THROTTLE, 0,
                                        0, 1);

  expect_front(mock_image_request_wq, &mock_queued_image_request);
  expect_is_refresh_request(mock_image_ctx, false);
  expect_is_write_op(mock_queued_image_request, true);
  expect_dequeue(mock_image_request_wq, &mock_queued_image_request);
  ASSERT_TRUE(mock_image_request_wq.invoke_dequeue() == &mock_queued_image_request);
}

TEST_F(TestMockIoImageRequestWQ, BPSQosNoBurst) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  MockImageDispatchSpec mock_queued_image_request;
  expect_was_throttled(mock_queued_image_request, false);
  expect_set_throttled(mock_queued_image_request);

  InSequence seq;
  MockImageRequestWQ mock_image_request_wq(&mock_image_ctx, "io", 60, nullptr);

  mock_image_request_wq.apply_qos_limit(IMAGE_DISPATCH_FLAG_QOS_BPS_THROTTLE, 1,
                                        0, 1);

  expect_front(mock_image_request_wq, &mock_queued_image_request);
  expect_tokens_requested(mock_queued_image_request, 2, true);
  expect_dequeue(mock_image_request_wq, &mock_queued_image_request);
  expect_all_throttled(mock_queued_image_request, true);
  expect_requeue_back(mock_image_request_wq);
  expect_signal(mock_image_request_wq);
  ASSERT_TRUE(mock_image_request_wq.invoke_dequeue() == nullptr);
}

TEST_F(TestMockIoImageRequestWQ, BPSQosWithBurst) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  MockImageDispatchSpec mock_queued_image_request;
  expect_was_throttled(mock_queued_image_request, false);
  expect_set_throttled(mock_queued_image_request);

  InSequence seq;
  MockImageRequestWQ mock_image_request_wq(&mock_image_ctx, "io", 60, nullptr);

  mock_image_request_wq.apply_qos_limit(IMAGE_DISPATCH_FLAG_QOS_BPS_THROTTLE, 1,
                                        1, 1);

  expect_front(mock_image_request_wq, &mock_queued_image_request);
  expect_tokens_requested(mock_queued_image_request, 2, true);
  expect_dequeue(mock_image_request_wq, &mock_queued_image_request);
  expect_all_throttled(mock_queued_image_request, true);
  expect_requeue_back(mock_image_request_wq);
  expect_signal(mock_image_request_wq);
  ASSERT_TRUE(mock_image_request_wq.invoke_dequeue() == nullptr);
}

} // namespace io
} // namespace librbd
