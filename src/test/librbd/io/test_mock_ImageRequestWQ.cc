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
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace io {

template <>
struct ImageRequest<librbd::MockTestImageCtx> {
  static ImageRequest* s_instance;
  AioCompletion *aio_comp = nullptr;

  static void aio_write(librbd::MockTestImageCtx *ictx, AioCompletion *c,
                        Extents &&image_extents, bufferlist &&bl, int op_flags,
                        const ZTracer::Trace &parent_trace) {
  }

  ImageRequest() {
    s_instance = this;
  }
};

template <>
struct ImageDispatchSpec<librbd::MockTestImageCtx> {
  static ImageDispatchSpec* s_instance;
  AioCompletion *aio_comp = nullptr;

  static ImageDispatchSpec* create_write_request(
      librbd::MockTestImageCtx &image_ctx, AioCompletion *aio_comp,
      Extents &&image_extents, bufferlist &&bl, int op_flags,
      const ZTracer::Trace &parent_trace) {
    ceph_assert(s_instance != nullptr);
    s_instance->aio_comp = aio_comp;
    return s_instance;
  }

  static ImageDispatchSpec* create_flush_request(
      librbd::MockTestImageCtx &image_ctx, AioCompletion *aio_comp,
      FlushSource flush_source, const ZTracer::Trace &parent_trace) {
    ceph_assert(s_instance != nullptr);
    s_instance->aio_comp = aio_comp;
    return s_instance;
  }

  MOCK_CONST_METHOD0(is_write_op, bool());
  MOCK_CONST_METHOD0(start_op, void());
  MOCK_CONST_METHOD0(send, void());
  MOCK_CONST_METHOD1(fail, void(int));
  MOCK_CONST_METHOD1(was_throttled, bool(uint64_t));
  MOCK_CONST_METHOD0(were_all_throttled, bool());
  MOCK_CONST_METHOD1(set_throttled, void(uint64_t));
  MOCK_CONST_METHOD2(tokens_requested, bool(uint64_t, uint64_t *));

  ImageDispatchSpec() {
    s_instance = this;
  }
};
} // namespace io

namespace util {

inline ImageCtx *get_image_ctx(MockTestImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util

} // namespace librbd

template <>
struct ThreadPool::PointerWQ<librbd::io::ImageDispatchSpec<librbd::MockTestImageCtx>> {
  typedef librbd::io::ImageDispatchSpec<librbd::MockTestImageCtx> ImageDispatchSpec;
  static PointerWQ* s_instance;

  Mutex m_lock;

  PointerWQ(const std::string &name, time_t, int, ThreadPool *)
    : m_lock(name) {
    s_instance = this;
  }
  virtual ~PointerWQ() {
  }

  MOCK_METHOD0(drain, void());
  MOCK_METHOD0(empty, bool());
  MOCK_METHOD0(mock_empty, bool());
  MOCK_METHOD0(signal, void());
  MOCK_METHOD0(process_finish, void());

  MOCK_METHOD0(front, ImageDispatchSpec*());
  MOCK_METHOD1(requeue_front, void(ImageDispatchSpec*));
  MOCK_METHOD1(requeue_back, void(ImageDispatchSpec*));

  MOCK_METHOD0(dequeue, void*());
  MOCK_METHOD1(queue, void(ImageDispatchSpec*));

  void register_work_queue() {
    // no-op
  }
  Mutex &get_pool_lock() {
    return m_lock;
  }

  void* invoke_dequeue() {
    Mutex::Locker locker(m_lock);
    return _void_dequeue();
  }
  void invoke_process(ImageDispatchSpec *image_request) {
    process(image_request);
  }

  virtual void *_void_dequeue() {
    return dequeue();
  }
  virtual void process(ImageDispatchSpec *req) = 0;
  virtual bool _empty() {
    return mock_empty();
  }

};

ThreadPool::PointerWQ<librbd::io::ImageDispatchSpec<librbd::MockTestImageCtx>>*
  ThreadPool::PointerWQ<librbd::io::ImageDispatchSpec<librbd::MockTestImageCtx>>::s_instance = nullptr;
librbd::io::ImageRequest<librbd::MockTestImageCtx>*
  librbd::io::ImageRequest<librbd::MockTestImageCtx>::s_instance = nullptr;
librbd::io::ImageDispatchSpec<librbd::MockTestImageCtx>*
  librbd::io::ImageDispatchSpec<librbd::MockTestImageCtx>::s_instance = nullptr;

#include "librbd/io/ImageRequestWQ.cc"

namespace librbd {
namespace io {

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;

struct TestMockIoImageRequestWQ : public TestMockFixture {
  typedef ImageRequestWQ<librbd::MockTestImageCtx> MockImageRequestWQ;
  typedef ImageRequest<librbd::MockTestImageCtx> MockImageRequest;
  typedef ImageDispatchSpec<librbd::MockTestImageCtx> MockImageDispatchSpec;

  void expect_is_write_op(MockImageDispatchSpec &image_request,
                          bool write_op) {
    EXPECT_CALL(image_request, is_write_op()).WillOnce(Return(write_op));
  }

  void expect_signal(MockImageRequestWQ &image_request_wq) {
    EXPECT_CALL(image_request_wq, signal());
  }

  void expect_queue(MockImageRequestWQ &image_request_wq) {
    EXPECT_CALL(image_request_wq, queue(_));
  }

  void expect_requeue_back(MockImageRequestWQ &image_request_wq) {
    EXPECT_CALL(image_request_wq, requeue_back(_));
  }

  void expect_front(MockImageRequestWQ &image_request_wq,
                    MockImageDispatchSpec *image_request) {
    EXPECT_CALL(image_request_wq, front()).WillOnce(Return(image_request));
  }

  void expect_is_refresh_request(MockTestImageCtx &mock_image_ctx,
                                 bool required) {
    EXPECT_CALL(*mock_image_ctx.state, is_refresh_required()).WillOnce(
      Return(required));
  }

  void expect_dequeue(MockImageRequestWQ &image_request_wq,
                      MockImageDispatchSpec *image_request) {
    EXPECT_CALL(image_request_wq, dequeue()).WillOnce(Return(image_request));
  }

  void expect_get_exclusive_lock_policy(MockTestImageCtx &mock_image_ctx,
                                        librbd::exclusive_lock::MockPolicy &policy) {
    EXPECT_CALL(mock_image_ctx,
                get_exclusive_lock_policy()).WillOnce(Return(&policy));
  }

  void expect_may_auto_request_lock(librbd::exclusive_lock::MockPolicy &policy,
                                    bool value) {
    EXPECT_CALL(policy, may_auto_request_lock()).WillOnce(Return(value));
  }

  void expect_acquire_lock(MockExclusiveLock &mock_exclusive_lock,
                           Context **on_finish) {
    EXPECT_CALL(mock_exclusive_lock, acquire_lock(_))
      .WillOnce(Invoke([on_finish](Context *ctx) {
                    *on_finish = ctx;
                  }));
  }

  void expect_process_finish(MockImageRequestWQ &mock_image_request_wq) {
    EXPECT_CALL(mock_image_request_wq, process_finish()).Times(1);
  }

  void expect_fail(MockImageDispatchSpec &mock_image_request, int r) {
    EXPECT_CALL(mock_image_request, fail(r))
      .WillOnce(Invoke([&mock_image_request](int r) {
                    mock_image_request.aio_comp->get();
                    mock_image_request.aio_comp->fail(r);
                  }));
  }

  void expect_refresh(MockTestImageCtx &mock_image_ctx, Context **on_finish) {
    EXPECT_CALL(*mock_image_ctx.state, refresh(_))
      .WillOnce(Invoke([on_finish](Context *ctx) {
                    *on_finish = ctx;
                  }));
  }

  void expect_set_throttled(MockImageDispatchSpec &mock_image_request) {
    EXPECT_CALL(mock_image_request, set_throttled(_)).Times(6);
  }

  void expect_was_throttled(MockImageDispatchSpec &mock_image_request, bool value) {
    EXPECT_CALL(mock_image_request, was_throttled(_)).Times(6).WillRepeatedly(Return(value));
  }

  void expect_tokens_requested(MockImageDispatchSpec &mock_image_request,
                               uint64_t tokens, bool r) {
    EXPECT_CALL(mock_image_request, tokens_requested(_, _))
      .WillOnce(WithArg<1>(Invoke([tokens, r](uint64_t *t) {
                         *t = tokens;
                         return r;
                       })));
  }

  void expect_all_throttled(MockImageDispatchSpec &mock_image_request, bool value) {
    EXPECT_CALL(mock_image_request, were_all_throttled()).WillOnce(Return(value));
  }

  void expect_start_op(MockImageDispatchSpec &mock_image_request) {
    EXPECT_CALL(mock_image_request, start_op()).Times(1);
  }
};

TEST_F(TestMockIoImageRequestWQ, AcquireLockError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  auto mock_queued_image_request = new MockImageDispatchSpec();
  expect_was_throttled(*mock_queued_image_request, false);
  expect_set_throttled(*mock_queued_image_request);

  InSequence seq;
  MockImageRequestWQ mock_image_request_wq(&mock_image_ctx, "io", 60, nullptr);
  expect_signal(mock_image_request_wq);
  mock_image_request_wq.set_require_lock(DIRECTION_WRITE, true);

  expect_is_write_op(*mock_queued_image_request, true);
  expect_queue(mock_image_request_wq);
  auto *aio_comp = new librbd::io::AioCompletion();
  mock_image_request_wq.aio_write(aio_comp, 0, 0, {}, 0);

  librbd::exclusive_lock::MockPolicy mock_exclusive_lock_policy;
  expect_front(mock_image_request_wq, mock_queued_image_request);
  expect_is_refresh_request(mock_image_ctx, false);
  expect_is_write_op(*mock_queued_image_request, true);
  expect_dequeue(mock_image_request_wq, mock_queued_image_request);
  expect_get_exclusive_lock_policy(mock_image_ctx, mock_exclusive_lock_policy);
  expect_may_auto_request_lock(mock_exclusive_lock_policy, true);
  Context *on_acquire = nullptr;
  expect_acquire_lock(mock_exclusive_lock, &on_acquire);
  ASSERT_TRUE(mock_image_request_wq.invoke_dequeue() == nullptr);
  ASSERT_TRUE(on_acquire != nullptr);

  expect_process_finish(mock_image_request_wq);
  expect_fail(*mock_queued_image_request, -EPERM);
  expect_is_write_op(*mock_queued_image_request, true);
  expect_signal(mock_image_request_wq);
  on_acquire->complete(-EPERM);

  ASSERT_EQ(0, aio_comp->wait_for_complete());
  ASSERT_EQ(-EPERM, aio_comp->get_return_value());
  aio_comp->release();
}

TEST_F(TestMockIoImageRequestWQ, AcquireLockBlacklisted) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  auto mock_queued_image_request = new MockImageDispatchSpec();
  expect_was_throttled(*mock_queued_image_request, false);
  expect_set_throttled(*mock_queued_image_request);

  InSequence seq;
  MockImageRequestWQ mock_image_request_wq(&mock_image_ctx, "io", 60, nullptr);
  expect_signal(mock_image_request_wq);
  mock_image_request_wq.set_require_lock(DIRECTION_WRITE, true);

  expect_is_write_op(*mock_queued_image_request, true);
  expect_queue(mock_image_request_wq);
  auto *aio_comp = new librbd::io::AioCompletion();
  mock_image_request_wq.aio_write(aio_comp, 0, 0, {}, 0);

  librbd::exclusive_lock::MockPolicy mock_exclusive_lock_policy;
  expect_front(mock_image_request_wq, mock_queued_image_request);
  expect_is_refresh_request(mock_image_ctx, false);
  expect_is_write_op(*mock_queued_image_request, true);
  expect_dequeue(mock_image_request_wq, mock_queued_image_request);
  expect_get_exclusive_lock_policy(mock_image_ctx, mock_exclusive_lock_policy);
  expect_may_auto_request_lock(mock_exclusive_lock_policy, false);
  EXPECT_CALL(*mock_image_ctx.exclusive_lock, get_unlocked_op_error())
    .WillOnce(Return(-EBLACKLISTED));
  expect_process_finish(mock_image_request_wq);
  expect_fail(*mock_queued_image_request, -EBLACKLISTED);
  expect_is_write_op(*mock_queued_image_request, true);
  expect_signal(mock_image_request_wq);
  ASSERT_TRUE(mock_image_request_wq.invoke_dequeue() == nullptr);

  ASSERT_EQ(0, aio_comp->wait_for_complete());
  ASSERT_EQ(-EBLACKLISTED, aio_comp->get_return_value());
  aio_comp->release();
}

TEST_F(TestMockIoImageRequestWQ, RefreshError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  auto mock_queued_image_request = new MockImageDispatchSpec();
  expect_was_throttled(*mock_queued_image_request, false);
  expect_set_throttled(*mock_queued_image_request);

  InSequence seq;
  MockImageRequestWQ mock_image_request_wq(&mock_image_ctx, "io", 60, nullptr);

  expect_is_write_op(*mock_queued_image_request, true);
  expect_queue(mock_image_request_wq);
  auto *aio_comp = new librbd::io::AioCompletion();
  mock_image_request_wq.aio_write(aio_comp, 0, 0, {}, 0);

  expect_front(mock_image_request_wq, mock_queued_image_request);
  expect_is_refresh_request(mock_image_ctx, true);
  expect_is_write_op(*mock_queued_image_request, true);
  expect_dequeue(mock_image_request_wq, mock_queued_image_request);
  Context *on_refresh = nullptr;
  expect_refresh(mock_image_ctx, &on_refresh);
  ASSERT_TRUE(mock_image_request_wq.invoke_dequeue() == nullptr);
  ASSERT_TRUE(on_refresh != nullptr);

  expect_process_finish(mock_image_request_wq);
  expect_fail(*mock_queued_image_request, -EPERM);
  expect_is_write_op(*mock_queued_image_request, true);
  expect_signal(mock_image_request_wq);
  on_refresh->complete(-EPERM);

  ASSERT_EQ(0, aio_comp->wait_for_complete());
  ASSERT_EQ(-EPERM, aio_comp->get_return_value());
  aio_comp->release();
}

TEST_F(TestMockIoImageRequestWQ, QosNoLimit) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  MockImageDispatchSpec mock_queued_image_request;
  expect_was_throttled(mock_queued_image_request, false);
  expect_set_throttled(mock_queued_image_request);

  InSequence seq;
  MockImageRequestWQ mock_image_request_wq(&mock_image_ctx, "io", 60, nullptr);

  mock_image_request_wq.apply_qos_limit(RBD_QOS_BPS_THROTTLE, 0, 0);

  expect_front(mock_image_request_wq, &mock_queued_image_request);
  expect_is_refresh_request(mock_image_ctx, false);
  expect_is_write_op(mock_queued_image_request, true);
  expect_dequeue(mock_image_request_wq, &mock_queued_image_request);
  expect_start_op(mock_queued_image_request);
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

  mock_image_request_wq.apply_qos_limit(RBD_QOS_BPS_THROTTLE, 1, 0);

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

  mock_image_request_wq.apply_qos_limit(RBD_QOS_BPS_THROTTLE, 1, 1);

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
