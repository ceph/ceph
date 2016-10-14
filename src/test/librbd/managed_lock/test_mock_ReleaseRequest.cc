// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "librbd/managed_lock/ReleaseRequest.h"
#include "common/WorkQueue.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <list>

namespace librbd {
namespace managed_lock {

struct MockLockWatcher {
  MOCK_METHOD1(flush, void(Context *));
  MOCK_METHOD0(work_queue, ContextWQ*());
};

}
}

// template definitions
#include "librbd/managed_lock/ReleaseRequest.cc"
template class librbd::managed_lock::ReleaseRequest<librbd::managed_lock::MockLockWatcher>;

namespace librbd {
namespace managed_lock {

namespace {

struct MockContext : public Context {
  MOCK_METHOD1(complete, void(int));
  MOCK_METHOD1(finish, void(int));
};

struct MockWorkQueue {
  ThreadPool thread_pool;
  ContextWQ work_queue;

  MockWorkQueue(CephContext *cct) :
    thread_pool(cct, "pool_name", "thread_name", 1, ""),
    work_queue("work_queue", 0, &thread_pool) {
      thread_pool.start();
    }

  ~MockWorkQueue() {
    thread_pool.stop();
  }
};

} // anonymous namespace

using ::testing::_;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;

static const std::string TEST_COOKIE("auto 123");

class TestMockManagedLockReleaseRequest : public TestMockFixture {
public:
  typedef ReleaseRequest<MockLockWatcher> MockReleaseRequest;

  void expect_complete_context(MockContext &mock_context, int r) {
    EXPECT_CALL(mock_context, complete(r));
  }

  void expect_unlock(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"),
                     StrEq("unlock"), _, _, _))
                        .WillOnce(Return(r));
  }

  void expect_flush_notifies(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(m_lock_watcher, flush(_))
                  .WillOnce(CompleteContext(0, (ContextWQ *)nullptr));
  }

  void expect_work_queue() {
    EXPECT_CALL(m_lock_watcher, work_queue()).WillOnce(
                                    Return(&(m_work_queue->work_queue)));
  }

  void SetUp() {
    TestMockFixture::SetUp();
    m_work_queue = new MockWorkQueue(reinterpret_cast<CephContext *>(
                                     m_ioctx.cct()));
    expect_work_queue();
  }

  void TearDown() {
    TestMockFixture::TearDown();
    delete m_work_queue;
  }

  MockWorkQueue *m_work_queue;
  MockLockWatcher m_lock_watcher;
};

TEST_F(TestMockManagedLockReleaseRequest, Success) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);

  MockContext mock_releasing_ctx;
  expect_complete_context(mock_releasing_ctx, 0);

  expect_unlock(mock_image_ctx, 0);

  C_SaferCond ctx;
  MockReleaseRequest *req = MockReleaseRequest::create(mock_image_ctx.md_ctx,
                                                       &m_lock_watcher,
                                                       mock_image_ctx.header_oid,
                                                       TEST_COOKIE,
                                                       &mock_releasing_ctx,
                                                       &ctx, false);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockManagedLockReleaseRequest, UnlockError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);

  expect_unlock(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockReleaseRequest *req = MockReleaseRequest::create(mock_image_ctx.md_ctx,
                                                       &m_lock_watcher,
                                                       mock_image_ctx.header_oid,
                                                       TEST_COOKIE,
                                                       nullptr,
                                                       &ctx, false);
  req->send();
  ASSERT_EQ(0, ctx.wait());

}

} // namespace managed_lock
} // namespace librbd
