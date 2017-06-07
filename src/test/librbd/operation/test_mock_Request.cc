// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockJournal.h"
#include "librbd/AsyncRequest.h"
#include "librbd/operation/Request.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

template <>
struct AsyncRequest<librbd::MockTestImageCtx> {
  librbd::MockTestImageCtx &m_image_ctx;
  Context *m_on_finish;

  AsyncRequest(librbd::MockTestImageCtx &image_ctx, Context *on_finish)
    : m_image_ctx(image_ctx), m_on_finish(on_finish) {
  }
  virtual ~AsyncRequest() {
  }

  virtual void finish(int r) {
    m_on_finish->complete(r);
  }
  virtual void finish_and_destroy(int r) {
    finish(r);
    delete this;
  }
};

} // namespace librbd

#include "librbd/operation/Request.cc"

namespace librbd {
namespace journal {

std::ostream& operator<<(std::ostream& os, const Event&) {
  return os;
}

} // namespace journal

namespace operation {

using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;

struct MockRequest : public Request<librbd::MockTestImageCtx> {
  MockRequest(librbd::MockTestImageCtx &image_ctx, Context *on_finish,
              uint64_t journal_op_tid)
    : Request<librbd::MockTestImageCtx>(image_ctx, on_finish, journal_op_tid) {
  }

  void complete(int r) {
    finish_and_destroy(r);
  }

  void send_op_impl(int r) {
    bool appending = append_op_event<
      MockRequest, &MockRequest::handle_send>(this);
    if (!appending) {
      complete(r);
    }
  }
  MOCK_METHOD1(should_complete, bool(int));
  MOCK_METHOD0(send_op, void());
  MOCK_METHOD1(handle_send, Context*(int*));
  MOCK_CONST_METHOD0(can_affect_io, bool());
  MOCK_CONST_METHOD1(create_event, journal::Event(uint64_t));
};

struct TestMockOperationRequest : public TestMockFixture {
  void expect_can_affect_io(MockRequest &mock_request, bool can_affect) {
    EXPECT_CALL(mock_request, can_affect_io())
      .WillOnce(Return(can_affect));
  }

  void expect_is_journal_replaying(MockJournal &mock_journal, bool replaying) {
    EXPECT_CALL(mock_journal, is_journal_replaying())
      .WillOnce(Return(replaying));
  }

  void expect_is_journal_appending(MockJournal &mock_journal, bool appending) {
    EXPECT_CALL(mock_journal, is_journal_appending())
      .WillOnce(Return(appending));
  }

  void expect_send_op(MockRequest &mock_request, int r) {
    EXPECT_CALL(mock_request, send_op())
      .WillOnce(Invoke([&mock_request, r]() {
                  mock_request.complete(r);
                }));
  }

  void expect_send_op_affects_io(MockImageCtx &mock_image_ctx,
                                 MockRequest &mock_request, int r) {
    EXPECT_CALL(mock_request, send_op())
      .WillOnce(Invoke([&mock_image_ctx, &mock_request, r]() {
                  mock_image_ctx.image_ctx->op_work_queue->queue(
                    new FunctionContext([&mock_request, r](int _) {
                      mock_request.send_op_impl(r);
                    }), 0);
                }));
  }

};

TEST_F(TestMockOperationRequest, SendJournalDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  C_SaferCond ctx;
  MockRequest *mock_request = new MockRequest(mock_image_ctx, &ctx, 0);

  InSequence seq;
  expect_can_affect_io(*mock_request, false);
  expect_is_journal_appending(mock_journal, false);
  expect_send_op(*mock_request, 0);

  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_request->send();
  }

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockOperationRequest, SendAffectsIOJournalDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  C_SaferCond ctx;
  MockRequest *mock_request = new MockRequest(mock_image_ctx, &ctx, 0);

  InSequence seq;
  expect_can_affect_io(*mock_request, true);
  expect_send_op_affects_io(mock_image_ctx, *mock_request, 0);
  expect_can_affect_io(*mock_request, true);
  expect_is_journal_replaying(mock_journal, false);
  expect_is_journal_appending(mock_journal, false);

  {
    RWLock::RLocker owner_locker(mock_image_ctx.owner_lock);
    mock_request->send();
  }

  ASSERT_EQ(0, ctx.wait());
}

} // namespace operation
} // namespace librbd
