// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librados_test_stub/LibradosTestStub.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"

// template definitions
#include "librbd/AsyncRequest.cc"
#include "librbd/AsyncObjectThrottle.cc"
#include "librbd/operation/Request.cc"

template class librbd::AsyncRequest<librbd::MockImageCtx>;
template class librbd::AsyncObjectThrottle<librbd::MockImageCtx>;
template class librbd::operation::Request<librbd::MockImageCtx>;

using ::testing::_;
using ::testing::DoDefault;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

TestMockFixture::TestRadosClientPtr TestMockFixture::s_test_rados_client;
::testing::NiceMock<librados::MockTestMemRadosClient> *
  TestMockFixture::s_mock_rados_client = NULL;

void TestMockFixture::SetUpTestCase() {
  s_test_rados_client = librados_test_stub::get_rados_client();

  // use a mock version of the in-memory rados client
  s_mock_rados_client = new ::testing::NiceMock<librados::MockTestMemRadosClient>(
      s_test_rados_client->cct());
  librados_test_stub::set_rados_client(TestRadosClientPtr(s_mock_rados_client));
  TestFixture::SetUpTestCase();
}

void TestMockFixture::TearDownTestCase() {
  TestFixture::TearDownTestCase();
  librados_test_stub::set_rados_client(s_test_rados_client);
  s_test_rados_client->put();
  s_test_rados_client.reset();
}

void TestMockFixture::SetUp() {
  TestFixture::SetUp();
}

void TestMockFixture::TearDown() {
  TestFixture::TearDown();

  // Mock rados client lives across tests -- reset it to initial state
  ::testing::Mock::VerifyAndClear(s_mock_rados_client);
  s_mock_rados_client->default_to_dispatch();
}

void TestMockFixture::expect_unlock_exclusive_lock(librbd::ImageCtx &ictx) {
  EXPECT_CALL(get_mock_io_ctx(ictx.md_ctx),
              exec(_, _, StrEq("lock"), StrEq("unlock"), _, _, _))
                .WillRepeatedly(DoDefault());
}

void TestMockFixture::expect_op_work_queue(librbd::MockImageCtx &mock_image_ctx) {
  EXPECT_CALL(*mock_image_ctx.op_work_queue, queue(_, _))
                .WillRepeatedly(DispatchContext(
                  mock_image_ctx.image_ctx->op_work_queue));
}

void TestMockFixture::initialize_features(librbd::ImageCtx *ictx,
                                          librbd::MockImageCtx &mock_image_ctx,
                                          librbd::MockExclusiveLock &mock_exclusive_lock,
                                          librbd::MockJournal &mock_journal,
                                          librbd::MockObjectMap &mock_object_map) {
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    mock_image_ctx.exclusive_lock = &mock_exclusive_lock;
  }
  if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
    mock_image_ctx.journal = &mock_journal;
  }
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }
}

void TestMockFixture::expect_is_journal_appending(librbd::MockJournal &mock_journal,
                                                  bool appending) {
  EXPECT_CALL(mock_journal, is_journal_appending()).WillOnce(Return(appending));
}

void TestMockFixture::expect_is_journal_replaying(librbd::MockJournal &mock_journal) {
  EXPECT_CALL(mock_journal, is_journal_replaying()).WillOnce(Return(false));
}

void TestMockFixture::expect_is_journal_ready(librbd::MockJournal &mock_journal) {
  EXPECT_CALL(mock_journal, is_journal_ready()).WillOnce(Return(true));
}

void TestMockFixture::expect_allocate_op_tid(librbd::MockImageCtx &mock_image_ctx) {
  if (mock_image_ctx.journal != nullptr) {
    EXPECT_CALL(*mock_image_ctx.journal, allocate_op_tid())
                  .WillOnce(Return(1U));
  }
}

void TestMockFixture::expect_append_op_event(librbd::MockImageCtx &mock_image_ctx,
                                             bool can_affect_io, int r) {
  if (mock_image_ctx.journal != nullptr) {
    if (can_affect_io) {
      expect_is_journal_replaying(*mock_image_ctx.journal);
    }
    expect_is_journal_appending(*mock_image_ctx.journal, true);
    expect_allocate_op_tid(mock_image_ctx);
    EXPECT_CALL(*mock_image_ctx.journal, append_op_event_mock(_, _, _))
                  .WillOnce(WithArg<2>(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue)));
  }
}

void TestMockFixture::expect_commit_op_event(librbd::MockImageCtx &mock_image_ctx, int r) {
  if (mock_image_ctx.journal != nullptr) {
    expect_is_journal_appending(*mock_image_ctx.journal, true);
    expect_is_journal_ready(*mock_image_ctx.journal);
    EXPECT_CALL(*mock_image_ctx.journal, commit_op_event(1U, r, _))
                  .WillOnce(WithArg<2>(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue)));
  }
}

