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
              exec(_, _, "lock", "unlock", _, _, _))
                .WillRepeatedly(DoDefault());
}

void TestMockFixture::expect_op_work_queue(librbd::MockImageCtx &mock_image_ctx) {
  EXPECT_CALL(*mock_image_ctx.op_work_queue, queue(_, _))
                .WillRepeatedly(DispatchContext(
                  mock_image_ctx.image_ctx->op_work_queue));
}

librados::MockTestMemIoCtxImpl &TestMockFixture::get_mock_io_ctx(
    librados::IoCtx &ioctx) {
  // TODO become friend of IoCtx so that we can cleanly extract io_ctx_impl
  librados::MockTestMemIoCtxImpl **mock =
    reinterpret_cast<librados::MockTestMemIoCtxImpl **>(&ioctx);
  return **mock;
}
