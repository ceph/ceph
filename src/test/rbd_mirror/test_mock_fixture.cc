// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "test/librados_test_stub/LibradosTestStub.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "test/librbd/mock/MockImageCtx.h"

namespace rbd {
namespace mirror {

using ::testing::_;
using ::testing::Invoke;
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

void TestMockFixture::expect_test_features(librbd::MockImageCtx &mock_image_ctx) {
  EXPECT_CALL(mock_image_ctx, test_features(_, _))
    .WillRepeatedly(WithArg<0>(Invoke([&mock_image_ctx](uint64_t features) {
        return (mock_image_ctx.features & features) != 0;
      })));
}

} // namespace mirror
} // namespace rbd

