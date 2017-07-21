// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "include/rbd/librbd.hpp"
#include "test/librados_test_stub/LibradosTestStub.h"
#include "test/librados_test_stub/MockTestMemCluster.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "test/librbd/mock/MockImageCtx.h"

namespace rbd {
namespace mirror {

using ::testing::_;
using ::testing::Invoke;
using ::testing::WithArg;

TestMockFixture::TestClusterRef TestMockFixture::s_test_cluster;

void TestMockFixture::SetUpTestCase() {
  s_test_cluster = librados_test_stub::get_cluster();

  // use a mock version of the in-memory rados client
  librados_test_stub::set_cluster(boost::shared_ptr<librados::TestCluster>(
    new librados::MockTestMemCluster()));
  TestFixture::SetUpTestCase();
}

void TestMockFixture::TearDownTestCase() {
  TestFixture::TearDownTestCase();
  librados_test_stub::set_cluster(s_test_cluster);
}

void TestMockFixture::TearDown() {
  // Mock rados client lives across tests -- reset it to initial state
  librados::MockTestMemRadosClient *mock_rados_client =
    get_mock_io_ctx(m_local_io_ctx).get_mock_rados_client();
  ASSERT_TRUE(mock_rados_client != nullptr);

  ::testing::Mock::VerifyAndClear(mock_rados_client);
  mock_rados_client->default_to_dispatch();

  TestFixture::TearDown();
}

void TestMockFixture::expect_test_features(librbd::MockImageCtx &mock_image_ctx) {
  EXPECT_CALL(mock_image_ctx, test_features(_, _))
    .WillRepeatedly(WithArg<0>(Invoke([&mock_image_ctx](uint64_t features) {
        return (mock_image_ctx.features & features) != 0;
      })));
}

} // namespace mirror
} // namespace rbd

