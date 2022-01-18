// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "include/rbd_types.h"
#include "librbd/migration/NBDStream.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "json_spirit/json_spirit.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

#include "librbd/migration/NBDStream.cc"

namespace librbd {
namespace migration {

using ::testing::Invoke;

class TestMockMigrationNBDStream : public TestMockFixture {
public:
  typedef NBDStream<MockTestImageCtx> MockNBDStream;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_image_ctx));
    json_object["url"] = "localhost";
    json_object["port"] = "10809";
  }

  librbd::ImageCtx *m_image_ctx;
  json_spirit::mObject json_object;
};

TEST_F(TestMockMigrationNBDStream, OpenClose) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  // Since we don't have an nbd server running, we actually expect a failure.
  ASSERT_EQ(-22, ctx1.wait());

  C_SaferCond ctx2;
  mock_nbd_stream.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

} // namespace migration
} // namespace librbd
