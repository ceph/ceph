// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"

using librbd::util::data_object_name;

void register_test_utils() {
}

struct TestUtils : public TestFixture {
  void SetUp() override {
    TestFixture::SetUp();
    ASSERT_EQ(0, open_image(m_image_name, &m_ictx));
  }

  void expect_data_object_name(uint64_t object_no,
                               const std::string& expected_object_suffix) {
    EXPECT_EQ(data_object_name(m_ictx, object_no),
              m_ictx->object_prefix + std::string(".") +
              expected_object_suffix);
    }

    librbd::ImageCtx *m_ictx;
};

TEST_F(TestUtils, DataObjectNameV1) {
  REQUIRE_FORMAT_V1();
  expect_data_object_name(0, "000000000000");
  expect_data_object_name(1, "000000000001");
  expect_data_object_name(0x123456789ab, "0123456789ab");
  expect_data_object_name(0x800000000000, "800000000000");
  expect_data_object_name(0x800000000001, "800000000001");
  expect_data_object_name(0xffffffffffff, "ffffffffffff");
}

TEST_F(TestUtils, DataObjectNameV2) {
  REQUIRE_FORMAT_V2();
  expect_data_object_name(0, "0000000000000000");
  expect_data_object_name(1, "0000000000000001");
  expect_data_object_name(0x123456789abcdef, "0123456789abcdef");
  expect_data_object_name(0x8000000000000000, "8000000000000000");
  expect_data_object_name(0x8000000000000001, "8000000000000001");
  expect_data_object_name(0xffffffffffffffff, "ffffffffffffffff");
}

