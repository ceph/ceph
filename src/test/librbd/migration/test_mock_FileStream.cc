// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "include/rbd_types.h"
#include "common/ceph_mutex.h"
#include "librbd/migration/FileStream.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "json_spirit/json_spirit.h"
#include <filesystem>

namespace fs = std::filesystem;

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

#include "librbd/migration/FileStream.cc"

namespace librbd {
namespace migration {

using ::testing::Invoke;

class TestMockMigrationFileStream : public TestMockFixture {
public:
  typedef FileStream<MockTestImageCtx> MockFileStream;

  librbd::ImageCtx *m_image_ctx;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_image_ctx));
    file_name = fs::temp_directory_path() / "TestMockMigrationFileStream";
    file_name += stringify(getpid());
    json_object["file_path"] = file_name;
  }

  void TearDown() override {
    fs::remove(file_name);
    TestMockFixture::TearDown();
  }

  std::string file_name;
  json_spirit::mObject json_object;
};

TEST_F(TestMockMigrationFileStream, OpenClose) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  bufferlist bl;
  ASSERT_EQ(0, bl.write_file(file_name.c_str()));

  MockFileStream mock_file_stream(&mock_image_ctx, json_object);

  C_SaferCond ctx1;
  mock_file_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  mock_file_stream.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationFileStream, GetSize) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  bufferlist expect_bl;
  expect_bl.append(std::string(128, '1'));
  ASSERT_EQ(0, expect_bl.write_file(file_name.c_str()));

  MockFileStream mock_file_stream(&mock_image_ctx, json_object);

  C_SaferCond ctx1;
  mock_file_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  uint64_t size;
  mock_file_stream.get_size(&size, &ctx2);
  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(128, size);

  C_SaferCond ctx3;
  mock_file_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationFileStream, Read) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  bufferlist expect_bl;
  expect_bl.append(std::string(128, '1'));
  ASSERT_EQ(0, expect_bl.write_file(file_name.c_str()));

  MockFileStream mock_file_stream(&mock_image_ctx, json_object);

  C_SaferCond ctx1;
  mock_file_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  bufferlist bl;
  mock_file_stream.read({{0, 128}}, &bl, &ctx2);
  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(expect_bl, bl);

  C_SaferCond ctx3;
  mock_file_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationFileStream, SeekRead) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  bufferlist write_bl;
  write_bl.append(std::string(32, '1'));
  write_bl.append(std::string(64, '2'));
  write_bl.append(std::string(16, '3'));
  ASSERT_EQ(0, write_bl.write_file(file_name.c_str()));

  MockFileStream mock_file_stream(&mock_image_ctx, json_object);

  C_SaferCond ctx1;
  mock_file_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  bufferlist bl;
  mock_file_stream.read({{96, 16}, {32, 64}, {0, 32}}, &bl, &ctx2);
  ASSERT_EQ(0, ctx2.wait());

  bufferlist expect_bl;
  expect_bl.append(std::string(16, '3'));
  expect_bl.append(std::string(64, '2'));
  expect_bl.append(std::string(32, '1'));
  ASSERT_EQ(expect_bl, bl);

  C_SaferCond ctx3;
  mock_file_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationFileStream, DNE) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  MockFileStream mock_file_stream(&mock_image_ctx, json_object);

  C_SaferCond ctx1;
  mock_file_stream.open(&ctx1);
  ASSERT_EQ(-ENOENT, ctx1.wait());

  C_SaferCond ctx2;
  mock_file_stream.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationFileStream, SeekError) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  bufferlist bl;
  ASSERT_EQ(0, bl.write_file(file_name.c_str()));

  MockFileStream mock_file_stream(&mock_image_ctx, json_object);

  C_SaferCond ctx1;
  mock_file_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  mock_file_stream.read({{128, 128}}, &bl, &ctx2);
  ASSERT_EQ(-ERANGE, ctx2.wait());

  C_SaferCond ctx3;
  mock_file_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationFileStream, ShortReadError) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  bufferlist expect_bl;
  expect_bl.append(std::string(128, '1'));
  ASSERT_EQ(0, expect_bl.write_file(file_name.c_str()));

  MockFileStream mock_file_stream(&mock_image_ctx, json_object);

  C_SaferCond ctx1;
  mock_file_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  bufferlist bl;
  mock_file_stream.read({{0, 256}}, &bl, &ctx2);
  ASSERT_EQ(-ERANGE, ctx2.wait());

  C_SaferCond ctx3;
  mock_file_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

} // namespace migration
} // namespace librbd
