// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/crypto/MockEncryptionFormat.h"

#include "librbd/crypto/FormatRequest.cc"

namespace librbd {
namespace crypto {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;

struct TestMockCryptoFormatRequest : public TestMockFixture {
  typedef FormatRequest<librbd::MockImageCtx> MockFormatRequest;

  MockImageCtx* mock_image_ctx;
  C_SaferCond finished_cond;
  Context *on_finish = &finished_cond;
  MockEncryptionFormat* mock_encryption_format;
  Context* format_context;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    mock_image_ctx = new MockImageCtx(*ictx);
    mock_encryption_format = new MockEncryptionFormat();
  }

  void TearDown() override {
    delete mock_image_ctx;
    TestMockFixture::TearDown();
  }

  void expect_crypto_layer_exists_check(bool exists) {
    EXPECT_CALL(*mock_image_ctx->io_object_dispatcher, exists(
            io::OBJECT_DISPATCH_LAYER_CRYPTO)).WillOnce(Return(exists));
  }

  void expect_test_journal_feature(bool has_journal=false) {
    EXPECT_CALL(*mock_image_ctx, test_features(
            RBD_FEATURE_JOURNALING)).WillOnce(Return(has_journal));
  }

  void expect_encryption_format() {
    EXPECT_CALL(*mock_encryption_format, format(
            mock_image_ctx, _)).WillOnce(
                    WithArg<1>(Invoke([this](Context* ctx) {
                      format_context = ctx;
    })));
  }
};

TEST_F(TestMockCryptoFormatRequest, CryptoAlreadyLoaded) {
  auto mock_format_request = MockFormatRequest::create(
          mock_image_ctx,
          std::unique_ptr<MockEncryptionFormat>(mock_encryption_format),
          on_finish);
  expect_crypto_layer_exists_check(true);
  mock_format_request->send();
  ASSERT_EQ(-EEXIST, finished_cond.wait());
}

TEST_F(TestMockCryptoFormatRequest, JournalEnabled) {
  auto mock_format_request = MockFormatRequest::create(
          mock_image_ctx,
          std::unique_ptr<MockEncryptionFormat>(mock_encryption_format),
          on_finish);
  expect_crypto_layer_exists_check(false);
  expect_test_journal_feature(true);
  mock_format_request->send();
  ASSERT_EQ(-ENOTSUP, finished_cond.wait());
}

TEST_F(TestMockCryptoFormatRequest, CloneFormat) {
  mock_image_ctx->parent = mock_image_ctx;
  auto mock_format_request = MockFormatRequest::create(
          mock_image_ctx,
          std::unique_ptr<MockEncryptionFormat>(mock_encryption_format),
          on_finish);
  expect_crypto_layer_exists_check(false);
  mock_format_request->send();
  ASSERT_EQ(-ENOTSUP, finished_cond.wait());
}

TEST_F(TestMockCryptoFormatRequest, FormatFail) {
  auto mock_format_request = MockFormatRequest::create(
          mock_image_ctx,
          std::unique_ptr<MockEncryptionFormat>(mock_encryption_format),
          on_finish);
  expect_crypto_layer_exists_check(false);
  expect_test_journal_feature(false);
  expect_encryption_format();
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  format_context->complete(-EIO);
  ASSERT_EQ(-EIO, finished_cond.wait());
}

TEST_F(TestMockCryptoFormatRequest, Success) {
  auto mock_format_request = MockFormatRequest::create(
          mock_image_ctx,
          std::unique_ptr<MockEncryptionFormat>(mock_encryption_format),
          on_finish);
  expect_crypto_layer_exists_check(false);
  expect_test_journal_feature(false);
  expect_encryption_format();
  mock_format_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  format_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
}

} // namespace crypto
} // namespace librbd
