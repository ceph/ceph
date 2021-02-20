// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/crypto/CryptoObjectDispatch.h"
#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/crypto/MockCryptoInterface.h"
#include "test/librbd/mock/crypto/MockEncryptionFormat.h"
#include "test/librbd/mock/io/MockObjectDispatch.h"

namespace librbd {
namespace crypto {

template <>
struct CryptoObjectDispatch<MockImageCtx> : public io::MockObjectDispatch {

  static CryptoObjectDispatch* create(
          MockImageCtx* image_ctx,ceph::ref_t<CryptoInterface> crypto) {
    return new CryptoObjectDispatch();
  }

  CryptoObjectDispatch() {
  }
};

} // namespace crypto
} // namespace librbd

#include "librbd/crypto/LoadRequest.cc"

namespace librbd {
namespace crypto {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArgs;

struct TestMockCryptoLoadRequest : public TestMockFixture {
  typedef LoadRequest<librbd::MockImageCtx> MockLoadRequest;

  MockImageCtx* mock_image_ctx;
  C_SaferCond finished_cond;
  Context *on_finish = &finished_cond;
  MockEncryptionFormat* mock_encryption_format;
  Context* load_context;

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

  void expect_encryption_load() {
    EXPECT_CALL(*mock_encryption_format, load(
            mock_image_ctx, _, _)).WillOnce(
                    WithArgs<1, 2>(Invoke([this](
                            ceph::ref_t<CryptoInterface>* result_crypto,
                            Context* ctx) {
                      load_context = ctx;
                      *result_crypto = new MockCryptoInterface();
    })));
  }
};

TEST_F(TestMockCryptoLoadRequest, CryptoAlreadyLoaded) {
  auto mock_load_request = MockLoadRequest::create(
          mock_image_ctx,
          std::unique_ptr<MockEncryptionFormat>(mock_encryption_format),
          on_finish);
  expect_crypto_layer_exists_check(true);
  mock_load_request->send();
  ASSERT_EQ(-EEXIST, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, JournalEnabled) {
  auto mock_load_request = MockLoadRequest::create(
          mock_image_ctx,
          std::unique_ptr<MockEncryptionFormat>(mock_encryption_format),
          on_finish);
  expect_crypto_layer_exists_check(false);
  expect_test_journal_feature(true);
  mock_load_request->send();
  ASSERT_EQ(-ENOTSUP, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, LoadFail) {
  auto mock_load_request = MockLoadRequest::create(
          mock_image_ctx,
          std::unique_ptr<MockEncryptionFormat>(mock_encryption_format),
          on_finish);
  expect_crypto_layer_exists_check(false);
  expect_test_journal_feature(false);
  expect_encryption_load();
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  load_context->complete(-EIO);
  ASSERT_EQ(-EIO, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, Success) {
  auto mock_load_request = MockLoadRequest::create(
          mock_image_ctx,
          std::unique_ptr<MockEncryptionFormat>(mock_encryption_format),
          on_finish);
  expect_crypto_layer_exists_check(false);
  expect_test_journal_feature(false);
  expect_encryption_load();
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  EXPECT_CALL(*mock_image_ctx->io_object_dispatcher, register_dispatch(_));
  EXPECT_CALL(*mock_image_ctx->io_image_dispatcher, register_dispatch(_));
  load_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
}

} // namespace crypto
} // namespace librbd
