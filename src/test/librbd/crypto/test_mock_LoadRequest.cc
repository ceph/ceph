// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/crypto/CryptoObjectDispatch.h"
#include "librbd/crypto/Utils.h"
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
  MockCryptoInterface* crypto;
  Context* load_context;
  MockLoadRequest* mock_load_request;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    mock_image_ctx = new MockImageCtx(*ictx);
    mock_encryption_format = new MockEncryptionFormat();
    crypto = new MockCryptoInterface();
    mock_load_request = MockLoadRequest::create(
          mock_image_ctx,
          std::unique_ptr<MockEncryptionFormat>(mock_encryption_format),
          on_finish);
  }

  void TearDown() override {
    crypto->put();
    delete mock_image_ctx;
    TestMockFixture::TearDown();
  }

  void expect_test_journal_feature() {
    expect_test_journal_feature(mock_image_ctx, false);
  }

  void expect_test_journal_feature(MockImageCtx* ctx, bool has_journal=false) {
    EXPECT_CALL(*ctx, test_features(
            RBD_FEATURE_JOURNALING)).WillOnce(Return(has_journal));
  }

  void expect_encryption_load() {
    EXPECT_CALL(*mock_encryption_format, load(
            mock_image_ctx, _)).WillOnce(
                    WithArgs<1>(Invoke([this](Context* ctx) {
                      load_context = ctx;
    })));
  }

};

TEST_F(TestMockCryptoLoadRequest, CryptoAlreadyLoaded) {
  mock_image_ctx->crypto = crypto;
  mock_load_request->send();
  ASSERT_EQ(-EEXIST, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, JournalEnabled) {
  expect_test_journal_feature(mock_image_ctx, true);
  mock_load_request->send();
  ASSERT_EQ(-ENOTSUP, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, LoadFail) {
  expect_test_journal_feature();
  expect_encryption_load();
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  load_context->complete(-EIO);
  ASSERT_EQ(-EIO, finished_cond.wait());
}

TEST_F(TestMockCryptoLoadRequest, Success) {
  mock_image_ctx->parent = nullptr;
  expect_test_journal_feature(mock_image_ctx, false);
  expect_encryption_load();
  mock_load_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  EXPECT_CALL(*mock_encryption_format, get_crypto()).WillOnce(Return(crypto));
  load_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(crypto, mock_image_ctx->crypto);
}

} // namespace crypto
} // namespace librbd
