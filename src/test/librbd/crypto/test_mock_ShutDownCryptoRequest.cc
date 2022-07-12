// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/crypto/Utils.h"
#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/crypto/MockCryptoInterface.h"

#include "librbd/crypto/ShutDownCryptoRequest.cc"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace crypto {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArgs;

struct TestMockShutDownCryptoRequest : public TestMockFixture {
  typedef ShutDownCryptoRequest<MockTestImageCtx> MockShutDownCryptoRequest;

  MockTestImageCtx* mock_image_ctx;
  C_SaferCond finished_cond;
  Context *on_finish = &finished_cond;
  MockShutDownCryptoRequest* mock_shutdown_crypto_request;
  MockCryptoInterface* crypto;
  Context* shutdown_object_dispatch_context;
  Context* shutdown_image_dispatch_context;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    mock_image_ctx = new MockTestImageCtx(*ictx);
    crypto = new MockCryptoInterface();
    mock_image_ctx->crypto = crypto;
    mock_shutdown_crypto_request = MockShutDownCryptoRequest::create(
          mock_image_ctx, on_finish);
  }

  void TearDown() override {
    crypto->put();
    delete mock_image_ctx;
    TestMockFixture::TearDown();
  }

  void expect_crypto_object_layer_exists_check(bool exists) {
    EXPECT_CALL(*mock_image_ctx->io_object_dispatcher, exists(
            io::OBJECT_DISPATCH_LAYER_CRYPTO)).WillOnce(Return(exists));
  }

  void expect_crypto_image_layer_exists_check(bool exists) {
    EXPECT_CALL(*mock_image_ctx->io_image_dispatcher, exists(
            io::IMAGE_DISPATCH_LAYER_CRYPTO)).WillOnce(Return(exists));
  }

  void expect_shutdown_crypto_object_dispatch() {
    EXPECT_CALL(*mock_image_ctx->io_object_dispatcher, shut_down_dispatch(
            io::OBJECT_DISPATCH_LAYER_CRYPTO, _)).WillOnce(
                    WithArgs<1>(Invoke([this](Context* ctx) {
                      shutdown_object_dispatch_context = ctx;
    })));
  }

  void expect_shutdown_crypto_image_dispatch() {
    EXPECT_CALL(*mock_image_ctx->io_image_dispatcher, shut_down_dispatch(
            io::IMAGE_DISPATCH_LAYER_CRYPTO, _)).WillOnce(
                    WithArgs<1>(Invoke([this](Context* ctx) {
                      shutdown_image_dispatch_context = ctx;
    })));
  }
};

TEST_F(TestMockShutDownCryptoRequest, NoCryptoObjectDispatch) {
  expect_crypto_object_layer_exists_check(false);
  mock_shutdown_crypto_request->send();
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(nullptr, mock_image_ctx->crypto);
}

TEST_F(TestMockShutDownCryptoRequest, FailShutdownObjectDispatch) {
  expect_crypto_object_layer_exists_check(true);
  expect_shutdown_crypto_object_dispatch();
  mock_shutdown_crypto_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  shutdown_object_dispatch_context->complete(-EIO);
  ASSERT_EQ(-EIO, finished_cond.wait());
  ASSERT_NE(nullptr, mock_image_ctx->crypto);
}

TEST_F(TestMockShutDownCryptoRequest, NoCryptoImageDispatch) {
  expect_crypto_object_layer_exists_check(true);
  expect_shutdown_crypto_object_dispatch();
  mock_shutdown_crypto_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_crypto_image_layer_exists_check(false);
  shutdown_object_dispatch_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(nullptr, mock_image_ctx->crypto);
}

TEST_F(TestMockShutDownCryptoRequest, FailShutdownImageDispatch) {
  expect_crypto_object_layer_exists_check(true);
  expect_shutdown_crypto_object_dispatch();
  mock_shutdown_crypto_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_crypto_image_layer_exists_check(true);
  expect_shutdown_crypto_image_dispatch();
  shutdown_object_dispatch_context->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  shutdown_image_dispatch_context->complete(-EIO);
  ASSERT_EQ(-EIO, finished_cond.wait());
  ASSERT_NE(nullptr, mock_image_ctx->crypto);
}

TEST_F(TestMockShutDownCryptoRequest, Success) {
  expect_crypto_object_layer_exists_check(true);
  expect_shutdown_crypto_object_dispatch();
  mock_shutdown_crypto_request->send();
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  expect_crypto_image_layer_exists_check(true);
  expect_shutdown_crypto_image_dispatch();
  shutdown_object_dispatch_context->complete(0);
  ASSERT_EQ(ETIMEDOUT, finished_cond.wait_for(0));
  shutdown_image_dispatch_context->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
  ASSERT_EQ(nullptr, mock_image_ctx->crypto);
}

} // namespace crypto
} // namespace librbd
