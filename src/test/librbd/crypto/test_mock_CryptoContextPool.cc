// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "librbd/crypto/CryptoContextPool.h"
#include "test/librbd/mock/crypto/MockDataCryptor.h"

#include "librbd/crypto/CryptoContextPool.cc"
template class librbd::crypto::CryptoContextPool<
        librbd::crypto::MockCryptoContext>;

using ::testing::Return;

namespace librbd {
namespace crypto {

struct TestMockCryptoCryptoContextPool : public ::testing::Test {
    MockDataCryptor cryptor;

    void expect_get_context(CipherMode mode) {
      EXPECT_CALL(cryptor, get_context(mode)).WillOnce(Return(
              new MockCryptoContext()));
    }

    void expect_return_context(MockCryptoContext* ctx, CipherMode mode) {
      delete ctx;
      EXPECT_CALL(cryptor, return_context(ctx, mode));
    }
};

TEST_F(TestMockCryptoCryptoContextPool, Test) {
  CryptoContextPool<MockCryptoContext> pool(&cryptor, 1);

  expect_get_context(CipherMode::CIPHER_MODE_ENC);
  auto enc_ctx = pool.get_context(CipherMode::CIPHER_MODE_ENC);

  expect_get_context(CipherMode::CIPHER_MODE_DEC);
  auto dec_ctx1 = pool.get_context(CipherMode::CIPHER_MODE_DEC);
  expect_get_context(CipherMode::CIPHER_MODE_DEC);
  auto dec_ctx2 = pool.get_context(CipherMode::CIPHER_MODE_DEC);
  pool.return_context(dec_ctx1, CipherMode::CIPHER_MODE_DEC);
  expect_return_context(dec_ctx2, CipherMode::CIPHER_MODE_DEC);
  pool.return_context(dec_ctx2, CipherMode::CIPHER_MODE_DEC);

  pool.return_context(enc_ctx, CipherMode::CIPHER_MODE_ENC);
  ASSERT_EQ(enc_ctx, pool.get_context(CipherMode::CIPHER_MODE_ENC));
  pool.return_context(enc_ctx, CipherMode::CIPHER_MODE_ENC);

  expect_return_context(enc_ctx, CipherMode::CIPHER_MODE_ENC);
  expect_return_context(dec_ctx1, CipherMode::CIPHER_MODE_DEC);
}

} // namespace crypto
} // namespace librbd
