// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "librbd/crypto/openssl/DataCryptor.h"

namespace librbd {
namespace crypto {
namespace openssl {

const char* TEST_CIPHER_NAME = "aes-256-xts";
const unsigned char TEST_KEY[64] = {1};
const unsigned char TEST_IV[16] = {2};
const unsigned char TEST_IV_2[16] = {3};
const unsigned char TEST_DATA[4096] = {4};

struct TestCryptoOpensslDataCryptor : public TestFixture {
    DataCryptor *cryptor;

    void SetUp() override {
      TestFixture::SetUp();
      cryptor = new DataCryptor(reinterpret_cast<CephContext*>(m_ioctx.cct()));
      ASSERT_EQ(0,
                cryptor->init(TEST_CIPHER_NAME, TEST_KEY, sizeof(TEST_KEY)));
    }

    void TearDown() override {
      delete cryptor;
      TestFixture::TearDown();
    }
};

TEST_F(TestCryptoOpensslDataCryptor, InvalidCipherName) {
  EXPECT_EQ(-EINVAL, cryptor->init(nullptr, TEST_KEY, sizeof(TEST_KEY)));
  EXPECT_EQ(-EINVAL, cryptor->init("", TEST_KEY, sizeof(TEST_KEY)));
  EXPECT_EQ(-EINVAL, cryptor->init("Invalid", TEST_KEY, sizeof(TEST_KEY)));
}

TEST_F(TestCryptoOpensslDataCryptor, InvalidKey) {
  EXPECT_EQ(-EINVAL, cryptor->init(TEST_CIPHER_NAME, nullptr, 0));
  EXPECT_EQ(-EINVAL, cryptor->init(TEST_CIPHER_NAME, nullptr,
                                   sizeof(TEST_KEY)));
  EXPECT_EQ(-EINVAL, cryptor->init(TEST_CIPHER_NAME, TEST_KEY, 1));
}

TEST_F(TestCryptoOpensslDataCryptor, GetContextInvalidMode) {
  EXPECT_EQ(nullptr, cryptor->get_context(static_cast<CipherMode>(-1)));
}

TEST_F(TestCryptoOpensslDataCryptor, ReturnNullContext) {
  cryptor->return_context(nullptr, static_cast<CipherMode>(-1));
}

TEST_F(TestCryptoOpensslDataCryptor, ReturnContextInvalidMode) {
  auto ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_DEC);
  ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);
  cryptor->return_context(ctx, static_cast<CipherMode>(-1));
}

TEST_F(TestCryptoOpensslDataCryptor, EncryptDecrypt) {
  auto ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);
  cryptor->init_context(ctx, TEST_IV, sizeof(TEST_IV));

  unsigned char out[sizeof(TEST_DATA)];
  ASSERT_EQ(sizeof(TEST_DATA),
            cryptor->update_context(ctx, TEST_DATA, out, sizeof(TEST_DATA)));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_ENC);
  ctx = cryptor->get_context(CipherMode::CIPHER_MODE_DEC);
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(0, cryptor->init_context(ctx, TEST_IV, sizeof(TEST_IV)));
  ASSERT_EQ(sizeof(TEST_DATA),
            cryptor->update_context(ctx, out, out, sizeof(TEST_DATA)));
  ASSERT_EQ(0, memcmp(out, TEST_DATA, sizeof(TEST_DATA)));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_DEC);
}

TEST_F(TestCryptoOpensslDataCryptor, ReuseContext) {
  auto ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);

  ASSERT_EQ(0, cryptor->init_context(ctx, TEST_IV, sizeof(TEST_IV)));
  unsigned char out[sizeof(TEST_DATA)];
  ASSERT_EQ(sizeof(TEST_DATA),
            cryptor->update_context(ctx, TEST_DATA, out, sizeof(TEST_DATA)));

  ASSERT_EQ(0, cryptor->init_context(ctx, TEST_IV_2, sizeof(TEST_IV_2)));
  ASSERT_EQ(sizeof(TEST_DATA),
            cryptor->update_context(ctx, TEST_DATA, out, sizeof(TEST_DATA)));

  auto ctx2 = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx2, nullptr);

  ASSERT_EQ(0, cryptor->init_context(ctx2, TEST_IV_2, sizeof(TEST_IV_2)));
  unsigned char out2[sizeof(TEST_DATA)];
  ASSERT_EQ(sizeof(TEST_DATA),
            cryptor->update_context(ctx2, TEST_DATA, out2, sizeof(TEST_DATA)));

  ASSERT_EQ(0, memcmp(out, out2, sizeof(TEST_DATA)));

  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_ENC);
  cryptor->return_context(ctx2, CipherMode::CIPHER_MODE_ENC);
}

TEST_F(TestCryptoOpensslDataCryptor, InvalidIVLength) {
  auto ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);

  ASSERT_EQ(-EINVAL, cryptor->init_context(ctx, TEST_IV, 1));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_ENC);
}

} // namespace openssl
} // namespace crypto
} // namespace librbd
