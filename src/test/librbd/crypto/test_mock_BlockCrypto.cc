// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "librbd/crypto/BlockCrypto.h"
#include "test/librbd/mock/crypto/MockDataCryptor.h"

#include "librbd/crypto/BlockCrypto.cc"
template class librbd::crypto::BlockCrypto<
        librbd::crypto::MockCryptoContext>;

using ::testing::ExpectationSet;
using ::testing::internal::ExpectationBase;
using ::testing::Return;
using ::testing::_;

namespace librbd {
namespace crypto {

MATCHER_P(CompareArrayToString, s, "") {
  return (memcmp(arg, s.c_str(), s.length()) == 0);
}

struct TestMockCryptoBlockCrypto : public TestFixture {
    MockDataCryptor cryptor;
    ceph::ref_t<BlockCrypto<MockCryptoContext>> bc;
    int cryptor_block_size = 2;
    int cryptor_iv_size = 16;
    int block_size = 4;
    int data_offset = 0;
    ExpectationSet* expectation_set;

    void SetUp() override {
      TestFixture::SetUp();

      cryptor.block_size = cryptor_block_size;
      bc = new BlockCrypto<MockCryptoContext>(
              reinterpret_cast<CephContext*>(m_ioctx.cct()), &cryptor,
              block_size, data_offset);
      expectation_set = new ExpectationSet();
    }

    void TearDown() override {
      delete expectation_set;
      TestFixture::TearDown();
    }

    void expect_get_context(CipherMode mode) {
      _set_last_expectation(
              EXPECT_CALL(cryptor, get_context(mode))
              .After(*expectation_set).WillOnce(Return(
                      new MockCryptoContext())));
    }

    void expect_init_context(const std::string& iv) {
      _set_last_expectation(
              EXPECT_CALL(cryptor, init_context(_, CompareArrayToString(iv),
                                                cryptor_iv_size))
              .After(*expectation_set));
    }

    void expect_update_context(const std::string& in_str, int out_ret) {
      _set_last_expectation(
              EXPECT_CALL(cryptor, update_context(_,
                                                  CompareArrayToString(in_str),
                                                  _, in_str.length()))
              .After(*expectation_set).WillOnce(Return(out_ret)));
    }

    void _set_last_expectation(ExpectationBase& expectation) {
      delete expectation_set;
      expectation_set = new ExpectationSet(expectation);
    }
};

TEST_F(TestMockCryptoBlockCrypto, Encrypt) {
  uint32_t image_offset = 0x1234 * block_size;

  ceph::bufferlist data1;
  data1.append("123");
  ceph::bufferlist data2;
  data2.append("456");
  ceph::bufferlist data3;
  data3.append("78");

  // bufferlist buffers: "123", "456", "78"
  ceph::bufferlist data;
  data.claim_append(data1);
  data.claim_append(data2);
  data.claim_append(data3);

  expect_get_context(CipherMode::CIPHER_MODE_ENC);
  expect_init_context(std::string("\x34\x12\0\0\0\0\0\0\0\0\0\0\0\0\0\0", 16));
  expect_update_context("1234", 4);
  expect_init_context(std::string("\x35\x12\0\0\0\0\0\0\0\0\0\0\0\0\0\0", 16));
  expect_update_context("5678", 4);
  EXPECT_CALL(cryptor, return_context(_, CipherMode::CIPHER_MODE_ENC));

  ASSERT_EQ(0, bc->encrypt(&data, image_offset));

  ASSERT_EQ(data.length(), 8);
  ASSERT_TRUE(data.is_aligned(block_size));
}

TEST_F(TestMockCryptoBlockCrypto, UnalignedImageOffset) {
  ceph::bufferlist data;
  data.append("1234");
  ASSERT_EQ(-EINVAL, bc->encrypt(&data, 2));
}

TEST_F(TestMockCryptoBlockCrypto, UnalignedDataLength) {
  ceph::bufferlist data;
  data.append("123");
  ASSERT_EQ(-EINVAL, bc->encrypt(&data, 0));
}

TEST_F(TestMockCryptoBlockCrypto, GetContextError) {
  ceph::bufferlist data;
  data.append("1234");
  EXPECT_CALL(cryptor, get_context(CipherMode::CIPHER_MODE_ENC)).WillOnce(
          Return(nullptr));
  ASSERT_EQ(-EIO, bc->encrypt(&data, 0));
}

TEST_F(TestMockCryptoBlockCrypto, InitContextError) {
  ceph::bufferlist data;
  data.append("1234");
  expect_get_context(CipherMode::CIPHER_MODE_ENC);
  EXPECT_CALL(cryptor, init_context(_, _, _)).WillOnce(Return(-123));
  ASSERT_EQ(-123, bc->encrypt(&data, 0));
}

TEST_F(TestMockCryptoBlockCrypto, UpdateContextError) {
  ceph::bufferlist data;
  data.append("1234");
  expect_get_context(CipherMode::CIPHER_MODE_ENC);
  EXPECT_CALL(cryptor, init_context(_, _, _));
  EXPECT_CALL(cryptor, update_context(_, _, _, _)).WillOnce(Return(-123));
  ASSERT_EQ(-123, bc->encrypt(&data, 0));
}

} // namespace crypto
} // namespace librbd
