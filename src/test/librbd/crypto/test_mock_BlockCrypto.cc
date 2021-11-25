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
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;
using ::testing::_;

namespace librbd {
namespace crypto {

MATCHER_P(CompareArrayToString, s, "") {
  return (memcmp(arg, s.c_str(), s.length()) == 0);
}

struct TestMockCryptoBlockCrypto : public TestFixture {
    MockDataCryptor* cryptor;
    ceph::ref_t<BlockCrypto<MockCryptoContext>> bc;
    int cryptor_block_size = 16;
    int cryptor_iv_size = 16;
    int block_size = 4096;
    int data_offset = 0;
    ExpectationSet* expectation_set;

    void SetUp() override {
      TestFixture::SetUp();

      cryptor = new MockDataCryptor();
      cryptor->block_size = cryptor_block_size;
      bc = new BlockCrypto<MockCryptoContext>(
              reinterpret_cast<CephContext*>(m_ioctx.cct()), cryptor,
              block_size, data_offset);
      expectation_set = new ExpectationSet();
    }

    void TearDown() override {
      delete expectation_set;
      bc->put();
      TestFixture::TearDown();
    }

    void expect_get_context(CipherMode mode) {
      _set_last_expectation(
              EXPECT_CALL(*cryptor, get_context(mode))
              .After(*expectation_set).WillOnce(Return(
                      new MockCryptoContext())));
    }

    void expect_return_context(CipherMode mode) {
      _set_last_expectation(
              EXPECT_CALL(*cryptor, return_context(_, mode))
              .After(*expectation_set).WillOnce(WithArg<0>(
                      Invoke([](MockCryptoContext* ctx) {
                        delete ctx;
                      }))));
    }

    void expect_init_context(const std::string& iv) {
      _set_last_expectation(
              EXPECT_CALL(*cryptor, init_context(_, CompareArrayToString(iv),
                                                cryptor_iv_size))
              .After(*expectation_set));
    }

    void expect_update_context(const std::string& in_str, int out_ret) {
      _set_last_expectation(
              EXPECT_CALL(*cryptor, update_context(_,
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
  uint32_t image_offset = 0x1230 * 512;

  ceph::bufferlist data1;
  data1.append(std::string(2048, '1'));
  ceph::bufferlist data2;
  data2.append(std::string(4096, '2'));
  ceph::bufferlist data3;
  data3.append(std::string(2048, '3'));

  ceph::bufferlist data;
  data.claim_append(data1);
  data.claim_append(data2);
  data.claim_append(data3);

  expect_get_context(CipherMode::CIPHER_MODE_ENC);
  expect_init_context(std::string("\x30\x12\0\0\0\0\0\0\0\0\0\0\0\0\0\0", 16));
  expect_update_context(std::string(2048, '1') + std::string(2048, '2'), 4096);
  expect_init_context(std::string("\x38\x12\0\0\0\0\0\0\0\0\0\0\0\0\0\0", 16));
  expect_update_context(std::string(2048, '2') + std::string(2048, '3'), 4096);
  expect_return_context(CipherMode::CIPHER_MODE_ENC);

  ASSERT_EQ(0, bc->encrypt(&data, image_offset));

  ASSERT_EQ(data.length(), 8192);
}

TEST_F(TestMockCryptoBlockCrypto, UnalignedImageOffset) {
  ceph::bufferlist data;
  data.append(std::string(4096, '1'));
  ASSERT_EQ(-EINVAL, bc->encrypt(&data, 2));
}

TEST_F(TestMockCryptoBlockCrypto, UnalignedDataLength) {
  ceph::bufferlist data;
  data.append(std::string(512, '1'));
  ASSERT_EQ(-EINVAL, bc->encrypt(&data, 0));
}

TEST_F(TestMockCryptoBlockCrypto, GetContextError) {
  ceph::bufferlist data;
  data.append(std::string(4096, '1'));
  EXPECT_CALL(*cryptor, get_context(CipherMode::CIPHER_MODE_ENC)).WillOnce(
          Return(nullptr));
  ASSERT_EQ(-EIO, bc->encrypt(&data, 0));
}

TEST_F(TestMockCryptoBlockCrypto, InitContextError) {
  ceph::bufferlist data;
  data.append(std::string(4096, '1'));
  expect_get_context(CipherMode::CIPHER_MODE_ENC);
  EXPECT_CALL(*cryptor, init_context(_, _, _)).WillOnce(Return(-123));
  expect_return_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_EQ(-123, bc->encrypt(&data, 0));
}

TEST_F(TestMockCryptoBlockCrypto, UpdateContextError) {
  ceph::bufferlist data;
  data.append(std::string(4096, '1'));
  expect_get_context(CipherMode::CIPHER_MODE_ENC);
  EXPECT_CALL(*cryptor, init_context(_, _, _));
  EXPECT_CALL(*cryptor, update_context(_, _, _, _)).WillOnce(Return(-123));
  expect_return_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_EQ(-123, bc->encrypt(&data, 0));
}

} // namespace crypto
} // namespace librbd
