// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_CRYPTO_MOCK_DATA_CRYPTOR_H
#define CEPH_TEST_LIBRBD_MOCK_CRYPTO_MOCK_DATA_CRYPTOR_H

#include "gmock/gmock.h"
#include "librbd/crypto/DataCryptor.h"

namespace librbd {
namespace crypto {

struct MockCryptoContext {};

class MockDataCryptor : public DataCryptor<MockCryptoContext> {

public:
  uint32_t block_size = 16;
  uint32_t iv_size = 16;

  uint32_t get_block_size() const override {
    return block_size;
  }

  uint32_t get_iv_size() const override {
    return iv_size;
  }

  MOCK_METHOD1(get_context, MockCryptoContext*(CipherMode));
  MOCK_METHOD2(return_context, void(MockCryptoContext*, CipherMode));
  MOCK_CONST_METHOD3(init_context, int(MockCryptoContext*,
                                       const unsigned char*, uint32_t));
  MOCK_CONST_METHOD4(update_context, int(MockCryptoContext*,
                                         const unsigned char*, unsigned char*,
                                         uint32_t));
  MOCK_CONST_METHOD0(get_key, const unsigned char*());
  MOCK_CONST_METHOD0(get_key_length, int());
};

} // namespace crypto
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_CRYPTO_MOCK_DATA_CRYPTOR_H
