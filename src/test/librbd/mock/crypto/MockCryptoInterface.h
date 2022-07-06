// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_CRYPTO_MOCK_CRYPTO_INTERFACE_H
#define CEPH_TEST_LIBRBD_MOCK_CRYPTO_MOCK_CRYPTO_INTERFACE_H

#include "include/buffer.h"
#include "gmock/gmock.h"
#include "librbd/crypto/CryptoInterface.h"

namespace librbd {
namespace crypto {

struct MockCryptoInterface : CryptoInterface {

  static const uint64_t BLOCK_SIZE = 4096;
  static const uint64_t DATA_OFFSET = 4 * 1024 * 1024;

  MOCK_METHOD2(encrypt, int(ceph::bufferlist*, uint64_t));
  MOCK_METHOD2(decrypt, int(ceph::bufferlist*, uint64_t));
  MOCK_CONST_METHOD0(get_key, const unsigned char*());
  MOCK_CONST_METHOD0(get_key_length, int());

  uint64_t get_block_size() const override {
    return BLOCK_SIZE;
  }

  uint64_t get_data_offset() const override {
    return DATA_OFFSET;
  }
};

} // namespace crypto
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_CRYPTO_MOCK_CRYPTO_INTERFACE_H
