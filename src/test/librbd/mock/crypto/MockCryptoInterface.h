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

  MOCK_CONST_METHOD2(encrypt, void(ceph::bufferlist&&, uint64_t));
  MOCK_CONST_METHOD2(decrypt, void(ceph::bufferlist&&, uint64_t));
};

} // namespace crypto
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_CRYPTO_MOCK_CRYPTO_INTERFACE_H
