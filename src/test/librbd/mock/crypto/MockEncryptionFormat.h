// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_CRYPTO_MOCK_ENCRYPTION_FORMAT_H
#define CEPH_TEST_LIBRBD_MOCK_CRYPTO_MOCK_ENCRYPTION_FORMAT_H

#include "gmock/gmock.h"
#include "librbd/crypto/EncryptionFormat.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/crypto/MockCryptoInterface.h"

namespace librbd {
namespace crypto {

struct MockEncryptionFormat {
  MOCK_CONST_METHOD0(clone, std::unique_ptr<MockEncryptionFormat>());
  MOCK_METHOD2(format, void(MockImageCtx*, Context*));
  MOCK_METHOD2(load, void(MockImageCtx*, Context*));
  MOCK_METHOD0(get_crypto, MockCryptoInterface*());
};

} // namespace crypto
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_CRYPTO_MOCK_ENCRYPTION_FORMAT_H
