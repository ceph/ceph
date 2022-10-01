// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_CRYPTO_MOCK_ENCRYPTION_FORMAT_H
#define CEPH_TEST_LIBRBD_MOCK_CRYPTO_MOCK_ENCRYPTION_FORMAT_H

#include "gmock/gmock.h"
#include "librbd/crypto/EncryptionFormat.h"
#include "test/librbd/mock/MockImageCtx.h"

namespace librbd {
namespace crypto {

struct MockEncryptionFormat : EncryptionFormat<MockImageCtx> {

  MOCK_METHOD2(format, void(MockImageCtx* ictx, Context* on_finish));
  MOCK_METHOD2(load, void(MockImageCtx* ictx, Context* on_finish));
  MOCK_METHOD0(get_crypto, ceph::ref_t<CryptoInterface>());
};

} // namespace crypto

namespace api {
namespace util {

inline int create_encryption_format(
        CephContext* cct, encryption_format_t format,
        encryption_options_t opts, size_t opts_size, bool c_api,
        crypto::EncryptionFormat<MockImageCtx>** result_format) {
  if (opts == nullptr) {
    return -ENOTSUP;
  }
  *result_format = (crypto::MockEncryptionFormat*)opts;
  return 0;
}

} // namespace util
} // namespace api
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_CRYPTO_MOCK_ENCRYPTION_FORMAT_H
