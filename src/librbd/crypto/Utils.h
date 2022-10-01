// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_UTILS_H
#define CEPH_LIBRBD_CRYPTO_UTILS_H

#include "include/Context.h"
#include "librbd/crypto/CryptoInterface.h"

namespace librbd {

struct ImageCtx;

namespace crypto {
namespace util {

template <typename ImageCtxT = librbd::ImageCtx>
void set_crypto(ImageCtxT *image_ctx, ceph::ref_t<CryptoInterface> crypto);

int build_crypto(
        CephContext* cct, const unsigned char* key, uint32_t key_length,
        uint64_t block_size, uint64_t data_offset,
        ceph::ref_t<CryptoInterface>* result_crypto);

} // namespace util
} // namespace crypto
} // namespace librbd

#endif // CEPH_LIBRBD_CRYPTO_UTILS_H
