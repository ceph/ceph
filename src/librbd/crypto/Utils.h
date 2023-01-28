// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_UTILS_H
#define CEPH_LIBRBD_CRYPTO_UTILS_H

#include "include/Context.h"

namespace librbd {

struct ImageCtx;

namespace crypto {

class CryptoInterface;
template <typename> class EncryptionFormat;

namespace util {

template <typename ImageCtxT = librbd::ImageCtx>
void set_crypto(ImageCtxT *image_ctx,
                decltype(ImageCtxT::encryption_format) encryption_format);

int build_crypto(
        CephContext* cct, const unsigned char* key, uint32_t key_length,
        uint64_t block_size, uint64_t data_offset,
        std::unique_ptr<CryptoInterface>* result_crypto);

} // namespace util
} // namespace crypto
} // namespace librbd

#endif // CEPH_LIBRBD_CRYPTO_UTILS_H
