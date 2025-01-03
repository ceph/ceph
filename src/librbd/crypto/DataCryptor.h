// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_DATA_CRYPTOR_H
#define CEPH_LIBRBD_CRYPTO_DATA_CRYPTOR_H

#include "include/int_types.h"
#include "librbd/crypto/Types.h"

namespace librbd {
namespace crypto {

template <typename T>
class DataCryptor {

public:

  virtual ~DataCryptor() = default;

  virtual uint32_t get_block_size() const = 0;
  virtual uint32_t get_iv_size() const = 0;
  virtual const unsigned char* get_key() const = 0;
  virtual int get_key_length() const = 0;

  virtual T* get_context(CipherMode mode) = 0;
  virtual void return_context(T* ctx, CipherMode mode) = 0;

  virtual int init_context(T* ctx, const unsigned char* iv,
                           uint32_t iv_length) const = 0;
  virtual int update_context(T* ctx, const unsigned char* in,
                             unsigned char* out, uint32_t len) const = 0;
};

} // namespace crypto
} // namespace librbd

#endif // CEPH_LIBRBD_CRYPTO_DATA_CRYPTOR_H
