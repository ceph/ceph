// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_BLOCK_CRYPTO_H
#define CEPH_LIBRBD_CRYPTO_BLOCK_CRYPTO_H

#include "include/Context.h"
#include "librbd/crypto/CryptoInterface.h"
#include "librbd/crypto/DataCryptor.h"

namespace librbd {
namespace crypto {

template <typename T>
class BlockCrypto : public CryptoInterface {

public:
    BlockCrypto(CephContext* cct, DataCryptor<T>* data_cryptor,
                uint64_t block_size);

    int encrypt(ceph::bufferlist* data, uint64_t image_offset) override;
    int decrypt(ceph::bufferlist* data, uint64_t image_offset) override;

    uint64_t get_block_size() const override {
      return m_block_size;
    }

private:
    CephContext* m_cct;
    DataCryptor<T>* m_data_cryptor;
    uint64_t m_block_size;
    uint32_t m_iv_size;

    int crypt(ceph::bufferlist* data, uint64_t image_offset, CipherMode mode);
};

} // namespace crypto
} // namespace librbd

#endif //CEPH_LIBRBD_CRYPTO_BLOCK_CRYPTO_H
