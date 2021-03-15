// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_BLOCK_CRYPTO_H
#define CEPH_LIBRBD_CRYPTO_BLOCK_CRYPTO_H

#include "include/Context.h"
#include "librbd/crypto/CryptoInterface.h"
#include "librbd/crypto/openssl/DataCryptor.h"

namespace librbd {
namespace crypto {

template <typename T>
class BlockCrypto : public CryptoInterface {

public:
    static BlockCrypto* create(CephContext* cct, DataCryptor<T>* data_cryptor,
                               uint32_t block_size, uint64_t data_offset) {
      return new BlockCrypto(cct, data_cryptor, block_size, data_offset);
    }
    BlockCrypto(CephContext* cct, DataCryptor<T>* data_cryptor,
                uint64_t block_size, uint64_t data_offset);
    ~BlockCrypto();

    int encrypt(ceph::bufferlist* data, uint64_t image_offset) override;
    int decrypt(ceph::bufferlist* data, uint64_t image_offset) override;

    uint64_t get_block_size() const override {
      return m_block_size;
    }

    uint64_t get_data_offset() const override {
      return m_data_offset;
    }

    const unsigned char* get_key() const override {
      return m_data_cryptor->get_key();
    }

    int get_key_length() const override {
      return m_data_cryptor->get_key_length();
    }

private:
    CephContext* m_cct;
    DataCryptor<T>* m_data_cryptor;
    uint64_t m_block_size;
    uint64_t m_data_offset;
    uint32_t m_iv_size;

    int crypt(ceph::bufferlist* data, uint64_t image_offset, CipherMode mode);
};

} // namespace crypto
} // namespace librbd

extern template class librbd::crypto::BlockCrypto<EVP_CIPHER_CTX>;

#endif //CEPH_LIBRBD_CRYPTO_BLOCK_CRYPTO_H
