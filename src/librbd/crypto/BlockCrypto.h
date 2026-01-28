// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_CRYPTO_BLOCK_CRYPTO_H
#define CEPH_LIBRBD_CRYPTO_BLOCK_CRYPTO_H

#include "include/Context.h"
#include "librbd/crypto/CryptoInterface.h"
#include "librbd/crypto/openssl/DataCryptor.h"

namespace librbd {
namespace crypto {

inline bool is_aead(const char* algo) {
  auto cipher = EVP_CIPHER_fetch(NULL, algo, NULL);
  if (cipher == nullptr) {
    return false;
  }
  return (EVP_CIPHER_flags(cipher) & EVP_CIPH_FLAG_AEAD_CIPHER) != 0;
}

template <typename T>
class BlockCrypto : public CryptoInterface {

public:
    static BlockCrypto* create(CephContext* cct, DataCryptor<T>* data_cryptor,
                               uint32_t block_size, uint64_t data_offset, uint32_t meta_size) {
      return new BlockCrypto(cct, data_cryptor, block_size, data_offset, meta_size);
    }
    
    BlockCrypto(CephContext* cct, DataCryptor<T>* data_cryptor,
                uint64_t block_size, uint64_t data_offset, uint32_t meta_size = 0);
  
    ~BlockCrypto();

    int encrypt(ceph::bufferlist* data, uint64_t image_offset) override;
    int decrypt(ceph::bufferlist* data, uint64_t image_offset) override;

    uint64_t get_block_size() const override {
      return m_block_size;
    }

    uint32_t get_meta_size() const override {
      return m_meta_size;
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
    uint32_t m_meta_size;
    uint32_t m_iv_size;

    int crypt(ceph::bufferlist* data, uint64_t image_offset, CipherMode mode);
};

} // namespace crypto
} // namespace librbd

extern template class librbd::crypto::BlockCrypto<EVP_CIPHER_CTX>;

#endif //CEPH_LIBRBD_CRYPTO_BLOCK_CRYPTO_H
