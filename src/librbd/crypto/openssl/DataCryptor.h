// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_CRYPTO_OPENSSL_DATA_CRYPTOR_H
#define CEPH_LIBRBD_CRYPTO_OPENSSL_DATA_CRYPTOR_H

#include "librbd/crypto/DataCryptor.h"
#include "include/Context.h"
#include <openssl/evp.h>

namespace librbd {
namespace crypto {
namespace openssl {

struct CipherDeleter {
    void operator()(EVP_CIPHER* c) { EVP_CIPHER_free(c); }
};
using CipherPtr = std::unique_ptr<EVP_CIPHER, CipherDeleter>;

class DataCryptor : public crypto::DataCryptor<EVP_CIPHER_CTX> {

public:
    DataCryptor(CephContext* cct) : m_cct(cct) {};
    ~DataCryptor();

    int init(const char* cipher_name, const unsigned char* key,
             uint16_t key_length);
    uint32_t get_block_size() const override;
    uint32_t get_iv_size() const override;
    const unsigned char* get_key() const override;
    int get_key_length() const override;

    EVP_CIPHER_CTX* get_context(CipherMode mode) override;
    void return_context(EVP_CIPHER_CTX* ctx, CipherMode mode) override;
    int init_context(EVP_CIPHER_CTX* ctx, const unsigned char* iv,
                     uint32_t iv_length) const override;
    int update_context(EVP_CIPHER_CTX* ctx, const unsigned char* in,
                       unsigned char* out, uint32_t in_len, uint32_t out_len) const override;

    int decrypt(EVP_CIPHER_CTX* ctx, const unsigned char* in,
                             unsigned char* out, uint32_t in_len, uint32_t out_len) const override;
protected:
    CephContext* m_cct;
    unsigned char* m_key = nullptr;
    uint16_t m_key_size = 0;
    CipherPtr m_cipher;
    uint32_t m_iv_size;

    void log_errors() const;
};

class AEADDataCryptor : public DataCryptor {
public:
    AEADDataCryptor(CephContext* cct) : DataCryptor(cct) {};

  int init_context(EVP_CIPHER_CTX* ctx, const unsigned char* iv, 
                    uint32_t iv_length) const override;

  int update_context(EVP_CIPHER_CTX* ctx, const unsigned char* in,
        unsigned char* out, uint32_t in_len, uint32_t out_len) const override;

  int decrypt(EVP_CIPHER_CTX* ctx, const unsigned char* in,
        unsigned char* out, uint32_t in_len, uint32_t out_len) const override;

private:
    static constexpr size_t AES_256_SIV_TAG_SIZE = 16;
    static constexpr size_t AES_256_SIV_NONCE_SIZE = 16;
    static constexpr size_t AES_256_SIV_OVERHEAD = AES_256_SIV_TAG_SIZE + AES_256_SIV_NONCE_SIZE;
};

} // namespace openssl
} // namespace crypto
} // namespace librbd

#endif // CEPH_LIBRBD_CRYPTO_OPENSSL_DATA_CRYPTOR_H
