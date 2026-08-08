// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_CRYPTO_OPENSSL_DATA_CRYPTOR_H
#define CEPH_LIBRBD_CRYPTO_OPENSSL_DATA_CRYPTOR_H

#include "librbd/crypto/DataCryptor.h"
#include "include/Context.h"
#include "include/compat.h"
#include <openssl/evp.h>
#include <openssl/core_names.h>

namespace librbd {
namespace crypto {
namespace openssl {

template <typename T, void(*FreeFn)(T*)>
struct OSSLDeleter {
    void operator()(T* p) const { FreeFn(p); }
};

using CipherPtr  = std::unique_ptr<EVP_CIPHER, OSSLDeleter<EVP_CIPHER, EVP_CIPHER_free>>;
using MACPtr     = std::unique_ptr<EVP_MAC, OSSLDeleter<EVP_MAC, EVP_MAC_free>>;
using MACCtxPtr  = std::unique_ptr<EVP_MAC_CTX, OSSLDeleter<EVP_MAC_CTX, EVP_MAC_CTX_free>>;

template <size_t N>
struct SecureKeyDeleter {
    void operator()(unsigned char* p) {
        ceph_memzero_s(p, N, N);
        delete[] p;
    }
};
template <size_t N>
using SecureKeyPtr = std::unique_ptr<unsigned char[], SecureKeyDeleter<N>>;

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
    virtual int update_context(EVP_CIPHER_CTX* ctx, const CryptArgs& params) const override;
    virtual int decrypt(EVP_CIPHER_CTX* ctx,  const CryptArgs& params) const override;

protected:
    CephContext* m_cct;
    unsigned char* m_key = nullptr;
    uint16_t m_key_size = 0;
    CipherPtr m_cipher;
    uint32_t m_iv_size;

    void log_errors() const;
};

// Implements authenc(hmac(sha256),xts(aes)) — AES-256-XTS encryption with
// HMAC-SHA256 authentication, matching the Linux kernel crypto API template.
//
// Key members:
//   m_key (inherited, 64 bytes): AES-256-XTS key, used by get_context()
//   m_hmac_key (32 bytes): HMAC-SHA256 key, used by compute_hmac()
//   m_volume_key (96 bytes): contiguous copy of full volume key [XTS || HMAC],
//                            returned by get_key() with consistency assertion
//
// Per-sector metadata layout (48 bytes, stored in CryptArgs::meta):
//   [0..31]  HMAC-SHA256 tag
//   [32..47] random IV (AES-XTS tweak)
//
// HMAC Input: sector_number_LE(8) || IV(16) || ciphertext
class AuthEncDataCryptor : public DataCryptor {
public:
    AuthEncDataCryptor(CephContext* cct) : DataCryptor(cct) {};

    int init(const char* cipher_name, const unsigned char* key,
             uint16_t key_length);

    EVP_CIPHER_CTX* get_context(CipherMode mode) override;
    int init_context(EVP_CIPHER_CTX* ctx, const unsigned char* iv,
                     uint32_t iv_length) const override;
    int update_context(EVP_CIPHER_CTX* ctx,
                       const CryptArgs& params) const override;
    int decrypt(EVP_CIPHER_CTX* ctx,
                const CryptArgs& params) const override;
    const unsigned char* get_key() const override;
    int get_key_length() const override;

    static constexpr size_t HMAC_SHA256_TAG_SIZE = 32;
    static constexpr size_t RANDOM_IV_SIZE = 16;
    static constexpr size_t AUTHENC_OVERHEAD = HMAC_SHA256_TAG_SIZE + RANDOM_IV_SIZE;
    static constexpr size_t HMAC_KEY_SIZE = 32;
    static constexpr size_t XTS_KEY_SIZE = 64;
    static constexpr size_t TOTAL_KEY_SIZE = XTS_KEY_SIZE + HMAC_KEY_SIZE;

private:
    int compute_hmac(const unsigned char* sector_iv, uint32_t sector_iv_len,
                     const unsigned char* iv, const unsigned char* data,
                     uint32_t data_len, unsigned char* tag_out) const;

    SecureKeyPtr<TOTAL_KEY_SIZE> m_volume_key;
    SecureKeyPtr<HMAC_KEY_SIZE> m_hmac_key;
    MACPtr m_mac;
    MACCtxPtr m_mac_template;
};

} // namespace openssl
} // namespace crypto
} // namespace librbd

#endif // CEPH_LIBRBD_CRYPTO_OPENSSL_DATA_CRYPTOR_H
