// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CRYPTO_ONWIRE_AESGCM_H
#define CEPH_CRYPTO_ONWIRE_AESGCM_H

#include <array>

#include "crypto_onwire.h"

namespace ceph::crypto::onwire {

static constexpr const std::size_t AESGCM_KEY_LEN{16};
static constexpr const std::size_t AESGCM_IV_LEN{12};
static constexpr const std::size_t AESGCM_TAG_LEN{16};
static constexpr const std::size_t AESGCM_BLOCK_LEN{16};

struct nonce_t {
  ceph_le32 fixed;
  ceph_le64 counter;

  bool operator==(const nonce_t& rhs) const {
    return !memcmp(this, &rhs, sizeof(*this));
  }
} __attribute__((packed));
static_assert(sizeof(nonce_t) == AESGCM_IV_LEN);

using key_t = std::array<std::uint8_t, AESGCM_KEY_LEN>;

// http://www.mindspring.com/~dmcgrew/gcm-nist-6.pdf
// https://www.openssl.org/docs/man1.0.2/crypto/EVP_aes_128_gcm.html#GCM-mode
// https://wiki.openssl.org/index.php/EVP_Authenticated_Encryption_and_Decryption
// https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf
class AES128GCM_OnWireTxHandler : public ceph::crypto::onwire::TxHandler {
  CephContext* const cct;
  std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)> ectx;
  ceph::bufferlist buffer;
  nonce_t nonce, initial_nonce;
  bool used_initial_nonce;
  bool new_nonce_format;  // 64-bit counter?
  static_assert(sizeof(nonce) == AESGCM_IV_LEN);

public:
  AES128GCM_OnWireTxHandler(CephContext* const cct,
			    const key_t& key,
			    const nonce_t& nonce,
                            bool new_nonce_format);
  ~AES128GCM_OnWireTxHandler() override;

  void reset_tx_handler(const uint32_t* first, const uint32_t* last) override;

  void authenticated_encrypt_update(const ceph::bufferlist& plaintext) override;
  ceph::bufferlist authenticated_encrypt_final() override;
};


// RX PART
class AES128GCM_OnWireRxHandler : public ceph::crypto::onwire::RxHandler {
  std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)> ectx;
  nonce_t nonce;
  bool new_nonce_format;  // 64-bit counter?
  static_assert(sizeof(nonce) == AESGCM_IV_LEN);

public:
  AES128GCM_OnWireRxHandler(CephContext* const cct,
			    const key_t& key,
			    const nonce_t& nonce,
			    bool new_nonce_format);
  ~AES128GCM_OnWireRxHandler() override;

  std::uint32_t get_extra_size_at_final() override {
    return AESGCM_TAG_LEN;
  }
  void reset_rx_handler() override;
  void authenticated_decrypt_update(ceph::bufferlist& bl) override;
  void authenticated_decrypt_update_final(ceph::bufferlist& bl) override;
};

} // namespace ceph::crypto::onwire

#endif // CEPH_CRYPTO_ONWIRE_AESGCM_H
