// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/crypto/openssl/DataCryptor.h"
#include <openssl/err.h>
#include <openssl/rand.h>
#include <openssl/crypto.h>
#include <string.h>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::openssl::DataCryptor: " << this << " "<< __func__ << ": "

namespace librbd {
namespace crypto {
namespace openssl {

int DataCryptor::init(const char* cipher_name, const unsigned char* key,
                      uint16_t key_length) {
  if (m_key != nullptr) {
    ceph_memzero_s(m_key, m_key_size, m_key_size);
    delete [] m_key;
    m_key = nullptr;
    m_key_size = 0;
  }
  if (cipher_name == nullptr) {
    lderr(m_cct) << "missing cipher name" << dendl;
    return -EINVAL;
  }
  if (key == nullptr) {
    lderr(m_cct) << "missing key" << dendl;
    return -EINVAL;
  }

  m_cipher.reset(EVP_CIPHER_fetch(NULL, cipher_name, NULL));
  if (m_cipher.get() == nullptr) {
    lderr(m_cct) << "EVP_get_cipherbyname failed. Cipher name: " << cipher_name
                 << dendl;
    log_errors();
    return -EINVAL;
  }

  auto expected_key_length = EVP_CIPHER_key_length(m_cipher.get());
  if (expected_key_length != key_length) {
    lderr(m_cct) << "cipher " << cipher_name << " expects key of "
                 << expected_key_length << " bytes. got: " << key_length
                 << dendl;
    return -EINVAL;
  }

  m_key_size = key_length;
  m_key = new unsigned char[key_length];
  memcpy(m_key, key, key_length);
  m_iv_size = static_cast<uint32_t>(EVP_CIPHER_iv_length(m_cipher.get()));
  return 0;
}

DataCryptor::~DataCryptor() {
  if (m_key != nullptr) {
    ceph_memzero_s(m_key, m_key_size, m_key_size);
    delete [] m_key;
    m_key = nullptr;
  }
}

uint32_t DataCryptor::get_block_size() const {
  return EVP_CIPHER_block_size(m_cipher.get());
}

uint32_t DataCryptor::get_iv_size() const {
  return m_iv_size;
}

const unsigned char* DataCryptor::get_key() const {
  return m_key;
}

int DataCryptor::get_key_length() const {
  return EVP_CIPHER_key_length(m_cipher.get());
}

EVP_CIPHER_CTX* DataCryptor::get_context(CipherMode mode) {
  int enc;
  switch(mode) {
    case CIPHER_MODE_ENC:
      enc = 1;
      break;
    case CIPHER_MODE_DEC:
      enc = 0;
      break;
    default:
      lderr(m_cct) << "Invalid CipherMode:" << mode << dendl;
      return nullptr;
  }

  auto ctx = EVP_CIPHER_CTX_new();
  if (ctx == nullptr) {
    lderr(m_cct) << "EVP_CIPHER_CTX_new failed" << dendl;
    log_errors();
    return nullptr;
  }

  if (1 != EVP_CipherInit_ex(ctx, m_cipher.get(), nullptr, m_key, nullptr, enc)) {
    lderr(m_cct) << "EVP_CipherInit_ex failed" << dendl;
    log_errors();
    return nullptr;
  }

  return ctx;
}

void DataCryptor::return_context(EVP_CIPHER_CTX* ctx, CipherMode mode) {
  if (ctx != nullptr) {
    EVP_CIPHER_CTX_free(ctx);
  }
}

int DataCryptor::init_context(EVP_CIPHER_CTX* ctx, const unsigned char* iv,
                              uint32_t iv_length) const {
  if (iv_length != m_iv_size) {
    lderr(m_cct) << "cipher expects IV of " << m_iv_size << " bytes. got: "
                 << iv_length << dendl;
    return -EINVAL;
  }
  if (1 != EVP_CipherInit_ex(ctx, nullptr, nullptr, nullptr, iv, -1)) {
    lderr(m_cct) << "EVP_CipherInit_ex failed" << dendl;
    log_errors();
    return -EIO;
  }
  return 0;
}

int DataCryptor::update_context(EVP_CIPHER_CTX* ctx, const CryptArgs& params) const {
  int result_len;
  if (1 != EVP_CipherUpdate(ctx, params.out, &result_len, params.in, params.len)) {
    lderr(m_cct) << "EVP_CipherUpdate failed. in_len=" << params.len << dendl;
    log_errors();
    return -EIO;
  }
  return result_len;
}

void DataCryptor::log_errors() const {
  while (true) {
    auto error = ERR_get_error();
    if (error == 0) {
      break;
    }
    lderr(m_cct) << "OpenSSL error: " << ERR_error_string(error, nullptr)
                 << dendl;
  }
}

int DataCryptor::decrypt(EVP_CIPHER_CTX *ctx, const CryptArgs& params) const {
  return update_context(ctx, params);
}

// AuthEncDataCryptor: authenc(hmac(sha256),xts(aes)) implementation

int AuthEncDataCryptor::init(const char* cipher_name, const unsigned char* key,
                              uint16_t key_length) {
  if (key_length != TOTAL_KEY_SIZE) {
    lderr(m_cct) << "authenc expects " << TOTAL_KEY_SIZE
                 << "-byte key, got: " << key_length << dendl;
    return -EINVAL;
  }

  // Store full volume key for get_key()
  m_volume_key.reset(new unsigned char[TOTAL_KEY_SIZE]);
  memcpy(m_volume_key.get(), key, TOTAL_KEY_SIZE);

  // Init XTS cipher with the first 64 bytes
  int r = DataCryptor::init(cipher_name, key, XTS_KEY_SIZE);
  if (r != 0) {
    return r;
  }

  // Store HMAC key (last 32 bytes)
  m_hmac_key.reset(new unsigned char[HMAC_KEY_SIZE]);
  memcpy(m_hmac_key.get(), key + XTS_KEY_SIZE, HMAC_KEY_SIZE);

  // Cache EVP_MAC and pre-initialized HMAC template context
  m_mac.reset(EVP_MAC_fetch(NULL, "HMAC", NULL));
  if (m_mac == nullptr) {
    lderr(m_cct) << "EVP_MAC_fetch(HMAC) failed" << dendl;
    log_errors();
    return -EINVAL;
  }

  m_mac_template.reset(EVP_MAC_CTX_new(m_mac.get()));
  if (m_mac_template == nullptr) {
    lderr(m_cct) << "EVP_MAC_CTX_new failed" << dendl;
    log_errors();
    return -EIO;
  }

  OSSL_PARAM mac_params[2];
  mac_params[0] = OSSL_PARAM_construct_utf8_string(
      OSSL_MAC_PARAM_DIGEST, const_cast<char*>("SHA256"), 0);
  mac_params[1] = OSSL_PARAM_construct_end();

  if (1 != EVP_MAC_init(m_mac_template.get(), m_hmac_key.get(),
                         HMAC_KEY_SIZE, mac_params)) {
    lderr(m_cct) << "EVP_MAC_init (template) failed" << dendl;
    log_errors();
    return -EIO;
  }

  // Override m_iv_size to match sector IV size (8 bytes LE sector number)
  m_iv_size = sizeof(ceph_le64);

  return 0;
}

EVP_CIPHER_CTX* AuthEncDataCryptor::get_context(CipherMode mode) {
  auto ctx = DataCryptor::get_context(mode);
  if (ctx != nullptr) {
    EVP_CIPHER_CTX_set_padding(ctx, 0);
  }
  return ctx;
}

const unsigned char* AuthEncDataCryptor::get_key() const {
  ceph_assert(CRYPTO_memcmp(m_volume_key.get(), m_key, m_key_size) == 0);
  ceph_assert(CRYPTO_memcmp(m_volume_key.get() + XTS_KEY_SIZE,
                             m_hmac_key.get(), HMAC_KEY_SIZE) == 0);
  return m_volume_key.get();
}

int AuthEncDataCryptor::get_key_length() const {
  return TOTAL_KEY_SIZE;
}

int AuthEncDataCryptor::compute_hmac(
    const unsigned char* sector_iv, uint32_t sector_iv_len,
    const unsigned char* iv, const unsigned char* data,
    uint32_t data_len, unsigned char* tag_out) const {
  // HMAC(key, sector_number_LE || IV || ciphertext)
  // Matches dm-crypt authenc AAD layout
  // Dup from pre-initialized template (key + SHA256 already set)
  auto mac_ctx = EVP_MAC_CTX_dup(m_mac_template.get());
  if (mac_ctx == nullptr) {
    lderr(m_cct) << "EVP_MAC_CTX_dup failed" << dendl;
    log_errors();
    return -EIO;
  }
  // AAD: sector_number_LE (8 bytes)
  if (1 != EVP_MAC_update(mac_ctx, sector_iv, sector_iv_len)) {
    lderr(m_cct) << "EVP_MAC_update (sector IV) failed" << dendl;
    log_errors();
    EVP_MAC_CTX_free(mac_ctx);
    return -EIO;
  }
  // AAD: random IV (16 bytes)
  if (1 != EVP_MAC_update(mac_ctx, iv, RANDOM_IV_SIZE)) {
    lderr(m_cct) << "EVP_MAC_update (IV) failed" << dendl;
    log_errors();
    EVP_MAC_CTX_free(mac_ctx);
    return -EIO;
  }
  // Data: ciphertext
  if (1 != EVP_MAC_update(mac_ctx, data, data_len)) {
    lderr(m_cct) << "EVP_MAC_update (data) failed" << dendl;
    log_errors();
    EVP_MAC_CTX_free(mac_ctx);
    return -EIO;
  }

  size_t tag_len = HMAC_SHA256_TAG_SIZE;
  if (1 != EVP_MAC_final(mac_ctx, tag_out, &tag_len, HMAC_SHA256_TAG_SIZE)) {
    lderr(m_cct) << "EVP_MAC_final failed" << dendl;
    log_errors();
    EVP_MAC_CTX_free(mac_ctx);
    return -EIO;
  }

  EVP_MAC_CTX_free(mac_ctx);
  return 0;
}

int AuthEncDataCryptor::init_context(EVP_CIPHER_CTX* ctx,
                                      const unsigned char* iv,
                                      uint32_t iv_length) const {
  // No-op: random IV is set per-block in update_context/decrypt
  return 0;
}

int AuthEncDataCryptor::update_context(EVP_CIPHER_CTX* ctx,
                                        const CryptArgs& params) const {
  if (params.meta_len != AUTHENC_OVERHEAD) {
    lderr(m_cct) << "metadata buffer size mismatch. "
                 << "got: " << params.meta_len
                 << ", expected: " << AUTHENC_OVERHEAD << dendl;
    return -EIO;
  }

  // Metadata layout: [HMAC tag (32) | random IV (16)]
  unsigned char* random_iv = params.meta + HMAC_SHA256_TAG_SIZE;

  // 1. Generate random 16-byte IV
  if (1 != RAND_bytes(random_iv, RANDOM_IV_SIZE)) {
    lderr(m_cct) << "RAND_bytes failed" << dendl;
    log_errors();
    return -EIO;
  }

  // 2. AES-256-XTS encrypt with random IV as tweak
  if (1 != EVP_CipherInit_ex(ctx, nullptr, nullptr, nullptr, random_iv, 1)) {
    lderr(m_cct) << "EVP_CipherInit_ex (set IV) failed" << dendl;
    log_errors();
    return -EIO;
  }
  int ciphertext_len = 0;
  if (1 != EVP_EncryptUpdate(ctx, params.out, &ciphertext_len,
                              params.in, params.len)) {
    lderr(m_cct) << "EVP_EncryptUpdate failed, len=" << params.len << dendl;
    log_errors();
    return -EIO;
  }

  if (std::cmp_not_equal(ciphertext_len, params.len)) {
    lderr(m_cct) << "ciphertext length mismatch: " << ciphertext_len
                 << " vs " << params.len << dendl;
    return -EIO;
  }

  // 3. HMAC-SHA256 over (sector_LE || IV || ciphertext)
  int r = compute_hmac(params.iv, params.iv_len, random_iv,
                       params.out, ciphertext_len, params.meta);
  if (r != 0) {
    return r;
  }

  return ciphertext_len;
}

int AuthEncDataCryptor::decrypt(EVP_CIPHER_CTX* ctx,
                                 const CryptArgs& params) const {
  if (params.meta_len != AUTHENC_OVERHEAD) {
    lderr(m_cct) << "metadata buffer size mismatch. "
                 << "got: " << params.meta_len
                 << ", expected: " << AUTHENC_OVERHEAD << dendl;
    return -EIO;
  }

  // Metadata layout: [HMAC tag (32) | random IV (16)]
  const unsigned char* stored_tag = params.meta;
  const unsigned char* iv = params.meta + HMAC_SHA256_TAG_SIZE;

  // 1. Verify HMAC over (sector_LE || IV || ciphertext)
  unsigned char computed_tag[HMAC_SHA256_TAG_SIZE];
  int r = compute_hmac(params.iv, params.iv_len, iv,
                       params.in, params.len, computed_tag);
  if (r != 0) {
    return r;
  }

  if (CRYPTO_memcmp(stored_tag, computed_tag, HMAC_SHA256_TAG_SIZE) != 0) {
    lderr(m_cct) << "HMAC verification failed" << dendl;
    return -EBADMSG;
  }

  // 2. AES-256-XTS decrypt with stored IV
  if (1 != EVP_CipherInit_ex(ctx, nullptr, nullptr, nullptr, iv, 0)) {
    lderr(m_cct) << "EVP_CipherInit_ex (set IV) failed" << dendl;
    log_errors();
    return -EIO;
  }
  int plaintext_len = 0;
  if (1 != EVP_DecryptUpdate(ctx, params.out, &plaintext_len,
                              params.in, params.len)) {
    lderr(m_cct) << "EVP_DecryptUpdate failed" << dendl;
    log_errors();
    return -EIO;
  }

  if (std::cmp_not_equal(plaintext_len, params.len)) {
    lderr(m_cct) << "plaintext length mismatch: " << plaintext_len
                 << " vs " << params.len << dendl;
    return -EIO;
  }

  return plaintext_len;
}

} // namespace openssl
} // namespace crypto
} // namespace librbd
