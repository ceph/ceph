// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/crypto/openssl/DataCryptor.h"
#include <openssl/err.h>
#include <openssl/rand.h>
#include <string.h>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::openssl::AEADDataCryptor: " << this << " "<< __func__ << ": "

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
  if (m_iv_size == 0) {
    // When AEAD cipher is used IV equals 0. However I still want to use IV 
    // as AAD.
    m_iv_size = sizeof(ceph_le64);
  }
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

int AEADDataCryptor::init_context(EVP_CIPHER_CTX* ctx, const unsigned char* iv, 
                                  uint32_t iv_length) const {
  // IV can be any length since we use it as AAD but this check is nice to have 
  // to catch some nasty bugs. 
  if (iv_length != m_iv_size) {
    lderr(m_cct) << "cipher expects IV of " << m_iv_size << " bytes. got: "
                 << iv_length << dendl;
    return -EINVAL;
  }
  // TODO: Maybe Rework AEAD context initialization to match 
  //  original init_context() semantics
  return 0;
}

  int AEADDataCryptor::update_context(EVP_CIPHER_CTX* ctx, const CryptArgs& params) const {
    int result_len = 0;
    int ciphertext_len = 0;
    if (params.meta_len != AES_256_SIV_OVERHEAD) {
      lderr(m_cct) << "Encryption buffer size mismatch. "
                   << "Input meata Length: " << params.meta_len
                   << ", Overhead Length: " << AES_256_SIV_OVERHEAD << dendl;
      log_errors();
      return -EIO;
    }
    // Data layout is: [ Data (len) | Tag (AES_256_SIV_TAG_SIZE) | Nonce (AES_256_SIV_NONCE_SIZE) ]
    unsigned char* random_nonce = params.meta + AES_256_SIV_TAG_SIZE;
    // TODO: Maybe replace with Ceph random
    if (1 != RAND_bytes(random_nonce, AES_256_SIV_NONCE_SIZE)) {
        lderr(m_cct) << "RAND_bytes failed" << dendl;
        log_errors();
        return -EIO;
    }
    // TODO: Maybe do full EVP_EncryptInit_ex re-init to match semantics 
    if (1 != EVP_CipherInit_ex(ctx, nullptr, nullptr, m_key, random_nonce, -1)) {
      lderr(m_cct) << "EVP_CipherInit_ex reset failed" << dendl;
      log_errors();
      return -EIO;
    }
    if (1 != EVP_EncryptUpdate(ctx, nullptr, &result_len, params.iv, params.iv_len)) {
      lderr(m_cct) << "Sector IV AAD update failed" << dendl;
      log_errors();
      return -EIO;
    }
    if (1 != EVP_EncryptUpdate(ctx, params.out, &result_len, params.in, params.len)) {
      lderr(m_cct) << "Failed encryption, len=" << params.len << dendl;
      log_errors();
      return -EIO;
    }
    ciphertext_len += result_len;
    if (1 != EVP_EncryptFinal_ex(ctx, params.out + result_len, &result_len)) {
      lderr(m_cct) << "Failed to finalize encryption, len=" << params.len << dendl;
      log_errors();
      return -EIO;
    }
    ciphertext_len += result_len;
    if (1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_AEAD_GET_TAG, AES_256_SIV_TAG_SIZE,
                                params.meta)) {
      lderr(m_cct) << "Failed to retrieve authentication tag, len=" << params.len << dendl;
      log_errors();
      return -EIO;
    }
    if (std::cmp_not_equal(ciphertext_len, params.len)) {
      lderr(m_cct) << "Decryption failed" << dendl;
      log_errors();
      return -EIO;
    }
    return ciphertext_len;
  }

  int AEADDataCryptor::decrypt(EVP_CIPHER_CTX* ctx, const CryptArgs& params) const {                      
    if (params.meta_len != ( AES_256_SIV_OVERHEAD)) {
      lderr(m_cct) << "Encryption buffer size mismatch. "
                   << "Input Length: " << params.meta_len
                   << ", Overhead Length: " << AES_256_SIV_OVERHEAD << dendl;
      log_errors();
      return -EIO;
    }
    int result_len = 0;
    int plaintext_len = 0;
    // Data layout is: [ Data (len) | Tag (AES_256_SIV_TAG_SIZE) | Nonce (AES_256_SIV_NONCE_SIZE) ]
    const unsigned char* tag = params.meta; //in + data_len;
    const unsigned char* nonce = params.meta + AES_256_SIV_TAG_SIZE;
    if (1 != EVP_CipherInit_ex(ctx, nullptr, nullptr, m_key, nonce, -1)) {
      lderr(m_cct) << "EVP_CipherInit_ex reset failed" << dendl;
      log_errors();
      return -EIO;
    }
    if (!EVP_CIPHER_CTX_ctrl(
            ctx, EVP_CTRL_AEAD_SET_TAG, AES_256_SIV_TAG_SIZE,
            const_cast<void*>(static_cast<const void*>(tag)))) {
      lderr(m_cct) << "failed to set auth-tag" << dendl;
      log_errors();
      return -EIO;
    }

    if (1 != EVP_DecryptUpdate(ctx, NULL, &result_len, params.iv, params.iv_len)) {
      lderr(m_cct) << "Failed to decrypt update context with nonce"<< dendl;
      log_errors();
      return -EIO;
    }
    if (1 != EVP_DecryptUpdate(ctx, params.out, &result_len, params.in, params.len)) {
      lderr(m_cct) << "Failed decryption" << dendl;
      log_errors();
      // TODO: Correct error code? 
      return -EBADMSG;
    }
    plaintext_len += result_len;
    if (1 != EVP_DecryptFinal_ex(ctx, params.out + result_len, &result_len)) {
      lderr(m_cct) << "Failed to finalize decryption" << dendl;
      log_errors();
      return -EIO;  
    }
    plaintext_len += result_len;
    if (std::cmp_not_equal(plaintext_len, params.len)) {
      lderr(m_cct) << "Decryption failed" << dendl;
      log_errors();
      return -EIO;
    }
    return plaintext_len;
}

} // namespace openssl
} // namespace crypto
} // namespace librbd
