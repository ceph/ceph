// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/crypto/openssl/DataCryptor.h"
#include <openssl/err.h>
#include <string.h>
#include "include/ceph_assert.h"
#include "include/compat.h"

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

  m_cipher = EVP_get_cipherbyname(cipher_name);
  if (m_cipher == nullptr) {
    lderr(m_cct) << "EVP_get_cipherbyname failed. Cipher name: " << cipher_name
                 << dendl;
    log_errors();
    return -EINVAL;
  }

  auto expected_key_length = EVP_CIPHER_key_length(m_cipher);
  if (expected_key_length != key_length) {
    lderr(m_cct) << "cipher " << cipher_name << " expects key of "
                 << expected_key_length << " bytes. got: " << key_length
                 << dendl;
    return -EINVAL;
  }

  m_key_size = key_length;
  m_key = new unsigned char[key_length];
  memcpy(m_key, key, key_length);
  m_iv_size = static_cast<uint32_t>(EVP_CIPHER_iv_length(m_cipher));
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
  return EVP_CIPHER_block_size(m_cipher);
}

uint32_t DataCryptor::get_iv_size() const {
  return m_iv_size;
}

const unsigned char* DataCryptor::get_key() const {
  return m_key;
}

int DataCryptor::get_key_length() const {
  return EVP_CIPHER_key_length(m_cipher);
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

  if (1 != EVP_CipherInit_ex(ctx, m_cipher, nullptr, m_key, nullptr, enc)) {
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

int DataCryptor::update_context(EVP_CIPHER_CTX* ctx, const unsigned char* in,
                                unsigned char* out, uint32_t len) const {
  int out_length;
  if (1 != EVP_CipherUpdate(ctx, out, &out_length, in, len)) {
    lderr(m_cct) << "EVP_CipherUpdate failed. len=" << len << dendl;
    log_errors();
    return -EIO;
  }
  return out_length;
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

} // namespace openssl
} // namespace crypto
} // namespace librbd
