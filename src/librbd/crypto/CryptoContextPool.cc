// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/crypto/CryptoContextPool.h"

namespace librbd {
namespace crypto {

template <typename T>
CryptoContextPool<T>::CryptoContextPool(DataCryptor<T>* data_cryptor,
                                        uint32_t pool_size)
     : m_data_cryptor(data_cryptor), m_encrypt_contexts(pool_size),
       m_decrypt_contexts(pool_size) {
}

template <typename T>
CryptoContextPool<T>::~CryptoContextPool() {
  T* ctx;
  while (m_encrypt_contexts.pop(ctx)) {
    m_data_cryptor->return_context(ctx, CipherMode::CIPHER_MODE_ENC);
  }
  while (m_decrypt_contexts.pop(ctx)) {
    m_data_cryptor->return_context(ctx, CipherMode::CIPHER_MODE_DEC);
  }
}

template <typename T>
T* CryptoContextPool<T>::get_context(CipherMode mode) {
  T* ctx;
  if (!get_contexts(mode).pop(ctx)) {
    ctx = m_data_cryptor->get_context(mode);
  }
  return ctx;
}

template <typename T>
void CryptoContextPool<T>::return_context(T* ctx, CipherMode mode) {
  if (!get_contexts(mode).push(ctx)) {
    m_data_cryptor->return_context(ctx, mode);
  }
}

} // namespace crypto
} // namespace librbd
