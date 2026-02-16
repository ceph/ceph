// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/crypto/BlockCrypto.h"
#include "include/buffer_fwd.h"
#include "include/byteorder.h"
#include "include/ceph_assert.h"
#include "include/rados/buffer_fwd.h"
#include "include/scope_guard.h"
#include "librbd/crypto/DataCryptor.h"
#include "librbd/crypto/Types.h"

#include <bit>
#include <cstddef>
#include <optional>
#include <stdlib.h>

namespace librbd {
namespace crypto {

template <typename T>
BlockCrypto<T>::BlockCrypto(CephContext* cct, DataCryptor<T>* data_cryptor,
                            uint64_t block_size, uint64_t data_offset, uint32_t meta_size)
     : m_cct(cct), m_data_cryptor(data_cryptor), m_block_size(block_size),
       m_data_offset(data_offset), m_meta_size(meta_size), m_iv_size(data_cryptor->get_iv_size()) {
  ceph_assert(std::has_single_bit(block_size));
  ceph_assert((block_size % data_cryptor->get_block_size()) == 0);
  ceph_assert((block_size % 512) == 0);
}

template <typename T>
BlockCrypto<T>::~BlockCrypto() {
  if (m_data_cryptor != nullptr) {
    delete m_data_cryptor;
    m_data_cryptor = nullptr;
  }
}

template <typename T>
int BlockCrypto<T>::crypt(ceph::bufferlist* data, uint64_t image_offset,
                           CipherMode mode) {
  size_t input_size = (mode == CipherMode::CIPHER_MODE_ENC) 
                              ? m_block_size 
                              : m_block_size + m_meta_size;
  if (image_offset % m_block_size != 0) {
    lderr(m_cct) << "image offset: " << image_offset
                 << " not aligned to block size: " << m_block_size << dendl;
    return -EINVAL;
  }
  if (data->length() % input_size != 0) {
    lderr(m_cct) << "data length: " << data->length()
                 << " not aligned to block size: " << m_block_size << dendl;
    return -EINVAL;
  }
  bufferlist meta;
  // make block to flush contiguous_appender(
  {
  bufferlist::iterator meta_iter;
  std::optional<bufferlist::contiguous_appender> meta_appender;
  auto sector_number = image_offset / 512;
  size_t num_blocks = data->length() / (input_size);
  if (m_meta_size != 0 && mode == CipherMode::CIPHER_MODE_DEC) {
    data->splice(data->length()- (num_blocks * m_meta_size), num_blocks * m_meta_size, &meta);
    meta_iter = meta.begin();
  } else if (m_meta_size != 0 && mode == CipherMode::CIPHER_MODE_ENC) {
    meta_appender.emplace(meta.get_contiguous_appender(num_blocks * m_meta_size));
  }

  bufferlist src = *data;
  data->clear();
  auto data_appender = data->get_contiguous_appender(num_blocks * m_block_size);
  auto src_iter = src.begin();
  auto ctx = m_data_cryptor->get_context(mode);
  if (ctx == nullptr) {
    lderr(m_cct) << "unable to get crypt context" << dendl;
    return -EIO;
  }
  auto sg = make_scope_guard([&] {
      m_data_cryptor->return_context(ctx, mode); });
  unsigned char* meta = (unsigned char*)alloca(m_meta_size);
  std::vector<unsigned char> iv(m_iv_size);
  std::vector<unsigned char> data_buf(m_block_size);
  for (size_t i = 0; i < num_blocks; ++i) {
    auto block_offset_le = ceph_le64(sector_number);
    memcpy(iv.data(), &block_offset_le, sizeof(block_offset_le));
    auto r = m_data_cryptor->init_context(ctx, iv.data(), m_iv_size);
    if (r != 0) {
      lderr(m_cct) << "unable to init cipher's IV" << dendl;
      return r;
    }
    // no need to interate over the items of a bufferlist manually. 
    src_iter.copy(m_block_size, reinterpret_cast<char*>(data_buf.data()));
    unsigned char* out_buf_ptr = reinterpret_cast<unsigned char*>(
        data_appender.get_pos_add(m_block_size));
    if (m_meta_size != 0) {
      if (mode == CIPHER_MODE_ENC) {
        meta = reinterpret_cast<unsigned char*>(
            meta_appender->get_pos_add(m_meta_size));
      } else {
        meta_iter.copy(m_meta_size, reinterpret_cast<char*>(meta));
      }
    }
    CryptArgs params;
    params.in = data_buf.data();
    params.out = out_buf_ptr;
    params.len = m_block_size;
    if (m_meta_size != 0) {
      params.iv = iv.data();
      params.iv_len = m_iv_size;
      params.meta = meta;
      params.meta_len = m_meta_size;
    }
    int crypto_output_length = (mode == CIPHER_MODE_DEC) ? 
                          m_data_cryptor->decrypt(ctx, params) : 
                          m_data_cryptor->update_context(ctx, params);
    if (crypto_output_length < 0) {
      lderr(m_cct) << "crypt update failed" << dendl;
      return crypto_output_length;
    }
    if (std::cmp_not_equal(crypto_output_length, m_block_size)) {
      lderr(m_cct) << "output size not expected\n expected: "
                    << m_block_size << " got: " << crypto_output_length
                    << dendl;
      return -EINVAL;
    }
    sector_number += m_block_size / 512;
  }
  // end block to flush contiguous_appender(
  }
  if (mode == CIPHER_MODE_ENC) {
    data->claim_append(meta);
  }
  return 0;
}

template <typename T>
int BlockCrypto<T>::encrypt(ceph::bufferlist* data, uint64_t image_offset) {
  return crypt(data, image_offset, CipherMode::CIPHER_MODE_ENC);
}

template <typename T>
int BlockCrypto<T>::decrypt(ceph::bufferlist* data, uint64_t image_offset) {
  return crypt(data, image_offset, CipherMode::CIPHER_MODE_DEC);
}

} // namespace crypto
} // namespace librbd

template class librbd::crypto::BlockCrypto<EVP_CIPHER_CTX>;
