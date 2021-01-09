// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/crypto/BlockCrypto.h"
#include "include/byteorder.h"
#include "include/ceph_assert.h"

#include <stdlib.h>

namespace librbd {
namespace crypto {

template <typename T>
BlockCrypto<T>::BlockCrypto(CephContext* cct, DataCryptor<T>* data_cryptor,
                            uint64_t block_size, uint64_t data_offset)
     : m_cct(cct), m_data_cryptor(data_cryptor), m_block_size(block_size),
       m_data_offset(data_offset), m_iv_size(data_cryptor->get_iv_size()) {
  ceph_assert(isp2(block_size));
  ceph_assert((block_size % data_cryptor->get_block_size()) == 0);
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
  if (image_offset % m_block_size != 0) {
    lderr(m_cct) << "image offset: " << image_offset
                 << " not aligned to block size: " << m_block_size << dendl;
    return -EINVAL;
  }
  if (data->length() % m_block_size != 0) {
    lderr(m_cct) << "data length: " << data->length()
                 << " not aligned to block size: " << m_block_size << dendl;
    return -EINVAL;
  }

  unsigned char* iv = (unsigned char*)alloca(m_iv_size);
  memset(iv, 0, m_iv_size);

  bufferlist src = *data;
  data->clear();
  src.rebuild_aligned_size_and_memory(m_block_size, CEPH_PAGE_SIZE);

  auto ctx = m_data_cryptor->get_context(mode);
  if (ctx == nullptr) {
    lderr(m_cct) << "unable to get crypt context" << dendl;
    return -EIO;
  }
  auto block_offset = image_offset / m_block_size;
  auto appender = data->get_contiguous_appender(src.length());
  unsigned char* out_buf_ptr = nullptr;
  uint32_t remaining_block_bytes = 0;
  for (auto buf = src.buffers().begin(); buf != src.buffers().end(); ++buf) {
    auto in_buf_ptr = reinterpret_cast<const unsigned char*>(buf->c_str());
    auto remaining_buf_bytes = buf->length();
    while (remaining_buf_bytes > 0) {
      if (remaining_block_bytes == 0) {
        auto block_offset_le = init_le64(block_offset);
        memcpy(iv, &block_offset_le, sizeof(block_offset_le));
        auto r = m_data_cryptor->init_context(ctx, iv, m_iv_size);
        if (r != 0) {
          lderr(m_cct) << "unable to init cipher's IV" << dendl;
          return r;
        }

        out_buf_ptr = reinterpret_cast<unsigned char*>(
                appender.get_pos_add(m_block_size));
        remaining_block_bytes = m_block_size;
        ++block_offset;
      }

      auto crypto_input_length = std::min(remaining_buf_bytes,
                                          remaining_block_bytes);
      auto crypto_output_length = m_data_cryptor->update_context(
              ctx, in_buf_ptr, out_buf_ptr, crypto_input_length);
      if (crypto_output_length < 0) {
        lderr(m_cct) << "crypt update failed" << dendl;
        return crypto_output_length;
      }

      out_buf_ptr += crypto_output_length;
      in_buf_ptr += crypto_input_length;
      remaining_buf_bytes -= crypto_input_length;
      remaining_block_bytes -= crypto_input_length;
    }
  }

  m_data_cryptor->return_context(ctx, mode);

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
