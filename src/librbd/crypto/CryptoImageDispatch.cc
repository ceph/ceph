// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/crypto/CryptoImageDispatch.h"

namespace librbd {
namespace crypto {

CryptoImageDispatch::CryptoImageDispatch(
        uint64_t data_offset) : m_data_offset(data_offset) {
}

void CryptoImageDispatch::remap_to_physical(io::Extents& image_extents,
                                            io::ImageArea area) {
  switch (area) {
  case io::ImageArea::DATA:
    for (auto& [off, _] : image_extents) {
      off += m_data_offset;
    }
    break;
  case io::ImageArea::CRYPTO_HEADER:
    // direct mapping
    break;
  default:
    ceph_abort();
  }
}

io::ImageArea CryptoImageDispatch::remap_to_logical(
    io::Extents& image_extents) {
  bool saw_data = false;
  bool saw_crypto_header = false;
  for (auto& [off, _] : image_extents) {
    if (off >= m_data_offset) {
      off -= m_data_offset;
      saw_data = true;
    } else {
      saw_crypto_header = true;
    }
  }
  if (saw_crypto_header) {
    ceph_assert(!saw_data);
    return io::ImageArea::CRYPTO_HEADER;
  }
  return io::ImageArea::DATA;
}

} // namespace crypto
} // namespace librbd
