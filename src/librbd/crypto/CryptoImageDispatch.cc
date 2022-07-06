// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/crypto/CryptoImageDispatch.h"

namespace librbd {
namespace crypto {

CryptoImageDispatch::CryptoImageDispatch(
        uint64_t data_offset) : m_data_offset(data_offset) {
}


void CryptoImageDispatch::remap_extents(
        io::Extents& image_extents, io::ImageExtentsMapType type) {
  if (type == io::IMAGE_EXTENTS_MAP_TYPE_LOGICAL_TO_PHYSICAL) {
    for (auto& extent: image_extents) {
      extent.first += m_data_offset;
    }
  } else if (type == io::IMAGE_EXTENTS_MAP_TYPE_PHYSICAL_TO_LOGICAL) {
    for (auto& extent: image_extents) {
      extent.first -= m_data_offset;
    }
  }
}

} // namespace crypto
} // namespace librbd
