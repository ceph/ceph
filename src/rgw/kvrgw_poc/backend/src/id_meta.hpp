// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 sts=2 expandtab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef KVRGW_ID_META_HPP
#define KVRGW_ID_META_HPP

#include "id_tag.hpp"

#include <array>
#include <cstdint>
#include <span>
#include <string_view>
#include <utility>

namespace kvrgw {

using MetaPair = std::pair<std::string_view, std::string_view>;

static constexpr size_t MAX_META_COUNT = 128;
static constexpr size_t MAX_META_KEY_SIZE = 256;
static constexpr size_t MAX_META_VALUE_SIZE = 2048;
static constexpr size_t MAX_META_FRAME_BYTES = 2048;

bool encode_metadata(std::span<const MetaPair> meta, std::span<uint8_t> out_buf, size_t& out_size);
bool decode_metadata(std::span<const uint8_t> input_buffer,
                     std::array<MetaPair, MAX_META_COUNT>& out_meta);
bool encoded_metadata_frame_size(std::span<const uint8_t> input, size_t& out_size);

}  // namespace kvrgw

#endif // KVRGW_ID_META_HPP
