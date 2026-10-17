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
#include "constants.hpp"

#include <array>
#include <cstdint>
#include <span>
#include <string_view>
#include <utility>

namespace kvrgw {

using MetaPair = std::pair<std::string_view, std::string_view>;
static constexpr std::string_view AWS_METADATA_HDR = "x-amz-meta-";
static constexpr size_t AWS_METADATA_HDR_SIZE = AWS_METADATA_HDR.length();
static constexpr size_t AWS_METADATA_SHORTEST_KEY_LEN = AWS_METADATA_HDR_SIZE + 1; 

// Worst case key-count (all values empty, wire budget = 2048):
//  36 × (1+11) +  124 × (2+11) = 432 + 1612 = 2044 bytes → 160 entries
//  36 1-byte keys (plus 11 bytes 'x-amz-meta-' prefix)
// 124 2-byte keys (plus 11 bytes 'x-amz-meta-' prefix)
// 36x(1+11) + 124x(2+11) = 2044
static constexpr size_t MAX_META_COUNT = 160;
static constexpr size_t MAX_META_KEY_SIZE = AWS_MaxMetadataBytes - AWS_METADATA_HDR_SIZE;
static constexpr size_t MAX_META_VALUE_SIZE = AWS_MaxMetadataBytes - AWS_METADATA_SHORTEST_KEY_LEN;

// Storage frame layout: [tag_count_t | N×(key_len,val_len) | key0 | val0 | ...]
// Keys are stored WITHOUT the "x-amz-meta-" prefix (11 bytes stripped per key).
// Wire budget: AWS_MaxMetadataBytes = 2048 bytes (sum of full header names + values).
// Stripping 11 bytes/key means stored frames are always smaller than the wire frame.
// Tightest case: 1 entry → 2 + 4 + 1 + 2036 = 2043 bytes.
// We use AWS_MaxMetadataBytes (2048) as the ceiling — simple, safe, 5-byte margin.
static constexpr size_t MAX_META_FRAME_BYTES = AWS_MaxMetadataBytes;

bool encode_metadata(std::span<const MetaPair> meta, std::span<uint8_t> out_buf, size_t& out_size);
bool decode_metadata(std::span<const uint8_t> input_buffer,
                     std::array<MetaPair, MAX_META_COUNT>& out_meta);
bool encoded_metadata_frame_size(std::span<const uint8_t> input, size_t& out_size);

}  // namespace kvrgw

#endif // KVRGW_ID_META_HPP
