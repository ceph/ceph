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

#ifndef KVRGW_ID_TAG_HPP
#define KVRGW_ID_TAG_HPP

#include <algorithm>
#include <array>
#include <bit>
#include <concepts>
#include <cstdint>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace kvrgw {

namespace detail {
  template <std::integral T>
  constexpr T process_endian(T value) {
    if constexpr (std::endian::native != std::endian::big) {
      return std::byteswap(value);
    }
    return value;
  }
} // namespace detail

template <std::integral T>
constexpr T to_big_endian(T value) {
  return detail::process_endian(value);
}

template <std::integral T>
constexpr T from_big_endian(T value) {
  return detail::process_endian(value);
}

template <std::integral T>
constexpr inline T read_be_field(const uint8_t* source_ptr) {
    std::array<uint8_t, sizeof(T)> temp_bytes{};
    std::copy_n(source_ptr, sizeof(T), temp_bytes.begin());
    return from_big_endian(std::bit_cast<T>(temp_bytes));
}

template <std::integral T>
constexpr inline void write_be_field(uint8_t* dest_ptr, T value) {
    const T encoded = to_big_endian(value);
    const auto bytes = std::bit_cast<std::array<uint8_t, sizeof(T)>>(encoded);
    std::copy(bytes.begin(), bytes.end(), dest_ptr);
}

using tag_count_t = uint16_t;
using tag_size_t  = uint16_t;
using TagPair     = std::pair<std::string_view, std::string_view>;

static constexpr size_t MAX_TAG_COUNT = 10;
static constexpr size_t MAX_KEY_SIZE = 128;
static constexpr size_t MAX_VALUE_SIZE = 256;
static constexpr size_t SIZES_PER_TAG_BYTES = sizeof(tag_size_t) * 2;

bool encode(std::span<const TagPair> tags, std::vector<uint8_t>& out);
bool decode(std::span<const uint8_t> input_buffer, std::array<TagPair, MAX_TAG_COUNT>& out_tags);
bool encoded_tag_frame_size(std::span<const uint8_t> input, size_t& out_size);

}  // namespace kvrgw

#endif // KVRGW_ID_TAG_HPP
