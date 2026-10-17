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

#include <span>
#include <iostream>
#include <string>
#include <string_view>
#include <algorithm>
#include <array>
#include <cctype>
#include <utility>
#include <cstdint>

#ifndef ID_TAG_SET_ENCODER_HPP
#define ID_TAG_SET_ENCODER_HPP

#include <bit>
#include <concepts>
#include <cstdint>
#include <string_view>
#include <array>
#include <utility>
#include <span>

#pragma once
namespace kvrgw {

//--------------------------------------------------------------------------------
constexpr bool
validate_characters_strict(std::string_view view, bool allow_all_spaces) noexcept
{
  bool hasNonSpace = false;

  // Validate allowed characters using explicit ASCII checks
  for (const char ch : view) {
    // Alphanumeric ASCII check
    if ((ch >= 'a' && ch <= 'z') ||
        (ch >= 'A' && ch <= 'Z') ||
        (ch >= '0' && ch <= '9')) {
      hasNonSpace = true;
      continue;
    }

    // Specific allowed punctuation symbols and standard space
    switch (ch) {
    case '_': case '.': case ':': case '/':
    case '=': case '+': case '-': case '@':
      hasNonSpace = true;
      continue;
    case ' ':
      // Leading, trailing, and middle spaces are all allowed
      continue;
    default:
      return false; // Found illegal character
    }
  }

  // Keys must have non-space chars. Values can be all spaces if allowed.
  return hasNonSpace || allow_all_spaces;
}

//--------------------------------------------------------------------------------
constexpr bool isValidIdTagKey(std::string_view key) noexcept
{
  // Check for empty keys or keys exceeding maximum length (128 bytes)
  if (key.empty() || key.length() > 128) {
    return false;
  }

  // Check for reserved case-insensitive "aws:" prefix
  if (key.length() >= 4) {
    if ((key[0] == 'a' || key[0] == 'A') &&
        (key[1] == 'w' || key[1] == 'W') &&
        (key[2] == 's' || key[2] == 'S') &&
        (key[3] == ':')) {
      return false;
    }
  }

  // Keys cannot be exclusively space characters
  return validate_characters_strict(key, /*allow_all_spaces=*/false);
}

//--------------------------------------------------------------------------------
constexpr bool isValidIdTagValue(std::string_view value) noexcept
{
  // Check for empty values (S3 allows completely empty/zero-length tag values)
  if (value.empty()) {
    return true;
  }

  // Check maximum length (256 characters/bytes)
  if (value.length() > 256) {
    return false;
  }

  // Values CAN be exclusively space characters per AWS S3 specification
  return validate_characters_strict(value, /*allow_all_spaces=*/true);
}

//=================================================================
// Class IDTagSetEncoder
//=================================================================
// --- Compile-Time Native-to-Big-Endian Utility ---
template <std::integral T>
constexpr T ensure_big_endian(T value) {
    if constexpr (std::endian::native != std::endian::big) {
        return std::byteswap(value); 
    }
    return value;
}

// --- Compile-Time Big-Endian-to-Native Utility ---
template <std::integral T>
constexpr T from_big_endian(T value) {
    if constexpr (std::endian::native != std::endian::big) {
        return std::byteswap(value); 
    }
    return value;
}

class IDTagSetEncoder {
public:
  static constexpr size_t BUFFER_SIZE = 8*1024;
  static constexpr size_t MAX_TAG_COUNT = 10;
  static constexpr size_t MAX_KEY_SIZE = 128;
  static constexpr size_t MAX_VALUE_SIZE = 256;

  using TagPair = std::pair<std::string_view, std::string_view>;

  IDTagSetEncoder() = default;

  // Serializes a span of key-value views
  bool set_tags(std::span<const TagPair> tags) {
    return set_tags_impl(tags);
  }

  // Consumes a raw pre-encoded data buffer array
  bool set_tags(std::array<uint8_t, BUFFER_SIZE> input_buffer);


  // Read-only accessors defined inline for optimal translation units
  inline std::span<const TagPair> get_tags() const {
    if (valid) {
      return std::span<const TagPair>(views_cache.data(), active_tag_count);
    }
    // should not happen, but ...
    return {};
  }

  inline std::span<const uint8_t> get_buffer() const {
    if (valid) {
      return std::span<const uint8_t>(buffer.data(), encoded_size);
    }
    // should not happen, but ...
    return {};
  }

  inline size_t size() const { return encoded_size; }
  inline bool is_valid() const { return valid; }

private:
  using tag_count_t = uint16_t;
  using tag_size_t  = uint16_t;

  // 2B key size + 2B value size
  static constexpr size_t SIZES_PER_TAG_BYTES = sizeof(tag_size_t) * 2;

  std::array<uint8_t, BUFFER_SIZE> buffer{};
  std::array<TagPair, MAX_TAG_COUNT> views_cache{};
  uint16_t encoded_size = 0;
  tag_count_t active_tag_count = 0;
  bool valid = false;
};

#endif // STACK_TAG_SET_ENCODER_HPP























































class IDTagSetEncoder {
public:
  static constexpr size_t BUFFER_SIZE = 8192;
  static constexpr size_t MAX_TAG_COUNT = 10;
  static constexpr size_t MAX_KEY_SIZE = 128;
  static constexpr size_t MAX_VALUE_SIZE = 256;

  using Tag = std::pair<std::string_view, std::string_view>'
private:
  // Fixed 8KB buffer as a continuous data member
  std::array<uint8_t, BUFFER_SIZE> buffer{};
  size_t encoded_size = 0;
  bool valid = false;

public:
  IDTagSetEncoder() = default;

  bool set_tags(std::span<const Tag>> tags) {
    return set_tags_impl(tags);
  }

private:
  bool set_tags_impl(std::span<const Tag>> tags)
  {
    valid = false;
    encoded_size = 0;

    // 1. Enforce max count constraint
    if (tags.size() > MAX_TAG_COUNT) {
      return false;
    }

    // TBD: must validate no repeating keys    


    const uint16_t tag_count = static_cast<uint16_t>(tags.size());
    const size_t sizes_array_bytes = tag_count * 4;
    const size_t metadata_bytes = 2 + sizes_array_bytes;

    // 2. Pre-verify individual bounds and calculate total capacity requirements
    size_t total_data_bytes = 0;
    for (const auto& [key, value] : tags) {
      if (key.size() > MAX_KEY_SIZE || value.size() > MAX_VALUE_SIZE) {
        return false;
      }
      total_data_bytes += (key.size() + value.size());
    }

    const size_t total_required_bytes = metadata_bytes + total_data_bytes;
    if (total_required_bytes > BUFFER_SIZE) {
      return false; // Fits well within 8KB, but keeps code memory-safe
    }

    // 3. Begin serialization into the data member array
    write_be16(&buffer[0], tag_count);

    uint8_t* size_ptr = &buffer[2];
    uint8_t* data_ptr = &buffer[metadata_bytes];

    // 4. Fill parallel metadata indices and value streams
    for (const auto& [key, value] : tags) {
      const uint16_t key_len = static_cast<uint16_t>(key.size());
      const uint16_t val_len = static_cast<uint16_t>(value.size());

      write_be16(size_ptr, key_len);
      write_be16(size_ptr + 2, val_len);
      size_ptr += 4;

      if (key_len > 0) {
        std::copy(key.begin(), key.end(), reinterpret_cast<char*>(data_ptr));
        data_ptr += key_len;
      }
      if (val_len > 0) {
        std::copy(value.begin(), value.end(), reinterpret_cast<char*>(data_ptr));
        data_ptr += val_len;
      }
    }

    encoded_size = total_required_bytes;
    valid = true;
    return true;
  }  

public:
    [[nodiscard]] std::span<const uint8_t> get_buffer() const {
        if (!valid) return {};
        return std::span<const uint8_t>(buffer.data(), encoded_size);
    }

    [[nodiscard]] size_t size() const { return encoded_size; }
    [[nodiscard]] bool is_valid() const { return valid; }
};








} //namespace kvrgw



int main() {
    std::cout << std::boolalpha;

    // Performance test utilizing zero-allocation string_view literals
    std::cout << "Valid key 'Environment': " << isValidIdTagKey("Environment") << "\n";
    std::cout << "Valid key 'Project-ID@2026': " << isValidIdTagKey("Project-ID@2026") << "\n";
    std::cout << "Invalid empty key: " << isValidIdTagKey("") << "\n";
    std::cout << "Invalid prefix 'aws:Managed': " << isValidIdTagKey("AWS:Managed") << "\n";
    std::cout << "Invalid character 'Cost,Center': " << isValidIdTagKey("Cost,Center") << "\n";

    return 0;
}
