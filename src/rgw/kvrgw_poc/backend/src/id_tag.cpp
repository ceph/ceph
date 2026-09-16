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

#include "id_tag.hpp"

namespace kvrgw {

bool encoded_tag_frame_size(std::span<const uint8_t> input, size_t &out_size)
{
  out_size = 0;
  if (input.size() < sizeof(tag_count_t)) {
    return false;
  }

  const tag_count_t tag_count = read_be_field<tag_count_t>(input.data());
  if (!tag_count || tag_count > MAX_TAG_COUNT) {
    return false;
  }

  const size_t sizes_array_bytes = tag_count * SIZES_PER_TAG_BYTES;
  const size_t metadata_bytes = sizeof(tag_count_t) + sizes_array_bytes;
  if (input.size() < metadata_bytes) {
    return false;
  }

  const uint8_t *size_ptr = input.data() + sizeof(tag_count_t);
  size_t calculated_data_bytes = 0;
  for (tag_count_t i = 0; i < tag_count; ++i) {
    const tag_size_t key_len = read_be_field<tag_size_t>(size_ptr);
    const tag_size_t val_len =
        read_be_field<tag_size_t>(size_ptr + sizeof(tag_size_t));
    size_ptr += SIZES_PER_TAG_BYTES;
    if (!key_len || key_len > MAX_KEY_SIZE || val_len > MAX_VALUE_SIZE) {
      return false;
    }
    calculated_data_bytes += (key_len + val_len);
  }

  const size_t total_required_bytes = metadata_bytes + calculated_data_bytes;
  if (input.size() < total_required_bytes) {
    return false;
  }
  out_size = total_required_bytes;
  return true;
}

bool encode(std::span<const TagPair> tags_in, std::vector<uint8_t> &out)
{
  out.clear();

  if (tags_in.size() > MAX_TAG_COUNT) {
    return false;
  }
  if (!tags_in.size()) {
    return false;
  }

  const tag_count_t tag_count = static_cast<tag_count_t>(tags_in.size());
  std::span<const TagPair> tags_sorted;
  std::array<TagPair, MAX_TAG_COUNT> tags_arr;

  if (tag_count > 1) {
    tags_arr[0] = tags_in[0];
    for (tag_count_t i = 1; i < tag_count; ++i) {
      TagPair key = tags_in[i];
      int j = static_cast<int>(i) - 1;
      while (j >= 0 && tags_arr[j].first >= key.first) {
        if (tags_arr[j].first == key.first) {
          return false;
        }
        tags_arr[j + 1] = tags_arr[j];
        j--;
      }
      tags_arr[j + 1] = key;
    }
    tags_sorted = std::span<const TagPair>(tags_arr.data(), tag_count);
  }
  else {
    tags_sorted = tags_in;
  }

  const size_t sizes_array_bytes = tag_count * SIZES_PER_TAG_BYTES;
  const size_t metadata_bytes = sizeof(tag_count_t) + sizes_array_bytes;

  size_t total_data_bytes = 0;
  for (const auto &[key, value] : tags_sorted) {
    if (!key.size() || key.size() > MAX_KEY_SIZE ||
        value.size() > MAX_VALUE_SIZE) {
      return false;
    }
    total_data_bytes += (key.size() + value.size());
  }

  const size_t total_required_bytes = metadata_bytes + total_data_bytes;
  out.resize(total_required_bytes);

  write_be_field<tag_count_t>(out.data(), tag_count);

  uint8_t *size_ptr = out.data() + sizeof(tag_count_t);
  uint8_t *data_ptr = out.data() + metadata_bytes;

  for (size_t i = 0; i < tags_sorted.size(); ++i) {
    const auto &[key, value] = tags_sorted[i];
    const tag_size_t key_len = static_cast<tag_size_t>(key.size());
    const tag_size_t val_len = static_cast<tag_size_t>(value.size());

    write_be_field<tag_size_t>(size_ptr, key_len);
    write_be_field<tag_size_t>(size_ptr + sizeof(tag_size_t), val_len);
    size_ptr += SIZES_PER_TAG_BYTES;

    std::copy_n(key.begin(), key_len, reinterpret_cast<char *>(data_ptr));
    data_ptr += key_len;
    if (val_len > 0) {
      std::copy_n(value.begin(), val_len, reinterpret_cast<char *>(data_ptr));
      data_ptr += val_len;
    }
  }

  return true;
}

bool decode(std::span<const uint8_t> input_buffer,
            std::array<TagPair, MAX_TAG_COUNT> &out_tags)
{
  size_t total_required_bytes = 0;
  if (!encoded_tag_frame_size(input_buffer, total_required_bytes)) {
    return false;
  }

  const tag_count_t tag_count = read_be_field<tag_count_t>(input_buffer.data());
  const size_t metadata_bytes =
      sizeof(tag_count_t) + tag_count * SIZES_PER_TAG_BYTES;
  const uint8_t *size_ptr = input_buffer.data() + sizeof(tag_count_t);
  const uint8_t *data_ptr = input_buffer.data() + metadata_bytes;

  for (tag_count_t i = 0; i < tag_count; ++i) {
    const tag_size_t key_len = read_be_field<tag_size_t>(size_ptr);
    const tag_size_t val_len =
        read_be_field<tag_size_t>(size_ptr + sizeof(tag_size_t));
    size_ptr += SIZES_PER_TAG_BYTES;

    const char *key_start = reinterpret_cast<const char *>(data_ptr);
    data_ptr += key_len;
    const char *val_start = reinterpret_cast<const char *>(data_ptr);
    data_ptr += val_len;

    out_tags[i] = TagPair{std::string_view(key_start, key_len),
                          std::string_view(val_start, val_len)};
  }

  return true;
}

} // namespace kvrgw
