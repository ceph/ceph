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

#include "id_meta.hpp"

namespace kvrgw {

bool encoded_metadata_frame_size(std::span<const uint8_t> input,
                                 size_t &out_size)
{
  out_size = 0;
  if (input.size() < sizeof(tag_count_t)) {
    return false;
  }

  const tag_count_t count = read_be_field<tag_count_t>(input.data());
  if (!count || count > MAX_META_COUNT) {
    return false;
  }

  const size_t sizes_array_bytes = count * SIZES_PER_TAG_BYTES;
  const size_t header_bytes = sizeof(tag_count_t) + sizes_array_bytes;
  if (input.size() < header_bytes) {
    return false;
  }

  const uint8_t *size_ptr = input.data() + sizeof(tag_count_t);
  size_t calculated_data_bytes = 0;
  for (tag_count_t i = 0; i < count; ++i) {
    const tag_size_t key_len = read_be_field<tag_size_t>(size_ptr);
    const tag_size_t val_len =
        read_be_field<tag_size_t>(size_ptr + sizeof(tag_size_t));
    size_ptr += SIZES_PER_TAG_BYTES;
    if (!key_len || key_len > MAX_META_KEY_SIZE ||
        val_len > MAX_META_VALUE_SIZE) {
      return false;
    }
    calculated_data_bytes += (key_len + val_len);
  }

  const size_t total_required_bytes = header_bytes + calculated_data_bytes;
  if (total_required_bytes > MAX_META_FRAME_BYTES ||
      input.size() < total_required_bytes) {
    return false;
  }
  out_size = total_required_bytes;
  return true;
}

bool encode_metadata(std::span<const MetaPair> meta_in,
                     std::span<uint8_t> out_buf, size_t &out_size)
{
  out_size = 0;

  if (meta_in.size() > MAX_META_COUNT) {
    return false;
  }
  if (!meta_in.size()) {
    return false;
  }

  const tag_count_t count = static_cast<tag_count_t>(meta_in.size());
  std::span<const MetaPair> meta_sorted;
  std::array<MetaPair, MAX_META_COUNT> meta_arr;

  if (count > 1) {
    meta_arr[0] = meta_in[0];
    for (tag_count_t i = 1; i < count; ++i) {
      MetaPair key = meta_in[i];
      int j = static_cast<int>(i) - 1;
      while (j >= 0 && meta_arr[j].first >= key.first) {
        if (meta_arr[j].first == key.first) {
          return false;
        }
        meta_arr[j + 1] = meta_arr[j];
        j--;
      }
      meta_arr[j + 1] = key;
    }
    meta_sorted = std::span<const MetaPair>(meta_arr.data(), count);
  }
  else {
    meta_sorted = meta_in;
  }

  const size_t sizes_array_bytes = count * SIZES_PER_TAG_BYTES;
  const size_t header_bytes = sizeof(tag_count_t) + sizes_array_bytes;

  size_t total_data_bytes = 0;
  for (const auto &[key, value] : meta_sorted) {
    if (!key.size() || key.size() > MAX_META_KEY_SIZE ||
        value.size() > MAX_META_VALUE_SIZE) {
      return false;
    }
    total_data_bytes += (key.size() + value.size());
  }

  const size_t total_required_bytes = header_bytes + total_data_bytes;
  if (total_required_bytes > MAX_META_FRAME_BYTES ||
      total_required_bytes > out_buf.size()) {
    return false;
  }

  write_be_field<tag_count_t>(out_buf.data(), count);

  uint8_t *size_ptr = out_buf.data() + sizeof(tag_count_t);
  uint8_t *data_ptr = out_buf.data() + header_bytes;

  for (size_t i = 0; i < meta_sorted.size(); ++i) {
    const auto &[key, value] = meta_sorted[i];
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

  out_size = total_required_bytes;
  return true;
}

bool decode_metadata(std::span<const uint8_t> input_buffer,
                     std::array<MetaPair, MAX_META_COUNT> &out_meta)
{
  size_t total_required_bytes = 0;
  if (!encoded_metadata_frame_size(input_buffer, total_required_bytes)) {
    return false;
  }

  const tag_count_t count = read_be_field<tag_count_t>(input_buffer.data());
  const size_t header_bytes = sizeof(tag_count_t) + count * SIZES_PER_TAG_BYTES;
  const uint8_t *size_ptr = input_buffer.data() + sizeof(tag_count_t);
  const uint8_t *data_ptr = input_buffer.data() + header_bytes;

  for (tag_count_t i = 0; i < count; ++i) {
    const tag_size_t key_len = read_be_field<tag_size_t>(size_ptr);
    const tag_size_t val_len =
        read_be_field<tag_size_t>(size_ptr + sizeof(tag_size_t));
    size_ptr += SIZES_PER_TAG_BYTES;

    const char *key_start = reinterpret_cast<const char *>(data_ptr);
    data_ptr += key_len;
    const char *val_start = reinterpret_cast<const char *>(data_ptr);
    data_ptr += val_len;

    out_meta[i] = MetaPair{std::string_view(key_start, key_len),
                           std::string_view(val_start, val_len)};
  }

  return true;
}

} // namespace kvrgw
