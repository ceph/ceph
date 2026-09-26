// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
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

#include "data_store.hpp"

#include "ref_tag.hpp"

#include <algorithm>
#include <cerrno>
#include <fstream>
#include <iostream>
#include <system_error>

namespace kvrgw {

// --- FileDataStore ---

FileDataStore::FileDataStore(std::filesystem::path root)
    : root_(std::move(root))
{
  std::filesystem::create_directories(root_);
}

std::string FileDataStore::ref_tag_to_filename(std::string_view ref_tag) const
{
  return RefTagGenerator::filename_for(ref_tag);
}

std::filesystem::path FileDataStore::path_for(std::string_view ref_tag) const
{
  return root_ / ref_tag_to_filename(ref_tag);
}

std::error_code FileDataStore::write(std::string_view ref_tag,
                                     std::string_view data)
{
  const auto path = path_for(ref_tag);
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) {
    return {errno, std::system_category()};
  }
  out.write(data.data(), static_cast<std::streamsize>(data.size()));
  if (!out) {
    return {errno, std::system_category()};
  }
  return {};
}

std::error_code FileDataStore::read(std::string_view ref_tag, uint64_t offset,
                                    uint64_t length, std::string *out)
{
  if (length == 0) {
    out->clear();
    return {};
  }

  const auto path = path_for(ref_tag);
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    return {errno, std::system_category()};
  }

  in.seekg(0, std::ios::end);
  if (!in) {
    return {errno, std::system_category()};
  }
  const auto end_pos = in.tellg();
  if (end_pos < 0) {
    return {errno, std::system_category()};
  }
  const uint64_t file_size = static_cast<uint64_t>(end_pos);

  uint64_t actual_offset = offset;
  if (length == UINT64_MAX) {
    actual_offset = 0;
    length = file_size;
  }
  if (actual_offset > file_size) {
    return std::make_error_code(std::errc::invalid_argument);
  }

  const uint64_t available = file_size - actual_offset;
  const uint64_t to_read = std::min(length, available);

  in.seekg(static_cast<std::streamoff>(actual_offset));
  if (!in) {
    return {errno, std::system_category()};
  }

  out->resize(static_cast<size_t>(to_read));
  in.read(out->data(), static_cast<std::streamsize>(to_read));
  if (in.gcount() != static_cast<std::streamsize>(to_read)) {
    return std::make_error_code(std::errc::io_error);
  }
  return {};
}

std::error_code FileDataStore::remove(std::string_view ref_tag)
{
  const auto path = path_for(ref_tag);
  std::error_code ec;
  std::filesystem::remove(path, ec);
  if (ec && ec != std::errc::no_such_file_or_directory) {
    std::cerr << "data_store remove failed: " << path.string() << ": "
              << ec.message() << std::endl;
    return ec;
  }
  return {};
}

// --- PerfDataStore ---

std::error_code PerfDataStore::write(std::string_view, std::string_view data)
{
  auto us = sim_write_us_.load(std::memory_order_relaxed);
  if (us > 0) {
    std::this_thread::sleep_for(std::chrono::microseconds(us));
  }
  return {};
}

std::error_code PerfDataStore::read(std::string_view, uint64_t, uint64_t length,
                                    std::string *out)
{
  auto us = sim_read_us_.load(std::memory_order_relaxed);
  if (us > 0) {
    std::this_thread::sleep_for(std::chrono::microseconds(us));
  }
  if (length != UINT64_MAX && length > 0) {
    out->assign(static_cast<size_t>(length), '\0');
  }
  else {
    out->clear();
  }
  return {};
}

std::error_code PerfDataStore::remove(std::string_view) { return {}; }

std::filesystem::path PerfDataStore::path_for(std::string_view) const
{
  return "/dev/null";
}

} // namespace kvrgw
