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

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>

namespace kvrgw {

class DataStore {
 public:
  virtual ~DataStore() = default;

  virtual std::error_code write(std::string_view ref_tag, std::string_view data) = 0;
  virtual std::error_code read(std::string_view ref_tag, uint64_t offset, uint64_t length, std::string* out) = 0;
  virtual std::error_code remove(std::string_view ref_tag) = 0;
  virtual std::filesystem::path path_for(std::string_view ref_tag) const = 0;

  std::error_code read_all(std::string_view ref_tag, std::string* out) {
    return read(ref_tag, 0, UINT64_MAX, out);
  }
};

class FileDataStore : public DataStore {
 public:
  explicit FileDataStore(std::filesystem::path root);

  std::error_code write(std::string_view ref_tag, std::string_view data) override;
  std::error_code read(std::string_view ref_tag, uint64_t offset, uint64_t length, std::string* out) override;
  std::error_code remove(std::string_view ref_tag) override;
  std::filesystem::path path_for(std::string_view ref_tag) const override;

 private:
  std::string ref_tag_to_filename(std::string_view ref_tag) const;
  std::filesystem::path root_;
};

class PerfDataStore : public DataStore {
 public:
  std::error_code write(std::string_view ref_tag, std::string_view data) override;
  std::error_code read(std::string_view ref_tag, uint64_t offset, uint64_t length, std::string* out) override;
  std::error_code remove(std::string_view ref_tag) override;
  std::filesystem::path path_for(std::string_view ref_tag) const override;

  void set_sim_write_us(int64_t us) { sim_write_us_.store(us, std::memory_order_relaxed); }
  void set_sim_read_us(int64_t us) { sim_read_us_.store(us, std::memory_order_relaxed); }
  int64_t sim_write_us() const { return sim_write_us_.load(std::memory_order_relaxed); }
  int64_t sim_read_us() const { return sim_read_us_.load(std::memory_order_relaxed); }

 private:
  std::atomic<int64_t> sim_write_us_{0};
  std::atomic<int64_t> sim_read_us_{0};
};

}  // namespace kvrgw
