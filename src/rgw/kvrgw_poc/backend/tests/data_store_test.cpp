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

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

namespace {

std::string make_ref_tag(char fill) { return std::string(12, fill); }

void test_read_all_and_partial()
{
  const auto tmp =
      std::filesystem::temp_directory_path() / "kv-data-store-test";
  std::filesystem::remove_all(tmp);
  kvrgw::FileDataStore store(tmp);

  std::string payload;
  payload.reserve(4096);
  for (int i = 0; i < 4096; ++i) {
    payload.push_back(static_cast<char>(i & 0xFF));
  }

  const auto ref_tag = make_ref_tag('a');
  auto wrc = store.write(ref_tag, payload);
  assert(!wrc);

  std::string all;
  auto rc = store.read_all(ref_tag, &all);
  assert(!rc);
  assert(all == payload);

  for (uint64_t offset : {0U, 1U, 17U, 100U, 1024U, 4095U}) {
    for (uint64_t length : {1U, 4U, 100U, 512U}) {
      if (offset + length > payload.size()) {
        continue;
      }
      std::string got;
      auto ec = store.read(ref_tag, offset, length, &got);
      assert(!ec);
      assert(got.size() == length);
      assert(got == payload.substr(static_cast<size_t>(offset),
                                   static_cast<size_t>(length)));
    }
  }

  std::string tail;
  auto ec = store.read(ref_tag, 4000, 96, &tail);
  assert(!ec);
  assert(tail == payload.substr(4000));

  std::string empty;
  ec = store.read(ref_tag, 0, 0, &empty);
  assert(!ec);
  assert(empty.empty());
}

void test_large_file_partial_read()
{
  const auto tmp =
      std::filesystem::temp_directory_path() / "kv-data-store-test-large";
  std::filesystem::remove_all(tmp);
  kvrgw::FileDataStore store(tmp);

  constexpr size_t kSize = 4 * 1024 * 1024;
  std::string payload(kSize, '\0');
  for (size_t i = 0; i < kSize; ++i) {
    payload[i] = static_cast<char>((i * 131) & 0xFF);
  }

  const auto ref_tag = make_ref_tag('b');
  auto wrc = store.write(ref_tag, payload);
  assert(!wrc);

  const uint64_t offset = 3 * 1024 * 1024 + 123;
  const uint64_t length = 4096;
  std::string got;
  auto ec = store.read(ref_tag, offset, length, &got);
  assert(!ec);
  assert(got.size() == length);
  assert(got == payload.substr(static_cast<size_t>(offset),
                               static_cast<size_t>(length)));
}

} // namespace

int main()
{
  test_read_all_and_partial();
  test_large_file_partial_read();
  std::cout << "data_store_test passed\n";
  return 0;
}
