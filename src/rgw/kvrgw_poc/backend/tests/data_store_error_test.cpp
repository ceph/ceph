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

// DataStore error-injection test suite.
//
// Exercises every FileDataStore error path via real filesystem triggers.
// No mocks — errors are caused by missing files, read-only dirs, bad offsets.

#include "data_store.hpp"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include <sys/stat.h>

namespace {

std::string make_ref_tag(char fill) { return std::string(12, fill); }

const auto kTmpRoot =
    std::filesystem::temp_directory_path() / "kv-ds-error-test";

int g_pass = 0;
int g_fail = 0;

#define RUN_TEST(name)                                                         \
  do {                                                                         \
    std::filesystem::remove_all(kTmpRoot);                                     \
    std::filesystem::create_directories(kTmpRoot);                             \
    test_##name();                                                             \
    ++g_pass;                                                                  \
    std::cout << "  PASS: " #name "\n";                                        \
  } while (0)

#define ASSERT_TRUE(expr)                                                      \
  do {                                                                         \
    if (!(expr)) {                                                             \
      std::cerr << "  FAIL: " #expr " at line " << __LINE__ << "\n";           \
      ++g_fail;                                                                \
      --g_pass;                                                                \
      return;                                                                  \
    }                                                                          \
  } while (0)

// --- 1. write success ---
void test_write_success()
{
  kvrgw::FileDataStore store(kTmpRoot / "store");
  auto rc = store.write(make_ref_tag('a'), "hello");
  ASSERT_TRUE(!rc);
}

// --- 2. write open fail (read-only dir) ---
void test_write_open_fail()
{
  auto dir = kTmpRoot / "readonly-store";
  std::filesystem::create_directories(dir);
  chmod(dir.c_str(), 0444);
  kvrgw::FileDataStore store(dir);
  auto rc = store.write(make_ref_tag('b'), "data");
  chmod(dir.c_str(), 0755);
  ASSERT_TRUE(rc);
  ASSERT_TRUE(rc.value() != 0);
}

// --- 3. read success full ---
void test_read_success_full()
{
  kvrgw::FileDataStore store(kTmpRoot / "store");
  const std::string payload = "hello world";
  auto wrc = store.write(make_ref_tag('c'), payload);
  ASSERT_TRUE(!wrc);
  std::string out;
  auto rrc = store.read_all(make_ref_tag('c'), &out);
  ASSERT_TRUE(!rrc);
  ASSERT_TRUE(out == payload);
}

// --- 4. read success partial ---
void test_read_success_partial()
{
  kvrgw::FileDataStore store(kTmpRoot / "store");
  const std::string payload = "0123456789abcdef";
  auto wrc = store.write(make_ref_tag('d'), payload);
  ASSERT_TRUE(!wrc);
  std::string out;
  auto rrc = store.read(make_ref_tag('d'), 5, 3, &out);
  ASSERT_TRUE(!rrc);
  ASSERT_TRUE(out == "567");
}

// --- 5. read missing file ---
void test_read_missing_file()
{
  kvrgw::FileDataStore store(kTmpRoot / "store");
  std::string out;
  auto rrc = store.read(make_ref_tag('e'), 0, 10, &out);
  ASSERT_TRUE(rrc);
  ASSERT_TRUE(rrc.value() != 0);
}

// --- 6. read_all missing file ---
void test_read_all_missing_file()
{
  kvrgw::FileDataStore store(kTmpRoot / "store");
  std::string out;
  auto rrc = store.read_all(make_ref_tag('f'), &out);
  ASSERT_TRUE(rrc);
  ASSERT_TRUE(rrc.value() != 0);
}

// --- 7. read offset past end ---
void test_read_offset_past_end()
{
  kvrgw::FileDataStore store(kTmpRoot / "store");
  auto wrc = store.write(make_ref_tag('g'), "short");
  ASSERT_TRUE(!wrc);
  std::string out;
  auto rrc = store.read(make_ref_tag('g'), 100, 1, &out);
  ASSERT_TRUE(rrc);
  ASSERT_TRUE(rrc == std::make_error_code(std::errc::invalid_argument));
}

// --- 8. remove success ---
void test_remove_success()
{
  kvrgw::FileDataStore store(kTmpRoot / "store");
  auto wrc = store.write(make_ref_tag('h'), "data");
  ASSERT_TRUE(!wrc);
  auto rrc = store.remove(make_ref_tag('h'));
  ASSERT_TRUE(!rrc);
  std::string out;
  auto check = store.read(make_ref_tag('h'), 0, 1, &out);
  ASSERT_TRUE(check);
}

// --- 9. remove idempotent (non-existent file) ---
void test_remove_idempotent()
{
  kvrgw::FileDataStore store(kTmpRoot / "store");
  auto rc = store.remove(make_ref_tag('i'));
  ASSERT_TRUE(!rc);
}

} // namespace

int main()
{
  RUN_TEST(write_success);
  RUN_TEST(write_open_fail);
  RUN_TEST(read_success_full);
  RUN_TEST(read_success_partial);
  RUN_TEST(read_missing_file);
  RUN_TEST(read_all_missing_file);
  RUN_TEST(read_offset_past_end);
  RUN_TEST(remove_success);
  RUN_TEST(remove_idempotent);

  std::filesystem::remove_all(kTmpRoot);
  std::cout << "\ndata_store_error_test: " << g_pass << " passed, " << g_fail
            << " failed\n";
  return g_fail > 0 ? 1 : 0;
}
