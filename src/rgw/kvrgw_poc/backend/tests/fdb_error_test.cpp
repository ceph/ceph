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

// FDB error-injection test suite.
//
// Verifies that every fdb_error_t injected at the FDB C API layer propagates
// correctly through kv_store with the exact error code, and that
// is_retriable / retry loops treat each code as expected.
//
// Approach: mock the FDB C API functions (link-time replacement — this binary
// does NOT link libfdb_c.so). Injection queues control what each mock future
// returns. Real kv_store.cpp is linked, so the full error-propagation path
// is exercised.

#include "error_codes.hpp"
#include "kv_store.hpp"

#include <cassert>
#include <cstring>
#include <deque>
#include <iostream>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Injection state
// ---------------------------------------------------------------------------

namespace inject {

struct GetResult {
  fdb_error_t wait_err = 0;
  fdb_error_t get_err = 0;
  bool present = false;
  std::string value;
};

struct CommitResult {
  fdb_error_t wait_err = 0;
  fdb_error_t get_err = 0;
};

struct RangeResult {
  fdb_error_t wait_err = 0;
  fdb_error_t get_err = 0;
  std::vector<std::pair<std::string, std::string>> rows;
};

static std::deque<GetResult> g_get_queue;
static std::deque<CommitResult> g_commit_queue;
static std::deque<RangeResult> g_range_queue;
static std::deque<fdb_error_t> g_create_txn_queue;

static int g_get_calls = 0;
static int g_commit_calls = 0;
static int g_range_calls = 0;
static int g_create_txn_calls = 0;

void reset()
{
  g_get_queue.clear();
  g_commit_queue.clear();
  g_range_queue.clear();
  g_create_txn_queue.clear();
  g_get_calls = 0;
  g_commit_calls = 0;
  g_range_calls = 0;
  g_create_txn_calls = 0;
}

void push_create_txn_error(fdb_error_t err)
{
  g_create_txn_queue.push_back(err);
}

void push_create_txn_success() { g_create_txn_queue.push_back(0); }

void push_get_error(fdb_error_t err)
{
  g_get_queue.push_back({0, err, false, {}});
}

void push_get_success(bool present, const std::string &value = {})
{
  g_get_queue.push_back({0, 0, present, value});
}

void push_commit_error(fdb_error_t err) { g_commit_queue.push_back({0, err}); }

void push_commit_success() { g_commit_queue.push_back({0, 0}); }

void push_range_error(fdb_error_t err)
{
  g_range_queue.push_back({0, err, {}});
}

void push_range_success(
    std::vector<std::pair<std::string, std::string>> rows = {})
{
  g_range_queue.push_back({0, 0, std::move(rows)});
}

} // namespace inject

// ---------------------------------------------------------------------------
// Mock FDB future
// ---------------------------------------------------------------------------

enum class FutureKind { GET, COMMIT, RANGE };

struct MockFuture {
  FutureKind kind;
  fdb_error_t wait_err = 0;
  fdb_error_t result_err = 0;
  bool present = false;
  std::string value;
  std::vector<FDBKeyValue> kv_array;
  std::vector<std::string> key_storage;
  std::vector<std::string> val_storage;
};

// ---------------------------------------------------------------------------
// Mock FDB C API
// ---------------------------------------------------------------------------

extern "C" {

fdb_error_t fdb_select_api_version_impl(int, int) { return 0; }

fdb_error_t fdb_create_database(const char *, FDBDatabase **out)
{
  static int dummy_db;
  *out = reinterpret_cast<FDBDatabase *>(&dummy_db);
  return 0;
}

void fdb_database_destroy(FDBDatabase *) {}

fdb_error_t fdb_database_create_transaction(FDBDatabase *, FDBTransaction **out)
{
  ++inject::g_create_txn_calls;
  if (!inject::g_create_txn_queue.empty()) {
    fdb_error_t err = inject::g_create_txn_queue.front();
    inject::g_create_txn_queue.pop_front();
    if (err) {
      *out = nullptr;
      return err;
    }
  }
  static int dummy_tr;
  *out = reinterpret_cast<FDBTransaction *>(&dummy_tr);
  return 0;
}

void fdb_transaction_destroy(FDBTransaction *) {}

void fdb_transaction_set(FDBTransaction *, const uint8_t *, int,
                         const uint8_t *, int)
{
}

void fdb_transaction_clear(FDBTransaction *, const uint8_t *, int) {}

void fdb_transaction_atomic_op(FDBTransaction *, const uint8_t *, int,
                               const uint8_t *, int, FDBMutationType)
{
}

FDBFuture *fdb_transaction_get(FDBTransaction *, const uint8_t *, int,
                               fdb_bool_t)
{
  auto *f = new MockFuture();
  f->kind = FutureKind::GET;
  ++inject::g_get_calls;
  if (!inject::g_get_queue.empty()) {
    auto front = inject::g_get_queue.front();
    inject::g_get_queue.pop_front();
    f->wait_err = front.wait_err;
    f->result_err = front.get_err;
    f->present = front.present;
    f->value = front.value;
  }
  else {
    f->present = false;
  }
  return reinterpret_cast<FDBFuture *>(f);
}

FDBFuture *fdb_transaction_commit(FDBTransaction *)
{
  auto *f = new MockFuture();
  f->kind = FutureKind::COMMIT;
  ++inject::g_commit_calls;
  if (!inject::g_commit_queue.empty()) {
    auto front = inject::g_commit_queue.front();
    inject::g_commit_queue.pop_front();
    f->wait_err = front.wait_err;
    f->result_err = front.get_err;
  }
  return reinterpret_cast<FDBFuture *>(f);
}

FDBFuture *fdb_transaction_get_range(FDBTransaction *, const uint8_t *, int,
                                     fdb_bool_t, int, const uint8_t *, int,
                                     fdb_bool_t, int, int, int,
                                     FDBStreamingMode, int, fdb_bool_t,
                                     fdb_bool_t)
{
  auto *f = new MockFuture();
  f->kind = FutureKind::RANGE;
  ++inject::g_range_calls;
  if (!inject::g_range_queue.empty()) {
    auto front = inject::g_range_queue.front();
    inject::g_range_queue.pop_front();
    f->wait_err = front.wait_err;
    f->result_err = front.get_err;
    if (f->wait_err == 0 && f->result_err == 0) {
      for (const auto &[k, v] : front.rows) {
        f->key_storage.push_back(k);
        f->val_storage.push_back(v);
      }
      for (size_t i = 0; i < f->key_storage.size(); ++i) {
        FDBKeyValue kv;
        kv.key = reinterpret_cast<const uint8_t *>(f->key_storage[i].data());
        kv.key_length = static_cast<int>(f->key_storage[i].size());
        kv.value = reinterpret_cast<const uint8_t *>(f->val_storage[i].data());
        kv.value_length = static_cast<int>(f->val_storage[i].size());
        f->kv_array.push_back(kv);
      }
    }
  }
  return reinterpret_cast<FDBFuture *>(f);
}

fdb_error_t fdb_future_block_until_ready(FDBFuture *future)
{
  auto *f = reinterpret_cast<MockFuture *>(future);
  return f->wait_err;
}

fdb_error_t fdb_future_get_value(FDBFuture *future, fdb_bool_t *out_present,
                                 const uint8_t **out_value,
                                 int *out_value_length)
{
  auto *f = reinterpret_cast<MockFuture *>(future);
  if (f->result_err) {
    return f->result_err;
  }
  *out_present = f->present ? 1 : 0;
  if (f->present) {
    *out_value = reinterpret_cast<const uint8_t *>(f->value.data());
    *out_value_length = static_cast<int>(f->value.size());
  }
  else {
    *out_value = nullptr;
    *out_value_length = 0;
  }
  return 0;
}

fdb_error_t fdb_future_get_error(FDBFuture *future)
{
  auto *f = reinterpret_cast<MockFuture *>(future);
  return f->result_err;
}

fdb_error_t fdb_future_get_keyvalue_array(FDBFuture *future,
                                          const FDBKeyValue **out_kv,
                                          int *out_count, fdb_bool_t *out_more)
{
  auto *f = reinterpret_cast<MockFuture *>(future);
  if (f->result_err) {
    return f->result_err;
  }
  *out_kv = f->kv_array.empty() ? nullptr : f->kv_array.data();
  *out_count = static_cast<int>(f->kv_array.size());
  *out_more = 0;
  return 0;
}

void fdb_future_destroy(FDBFuture *future)
{
  delete reinterpret_cast<MockFuture *>(future);
}

const char *fdb_get_error(fdb_error_t code)
{
  switch (code) {
  case 0:
    return "Success";
  case 1020:
    return "Transaction not committed due to conflict with another transaction";
  case 1021:
    return "Commit result is unknown";
  case 1025:
    return "Operation aborted because the transaction was cancelled";
  case 1031:
    return "Operation aborted because the transaction timed out";
  case 2000:
    return "Client invalid operation";
  case 4000:
    return "Io_error";
  case 1513:
    return "File not readable";
  case 4100:
    return "An internal error occurred";
  default:
    return "Unknown error";
  }
}

} // extern "C"

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

static int g_pass = 0;
static int g_fail = 0;

#define TEST(name)                                                             \
  static void test_##name();                                                   \
  static struct Register_##name {                                              \
    Register_##name() { tests().push_back({#name, test_##name}); }             \
  } reg_##name;                                                                \
  static void test_##name()

#define ASSERT_TRUE(expr)                                                      \
  do {                                                                         \
    if (!(expr)) {                                                             \
      std::cerr << "  FAIL: " #expr " at line " << __LINE__ << "\n";           \
      ++g_fail;                                                                \
      return;                                                                  \
    }                                                                          \
  } while (0)

#define ASSERT_EQ(a, b)                                                        \
  do {                                                                         \
    if ((a) != (b)) {                                                          \
      std::cerr << "  FAIL: " #a " == " #b " (" << (a) << " vs " << (b)        \
                << ") at line " << __LINE__ << "\n";                           \
      ++g_fail;                                                                \
      return;                                                                  \
    }                                                                          \
  } while (0)

struct TestEntry {
  const char *name;
  void (*fn)();
};

static std::vector<TestEntry> &tests()
{
  static std::vector<TestEntry> t;
  return t;
}

// ---------------------------------------------------------------------------
// Group 1: is_retriable
// ---------------------------------------------------------------------------

TEST(is_retriable_1020)
{
  ASSERT_TRUE(kvrgw::is_retriable(kvrgw::fdb_to_error(1020)));
}

TEST(is_retriable_0)
{
  ASSERT_TRUE(!kvrgw::is_retriable(kvrgw::fdb_to_error(0)));
}

TEST(is_retriable_1021)
{
  ASSERT_TRUE(kvrgw::is_retriable(kvrgw::fdb_to_error(1021)));
}

TEST(is_retriable_2000)
{
  ASSERT_TRUE(!kvrgw::is_retriable(kvrgw::fdb_to_error(2000)));
}

TEST(is_retriable_4000)
{
  ASSERT_TRUE(!kvrgw::is_retriable(kvrgw::fdb_to_error(4000)));
}

// ---------------------------------------------------------------------------
// Group 2: KvTransaction::get error propagation
// ---------------------------------------------------------------------------

TEST(txn_get_propagates_1020)
{
  inject::reset();
  inject::push_get_error(1020);
  auto store = *kvrgw::KvStore::create();
  auto tr = *store.begin_transaction();
  auto result = tr->kv_get("key");
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), static_cast<fdb_error_t>(1020));
}

TEST(txn_get_propagates_2000)
{
  inject::reset();
  inject::push_get_error(2000);
  auto store = *kvrgw::KvStore::create();
  auto tr = *store.begin_transaction();
  auto result = tr->kv_get("key");
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), static_cast<fdb_error_t>(2000));
}

TEST(txn_get_propagates_4000)
{
  inject::reset();
  inject::push_get_error(4000);
  auto store = *kvrgw::KvStore::create();
  auto tr = *store.begin_transaction();
  auto result = tr->kv_get("key");
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), static_cast<fdb_error_t>(4000));
}

TEST(txn_get_success_present)
{
  inject::reset();
  inject::push_get_success(true, "hello");
  auto store = *kvrgw::KvStore::create();
  auto tr = *store.begin_transaction();
  auto result = tr->kv_get("key");
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->has_value());
  ASSERT_EQ(**result, std::string("hello"));
}

TEST(txn_get_success_absent)
{
  inject::reset();
  inject::push_get_success(false);
  auto store = *kvrgw::KvStore::create();
  auto tr = *store.begin_transaction();
  auto result = tr->kv_get("key");
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(!result->has_value());
}

// ---------------------------------------------------------------------------
// Group 3: KvTransaction::commit error propagation
// ---------------------------------------------------------------------------

TEST(txn_commit_propagates_1020)
{
  inject::reset();
  inject::push_commit_error(1020);
  auto store = *kvrgw::KvStore::create();
  auto tr = *store.begin_transaction();
  auto result = tr->commit();
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), static_cast<fdb_error_t>(1020));
}

TEST(txn_commit_propagates_2000)
{
  inject::reset();
  inject::push_commit_error(2000);
  auto store = *kvrgw::KvStore::create();
  auto tr = *store.begin_transaction();
  auto result = tr->commit();
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), static_cast<fdb_error_t>(2000));
}

TEST(txn_commit_success)
{
  inject::reset();
  inject::push_commit_success();
  auto store = *kvrgw::KvStore::create();
  auto tr = *store.begin_transaction();
  auto result = tr->commit();
  ASSERT_TRUE(result.has_value());
}

// ---------------------------------------------------------------------------
// Group 4: KvTransaction::range_scan error propagation
// ---------------------------------------------------------------------------

TEST(txn_range_scan_propagates_1020)
{
  inject::reset();
  inject::push_range_error(1020);
  auto store = *kvrgw::KvStore::create();
  auto tr = *store.begin_transaction();
  auto result = tr->kv_range_scan("a", "z", 100);
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), static_cast<fdb_error_t>(1020));
}

TEST(txn_range_scan_propagates_4000)
{
  inject::reset();
  inject::push_range_error(4000);
  auto store = *kvrgw::KvStore::create();
  auto tr = *store.begin_transaction();
  auto result = tr->kv_range_scan("a", "z", 100);
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), static_cast<fdb_error_t>(4000));
}

TEST(txn_range_scan_success_empty)
{
  inject::reset();
  inject::push_range_success({});
  auto store = *kvrgw::KvStore::create();
  auto tr = *store.begin_transaction();
  auto result = tr->kv_range_scan("a", "z", 100);
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->empty());
}

TEST(txn_range_scan_success_rows)
{
  inject::reset();
  inject::push_range_success({{"k1", "v1"}, {"k2", "v2"}});
  auto store = *kvrgw::KvStore::create();
  auto tr = *store.begin_transaction();
  auto result = tr->kv_range_scan("a", "z", 100);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->size(), static_cast<size_t>(2));
  ASSERT_EQ((*result)[0].key, std::string("k1"));
  ASSERT_EQ((*result)[1].value, std::string("v2"));
}

// ---------------------------------------------------------------------------
// Group 5: KvStore::get error propagation
// ---------------------------------------------------------------------------

TEST(store_get_propagates_error)
{
  inject::reset();
  inject::push_get_error(2000);
  auto store = *kvrgw::KvStore::create();
  auto result = store.get("key");
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), static_cast<fdb_error_t>(2000));
}

TEST(store_get_success)
{
  inject::reset();
  inject::push_get_success(true, "val");
  auto store = *kvrgw::KvStore::create();
  auto result = store.get("key");
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->has_value());
  ASSERT_EQ(**result, std::string("val"));
}

// ---------------------------------------------------------------------------
// Group 6: KvStore::range_scan error propagation
// ---------------------------------------------------------------------------

TEST(store_range_scan_propagates_error)
{
  inject::reset();
  inject::push_range_error(4000);
  auto store = *kvrgw::KvStore::create();
  auto result = store.range_scan("a", "z", 10);
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), static_cast<fdb_error_t>(4000));
}

TEST(store_range_scan_success)
{
  inject::reset();
  inject::push_range_success({{"k", "v"}});
  auto store = *kvrgw::KvStore::create();
  auto result = store.range_scan("a", "z", 10);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->size(), static_cast<size_t>(1));
}

// ---------------------------------------------------------------------------
// Group 7: allocate_rgw_id retry and error-code preservation
// ---------------------------------------------------------------------------

TEST(allocate_retries_on_1020_commit)
{
  inject::reset();
  // get succeeds (counter absent) on each attempt, commit fails 3x then
  // succeeds
  for (int i = 0; i < 4; ++i) {
    inject::push_get_success(false);
  }
  inject::push_commit_error(1020);
  inject::push_commit_error(1020);
  inject::push_commit_error(1020);
  inject::push_commit_success();
  auto store = *kvrgw::KvStore::create();
  auto result = store.allocate_rgw_id();
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(*result, static_cast<uint32_t>(1));
  ASSERT_EQ(inject::g_commit_calls, 4);
}

TEST(allocate_retries_on_1020_get)
{
  inject::reset();
  // get fails 3x with 1020, then succeeds (absent), commit succeeds
  inject::push_get_error(1020);
  inject::push_get_error(1020);
  inject::push_get_error(1020);
  inject::push_get_success(false);
  inject::push_commit_success();
  auto store = *kvrgw::KvStore::create();
  auto result = store.allocate_rgw_id();
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(*result, static_cast<uint32_t>(1));
  ASSERT_EQ(inject::g_get_calls, 4);
}

TEST(allocate_stops_on_non_retriable_commit)
{
  inject::reset();
  inject::push_get_success(false);
  inject::push_commit_error(2000);
  auto store = *kvrgw::KvStore::create();
  auto result = store.allocate_rgw_id();
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), kvrgw::KVRGW_ERR_INTERNAL);
  ASSERT_EQ(inject::g_commit_calls, 1);
}

TEST(allocate_stops_on_non_retriable_get)
{
  inject::reset();
  inject::push_get_error(4000);
  auto store = *kvrgw::KvStore::create();
  auto result = store.allocate_rgw_id();
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), kvrgw::KVRGW_ERR_INTERNAL);
  ASSERT_EQ(inject::g_get_calls, 1);
}

TEST(allocate_exhausts_retries)
{
  inject::reset();
  for (int i = 0; i < 10; ++i) {
    inject::push_get_success(false);
    inject::push_commit_error(1020);
  }
  auto store = *kvrgw::KvStore::create();
  auto result = store.allocate_rgw_id();
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), kvrgw::KVRGW_ERR_MAX_RETRIES_EXCEEDED);
  ASSERT_EQ(inject::g_commit_calls, 10);
}

TEST(allocate_maps_unknown_fdb_code_to_internal)
{
  inject::reset();
  inject::push_get_success(false);
  inject::push_commit_error(4100);
  auto store = *kvrgw::KvStore::create();
  auto result = store.allocate_rgw_id();
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), kvrgw::KVRGW_ERR_INTERNAL);
}

TEST(allocate_fails_on_short_counter)
{
  inject::reset();
  inject::push_get_success(true, std::string(sizeof(uint64_t) - 1, '\0'));
  auto store = *kvrgw::KvStore::create();
  auto result = store.allocate_rgw_id();
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), kvrgw::KVRGW_ERR_CORRUPT_VALUE);
}

// ---------------------------------------------------------------------------
// Group 8: begin_transaction error propagation
// ---------------------------------------------------------------------------

TEST(begin_transaction_propagates_error)
{
  inject::reset();
  inject::push_create_txn_error(2000);
  auto store = *kvrgw::KvStore::create();
  auto tr = store.begin_transaction();
  ASSERT_TRUE(!tr);
  ASSERT_EQ(tr.error(), static_cast<fdb_error_t>(2000));
}

TEST(begin_transaction_success)
{
  inject::reset();
  auto store = *kvrgw::KvStore::create();
  auto tr = store.begin_transaction();
  ASSERT_TRUE(tr.has_value());
}

TEST(store_get_propagates_begin_txn_error)
{
  inject::reset();
  inject::push_create_txn_error(4000);
  auto store = *kvrgw::KvStore::create();
  auto result = store.get("key");
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), static_cast<fdb_error_t>(4000));
}

TEST(allocate_retries_begin_txn_1020)
{
  inject::reset();
  inject::push_create_txn_error(1020);
  inject::push_create_txn_error(1020);
  inject::push_create_txn_success();
  inject::push_get_success(false);
  inject::push_commit_success();
  auto store = *kvrgw::KvStore::create();
  auto result = store.allocate_rgw_id();
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(*result, static_cast<uint32_t>(1));
  ASSERT_EQ(inject::g_create_txn_calls, 3);
}

TEST(allocate_stops_begin_txn_non_retriable)
{
  inject::reset();
  inject::push_create_txn_error(2000);
  auto store = *kvrgw::KvStore::create();
  auto result = store.allocate_rgw_id();
  ASSERT_TRUE(!result);
  ASSERT_EQ(result.error(), kvrgw::KVRGW_ERR_INTERNAL);
  ASSERT_EQ(inject::g_create_txn_calls, 1);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main()
{
  for (const auto &t : tests()) {
    inject::reset();
    t.fn();
    ++g_pass;
    std::cout << "  PASS: " << t.name << "\n";
  }

  std::cout << "\nfdb_error_test: " << g_pass << " passed, " << g_fail
            << " failed\n";
  return g_fail > 0 ? 1 : 0;
}
