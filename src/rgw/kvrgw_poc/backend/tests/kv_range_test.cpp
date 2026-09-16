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

#include "error_codes.hpp"
#include "fdb.hpp"
#include "keys.hpp"
#include "kv_store.hpp"
#include "object_value.hpp"

#include <cassert>
#include <chrono>
#include <iostream>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>

namespace {

void run_network()
{
  if (fdb_error_t err = fdb_run_network()) {
    std::cerr << "fdb_run_network failed: " << fdb_get_error(err) << std::endl;
    std::abort();
  }
}

std::string prefix_range_end(std::string_view prefix)
{
  std::string end(prefix);
  while (!end.empty()) {
    const unsigned char last = static_cast<unsigned char>(end.back());
    if (last < 0xFF) {
      end.back() = static_cast<char>(last + 1);
      return end;
    }
    end.pop_back();
  }
  return std::string("\xFF", 1);
}

int64_t now_unix()
{
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

void txn_put(kvrgw::KvStore &store, std::string_view key,
             std::string_view value)
{
  auto tr = store.begin_transaction();
  assert(tr);
  (*tr)->kv_put(key, value);
  auto rc = (*tr)->commit();
  assert(rc);
}

void txn_del(kvrgw::KvStore &store, std::string_view key)
{
  auto tr = store.begin_transaction();
  assert(tr);
  (*tr)->kv_del(key);
  auto rc = (*tr)->commit();
  assert(rc);
}

void cleanup_tenant_buckets(kvrgw::KvStore &store, kvrgw::tenant_id_t tenant_id)
{
  const auto prefix = kvrgw::make_bucket_prefix(tenant_id);
  const auto end = prefix_range_end(prefix.view());
  auto rows = store.range_scan(prefix.view(), end, 0);
  assert(rows);
  for (const auto &row : *rows) {
    txn_del(store, row.key);
  }
}

} // namespace

int main()
{
  fdb_error_t code = fdb_select_api_version(FDB_API_VERSION);
  if (code) {
    std::cerr << "fdb_select_api_version failed: " << fdb_get_error(code)
              << "\n";
    return 1;
  }
  if (fdb_error_t net_err = fdb_setup_network()) {
    std::cerr << "fdb_setup_network failed: " << fdb_get_error(net_err) << "\n";
    return 1;
  }
  std::thread network_thread(run_network);

  try {
    auto store_result = kvrgw::KvStore::create();
    if (!store_result) {
      std::cerr << "fdb_create_database failed: "
                << fdb_get_error(store_result.error()) << "\n";
      if (fdb_error_t e = fdb_stop_network()) {
        std::cerr << "fdb_stop_network failed: " << fdb_get_error(e) << "\n";
      }
      network_thread.join();
      return 1;
    }
    auto &store = *store_result;
    const kvrgw::tenant_id_t tenant_id = 99999;
    cleanup_tenant_buckets(store, tenant_id);

    const std::string run_tag =
        std::to_string(static_cast<long long>(getpid()));
    const std::string names[] = {
        "range-test-" + run_tag + "-bucket1",
        "range-test-" + run_tag + "-bucket2",
        "range-test-" + run_tag + "-bucket3",
        "range-test-" + run_tag + "-bucket4",
    };

    kvrgw::bucket_id_t bucket_id = 1;
    const int64_t created = now_unix();

    for (const auto &name : names) {
      txn_put(store, kvrgw::make_bucket_key(tenant_id, name).view(),
              kvrgw::make_bucket_value(bucket_id, created));
    }

    const auto prefix = kvrgw::make_bucket_prefix(tenant_id);
    const auto end = prefix_range_end(prefix.view());
    auto rows = store.range_scan(prefix.view(), end, 0);
    assert(rows);
    assert(rows->size() == 4);

    const kvrgw::bucket_id_t unused_bucket_id = 0xFFFFFFFFFFFFFFFEULL;
    const auto object_prefix = kvrgw::make_object_prefix(unused_bucket_id);
    const auto object_end = prefix_range_end(object_prefix.view());
    auto empty_objects = store.range_scan(object_prefix.view(), object_end, 1);
    assert(empty_objects);
    assert(empty_objects->empty());

    size_t matched = 0;
    for (const auto &name : names) {
      for (const auto &row : *rows) {
        const auto parts = kvrgw::parse_bucket_key(row.key);
        if (parts && parts->bucket_name == name) {
          ++matched;
          break;
        }
      }
    }
    assert(matched == 4);

    constexpr int kMany = 150;
    for (int i = 0; i < kMany; ++i) {
      const std::string name = "scan-many-" + run_tag + "-" + std::to_string(i);
      txn_put(store, kvrgw::make_bucket_key(tenant_id, name).view(),
              kvrgw::make_bucket_value(bucket_id, created));
    }
    auto many_rows = store.range_scan(prefix.view(), end, 0);
    assert(many_rows);
    assert(static_cast<int>(many_rows->size()) == 4 + kMany);
    std::set<std::string> seen_keys;
    for (const auto &row : *many_rows) {
      seen_keys.insert(row.key);
    }
    assert(static_cast<int>(seen_keys.size()) == 4 + kMany);
    for (int i = 0; i < kMany; ++i) {
      const std::string name = "scan-many-" + run_tag + "-" + std::to_string(i);
      txn_del(store, kvrgw::make_bucket_key(tenant_id, name).view());
    }

    for (const auto &name : names) {
      txn_del(store, kvrgw::make_bucket_key(tenant_id, name).view());
    }
    cleanup_tenant_buckets(store, tenant_id);

    // --- Live FDB error injection tests ---

    // Test 1: conflict (1020) — two transactions, interleaved read/write
    {
      const std::string conflict_key = "kv_range_test_conflict_" + run_tag;
      txn_put(store, conflict_key, "initial");

      auto a = store.begin_transaction();
      assert(a);
      auto b = store.begin_transaction();
      assert(b);

      auto a_read = (*a)->kv_get(conflict_key);
      assert(a_read);
      auto b_read = (*b)->kv_get(conflict_key);
      assert(b_read);

      (*a)->kv_put(conflict_key, "from-a");
      auto a_commit = (*a)->commit();
      assert(a_commit);

      (*b)->kv_put(conflict_key, "from-b");
      auto b_commit = (*b)->commit();
      assert(!b_commit);
      assert(b_commit.error() == 1020);
      assert(kvrgw::is_retriable(kvrgw::fdb_to_error(b_commit.error())));

      auto retry = store.begin_transaction();
      assert(retry);
      (*retry)->kv_put(conflict_key, "from-b-retry");
      auto retry_commit = (*retry)->commit();
      assert(retry_commit);

      txn_del(store, conflict_key);
      std::cout << "  live fdb error test: conflict (1020) passed\n";
    }

    // Test 2: transaction too old (1007) — ancient read version via raw FDB txn
    {
      FDBTransaction *raw_tr = nullptr;
      fdb_error_t err =
          fdb_database_create_transaction(store.database(), &raw_tr);
      assert(err == 0);
      fdb_transaction_set_read_version(raw_tr, 1);
      kvrgw::FdbFuture f(fdb_transaction_get(
          raw_tr, reinterpret_cast<const uint8_t *>("any_key"), 7, 0));
      fdb_error_t wait_err = fdb_future_block_until_ready(f.raw());
      std::expected<std::optional<std::string>, fdb_error_t> result;
      if (wait_err) {
        result = std::unexpected(wait_err);
      }
      else {
        fdb_bool_t present = 0;
        const uint8_t *val = nullptr;
        int vlen = 0;
        if (fdb_error_t e =
                fdb_future_get_value(f.raw(), &present, &val, &vlen)) {
          result = std::unexpected(e);
        }
        else if (!present) {
          result = std::nullopt;
        }
        else {
          result = std::string(reinterpret_cast<const char *>(val), vlen);
        }
      }
      assert(!result);
      assert(result.error() == 1007);
      assert(kvrgw::is_retriable(kvrgw::fdb_to_error(result.error())));
      fdb_transaction_destroy(raw_tr);
      std::cout << "  live fdb error test: transaction_too_old (1007) passed\n";
    }

    // Test 3: key too large (2102) — key > 10KB
    {
      auto tr = store.begin_transaction();
      assert(tr);
      const std::string huge_key(11000, 'K');
      (*tr)->kv_put(huge_key, "value");
      auto rc = (*tr)->commit();
      assert(!rc);
      assert(rc.error() == 2102);
      assert(!kvrgw::is_retriable(kvrgw::fdb_to_error(rc.error())));
      std::cout << "  live fdb error test: key_too_large (2102) passed\n";
    }
  }
  catch (const std::exception &ex) {
    if (fdb_error_t e = fdb_stop_network()) {
      std::cerr << "fdb_stop_network failed: " << fdb_get_error(e) << "\n";
    }
    network_thread.join();
    std::cerr << "kv_range_test failed: " << ex.what() << "\n";
    return 1;
  }

  if (fdb_error_t e = fdb_stop_network()) {
    std::cerr << "fdb_stop_network failed: " << fdb_get_error(e) << "\n";
  }
  network_thread.join();
  std::cout << "kv_range_test passed\n";
  return 0;
}
