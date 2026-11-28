// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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

// create_bucket() atomicity test against a live FDB cluster.
//
// Verifies that concurrent create_bucket() calls for the same name all end up
// with the id stored in FDB (no blind overwrite), consume exactly one bucket
// id, that distinct names get distinct ids, and that a stale cache entry never
// short-circuits creation.
//
// Run: FDB_CLUSTER_FILE=<cluster file> build/create_bucket_test

#include "constants.hpp"
#include "error_codes.hpp"
#include "keys.hpp"
#include "kv_store.hpp"
#include "kvrgw_runtime.hpp"
#include "l_keys.hpp"
#include "object_value.hpp"
#include "service_impl.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace kvrgw {
namespace {

int g_pass = 0;
int g_fail = 0;

#define CHECK(cond)                                                            \
  do {                                                                         \
    if (cond) {                                                                \
      ++g_pass;                                                                \
    } else {                                                                   \
      ++g_fail;                                                                \
      std::cerr << "FAIL: " #cond " (" << __FILE__ << ":" << __LINE__ << ")\n"; \
    }                                                                          \
  } while (0)

struct Env {
  KvRgwServiceImpl *svc{};
  KvStore *store{};
  tenant_id_t tenant{};
};

std::string unique_name(const char *tag)
{
  return std::string("cb-") + tag + "-" + std::to_string(getpid()) + "-" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() % 1000000);
}

// Reads the bucket value straight from FDB (bypassing the cache).
std::optional<BucketValue> read_bucket(Env &env, const std::string &name)
{
  KeyBuf key;
  make_bucket_key(env.tenant, name, key);
  auto v = env.store->get(key.view());
  if (!v || !*v) {
    return std::nullopt;
  }
  return parse_bucket_value(**v);
}

uint64_t read_bucket_counter(Env &env)
{
  const auto key = make_l_key(kLocalTypeNumeric, kLocalCounterBucketId);
  auto v = env.store->get(key.view());
  uint64_t cur = 0;
  if (v && *v) {
    decode_numeric_counter(**v, cur);
  }
  return cur;
}

void test_concurrent_same_name(Env &env)
{
  std::cout << "--- concurrent create_bucket, same name ---\n";
  constexpr int kThreads = 16;
  const std::string name = unique_name("same");
  const uint64_t counter_before = read_bucket_counter(env);

  std::atomic<bool> go{false};
  std::vector<KvrgwErrorCode> ecs(kThreads, KVRGW_ERR_INTERNAL);
  std::vector<std::thread> th;
  for (int i = 0; i < kThreads; ++i) {
    th.emplace_back([&, i]() {
      while (!go.load()) {
      }
      ecs[i] = env.svc->create_bucket(env.tenant, name);
    });
  }
  go.store(true);
  for (auto &t : th) {
    t.join();
  }

  for (int i = 0; i < kThreads; ++i) {
    CHECK(ecs[i] == KVRGW_ERR_OK);
  }
  auto stored = read_bucket(env, name);
  CHECK(stored.has_value());
  auto cached = env.svc->resolve_bucket_id(env.tenant, name);
  CHECK(cached.has_value() && cached->has_value());
  if (stored && cached && cached->has_value()) {
    CHECK(stored->bucket_id == **cached);
  }
  // Exactly one id was consumed: losers adopt the winner's bucket and their
  // in-transaction allocation is discarded.
  CHECK(read_bucket_counter(env) == counter_before + 1);

  env.svc->delete_bucket(env.tenant, name);
}

void test_concurrent_distinct_names(Env &env)
{
  std::cout << "--- concurrent create_bucket, distinct names ---\n";
  constexpr int kThreads = 8;
  std::vector<std::string> names;
  for (int i = 0; i < kThreads; ++i) {
    names.push_back(unique_name(("d" + std::to_string(i)).c_str()));
  }
  const uint64_t counter_before = read_bucket_counter(env);

  std::atomic<bool> go{false};
  std::vector<KvrgwErrorCode> ecs(kThreads, KVRGW_ERR_INTERNAL);
  std::vector<std::thread> th;
  for (int i = 0; i < kThreads; ++i) {
    th.emplace_back([&, i]() {
      while (!go.load()) {
      }
      ecs[i] = env.svc->create_bucket(env.tenant, names[i]);
    });
  }
  go.store(true);
  for (auto &t : th) {
    t.join();
  }

  std::set<std::string> ids;
  for (int i = 0; i < kThreads; ++i) {
    CHECK(ecs[i] == KVRGW_ERR_OK);
    auto b = read_bucket(env, names[i]);
    CHECK(b.has_value());
    if (b) {
      ids.insert(std::string(b->bucket_id.view()));
    }
  }
  CHECK(ids.size() == static_cast<size_t>(kThreads));
  CHECK(read_bucket_counter(env) == counter_before + kThreads);

  for (const auto &n : names) {
    env.svc->delete_bucket(env.tenant, n);
  }
}

void test_existing_bucket_not_overwritten(Env &env)
{
  std::cout << "--- create_bucket on existing bucket ---\n";
  const std::string name = unique_name("exist");
  CHECK(env.svc->create_bucket(env.tenant, name) == KVRGW_ERR_OK);
  auto first = read_bucket(env, name);
  CHECK(first.has_value());
  const uint64_t counter_before = read_bucket_counter(env);

  CHECK(env.svc->create_bucket(env.tenant, name) == KVRGW_ERR_OK);
  auto second = read_bucket(env, name);
  CHECK(second.has_value());
  if (first && second) {
    CHECK(first->bucket_id == second->bucket_id);
  }
  CHECK(read_bucket_counter(env) == counter_before);

  env.svc->delete_bucket(env.tenant, name);
}

void test_stale_cache_does_not_short_circuit(Env &env)
{
  std::cout << "--- stale cache entry for a removed bucket ---\n";
  const std::string name = unique_name("stale");
  CHECK(env.svc->create_bucket(env.tenant, name) == KVRGW_ERR_OK);  // caches
  auto first = read_bucket(env, name);
  CHECK(first.has_value());

  // Remove the bucket key behind the cache's back.
  KeyBuf key;
  make_bucket_key(env.tenant, name, key);
  auto tr = env.store->begin_transaction();
  CHECK(tr.has_value());
  if (tr) {
    (*tr)->kv_del(key.view());
    CHECK((*tr)->commit() == 0);
  }
  CHECK(!read_bucket(env, name).has_value());

  CHECK(env.svc->create_bucket(env.tenant, name) == KVRGW_ERR_OK);
  auto second = read_bucket(env, name);
  CHECK(second.has_value());
  if (first && second) {
    CHECK(!(first->bucket_id == second->bucket_id));
  }

  env.svc->delete_bucket(env.tenant, name);
}

int run(KvRgwRuntime &rt)
{
  Env env;
  env.svc = &rt.service();
  env.store = &rt.store();

  const std::string tenant_name = "create-bucket-test";
  bool exists = false;
  auto ec = env.svc->resolve_tenant(tenant_name, &exists, &env.tenant);
  if (ec != KVRGW_ERR_OK || !exists) {
    ec = env.svc->add_tenant(tenant_name, &env.tenant);
    if (ec != KVRGW_ERR_OK) {
      std::cerr << "FAIL: add_tenant ec=" << kvrgw_strerror(ec) << "\n";
      return 1;
    }
  }

  test_concurrent_same_name(env);
  test_concurrent_distinct_names(env);
  test_existing_bucket_not_overwritten(env);
  test_stale_cache_does_not_short_circuit(env);

  std::cout << "\n=== create_bucket_test: " << g_pass << " passed, " << g_fail
            << " failed ===\n";
  return g_fail == 0 ? 0 : 1;
}

}  // namespace
}  // namespace kvrgw

int main()
{
  char admin_sock[64];
  std::snprintf(admin_sock, sizeof(admin_sock), "/tmp/kvrgw-cb-test-%d.sock",
                static_cast<int>(getpid()));
  setenv("KVRGW_ADMIN_SOCKET", admin_sock, 1);
  setenv("KVRGW_BATCH_SIZE", "1", 0);
  setenv("KVRGW_BATCH_TIMEOUT_US", "20000", 0);
  setenv("KVRGW_BATCH_THREADS", "1", 0);

  kvrgw::KvRgwStartOptions opts;
  opts.perf_mode = false;
  char data_root_buf[64];
  std::snprintf(data_root_buf, sizeof(data_root_buf), "/tmp/kvrgw-cb-test-data-%d",
                static_cast<int>(getpid()));
  opts.data_root = data_root_buf;
  std::error_code fs_ec;
  std::filesystem::create_directories(opts.data_root, fs_ec);

  kvrgw::KvRgwRuntime runtime;
  if (!runtime.start(opts)) {
    std::cerr << "FAIL: runtime start failed\n";
    return 1;
  }
  const int rc = kvrgw::run(runtime);
  runtime.stop();
  std::filesystem::remove_all(opts.data_root, fs_ec);
  return rc;
}
