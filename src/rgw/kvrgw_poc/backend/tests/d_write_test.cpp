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

// D: write-path test (plans/refactor_header_alloc.md, sub-task 4).
//
// For payload sizes around the tier boundaries, in non-batch and batch mode:
//   * PUT then GET returns the exact body and the expected chunk type;
//   * for CHUNK_CHILD_D the raw D: KV value is byte-identical to
//     make_child_value(ChildValueHeader{}, payload) and sits under the key
//     built from the payload length (not payload + header);
// plus:
//   * concurrent PUTs to one key (txn conflicts -> retries / fallback_to_single)
//     leave a correct, readable D: value;
//   * a tier-config change between two PUTs leaves both objects correct.
//
// Run against a live FoundationDB cluster.

#include "constants.hpp"
#include "error_codes.hpp"
#include "keys.hpp"
#include "kv_store.hpp"
#include "kvrgw_runtime.hpp"
#include "object_value.hpp"
#include "service_impl.hpp"
#include "tier_config_state.hpp"

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <iostream>
#include <optional>
#include <string>
#include <unistd.h>
#include <vector>

namespace kvrgw {
namespace {

int g_failures = 0;

#define CHECK(cond) \
  do { \
    if (!(cond)) { \
      std::cerr << "FAIL: " << #cond << " at " << __FILE__ << ":" << __LINE__ << "\n"; \
      ++g_failures; \
    } \
  } while (0)

constexpr uint32_t kMaxInline  = 256;
constexpr uint32_t kMaxKvStore = 4096;
constexpr const char *kTenantName = "d-write-test";

using Svc = KvRgwServiceImpl;

void set_tier(Svc &svc, uint32_t max_inline, uint32_t max_kv_store, int batch_size)
{
  TierConfig tc = svc.tier_config_state().active_copy();
  tc.max_inline = max_inline;
  tc.max_kv_store = max_kv_store;
  tc.batch_size = batch_size;
  tc.batch_timeout_us = 1000;
  svc.tier_config_state().try_stage(tc);
  svc.tier_config_state().maybe_apply_pending();
}

ChunkType expected_type(size_t n, uint32_t max_inline, uint32_t max_kv_store)
{
  if (n <= max_inline) return CHUNK_INLINE;
  if (n <= max_kv_store) return CHUNK_CHILD_D;
  return CHUNK_STORAGE;
}

Svc::PutObjectResult put(Svc &svc, tenant_id_t t, const std::string &bucket,
                         const std::string &key, const std::string &body)
{
  return svc.put_object(t, bucket, key,
                        reinterpret_cast<const uint8_t *>(body.data()), body.size(),
                        "application/octet-stream", body.size(), nullptr, {}, {});
}

// GET, then (for D:) compare the raw KV value against the golden bytes.
void verify(Svc &svc, KvStore &store, tenant_id_t t, bucket_id_t bid,
            const std::string &bucket, const std::string &key,
            const std::string &body, ChunkType want)
{
  Svc::GetObjectResult res;
  const auto ec = svc.get_object(t, bucket, key, std::nullopt, nullptr, 0, &res);
  CHECK(ec == KVRGW_ERR_OK);
  if (ec != KVRGW_ERR_OK) return;
  CHECK(res.body == body);
  CHECK(res.value.hdr.chunk_type == want);
  CHECK(res.value.hdr.size == body.size());
  if (want != CHUNK_CHILD_D) return;

  KeyBuf d_key;
  make_d_key(bid, d_size_tier_from_size(body.size()),
             res.value.hdr.ref_tag.view(),
             static_cast<uint32_t>(res.value.hdr.last_modified_sec), d_key);
  auto raw = store.get(d_key.view());
  CHECK(raw.has_value() && raw->has_value());
  if (!raw.has_value() || !raw->has_value()) return;
  CHECK(**raw == make_child_value(ChildValueHeader{}, body));
  CHECK((**raw).size() == sizeof(ChildValueHeader) + body.size());
}

void test_sizes(Svc &svc, KvStore &store, tenant_id_t t, bucket_id_t bid,
                const std::string &bucket, int batch_size)
{
  std::cout << "\n[sizes] batch_size=" << batch_size << "\n";
  set_tier(svc, kMaxInline, kMaxKvStore, batch_size);
  const size_t sizes[] = {0, 1, kMaxInline, kMaxInline + 1, 1023, 1024, 1025,
                          4095, kMaxKvStore, kMaxKvStore + 1};
  for (const size_t n : sizes) {
    const std::string key = "sz-" + std::to_string(batch_size) + "-" + std::to_string(n);
    const std::string body(n, static_cast<char>('a' + n % 26));
    const auto r = put(svc, t, bucket, key, body);
    CHECK(r.error_code == KVRGW_ERR_OK);
    verify(svc, store, t, bid, bucket, key, body,
           expected_type(n, kMaxInline, kMaxKvStore));
  }
}

void test_contended_key(Svc &svc, KvStore &store, tenant_id_t t, bucket_id_t bid,
                        const std::string &bucket, int batch_size)
{
  std::cout << "\n[contention] batch_size=" << batch_size << "\n";
  set_tier(svc, kMaxInline, kMaxKvStore, batch_size);
  const std::string key = "contended-" + std::to_string(batch_size);
  constexpr int kWriters = 8;
  std::vector<std::string> bodies;
  for (int i = 0; i < kWriters; ++i) {
    bodies.emplace_back(1000 + 37 * i, static_cast<char>('A' + i));
  }
  std::vector<std::future<Svc::PutObjectResult>> futs;
  for (int i = 0; i < kWriters; ++i) {
    futs.push_back(std::async(std::launch::async, [&, i] {
      return put(svc, t, bucket, key, bodies[i]);
    }));
  }
  // Losers of a write-write race may run out of retries; any other error is a bug.
  int ok = 0;
  for (auto &f : futs) {
    const auto rc = f.get().error_code;
    if (rc == KVRGW_ERR_OK) {
      ++ok;
    }
    else {
      std::cout << "  writer error: " << kvrgw_strerror(rc) << "\n";
      CHECK(rc == KVRGW_ERR_MAX_RETRIES_EXCEEDED ||
            rc == KVRGW_ERR_TRANSACTION_CONFLICT);
    }
  }
  CHECK(ok >= 1);
  // Whichever writer won, the object and its D: value must be consistent.
  Svc::GetObjectResult res;
  CHECK(svc.get_object(t, bucket, key, std::nullopt, nullptr, 0, &res) == KVRGW_ERR_OK);
  bool matched = false;
  for (const auto &b : bodies) {
    if (res.body == b) {
      matched = true;
      verify(svc, store, t, bid, bucket, key, b, CHUNK_CHILD_D);
    }
  }
  CHECK(matched);
}

void test_tier_change(Svc &svc, KvStore &store, tenant_id_t t, bucket_id_t bid,
                      const std::string &bucket, int batch_size)
{
  std::cout << "\n[tier change] batch_size=" << batch_size << "\n";
  const std::string body_a(600, 'p');
  const std::string body_b(600, 'q');

  set_tier(svc, 700, kMaxKvStore, batch_size);         // 600 B -> inline
  CHECK(put(svc, t, bucket, "tc-a", body_a).error_code == KVRGW_ERR_OK);
  set_tier(svc, kMaxInline, kMaxKvStore, batch_size);  // 600 B -> D:
  CHECK(put(svc, t, bucket, "tc-b", body_b).error_code == KVRGW_ERR_OK);
  set_tier(svc, kMaxInline, 512, batch_size);          // 600 B -> storage
  const std::string body_c(600, 'r');
  CHECK(put(svc, t, bucket, "tc-c", body_c).error_code == KVRGW_ERR_OK);

  verify(svc, store, t, bid, bucket, "tc-a", body_a, CHUNK_INLINE);
  verify(svc, store, t, bid, bucket, "tc-b", body_b, CHUNK_CHILD_D);
  verify(svc, store, t, bid, bucket, "tc-c", body_c, CHUNK_STORAGE);
}

int run(KvRgwRuntime &runtime)
{
  auto &svc = runtime.service();
  auto &store = runtime.store();

  tenant_id_t tenant_id{};
  bool exists = false;
  auto ec = svc.resolve_tenant(kTenantName, &exists, &tenant_id);
  if (ec != KVRGW_ERR_OK || !exists) {
    ec = svc.add_tenant(kTenantName, &tenant_id);
    CHECK(ec == KVRGW_ERR_OK);
  }

  char bucket_buf[64];
  std::snprintf(bucket_buf, sizeof(bucket_buf), "d-write-%d", static_cast<int>(getpid()));
  const std::string bucket(bucket_buf);
  CHECK(svc.create_bucket(tenant_id, bucket) == KVRGW_ERR_OK);
  auto bid_result = svc.resolve_bucket_id(tenant_id, bucket);
  if (!bid_result.has_value() || !bid_result->has_value()) {
    std::cerr << "FAIL: could not resolve bucket_id\n";
    return 1;
  }
  const bucket_id_t bid = **bid_result;

  for (const int batch_size : {1, 4}) {
    test_sizes(svc, store, tenant_id, bid, bucket, batch_size);
    test_contended_key(svc, store, tenant_id, bid, bucket, batch_size);
    test_tier_change(svc, store, tenant_id, bid, bucket, batch_size);
  }

  if (g_failures == 0) {
    std::cout << "\nALL D: WRITE TESTS PASSED\n";
  }
  else {
    std::cout << "\n" << g_failures << " CHECK(S) FAILED\n";
  }
  return g_failures == 0 ? 0 : 1;
}

}  // namespace
}  // namespace kvrgw

int main()
{
  char admin_sock[64];
  std::snprintf(admin_sock, sizeof(admin_sock),
                "/tmp/kvrgw-test-%d.sock", static_cast<int>(getpid()));
  setenv("KVRGW_ADMIN_SOCKET", admin_sock, 1);
  setenv("KVRGW_MAX_INLINE",       "256",  1);
  setenv("KVRGW_MAX_KV_STORE",     "4096", 1);
  setenv("KVRGW_BATCH_SIZE",       "4",    1);
  setenv("KVRGW_BATCH_TIMEOUT_US", "1000", 1);
  setenv("KVRGW_BATCH_THREADS",    "1",    1);

  kvrgw::KvRgwStartOptions opts;
  opts.perf_mode = false;
  char data_root_buf[64];
  std::snprintf(data_root_buf, sizeof(data_root_buf),
                "/tmp/kvrgw-dwrite-test-data-%d", static_cast<int>(getpid()));
  opts.data_root = data_root_buf;

  kvrgw::KvRgwRuntime runtime;
  if (!runtime.start(opts)) {
    std::cerr << "FAIL: runtime start failed\n";
    return 1;
  }
  const int rc = kvrgw::run(runtime);
  runtime.stop();
  std::error_code fs_ec;
  std::filesystem::remove_all(opts.data_root, fs_ec);
  return rc;
}
