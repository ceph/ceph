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

// CHUNK_MULTIPART_REF zero-copy server-side-copy tests (plans/<copy bugs
// plan>'s "Bug 2" design): copying a completed multipart object must share
// the underlying part files via ref-counting instead of materializing a
// new copy, and deleting any one of N sharing objects must only free the
// part files once the *last* reference is dropped.
//
// Isolated binary (own GcWorker interval override) -- run against a live
// FoundationDB cluster.

#include "constants.hpp"
#include "error_codes.hpp"
#include "keys.hpp"
#include "kvrgw_runtime.hpp"
#include "kv_store.hpp"
#include "object_value.hpp"
#include "service_impl.hpp"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>

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

bool wait_until(std::function<bool()> pred, int timeout_ms, int poll_ms = 50)
{
  const auto deadline = std::chrono::steady_clock::now()
                        + std::chrono::milliseconds(timeout_ms);
  while (!pred()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
  }
  return true;
}

std::filesystem::path part_file_path(const std::filesystem::path& data_root,
                                     std::string_view ref_tag_hex,
                                     uint16_t part_number,
                                     uint32_t generation)
{
  char buf[64];
  int n = std::snprintf(buf, sizeof(buf), "%.*s-%u-%u",
                        static_cast<int>(ref_tag_hex.size()), ref_tag_hex.data(),
                        static_cast<unsigned>(part_number),
                        static_cast<unsigned>(generation));
  if (n <= 0 || static_cast<size_t>(n) >= sizeof(buf)) {
    return {};
  }
  return data_root / std::string(buf, static_cast<size_t>(n));
}

bool file_gone(const std::filesystem::path& p)
{
  return !std::filesystem::exists(p);
}

bool file_exists(const std::filesystem::path& p)
{
  return std::filesystem::exists(p);
}

// ============================================================================
// Scenario 1: copy a completed multipart object; verify full/range/
// PartNumber reads all match through the CHUNK_MULTIPART_REF copy.
// ============================================================================
void test_copy_basic(KvRgwServiceImpl& svc, tenant_id_t tenant_id, const std::string& bucket_name)
{
  std::cout << "\n[REFCOPY-1] Copy + full/range/PartNumber reads\n";

  const std::string src_key = "refcopy-test/basic-src";
  const std::string dst_key = "refcopy-test/basic-dst";

  auto init = svc.initiate_multipart_upload(tenant_id, bucket_name, src_key,
                                            "text/plain", {}, {});
  CHECK(init.error_code == KVRGW_ERR_OK);
  const std::string uid = init.upload_id;

  const std::string p1(5 * 1024 * 1024, 'A');
  auto up1 = svc.upload_part(tenant_id, bucket_name, src_key, uid, 1,
                             reinterpret_cast<const uint8_t*>(p1.data()), p1.size());
  CHECK(up1.error_code == KVRGW_ERR_OK);

  const std::string p2(1024, 'B');
  auto up2 = svc.upload_part(tenant_id, bucket_name, src_key, uid, 2,
                             reinterpret_cast<const uint8_t*>(p2.data()), p2.size());
  CHECK(up2.error_code == KVRGW_ERR_OK);

  std::vector<KvRgwServiceImpl::CompletedPart> parts{{1, up1.etag}, {2, up2.etag}};
  auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, src_key, uid, parts);
  CHECK(cm.error_code == KVRGW_ERR_OK);
  const std::string whole = p1 + p2;

  KvRgwServiceImpl::CopyObjectRequest req;
  req.tenant_id = tenant_id;
  req.src_bucket_name = bucket_name;
  req.src_key = src_key;
  req.dst_bucket_name = bucket_name;
  req.dst_key = dst_key;
  KvRgwServiceImpl::CopyObjectResult cres;
  auto copy_ec = svc.copy_object(req, &cres);
  CHECK(copy_ec == KVRGW_ERR_OK);

  OwnedObjectValue dst_hv;
  std::string err_detail;
  auto head_ec = svc.head_object(tenant_id, bucket_name, dst_key, std::nullopt, 0, &dst_hv, &err_detail);
  CHECK(head_ec == KVRGW_ERR_OK);
  CHECK(dst_hv.hdr.chunk_type == CHUNK_MULTIPART_REF);
  CHECK(dst_hv.hdr.size == whole.size());
  CHECK(dst_hv.part_count() == 2);

  // Full read.
  {
    KvRgwServiceImpl::GetObjectResult g;
    auto ec = svc.get_object(tenant_id, bucket_name, dst_key, std::nullopt, nullptr, 0, &g);
    CHECK(ec == KVRGW_ERR_OK);
    CHECK(g.body == whole);
  }
  // Range read spanning both parts.
  {
    ByteRange range{static_cast<int64_t>(p1.size()) - 10, static_cast<int64_t>(p1.size()) + 10};
    KvRgwServiceImpl::GetObjectResult g;
    auto ec = svc.get_object(tenant_id, bucket_name, dst_key, std::nullopt, &range, 0, &g);
    CHECK(ec == KVRGW_ERR_OK);
    CHECK(g.body == whole.substr(p1.size() - 10, 21));
  }
  // PartNumber reads.
  {
    KvRgwServiceImpl::GetObjectResult g;
    auto ec = svc.get_object(tenant_id, bucket_name, dst_key, std::nullopt, nullptr, 1, &g);
    CHECK(ec == KVRGW_ERR_OK);
    CHECK(g.body == p1);
  }
  {
    KvRgwServiceImpl::GetObjectResult g;
    auto ec = svc.get_object(tenant_id, bucket_name, dst_key, std::nullopt, nullptr, 2, &g);
    CHECK(ec == KVRGW_ERR_OK);
    CHECK(g.body == p2);
  }
  std::cout << "  dst (CHUNK_MULTIPART_REF) full/range/PartNumber reads all match\n";

  // Cleanup -- delete both, leave no dangling state for later scenarios.
  KvRgwServiceImpl::DeleteResult del;
  CHECK(svc.delete_object(tenant_id, bucket_name, dst_key, nullptr, &del) == KVRGW_ERR_OK);
  CHECK(svc.delete_object(tenant_id, bucket_name, src_key, nullptr, &del) == KVRGW_ERR_OK);
}

// ============================================================================
// Scenario 2: copy-of-copy -- N-way sharing (ref_count > 2). All three
// objects must remain independently, fully readable.
// ============================================================================
void test_copy_of_copy(KvRgwServiceImpl& svc, tenant_id_t tenant_id, const std::string& bucket_name)
{
  std::cout << "\n[REFCOPY-2] Copy-of-copy (3-way sharing)\n";

  const std::string k1 = "refcopy-test/chain-1";
  const std::string k2 = "refcopy-test/chain-2";
  const std::string k3 = "refcopy-test/chain-3";

  auto init = svc.initiate_multipart_upload(tenant_id, bucket_name, k1,
                                            "text/plain", {}, {});
  CHECK(init.error_code == KVRGW_ERR_OK);
  const std::string uid = init.upload_id;
  const std::string p1(6 * 1024 * 1024, 'C');
  auto up1 = svc.upload_part(tenant_id, bucket_name, k1, uid, 1,
                             reinterpret_cast<const uint8_t*>(p1.data()), p1.size());
  CHECK(up1.error_code == KVRGW_ERR_OK);
  std::vector<KvRgwServiceImpl::CompletedPart> parts{{1, up1.etag}};
  auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, k1, uid, parts);
  CHECK(cm.error_code == KVRGW_ERR_OK);

  auto do_copy = [&](const std::string& src, const std::string& dst) {
    KvRgwServiceImpl::CopyObjectRequest req;
    req.tenant_id = tenant_id;
    req.src_bucket_name = bucket_name;
    req.src_key = src;
    req.dst_bucket_name = bucket_name;
    req.dst_key = dst;
    KvRgwServiceImpl::CopyObjectResult cres;
    return svc.copy_object(req, &cres);
  };

  CHECK(do_copy(k1, k2) == KVRGW_ERR_OK);       // first share: k1 promoted, k2 == _REF
  CHECK(do_copy(k2, k3) == KVRGW_ERR_OK);       // copy-of-copy: k3 == _REF, ref_count now 3

  for (const auto& k : {k1, k2, k3}) {
    KvRgwServiceImpl::GetObjectResult g;
    auto ec = svc.get_object(tenant_id, bucket_name, k, std::nullopt, nullptr, 0, &g);
    CHECK(ec == KVRGW_ERR_OK);
    CHECK(g.body == p1);
  }
  std::cout << "  k1, k2 (copy), k3 (copy-of-copy) all independently readable\n";

  KvRgwServiceImpl::DeleteResult del;
  CHECK(svc.delete_object(tenant_id, bucket_name, k1, nullptr, &del) == KVRGW_ERR_OK);
  CHECK(svc.delete_object(tenant_id, bucket_name, k2, nullptr, &del) == KVRGW_ERR_OK);
  CHECK(svc.delete_object(tenant_id, bucket_name, k3, nullptr, &del) == KVRGW_ERR_OK);
}

// ============================================================================
// Scenario 3: delete the original first -- the copy must remain fully
// readable; only once the copy is *also* deleted do the part files finally
// get freed by GcWorker.
// ============================================================================
void test_delete_original_first(KvRgwServiceImpl& svc, tenant_id_t tenant_id, const std::string& bucket_name,
                                 const std::filesystem::path& data_root)
{
  std::cout << "\n[REFCOPY-3] Delete original first -- copy stays readable; parts freed only after both gone\n";

  const std::string src_key = "refcopy-test/delorig-src";
  const std::string dst_key = "refcopy-test/delorig-dst";

  auto init = svc.initiate_multipart_upload(tenant_id, bucket_name, src_key,
                                            "text/plain", {}, {});
  CHECK(init.error_code == KVRGW_ERR_OK);
  const std::string uid = init.upload_id;
  const std::string p1(5 * 1024 * 1024, 'D');
  auto up1 = svc.upload_part(tenant_id, bucket_name, src_key, uid, 1,
                             reinterpret_cast<const uint8_t*>(p1.data()), p1.size());
  CHECK(up1.error_code == KVRGW_ERR_OK);
  std::vector<KvRgwServiceImpl::CompletedPart> parts{{1, up1.etag}};
  auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, src_key, uid, parts);
  CHECK(cm.error_code == KVRGW_ERR_OK);

  const std::string ref_tag_hex = uid;  // upload_id == src's own ref_tag, hex
  auto f1 = part_file_path(data_root, ref_tag_hex, 1, 0);
  CHECK(!f1.empty() && file_exists(f1));

  KvRgwServiceImpl::CopyObjectRequest req;
  req.tenant_id = tenant_id;
  req.src_bucket_name = bucket_name;
  req.src_key = src_key;
  req.dst_bucket_name = bucket_name;
  req.dst_key = dst_key;
  KvRgwServiceImpl::CopyObjectResult cres;
  CHECK(svc.copy_object(req, &cres) == KVRGW_ERR_OK);

  KvRgwServiceImpl::DeleteResult del;
  CHECK(svc.delete_object(tenant_id, bucket_name, src_key, nullptr, &del) == KVRGW_ERR_OK);

  // Give GcWorker a moment -- the part file must NOT be removed; the copy
  // still references it via the shared R: ref-count.
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  CHECK(file_exists(f1));

  {
    KvRgwServiceImpl::GetObjectResult g;
    auto ec = svc.get_object(tenant_id, bucket_name, dst_key, std::nullopt, nullptr, 0, &g);
    CHECK(ec == KVRGW_ERR_OK);
    CHECK(g.body == p1);
  }
  std::cout << "  copy still fully readable after original deleted; part file untouched\n";

  CHECK(svc.delete_object(tenant_id, bucket_name, dst_key, nullptr, &del) == KVRGW_ERR_OK);
  constexpr int kTimeoutMs = 15000;
  CHECK(wait_until([&] { return file_gone(f1); }, kTimeoutMs));
  std::cout << "  part file freed once the last reference (copy) was deleted\n";
}

// ============================================================================
// Scenario 4: delete the copy first -- the original must remain fully
// readable; only once the original is *also* deleted do the part files
// finally get freed.
// ============================================================================
void test_delete_copy_first(KvRgwServiceImpl& svc, tenant_id_t tenant_id, const std::string& bucket_name,
                            const std::filesystem::path& data_root)
{
  std::cout << "\n[REFCOPY-4] Delete copy first -- original stays readable; parts freed only after both gone\n";

  const std::string src_key = "refcopy-test/delcopy-src";
  const std::string dst_key = "refcopy-test/delcopy-dst";

  auto init = svc.initiate_multipart_upload(tenant_id, bucket_name, src_key,
                                            "text/plain", {}, {});
  CHECK(init.error_code == KVRGW_ERR_OK);
  const std::string uid = init.upload_id;
  const std::string p1(5 * 1024 * 1024, 'E');
  auto up1 = svc.upload_part(tenant_id, bucket_name, src_key, uid, 1,
                             reinterpret_cast<const uint8_t*>(p1.data()), p1.size());
  CHECK(up1.error_code == KVRGW_ERR_OK);
  std::vector<KvRgwServiceImpl::CompletedPart> parts{{1, up1.etag}};
  auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, src_key, uid, parts);
  CHECK(cm.error_code == KVRGW_ERR_OK);

  const std::string ref_tag_hex = uid;
  auto f1 = part_file_path(data_root, ref_tag_hex, 1, 0);
  CHECK(!f1.empty() && file_exists(f1));

  KvRgwServiceImpl::CopyObjectRequest req;
  req.tenant_id = tenant_id;
  req.src_bucket_name = bucket_name;
  req.src_key = src_key;
  req.dst_bucket_name = bucket_name;
  req.dst_key = dst_key;
  KvRgwServiceImpl::CopyObjectResult cres;
  CHECK(svc.copy_object(req, &cres) == KVRGW_ERR_OK);

  KvRgwServiceImpl::DeleteResult del;
  CHECK(svc.delete_object(tenant_id, bucket_name, dst_key, nullptr, &del) == KVRGW_ERR_OK);

  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  CHECK(file_exists(f1));

  {
    KvRgwServiceImpl::GetObjectResult g;
    auto ec = svc.get_object(tenant_id, bucket_name, src_key, std::nullopt, nullptr, 0, &g);
    CHECK(ec == KVRGW_ERR_OK);
    CHECK(g.body == p1);
  }
  std::cout << "  original still fully readable after copy deleted; part file untouched\n";

  CHECK(svc.delete_object(tenant_id, bucket_name, src_key, nullptr, &del) == KVRGW_ERR_OK);
  constexpr int kTimeoutMs = 15000;
  CHECK(wait_until([&] { return file_gone(f1); }, kTimeoutMs));
  std::cout << "  part file freed once the last reference (original) was deleted\n";
}

int run(KvRgwRuntime& runtime, const std::filesystem::path& data_root)
{
  auto& svc = runtime.service();

  const std::string tenant_name = "multipart-refcopy-test";
  tenant_id_t tenant_id{};
  bool exists = false;
  auto ec = svc.resolve_tenant(tenant_name, &exists, &tenant_id);
  if (ec != KVRGW_ERR_OK || !exists) {
    ec = svc.add_tenant(tenant_name, &tenant_id);
    CHECK(ec == KVRGW_ERR_OK);
  }

  char bucket_buf[64];
  std::snprintf(bucket_buf, sizeof(bucket_buf),
                "refcopy-test-%d", static_cast<int>(getpid()));
  const std::string bucket_name(bucket_buf);
  ec = svc.create_bucket(tenant_id, bucket_name);
  CHECK(ec == KVRGW_ERR_OK);

  test_copy_basic(svc, tenant_id, bucket_name);
  test_copy_of_copy(svc, tenant_id, bucket_name);
  test_delete_original_first(svc, tenant_id, bucket_name, data_root);
  test_delete_copy_first(svc, tenant_id, bucket_name, data_root);

  if (g_failures == 0) {
    std::cout << "\nALL MULTIPART REF-COPY TESTS PASSED\n";
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
  ::setenv("KVRGW_GC_INTERVAL_SEC", "1", 1);

  kvrgw::KvRgwStartOptions opts;
  opts.perf_mode = false;

  char data_root_buf[64];
  std::snprintf(data_root_buf, sizeof(data_root_buf),
                "/tmp/kvrgw-refcopy-test-data-%d", static_cast<int>(getpid()));
  opts.data_root = data_root_buf;

  kvrgw::KvRgwRuntime runtime;
  if (!runtime.start(opts)) {
    std::cerr << "FAIL: runtime start failed\n";
    return 1;
  }

  const std::filesystem::path data_root(data_root_buf);
  const int rc = kvrgw::run(runtime, data_root);
  runtime.stop();
  std::error_code fs_ec;
  std::filesystem::remove_all(data_root, fs_ec);
  return rc;
}
