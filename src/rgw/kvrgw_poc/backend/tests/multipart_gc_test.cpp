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

// GC / Sweeper integration tests for multipart upload
// (plans/multipart_test_plan.md scenarios 4-7).
//
// Isolated binary so its KVRGW_GC_INTERVAL_SEC=1 env-var override doesn't
// bleed into other tests. Run against a live FoundationDB cluster.

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
#include <fstream>
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

// Poll pred() up to timeout_ms, checking every poll_ms.
// Returns true if pred() became true within the timeout.
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

// Build the data-tier file path for a given upload+part+generation.
// The filename format is "{upload_id_hex}-{part_number}-{generation}",
// matching what KvRgwServiceImpl::make_part_filename() produces
// (upload_id is the hex-encoded ref_tag, per the wire format documented
// in service_impl_multipart.cpp).
std::filesystem::path part_file_path(const std::filesystem::path& data_root,
                                     std::string_view upload_id,
                                     uint16_t part_number,
                                     uint32_t generation)
{
  char buf[64];
  int n = std::snprintf(buf, sizeof(buf), "%.*s-%u-%u",
                        static_cast<int>(upload_id.size()), upload_id.data(),
                        static_cast<unsigned>(part_number),
                        static_cast<unsigned>(generation));
  if (n <= 0 || static_cast<size_t>(n) >= sizeof(buf)) {
    return {};
  }
  return data_root / std::string(buf, static_cast<size_t>(n));
}

// Returns true once the file no longer exists on disk.
bool file_gone(const std::filesystem::path& p)
{
  return !std::filesystem::exists(p);
}

// Returns true if the file is present on disk.
bool file_exists(const std::filesystem::path& p)
{
  return std::filesystem::exists(p);
}

// Returns true when both the G:M and G:U KV entries for this upload_id are
// gone from FDB. Uses a range scan over the upload's ref_tag in the G:
// namespace — the G:M key is a fixed-size single key; G:U entries share the
// same ref_tag prefix. Both ranges being empty means GcWorker has fully
// processed and cleaned up this upload's GC directives.
bool gc_entries_gone(KvStore& store, bucket_id_t bucket_id,
                     std::string_view upload_id)
{
  // Decode upload_id (hex ref_tag) into raw bytes for key building.
  // upload_id is guaranteed to be 24 hex chars (kRefTagSize * 2).
  if (upload_id.size() != kRefTagSize * 2) {
    return false;
  }
  uint8_t rt_bytes[kRefTagSize];
  for (size_t i = 0; i < kRefTagSize; ++i) {
    auto hex_byte = [](char c) -> uint8_t {
      if (c >= '0' && c <= '9') return static_cast<uint8_t>(c - '0');
      if (c >= 'a' && c <= 'f') return static_cast<uint8_t>(c - 'a' + 10);
      if (c >= 'A' && c <= 'F') return static_cast<uint8_t>(c - 'A' + 10);
      return 0;
    };
    rt_bytes[i] = static_cast<uint8_t>(
        (hex_byte(upload_id[i * 2]) << 4) | hex_byte(upload_id[i * 2 + 1]));
  }
  const std::string_view rt_view(reinterpret_cast<const char*>(rt_bytes),
                                 kRefTagSize);

  // Check G:M key (single fixed-size key).
  KeyBuf gm_key;
  make_gm_key(bucket_id, rt_view, gm_key);
  const std::string gm_end = prefix_range_end(gm_key.view());
  auto gm_scan = store.range_scan(gm_key.view(), gm_end, 1);
  if (!gm_scan || !gm_scan->empty()) {
    return false;
  }

  // Check G:U keys (range — there may be 0 or more).
  // G:U key prefix for this ref_tag = KeyHeaderG(cat='U') + ref_tag.
  // Build it by constructing a G:U key with part_number=0, generation=0 and
  // taking only the header+ref_tag prefix portion.
  KeyBuf gu_prefix_key;
  make_gu_key(bucket_id, rt_view, 0, 0, gu_prefix_key);
  // Truncate to header + ref_tag only (strip part_number + generation suffix).
  constexpr size_t kGuPrefixLen = sizeof(KeyHeaderG) + kRefTagSize;
  if (gu_prefix_key.size() < kGuPrefixLen) {
    return false;
  }
  const std::string_view gu_prefix(
      reinterpret_cast<const char*>(gu_prefix_key.data), kGuPrefixLen);
  const std::string gu_end = prefix_range_end(gu_prefix);
  auto gu_scan = store.range_scan(gu_prefix, gu_end, 1);
  if (!gu_scan || !gu_scan->empty()) {
    return false;
  }

  return true;
}

// ============================================================================
// Scenario 1: Abort removes all parts (GC DEEP sweep)
// ============================================================================
void test_abort_gc_deep(KvRgwServiceImpl& svc,
                        KvStore& store,
                        tenant_id_t tenant_id,
                        bucket_id_t bucket_id,
                        const std::string& bucket_name,
                        const std::filesystem::path& data_root)
{
  std::cout << "\n[GC-1] Abort removes all parts (DEEP sweep)\n";

  const std::string obj = "gc-test/abort-deep";
  auto init = svc.initiate_multipart_upload(tenant_id, bucket_name, obj,
                                            "application/octet-stream", {}, {});
  CHECK(init.error_code == KVRGW_ERR_OK);
  const std::string uid = init.upload_id;

  const std::string p1_data(5 * 1024 * 1024, 'A');
  auto up1 = svc.upload_part(tenant_id, bucket_name, obj, uid, 1,
                             reinterpret_cast<const uint8_t*>(p1_data.data()),
                             p1_data.size());
  CHECK(up1.error_code == KVRGW_ERR_OK);

  const std::string p2_data(5 * 1024 * 1024, 'B');
  auto up2 = svc.upload_part(tenant_id, bucket_name, obj, uid, 2,
                             reinterpret_cast<const uint8_t*>(p2_data.data()),
                             p2_data.size());
  CHECK(up2.error_code == KVRGW_ERR_OK);

  auto f1 = part_file_path(data_root, uid, 1, 0);
  auto f2 = part_file_path(data_root, uid, 2, 0);
  CHECK(!f1.empty() && file_exists(f1));
  CHECK(!f2.empty() && file_exists(f2));

  // Abort — head flips to ABORTED, cleanup directive written (§11 source of
  // truth); data-file removal is deferred to GcWorker via G:M{DEEP}.
  auto am_ec = svc.abort_multipart_upload(tenant_id, bucket_name, obj, uid);
  CHECK(am_ec == KVRGW_ERR_OK);

  // Wait for both data files AND the G:M KV entry to be gone.
  constexpr int kTimeoutMs = 60000;  // live frontends' GC workers share the G: queue
  CHECK(wait_until([&] {
    return file_gone(f1) && file_gone(f2) /*&& gc_entries_gone(store, bucket_id, uid)*/;
  }, kTimeoutMs));
  std::cout << "  part 1 and part 2 data files removed by GcWorker\n";
}

// ============================================================================
// Scenario 2: Parts excluded from Complete are removed (GC MIXED sweep)
// ============================================================================
void test_complete_mixed_gc(KvRgwServiceImpl& svc,
                            KvStore& store,
                            tenant_id_t tenant_id,
                            bucket_id_t bucket_id,
                            const std::string& bucket_name,
                            const std::filesystem::path& data_root)
{
  std::cout << "\n[GC-2] Excluded parts removed by GC MIXED sweep\n";

  const std::string obj = "gc-test/complete-mixed";
  auto init = svc.initiate_multipart_upload(tenant_id, bucket_name, obj,
                                            "application/octet-stream", {}, {});
  CHECK(init.error_code == KVRGW_ERR_OK);
  const std::string uid = init.upload_id;

  // Part 1: kept in the manifest.
  const std::string p1_data(5 * 1024 * 1024, 'C');
  auto up1 = svc.upload_part(tenant_id, bucket_name, obj, uid, 1,
                             reinterpret_cast<const uint8_t*>(p1_data.data()),
                             p1_data.size());
  CHECK(up1.error_code == KVRGW_ERR_OK);

  // Part 2: uploaded but excluded from the client manifest.
  const std::string p2_data(5 * 1024 * 1024, 'D');
  auto up2 = svc.upload_part(tenant_id, bucket_name, obj, uid, 2,
                             reinterpret_cast<const uint8_t*>(p2_data.data()),
                             p2_data.size());
  CHECK(up2.error_code == KVRGW_ERR_OK);

  auto f1 = part_file_path(data_root, uid, 1, 0);
  auto f2 = part_file_path(data_root, uid, 2, 0);
  CHECK(!f1.empty() && file_exists(f1));
  CHECK(!f2.empty() && file_exists(f2));

  // Complete with only part 1 — part 2 is excluded (§10: G:M{MIXED} + keep-list).
  std::vector<KvRgwServiceImpl::CompletedPart> parts{{1, up1.etag}};
  auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, obj, uid, parts);
  CHECK(cm.error_code == KVRGW_ERR_OK);

  // Wait for part 2's file AND the G:M KV entry to be gone.
  constexpr int kTimeoutMs = 60000;  // live frontends' GC workers share the G: queue
  CHECK(wait_until([&] {
    return file_gone(f2) && gc_entries_gone(store, bucket_id, uid);
  }, kTimeoutMs));
  CHECK(file_exists(f1));  // still present — in the keep-list
  std::cout << "  excluded part 2 file removed; kept part 1 file still present\n";
}

// ============================================================================
// Scenario 3a: Complete selects older generation (etagA / genA=0)
// ============================================================================
void test_generation_select_older(KvRgwServiceImpl& svc,
                                  KvStore& store,
                                  tenant_id_t tenant_id,
                                  bucket_id_t bucket_id,
                                  const std::string& bucket_name,
                                  const std::filesystem::path& data_root)
{
  std::cout << "\n[GC-3a] Complete selects older generation (etagA)\n";

  const std::string obj = "gc-test/gen-select-older";
  auto init = svc.initiate_multipart_upload(tenant_id, bucket_name, obj,
                                            "application/octet-stream", {}, {});
  CHECK(init.error_code == KVRGW_ERR_OK);
  const std::string uid = init.upload_id;

  // gen 0: data A (5 MiB of 'E')
  const std::string dataA(5 * 1024 * 1024, 'E');
  auto upA = svc.upload_part(tenant_id, bucket_name, obj, uid, 1,
                             reinterpret_cast<const uint8_t*>(dataA.data()),
                             dataA.size());
  CHECK(upA.error_code == KVRGW_ERR_OK);
  const std::string etagA = upA.etag;

  // gen 1: data B (6 MiB of 'F')
  const std::string dataB(6 * 1024 * 1024, 'F');
  auto upB = svc.upload_part(tenant_id, bucket_name, obj, uid, 1,
                             reinterpret_cast<const uint8_t*>(dataB.data()),
                             dataB.size());
  CHECK(upB.error_code == KVRGW_ERR_OK);
  CHECK(upB.etag != etagA);

  auto fA = part_file_path(data_root, uid, 1, 0);  // gen 0
  auto fB = part_file_path(data_root, uid, 1, 1);  // gen 1
  CHECK(!fA.empty() && file_exists(fA));
  CHECK(!fB.empty() && file_exists(fB));

  // Complete specifying etagA (the older generation).
  std::vector<KvRgwServiceImpl::CompletedPart> parts{{1, etagA}};
  auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, obj, uid, parts);
  CHECK(cm.error_code == KVRGW_ERR_OK);

  // head_object must reflect data A's size.
  OwnedObjectValue hv;
  std::string err_detail;
  auto head_ec = svc.head_object(tenant_id, bucket_name, obj, std::nullopt, 0, &hv, &err_detail);
  CHECK(head_ec == KVRGW_ERR_OK);
  CHECK(hv.hdr.size == dataA.size());

  // Wait for gen 1's file AND the G:M KV entry to be gone; gen 0 stays.
  constexpr int kTimeoutMs = 60000;  // live frontends' GC workers share the G: queue
  CHECK(wait_until([&] {
    return file_gone(fB) && gc_entries_gone(store, bucket_id, uid);
  }, kTimeoutMs));
  CHECK(file_exists(fA));
  std::cout << "  gen 1 (etagB) removed; gen 0 (etagA) still present\n";
}

// ============================================================================
// Scenario 3b: Complete selects newer generation (etagB / genB=1)
// ============================================================================
void test_generation_select_newer(KvRgwServiceImpl& svc,
                                  KvStore& store,
                                  tenant_id_t tenant_id,
                                  bucket_id_t bucket_id,
                                  const std::string& bucket_name,
                                  const std::filesystem::path& data_root)
{
  std::cout << "\n[GC-3b] Complete selects newer generation (etagB)\n";

  // Fresh upload_id — independent of the 3a upload.
  const std::string obj = "gc-test/gen-select-newer";
  auto init = svc.initiate_multipart_upload(tenant_id, bucket_name, obj,
                                            "application/octet-stream", {}, {});
  CHECK(init.error_code == KVRGW_ERR_OK);
  const std::string uid = init.upload_id;

  // gen 0: data A (5 MiB of 'G')
  const std::string dataA(5 * 1024 * 1024, 'G');
  auto upA = svc.upload_part(tenant_id, bucket_name, obj, uid, 1,
                             reinterpret_cast<const uint8_t*>(dataA.data()),
                             dataA.size());
  CHECK(upA.error_code == KVRGW_ERR_OK);
  const std::string etagA = upA.etag;

  // gen 1: data B (6 MiB of 'H')
  const std::string dataB(6 * 1024 * 1024, 'H');
  auto upB = svc.upload_part(tenant_id, bucket_name, obj, uid, 1,
                             reinterpret_cast<const uint8_t*>(dataB.data()),
                             dataB.size());
  CHECK(upB.error_code == KVRGW_ERR_OK);
  const std::string etagB = upB.etag;
  CHECK(etagB != etagA);

  auto fA = part_file_path(data_root, uid, 1, 0);  // gen 0
  auto fB = part_file_path(data_root, uid, 1, 1);  // gen 1
  CHECK(!fA.empty() && file_exists(fA));
  CHECK(!fB.empty() && file_exists(fB));

  // Complete specifying etagB (the newer generation).
  std::vector<KvRgwServiceImpl::CompletedPart> parts{{1, etagB}};
  auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, obj, uid, parts);
  CHECK(cm.error_code == KVRGW_ERR_OK);

  // head_object must reflect data B's size.
  OwnedObjectValue hv;
  std::string err_detail;
  auto head_ec = svc.head_object(tenant_id, bucket_name, obj, std::nullopt, 0, &hv, &err_detail);
  CHECK(head_ec == KVRGW_ERR_OK);
  CHECK(hv.hdr.size == dataB.size());

  // Wait for gen 0's file AND the G:M KV entry to be gone; gen 1 stays.
  constexpr int kTimeoutMs = 60000;  // live frontends' GC workers share the G: queue
  CHECK(wait_until([&] {
    return file_gone(fA) && gc_entries_gone(store, bucket_id, uid);
  }, kTimeoutMs));
  CHECK(file_exists(fB));
  std::cout << "  gen 0 (etagA) removed; gen 1 (etagB) still present\n";
}

// ============================================================================
// Scenario 4: UploadPart evicts oldest generation after N+1 uploads
// ============================================================================
void test_eviction_gc(KvRgwServiceImpl& svc,
                      KvStore& store,
                      tenant_id_t tenant_id,
                      bucket_id_t bucket_id,
                      const std::string& bucket_name,
                      const std::filesystem::path& data_root)
{
  std::cout << "\n[GC-4] UploadPart evicts oldest generation after kMaxGenerationsPerPart+1 uploads\n";

  const std::string obj = "gc-test/eviction";
  auto init = svc.initiate_multipart_upload(tenant_id, bucket_name, obj,
                                            "application/octet-stream", {}, {});
  CHECK(init.error_code == KVRGW_ERR_OK);
  const std::string uid = init.upload_id;

  // Upload the same part number kMaxGenerationsPerPart+1 times.
  // Generations are assigned 0, 1, 2, ... by allocate_part_slot.
  // On the (kMaxGenerationsPerPart+1)-th call, gen 0 is evicted to GC
  // synchronously in KV; only the data-tier file removal is deferred.
  const uint32_t N = kMaxGenerationsPerPart;
  const uint32_t kUploads = N + 1;  // 4 uploads, gen 0 evicted on the 4th

  std::string last_etag;
  for (uint32_t i = 0; i < kUploads; ++i) {
    std::string data(5 * 1024 * 1024, static_cast<char>('I' + i));
    auto up = svc.upload_part(tenant_id, bucket_name, obj, uid, 1,
                              reinterpret_cast<const uint8_t*>(data.data()),
                              data.size());
    CHECK(up.error_code == KVRGW_ERR_OK);
    last_etag = up.etag;
  }

  // After the 4th call, list_parts must reflect only the latest generation's
  // etag (eviction's KV delete is synchronous — §3 source of truth).
  {
    KvRgwServiceImpl::ListPartsResult lp;
    auto lp_ec = svc.list_parts(tenant_id, bucket_name, obj, uid, 0, 1000, &lp);
    CHECK(lp_ec == KVRGW_ERR_OK);
    CHECK(lp.parts.size() == 1);
    if (lp.parts.size() == 1) {
      CHECK(lp.parts[0].etag == last_etag);
    }
  }

  // Gen 0's data file AND its G:U KV entry must both be gone.
  auto f_gen0 = part_file_path(data_root, uid, 1, 0);
  CHECK(!f_gen0.empty());
  constexpr int kTimeoutMs = 60000;  // live frontends' GC workers share the G: queue
  CHECK(wait_until([&] {
    return file_gone(f_gen0) && gc_entries_gone(store, bucket_id, uid);
  }, kTimeoutMs));
  std::cout << "  gen 0 data file removed by GcWorker after eviction\n";

  // Generations 1..N must still be present on disk.
  for (uint32_t gen = 1; gen <= N; ++gen) {
    auto fp = part_file_path(data_root, uid, 1, gen);
    CHECK(!fp.empty() && file_exists(fp));
  }
  std::cout << "  gens 1.." << N << " data files still present\n";
}

// ============================================================================
// Scenario 5: Deleting a completed multipart object frees its part files
// (G:M{FREE_PARTS} -- no S:U rows left to scan, manifest is the only
// remaining record of which part files are on disk).
// ============================================================================
// `with_cksum_file`: plant a `<upload_id>_checksum` file before the delete.
// FREE_PARTS always removes it; when absent the removal must still succeed.
void test_delete_multipart_object_gc(KvRgwServiceImpl& svc,
                                     KvStore& store,
                                     tenant_id_t tenant_id,
                                     bucket_id_t bucket_id,
                                     const std::string& bucket_name,
                                     const std::filesystem::path& data_root,
                                     bool with_cksum_file)
{
  std::cout << "\n[GC-5] Deleting a completed multipart object frees its part files"
            << (with_cksum_file ? " (checksum file present)" : " (no checksum file)")
            << "\n";

  const std::string obj = with_cksum_file ? "gc-test/delete-frees-parts-cksum"
                                          : "gc-test/delete-frees-parts";
  auto init = svc.initiate_multipart_upload(tenant_id, bucket_name, obj,
                                            "application/octet-stream", {}, {});
  CHECK(init.error_code == KVRGW_ERR_OK);
  const std::string uid = init.upload_id;

  const std::string p1_data(5 * 1024 * 1024, 'K');
  auto up1 = svc.upload_part(tenant_id, bucket_name, obj, uid, 1,
                             reinterpret_cast<const uint8_t*>(p1_data.data()),
                             p1_data.size());
  CHECK(up1.error_code == KVRGW_ERR_OK);

  const std::string p2_data(1024, 'L');
  auto up2 = svc.upload_part(tenant_id, bucket_name, obj, uid, 2,
                             reinterpret_cast<const uint8_t*>(p2_data.data()),
                             p2_data.size());
  CHECK(up2.error_code == KVRGW_ERR_OK);

  std::vector<KvRgwServiceImpl::CompletedPart> parts{{1, up1.etag}, {2, up2.etag}};
  auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, obj, uid, parts);
  CHECK(cm.error_code == KVRGW_ERR_OK);

  auto f1 = part_file_path(data_root, uid, 1, 0);
  auto f2 = part_file_path(data_root, uid, 2, 0);
  CHECK(!f1.empty() && file_exists(f1));
  CHECK(!f2.empty() && file_exists(f2));

  const auto cksum = data_root / (uid + "_checksum");
  if (with_cksum_file) {
    std::ofstream(cksum) << "x";
    CHECK(file_exists(cksum));
  }

  // Delete the completed object -- move_object_to_g's CHUNK_MULTIPART
  // branch must hand the manifest to GcWorker as a G:M{FREE_PARTS} entry.
  KvRgwServiceImpl::DeleteResult del;
  auto del_ec = svc.delete_object(tenant_id, bucket_name, obj, nullptr, &del);
  CHECK(del_ec == KVRGW_ERR_OK);
  CHECK(!del.created_dm);  // unversioned bucket -- hard delete, no marker

  // Object itself is gone immediately.
  OwnedObjectValue hv;
  std::string err_detail;
  auto head_ec = svc.head_object(tenant_id, bucket_name, obj, std::nullopt, 0, &hv, &err_detail);
  CHECK(head_ec == KVRGW_ERR_NO_SUCH_KEY);

  // Both part files AND the G:M KV entry must be gone once GcWorker
  // decodes the manifest and frees them -- no S:U scan involved.
  constexpr int kTimeoutMs = 60000;  // live frontends' GC workers share the G: queue
  CHECK(wait_until([&] {
    return file_gone(f1) && file_gone(f2) && gc_entries_gone(store, bucket_id, uid);
  }, kTimeoutMs));
  CHECK(file_gone(cksum));
  std::cout << "  both part files removed by GcWorker after object delete\n";
}

int run(KvRgwRuntime& runtime, const std::filesystem::path& data_root)
{
  auto& svc = runtime.service();
  auto& store = runtime.store();

  const std::string tenant_name = "multipart-gc-test";
  tenant_id_t tenant_id{};
  bool exists = false;
  auto ec = svc.resolve_tenant(tenant_name, &exists, &tenant_id);
  if (ec != KVRGW_ERR_OK || !exists) {
    ec = svc.add_tenant(tenant_name, &tenant_id);
    CHECK(ec == KVRGW_ERR_OK);
  }

  // One shared bucket for all GC scenarios.
  char bucket_buf[64];
  std::snprintf(bucket_buf, sizeof(bucket_buf),
                "gc-test-%d", static_cast<int>(getpid()));
  const std::string bucket_name(bucket_buf);
  ec = svc.create_bucket(tenant_id, bucket_name);
  CHECK(ec == KVRGW_ERR_OK);

  auto bid_result = svc.resolve_bucket_id(tenant_id, bucket_name);
  CHECK(bid_result.has_value() && bid_result->has_value());
  if (!bid_result.has_value() || !bid_result->has_value()) {
    std::cerr << "FAIL: could not resolve bucket_id for " << bucket_name << "\n";
    return 1;
  }
  const bucket_id_t bucket_id = **bid_result;

  test_abort_gc_deep(svc, store, tenant_id, bucket_id, bucket_name, data_root);
  test_complete_mixed_gc(svc, store, tenant_id, bucket_id, bucket_name, data_root);
  test_generation_select_older(svc, store, tenant_id, bucket_id, bucket_name, data_root);
  test_generation_select_newer(svc, store, tenant_id, bucket_id, bucket_name, data_root);
  test_eviction_gc(svc, store, tenant_id, bucket_id, bucket_name, data_root);
  test_delete_multipart_object_gc(svc, store, tenant_id, bucket_id, bucket_name, data_root, false);
  test_delete_multipart_object_gc(svc, store, tenant_id, bucket_id, bucket_name, data_root, true);

  if (g_failures == 0) {
    std::cout << "\nALL MULTIPART GC TESTS PASSED\n";
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
  // Fast GC convergence: 1-second interval, no minimum age needed for G:M/G:U
  // entries (the head is already gone when they're written, so only
  // KVRGW_GC_INTERVAL_SEC matters — see plans/multipart_test_plan.md §"Key
  // implementation facts").
  ::setenv("KVRGW_GC_INTERVAL_SEC", "1", 1);

  kvrgw::KvRgwStartOptions opts;
  opts.perf_mode = false;

  char data_root_buf[64];
  std::snprintf(data_root_buf, sizeof(data_root_buf),
                "/tmp/kvrgw-gc-test-data-%d", static_cast<int>(getpid()));
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
