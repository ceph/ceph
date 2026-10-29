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

// Ad hoc smoke test for Phase 2 (plans/multipart.md): Initiate/UploadPart/
// ListParts/ListMultipartUploads, run against a real FoundationDB cluster
// via KvRgwRuntime (same harness as batch_mixed_test). Not wired into
// perf_driver yet -- that is Phase 7's job; this is just a quick functional
// sanity check for Phase 2's implementation.

#include "byte_range.hpp"
#include "constants.hpp"
#include "error_codes.hpp"
#include "kvrgw_runtime.hpp"
#include "object_value.hpp"
#include "service_impl.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
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

std::string make_random_name(const char *prefix)
{
  static std::mt19937 rng(std::random_device{}());
  static constexpr std::string_view kChars = "abcdefghijklmnopqrstuvwxyz0123456789";
  std::uniform_int_distribution<size_t> dist(0, kChars.size() - 1);
  std::string name(prefix);
  for (int i = 0; i < 16; ++i) {
    name += kChars[dist(rng)];
  }
  return name;
}

int run(KvRgwRuntime &runtime)
{
  auto &svc = runtime.service();

  const std::string tenant_name = "multipart-phase2-test";
  tenant_id_t tenant_id{};
  bool exists = false;
  auto ec = svc.resolve_tenant(tenant_name, &exists, &tenant_id);
  if (ec != KVRGW_ERR_OK || !exists) {
    ec = svc.add_tenant(tenant_name, &tenant_id);
    CHECK(ec == KVRGW_ERR_OK);
  }

  const std::string bucket_name = make_random_name("mpu-test-");
  ec = svc.create_bucket(tenant_id, bucket_name);
  CHECK(ec == KVRGW_ERR_OK);

  const std::string object_name = "my/multipart/object.bin";

  // --- Malformed / nonexistent upload_id -> NoSuchUpload everywhere ---
  {
    auto up = svc.upload_part(tenant_id, bucket_name, object_name, "not-a-valid-hex-upload-id",
                              1, reinterpret_cast<const uint8_t *>("x"), 1);
    CHECK(up.error_code == KVRGW_ERR_NO_SUCH_UPLOAD);

    KvRgwServiceImpl::ListPartsResult lp;
    auto lp_ec = svc.list_parts(tenant_id, bucket_name, object_name, "deadbeefdeadbeefdeadbeef",
                                0, 1000, &lp);
    CHECK(lp_ec == KVRGW_ERR_NO_SUCH_UPLOAD);
  }

  // --- Initiate ---
  auto init = svc.initiate_multipart_upload(tenant_id, bucket_name, object_name,
                                            "application/octet-stream", {}, {});
  CHECK(init.error_code == KVRGW_ERR_OK);
  CHECK(init.upload_id.size() == 24);  // 12 bytes -> 24 hex chars
  const std::string upload_id = init.upload_id;
  std::cout << "upload_id=" << upload_id << "\n";

  // --- Phase 0 AWS validation on UploadPart ---
  {
    auto up = svc.upload_part(tenant_id, bucket_name, object_name, upload_id, 0,
                              reinterpret_cast<const uint8_t *>("x"), 1);
    CHECK(up.error_code == KVRGW_ERR_INVALID_ARGUMENT);

    up = svc.upload_part(tenant_id, bucket_name, object_name, upload_id, 10001,
                         reinterpret_cast<const uint8_t *>("x"), 1);
    CHECK(up.error_code == KVRGW_ERR_INVALID_ARGUMENT);

    up = svc.upload_part(tenant_id, bucket_name, object_name, upload_id, 1,
                         reinterpret_cast<const uint8_t *>("x"), 0);
    CHECK(up.error_code == KVRGW_ERR_INVALID_ARGUMENT);
  }

  // --- Upload part 1 ---
  const std::string part1_data(5 * 1024 * 1024, 'A');  // 5MB, >= min part size
  auto up1 = svc.upload_part(tenant_id, bucket_name, object_name, upload_id, 1,
                             reinterpret_cast<const uint8_t *>(part1_data.data()),
                             part1_data.size());
  CHECK(up1.error_code == KVRGW_ERR_OK);
  CHECK(!up1.etag.empty());
  std::cout << "part1 etag=" << up1.etag << "\n";

  // --- Upload part 2 ---
  const std::string part2_data(1024, 'B');  // small last part
  auto up2 = svc.upload_part(tenant_id, bucket_name, object_name, upload_id, 2,
                             reinterpret_cast<const uint8_t *>(part2_data.data()),
                             part2_data.size());
  CHECK(up2.error_code == KVRGW_ERR_OK);

  // --- ListParts: expect 2 committed parts ---
  {
    KvRgwServiceImpl::ListPartsResult lp;
    auto lp_ec = svc.list_parts(tenant_id, bucket_name, object_name, upload_id, 0, 1000, &lp);
    CHECK(lp_ec == KVRGW_ERR_OK);
    CHECK(lp.parts.size() == 2);
    CHECK(!lp.is_truncated);
    if (lp.parts.size() == 2) {
      CHECK(lp.parts[0].part_number == 1);
      CHECK(lp.parts[0].size == part1_data.size());
      CHECK(lp.parts[0].etag == up1.etag);
      CHECK(lp.parts[1].part_number == 2);
      CHECK(lp.parts[1].size == part2_data.size());
      CHECK(lp.parts[1].etag == up2.etag);
    }
  }

  // --- Re-upload part 1 (generation bump) ---
  const std::string part1_data_v2(6 * 1024 * 1024, 'C');
  auto up1v2 = svc.upload_part(tenant_id, bucket_name, object_name, upload_id, 1,
                               reinterpret_cast<const uint8_t *>(part1_data_v2.data()),
                               part1_data_v2.size());
  CHECK(up1v2.error_code == KVRGW_ERR_OK);
  CHECK(up1v2.etag != up1.etag);

  // --- ListParts again: part1 should now reflect the new generation ---
  {
    KvRgwServiceImpl::ListPartsResult lp;
    auto lp_ec = svc.list_parts(tenant_id, bucket_name, object_name, upload_id, 0, 1000, &lp);
    CHECK(lp_ec == KVRGW_ERR_OK);
    CHECK(lp.parts.size() == 2);
    if (lp.parts.size() == 2) {
      CHECK(lp.parts[0].etag == up1v2.etag);
      CHECK(lp.parts[0].size == part1_data_v2.size());
    }
  }

  // --- ListParts pagination: max_parts=1 ---
  {
    KvRgwServiceImpl::ListPartsResult lp;
    auto lp_ec = svc.list_parts(tenant_id, bucket_name, object_name, upload_id, 0, 1, &lp);
    CHECK(lp_ec == KVRGW_ERR_OK);
    CHECK(lp.parts.size() == 1);
    CHECK(lp.is_truncated);
    CHECK(lp.next_part_number_marker == 1);

    KvRgwServiceImpl::ListPartsResult lp2;
    lp_ec = svc.list_parts(tenant_id, bucket_name, object_name, upload_id,
                           lp.next_part_number_marker, 1, &lp2);
    CHECK(lp_ec == KVRGW_ERR_OK);
    CHECK(lp2.parts.size() == 1);
    CHECK(!lp2.is_truncated);
    if (lp2.parts.size() == 1) {
      CHECK(lp2.parts[0].part_number == 2);
    }
  }

  // --- A part still mid-upload (never committed) should be skipped by
  // ListParts -- simulate by writing a pending row directly is intrusive;
  // instead, rely on the earlier coverage of committed-only listing above.

  // --- ListMultipartUploads: our upload should be visible ---
  {
    KvRgwServiceImpl::ListMultipartUploadsResult lmu;
    auto lmu_ec = svc.list_multipart_uploads(tenant_id, bucket_name, "", "", "", 1000, &lmu);
    CHECK(lmu_ec == KVRGW_ERR_OK);
    bool found = false;
    for (const auto &u : lmu.uploads) {
      if (u.object_name == object_name && u.upload_id == upload_id) {
        found = true;
        CHECK(u.initiated_unix > 0);
      }
    }
    CHECK(found);
  }

  // --- A second, unrelated upload on a different (sorted-after) key, to
  // exercise ListMultipartUploads pagination across more than one head. ---
  const std::string object_name2 = "zzz/another/object.bin";
  auto init2 = svc.initiate_multipart_upload(tenant_id, bucket_name, object_name2,
                                             "text/plain", {}, {});
  CHECK(init2.error_code == KVRGW_ERR_OK);

  {
    KvRgwServiceImpl::ListMultipartUploadsResult lmu;
    auto lmu_ec = svc.list_multipart_uploads(tenant_id, bucket_name, "", "", "", 1, &lmu);
    CHECK(lmu_ec == KVRGW_ERR_OK);
    CHECK(lmu.uploads.size() == 1);
    CHECK(lmu.is_truncated);
    CHECK(!lmu.next_key_marker.empty());
    CHECK(!lmu.next_upload_id_marker.empty());

    KvRgwServiceImpl::ListMultipartUploadsResult lmu2;
    lmu_ec = svc.list_multipart_uploads(tenant_id, bucket_name, "", lmu.next_key_marker,
                                        lmu.next_upload_id_marker, 1000, &lmu2);
    CHECK(lmu_ec == KVRGW_ERR_OK);
    CHECK(lmu2.uploads.size() == 1);
    if (lmu2.uploads.size() == 1) {
      CHECK(lmu2.uploads[0].object_name == object_name2);
    }
  }

  // --- key_marker-only pagination (no upload-id-marker): resume strictly
  // after all of object_name's own uploads. ---
  {
    KvRgwServiceImpl::ListMultipartUploadsResult lmu;
    auto lmu_ec = svc.list_multipart_uploads(tenant_id, bucket_name, "", object_name, "",
                                             1000, &lmu);
    CHECK(lmu_ec == KVRGW_ERR_OK);
    CHECK(lmu.uploads.size() == 1);
    if (lmu.uploads.size() == 1) {
      CHECK(lmu.uploads[0].object_name == object_name2);
    }
  }

  // ==========================================================================
  // Phase 3 (plans/multipart.md): Complete / Abort.
  // ==========================================================================

  // --- Malformed upload_id -> NoSuchUpload for both Complete and Abort ---
  {
    std::vector<KvRgwServiceImpl::CompletedPart> parts{{1, "deadbeefdeadbeefdeadbeefdeadbeef"}};
    auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, object_name,
                                            "not-a-valid-hex-upload-id", parts);
    CHECK(cm.error_code == KVRGW_ERR_NO_SUCH_UPLOAD);

    auto am_ec = svc.abort_multipart_upload(tenant_id, bucket_name, object_name,
                                            "not-a-valid-hex-upload-id");
    CHECK(am_ec == KVRGW_ERR_NO_SUCH_UPLOAD);
  }

  // --- Complete Phase 0 validation (no KV touched) ---
  {
    auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, object_name, upload_id, {});
    CHECK(cm.error_code == KVRGW_ERR_INVALID_ARGUMENT);

    std::vector<KvRgwServiceImpl::CompletedPart> unsorted{
        {2, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}, {1, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}};
    cm = svc.complete_multipart_upload(tenant_id, bucket_name, object_name, upload_id, unsorted);
    CHECK(cm.error_code == KVRGW_ERR_INVALID_PART_ORDER);

    std::vector<KvRgwServiceImpl::CompletedPart> dup{
        {1, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, {1, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}};
    cm = svc.complete_multipart_upload(tenant_id, bucket_name, object_name, upload_id, dup);
    CHECK(cm.error_code == KVRGW_ERR_INVALID_PART_ORDER);

    // >10000 parts exceeds kMaxPartNumber -- Phase 0 must reject before any KV.
    std::vector<KvRgwServiceImpl::CompletedPart> too_many;
    too_many.reserve(kMaxPartNumber + 1);
    for (uint16_t i = 1; i <= kMaxPartNumber; ++i) {
      too_many.push_back({i, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"});
    }
    too_many.push_back({static_cast<uint16_t>(kMaxPartNumber + 1),
                        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"});
    cm = svc.complete_multipart_upload(tenant_id, bucket_name, object_name, upload_id, too_many);
    CHECK(cm.error_code == KVRGW_ERR_INVALID_ARGUMENT);
  }

  // --- Complete happy path on the original upload_id (parts 1,2 after the
  // earlier re-upload of part1 to 6MB; part 2 (1024B) is the client's last
  // part, so it's exempt from the >=5MB check). ---
  {
    std::vector<KvRgwServiceImpl::CompletedPart> parts{{1, up1v2.etag}, {2, up2.etag}};
    auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, object_name, upload_id, parts);
    CHECK(cm.error_code == KVRGW_ERR_OK);
    CHECK(cm.etag.size() > 32 && cm.etag.find("-2") != std::string::npos);
    std::cout << "complete etag=" << cm.etag << "\n";

    ObjectValue hv;
    std::string err_detail;
    auto head_ec = svc.head_object(tenant_id, bucket_name, object_name, std::nullopt, 0, &hv,
                                   &err_detail);
    CHECK(head_ec == KVRGW_ERR_OK);
    CHECK(hv.hdr.size == part1_data_v2.size() + part2_data.size());
    CHECK(hv.get_etag().to_hex() == cm.etag);
    CHECK(hv.content_type == "application/octet-stream");

    // Idempotent-fencing: upload_id is gone now.
    KvRgwServiceImpl::ListPartsResult lp;
    auto lp_ec = svc.list_parts(tenant_id, bucket_name, object_name, upload_id, 0, 1000, &lp);
    CHECK(lp_ec == KVRGW_ERR_NO_SUCH_UPLOAD);

    auto cm2 = svc.complete_multipart_upload(tenant_id, bucket_name, object_name, upload_id, parts);
    CHECK(cm2.error_code == KVRGW_ERR_NO_SUCH_UPLOAD);

    auto am_ec = svc.abort_multipart_upload(tenant_id, bucket_name, object_name, upload_id);
    CHECK(am_ec == KVRGW_ERR_OK);  // idempotent (already gone)
  }

  // --- Complete finding a pending part -> whole-upload abort, InvalidPart ---
  {
    const std::string object_name3 = make_random_name("pending-part-");
    auto init3 = svc.initiate_multipart_upload(tenant_id, bucket_name, object_name3,
                                               "text/plain", {}, {});
    CHECK(init3.error_code == KVRGW_ERR_OK);

    const std::string p1(5 * 1024 * 1024, 'D');
    auto up_p1 = svc.upload_part(tenant_id, bucket_name, object_name3, init3.upload_id, 1,
                                 reinterpret_cast<const uint8_t *>(p1.data()), p1.size());
    CHECK(up_p1.error_code == KVRGW_ERR_OK);

    // part 2 never uploaded -- client still lists it as part of the set.
    std::vector<KvRgwServiceImpl::CompletedPart> parts{{1, up_p1.etag},
                                                       {2, "cccccccccccccccccccccccccccccccc"}};
    auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, object_name3, init3.upload_id,
                                            parts);
    CHECK(cm.error_code == KVRGW_ERR_INVALID_PART);

    // InvalidPart rolls the head back to ACTIVE (plans/fixing_agent_mess.md
    // Section 5) -- the upload is NOT fenced, client can retry. Same
    // (still-missing) parts list reproduces the same InvalidPart.
    auto cm2 = svc.complete_multipart_upload(tenant_id, bucket_name, object_name3,
                                             init3.upload_id, parts);
    CHECK(cm2.error_code == KVRGW_ERR_INVALID_PART);
  }

  // --- Complete with wrong client-supplied etag -> abort, InvalidPart ---
  {
    const std::string object_name4 = make_random_name("etag-mismatch-");
    auto init4 = svc.initiate_multipart_upload(tenant_id, bucket_name, object_name4,
                                               "text/plain", {}, {});
    CHECK(init4.error_code == KVRGW_ERR_OK);

    const std::string p1(1024, 'E');  // sole/last part -- size exempt
    auto up_p1 = svc.upload_part(tenant_id, bucket_name, object_name4, init4.upload_id, 1,
                                 reinterpret_cast<const uint8_t *>(p1.data()), p1.size());
    CHECK(up_p1.error_code == KVRGW_ERR_OK);

    std::vector<KvRgwServiceImpl::CompletedPart> parts{
        {1, "ffffffffffffffffffffffffffffffff"}};  // deliberately wrong
    auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, object_name4,
                                            init4.upload_id, parts);
    CHECK(cm.error_code == KVRGW_ERR_INVALID_PART);
  }

  // --- Complete with a non-last included part under 5MB -> EntityTooSmall ---
  {
    const std::string object_name5 = make_random_name("too-small-");
    auto init5 = svc.initiate_multipart_upload(tenant_id, bucket_name, object_name5,
                                               "text/plain", {}, {});
    CHECK(init5.error_code == KVRGW_ERR_OK);

    const std::string p1(1024, 'F');  // too small to be a non-last part
    auto up_p1 = svc.upload_part(tenant_id, bucket_name, object_name5, init5.upload_id, 1,
                                 reinterpret_cast<const uint8_t *>(p1.data()), p1.size());
    CHECK(up_p1.error_code == KVRGW_ERR_OK);
    const std::string p2(1024, 'G');
    auto up_p2 = svc.upload_part(tenant_id, bucket_name, object_name5, init5.upload_id, 2,
                                 reinterpret_cast<const uint8_t *>(p2.data()), p2.size());
    CHECK(up_p2.error_code == KVRGW_ERR_OK);

    std::vector<KvRgwServiceImpl::CompletedPart> parts{{1, up_p1.etag}, {2, up_p2.etag}};
    auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, object_name5,
                                            init5.upload_id, parts);
    CHECK(cm.error_code == KVRGW_ERR_ENTITY_TOO_SMALL);
  }

  // --- Complete excluding an uploaded part (client's manifest omits it) ---
  {
    const std::string object_name6 = make_random_name("excluded-part-");
    auto init6 = svc.initiate_multipart_upload(tenant_id, bucket_name, object_name6,
                                               "text/plain", {}, {});
    CHECK(init6.error_code == KVRGW_ERR_OK);

    const std::string p1(5 * 1024 * 1024, 'H');
    auto up_p1 = svc.upload_part(tenant_id, bucket_name, object_name6, init6.upload_id, 1,
                                 reinterpret_cast<const uint8_t *>(p1.data()), p1.size());
    CHECK(up_p1.error_code == KVRGW_ERR_OK);
    const std::string p2(1024, 'I');  // uploaded, but will be excluded by the client
    auto up_p2 = svc.upload_part(tenant_id, bucket_name, object_name6, init6.upload_id, 2,
                                 reinterpret_cast<const uint8_t *>(p2.data()), p2.size());
    CHECK(up_p2.error_code == KVRGW_ERR_OK);

    std::vector<KvRgwServiceImpl::CompletedPart> parts{{1, up_p1.etag}};  // part 2 excluded
    auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, object_name6,
                                            init6.upload_id, parts);
    CHECK(cm.error_code == KVRGW_ERR_OK);
    CHECK(cm.etag.find("-1") != std::string::npos);

    ObjectValue hv;
    std::string err_detail;
    auto head_ec = svc.head_object(tenant_id, bucket_name, object_name6, std::nullopt, 0, &hv,
                                   &err_detail);
    CHECK(head_ec == KVRGW_ERR_OK);
    CHECK(hv.hdr.size == p1.size());  // excluded part's size not counted
  }

  // --- GetObject / Range / HeadObject on a completed multipart object with
  // 3 distinct-content parts -- exercises the CHUNK_MULTIPART manifest
  // readback path: full serial read across every part, and Range reads
  // that must first find the part holding the start offset (including a
  // range entirely inside the *last* part, not part 1). ---
  {
    const std::string object_name8 = make_random_name("get-range-");
    auto init8 = svc.initiate_multipart_upload(tenant_id, bucket_name, object_name8,
                                               "text/plain", {}, {});
    CHECK(init8.error_code == KVRGW_ERR_OK);

    const std::string p1(5 * 1024 * 1024, 'X');  // part 1: 5MB of 'X'
    const std::string p2(6 * 1024 * 1024, 'Y');  // part 2: 6MB of 'Y'
    const std::string p3(1024, 'Z');             // part 3: 1KB of 'Z' (last, exempt)
    auto up_p1 = svc.upload_part(tenant_id, bucket_name, object_name8, init8.upload_id, 1,
                                 reinterpret_cast<const uint8_t *>(p1.data()), p1.size());
    CHECK(up_p1.error_code == KVRGW_ERR_OK);
    auto up_p2 = svc.upload_part(tenant_id, bucket_name, object_name8, init8.upload_id, 2,
                                 reinterpret_cast<const uint8_t *>(p2.data()), p2.size());
    CHECK(up_p2.error_code == KVRGW_ERR_OK);
    auto up_p3 = svc.upload_part(tenant_id, bucket_name, object_name8, init8.upload_id, 3,
                                 reinterpret_cast<const uint8_t *>(p3.data()), p3.size());
    CHECK(up_p3.error_code == KVRGW_ERR_OK);

    std::vector<KvRgwServiceImpl::CompletedPart> parts{
        {1, up_p1.etag}, {2, up_p2.etag}, {3, up_p3.etag}};
    auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, object_name8,
                                            init8.upload_id, parts);
    CHECK(cm.error_code == KVRGW_ERR_OK);

    const std::string full = p1 + p2 + p3;

    ObjectValue hv;
    std::string err_detail;
    auto head_ec = svc.head_object(tenant_id, bucket_name, object_name8, std::nullopt, 0, &hv,
                                   &err_detail);
    CHECK(head_ec == KVRGW_ERR_OK);
    CHECK(hv.hdr.size == full.size());
    CHECK(hv.get_etag().to_hex() == cm.etag);
    CHECK(hv.hdr.chunk_type == CHUNK_MULTIPART);

    // Full GetObject (no range) -- serial read, one part after another.
    {
      KvRgwServiceImpl::GetObjectResult gr;
      auto get_ec = svc.get_object(tenant_id, bucket_name, object_name8, std::nullopt,
                                   nullptr, 0, &gr);
      CHECK(get_ec == KVRGW_ERR_OK);
      CHECK(gr.body == full);
    }

    // Range entirely inside part 1.
    {
      ByteRange range{10, 20, false, false};
      KvRgwServiceImpl::GetObjectResult gr;
      auto get_ec = svc.get_object(tenant_id, bucket_name, object_name8, std::nullopt,
                                   &range, 0, &gr);
      CHECK(get_ec == KVRGW_ERR_OK);
      CHECK(gr.body == full.substr(10, 11));
    }

    // Range spanning the part1/part2 boundary.
    {
      const int64_t start = static_cast<int64_t>(p1.size()) - 5;
      const int64_t end = static_cast<int64_t>(p1.size()) + 5;
      ByteRange range{start, end, false, false};
      KvRgwServiceImpl::GetObjectResult gr;
      auto get_ec = svc.get_object(tenant_id, bucket_name, object_name8, std::nullopt,
                                   &range, 0, &gr);
      CHECK(get_ec == KVRGW_ERR_OK);
      CHECK(gr.body == full.substr(static_cast<size_t>(start),
                                   static_cast<size_t>(end - start + 1)));
    }

    // Range entirely inside part 3 (the *last* part) -- the manifest
    // lookup must find part 3, not start scanning from part 1.
    {
      const int64_t start = static_cast<int64_t>(p1.size() + p2.size()) + 5;
      const int64_t end = start + 9;
      ByteRange range{start, end, false, false};
      KvRgwServiceImpl::GetObjectResult gr;
      auto get_ec = svc.get_object(tenant_id, bucket_name, object_name8, std::nullopt,
                                   &range, 0, &gr);
      CHECK(get_ec == KVRGW_ERR_OK);
      CHECK(gr.body == full.substr(static_cast<size_t>(start),
                                   static_cast<size_t>(end - start + 1)));
    }

    // Suffix range -- last 100 bytes (crosses into part 3 only).
    {
      ByteRange range{};
      range.from_end = true;
      range.end = 100;
      KvRgwServiceImpl::GetObjectResult gr;
      auto get_ec = svc.get_object(tenant_id, bucket_name, object_name8, std::nullopt,
                                   &range, 0, &gr);
      CHECK(get_ec == KVRGW_ERR_OK);
      CHECK(gr.body == full.substr(full.size() - 100));
    }

    // PartNumber addressing -- whole-part read, size override, PartsCount.
    CHECK(hv.hdr.part_count == 3);
    {
      ObjectValue hv1;
      std::string err_detail1;
      auto head_ec1 = svc.head_object(tenant_id, bucket_name, object_name8, std::nullopt,
                                      1, &hv1, &err_detail1);
      CHECK(head_ec1 == KVRGW_ERR_OK);
      CHECK(hv1.hdr.size == p1.size());
      CHECK(hv1.get_etag().to_hex() == cm.etag);  // whole-object composite ETag

      ObjectValue hv3;
      std::string err_detail3;
      auto head_ec3 = svc.head_object(tenant_id, bucket_name, object_name8, std::nullopt,
                                      3, &hv3, &err_detail3);
      CHECK(head_ec3 == KVRGW_ERR_OK);
      CHECK(hv3.hdr.size == p3.size());

      KvRgwServiceImpl::GetObjectResult gr1;
      auto get_ec1 = svc.get_object(tenant_id, bucket_name, object_name8, std::nullopt,
                                    nullptr, 1, &gr1);
      CHECK(get_ec1 == KVRGW_ERR_OK);
      CHECK(gr1.body == p1);
      CHECK(gr1.value.hdr.size == p1.size());
      CHECK(gr1.value.get_etag().to_hex() == cm.etag);

      KvRgwServiceImpl::GetObjectResult gr3;
      auto get_ec3 = svc.get_object(tenant_id, bucket_name, object_name8, std::nullopt,
                                    nullptr, 3, &gr3);
      CHECK(get_ec3 == KVRGW_ERR_OK);
      CHECK(gr3.body == p3);

      // Out of range (part_count == 3) -> InvalidPart.
      KvRgwServiceImpl::GetObjectResult gr_bad;
      auto get_ec_bad = svc.get_object(tenant_id, bucket_name, object_name8, std::nullopt,
                                       nullptr, 4, &gr_bad);
      CHECK(get_ec_bad == KVRGW_ERR_INVALID_PART);

      ObjectValue hv_bad;
      std::string err_detail_bad;
      auto head_ec_bad = svc.head_object(tenant_id, bucket_name, object_name8, std::nullopt,
                                         4, &hv_bad, &err_detail_bad);
      CHECK(head_ec_bad == KVRGW_ERR_INVALID_PART);
    }
  }

  // --- PartNumber on a non-multipart object: part_count defaults to 1 ---
  {
    const std::string object_name9 = make_random_name("partnum-single-");
    const std::string content = "plain object, not multipart";
    auto pr = svc.put_object(
        tenant_id, bucket_name, object_name9,
        reinterpret_cast<const uint8_t*>(content.data()), content.size(),
        "text/plain", content.size(), nullptr,
        std::span<const TagPair>{}, std::span<const MetaPair>{});
    CHECK(pr.error_code == KVRGW_ERR_OK);

    KvRgwServiceImpl::GetObjectResult gr_ok;
    auto get_ec_ok = svc.get_object(tenant_id, bucket_name, object_name9, std::nullopt,
                                    nullptr, 1, &gr_ok);
    CHECK(get_ec_ok == KVRGW_ERR_OK);
    CHECK(gr_ok.body == content);

    KvRgwServiceImpl::GetObjectResult gr_bad;
    auto get_ec_bad = svc.get_object(tenant_id, bucket_name, object_name9, std::nullopt,
                                     nullptr, 2, &gr_bad);
    CHECK(get_ec_bad == KVRGW_ERR_INVALID_PART);
  }

  // --- Explicit Abort ---
  {
    const std::string object_name7 = make_random_name("explicit-abort-");
    auto init7 = svc.initiate_multipart_upload(tenant_id, bucket_name, object_name7,
                                               "text/plain", {}, {});
    CHECK(init7.error_code == KVRGW_ERR_OK);

    const std::string p1(1024, 'J');
    auto up_p1 = svc.upload_part(tenant_id, bucket_name, object_name7, init7.upload_id, 1,
                                 reinterpret_cast<const uint8_t *>(p1.data()), p1.size());
    CHECK(up_p1.error_code == KVRGW_ERR_OK);

    auto am_ec = svc.abort_multipart_upload(tenant_id, bucket_name, object_name7,
                                            init7.upload_id);
    CHECK(am_ec == KVRGW_ERR_OK);

    // Double-Abort -- idempotent.
    am_ec = svc.abort_multipart_upload(tenant_id, bucket_name, object_name7, init7.upload_id);
    CHECK(am_ec == KVRGW_ERR_OK);

    // Complete-after-Abort -> NoSuchUpload.
    std::vector<KvRgwServiceImpl::CompletedPart> parts{{1, up_p1.etag}};
    auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, object_name7,
                                            init7.upload_id, parts);
    CHECK(cm.error_code == KVRGW_ERR_NO_SUCH_UPLOAD);
  }

  if (g_failures == 0) {
    std::cout << "ALL MULTIPART PHASE2/3 TESTS PASSED\n";
  }
  else {
    std::cout << g_failures << " CHECK(S) FAILED\n";
  }
  return g_failures == 0 ? 0 : 1;
}

}  // namespace
}  // namespace kvrgw

int main()
{
  kvrgw::KvRgwStartOptions opts;
  opts.perf_mode = false;

  char data_root_buf[64];
  std::snprintf(data_root_buf, sizeof(data_root_buf),
               "/tmp/kvrgw-mpu-test-data-%d", static_cast<int>(getpid()));
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
