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

// KvRgwServiceImpl::get_object_attributes pagination tests (plans/<copy
// bugs plan>'s "Bug/Gap 3"): no marker, mid-list marker, past-end marker,
// and non-multipart objects report has_parts=false.
//
// Isolated binary -- run against a live FoundationDB cluster.

#include "constants.hpp"
#include "error_codes.hpp"
#include "keys.hpp"
#include "kvrgw_runtime.hpp"
#include "kv_store.hpp"
#include "object_value.hpp"
#include "service_impl.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <span>
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

// ============================================================================
// Scenario 1: no marker -- full first page, part_number_marker == 0.
// Scenario 2: mid-list marker -- resumes strictly after the marker.
// Scenario 3: past-end marker -- empty page, not truncated.
// All three share the same 4-part multipart object.
// ============================================================================
void test_multipart_pagination(KvRgwServiceImpl& svc, tenant_id_t tenant_id,
                                const std::string& bucket_name)
{
  std::cout << "\n[GOA-1/2/3] Multipart pagination (no/mid/past-end marker)\n";

  const std::string key = "goa-test/paginated";
  auto init = svc.initiate_multipart_upload(tenant_id, bucket_name, key,
                                            "text/plain", {}, {});
  CHECK(init.error_code == KVRGW_ERR_OK);
  const std::string uid = init.upload_id;

  // AWS requires every part except the last to be >= 5 MiB.
  std::vector<std::string> bodies{
      std::string(5 * 1024 * 1024, 'A'), std::string(5 * 1024 * 1024, 'B'),
      std::string(5 * 1024 * 1024, 'C'), std::string(8192, 'D')};
  std::vector<KvRgwServiceImpl::CompletedPart> parts;
  for (uint16_t i = 0; i < bodies.size(); ++i) {
    auto up = svc.upload_part(tenant_id, bucket_name, key, uid,
                              static_cast<uint16_t>(i + 1),
                              reinterpret_cast<const uint8_t*>(bodies[i].data()),
                              bodies[i].size());
    CHECK(up.error_code == KVRGW_ERR_OK);
    parts.push_back({static_cast<uint16_t>(i + 1), up.etag});
  }
  auto cm = svc.complete_multipart_upload(tenant_id, bucket_name, key, uid, parts);
  CHECK(cm.error_code == KVRGW_ERR_OK);

  // --- No marker, max_parts covers all 4 parts: full single page. ---
  {
    KvRgwServiceImpl::GetObjectAttributesResult out;
    auto ec = svc.get_object_attributes(tenant_id, bucket_name, key, std::nullopt,
                                        /*max_parts=*/10, /*part_number_marker=*/0, &out);
    CHECK(ec == KVRGW_ERR_OK);
    CHECK(out.has_parts);
    CHECK(out.total_parts_count == 4);
    CHECK(out.part_numbers.size() == 4);
    CHECK(!out.parts_truncated);
    for (uint16_t i = 0; i < 4; ++i) {
      CHECK(out.part_numbers[i] == i + 1);
      CHECK(out.part_sizes[i] == bodies[i].size());
    }
  }

  // --- No marker, max_parts=2: truncated after part 2. ---
  {
    KvRgwServiceImpl::GetObjectAttributesResult out;
    auto ec = svc.get_object_attributes(tenant_id, bucket_name, key, std::nullopt,
                                        /*max_parts=*/2, /*part_number_marker=*/0, &out);
    CHECK(ec == KVRGW_ERR_OK);
    CHECK(out.has_parts);
    CHECK(out.total_parts_count == 4);
    CHECK(out.part_numbers.size() == 2);
    CHECK(out.part_numbers[0] == 1);
    CHECK(out.part_numbers[1] == 2);
    CHECK(out.parts_truncated);
    CHECK(out.next_part_number_marker == 2);
  }

  // --- Mid-list marker: resume strictly after part 2. ---
  {
    KvRgwServiceImpl::GetObjectAttributesResult out;
    auto ec = svc.get_object_attributes(tenant_id, bucket_name, key, std::nullopt,
                                        /*max_parts=*/10, /*part_number_marker=*/2, &out);
    CHECK(ec == KVRGW_ERR_OK);
    CHECK(out.has_parts);
    CHECK(out.total_parts_count == 4);
    CHECK(out.part_numbers.size() == 2);
    CHECK(out.part_numbers[0] == 3);
    CHECK(out.part_numbers[1] == 4);
    CHECK(out.part_sizes[0] == bodies[2].size());
    CHECK(out.part_sizes[1] == bodies[3].size());
    CHECK(!out.parts_truncated);
  }

  // --- Past-end marker: empty page, not truncated. ---
  {
    KvRgwServiceImpl::GetObjectAttributesResult out;
    auto ec = svc.get_object_attributes(tenant_id, bucket_name, key, std::nullopt,
                                        /*max_parts=*/10, /*part_number_marker=*/4, &out);
    CHECK(ec == KVRGW_ERR_OK);
    CHECK(out.has_parts);
    CHECK(out.total_parts_count == 4);
    CHECK(out.part_numbers.empty());
    CHECK(!out.parts_truncated);
  }

  std::cout << "  no-marker/max_parts-truncation/mid-list-marker/past-end-marker all correct\n";

  KvRgwServiceImpl::DeleteResult del;
  CHECK(svc.delete_object(tenant_id, bucket_name, key, nullptr, &del) == KVRGW_ERR_OK);
}

// ============================================================================
// Scenario 4: non-multipart object -- has_parts must be false, no synthetic
// "part 1" (unlike the PartNumber-addressing convention used elsewhere).
// ============================================================================
void test_non_multipart_no_parts(KvRgwServiceImpl& svc, tenant_id_t tenant_id,
                                  const std::string& bucket_name)
{
  std::cout << "\n[GOA-4] Non-multipart object -> has_parts=false\n";

  const std::string key = "goa-test/plain";
  const std::string content = "plain object, not multipart";
  auto pr = svc.put_object(
      tenant_id, bucket_name, key,
      reinterpret_cast<const uint8_t*>(content.data()), content.size(),
      "text/plain", content.size(), nullptr,
      std::span<const TagPair>{}, std::span<const MetaPair>{});
  CHECK(pr.error_code == KVRGW_ERR_OK);

  KvRgwServiceImpl::GetObjectAttributesResult out;
  auto ec = svc.get_object_attributes(tenant_id, bucket_name, key, std::nullopt,
                                      /*max_parts=*/10, /*part_number_marker=*/0, &out);
  CHECK(ec == KVRGW_ERR_OK);
  CHECK(!out.has_parts);
  CHECK(out.total_parts_count == 0);
  CHECK(out.part_numbers.empty());
  CHECK(out.value.hdr.size == content.size());

  std::cout << "  has_parts correctly false for plain object\n";

  KvRgwServiceImpl::DeleteResult del;
  CHECK(svc.delete_object(tenant_id, bucket_name, key, nullptr, &del) == KVRGW_ERR_OK);
}

int run(KvRgwRuntime& runtime)
{
  auto& svc = runtime.service();

  const std::string tenant_name = "multipart-goa-test";
  tenant_id_t tenant_id{};
  bool exists = false;
  auto ec = svc.resolve_tenant(tenant_name, &exists, &tenant_id);
  if (ec != KVRGW_ERR_OK || !exists) {
    ec = svc.add_tenant(tenant_name, &tenant_id);
    CHECK(ec == KVRGW_ERR_OK);
  }

  char bucket_buf[64];
  std::snprintf(bucket_buf, sizeof(bucket_buf),
                "goa-test-%d", static_cast<int>(getpid()));
  const std::string bucket_name(bucket_buf);
  ec = svc.create_bucket(tenant_id, bucket_name);
  CHECK(ec == KVRGW_ERR_OK);

  test_multipart_pagination(svc, tenant_id, bucket_name);
  test_non_multipart_no_parts(svc, tenant_id, bucket_name);

  if (g_failures == 0) {
    std::cout << "\nALL GET-OBJECT-ATTRIBUTES TESTS PASSED\n";
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
  kvrgw::KvRgwStartOptions opts;
  opts.perf_mode = false;

  char data_root_buf[64];
  std::snprintf(data_root_buf, sizeof(data_root_buf),
                "/tmp/kvrgw-goa-test-data-%d", static_cast<int>(getpid()));
  opts.data_root = data_root_buf;

  kvrgw::KvRgwRuntime runtime;
  if (!runtime.start(opts)) {
    std::cerr << "FAIL: runtime start failed\n";
    return 1;
  }

  const int rc = kvrgw::run(runtime);
  runtime.stop();
  std::error_code fs_ec;
  std::filesystem::remove_all(std::filesystem::path(data_root_buf), fs_ec);
  return rc;
}
