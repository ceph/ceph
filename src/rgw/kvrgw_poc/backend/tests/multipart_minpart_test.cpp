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

// Runtime min-part-size override (admin socket) and ListParts pagination over
// many small parts. Runs against a live FoundationDB cluster.

#include "error_codes.hpp"
#include "kvrgw_runtime.hpp"
#include "min_part_size_guard.hpp"
#include "service_impl.hpp"

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

// Uploads two parts (`first_size` then a 100-byte last part) and completes.
KvrgwErrorCode two_part_complete(KvRgwServiceImpl &svc, tenant_id_t tenant,
                                 const std::string &bucket, uint64_t first_size)
{
  const std::string obj = make_random_name("minpart-");
  auto init = svc.initiate_multipart_upload(tenant, bucket, obj, "text/plain", {}, {});
  CHECK(init.error_code == KVRGW_ERR_OK);
  const std::string p1(first_size, 'a');
  const std::string p2(100, 'b');
  auto u1 = svc.upload_part(tenant, bucket, obj, init.upload_id, 1,
                            reinterpret_cast<const uint8_t *>(p1.data()), p1.size());
  auto u2 = svc.upload_part(tenant, bucket, obj, init.upload_id, 2,
                            reinterpret_cast<const uint8_t *>(p2.data()), p2.size());
  CHECK(u1.error_code == KVRGW_ERR_OK && u2.error_code == KVRGW_ERR_OK);
  std::vector<KvRgwServiceImpl::CompletedPart> parts{{1, u1.etag}, {2, u2.etag}};
  auto cm = svc.complete_multipart_upload(tenant, bucket, obj, init.upload_id, parts);
  if (cm.error_code != KVRGW_ERR_OK) {
    (void)svc.abort_multipart_upload(tenant, bucket, obj, init.upload_id);
  }
  return cm.error_code;
}

int run(KvRgwRuntime &runtime, const std::string &admin_sock)
{
  using testutil::admin_cmd;
  auto &svc = runtime.service();

  const std::string tenant_name = "multipart-minpart-test";
  tenant_id_t tenant_id{};
  bool exists = false;
  auto ec = svc.resolve_tenant(tenant_name, &exists, &tenant_id);
  if (ec != KVRGW_ERR_OK || !exists) {
    ec = svc.add_tenant(tenant_name, &tenant_id);
    CHECK(ec == KVRGW_ERR_OK);
  }
  const std::string bucket = make_random_name("minpart-");
  CHECK(svc.create_bucket(tenant_id, bucket) == KVRGW_ERR_OK);

  // --- Admin commands ---
  CHECK(admin_cmd(admin_sock, "get-min-part-size") ==
        "min_part_size=" + std::to_string(kMinPartSizeBytes));
  CHECK(admin_cmd(admin_sock, "set-min-part-size 5120") == "OK min_part_size=5120");
  CHECK(admin_cmd(admin_sock, "get-min-part-size") == "min_part_size=5120");
  CHECK(svc.min_part_size().load() == 5120);

  // 5 KiB non-last part accepted, 4 KiB rejected.
  CHECK(two_part_complete(svc, tenant_id, bucket, 5120) == KVRGW_ERR_OK);
  CHECK(two_part_complete(svc, tenant_id, bucket, 4096) == KVRGW_ERR_ENTITY_TOO_SMALL);

  // Invalid values leave the setting unchanged.
  CHECK(admin_cmd(admin_sock, "set-min-part-size 0").rfind("ERROR", 0) == 0);
  CHECK(admin_cmd(admin_sock, "set-min-part-size abc").rfind("ERROR", 0) == 0);
  CHECK(admin_cmd(admin_sock, "set-min-part-size -5").rfind("ERROR", 0) == 0);
  CHECK(admin_cmd(admin_sock, "set-min-part-size " + std::to_string(kMinPartSizeBytes + 1))
            .rfind("ERROR", 0) == 0);
  CHECK(admin_cmd(admin_sock, "set-min-part-size").rfind("ERROR", 0) == 0);
  CHECK(admin_cmd(admin_sock, "set-min-part-size 1 2").rfind("ERROR", 0) == 0);
  CHECK(admin_cmd(admin_sock, "get-min-part-size") == "min_part_size=5120");

  // default restores 5 MiB: a 5 KiB non-last part is rejected again.
  CHECK(admin_cmd(admin_sock, "set-min-part-size default") ==
        "OK min_part_size=" + std::to_string(kMinPartSizeBytes));
  CHECK(two_part_complete(svc, tenant_id, bucket, 5120) == KVRGW_ERR_ENTITY_TOO_SMALL);

  // --- Scope guard restores the default ---
  {
    testutil::MinPartSizeGuard guard(svc, 5120);
    CHECK(svc.min_part_size().load() == 5120);
  }
  CHECK(svc.min_part_size().load() == kMinPartSizeBytes);

  // --- ListParts pagination over many small parts ---
  {
    testutil::MinPartSizeGuard guard(svc, 5120);
    constexpr uint16_t kParts = 256;
    const std::string obj = make_random_name("many-parts-");
    auto init = svc.initiate_multipart_upload(tenant_id, bucket, obj, "text/plain", {}, {});
    CHECK(init.error_code == KVRGW_ERR_OK);
    const std::string data(5120, 'z');
    for (uint16_t i = 1; i <= kParts; ++i) {
      auto up = svc.upload_part(tenant_id, bucket, obj, init.upload_id, i,
                                reinterpret_cast<const uint8_t *>(data.data()), data.size());
      CHECK(up.error_code == KVRGW_ERR_OK);
    }

    for (int max_parts : {1, 7, 100, 255, 256, 257, 1000}) {
      uint16_t marker = 0;
      uint16_t expect = 1;
      int pages = 0;
      for (;;) {
        KvRgwServiceImpl::ListPartsResult lp;
        CHECK(svc.list_parts(tenant_id, bucket, obj, init.upload_id, marker, max_parts, &lp) ==
              KVRGW_ERR_OK);
        ++pages;
        const size_t remaining = kParts - (expect - 1);
        const size_t want = std::min<size_t>(remaining, static_cast<size_t>(max_parts));
        CHECK(lp.parts.size() == want);
        for (const auto &p : lp.parts) {
          CHECK(p.part_number == expect);
          ++expect;
        }
        const bool truncated = expect <= kParts;
        CHECK(lp.is_truncated == truncated);
        if (!truncated) {
          break;
        }
        CHECK(lp.next_part_number_marker == expect - 1);
        marker = lp.next_part_number_marker;
        if (pages > kParts + 1) {
          break;
        }
      }
      CHECK(expect == kParts + 1);
    }

    // Marker at/after the end: empty, not truncated.
    KvRgwServiceImpl::ListPartsResult lp_end;
    CHECK(svc.list_parts(tenant_id, bucket, obj, init.upload_id, kParts, 10, &lp_end) ==
          KVRGW_ERR_OK);
    CHECK(lp_end.parts.empty() && !lp_end.is_truncated);

    CHECK(svc.abort_multipart_upload(tenant_id, bucket, obj, init.upload_id) == KVRGW_ERR_OK);
  }

  std::cout << (g_failures == 0 ? "ALL MULTIPART MINPART TESTS PASSED\n"
                                : "CHECK(S) FAILED\n");
  return g_failures == 0 ? 0 : 1;
}

}  // namespace
}  // namespace kvrgw

int main()
{
  char admin_sock[64];
  std::snprintf(admin_sock, sizeof(admin_sock), "/tmp/kvrgw-minpart-%d.sock",
                static_cast<int>(getpid()));
  setenv("KVRGW_ADMIN_SOCKET", admin_sock, 1);

  kvrgw::KvRgwStartOptions opts;
  opts.perf_mode = false;
  char data_root_buf[64];
  std::snprintf(data_root_buf, sizeof(data_root_buf), "/tmp/kvrgw-minpart-data-%d",
                static_cast<int>(getpid()));
  opts.data_root = data_root_buf;

  kvrgw::KvRgwRuntime runtime;
  if (!runtime.start(opts)) {
    std::cerr << "FAIL: runtime start failed\n";
    return 1;
  }
  // Give the admin thread a moment to bind.
  usleep(300 * 1000);

  const int rc = kvrgw::run(runtime, admin_sock);
  runtime.stop();
  std::error_code fs_ec;
  std::filesystem::remove_all(opts.data_root, fs_ec);
  return rc;
}
