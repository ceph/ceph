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

// Multipart checksums (x-amz-checksum-*) through the service API.
// Runs against a live FoundationDB cluster.

#include "checksum.hpp"
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

using Svc = KvRgwServiceImpl;

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

const uint8_t *u8(const std::string &s) { return reinterpret_cast<const uint8_t *>(s.data()); }

constexpr char kSha256_1024A[] = "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+0=";

std::string part_data(unsigned i)
{
  char buf[16];
  const int n = std::snprintf(buf, sizeof(buf), "part-%u", i);
  return std::string(buf, static_cast<size_t>(n));
}

std::string b64_sha256(const std::string &s)
{
  const std::string d = cksum_digest(CksumAlg::SHA256, s.data(), s.size());
  return base64_encode(reinterpret_cast<const uint8_t *>(d.data()), d.size());
}

// Uploads `nparts` tiny SHA256-checksummed parts, completes, and verifies the
// read paths: PartNumber HEAD/GET checksums and GetObjectAttributes pagination.
void check_read_paths(Svc &svc, tenant_id_t tenant, const std::string &bucket, unsigned nparts)
{
  testutil::MinPartSizeGuard guard(svc, 1);
  const std::string obj = make_random_name("rd-");
  auto init = svc.initiate_multipart_upload(tenant, bucket, obj, "t", {}, {}, CksumAlg::SHA256,
                                            CksumType::NONE);
  CHECK(init.error_code == KVRGW_ERR_OK);
  std::vector<Svc::CompletedPart> parts;
  std::vector<std::string> expect;
  for (unsigned i = 1; i <= nparts; ++i) {
    const std::string d = part_data(i);
    auto u = svc.upload_part(tenant, bucket, obj, init.upload_id, static_cast<uint16_t>(i),
                             u8(d), d.size());
    CHECK(u.error_code == KVRGW_ERR_OK);
    expect.push_back(u.checksum);
    CHECK(u.checksum == b64_sha256(d));
    parts.push_back({static_cast<uint16_t>(i), u.etag, CksumAlg::SHA256, u.checksum});
  }
  auto cm = svc.complete_multipart_upload(tenant, bucket, obj, init.upload_id, parts);
  CHECK(cm.error_code == KVRGW_ERR_OK);
  CHECK(cm.checksum_alg == CksumAlg::SHA256 && cm.checksum_type == CksumType::COMPOSITE);

  auto b64 = [](const std::string &raw) {
    return base64_encode(reinterpret_cast<const uint8_t *>(raw.data()), raw.size());
  };

  // Whole-object HEAD carries the combined checksum.
  OwnedObjectValue head;
  std::string detail;
  CHECK(svc.head_object(tenant, bucket, obj, std::nullopt, 0, &head, &detail) == KVRGW_ERR_OK);
  CHECK(head.cksum_alg() == CksumAlg::SHA256 && head.cksum_type() == CksumType::COMPOSITE);
  char rendered[64];
  const size_t rn = cksum_render(head.cksum_type(), head.cksum_digest().data(),
                                 head.cksum_combined_len(), head.part_count(), rendered);
  CHECK(std::string(rendered, rn) == cm.checksum);

  // PartNumber HEAD/GET: first, last and a middle part.
  for (unsigned pn : {1u, (nparts + 1) / 2, nparts}) {
    std::string pc;
    OwnedObjectValue hv;
    CHECK(svc.head_object(tenant, bucket, obj, std::nullopt, static_cast<uint16_t>(pn), &hv,
                          &detail, &pc) == KVRGW_ERR_OK);
    CHECK(b64(pc) == expect[pn - 1]);
    Svc::GetObjectResult gr;
    CHECK(svc.get_object(tenant, bucket, obj, std::nullopt, nullptr, static_cast<uint16_t>(pn),
                         &gr) == KVRGW_ERR_OK);
    CHECK(b64(gr.part_cksum) == expect[pn - 1]);
    CHECK(gr.body == part_data(pn));
  }

  // GetObjectAttributes pagination.
  struct Page {
    uint16_t max_parts;
    uint16_t marker;
  };
  const uint16_t n16 = static_cast<uint16_t>(nparts);
  const Page pages[] = {
      {1000, 0},        {3, 0},           {3, 2},   {n16, 0},
      {static_cast<uint16_t>(n16 + 5), 0}, {5, static_cast<uint16_t>(n16 > 2 ? n16 - 2 : 0)},
      {5, n16},         {5, static_cast<uint16_t>(n16 + 10)},
  };
  for (const Page &pg : pages) {
    Svc::GetObjectAttributesResult out;
    CHECK(svc.get_object_attributes(tenant, bucket, obj, std::nullopt, pg.max_parts, pg.marker,
                                    &out) == KVRGW_ERR_OK);
    CHECK(out.has_parts && out.total_parts_count == nparts);
    CHECK(out.value.cksum_alg() == CksumAlg::SHA256);
    const unsigned first = pg.marker + 1;
    unsigned want = first > nparts ? 0 : std::min<unsigned>(pg.max_parts, nparts - first + 1);
    CHECK(out.part_numbers.size() == want);
    CHECK(out.part_cksums.size() == want * 32);
    for (size_t i = 0; i < out.part_numbers.size(); ++i) {
      CHECK(out.part_numbers[i] == first + i);
      CHECK(b64(out.part_cksums.substr(i * 32, 32)) == expect[first + i - 1]);
    }
    CHECK(out.parts_truncated == (first + want <= nparts));
    if (out.parts_truncated) {
      CHECK(out.next_part_number_marker == first + want - 1);
    }
  }
}

int run(KvRgwRuntime &runtime)
{
  auto &svc = runtime.service();

  const std::string tenant_name = "multipart-cksum-test";
  tenant_id_t tenant{};
  bool exists = false;
  auto ec = svc.resolve_tenant(tenant_name, &exists, &tenant);
  if (ec != KVRGW_ERR_OK || !exists) {
    ec = svc.add_tenant(tenant_name, &tenant);
    CHECK(ec == KVRGW_ERR_OK);
  }
  const std::string bucket = make_random_name("cksum-");
  CHECK(svc.create_bucket(tenant, bucket) == KVRGW_ERR_OK);

  const std::string body(1024, 'A');

  // --- CreateMultipart argument validation ---
  {
    const std::string obj = make_random_name("bad-");
    CHECK(svc.initiate_multipart_upload(tenant, bucket, obj, "t", {}, {}, CksumAlg::NONE,
                                        CksumType::COMPOSITE).error_code ==
          KVRGW_ERR_INVALID_REQUEST);
    CHECK(svc.initiate_multipart_upload(tenant, bucket, obj, "t", {}, {}, CksumAlg::CRC64NVME,
                                        CksumType::COMPOSITE).error_code ==
          KVRGW_ERR_INVALID_REQUEST);
    CHECK(svc.initiate_multipart_upload(tenant, bucket, obj, "t", {}, {}, CksumAlg::SHA256,
                                        CksumType::FULL_OBJECT).error_code ==
          KVRGW_ERR_INVALID_REQUEST);
  }

  // --- UploadPart: stored checksum, client verification ---
  {
    const std::string obj = make_random_name("up-");
    auto init = svc.initiate_multipart_upload(tenant, bucket, obj, "t", {}, {}, CksumAlg::SHA256,
                                              CksumType::NONE);
    CHECK(init.error_code == KVRGW_ERR_OK);

    // Server computes it when the client sent none.
    auto u = svc.upload_part(tenant, bucket, obj, init.upload_id, 1, u8(body), body.size());
    CHECK(u.error_code == KVRGW_ERR_OK);
    CHECK(u.checksum == kSha256_1024A);
    CHECK(u.checksum_alg == CksumAlg::SHA256);

    // Matching client value.
    u = svc.upload_part(tenant, bucket, obj, init.upload_id, 1, u8(body), body.size(),
                        CksumAlg::SHA256, kSha256_1024A);
    CHECK(u.error_code == KVRGW_ERR_OK && u.checksum == kSha256_1024A);

    // Wrong value -> BadDigest; wrong algorithm -> InvalidRequest.
    u = svc.upload_part(tenant, bucket, obj, init.upload_id, 1, u8(body), body.size(),
                        CksumAlg::SHA256, "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+1=");
    CHECK(u.error_code == KVRGW_ERR_BAD_DIGEST);
    u = svc.upload_part(tenant, bucket, obj, init.upload_id, 1, u8(body), body.size(),
                        CksumAlg::SHA256, "bad");
    CHECK(u.error_code == KVRGW_ERR_BAD_DIGEST);
    u = svc.upload_part(tenant, bucket, obj, init.upload_id, 1, u8(body), body.size(),
                        CksumAlg::CRC32, "AAAAAA==");
    CHECK(u.error_code == KVRGW_ERR_BAD_DIGEST || u.error_code == KVRGW_ERR_INVALID_REQUEST);
    CHECK(svc.abort_multipart_upload(tenant, bucket, obj, init.upload_id) == KVRGW_ERR_OK);
  }

  // --- UploadPart on an upload without an algorithm ---
  {
    const std::string obj = make_random_name("noalg-");
    auto init = svc.initiate_multipart_upload(tenant, bucket, obj, "t", {}, {});
    CHECK(init.error_code == KVRGW_ERR_OK);
    auto u = svc.upload_part(tenant, bucket, obj, init.upload_id, 1, u8(body), body.size());
    CHECK(u.error_code == KVRGW_ERR_OK && u.checksum.empty() &&
          u.checksum_alg == CksumAlg::NONE);
    // A client value is verified but not stored.
    u = svc.upload_part(tenant, bucket, obj, init.upload_id, 1, u8(body), body.size(),
                        CksumAlg::SHA256, kSha256_1024A);
    CHECK(u.error_code == KVRGW_ERR_OK && u.checksum.empty());
    u = svc.upload_part(tenant, bucket, obj, init.upload_id, 1, u8(body), body.size(),
                        CksumAlg::SHA256, "arcu6553sHVAiX4MjW0j7I7vD4w6R+Gz9Ok0Q9lTa+1=");
    CHECK(u.error_code == KVRGW_ERR_BAD_DIGEST);
    CHECK(svc.abort_multipart_upload(tenant, bucket, obj, init.upload_id) == KVRGW_ERR_OK);
  }

  // --- All algorithms: part digest equals the reference ---
  for (CksumAlg alg : {CksumAlg::CRC32, CksumAlg::CRC32C, CksumAlg::SHA1, CksumAlg::SHA256,
                       CksumAlg::CRC64NVME}) {
    const std::string obj = make_random_name("alg-");
    auto init = svc.initiate_multipart_upload(tenant, bucket, obj, "t", {}, {}, alg,
                                              CksumType::NONE);
    CHECK(init.error_code == KVRGW_ERR_OK);
    auto u = svc.upload_part(tenant, bucket, obj, init.upload_id, 1, u8(body), body.size());
    const std::string d = cksum_digest(alg, body.data(), body.size());
    CHECK(u.error_code == KVRGW_ERR_OK);
    CHECK(u.checksum == base64_encode(reinterpret_cast<const uint8_t *>(d.data()), d.size()));
    CHECK(u.checksum_alg == alg);
    CHECK(svc.abort_multipart_upload(tenant, bucket, obj, init.upload_id) == KVRGW_ERR_OK);
  }

  // --- Complete: s3-test vectors (3 parts of 5 MiB 'A'/'B'/'C') ---
  struct Vec {
    CksumAlg alg;
    CksumType type;
    const char *combined;
  };
  const Vec vecs[] = {
      {CksumAlg::SHA256, CksumType::COMPOSITE, "uWBwpe1dxI4Vw8Gf0X9ynOdw/SS6VBzfWm9giiv1sf4=-3"},
      {CksumAlg::SHA1, CksumType::COMPOSITE, "sizjvY4eud3MrcHdZM3cQ/ol39o=-3"},
      {CksumAlg::CRC64NVME, CksumType::FULL_OBJECT, "i+6LR0y3eFo="},
  };
  // CRC32 / CRC32C accept FULL_OBJECT for multipart (s3-tests); the other
  // composite-only algorithms do not.
  for (CksumAlg alg : {CksumAlg::CRC32, CksumAlg::CRC32C}) {
    const std::string obj = make_random_name("full-");
    auto init = svc.initiate_multipart_upload(tenant, bucket, obj, "t", {}, {}, alg,
                                              CksumType::FULL_OBJECT);
    CHECK(init.error_code == KVRGW_ERR_OK);
    CHECK(svc.abort_multipart_upload(tenant, bucket, obj, init.upload_id) == KVRGW_ERR_OK);
  }
  const size_t kPart = 5 * 1024 * 1024;
  for (const Vec &v : vecs) {
    const std::string obj = make_random_name("vec-");
    auto init = svc.initiate_multipart_upload(tenant, bucket, obj, "t", {}, {}, v.alg,
                                              v.type);
    CHECK(init.error_code == KVRGW_ERR_OK);
    std::vector<Svc::CompletedPart> parts;
    for (int i = 0; i < 3; ++i) {
      const std::string data(kPart, static_cast<char>('A' + i));
      auto u = svc.upload_part(tenant, bucket, obj, init.upload_id, i + 1, u8(data), data.size());
      CHECK(u.error_code == KVRGW_ERR_OK);
      parts.push_back({static_cast<uint16_t>(i + 1), u.etag, v.alg, u.checksum});
    }
    // Bad whole-object checksum -> BadDigest, upload stays retryable.
    CompleteCksumReq bad{v.alg, CksumType::NONE, "bad"};
    auto cm = svc.complete_multipart_upload(tenant, bucket, obj, init.upload_id, parts, {}, {}, bad);
    CHECK(cm.error_code == KVRGW_ERR_BAD_DIGEST);
    // Mismatching type -> InvalidRequest.
    CompleteCksumReq badtype{v.alg, v.type == CksumType::COMPOSITE ? CksumType::FULL_OBJECT
                                                                    : CksumType::COMPOSITE,
                             v.combined};
    cm = svc.complete_multipart_upload(tenant, bucket, obj, init.upload_id, parts, {}, {}, badtype);
    CHECK(cm.error_code == KVRGW_ERR_INVALID_REQUEST);
    // Missing per-part checksum + bad whole-object checksum -> BadDigest.
    auto no_ck = parts;
    no_ck[1].cksum_alg = CksumAlg::NONE;
    no_ck[1].cksum.clear();
    cm = svc.complete_multipart_upload(tenant, bucket, obj, init.upload_id, no_ck, {}, {}, bad);
    CHECK(cm.error_code == KVRGW_ERR_BAD_DIGEST);
    // Wrong per-part checksum -> BadDigest.
    auto wrong_ck = parts;
    wrong_ck[0].cksum = parts[1].cksum;
    cm = svc.complete_multipart_upload(tenant, bucket, obj, init.upload_id, wrong_ck);
    CHECK(cm.error_code == KVRGW_ERR_BAD_DIGEST);
    // Correct completion after the rejections.
    CompleteCksumReq good{v.alg, CksumType::NONE, v.combined};
    cm = svc.complete_multipart_upload(tenant, bucket, obj, init.upload_id, parts, {}, {}, good);
    CHECK(cm.error_code == KVRGW_ERR_OK);
    CHECK(cm.checksum == v.combined);
    CHECK(cm.checksum_alg == v.alg && cm.checksum_type == v.type);
    // A finished upload cannot be completed again.
    cm = svc.complete_multipart_upload(tenant, bucket, obj, init.upload_id, parts);
    CHECK(cm.error_code == KVRGW_ERR_NO_SUCH_UPLOAD);
  }

  // --- Complete: single part composite, plain completion without request checksum ---
  {
    const std::string obj = make_random_name("one-");
    auto init = svc.initiate_multipart_upload(tenant, bucket, obj, "t", {}, {}, CksumAlg::SHA256,
                                              CksumType::NONE);
    auto u = svc.upload_part(tenant, bucket, obj, init.upload_id, 1, u8(body), body.size(),
                             CksumAlg::SHA256, kSha256_1024A);
    CHECK(u.error_code == KVRGW_ERR_OK);
    std::vector<Svc::CompletedPart> parts{{1, u.etag, CksumAlg::SHA256, u.checksum}};
    auto cm = svc.complete_multipart_upload(
        tenant, bucket, obj, init.upload_id, parts, {}, {},
        CompleteCksumReq{CksumAlg::SHA256, CksumType::NONE,
                         "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1"});
    CHECK(cm.error_code == KVRGW_ERR_OK);
    CHECK(cm.checksum == "Ok6Cs5b96ux6+MWQkJO7UBT5sKPBeXBLwvj/hK89smg=-1");
  }

  // --- Complete on an upload without an algorithm: no checksum, parts need none ---
  {
    const std::string obj = make_random_name("plain-");
    auto init = svc.initiate_multipart_upload(tenant, bucket, obj, "t", {}, {});
    auto u = svc.upload_part(tenant, bucket, obj, init.upload_id, 1, u8(body), body.size());
    std::vector<Svc::CompletedPart> parts{{1, u.etag}};
    auto cm = svc.complete_multipart_upload(tenant, bucket, obj, init.upload_id, parts);
    CHECK(cm.error_code == KVRGW_ERR_OK && cm.checksum.empty() &&
          cm.checksum_alg == CksumAlg::NONE);
  }

  // --- Read paths in every storage tier: 8 (embedded), 100 (S child),
  // 256 and 10000 (data-tier file) parts ---
  for (unsigned n : {1u, 8u, 100u, 256u, 10000u}) {
    check_read_paths(svc, tenant, bucket, n);
    std::cout << "read paths ok: " << n << " parts\n";
  }

  std::cout << (g_failures == 0 ? "ALL MULTIPART CKSUM TESTS PASSED\n" : "CHECK(S) FAILED\n");
  return g_failures == 0 ? 0 : 1;
}

}  // namespace
}  // namespace kvrgw

int main()
{
  kvrgw::KvRgwStartOptions opts;
  opts.perf_mode = false;
  char data_root_buf[64];
  std::snprintf(data_root_buf, sizeof(data_root_buf), "/tmp/kvrgw-cksum-data-%d",
                static_cast<int>(getpid()));
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
