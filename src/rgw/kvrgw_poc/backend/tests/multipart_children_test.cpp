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

// Multipart 'M' (manifest) and 'S' (checksum blob) child KVs and the tier 3
// <ref_tag>_checksum file (plans/multipart_checksum.md, sub-task 2): placement,
// every delete path, and CopyObject sharing. GC-sensitive: run against a live
// FoundationDB cluster with no other frontend GC workers running.
//
// Data (plans/refactor_multipart_tests.md): every object carries a position-
// tagged pattern -- the 8-byte unit at global offset p (multiple of 8) holds
// base + p -- so any wrong offset, part or version changes the bytes. Parts are
// uploaded in random order. Every read check (verify_reads) does a full
// sequential GET verification, a full part listing with every part's number,
// size and checksum, a PartNumber GET of every part, and lg(N) random ranges
// that cross part boundaries. KVRGW_TEST_SEED replays a run.

#include "checksum.hpp"
#include "constants.hpp"
#include "error_codes.hpp"
#include "keys.hpp"
#include "kv_store.hpp"
#include "kvrgw_runtime.hpp"
#include "min_part_size_guard.hpp"
#include "object_value.hpp"
#include "service_impl.hpp"
#include "tier_config_state.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <iostream>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>
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

// ---- seeded randomness ---------------------------------------------------------

uint64_t g_seed_state = 0;
uint64_t g_range_calls = 0;

uint64_t splitmix64(uint64_t &s)
{
  uint64_t z = (s += 0x9E3779B97F4A7C15ULL);
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
  return z ^ (z >> 31);
}

struct Rng {
  uint64_t s;
  uint64_t next() { return splitmix64(s); }
  uint64_t below(uint64_t n) { return next() % n; }
};

// A fresh pattern base for every object.
uint64_t next_base() { return splitmix64(g_seed_state); }

void init_seed()
{
  uint64_t seed;
  if (const char *e = std::getenv("KVRGW_TEST_SEED"); e != nullptr && *e != '\0') {
    seed = std::strtoull(e, nullptr, 0);
  } else {
    std::random_device rd;
    seed = (static_cast<uint64_t>(rd()) << 32) | rd();
  }
  g_seed_state = seed;
  std::cout << "master seed: " << seed << " (replay with KVRGW_TEST_SEED=" << seed << ")\n";
}

// ---- position-tagged data ------------------------------------------------------
// Byte at global offset x is byte (x % 8) of the native-endian uint64_t
// (base + (x & ~7)). Independent of how the object is split into parts.

void fill(uint8_t *dst, uint64_t base, uint64_t off, size_t len)
{
  const size_t head = off & 7;
  if (head != 0 && len != 0) {
    const size_t n = std::min<size_t>(8 - head, len);
    const uint64_t u = base + (off - head);
    std::memcpy(dst, reinterpret_cast<const uint8_t *>(&u) + head, n);
    dst += n;
    off += n;
    len -= n;
  }
  for (; len >= 8; len -= 8, off += 8, dst += 8) {
    const uint64_t u = base + off;
    std::memcpy(dst, &u, 8);
  }
  if (len != 0) [[unlikely]] {
    const uint64_t u = base + off;
    std::memcpy(dst, &u, len);
  }
}

// Returns len when every byte matches, else the index of the first mismatch.
size_t verify(const uint8_t *src, uint64_t base, uint64_t off, size_t len)
{
  size_t i = 0;
  // Byte-wise compare of n bytes at src+i; on mismatch leaves i at that byte.
  auto bytewise = [&](size_t n) -> bool {
    for (size_t k = 0; k < n; ++k) {
      const uint64_t o = off + i + k;
      const uint64_t u = base + (o & ~7ULL);
      if (src[i + k] != reinterpret_cast<const uint8_t *>(&u)[o & 7]) {
        i += k;
        return false;
      }
    }
    i += n;
    return true;
  };
  const size_t head = off & 7;
  if (head != 0 && len != 0) {
    if (!bytewise(std::min<size_t>(8 - head, len))) {
      return i;
    }
  }
  while (len - i >= 8) {
    uint64_t got;
    std::memcpy(&got, src + i, 8);
    if (got != base + off + i) [[unlikely]] {
      bytewise(8);
      return i;
    }
    i += 8;
  }
  if (i < len) [[unlikely]] {
    if (!bytewise(len - i)) {
      return i;
    }
  }
  return len;
}

const uint8_t *u8(const std::string &s) { return reinterpret_cast<const uint8_t *>(s.data()); }

// Verifies `got` against the pattern at object offset `off`; reports the first
// mismatch and the source offset the found bytes came from (found unit - base).
bool check_data(const char *what, const std::string &got, uint64_t base, uint64_t off)
{
  const size_t bad = verify(u8(got), base, off, got.size());
  if (bad == got.size()) {
    return true;
  }
  ++g_failures;
  const uint64_t abs = off + bad;
  const size_t rel = abs & 7;
  std::cerr << "FAIL: " << what << " data mismatch at object offset " << abs << " (len "
            << got.size() << ")";
  if (bad >= rel && bad - rel + 8 <= got.size()) {
    uint64_t found;
    std::memcpy(&found, got.data() + bad - rel, 8);
    std::cerr << ", found unit - base = " << (found - base) << " expected " << (abs - rel);
  }
  std::cerr << "\n";
  return false;
}

bool selfcheck_data()
{
  const uint64_t base = next_base();
  std::vector<uint8_t> whole(256);
  fill(whole.data(), base, 0, whole.size());
  bool ok = true;
  for (size_t off = 0; off < 16; ++off) {
    for (size_t len = 0; len <= 40; ++len) {
      std::vector<uint8_t> piece(len + 1);
      fill(piece.data(), base, off, len);
      ok &= std::memcmp(piece.data(), whole.data() + off, len) == 0;
      ok &= verify(whole.data() + off, base, off, len) == len;
    }
  }
  std::vector<uint8_t> big(100003);
  fill(big.data(), base, 5, big.size());
  ok &= verify(big.data(), base, 5, big.size()) == big.size();
  big[50000] ^= 1;
  ok &= verify(big.data(), base, 5, big.size()) == 50000;
  if (!ok) {
    std::cerr << "FAIL: data pattern self-check\n";
    ++g_failures;
  }
  return ok;
}

// ---- object geometry -----------------------------------------------------------

enum class SizePolicy {
  FIXED,   // every part kFixedPartSize: tiny manifest
  UNIQUE,  // part p has kUniqueBaseSize + (p - 1) bytes: every size distinct
};
constexpr uint32_t kFixedPartSize = 6003;  // not a multiple of 8
constexpr uint32_t kUniqueBaseSize = 4096;

struct Ctx {
  Svc &svc;
  tenant_id_t tenant;
  std::filesystem::path root;
};

struct Obj {
  std::string key, uid, combined;
  CksumAlg alg{CksumAlg::NONE};
  uint64_t base{};
  unsigned n{};                       // part count
  uint64_t total{};                   // object size
  std::vector<uint32_t> size, start;  // by part number (index 0 unused)
  std::vector<std::string> digests;   // raw per-part digest, by part number
  OwnedObjectValue head;
};

void build_geometry(Obj *o, unsigned n, SizePolicy pol)
{
  o->n = n;
  o->size.assign(n + 1, 0);
  o->start.assign(n + 1, 0);
  uint64_t pos = 0;
  for (unsigned p = 1; p <= n; ++p) {
    o->size[p] = pol == SizePolicy::FIXED ? kFixedPartSize : kUniqueBaseSize + (p - 1);
    o->start[p] = static_cast<uint32_t>(pos);
    pos += o->size[p];
  }
  o->total = pos;
}

// Part containing object offset `off`.
unsigned part_of(const Obj &o, uint64_t off)
{
  auto it = std::upper_bound(o.start.begin() + 1, o.start.end(), off);
  return static_cast<unsigned>(it - o.start.begin()) - 1;
}

bool wait_until(const std::function<bool()> &pred, int timeout_ms)
{
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (!pred()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return true;
}

std::filesystem::path part_path(const Ctx &c, const std::string &uid, unsigned pn)
{
  return c.root / (uid + "-" + std::to_string(pn) + "-0");
}
std::filesystem::path cksum_path(const Ctx &c, const std::string &uid)
{
  return c.root / (uid + "_checksum");
}

// Part files are sampled (first, middle, last) -- enough for 10,000-part objects.
bool part_files_exist(const Ctx &c, const Obj &o)
{
  for (unsigned pn : {1u, (o.n + 1) / 2, o.n}) {
    if (!std::filesystem::exists(part_path(c, o.uid, pn))) {
      return false;
    }
  }
  return true;
}
bool part_files_gone(const Ctx &c, const Obj &o)
{
  for (unsigned pn : {1u, (o.n + 1) / 2, o.n}) {
    if (std::filesystem::exists(part_path(c, o.uid, pn))) {
      return false;
    }
  }
  return true;
}
bool all_files_gone(const Ctx &c, const Obj &o)
{
  return part_files_gone(c, o) && !std::filesystem::exists(cksum_path(c, o.uid));
}

bucket_id_t bid_of(Ctx &c, const std::string &bucket)
{
  auto r = c.svc.resolve_bucket_id(c.tenant, bucket);
  CHECK(r && *r);
  return r && *r ? **r : bucket_id_t{};
}

// External-location summary of a multipart :O: value (plans/o_value_layout.md).
constexpr uint8_t kPlacementManifestChild = 1;  // manifest authoritative in C:M
constexpr uint8_t kPlacementChecksumChild = 2;  // checksum blob in C:S
constexpr uint8_t kPlacementChecksumFile = 4;   // checksum blob in <ref_tag>_checksum

uint8_t placement_of(const ObjectValue &v)
{
  uint8_t p = 0;
  if (v.manifest_loc == kManifestChildM) {
    p |= kPlacementManifestChild;
  }
  if (v.cksum_alg() != CksumAlg::NONE) {
    if (v.cksum_loc == kCksumChildS) {
      p |= kPlacementChecksumChild;
    } else if (v.cksum_loc == kCksumFile) {
      p |= kPlacementChecksumFile;
    }
  }
  return p;
}

bool child_exists(Ctx &c, bucket_id_t bid, const ObjectValue &v, char type)
{
  KeyBuf k;
  make_child_key(bid, v.hdr.ref_tag.view(), type, k);
  auto r = c.svc.store().get(k.view());
  return r && r->has_value();
}

bool children_gone(Ctx &c, bucket_id_t bid, const ObjectValue &v)
{
  return !child_exists(c, bid, v, kChildTypeManifest) &&
         !child_exists(c, bid, v, kChildTypeChecksum);
}

// Uploads `n` parts (min part size 1) in random order and completes. Batches of
// max(1, min(100, n / 10)) parts start at a random part number; marked slots
// are skipped (with wrap-around) until a full pass covered every part.
// `before_complete` runs while the upload is still open.
bool make_mpu(Ctx &c, const std::string &bucket, const std::string &key, unsigned n,
              CksumAlg alg, SizePolicy pol, Obj *o,
              const std::function<void(const Obj &)> &before_complete = nullptr)
{
  testutil::MinPartSizeGuard guard(c.svc, 1);
  auto init = c.svc.initiate_multipart_upload(c.tenant, bucket, key, "t", {}, {}, alg,
                                              CksumType::NONE);
  CHECK(init.error_code == KVRGW_ERR_OK);
  if (init.error_code != KVRGW_ERR_OK) {
    return false;
  }
  o->key = key;
  o->uid = init.upload_id;
  o->alg = alg;
  o->base = next_base();
  build_geometry(o, n, pol);
  o->digests.assign(n + 1, std::string());

  std::vector<Svc::CompletedPart> parts(n + 1);
  std::vector<uint8_t> uploaded(n + 1, 0);
  std::vector<uint8_t> buf(*std::max_element(o->size.begin() + 1, o->size.end()));
  Rng rng{o->base ^ 0x5851F42D4C957F2DULL};
  const unsigned batch = std::max(1u, std::min(100u, n / 10));
  unsigned count = 0;
  while (count < n) {
    unsigned p = 1 + static_cast<unsigned>(rng.below(n));
    for (unsigned k = 0; k < batch && count < n; ++k) {
      while (uploaded[p]) {
        p = p % n + 1;
      }
      fill(buf.data(), o->base, o->start[p], o->size[p]);
      auto u = c.svc.upload_part(c.tenant, bucket, key, o->uid, static_cast<uint16_t>(p),
                                 buf.data(), o->size[p]);
      CHECK(u.error_code == KVRGW_ERR_OK);
      if (u.error_code != KVRGW_ERR_OK) {
        return false;
      }
      parts[p] = {static_cast<uint16_t>(p), u.etag, alg, u.checksum};
      if (alg != CksumAlg::NONE) {
        o->digests[p] = cksum_digest(alg, buf.data(), o->size[p]);
      }
      uploaded[p] = 1;
      ++count;
      p = p % n + 1;
    }
  }
  if (before_complete) {
    before_complete(*o);
  }
  auto cm = c.svc.complete_multipart_upload(
      c.tenant, bucket, key, o->uid, std::span<const Svc::CompletedPart>(parts.data() + 1, n));
  CHECK(cm.error_code == KVRGW_ERR_OK);
  o->combined = cm.checksum;
  std::string detail;
  CHECK(c.svc.head_object(c.tenant, bucket, key, std::nullopt, 0, &o->head, &detail) ==
        KVRGW_ERR_OK);
  return cm.error_code == KVRGW_ERR_OK;
}

// ---- read verification ---------------------------------------------------------

// Pages through GetObjectAttributes: every part's number, size and checksum.
void verify_part_list(Ctx &c, const std::string &bucket, const Obj &o,
                      std::optional<version_id_t> vid)
{
  const size_t dl = cksum_digest_len(o.alg);
  unsigned marker = 0, seen = 0;
  for (;;) {
    Svc::GetObjectAttributesResult out;
    const auto ec = c.svc.get_object_attributes(c.tenant, bucket, o.key, vid, 1000,
                                                static_cast<uint16_t>(marker), &out);
    CHECK(ec == KVRGW_ERR_OK);
    if (ec != KVRGW_ERR_OK) {
      return;
    }
    if (seen == 0) {
      CHECK(out.total_parts_count == o.n);
    }
    CHECK(out.part_cksums.size() == out.part_numbers.size() * dl);
    for (size_t i = 0; i < out.part_numbers.size(); ++i) {
      const unsigned p = seen + 1;
      if (p > o.n) {
        CHECK(p <= o.n);
        return;
      }
      CHECK(out.part_numbers[i] == p);
      CHECK(out.part_sizes[i] == o.size[p]);
      CHECK(dl == 0 || out.part_cksums.compare(i * dl, dl, o.digests[p]) == 0);
      ++seen;
    }
    if (!out.parts_truncated) {
      break;
    }
    marker = out.next_part_number_marker;
  }
  CHECK(seen == o.n);
}

// max(1, ceil(log2(n))) ranged GETs: random offset, mapped to its part p, length
// uniform in [2*size[p], 3*size[p]] (end clamped to the object). An unclamped
// range must cross a part boundary.
void verify_random_ranges(Ctx &c, const std::string &bucket, const Obj &o,
                          std::optional<version_id_t> vid)
{
  unsigned k = 0;
  while ((1u << k) < o.n) {
    ++k;
  }
  k = std::max(k, 1u);
  Rng rng{o.base + (++g_range_calls) * 0x2545F4914F6CDD1DULL};
  for (unsigned i = 0; i < k; ++i) {
    const uint64_t off = rng.below(o.total);
    const unsigned p = part_of(o, off);
    uint64_t len = 2ULL * o.size[p] + rng.below(o.size[p] + 1ULL);
    bool clamped = false;
    if (off + len > o.total) {
      len = o.total - off;
      clamped = true;
    }
    if (!clamped) {
      CHECK(part_of(o, off + len - 1) > p);
    }
    ByteRange r{static_cast<int64_t>(off), static_cast<int64_t>(off + len - 1)};
    Svc::GetObjectResult rg;
    CHECK(c.svc.get_object(c.tenant, bucket, o.key, vid, &r, 0, &rg) == KVRGW_ERR_OK);
    CHECK(rg.body.size() == len);
    check_data("random range", rg.body, o.base, off);
  }
}

// Full sequential GET, PartNumber GET of every part, the full part list, lg(N)
// random ranges, and the combined checksum -- for the current version or `vid`.
void verify_reads(Ctx &c, const std::string &bucket, const Obj &o,
                  std::optional<version_id_t> vid = std::nullopt)
{
  Svc::GetObjectResult g;
  CHECK(c.svc.get_object(c.tenant, bucket, o.key, vid, nullptr, 0, &g) == KVRGW_ERR_OK);
  CHECK(g.body.size() == o.total);
  check_data("full GET", g.body, o.base, 0);

  verify_part_list(c, bucket, o, vid);
  verify_random_ranges(c, bucket, o, vid);

  for (unsigned p = 1; p <= o.n; ++p) {
    Svc::GetObjectResult pg;
    CHECK(c.svc.get_object(c.tenant, bucket, o.key, vid, nullptr, static_cast<uint16_t>(p),
                           &pg) == KVRGW_ERR_OK);
    CHECK(pg.body.size() == o.size[p]);
    check_data("PartNumber GET", pg.body, o.base, o.start[p]);
    CHECK(pg.part_cksum == o.digests[p]);  // both empty without a checksum
  }

  OwnedObjectValue hv;
  std::string detail;
  CHECK(c.svc.head_object(c.tenant, bucket, o.key, vid, 0, &hv, &detail) == KVRGW_ERR_OK);
  if (o.alg == CksumAlg::NONE) {
    CHECK(hv.cksum_alg() == CksumAlg::NONE && o.combined.empty());
    return;
  }
  char buf[64];
  const size_t bn = cksum_render(hv.cksum_type(), hv.cksum_digest().data(),
                                 hv.cksum_combined_len(), hv.part_count(), buf);
  CHECK(std::string(buf, bn) == o.combined);
}

constexpr int kGcWaitMs = 30000;

// Waits for GC to free every file of `o` and clear its children.
void expect_freed(Ctx &c, bucket_id_t bid, const Obj &o, int timeout_ms = kGcWaitMs)
{
  const bool ok = wait_until(
      [&] { return all_files_gone(c, o) && children_gone(c, bid, o.head); }, timeout_ms);
  CHECK(ok);
  if (!ok) {
    std::cerr << "  not freed: part_files_gone=" << part_files_gone(c, o)
              << " cksum_file_gone=" << !std::filesystem::exists(cksum_path(c, o.uid))
              << " children_gone=" << children_gone(c, bid, o.head) << "\n";
  }
}

void expect_alive(Ctx &c, bucket_id_t bid, const Obj &o)
{
  CHECK(part_files_exist(c, o));
  CHECK(child_exists(c, bid, o.head, kChildTypeManifest) ==
        ((placement_of(o.head) & kPlacementManifestChild) != 0));
  CHECK(child_exists(c, bid, o.head, kChildTypeChecksum) ==
        ((placement_of(o.head) & kPlacementChecksumChild) != 0));
  CHECK(std::filesystem::exists(cksum_path(c, o.uid)) ==
        ((placement_of(o.head) & kPlacementChecksumFile) != 0));
}

Svc::PutObjectResult put_small(Ctx &c, const std::string &bucket, const std::string &key)
{
  const std::string d = "small-object";
  auto r = c.svc.put_object(c.tenant, bucket, key, reinterpret_cast<const uint8_t *>(d.data()),
                            d.size(), "t", d.size(), nullptr, {}, {});
  CHECK(r.error_code == KVRGW_ERR_OK);
  return r;
}

// ---- Scenario 1: placement matrix --------------------------------------------
// Expected placement is derived from place_multipart(): the :O: value (header +
// content type + chunk descriptor + digest + embedded manifest) must stay within
// 1,024 B to embed; a blob up to kMaxChildPayloadBytes (75 KB) goes to a 'S'
// child; larger goes to the tier 3 file. Fixed sizes keep the manifest tiny
// (embedded); 500 unique sizes push it into an 'M' child.
void test_placement(Ctx &c, const std::string &bucket)
{
  std::cout << "\n[CHILD-1] placement matrix\n";
  const uint32_t max_kv = c.svc.tier_config_state().active_copy().max_kv_store;
  if (max_kv != kDefaultMaxKvStore) {
    std::cerr << "FAIL: max_kv_store is " << max_kv << ", this test expects "
              << kDefaultMaxKvStore << "\n";
    ++g_failures;
  }
  struct Case {
    unsigned parts;
    CksumAlg alg;
    SizePolicy pol;
    uint8_t want;
  };
  const Case cases[] = {
      {8, CksumAlg::SHA256, SizePolicy::FIXED, 0},                                  // 256 B inline
      {128, CksumAlg::SHA256, SizePolicy::FIXED, kPlacementChecksumChild},          // 4096 B
      {128, CksumAlg::CRC64NVME, SizePolicy::FIXED, kPlacementChecksumChild},       // 1024 B
      {256, CksumAlg::CRC64NVME, SizePolicy::FIXED, kPlacementChecksumChild},       // 2048 B
      {256, CksumAlg::SHA256, SizePolicy::FIXED, kPlacementChecksumChild},          // 8192 B
      {500, CksumAlg::NONE, SizePolicy::UNIQUE, kPlacementManifestChild},
      {500, CksumAlg::CRC64NVME, SizePolicy::UNIQUE,
       kPlacementManifestChild | kPlacementChecksumChild},                          // 4000 B
      {500, CksumAlg::SHA256, SizePolicy::UNIQUE,
       kPlacementManifestChild | kPlacementChecksumChild},                          // 16000 B
  };
  const bucket_id_t bid = bid_of(c, bucket);
  int i = 0;
  for (const Case &cs : cases) {
    Obj o;
    CHECK(make_mpu(c, bucket, "place-" + std::to_string(i++), cs.parts, cs.alg, cs.pol, &o));
    std::cout << "  parts=" << cs.parts << " alg=" << cksum_alg_name(cs.alg)
              << " placement=" << int(placement_of(o.head)) << " want=" << int(cs.want) << "\n";
    CHECK(placement_of(o.head) == cs.want);
    expect_alive(c, bid, o);
    verify_reads(c, bucket, o);
    Svc::DeleteResult del;
    CHECK(c.svc.delete_object(c.tenant, bucket, o.key, nullptr, &del) == KVRGW_ERR_OK);
    expect_freed(c, bid, o);
  }
}

// ---- Scenario 2: delete paths ------------------------------------------------
void test_delete_paths(Ctx &c)
{
  std::cout << "\n[CHILD-2] delete paths\n";
  Svc::DeleteResult del;
  const SizePolicy U = SizePolicy::UNIQUE;

  // 2a. Unversioned overwrite; a sibling object's children must survive.
  {
    const std::string b = "cd-unv-" + std::to_string(getpid());
    CHECK(c.svc.create_bucket(c.tenant, b) == KVRGW_ERR_OK);
    const bucket_id_t bid = bid_of(c, b);
    Obj a, sib;
    CHECK(make_mpu(c, b, "k", 500, CksumAlg::CRC64NVME, U, &a));
    CHECK(make_mpu(c, b, "sibling", 500, CksumAlg::CRC64NVME, U, &sib));
    CHECK(a.base != sib.base);
    CHECK(placement_of(a.head) == (kPlacementManifestChild | kPlacementChecksumChild));
    verify_reads(c, b, a);
    put_small(c, b, "k");
    expect_freed(c, bid, a);
    expect_alive(c, bid, sib);
    verify_reads(c, b, sib);
    // 2e. DeleteObject without a version id (unversioned): removes it.
    CHECK(c.svc.delete_object(c.tenant, b, "sibling", nullptr, &del) == KVRGW_ERR_OK);
    expect_freed(c, bid, sib);
  }

  // Versioned bucket.
  const std::string b = "cd-ver-" + std::to_string(getpid());
  CHECK(c.svc.create_bucket(c.tenant, b) == KVRGW_ERR_OK);
  CHECK(c.svc.put_bucket_versioning(c.tenant, b, VERSIONING_ENABLED) == KVRGW_ERR_OK);
  const bucket_id_t bid = bid_of(c, b);

  // 2b. Overwrite: the old multipart value moves to :V: and keeps its children;
  //     deleting that non-current version (2d) then frees everything.
  {
    Obj a;
    CHECK(make_mpu(c, b, "ow", 500, CksumAlg::CRC64NVME, U, &a));
    verify_reads(c, b, a);
    put_small(c, b, "ow");
    expect_alive(c, bid, a);
    verify_reads(c, b, a, a.head.hdr.version_id);
    CHECK(c.svc.delete_object_version(c.tenant, b, "ow", a.head.hdr.version_id, nullptr) ==
          KVRGW_ERR_OK);
    expect_freed(c, bid, a);
  }

  // 2c. Delete the current multipart version while a non-current one exists:
  //     the older version is promoted and stays readable.
  {
    auto p = put_small(c, b, "pr");
    Obj a;
    CHECK(make_mpu(c, b, "pr", 500, CksumAlg::SHA256, U, &a));
    CHECK(placement_of(a.head) == (kPlacementManifestChild | kPlacementChecksumChild));
    verify_reads(c, b, a);
    CHECK(c.svc.delete_object_version(c.tenant, b, "pr", a.head.hdr.version_id, nullptr) ==
          KVRGW_ERR_OK);
    expect_freed(c, bid, a);
    Svc::GetObjectResult g;
    CHECK(c.svc.get_object(c.tenant, b, "pr", std::nullopt, nullptr, 0, &g) == KVRGW_ERR_OK);
    CHECK(g.body == "small-object");
    CHECK(g.value.hdr.version_id == p.version_id);
  }

  // 2e'. DeleteObject without a version id on a versioned bucket only adds a
  //      delete marker: the version and its children survive.
  {
    Obj a;
    CHECK(make_mpu(c, b, "dm", 500, CksumAlg::CRC64NVME, U, &a));
    CHECK(c.svc.delete_object(c.tenant, b, "dm", nullptr, &del) == KVRGW_ERR_OK);
    CHECK(del.created_dm);
    expect_alive(c, bid, a);
    verify_reads(c, b, a, a.head.hdr.version_id);
    CHECK(c.svc.delete_object_version(c.tenant, b, "dm", a.head.hdr.version_id, nullptr) ==
          KVRGW_ERR_OK);
    expect_freed(c, bid, a);
  }
}

// ---- Scenario 3: CopyObject ---------------------------------------------------
void test_copy(Ctx &c)
{
  std::cout << "\n[CHILD-3] copy sharing\n";
  const std::string b = "cd-copy-" + std::to_string(getpid());
  CHECK(c.svc.create_bucket(c.tenant, b) == KVRGW_ERR_OK);
  const bucket_id_t bid = bid_of(c, b);
  Svc::DeleteResult del;

  for (CksumAlg alg : {CksumAlg::CRC64NVME, CksumAlg::SHA256}) {
    for (bool original_first : {true, false}) {
      Obj src;
      const std::string tag = std::string(cksum_alg_name(alg)) + (original_first ? "-o" : "-c");
      CHECK(make_mpu(c, b, "src-" + tag, 500, alg, SizePolicy::UNIQUE, &src));

      Svc::CopyObjectRequest req;
      req.tenant_id = c.tenant;
      req.src_bucket_name = b;
      req.src_key = src.key;
      req.dst_bucket_name = b;
      req.dst_key = "dst-" + tag;
      Svc::CopyObjectResult cres;
      CHECK(c.svc.copy_object(req, &cres) == KVRGW_ERR_OK);

      // The copy is byte-identical: same base and geometry (head is re-read below).
      Obj dst;
      dst.uid = src.uid;
      dst.combined = src.combined;
      dst.alg = src.alg;
      dst.base = src.base;
      dst.n = src.n;
      dst.total = src.total;
      dst.size = src.size;
      dst.start = src.start;
      dst.digests = src.digests;
      dst.key = req.dst_key;
      std::string detail;
      CHECK(c.svc.head_object(c.tenant, b, dst.key, std::nullopt, 0, &dst.head, &detail) ==
            KVRGW_ERR_OK);
      CHECK(dst.head.hdr.chunk_type == CHUNK_MULTIPART_REF);
      // Lazy promotion: the source becomes shared too; C:M / C:S stay keyed by the
      // original owner's ref_tag, which the copy's chunk descriptor points to.
      OwnedObjectValue src_now;
      CHECK(c.svc.head_object(c.tenant, b, src.key, std::nullopt, 0, &src_now, &detail) ==
            KVRGW_ERR_OK);
      CHECK(src_now.has_shared_data() && dst.head.has_shared_data());
      CHECK(src_now.manifest_loc == kManifestChildM && dst.head.manifest_loc == kManifestChildM);
      CHECK(placement_of(dst.head) == placement_of(src_now));
      CHECK(!(dst.head.hdr.ref_tag == src.head.hdr.ref_tag));
      CHECK(dst.head.chunk.get_ref_tag() == src.head.hdr.ref_tag);
      CHECK(child_exists(c, bid, src.head, kChildTypeManifest));
      CHECK(child_exists(c, bid, src.head, kChildTypeChecksum) ==
            ((placement_of(dst.head) & kPlacementChecksumChild) != 0));
      CHECK(!child_exists(c, bid, dst.head, kChildTypeManifest));
      verify_reads(c, b, dst);
      verify_reads(c, b, src);

      const Obj &first = original_first ? src : dst;
      const Obj &last = original_first ? dst : src;
      CHECK(c.svc.delete_object(c.tenant, b, first.key, nullptr, &del) == KVRGW_ERR_OK);
      // Shared C:M / C:S, part files and tier 3 file stay until the last reference drops.
      std::this_thread::sleep_for(std::chrono::milliseconds(3000));
      CHECK(child_exists(c, bid, src.head, kChildTypeManifest));
      CHECK(part_files_exist(c, src));
      CHECK(std::filesystem::exists(cksum_path(c, src.uid)) ==
            ((placement_of(src.head) & kPlacementChecksumFile) != 0));
      verify_reads(c, b, last);

      CHECK(c.svc.delete_object(c.tenant, b, last.key, nullptr, &del) == KVRGW_ERR_OK);
      expect_freed(c, bid, src);
      CHECK(children_gone(c, bid, dst.head));
      std::cout << "  " << tag << " ok\n";
    }
  }
}

// ---- Scenario 4: the maximum, 10,000 parts -------------------------------------
void test_max_parts(Ctx &c)
{
  std::cout << "\n[CHILD-4] 10,000 parts, every size distinct\n";
  const std::string b = "cd-max-" + std::to_string(getpid());
  CHECK(c.svc.create_bucket(c.tenant, b) == KVRGW_ERR_OK);
  const bucket_id_t bid = bid_of(c, b);
  Obj o;
  // While the upload is open and full: part numbers outside [1, 10000] are rejected.
  auto reject_out_of_range = [&](const Obj &up) {
    std::vector<uint8_t> payload(kUniqueBaseSize + kMaxPartNumber);
    fill(payload.data(), up.base, 0, payload.size());
    for (unsigned pn : {static_cast<unsigned>(kMaxPartNumber) + 1, 0u, 65535u}) {
      auto r = c.svc.upload_part(c.tenant, b, up.key, up.uid, static_cast<uint16_t>(pn),
                                 payload.data(), payload.size());
      CHECK(r.error_code == KVRGW_ERR_INVALID_ARGUMENT);
    }
  };
  CHECK(make_mpu(c, b, "max", kMaxPartNumber, CksumAlg::SHA256, SizePolicy::UNIQUE, &o,
                 reject_out_of_range));
  CHECK(o.head.part_count() == kMaxPartNumber);
  CHECK(placement_of(o.head) == (kPlacementManifestChild | kPlacementChecksumFile));
  expect_alive(c, bid, o);
  {
    KeyBuf k;
    make_child_key(bid, o.head.hdr.ref_tag.view(), kChildTypeManifest, k);
    auto r = c.svc.store().get(k.view());
    CHECK(r && r->has_value());
    std::cout << "  manifest child bytes: " << (r && r->has_value() ? (*r)->size() : 0)
              << ", object bytes: " << o.total << "\n";
  }
  verify_reads(c, b, o);

  Svc::DeleteResult del;
  CHECK(c.svc.delete_object(c.tenant, b, "max", nullptr, &del) == KVRGW_ERR_OK);
  expect_freed(c, bid, o, 240000);
}

int run(KvRgwRuntime &runtime, const std::filesystem::path &root)
{
  Ctx c{runtime.service(), tenant_id_t{}, root};
  bool exists = false;
  const std::string tenant_name = "multipart-children-test";
  auto ec = c.svc.resolve_tenant(tenant_name, &exists, &c.tenant);
  if (ec != KVRGW_ERR_OK || !exists) {
    CHECK(c.svc.add_tenant(tenant_name, &c.tenant) == KVRGW_ERR_OK);
  }
  const std::string bucket = "cd-place-" + std::to_string(getpid());
  CHECK(c.svc.create_bucket(c.tenant, bucket) == KVRGW_ERR_OK);

  test_placement(c, bucket);
  test_delete_paths(c);
  test_copy(c);
  test_max_parts(c);

  std::cout << (g_failures == 0 ? "\nALL MULTIPART CHILDREN TESTS PASSED\n"
                                : "\nCHECK(S) FAILED\n");
  return g_failures == 0 ? 0 : 1;
}

}  // namespace
}  // namespace kvrgw

int main()
{
  ::setenv("KVRGW_GC_INTERVAL_SEC", "1", 1);
  kvrgw::init_seed();
  if (!kvrgw::selfcheck_data()) {
    return 1;
  }
  kvrgw::KvRgwStartOptions opts;
  opts.perf_mode = false;
  char data_root_buf[64];
  std::snprintf(data_root_buf, sizeof(data_root_buf), "/tmp/kvrgw-children-data-%d",
                static_cast<int>(getpid()));
  opts.data_root = data_root_buf;

  kvrgw::KvRgwRuntime runtime;
  if (!runtime.start(opts)) {
    std::cerr << "FAIL: runtime start failed\n";
    return 1;
  }
  const std::filesystem::path root(data_root_buf);
  const int rc = kvrgw::run(runtime, root);
  runtime.stop();
  std::error_code fs_ec;
  std::filesystem::remove_all(root, fs_ec);
  return rc;
}
