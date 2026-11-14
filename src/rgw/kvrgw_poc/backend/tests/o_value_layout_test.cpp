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

// :O: value layout permutations (plans/o_value_layout.md, "New tests --
// location permutations"). Every reachable placement state is forced by sizing
// the inputs and runs the same operations: exact :O: fields and the exact set
// of C: KVs / files, HEAD / GET / ranged GET / PartNumber GET, ListObjects,
// GetObjectAttributes, CopyObject (lazy promotion, copy of a shared source),
// and deleting every owner in two orders (no orphan C:, file or R: key).
//
// Combinations that the placement rules cannot produce from real uploads
// (e.g. a shared object with Cached bits clear/set, an inline checksum next
// to a C:M manifest) are covered by the place_multipart unit test in
// multipart_key_test.cpp; parser rejects are in object_value_test.cpp.
//
// GC-sensitive: run against a live FoundationDB cluster with no other
// frontend GC workers running.

#include "checksum.hpp"
#include "constants.hpp"
#include "error_codes.hpp"
#include "keys.hpp"
#include "kv_store.hpp"
#include "kvrgw_runtime.hpp"
#include "min_part_size_guard.hpp"
#include "multipart_value.hpp"
#include "object_value.hpp"
#include "ref_count.hpp"
#include "service_impl.hpp"
#include "tier_config_state.hpp"

#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
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

#define CHECKW(cond, what) \
  do { \
    if (!(cond)) { \
      std::cerr << "FAIL: " << #cond << " [" << (what) << "] at " << __FILE__ << ":" \
                << __LINE__ << "\n"; \
      ++g_failures; \
    } \
  } while (0)

using Svc = KvRgwServiceImpl;

// ---- position-tagged data ------------------------------------------------------

uint8_t byte_at(uint64_t base, uint64_t x)
{
  return static_cast<uint8_t>(((x + 1) * 0x9E3779B97F4A7C15ULL + base) >> 56);
}

void fill(uint8_t *dst, uint64_t base, uint64_t off, size_t len)
{
  for (size_t i = 0; i < len; ++i) {
    dst[i] = byte_at(base, off + i);
  }
}

bool check_data(const char *what, std::string_view got, uint64_t base, uint64_t off)
{
  for (size_t i = 0; i < got.size(); ++i) {
    if (static_cast<uint8_t>(got[i]) != byte_at(base, off + i)) {
      std::cerr << "FAIL: " << what << " data mismatch at object offset " << off + i
                << " (len " << got.size() << ")\n";
      ++g_failures;
      return false;
    }
  }
  return true;
}

uint64_t g_base_state = 0x1234567;
uint64_t next_base()
{
  g_base_state = g_base_state * 6364136223846793005ULL + 1442695040888963407ULL;
  return g_base_state;
}

// ---- context and generic helpers -----------------------------------------------

struct Ctx {
  Svc &svc;
  tenant_id_t tenant;
  std::filesystem::path root;
};

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

constexpr int kGcWaitMs = 40000;

bucket_id_t bid_of(Ctx &c, const std::string &bucket)
{
  auto r = c.svc.resolve_bucket_id(c.tenant, bucket);
  CHECK(r && *r);
  return r && *r ? **r : bucket_id_t{};
}

std::string new_bucket(Ctx &c, const char *tag)
{
  const std::string b = std::string(tag) + "-" + std::to_string(getpid());
  CHECK(c.svc.create_bucket(c.tenant, b) == KVRGW_ERR_OK);
  return b;
}

bool kv_exists(Ctx &c, const KeyBuf &k)
{
  auto r = c.svc.store().get(k.view());
  return r && r->has_value();
}

bool child_exists(Ctx &c, bucket_id_t bid, std::string_view ref_tag, char type)
{
  KeyBuf k;
  make_child_key(bid, ref_tag, type, k);
  return kv_exists(c, k);
}

std::optional<uint64_t> r_count(Ctx &c, const RefTag &tag)
{
  const auto k = make_r_key(tag.view());
  auto r = c.svc.store().get(k.view());
  if (!r || !r->has_value()) {
    return std::nullopt;
  }
  const auto p = parse_r_value(**r);
  return p ? std::optional<uint64_t>(p->ref_count) : std::nullopt;
}

bool file_exists(Ctx &c, const std::string &name) { return std::filesystem::exists(c.root / name); }

// Number of C: KVs under (bucket, ref_tag).
size_t count_children(Ctx &c, bucket_id_t bid, std::string_view ref_tag)
{
  KeyBuf prefix;
  make_c_prefix(bid, ref_tag, prefix);
  auto r = c.svc.store().range_scan(prefix.view(), prefix_range_end(prefix.view()), 100);
  return r ? r->size() : static_cast<size_t>(-1);
}

// ---- multipart objects ---------------------------------------------------------

constexpr uint32_t kFixedPartSize = 5003;
constexpr uint32_t kUniqueBaseSize = 4096;

struct Shape {
  unsigned n{};
  CksumAlg alg{CksumAlg::NONE};
  bool unique{false};      // distinct part sizes: a large manifest
  size_t meta_bytes{0};    // one user-metadata value of this length
  std::string ct{"t"};     // content type
};

CksumType type_for(CksumAlg alg)
{
  return alg == CksumAlg::NONE ? CksumType::NONE
         : alg == CksumAlg::CRC64NVME ? CksumType::FULL_OBJECT
                                      : CksumType::COMPOSITE;
}

struct Mpu {
  std::string key, uid, combined;
  CksumAlg alg{CksumAlg::NONE};
  uint64_t base{};
  unsigned n{};
  uint64_t total{};
  std::vector<uint32_t> size;       // by part number (index 0 unused)
  std::vector<uint64_t> start;
  std::vector<std::string> digests;
  OwnedObjectValue head;

  void geometry(unsigned parts, bool unique)
  {
    n = parts;
    size.assign(n + 1, 0);
    start.assign(n + 1, 0);
    uint64_t pos = 0;
    for (unsigned p = 1; p <= n; ++p) {
      size[p] = unique ? kUniqueBaseSize + (p - 1) : kFixedPartSize;
      start[p] = pos;
      pos += size[p];
    }
    total = pos;
  }
};

bool refresh(Ctx &c, const std::string &bucket, Mpu &o)
{
  std::string detail;
  const auto ec = c.svc.head_object(c.tenant, bucket, o.key, std::nullopt, 0, &o.head, &detail);
  CHECK(ec == KVRGW_ERR_OK);
  return ec == KVRGW_ERR_OK;
}

bool make_mpu(Ctx &c, const std::string &bucket, const std::string &key, const Shape &sh, Mpu *o)
{
  testutil::MinPartSizeGuard guard(c.svc, 1);
  const std::string mval(sh.meta_bytes, 'm');
  const MetaPair mp{"mk", mval};
  const std::span<const MetaPair> meta =
      sh.meta_bytes != 0 ? std::span<const MetaPair>(&mp, 1) : std::span<const MetaPair>();
  auto init = c.svc.initiate_multipart_upload(c.tenant, bucket, key, sh.ct, {}, meta, sh.alg,
                                              type_for(sh.alg));
  CHECK(init.error_code == KVRGW_ERR_OK);
  if (init.error_code != KVRGW_ERR_OK) {
    return false;
  }
  o->key = key;
  o->uid = init.upload_id;
  o->alg = sh.alg;
  o->base = next_base();
  o->geometry(sh.n, sh.unique);
  o->digests.assign(sh.n + 1, std::string());

  std::vector<Svc::CompletedPart> parts(sh.n);
  std::vector<uint8_t> buf(kUniqueBaseSize + sh.n + kFixedPartSize);
  for (unsigned p = 1; p <= sh.n; ++p) {
    fill(buf.data(), o->base, o->start[p], o->size[p]);
    auto u = c.svc.upload_part(c.tenant, bucket, key, o->uid, static_cast<uint16_t>(p),
                               buf.data(), o->size[p]);
    CHECK(u.error_code == KVRGW_ERR_OK);
    if (u.error_code != KVRGW_ERR_OK) {
      return false;
    }
    parts[p - 1] = {static_cast<uint16_t>(p), u.etag, sh.alg, u.checksum};
    if (sh.alg != CksumAlg::NONE) {
      o->digests[p] = cksum_digest(sh.alg, buf.data(), o->size[p]);
    }
  }
  auto cm = c.svc.complete_multipart_upload(c.tenant, bucket, key, o->uid, parts);
  CHECK(cm.error_code == KVRGW_ERR_OK);
  if (cm.error_code != KVRGW_ERR_OK) {
    return false;
  }
  o->combined = cm.checksum;
  return refresh(c, bucket, *o);
}

// A byte-identical object under another key (head is read, geometry copied).
bool derive(Ctx &c, const std::string &bucket, const Mpu &src, const std::string &key, Mpu *d)
{
  d->key = key;
  d->uid = src.uid;
  d->combined = src.combined;
  d->alg = src.alg;
  d->base = src.base;
  d->n = src.n;
  d->total = src.total;
  d->size = src.size;
  d->start = src.start;
  d->digests = src.digests;
  return refresh(c, bucket, *d);
}

bool copy_to(Ctx &c, const std::string &bucket, const std::string &src, const std::string &dst)
{
  Svc::CopyObjectRequest req;
  req.tenant_id = c.tenant;
  req.src_bucket_name = bucket;
  req.src_key = src;
  req.dst_bucket_name = bucket;
  req.dst_key = dst;
  Svc::CopyObjectResult res;
  const auto ec = c.svc.copy_object(req, &res);
  CHECK(ec == KVRGW_ERR_OK);
  return ec == KVRGW_ERR_OK;
}

// ---- layout checks -------------------------------------------------------------

struct Exp {
  uint8_t manifest{kManifestInline};
  uint8_t cksum{kCksumInline};
  bool ext_meta{false};
};

// The C: KVs and the checksum file that must exist for `v` -- and no others.
// Shared (REF) objects find C:M / C:S / file under the data owner's ref_tag.
void check_children(Ctx &c, bucket_id_t bid, const ObjectValue &v, const std::string &what)
{
  if (!v.is_multipart()) {
    return;
  }
  const std::string_view dref = v.data_ref_view();
  const bucket_id_t dbk = v.data_bucket(bid);
  CHECKW(child_exists(c, dbk, dref, kChildTypeManifest) == (v.manifest_loc == kManifestChildM),
         what + " C:M");
  CHECKW(child_exists(c, dbk, dref, kChildTypeChecksum) == (v.cksum_loc == kCksumChildS),
         what + " C:S");
  CHECKW(file_exists(c, checksum_filename(v.data_ref_tag())) == (v.cksum_loc == kCksumFile),
         what + " checksum file");
  CHECKW(child_exists(c, bid, v.hdr.ref_tag.view(), kChildTypeExtended) == v.has_external_metadata(),
         what + " C:E");
  if (!v.is_ref()) {
    // Unshared and shared owners: exactly M / S / E, nothing else.
    const size_t want = (v.manifest_loc == kManifestChildM) + (v.cksum_loc == kCksumChildS) +
                        v.has_external_metadata();
    CHECKW(count_children(c, bid, v.hdr.ref_tag.view()) == want, what + " C: count");
  }
}

void check_fields(Ctx &c, bucket_id_t bid, const Mpu &o, const Exp &e, const std::string &what)
{
  const OwnedObjectValue &h = o.head;
  CHECKW(h.hdr.chunk_type == CHUNK_MULTIPART && !h.has_shared_data(), what + " type");
  CHECKW(h.manifest_loc == e.manifest, what + " manifest_loc");
  CHECKW(h.cksum_loc == e.cksum, what + " cksum_loc");
  CHECKW(h.has_external_metadata() == e.ext_meta, what + " external metadata");
  CHECKW(h.raw.size() <= kMaxOValueBytes, what + " :O: <= 1024 B");
  CHECKW(h.part_count() == o.n, what + " part_count");
  CHECKW(h.cksum_alg() == o.alg, what + " cksum_alg");
  CHECKW(((h.mflags & kMFlagFirstPartInfo) != 0) == (e.manifest != kManifestInline),
         what + " first_part_info");
  CHECKW((h.mflags & (kMFlagManifestCached | kMFlagCksumCached)) == 0, what + " no cached bits");
  CHECKW(h.cksum_blob.empty() == (o.alg == CksumAlg::NONE || e.cksum != kCksumInline),
         what + " cksum blob");
  check_children(c, bid, h, what);
}

// ---- read verification ---------------------------------------------------------

void verify_part_list(Ctx &c, const std::string &bucket, const Mpu &o)
{
  const size_t dl = cksum_digest_len(o.alg);
  unsigned marker = 0, seen = 0;
  for (;;) {
    Svc::GetObjectAttributesResult out;
    const auto ec = c.svc.get_object_attributes(c.tenant, bucket, o.key, std::nullopt, 1000,
                                                static_cast<uint16_t>(marker), &out);
    CHECK(ec == KVRGW_ERR_OK);
    if (ec != KVRGW_ERR_OK) {
      return;
    }
    CHECK(out.total_parts_count == o.n);
    CHECK(out.part_cksums.size() == out.part_numbers.size() * dl);
    for (size_t i = 0; i < out.part_numbers.size(); ++i) {
      const unsigned p = ++seen;
      if (p > o.n) {
        CHECK(p <= o.n);
        return;
      }
      CHECK(out.part_numbers[i] == p);
      CHECK(out.part_sizes[i] == o.size[p]);
      CHECK(dl == 0 || out.part_cksums.compare(i * dl, dl, o.digests[p]) == 0);
    }
    if (!out.parts_truncated) {
      break;
    }
    marker = out.next_part_number_marker;
  }
  CHECK(seen == o.n);
}

void verify_reads(Ctx &c, const std::string &bucket, const Mpu &o)
{
  Svc::GetObjectResult g;
  CHECK(c.svc.get_object(c.tenant, bucket, o.key, std::nullopt, nullptr, 0, &g) == KVRGW_ERR_OK);
  CHECK(g.body.size() == o.total);
  check_data("full GET", g.body, o.base, 0);

  // Ranges: head, across the first part boundary, tail.
  const uint64_t b1 = o.start[o.n > 1 ? 2 : 1] + (o.n > 1 ? 0 : o.size[1]);
  const std::array<std::pair<uint64_t, uint64_t>, 3> ranges = {{
      {0, std::min<uint64_t>(9, o.total - 1)},
      {b1 > 5 ? b1 - 5 : 0, std::min<uint64_t>(b1 + 5, o.total - 1)},
      {o.total - std::min<uint64_t>(10, o.total), o.total - 1}}};
  for (const auto &[from, to] : ranges) {
    ByteRange r{static_cast<int64_t>(from), static_cast<int64_t>(to)};
    Svc::GetObjectResult rg;
    CHECK(c.svc.get_object(c.tenant, bucket, o.key, std::nullopt, &r, 0, &rg) == KVRGW_ERR_OK);
    CHECK(rg.body.size() == to - from + 1);
    check_data("ranged GET", rg.body, o.base, from);
  }

  for (unsigned p : {1u, (o.n + 1) / 2, o.n}) {
    Svc::GetObjectResult pg;
    CHECK(c.svc.get_object(c.tenant, bucket, o.key, std::nullopt, nullptr,
                           static_cast<uint16_t>(p), &pg) == KVRGW_ERR_OK);
    CHECK(pg.body.size() == o.size[p]);
    check_data("PartNumber GET", pg.body, o.base, o.start[p]);
    CHECK(pg.part_cksum == o.digests[p]);
  }

  verify_part_list(c, bucket, o);

  OwnedObjectValue hv;
  std::string detail;
  CHECK(c.svc.head_object(c.tenant, bucket, o.key, std::nullopt, 0, &hv, &detail) == KVRGW_ERR_OK);
  CHECK(hv.hdr.size == o.total);
  if (o.alg != CksumAlg::NONE) {
    char buf[128];
    const size_t bn = cksum_render(hv.cksum_type(), hv.cksum_digest().data(),
                                   hv.cksum_combined_len(), hv.part_count(), buf);
    CHECK(std::string(buf, bn) == o.combined);
  }

  // ListObjects serves size and etag from :O: alone.
  Svc::ListObjectsResult lo;
  CHECK(c.svc.list_objects(c.tenant, bucket, o.key, "", 1000, "", "", &lo) == KVRGW_ERR_OK);
  bool found = false;
  for (const auto &e : lo.objects) {
    if (e.key == o.key) {
      found = true;
      CHECK(e.size == o.total);
      CHECK(e.etag == hv.etag_display());
    }
  }
  CHECK(found);
}

// ---- freed / alive -------------------------------------------------------------

bool part_files_exist(Ctx &c, const Mpu &o)
{
  for (unsigned pn : {1u, (o.n + 1) / 2, o.n}) {
    if (!file_exists(c, part_filename(o.head.hdr.ref_tag, static_cast<uint16_t>(pn), 0))) {
      return false;
    }
  }
  return true;
}

// `root` must be the owner whose ref_tag names the part files and C: KVs.
void expect_freed(Ctx &c, bucket_id_t bid, const Mpu &root, const std::string &what)
{
  const RefTag &tag = root.head.hdr.ref_tag;
  const bool ok = wait_until(
      [&] {
        return !file_exists(c, part_filename(tag, 1, 0)) &&
               !file_exists(c, part_filename(tag, static_cast<uint16_t>(root.n), 0)) &&
               !file_exists(c, checksum_filename(tag)) &&
               count_children(c, bid, tag.view()) == 0 && !r_count(c, tag).has_value();
      },
      kGcWaitMs);
  CHECKW(ok, what + " freed (files, C:, R:)");
  if (!ok) {
    std::cerr << "  files=" << file_exists(c, part_filename(tag, 1, 0))
              << " cksum_file=" << file_exists(c, checksum_filename(tag))
              << " children=" << count_children(c, bid, tag.view())
              << " R:=" << r_count(c, tag).has_value() << "\n";
  }
}

bool del(Ctx &c, const std::string &bucket, const std::string &key)
{
  Svc::DeleteResult dr;
  const auto ec = c.svc.delete_object(c.tenant, bucket, key, nullptr, &dr);
  CHECK(ec == KVRGW_ERR_OK);
  return ec == KVRGW_ERR_OK;
}


//--------------------------------------------------------------------------------
// ---- Scenario A: multipart location permutations ---------------------------------
struct MCase {
  const char *name;
  Shape sh;
  Exp exp;
};

template <class Pred>
bool wait_r(Ctx &c, const RefTag &tag, Pred pred)
{
  return wait_until([&] { return pred(r_count(c, tag)); }, kGcWaitMs);
}

// order 0: owner first; 1: last copy first; 2: unshared delete (no copy).
void run_mcase(Ctx &c, const std::string &bucket, const bucket_id_t bid, const MCase &mc,
               int order)
{
  const std::string what = std::string(mc.name) + "/order" + std::to_string(order);
  std::cout << "  " << what << "\n";
  const std::string key = std::string("m-") + mc.name + "-" + std::to_string(order);
  Mpu src;
  if (!make_mpu(c, bucket, key, mc.sh, &src)) {
    return;
  }
  check_fields(c, bid, src, mc.exp, what + " put");
  CHECKW(part_files_exist(c, src), what + " part files");
  verify_reads(c, bucket, src);
  const RefTag root_tag = src.head.hdr.ref_tag;

  if (order == 2) {
    CHECK(del(c, bucket, key));
    expect_freed(c, bid, src, what + " unshared delete");
    return;
  }

  // ---- CopyObject of an unshared source: lazy promotion
  const std::string k1 = key + "-c1", k2 = key + "-c2";
  if (!copy_to(c, bucket, key, k1)) {
    return;
  }
  Mpu d1;
  if (!derive(c, bucket, src, k1, &d1) || !refresh(c, bucket, src)) {
    return;
  }
  CHECKW(src.head.has_shared_data() && d1.head.has_shared_data(), what + " both shared");
  CHECKW(src.head.hdr.chunk_type == CHUNK_MULTIPART &&
             d1.head.hdr.chunk_type == CHUNK_MULTIPART_REF,
         what + " chunk types");
  CHECKW(src.head.manifest_loc == kManifestChildM && d1.head.manifest_loc == kManifestChildM,
         what + " manifest in C:M");
  CHECKW(d1.head.chunk.get_ref_tag() == root_tag, what + " REF points at owner");
  CHECKW(!(d1.head.hdr.ref_tag == root_tag), what + " REF has own ref_tag");
  if (mc.sh.alg != CksumAlg::NONE) {
    const uint8_t want = mc.exp.cksum == kCksumInline ? kCksumChildS : mc.exp.cksum;
    CHECKW(src.head.cksum_loc == want && d1.head.cksum_loc == want, what + " cksum loc");
    if (mc.exp.cksum == kCksumInline) {
      CHECKW((src.head.mflags & kMFlagCksumCached) && (d1.head.mflags & kMFlagCksumCached),
             what + " cksum cached");
    }
  }
  CHECKW(src.head.has_external_metadata() == mc.exp.ext_meta, what + " owner metadata");
  CHECKW(src.head.raw.size() <= kMaxOValueBytes && d1.head.raw.size() <= kMaxOValueBytes,
         what + " :O: <= 1024");
  CHECKW(r_count(c, root_tag) == 2u, what + " R: == 2");
  check_children(c, bid, src.head, what + " owner");
  check_children(c, bid, d1.head, what + " copy");
  verify_reads(c, bucket, d1);
  verify_reads(c, bucket, src);

  // ---- CopyObject of a shared source: ref-count only
  if (!copy_to(c, bucket, k1, k2)) {
    return;
  }
  Mpu d2;
  if (!derive(c, bucket, src, k2, &d2)) {
    return;
  }
  CHECKW(d2.head.hdr.chunk_type == CHUNK_MULTIPART_REF && d2.head.has_shared_data(),
         what + " copy-of-copy REF");
  CHECKW(d2.head.chunk.get_ref_tag() == root_tag, what + " copy-of-copy owner");
  CHECKW(r_count(c, root_tag) == 3u, what + " R: == 3");
  check_children(c, bid, d2.head, what + " copy2");
  verify_reads(c, bucket, d2);

  // ---- delete every owner; shared children go only at zero
  const auto still_alive = [&](const char *when) {
    CHECKW(child_exists(c, bid, root_tag.view(), kChildTypeManifest), what + " C:M " + when);
    CHECKW(part_files_exist(c, src), what + " part files " + when);
    if (mc.sh.alg != CksumAlg::NONE) {
      CHECKW(src.head.cksum_loc != kCksumFile || file_exists(c, checksum_filename(root_tag)),
             what + " cksum file " + when);
    }
  };
  if (order == 0) {
    CHECK(del(c, bucket, key));
    CHECKW(wait_r(c, root_tag, [](auto r) { return r == 2u; }), what + " R: -> 2");
    still_alive("after owner delete");
    verify_reads(c, bucket, d1);
    verify_reads(c, bucket, d2);
    CHECK(del(c, bucket, k1));
    CHECKW(wait_r(c, root_tag, [](auto r) { return r == 1u; }), what + " R: -> 1");
    still_alive("after second delete");
    verify_reads(c, bucket, d2);
    CHECK(del(c, bucket, k2));
  } else {
    CHECK(del(c, bucket, k2));
    CHECK(del(c, bucket, k1));
    CHECKW(wait_r(c, root_tag, [](auto r) { return r == 1u; }), what + " R: -> 1");
    still_alive("owner only");
    verify_reads(c, bucket, src);
    CHECK(del(c, bucket, key));
  }
  expect_freed(c, bid, src, what + " shared delete");
}

void test_multipart(Ctx &c)
{
  std::cout << "\n[LAYOUT-1] multipart location permutations\n";
  const std::string bucket = new_bucket(c, "ol-mp");
  const bucket_id_t bid = bid_of(c, bucket);
  const std::string ct255(255, 'c');
  constexpr uint8_t I = kManifestInline, E = kManifestChildE, M = kManifestChildM;
  constexpr uint8_t ci = kCksumInline, cs = kCksumChildS, cf = kCksumFile;
  const MCase cases[] = {
      {"none-8", {8, CksumAlg::NONE, false, 0, "t"}, {I, ci, false}},
      {"sha256-8", {8, CksumAlg::SHA256, false, 0, "t"}, {I, ci, false}},
      {"crc32-8", {8, CksumAlg::CRC32, false, 0, "t"}, {I, ci, false}},
      {"crc64-8", {8, CksumAlg::CRC64NVME, false, 0, "t"}, {I, ci, false}},
      {"sha256-128", {128, CksumAlg::SHA256, false, 0, "t"}, {I, cs, false}},
      {"sha512-1200", {1200, CksumAlg::SHA512, false, 0, "t"}, {I, cs, false}},
      {"sha512-1201", {1201, CksumAlg::SHA512, false, 0, "t"}, {I, cf, false}},
      {"none-500u", {500, CksumAlg::NONE, true, 0, "t"}, {M, ci, false}},
      {"crc64-500u", {500, CksumAlg::CRC64NVME, true, 0, "t"}, {M, cs, false}},
      {"sha256-500u", {500, CksumAlg::SHA256, true, 0, "t"}, {M, cs, false}},
      {"none-8-xm", {8, CksumAlg::NONE, false, 1000, "t"}, {I, ci, true}},
      {"sha256-8-xm", {8, CksumAlg::SHA256, false, 1000, "t"}, {I, ci, true}},
      {"sha256-128-xm", {128, CksumAlg::SHA256, false, 1000, "t"}, {I, cs, true}},
      {"none-500u-xm", {500, CksumAlg::NONE, true, 1000, "t"}, {E, ci, true}},
      {"sha256-500u-xm", {500, CksumAlg::SHA256, true, 1000, "t"}, {E, cs, true}},
      {"ct255-sha512-8", {8, CksumAlg::SHA512, false, 0, ct255}, {I, ci, false}},
      {"ct255-sha512-10", {10, CksumAlg::SHA512, false, 0, ct255}, {I, cs, false}},
  };
  for (const MCase &mc : cases) {
    for (int order = 0; order < 3; ++order) {
      run_mcase(c, bucket, bid, mc, order);
    }
  }
}

//--------------------------------------------------------------------------------
// ---- Scenario B: single-part data/metadata placement -----------------------------
void test_single(Ctx &c)
{
  std::cout << "\n[LAYOUT-2] single-part placement, copy and delete\n";
  const std::string bucket = new_bucket(c, "ol-sp");
  const bucket_id_t bid = bid_of(c, bucket);
  const auto tc = c.svc.tier_config_state().active_copy();
  const uint32_t mi = tc.max_inline, mk = tc.max_kv_store;
  const std::string mval(1000, 'm');
  const MetaPair mp{"mk", mval};

  const uint32_t sizes[] = {0, 1, mi, mi + 1, mk, mk + 1};
  for (const bool ext : {false, true}) {
    for (const uint32_t n : sizes) {
      for (int round = 0; round < 2; ++round) {
        const std::string what = "n=" + std::to_string(n) + (ext ? " xmeta" : " imeta") +
                                 " round" + std::to_string(round);
        std::cout << "  " << what << "\n";
        const std::string key = "sp-" + std::to_string(n) + (ext ? "x" : "i") + std::to_string(round);
        const std::string key2 = key + "-copy";
        const uint64_t base = next_base();
        std::vector<uint8_t> body(n);
        fill(body.data(), base, 0, n);
        const auto pr = c.svc.put_object(
            c.tenant, bucket, key, body.data(), n, "t", n, nullptr, {},
            ext ? std::span<const MetaPair>(&mp, 1) : std::span<const MetaPair>());
        CHECKW(pr.error_code == KVRGW_ERR_OK, what + " put");
        if (pr.error_code != KVRGW_ERR_OK) {
          continue;
        }

        const ChunkType want = ext ? (n <= mk ? CHUNK_INLINE : CHUNK_STORAGE)
                                   : (n <= mi ? CHUNK_INLINE : n <= mk ? CHUNK_CHILD_D : CHUNK_STORAGE);
        const bool data_in_e = ext && n <= mk;
        OwnedObjectValue h;
        std::string detail;
        CHECK(c.svc.head_object(c.tenant, bucket, key, std::nullopt, 0, &h, &detail) == KVRGW_ERR_OK);
        CHECKW(h.hdr.chunk_type == want, what + " chunk type");
        CHECKW(h.has_external_metadata() == ext, what + " external metadata");
        CHECKW(((h.hdr.flags & kFlagDataInChildE) != 0) == data_in_e, what + " DataInChildE");
        CHECKW(!h.has_shared_data(), what + " unshared");
        CHECKW(h.raw.size() <= kMaxOValueBytes, what + " :O: <= 1024");
        CHECKW(child_exists(c, bid, h.hdr.ref_tag.view(), kChildTypeExtended) == ext, what + " C:E");
        KeyBuf dkey;
        make_d_key(bid, d_size_tier_from_size(n), h.hdr.ref_tag.view(), h.hdr.last_modified_sec, dkey);
        CHECKW(kv_exists(c, dkey) == (want == CHUNK_CHILD_D), what + " C:D");
        const std::string stor = h.hdr.ref_tag.filename();
        CHECKW(file_exists(c, stor) == (want == CHUNK_STORAGE), what + " storage file");
        const RefTag root_tag = h.hdr.ref_tag;

        const auto read_ok = [&](const std::string &k, const char *when) {
          Svc::GetObjectResult g;
          CHECKW(c.svc.get_object(c.tenant, bucket, k, std::nullopt, nullptr, 0, &g) == KVRGW_ERR_OK,
                 what + " GET " + when);
          CHECKW(g.body.size() == n, what + " GET size " + when);
          check_data("single GET", g.body, base, 0);
          if (n > 2) {
            ByteRange r{static_cast<int64_t>(n / 2), static_cast<int64_t>(n - 1)};
            Svc::GetObjectResult rg;
            CHECKW(c.svc.get_object(c.tenant, bucket, k, std::nullopt, &r, 0, &rg) == KVRGW_ERR_OK,
                   what + " ranged GET " + when);
            check_data("single ranged GET", rg.body, base, n / 2);
          }
        };
        read_ok(key, "orig");
        Svc::ListObjectsResult lo;
        CHECK(c.svc.list_objects(c.tenant, bucket, key, "", 1000, "", "", &lo) == KVRGW_ERR_OK);
        bool found = false;
        for (const auto &e : lo.objects) {
          found |= e.key == key && e.size == n;
        }
        CHECKW(found, what + " list");

        // A copy in another second: a C:D copy shares the source's D: key.
        if (want == CHUNK_CHILD_D) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        }
        if (!copy_to(c, bucket, key, key2)) {
          continue;
        }
        OwnedObjectValue h2;
        CHECK(c.svc.head_object(c.tenant, bucket, key, std::nullopt, 0, &h, &detail) == KVRGW_ERR_OK);
        CHECK(c.svc.head_object(c.tenant, bucket, key2, std::nullopt, 0, &h2, &detail) == KVRGW_ERR_OK);
        if (want == CHUNK_INLINE) {
          // Inline and C:E data is copied, never shared.
          CHECKW(!h.has_shared_data() && !h2.has_shared_data(), what + " inline not shared");
          CHECKW(h2.hdr.chunk_type == CHUNK_INLINE, what + " copy inline");
          CHECKW(((h2.hdr.flags & kFlagDataInChildE) != 0) == data_in_e, what + " copy DataInChildE");
          CHECKW(!r_count(c, root_tag).has_value(), what + " no R:");
        } else if (want == CHUNK_CHILD_D) {
          CHECKW(h.has_shared_data() && h2.has_shared_data(), what + " D shared");
          CHECKW(h2.hdr.chunk_type == CHUNK_CHILD_D_REF, what + " copy D_REF");
          auto raw = c.svc.store().get(dkey.view());
          CHECKW(raw && raw->has_value() && read_d_ref_count(**raw).shared &&
                     read_d_ref_count(**raw).ref_count == 2,
                 what + " D ref_count == 2");
        } else {
          CHECKW(h.has_shared_data() && h2.has_shared_data(), what + " storage shared");
          CHECKW(h2.hdr.chunk_type == CHUNK_STORAGE_REF, what + " copy STORAGE_REF");
          CHECKW(r_count(c, root_tag) == 2u, what + " R: == 2");
        }
        CHECKW(h2.has_external_metadata() == ext, what + " copy external metadata");
        CHECKW(child_exists(c, bid, h2.hdr.ref_tag.view(), kChildTypeExtended) == ext, what + " copy C:E");
        read_ok(key2, "copy");

        const std::string &first = round == 0 ? key : key2;
        const std::string &last = round == 0 ? key2 : key;
        CHECK(del(c, bucket, first));
        read_ok(last, "after first delete");
        CHECK(del(c, bucket, last));

        const bool ok = wait_until(
            [&] {
              return !kv_exists(c, dkey) && !file_exists(c, stor) &&
                     !r_count(c, root_tag).has_value() &&
                     count_children(c, bid, root_tag.view()) == 0 &&
                     count_children(c, bid, h2.hdr.ref_tag.view()) == 0;
            },
            kGcWaitMs);
        CHECKW(ok, what + " freed (D:, file, R:, C:)");
      }
    }
  }
}

int run(KvRgwRuntime &runtime, const std::filesystem::path &root)
{
  Ctx c{runtime.service(), tenant_id_t{}, root};
  bool exists = false;
  const std::string tenant_name = "o-value-layout-test";
  auto ec = c.svc.resolve_tenant(tenant_name, &exists, &c.tenant);
  if (ec != KVRGW_ERR_OK || !exists) {
    CHECK(c.svc.add_tenant(tenant_name, &c.tenant) == KVRGW_ERR_OK);
  }
  test_single(c);
  test_multipart(c);
  std::cout << (g_failures == 0 ? "\nALL O-VALUE LAYOUT TESTS PASSED\n" : "\nCHECK(S) FAILED\n");
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
  std::snprintf(data_root_buf, sizeof(data_root_buf), "/tmp/kvrgw-olayout-data-%d",
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
