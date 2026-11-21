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

// put_condition_test: verifies If-Match / If-None-Match enforcement for every
// PUT tier (inline, child-D, storage, multipart) x versioning state x
// existing-object state x condition combination, against the real
// FileDataStore (perf_mode=false) so stored data -- not only KV state -- is
// validated (plans/put_finalize_and_multipart_step3_fixes.md, Sub-Task 1).
//
// Usage:  put_condition_test [--tier inline|childd|storage|multipart] [-v]
//                          [--strict-mixed-batch]
//
// Batch mode comes from the environment (KVRGW_BATCH_SIZE, default 1);
// scripts/run_put_condition_test.sh runs both batch_size=1 and batch_size>1.
//
// Per case the oracle expected() predicts the error code. Besides the code:
//   * failure: the current object is unchanged (etag/size/version id) and no
//     new version appears; a failed storage-tier PUT leaves no P:O (checked
//     immediately when batching is off) and its blob disappears once GC / the
//     sweeper ran; a failed multipart complete leaves the upload intact (no
//     P:M/G:M, part file still present, a following unconditional complete on
//     the same upload id succeeds).
//   * success: the new body/etag is readable; in versioned buckets the old
//     version stays retrievable by version id and a new version appears; in
//     unversioned buckets the old storage-tier blob is removed by GC.

#include "constants.hpp"
#include "error_codes.hpp"
#include "keys.hpp"
#include "kv_store.hpp"
#include "kvrgw_runtime.hpp"
#include "object_value.hpp"
#include "ref_tag.hpp"
#include "service_impl.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <iostream>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace kvrgw {
namespace {

using Svc = KvRgwServiceImpl;

constexpr size_t  kInlineSize    = 100;
constexpr size_t  kChildDSize    = 2048;
constexpr size_t  kStorageSize   = 16384;
constexpr size_t  kMpuSize       = 100;   // single (last) part: no min-size rule
constexpr int     kGoneTimeoutMs = 40000;
constexpr uint8_t kOldFill       = 0x11;
constexpr uint8_t kNewFill       = 0xAA;
constexpr const char *kMismatchEtag = "00112233445566778899aabbccddeeff";
constexpr const char *kTenantName   = "put-condition-test";

bool g_verbose = false;
// Whole-batch failure on a single failed condition is a known, user-deferred
// issue (plan open question 6); only fatal with --strict-mixed-batch.
bool g_strict_mixed = false;
int  g_pass    = 0;
int  g_fail    = 0;
int  g_batch   = 1;

// ---------------------------------------------------------------------------
enum class Tier { Inline, ChildD, Storage, Multipart };
enum class ExKind { Absent, Live, DeleteMarker };

struct ExState {
  ExKind kind;
  Tier   tier;   // meaningful for Live (and the base object of DeleteMarker)
};

const char *tier_name(Tier t)
{
  switch (t) {
  case Tier::Inline:    return "inline";
  case Tier::ChildD:    return "childd";
  case Tier::Storage:   return "storage";
  case Tier::Multipart: return "multipart";
  }
  return "?";
}

size_t tier_size(Tier t)
{
  switch (t) {
  case Tier::Inline:    return kInlineSize;
  case Tier::ChildD:    return kChildDSize;
  case Tier::Storage:   return kStorageSize;
  case Tier::Multipart: return kMpuSize;
  }
  return 0;
}

const char *vs_name(VersioningState v)
{
  switch (v) {
  case VERSIONING_ENABLED:   return "enabled";
  case VERSIONING_SUSPENDED: return "suspended";
  default:                   return "disabled";
  }
}

std::string ex_name(const ExState &e)
{
  switch (e.kind) {
  case ExKind::Absent:       return "absent";
  case ExKind::Live:         return std::string("live:") + tier_name(e.tier);
  case ExKind::DeleteMarker: return "delete-marker";
  }
  return "?";
}

// ---------------------------------------------------------------------------
// Condition table. Kinds: 0 none, 1 match, 2 mismatch, 3 star.
// ---------------------------------------------------------------------------
struct CondCase {
  const char *name;
  int im;
  int inm;
};

constexpr CondCase kConds[] = {
  {"none",                   0, 0},
  {"IM=match",               1, 0},
  {"IM=mismatch",            2, 0},
  {"IM=*",                   3, 0},
  {"INM=*",                  0, 3},
  {"INM=match",              0, 1},
  {"INM=mismatch",           0, 2},
  {"IM=match,INM=*",         1, 3},
  {"IM=*,INM=match",         3, 1},
  {"IM=match,INM=mismatch",  1, 2},
};

// Oracle: check_put_condition() semantics plus PutCondition::encode()'s
// rejection of two distinct non-"*" ETags.
KvrgwErrorCode expected(const CondCase &c, ExKind st)
{
  const bool im_etag  = (c.im == 1 || c.im == 2);
  const bool inm_etag = (c.inm == 1 || c.inm == 2);
  if (im_etag && inm_etag) {
    return KVRGW_ERR_INVALID_ARGUMENT;
  }
  const bool live = (st == ExKind::Live);
  if (c.im != 0) {
    if (!live) {
      return KVRGW_ERR_NO_SUCH_KEY;
    }
    if (c.im == 2) {
      return KVRGW_ERR_PRECONDITION_FAILED;
    }
  }
  if (c.inm != 0 && live && (c.inm == 3 || c.inm == 1)) {
    return KVRGW_ERR_PRECONDITION_FAILED;
  }
  return KVRGW_ERR_OK;
}

std::string cond_str(int kind, const std::string &match_etag)
{
  switch (kind) {
  case 1:  return match_etag;
  case 2:  return kMismatchEtag;
  case 3:  return "*";
  default: return {};
  }
}

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------
struct BucketInfo {
  std::string      name;
  bucket_id_t      id{};
  VersioningState  vs{};
};

struct Env {
  Svc                  *svc{};
  KvStore              *store{};
  tenant_id_t           tenant{};
  std::filesystem::path data_root;
  uint64_t              serial{0};
  BucketInfo            buckets[3];

  // Files expected to be removed by GC / sweeper: path -> reason.
  std::vector<std::pair<std::filesystem::path, std::string>> pending_gone;
  // Blob file names whose G:O entry was observed in FDB after the overwrite.
  std::set<std::string> go_written;
  // Blob file names of failed batched storage PUTs (cleanup left to sweeper).
  std::set<std::string> batch_abandoned;
};

std::set<std::string> list_files(const std::filesystem::path &root)
{
  std::set<std::string> out;
  std::error_code ec;
  for (const auto &e : std::filesystem::directory_iterator(root, ec)) {
    if (e.is_regular_file(ec)) {
      out.insert(e.path().filename().string());
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Object snapshot
// ---------------------------------------------------------------------------
struct Snap {
  KvrgwErrorCode ec{KVRGW_ERR_OK};
  std::string    etag;
  uint64_t       size{0};
  std::string    vid;
  ChunkType      chunk{CHUNK_INLINE};
  std::string    ref_file;
  int            versions{0};

  bool same_object(const Snap &o) const
  {
    if (ec != o.ec) return false;
    if (ec != KVRGW_ERR_OK) return true;
    return etag == o.etag && size == o.size && vid == o.vid;
  }
};

Snap snapshot(Env &env, const BucketInfo &b, const std::string &key)
{
  Snap s;
  OwnedObjectValue hv;
  std::string detail;
  s.ec = env.svc->head_object(env.tenant, b.name, key, std::nullopt, 0, &hv, &detail);
  if (s.ec == KVRGW_ERR_OK) {
    s.etag     = hv.etag_display();
    s.size     = hv.hdr.size;
    s.vid      = hv.hdr.version_id.to_hex();
    s.chunk    = static_cast<ChunkType>(hv.hdr.chunk_type);
    s.ref_file = hv.hdr.ref_tag.filename();
  }
  Svc::ListObjectVersionsResult lv;
  const auto lec = env.svc->list_object_versions(env.tenant, b.name, key, 1000, "",
                                                 version_id_t{}, &lv);
  if (lec == KVRGW_ERR_OK) {
    for (const auto &v : lv.versions) {
      if (v.key == key) ++s.versions;
    }
  }
  return s;
}

int count_po(Env &env, const BucketInfo &b)
{
  const auto prefix = make_p_bucket_prefix(b.id);
  const std::string end = prefix_range_end(prefix.view());
  auto r = env.store->range_scan(prefix.view(), end, 1000);
  return r ? static_cast<int>(r->size()) : -1;
}

bool kv_present(Env &env, const KeyBuf &k)
{
  auto r = env.store->get(k.view());
  return r && r->has_value();
}

// True if a G:O entry for the data file `ref_file` currently exists.
bool g_has_go(Env &env, const std::string &ref_file)
{
  const auto gp = make_g_prefix();
  auto gr = env.store->range_scan(gp.view(), prefix_range_end(gp.view()), 100000);
  if (!gr) return false;
  for (const auto &kvp : *gr) {
    const auto pk = parse_go_key(kvp.key);
    if (pk && pk->ref_tag.filename() == ref_file) return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// PUT helpers
// ---------------------------------------------------------------------------
struct PutRes {
  KvrgwErrorCode ec{KVRGW_ERR_OK};
  std::string    etag;
};

struct MpuPrep {
  KvrgwErrorCode ec{KVRGW_ERR_OK};
  std::string    uid;
  std::string    part_etag;
};

MpuPrep mpu_prep(Env &env, const BucketInfo &b, const std::string &key, uint8_t fill)
{
  MpuPrep p;
  auto init = env.svc->initiate_multipart_upload(env.tenant, b.name, key,
                                                 "application/octet-stream", {}, {});
  p.ec = init.error_code;
  if (p.ec != KVRGW_ERR_OK) return p;
  p.uid = init.upload_id;
  const std::string data(kMpuSize, static_cast<char>(fill));
  auto up = env.svc->upload_part(env.tenant, b.name, key, p.uid, 1,
                                 reinterpret_cast<const uint8_t *>(data.data()),
                                 data.size());
  p.ec = up.error_code;
  p.part_etag = up.etag;
  return p;
}

PutRes mpu_complete(Env &env, const BucketInfo &b, const std::string &key,
                    const MpuPrep &p, const std::string &im, const std::string &inm)
{
  std::vector<Svc::CompletedPart> parts{{1, p.part_etag}};
  auto cm = env.svc->complete_multipart_upload(env.tenant, b.name, key, p.uid, parts, im, inm);
  PutRes r;
  r.ec = cm.error_code;
  r.etag = cm.etag;
  return r;
}

PutRes put_plain(Env &env, const BucketInfo &b, const std::string &key, Tier t,
                 uint8_t fill, const std::string &im, const std::string &inm)
{
  const std::string data(tier_size(t), static_cast<char>(fill));
  Svc::AWSPutCondition cond{im, inm};
  const bool has_cond = !im.empty() || !inm.empty();
  auto r = env.svc->put_object(env.tenant, b.name, key,
                               reinterpret_cast<const uint8_t *>(data.data()), data.size(),
                               "application/octet-stream", data.size(),
                               has_cond ? &cond : nullptr, {}, {});
  return PutRes{r.error_code, r.etag};
}

PutRes put_unconditional(Env &env, const BucketInfo &b, const std::string &key,
                         Tier t, uint8_t fill)
{
  if (t == Tier::Multipart) {
    auto p = mpu_prep(env, b, key, fill);
    if (p.ec != KVRGW_ERR_OK) return PutRes{p.ec, {}};
    return mpu_complete(env, b, key, p, "", "");
  }
  return put_plain(env, b, key, t, fill, "", "");
}

// Reads the object (optionally a specific version) and compares the body.
bool read_matches(Env &env, const BucketInfo &b, const std::string &key,
                  std::optional<version_id_t> vid, size_t size, uint8_t fill,
                  std::string *why)
{
  Svc::GetObjectResult res;
  const auto ec = env.svc->get_object(env.tenant, b.name, key, vid, nullptr, 0, &res);
  if (ec != KVRGW_ERR_OK) {
    *why = std::string("get_object ec=") + kvrgw_strerror(ec);
    return false;
  }
  if (res.body.size() != size) {
    *why = "body size " + std::to_string(res.body.size()) + " != " + std::to_string(size);
    return false;
  }
  for (size_t i = 0; i < res.body.size(); ++i) {
    if (static_cast<uint8_t>(res.body[i]) != fill) {
      *why = "body mismatch at byte " + std::to_string(i);
      return false;
    }
  }
  return true;
}

// ---------------------------------------------------------------------------
// Result recording
// ---------------------------------------------------------------------------
void report(const std::string &label, const std::vector<std::string> &problems)
{
  if (problems.empty()) {
    ++g_pass;
    if (g_verbose) {
      std::cout << "PASS " << label << "\n";
    }
    return;
  }
  ++g_fail;
  std::cout << "FAIL " << label << "\n";
  for (const auto &p : problems) {
    std::cout << "       - " << p << "\n";
  }
}

// ---------------------------------------------------------------------------
// One matrix case
// ---------------------------------------------------------------------------
void run_case(Env &env, const BucketInfo &b, Tier new_tier, const ExState &ex,
              const CondCase &cc)
{
  const std::string label = std::string("new=") + tier_name(new_tier) +
                            " vs=" + vs_name(b.vs) + " existing=" + ex_name(ex) +
                            " cond=" + cc.name;
  std::vector<std::string> problems;
  const std::string key = "case-" + std::to_string(env.serial++);

  // ---- build the existing-object state ----
  if (ex.kind != ExKind::Absent) {
    auto r = put_unconditional(env, b, key, ex.tier, kOldFill);
    if (r.ec != KVRGW_ERR_OK) {
      problems.push_back(std::string("setup put failed: ") + kvrgw_strerror(r.ec));
      report(label, problems);
      return;
    }
    if (ex.kind == ExKind::DeleteMarker) {
      Svc::DeleteResult dr;
      const auto dec = env.svc->delete_object(env.tenant, b.name, key, nullptr, &dr);
      if (dec != KVRGW_ERR_OK || !dr.created_dm) {
        problems.push_back(std::string("setup delete-marker failed: ") + kvrgw_strerror(dec));
        report(label, problems);
        return;
      }
    }
  }

  const Snap before = snapshot(env, b, key);
  const std::string match_etag =
      before.ec == KVRGW_ERR_OK ? before.etag : std::string(kMismatchEtag);
  const std::string im  = cond_str(cc.im, match_etag);
  const std::string inm = cond_str(cc.inm, match_etag);
  const KvrgwErrorCode want = expected(cc, ex.kind);

  // ---- multipart: prepare the upload before sampling "before" resources ----
  MpuPrep prep;
  if (new_tier == Tier::Multipart) {
    prep = mpu_prep(env, b, key, kNewFill);
    if (prep.ec != KVRGW_ERR_OK) {
      problems.push_back(std::string("mpu prep failed: ") + kvrgw_strerror(prep.ec));
      report(label, problems);
      return;
    }
  }
  const auto files_before = list_files(env.data_root);
  const int  po_before    = count_po(env, b);

  // ---- the operation under test ----
  PutRes got;
  if (new_tier == Tier::Multipart) {
    got = mpu_complete(env, b, key, prep, im, inm);
  } else {
    got = put_plain(env, b, key, new_tier, kNewFill, im, inm);
  }

  if (got.ec != want) {
    problems.push_back(std::string("error code ") + kvrgw_strerror(got.ec) +
                       ", expected " + kvrgw_strerror(want));
  }

  const Snap after = snapshot(env, b, key);

  if (got.ec == KVRGW_ERR_OK && want == KVRGW_ERR_OK) {
    // ---- success assertions ----
    std::string why;
    if (!read_matches(env, b, key, std::nullopt, tier_size(new_tier), kNewFill, &why)) {
      problems.push_back("new object unreadable: " + why);
    }
    if (after.etag != got.etag) {
      problems.push_back("etag mismatch: stored " + after.etag + " returned " + got.etag);
    }
    if (b.vs == VERSIONING_ENABLED) {
      if (after.versions != before.versions + 1) {
        problems.push_back("version count " + std::to_string(after.versions) +
                           ", expected " + std::to_string(before.versions + 1));
      }
      if (before.ec == KVRGW_ERR_OK) {
        if (!read_matches(env, b, key, version_id_t::from_hex(before.vid),
                          tier_size(ex.tier), kOldFill, &why)) {
          problems.push_back("old version not retrievable: " + why);
        }
      }
    } else if (before.ec == KVRGW_ERR_OK && before.chunk == CHUNK_STORAGE) {
      if (g_has_go(env, before.ref_file)) {
        env.go_written.insert(before.ref_file);
      }
      env.pending_gone.emplace_back(env.data_root / before.ref_file,
                                    "old storage blob after overwrite [" + label + "]");
    }
  } else if (got.ec != KVRGW_ERR_OK) {
    // ---- failure assertions ----
    if (!before.same_object(after)) {
      problems.push_back("current object changed by a failed PUT (etag " + before.etag +
                         " -> " + after.etag + ", ec " + std::to_string(before.ec) +
                         " -> " + std::to_string(after.ec) + ")");
    }
    if (before.versions != after.versions) {
      problems.push_back("version count changed " + std::to_string(before.versions) +
                         " -> " + std::to_string(after.versions));
    }
    if (new_tier == Tier::Storage) {
      if (g_batch <= 1) {
        const int po_after = count_po(env, b);
        if (po_after != po_before) {
          problems.push_back("P:O left behind (" + std::to_string(po_before) + " -> " +
                             std::to_string(po_after) + ")");
        }
      }
      for (const auto &f : list_files(env.data_root)) {
        if (!files_before.count(f)) {
          env.pending_gone.emplace_back(env.data_root / f,
                                        "blob of failed storage PUT [" + label + "]");
          if (g_batch > 1) env.batch_abandoned.insert(f);
        }
      }
    } else if (new_tier == Tier::Multipart) {
      std::string raw;
      bool        hex_ok = prep.uid.size() % 2 == 0;
      for (size_t i = 0; hex_ok && i + 1 < prep.uid.size(); i += 2) {
        auto nib = [](char c) -> int {
          if (c >= '0' && c <= '9') return c - '0';
          if (c >= 'a' && c <= 'f') return c - 'a' + 10;
          if (c >= 'A' && c <= 'F') return c - 'A' + 10;
          return -1;
        };
        const int hi = nib(prep.uid[i]), lo = nib(prep.uid[i + 1]);
        if (hi < 0 || lo < 0) {
          hex_ok = false;
        } else {
          raw.push_back(static_cast<char>((hi << 4) | lo));
        }
      }
      if (hex_ok) {
        KeyBuf pm, gm;
        make_pm_key(b.id, raw, pm);
        make_gm_key(b.id, raw, gm);
        if (kv_present(env, pm)) problems.push_back("P:M written by failed complete");
        if (kv_present(env, gm)) problems.push_back("G:M written by failed complete");
      }
      if (!std::filesystem::exists(env.data_root / (prep.uid + "-1-0"))) {
        problems.push_back("part file removed by failed complete");
      }
      // The upload must still be usable -- only when the failure was a
      // post-Phase-0 condition failure (INVALID_ARGUMENT never touches it).
      auto retry = mpu_complete(env, b, key, prep, "", "");
      if (retry.ec != KVRGW_ERR_OK) {
        problems.push_back(std::string("upload not usable after failed complete: ") +
                           kvrgw_strerror(retry.ec));
      }
    }
  }

  report(label, problems);
}

// ---------------------------------------------------------------------------
// Mixed batch: one failing-condition entry + several unconditional entries
// submitted concurrently. Only meaningful when batching is on.
// ---------------------------------------------------------------------------
void run_mixed_batch(Env &env, const BucketInfo &b, Tier tier)
{
  constexpr int kOthers = 4;
  constexpr int kRounds = 5;
  const std::string label = std::string("mixed-batch tier=") + tier_name(tier) +
                            " vs=" + vs_name(b.vs);
  std::vector<std::string> problems;

  for (int round = 0; round < kRounds && problems.empty(); ++round) {
    const std::string base = "mixed-" + std::to_string(env.serial++) + "-";
    const std::string victim = base + "victim";
    auto setup = put_unconditional(env, b, victim, tier, kOldFill);
    if (setup.ec != KVRGW_ERR_OK) {
      problems.push_back("setup failed");
      break;
    }
    const Snap before = snapshot(env, b, victim);

    std::vector<std::future<PutRes>> futs;
    futs.push_back(std::async(std::launch::async, [&] {
      return put_plain(env, b, victim, tier, kNewFill, kMismatchEtag, "");
    }));
    for (int i = 0; i < kOthers; ++i) {
      const std::string k = base + std::to_string(i);
      futs.push_back(std::async(std::launch::async, [&env, &b, k, tier] {
        return put_plain(env, b, k, tier, kNewFill, "", "");
      }));
    }
    const auto r0 = futs[0].get();
    if (r0.ec != KVRGW_ERR_PRECONDITION_FAILED) {
      problems.push_back(std::string("failing entry returned ") + kvrgw_strerror(r0.ec));
    }
    for (int i = 0; i < kOthers; ++i) {
      const auto r = futs[static_cast<size_t>(i) + 1].get();
      if (r.ec != KVRGW_ERR_OK) {
        problems.push_back(std::string("unconditional entry ") + std::to_string(i) +
                           " failed with " + kvrgw_strerror(r.ec) +
                           " (one entry's failed condition fails its whole batch)");
      } else {
        std::string why;
        if (!read_matches(env, b, base + std::to_string(i), std::nullopt,
                          tier_size(tier), kNewFill, &why)) {
          problems.push_back("unconditional entry unreadable: " + why);
        }
      }
    }
    if (!before.same_object(snapshot(env, b, victim))) {
      problems.push_back("victim object changed by failed conditional PUT");
    }
  }
  if (!problems.empty() && !g_strict_mixed) {
    std::cout << "KNOWN-ISSUE " << label
              << " (whole-batch failure; use --strict-mixed-batch to enforce)\n";
    for (const auto &p : problems) {
      std::cout << "       - " << p << "\n";
    }
    return;
  }
  report(label, problems);
}

// ---------------------------------------------------------------------------
void wait_for_gc(Env &env)
{
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(kGoneTimeoutMs);
  for (;;) {
    bool any = false;
    for (const auto &[p, why] : env.pending_gone) {
      if (std::filesystem::exists(p)) {
        any = true;
        break;
      }
    }
    if (!any || std::chrono::steady_clock::now() >= deadline) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  for (const auto &[p, why] : env.pending_gone) {
    if (std::filesystem::exists(p)) {
      // Other GC workers on the same FDB cluster (running frontends) may
      // consume the G:O entry and delete the blob under *their* data root.
      // A G:O that was written and is now gone means the backend did its job.
      if (env.go_written.count(p.filename().string()) &&
          !g_has_go(env, p.filename().string())) {
        ++g_pass;
        if (g_verbose) {
          std::cout << "PASS G:O consumed by another GC instance: "
                    << p.filename().string() << "\n";
        }
        continue;
      }
      // Batched failed storage PUT: the sweeper (any instance on this FDB)
      // turns the surviving P:O into G:O and GC removes it. If no P:O/G:O
      // remains for the blob, another instance already consumed them.
      if (env.batch_abandoned.count(p.filename().string()) &&
          !g_has_go(env, p.filename().string()) &&
          count_po(env, env.buckets[0]) + count_po(env, env.buckets[1]) +
                  count_po(env, env.buckets[2]) == 0) {
        ++g_pass;
        continue;
      }
      ++g_fail;
      std::cout << "FAIL file not removed by GC: " << p.filename().string()
                << " (" << why << ")\n";
    } else {
      ++g_pass;
      if (g_verbose) {
        std::cout << "PASS file removed by GC: " << p.filename().string() << "\n";
      }
    }
  }
}

void cleanup_buckets(Env &env)
{
  for (auto &b : env.buckets) {
    if (b.name.empty()) continue;
    std::vector<std::string> keys;
    std::vector<std::string> ref_keys;
    std::vector<version_id_t> ref_vids;
    std::string key_marker;
    version_id_t vid_marker{};
    for (;;) {
      Svc::ListObjectVersionsResult lv;
      if (env.svc->list_object_versions(env.tenant, b.name, "", 1000, key_marker,
                                        vid_marker, &lv) != KVRGW_ERR_OK) {
        break;
      }
      for (const auto &v : lv.versions) {
        keys.push_back(v.key);
        ref_keys.push_back(v.key);
        ref_vids.push_back(v.version_id);
      }
      if (!lv.is_truncated) break;
      key_marker = lv.next_key_marker;
      vid_marker = lv.next_version_id_marker;
    }
    std::vector<Svc::DeleteMultiObjectRef> refs;
    refs.reserve(ref_keys.size());
    for (size_t i = 0; i < ref_keys.size(); ++i) {
      refs.push_back({ref_keys[i], b.vs == VERSIONING_DISABLED
                                       ? std::nullopt
                                       : std::optional<version_id_t>(ref_vids[i])});
    }
    std::vector<Svc::DeleteMultiKeyOutcome> outcomes;
    if (b.vs == VERSIONING_DISABLED) {
      std::sort(keys.begin(), keys.end());
      keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
      env.svc->delete_multi(env.tenant, b.name, std::span<const std::string>(keys),
                            std::span<const Svc::DeleteMultiObjectRef>{}, &outcomes);
    } else {
      env.svc->delete_multi(env.tenant, b.name, std::span<const std::string>{},
                            std::span<const Svc::DeleteMultiObjectRef>(refs), &outcomes);
    }
    const auto ec = env.svc->delete_bucket(env.tenant, b.name);
    if (ec != KVRGW_ERR_OK && g_verbose) {
      std::cerr << "WARN: cleanup delete_bucket(" << b.name << ") ec=" << kvrgw_strerror(ec)
                << "\n";
    }
  }
}

std::string unique_suffix()
{
  return std::to_string(getpid()) + "-" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() % 100000);
}

int run(KvRgwRuntime &rt, const std::filesystem::path &data_root,
        const std::set<Tier> &tiers)
{
  Env env;
  env.svc = &rt.service();
  env.store = &rt.store();
  env.data_root = data_root;

  bool exists = false;
  auto ec = env.svc->resolve_tenant(kTenantName, &exists, &env.tenant);
  if (ec != KVRGW_ERR_OK || !exists) {
    ec = env.svc->add_tenant(kTenantName, &env.tenant);
    if (ec != KVRGW_ERR_OK) {
      std::cerr << "FAIL: add_tenant ec=" << kvrgw_strerror(ec) << "\n";
      return 1;
    }
  }

  const VersioningState states[3] = {VERSIONING_DISABLED, VERSIONING_ENABLED,
                                     VERSIONING_SUSPENDED};
  for (int i = 0; i < 3; ++i) {
    BucketInfo &b = env.buckets[i];
    b.vs = states[i];
    b.name = std::string("pc-") + vs_name(states[i]) + "-" + unique_suffix();
    ec = env.svc->create_bucket(env.tenant, b.name);
    if (ec != KVRGW_ERR_OK) {
      std::cerr << "FAIL: create_bucket ec=" << kvrgw_strerror(ec) << "\n";
      return 1;
    }
    if (states[i] != VERSIONING_DISABLED) {
      ec = env.svc->put_bucket_versioning(env.tenant, b.name, states[i]);
      if (ec != KVRGW_ERR_OK) {
        std::cerr << "FAIL: put_bucket_versioning ec=" << kvrgw_strerror(ec) << "\n";
        return 1;
      }
    }
    auto bid = env.svc->resolve_bucket_id(env.tenant, b.name);
    if (!bid || !bid->has_value()) {
      std::cerr << "FAIL: resolve_bucket_id\n";
      return 1;
    }
    b.id = **bid;
  }

  const Tier all_tiers[4] = {Tier::Inline, Tier::ChildD, Tier::Storage, Tier::Multipart};

  std::cout << "=== put_condition_test  batch_size=" << g_batch << " ===\n";

  for (const Tier nt : all_tiers) {
    if (!tiers.count(nt)) continue;
    for (auto &b : env.buckets) {
      std::vector<ExState> states_ex;
      states_ex.push_back({ExKind::Absent, Tier::Inline});
      for (const Tier et : all_tiers) {
        states_ex.push_back({ExKind::Live, et});
      }
      if (b.vs != VERSIONING_DISABLED) {
        states_ex.push_back({ExKind::DeleteMarker, Tier::Inline});
      }
      for (const auto &ex : states_ex) {
        for (const auto &cc : kConds) {
          run_case(env, b, nt, ex, cc);
        }
      }
    }
  }

  if (g_batch > 1) {
    for (const Tier nt : {Tier::Inline, Tier::Storage}) {
      if (!tiers.count(nt)) continue;
      for (auto &b : env.buckets) {
        run_mixed_batch(env, b, nt);
      }
    }
  }

  wait_for_gc(env);
  cleanup_buckets(env);

  std::cout << "\n=== SUMMARY: " << g_pass << " passed, " << g_fail << " failed ===\n";
  return g_fail == 0 ? 0 : 1;
}

}  // namespace
}  // namespace kvrgw

int main(int argc, char **argv)
{
  std::set<kvrgw::Tier> tiers = {kvrgw::Tier::Inline, kvrgw::Tier::ChildD,
                                 kvrgw::Tier::Storage, kvrgw::Tier::Multipart};
  for (int i = 1; i < argc; ++i) {
    if (!std::strcmp(argv[i], "-v") || !std::strcmp(argv[i], "--verbose")) {
      kvrgw::g_verbose = true;
    } else if (!std::strcmp(argv[i], "--strict-mixed-batch")) {
      kvrgw::g_strict_mixed = true;
    } else if (!std::strcmp(argv[i], "--tier") && i + 1 < argc) {
      const std::string t = argv[++i];
      tiers.clear();
      if (t == "inline")         tiers.insert(kvrgw::Tier::Inline);
      else if (t == "childd")    tiers.insert(kvrgw::Tier::ChildD);
      else if (t == "storage")   tiers.insert(kvrgw::Tier::Storage);
      else if (t == "multipart") tiers.insert(kvrgw::Tier::Multipart);
      else if (t == "all") {
        tiers = {kvrgw::Tier::Inline, kvrgw::Tier::ChildD, kvrgw::Tier::Storage,
                 kvrgw::Tier::Multipart};
      } else {
        std::cerr << "unknown tier '" << t << "'\n";
        return 2;
      }
    }
  }

  char admin_sock[64];
  std::snprintf(admin_sock, sizeof(admin_sock), "/tmp/kvrgw-pc-test-%d.sock",
                static_cast<int>(getpid()));
  setenv("KVRGW_ADMIN_SOCKET", admin_sock, 1);

  // Tier thresholds forced before runtime.start(); batch size may be supplied
  // by the caller (scripts/run_put_condition_test.sh).
  setenv("KVRGW_MAX_INLINE",           "256",   1);
  setenv("KVRGW_MAX_KV_STORE",         "4096",  1);
  setenv("KVRGW_BATCH_SIZE",           "1",     0);
  setenv("KVRGW_BATCH_TIMEOUT_US",     "20000", 0);
  setenv("KVRGW_BATCH_THREADS",        "1",     0);
  // Fast GC / sweeper convergence so blob removal can be observed.
  setenv("KVRGW_GC_INTERVAL_SEC",      "1",     0);
  setenv("KVRGW_SWEEPER_INTERVAL_SEC", "1",     0);
  setenv("KVRGW_SWEEPER_MIN_AGE_SEC",  "2",     0);
  kvrgw::g_batch = std::atoi(std::getenv("KVRGW_BATCH_SIZE"));

  kvrgw::KvRgwStartOptions opts;
  opts.perf_mode = false;
  char data_root_buf[64];
  std::snprintf(data_root_buf, sizeof(data_root_buf), "/tmp/kvrgw-pc-test-data-%d",
                static_cast<int>(getpid()));
  const char *env_root = std::getenv("KVRGW_DATA_ROOT");
  opts.data_root = env_root ? env_root : data_root_buf;
  std::error_code fs_ec;
  std::filesystem::create_directories(opts.data_root, fs_ec);

  kvrgw::KvRgwRuntime runtime;
  if (!runtime.start(opts)) {
    std::cerr << "FAIL: runtime start failed\n";
    return 1;
  }
  const int rc = kvrgw::run(runtime, opts.data_root, tiers);
  runtime.stop();
  std::filesystem::remove_all(opts.data_root, fs_ec);
  return rc;
}
