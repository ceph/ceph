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

// batch_mixed_test: two phases of 100 iterations of random mixed-mode batches.
// Phase 1: short object names (iter-%03d-obj-%02d).
// Phase 2: long object names (512-1024 random chars).
// Each iteration generates 1-10 entries with interleaved CHUNK_INLINE,
// CHUNK_CHILD_D, and CHUNK_STORAGE types.  All entries in one iteration
// are submitted concurrently so they land in the same batch window.
// After each iteration every object is GET-verified before the next starts.
// A final sweep re-verifies all objects, then delete_multi + delete_bucket.
// Batch stats are printed at the end of each phase for tuning.

#include "admin_server.hpp"
#include "constants.hpp"
#include "error_codes.hpp"
#include "id_meta.hpp"
#include "id_tag.hpp"
#include "kvrgw_runtime.hpp"
#include "object_value.hpp"
#include "service_impl.hpp"
#include "tier_config_state.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <functional>
#include <future>
#include <iostream>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <unistd.h>
#include <vector>

namespace kvrgw {
namespace {

// ---------------------------------------------------------------------------
// Test configuration
// ---------------------------------------------------------------------------
static constexpr int      kNumIterations   = 100;
static constexpr int      kMaxEntries      = 10;
static constexpr uint32_t kMaxInlineBytes  = 256;
static constexpr uint32_t kMaxKvStoreBytes = 4096;
static constexpr int      kBatchSize       = 10;
static constexpr int      kBatchTimeoutUs  = 10000;   // 10 ms
static constexpr int      kBatchThreads    = 1;
static constexpr size_t   kLongKeyMin      = 512;
static constexpr size_t   kLongKeyMax      = 1024;
static constexpr size_t   kPhase3KeyMin    = 376;
static constexpr size_t   kPhase3KeyMax    = 1024;
static constexpr int      kPhase3DataMin   = 4097;
static constexpr int      kPhase3DataMax   = 8192;
static constexpr const char *kTenantName   = "batch-mixed-test";

// Generate a random 32-char lowercase alphanumeric bucket name.
static std::string make_random_bucket_name()
{
  static constexpr std::string_view kChars = "abcdefghijklmnopqrstuvwxyz0123456789";
  std::random_device rd;
  std::mt19937 rng(rd());
  std::uniform_int_distribution<size_t> dist(0, kChars.size() - 1);
  std::string name(32, ' ');
  // AWS bucket names must start with a letter
  name[0] = kChars[dist(rng) % 26];
  for (size_t i = 1; i < 32; ++i) {
    name[i] = kChars[dist(rng)];
  }
  return name;
}

// ---------------------------------------------------------------------------
// Per-entry description used for PUT and GET-verification
// ---------------------------------------------------------------------------
struct EntryDesc {
  std::string object_name;
  ChunkType   expected_chunk_type;
  size_t      data_size;
  uint8_t     fill_byte;
  std::string meta_value;   // value of metadata key "meta000"
  std::string tag_value;    // value of tag key "tag"
};

// ---------------------------------------------------------------------------
// Name generators
// ---------------------------------------------------------------------------
static std::string make_obj_name(int iter, int entry)
{
  char buf[64];
  std::snprintf(buf, sizeof(buf), "iter-%03d-obj-%02d", iter, entry);
  return buf;
}

// Generic random-length name generator.
static std::string make_rand_key_name(int iter, int entry,
                                      size_t min_len, size_t max_len,
                                      std::mt19937 &rng)
{
  static constexpr std::string_view kChars = "abcdefghijklmnopqrstuvwxyz0123456789";
  std::uniform_int_distribution<size_t> dist_len(min_len, max_len);
  std::uniform_int_distribution<size_t> dist_char(0, kChars.size() - 1);

  char prefix[16];
  const int pfx_len = std::snprintf(prefix, sizeof(prefix), "i%03de%02d-", iter, entry);

  const size_t total_len = dist_len(rng);
  const size_t rand_len  = total_len > static_cast<size_t>(pfx_len)
                               ? total_len - static_cast<size_t>(pfx_len) : 0;
  std::string name;
  name.reserve(total_len);
  name.append(prefix, static_cast<size_t>(pfx_len));
  for (size_t i = 0; i < rand_len; ++i) {
    name += kChars[dist_char(rng)];
  }
  return name;
}

// Long name: key length in [kLongKeyMin, kLongKeyMax].
static std::string make_long_key_name(int iter, int entry, std::mt19937 &rng)
{
  return make_rand_key_name(iter, entry, kLongKeyMin, kLongKeyMax, rng);
}

// Medium name: key length in [kPhase3KeyMin, kPhase3KeyMax].
static std::string make_medium_key_name(int iter, int entry, std::mt19937 &rng)
{
  return make_rand_key_name(iter, entry, kPhase3KeyMin, kPhase3KeyMax, rng);
}

static uint8_t compute_fill_byte(int iter, int entry)
{
  return static_cast<uint8_t>((iter * 13 + entry * 7) & 0xFF);
}

static ChunkType chunk_type_for_size(size_t sz)
{
  if (sz <= kMaxInlineBytes)   return CHUNK_INLINE;
  if (sz <= kMaxKvStoreBytes)  return CHUNK_CHILD_D;
  return CHUNK_STORAGE;
}

// ---------------------------------------------------------------------------
// PUT one entry (called from a std::async thread)
// ---------------------------------------------------------------------------
static KvRgwServiceImpl::PutObjectResult put_entry(
    KvRgwServiceImpl  &service,
    tenant_id_t        tenant_id,
    const std::string &bucket_name,
    const EntryDesc   &desc)
{
  std::vector<uint8_t> data(desc.data_size, desc.fill_byte);

  // Metadata: key="meta000"
  std::array<uint8_t, MAX_META_FRAME_BYTES> meta_buf{};
  size_t meta_size = 0;
  const std::string_view meta_key = "meta000";
  const std::string_view meta_val = desc.meta_value;
  MetaPair meta_pair{meta_key, meta_val};
  encode_metadata(std::span<const MetaPair>(&meta_pair, 1),
                  std::span<uint8_t>(meta_buf.data(), meta_buf.size()),
                  meta_size);

  // Tag: key="tag"
  const std::string_view tag_key = "tag";
  const std::string_view tag_val = desc.tag_value;
  TagPair tag_pair{tag_key, tag_val};

  return service.put_object(
      tenant_id,
      bucket_name,
      desc.object_name,
      data.data(), data.size(),
      "application/octet-stream",
      data.size(),
      nullptr,
      std::span<const TagPair>(&tag_pair, 1),
      std::span<const MetaPair>(&meta_pair, 1));
}

// ---------------------------------------------------------------------------
// GET + verify one entry; returns true on pass
// ---------------------------------------------------------------------------
static bool verify_entry(KvRgwServiceImpl  &service,
                         tenant_id_t        tenant_id,
                         const std::string &bucket_name,
                         const EntryDesc   &desc)
{
  KvRgwServiceImpl::GetObjectResult res;
  const auto ec = service.get_object(
      tenant_id, bucket_name, desc.object_name,
      std::nullopt, nullptr, 0, &res);

  if (ec != KVRGW_ERR_OK) {
    std::cerr << "FAIL get_object(" << desc.object_name
              << ") ec=" << kvrgw_strerror(ec) << "\n";
    return false;
  }

  // Body size + fill pattern
  if (res.body.size() != desc.data_size) {
    std::cerr << "FAIL " << desc.object_name
              << " body_size=" << res.body.size()
              << " expected=" << desc.data_size << "\n";
    return false;
  }
  for (size_t k = 0; k < res.body.size(); ++k) {
    if (static_cast<uint8_t>(res.body[k]) != desc.fill_byte) {
      std::cerr << "FAIL " << desc.object_name
                << " data mismatch at byte " << k << "\n";
      return false;
    }
  }

  // Chunk type
  if (res.value.hdr.chunk_type != desc.expected_chunk_type) {
    std::cerr << "FAIL " << desc.object_name
              << " chunk_type=" << static_cast<int>(res.value.hdr.chunk_type)
              << " expected=" << static_cast<int>(desc.expected_chunk_type) << "\n";
    return false;
  }

  // Metadata key "meta000"
  if (!res.value.metadata_frame.empty()) {
    std::array<MetaPair, MAX_META_COUNT> out_meta{};
    if (decode_metadata(std::span<const uint8_t>(res.value.metadata_frame),
                        out_meta)) {
      bool found = false;
      for (const auto &kv : out_meta) {
        if (kv.first.empty()) break;
        if (kv.first == "meta000") {
          found = true;
          if (kv.second != desc.meta_value) {
            std::cerr << "FAIL " << desc.object_name
                      << " meta value='" << kv.second
                      << "' expected='" << desc.meta_value << "'\n";
            return false;
          }
          break;
        }
      }
      if (!found) {
        std::cerr << "FAIL " << desc.object_name << " meta000 key not found\n";
        return false;
      }
    }
  } else if (!desc.meta_value.empty()) {
    std::cerr << "FAIL " << desc.object_name << " metadata_frame is empty\n";
    return false;
  }

  // Tag key "tag" — stored out-of-band; fetched via get_object_tagging().
  {
    std::vector<uint8_t> tag_live;
    std::array<TagPair, MAX_TAG_COUNT> out_tags{};
    size_t tag_count = 0;
    const auto tag_ec = service.get_object_tagging(
        tenant_id, bucket_name, desc.object_name, tag_live, out_tags, &tag_count);
    if (tag_ec != KVRGW_ERR_OK) {
      std::cerr << "FAIL " << desc.object_name
                << " get_object_tagging ec=" << kvrgw_strerror(tag_ec) << "\n";
      return false;
    }
    bool found = false;
    for (size_t i = 0; i < tag_count; ++i) {
      if (out_tags[i].first == "tag") {
        found = true;
        if (out_tags[i].second != desc.tag_value) {
          std::cerr << "FAIL " << desc.object_name
                    << " tag value='" << out_tags[i].second
                    << "' expected='" << desc.tag_value << "'\n";
          return false;
        }
        break;
      }
    }
    if (!found) {
      std::cerr << "FAIL " << desc.object_name << " tag key 'tag' not found\n";
      return false;
    }
  }

  return true;
}

// ---------------------------------------------------------------------------
// Print ErrorStats — non-zero counters printed in RED
// ---------------------------------------------------------------------------
static constexpr const char *kRed   = "\033[31m";
static constexpr const char *kReset = "\033[0m";

static bool print_error_stats(const ErrorStats &es)
{
  // Known error codes (sparse enum — iterate the canonical list).
  static constexpr KvrgwErrorCode kCodes[] = {
    KVRGW_ERR_NO_SUCH_KEY,
    KVRGW_ERR_NO_SUCH_BUCKET,
    KVRGW_ERR_BUCKET_ALREADY_EXISTS,
    KVRGW_ERR_BUCKET_NOT_EMPTY,
    KVRGW_ERR_PRECONDITION_FAILED,
    KVRGW_ERR_ACCESS_DENIED,
    KVRGW_ERR_INVALID_ARGUMENT,
    KVRGW_ERR_INVALID_BUCKET_NAME,
    KVRGW_ERR_NO_SUCH_VERSION,
    KVRGW_ERR_NO_SUCH_TENANT,
    KVRGW_ERR_INVALID_RANGE,
    KVRGW_ERR_INVALID_REQUEST,
    KVRGW_ERR_INVALID_TAG,
    KVRGW_ERR_TENANT_ALREADY_EXISTS,
    KVRGW_ERR_FDB_CONFLICT,
    KVRGW_ERR_FDB_PROCESS_BEHIND,
    KVRGW_ERR_FDB_FUTURE_VERSION,
    KVRGW_ERR_FDB_TRANSACTION_TOO_OLD,
    KVRGW_ERR_FDB_NOT_COMMITTED,
    KVRGW_ERR_FDB_COMMIT_UNKNOWN,
    KVRGW_ERR_FDB_CLUSTER_VERSION_CHANGED,
    KVRGW_ERR_FDB_KEY_TOO_LARGE,
    KVRGW_ERR_FDB_VALUE_TOO_LARGE,
    KVRGW_ERR_FDB_TRANSACTION_TOO_LARGE,
    KVRGW_ERR_INTERNAL,
    KVRGW_ERR_CORRUPT_VALUE,
    KVRGW_ERR_BUCKET_ID_MISMATCH,
    KVRGW_ERR_VALUE_TOO_LARGE,
    KVRGW_ERR_TRANSACTION_CONFLICT,
    KVRGW_ERR_MAX_RETRIES_EXCEEDED,
  };

  int64_t error_count = 0;
  for (auto code : kCodes) {
    error_count += es.get(code);
  }
  if (!error_count) {
    return false;
  }

  bool any_nonzero = false;
  std::cout << "\n=== ErrorStats ===\n";
  for (auto code : kCodes) {
    const int64_t cnt = es.get(code);
    if (cnt != 0) {
      any_nonzero = true;
      std::cout << kRed
                << "  " << KvrgwErrorCode_Name(code)
                << " : " << cnt
                << kReset << "\n";
    }
  }
  return any_nonzero;
}

// ---------------------------------------------------------------------------
// Sanity-check committed/attempted counters; print each check green/red
// ---------------------------------------------------------------------------
static constexpr const char *kGreen = "\033[32m";

// Set once from argv in main() before any work starts; read-only thereafter.
// Gates detail output (BatchStats dump, per-check [OK] lines) so default
// runs only report progress + PASS/FAIL, while -v/--verbose shows everything.
static bool g_verbose = false;

// Returns true iff every sanity check passed.
static bool print_batch_sanity(const BatchStats &s,
                               int64_t total_sent,
                               int64_t expected_commits)
{
  const int64_t c_commits = s.committed_batch_commits.load(std::memory_order_relaxed);
  const int64_t c_entries = s.committed_total_entries.load(std::memory_order_relaxed);
  const int64_t a_commits = s.attempted_batch_commits.load(std::memory_order_relaxed);
  const int64_t a_entries = s.attempted_total_entries_batch.load(std::memory_order_relaxed);
  const int64_t a_iol_c   = s.attempted_inline_only_batch_commits.load(std::memory_order_relaxed);
  const int64_t a_iol_e   = s.attempted_inline_only_batch_entries.load(std::memory_order_relaxed);

  struct Check {
    const char *label;
    int64_t     lhs;
    int64_t     rhs;
  };
  const Check checks[] = {
    { "committed_batch_commits == iterations",
      c_commits,
      expected_commits },
    { "committed_total_entries == total_sent",
      c_entries,
      total_sent },
    { "attempted_batch_commits + attempted_inline_only_batch_commits == committed_batch_commits",
      a_commits + a_iol_c,
      c_commits },
    { "attempted_total_entries_batch + attempted_inline_only_batch_entries == total_sent",
      a_entries + a_iol_e,
      total_sent },
  };

  std::cout << "\n=== BatchStats sanity ===\n";
  bool all_ok = true;
  for (const auto &c : checks) {
    const bool ok = (c.lhs == c.rhs);
    if (!ok) all_ok = false;
    if (!ok || g_verbose) {
      std::cout << (ok ? kGreen : kRed)
                << "  [" << (ok ? "OK" : "FAIL") << "] "
                << c.label << "  ("
                << c.lhs << " == " << c.rhs << ")"
                << kReset << "\n";
    }
  }
  return all_ok;
}

// ---------------------------------------------------------------------------
// Print BatchStats
// ---------------------------------------------------------------------------
static void print_batch_stats(const BatchStats &s,
                               int64_t total_sent,
                               int64_t single_entry_iters)
{
  const int64_t a_commits  = s.attempted_batch_commits.load(std::memory_order_relaxed);
  const int64_t a_entries  = s.attempted_total_entries_batch.load(std::memory_order_relaxed);
  const int64_t a_wait_us  = s.attempted_total_wait_us.load(std::memory_order_relaxed);
  const int64_t a_single   = s.attempted_single_entry_batches.load(std::memory_order_relaxed);
  const int64_t a_iol_c    = s.attempted_inline_only_batch_commits.load(std::memory_order_relaxed);
  const int64_t a_iol_e    = s.attempted_inline_only_batch_entries.load(std::memory_order_relaxed);
  const int64_t c_commits  = s.committed_batch_commits.load(std::memory_order_relaxed);
  const int64_t c_entries  = s.committed_total_entries.load(std::memory_order_relaxed);
  const int64_t c_single   = s.committed_single_entry_batches.load(std::memory_order_relaxed);
  const int64_t c_iol_c    = s.committed_inline_only_batch_commits.load(std::memory_order_relaxed);
  const int64_t c_iol_e    = s.committed_inline_only_batch_entries.load(std::memory_order_relaxed);
  const int64_t conflict_pushbacks = s.conflict_pushbacks.load(std::memory_order_relaxed);
  const int64_t fallback_count   = s.fallback_to_single_count.load(std::memory_order_relaxed);
  const int64_t fallback_entries = s.fallback_to_single_entries.load(std::memory_order_relaxed);

  std::cout << "\n=== BatchStats ===\n"
            << "  total_entries_sent                   : " << total_sent << "\n"
            << "  single_entry_iters_sent              : " << single_entry_iters << "\n"
            << "  -- attempted --\n"
            << "  attempted_batch_commits              : " << a_commits << "\n"
            << "  attempted_total_entries_batch        : " << a_entries << "\n"
            << "  attempted_avg_batch_size             : "
            << (a_commits > 0 ? a_entries / a_commits : 0) << "\n"
            << "  attempted_single_entry_batches       : " << a_single << "\n"
            << "  attempted_min_batch_size             : "
            << s.attempted_min_batch_size.load(std::memory_order_relaxed) << "\n"
            << "  attempted_max_batch_size             : "
            << s.attempted_max_batch_size.load(std::memory_order_relaxed) << "\n"
            << "  attempted_avg_wait_us                : "
            << (a_entries > 0 ? a_wait_us / a_entries : 0) << "\n"
            << "  attempted_min_wait_us                : "
            << s.attempted_min_wait_us.load(std::memory_order_relaxed) << "\n"
            << "  attempted_max_wait_us                : "
            << s.attempted_max_wait_us.load(std::memory_order_relaxed) << "\n"
            << "  attempted_inline_only_batch_commits  : " << a_iol_c << "\n"
            << "  attempted_inline_only_batch_entries  : " << a_iol_e << "\n"
            << "  -- committed --\n"
            << "  committed_batch_commits              : " << c_commits << "\n"
            << "  committed_total_entries              : " << c_entries << "\n"
            << "  committed_single_entry_batches       : " << c_single << "\n"
            << "  committed_inline_only_batch_commits  : " << c_iol_c << "\n"
            << "  committed_inline_only_batch_entries  : " << c_iol_e << "\n";

  if (fallback_count || fallback_entries || conflict_pushbacks) {
    std::cout << "  -- misc --\n"
              << "  conflict_pushbacks                   : " << conflict_pushbacks << "\n"
              << "  fallback_to_single_count             : " << fallback_count << "\n"
              << "  fallback_to_single_entries           : " << fallback_entries << "\n";
  }
}

// ---------------------------------------------------------------------------
// BucketRegistry — tracks every bucket created during the run along with the
// object keys known to exist in it (appended as each PUT succeeds). Used by
// BucketCleanupGuard to guarantee delete_multi + delete_bucket on any
// failure path, not just the normal success path.
// ---------------------------------------------------------------------------
struct BucketRegistry {
  struct Entry {
    std::string              bucket_name;
    std::vector<std::string> keys;
  };
  std::vector<Entry> entries;

  void add_bucket(const std::string &bucket_name)
  {
    entries.push_back(Entry{bucket_name, {}});
  }

  Entry *find(const std::string &bucket_name)
  {
    for (auto &e : entries) {
      if (e.bucket_name == bucket_name) return &e;
    }
    return nullptr;
  }

  void remove(const std::string &bucket_name)
  {
    entries.erase(
        std::remove_if(entries.begin(), entries.end(),
                       [&](const Entry &e) { return e.bucket_name == bucket_name; }),
        entries.end());
  }
};

// Best-effort delete_multi (of every tracked key) + delete_bucket for every
// bucket still registered. Errors are logged but not propagated — this is a
// last-resort safety net invoked while the test is already failing.
static void cleanup_registered_buckets(KvRgwServiceImpl &service,
                                       tenant_id_t tenant_id,
                                       BucketRegistry &registry)
{
  for (auto &e : registry.entries) {
    if (!e.keys.empty()) {
      std::vector<KvRgwServiceImpl::DeleteMultiKeyOutcome> outcomes;
      const auto ec = service.delete_multi(
          tenant_id, e.bucket_name,
          std::span<const std::string>(e.keys),
          std::span<const KvRgwServiceImpl::DeleteMultiObjectRef>{},
          &outcomes);
      if (ec != KVRGW_ERR_OK) {
        std::cerr << kRed << "cleanup: delete_multi failed bucket=" << e.bucket_name
                  << " ec=" << kvrgw_strerror(ec) << kReset << "\n";
      }
    }
    const auto ec2 = service.delete_bucket(tenant_id, e.bucket_name);
    if (ec2 != KVRGW_ERR_OK) {
      std::cerr << kRed << "cleanup: delete_bucket failed bucket=" << e.bucket_name
                << " ec=" << kvrgw_strerror(ec2) << kReset << "\n";
    }
  }
  registry.entries.clear();
}

// RAII guard: on destruction, if any buckets are still registered (meaning
// the normal success-path cleanup never reached them), force-deletes every
// object + the bucket itself so no S3-visible state leaks on any failure
// path (PUT failure, verify failure, delete_multi/delete_bucket failure,
// create_bucket failure, etc.).
struct BucketCleanupGuard {
  KvRgwServiceImpl &service;
  tenant_id_t       tenant_id;
  BucketRegistry   &registry;

  ~BucketCleanupGuard()
  {
    if (!registry.entries.empty()) {
      std::cerr << kRed << "cleanup: " << registry.entries.size()
                << " bucket(s) left registered after failure — force-deleting\n"
                << kReset;
      cleanup_registered_buckets(service, tenant_id, registry);
    }
  }
};

// ---------------------------------------------------------------------------
// run_phase — shared iteration loop for all phases
//
// make_name_fn : std::string(int iter, int entry, std::mt19937&)
// iter_offset  : added to iter in name generation to avoid log confusion
// seed         : seeds the RNG; printed in banners for reproducibility
// phase_label  : e.g. "Phase 1: short keys" printed in banners
// min_entries  : lower bound of per-iteration entry count (upper = kMaxEntries)
// min_data_sz,
// max_data_sz  : when non-zero, skip the 3-way type roll and draw size from
//                [min_data_sz, max_data_sz]; when zero use the type roll.
// ---------------------------------------------------------------------------
using NameFn = std::function<std::string(int, int, std::mt19937 &)>;

static int run_phase(KvRgwServiceImpl  &service,
                     tenant_id_t        tenant_id,
                     const std::string &bucket_name,
                     BucketRegistry    &registry,
                     uint32_t           seed,
                     int                iter_offset,
                     const char        *phase_label,
                     NameFn             make_name_fn,
                     int                min_entries  = 1,
                     int                min_data_sz  = 0,
                     int                max_data_sz  = 0)
{
  BucketRegistry::Entry *reg_entry = registry.find(bucket_name);
  if (!reg_entry) {
    std::cerr << "FAIL: bucket=" << bucket_name
              << " not registered in BucketRegistry before run_phase\n";
    return 1;
  }
  std::mt19937 rng(seed);
  std::uniform_int_distribution<int> dist_n(min_entries, kMaxEntries);
  std::uniform_int_distribution<int> dist_type(0, 2);
  std::uniform_int_distribution<int> dist_inline_sz(1, static_cast<int>(kMaxInlineBytes));
  std::uniform_int_distribution<int> dist_child_sz(
      static_cast<int>(kMaxInlineBytes) + 1, static_cast<int>(kMaxKvStoreBytes));
  std::uniform_int_distribution<int> dist_storage_sz(
      static_cast<int>(kMaxKvStoreBytes) + 1, static_cast<int>(kMaxKvStoreBytes) * 2);
  std::uniform_int_distribution<int> dist_fixed_sz(min_data_sz, max_data_sz > 0 ? max_data_sz : min_data_sz);

  std::cout << "=== " << phase_label << "  seed=" << seed
            << "  iterations=" << kNumIterations
            << "  bucket=" << bucket_name << " ===\n";

  std::vector<EntryDesc> all_entries;
  all_entries.reserve(static_cast<size_t>(kNumIterations * kMaxEntries));

  int64_t total_sent         = 0;
  int64_t single_entry_iters = 0;

  for (int iter = 0; iter < kNumIterations; ++iter) {
    const int n = dist_n(rng);
    total_sent += n;
    if (n == 1) ++single_entry_iters;
    std::vector<EntryDesc> descs;
    descs.reserve(static_cast<size_t>(n));

    // Build entry descriptors
    for (int j = 0; j < n; ++j) {
      EntryDesc d;
      d.object_name = make_name_fn(iter + iter_offset, j, rng);
      d.fill_byte   = compute_fill_byte(iter, j);

      size_t sz = 0;
      if (min_data_sz != 0) {
        // Fixed data-size range — no type roll (e.g. phase 3: storage-only).
        sz = static_cast<size_t>(dist_fixed_sz(rng));
      } else {
        const int type_roll = dist_type(rng);
        if (type_roll == 0) {
          // INLINE: first entry of every iteration tests zero-size
          sz = (j == 0) ? 0u : static_cast<size_t>(dist_inline_sz(rng));
        } else if (type_roll == 1) {
          sz = static_cast<size_t>(dist_child_sz(rng));
        } else {
          sz = static_cast<size_t>(dist_storage_sz(rng));
        }
      }
      d.data_size           = sz;
      d.expected_chunk_type = chunk_type_for_size(sz);

      char buf[32];
      std::snprintf(buf, sizeof(buf), "val-%d-%d", iter, j);
      d.meta_value = buf;

      std::snprintf(buf, sizeof(buf), "%d-%d", iter, j);
      d.tag_value = buf;

      descs.push_back(std::move(d));
    }

    // Launch all n PUTs concurrently before waiting on any
    std::vector<std::future<KvRgwServiceImpl::PutObjectResult>> futs;
    futs.reserve(static_cast<size_t>(n));
    for (const auto &d : descs) {
      futs.push_back(std::async(std::launch::async,
                                put_entry,
                                std::ref(service), tenant_id,
                                std::cref(bucket_name), std::cref(d)));
    }

    // Collect PUT results. Entries that succeeded are registered immediately
    // (even if a sibling entry in this same iteration fails) so the
    // BucketCleanupGuard can reliably find and delete them on any failure.
    bool iter_ok = true;
    for (int j = 0; j < n; ++j) {
      const auto res = futs[static_cast<size_t>(j)].get();
      if (res.error_code != KVRGW_ERR_OK) {
        std::cerr << "FAIL iter=" << iter << " entry=" << j
                  << " PUT ec=" << kvrgw_strerror(res.error_code)
                  << " obj=" << descs[static_cast<size_t>(j)].object_name << "\n";
        iter_ok = false;
      } else {
        reg_entry->keys.push_back(descs[static_cast<size_t>(j)].object_name);
      }
    }
    if (!iter_ok) {
      std::cerr << "FAIL: aborting at iter=" << iter
                << "  seed=" << seed << "\n";
      print_error_stats(service.error_stats());
      print_batch_stats(service.batch_queue().stats(), total_sent, single_entry_iters);
      print_batch_sanity(service.batch_queue().stats(), total_sent, iter);
      return 1;
    }

    // GET + verify every entry before next iteration
    for (const auto &d : descs) {
      if (!verify_entry(service, tenant_id, bucket_name, d)) {
        std::cerr << "FAIL: verify at iter=" << iter
                  << "  seed=" << seed << "\n";
        print_error_stats(service.error_stats());
        print_batch_stats(service.batch_queue().stats(), total_sent, single_entry_iters);
        print_batch_sanity(service.batch_queue().stats(), total_sent, iter);
        return 1;
      }
    }

    for (auto &d : descs) {
      all_entries.push_back(std::move(d));
    }
  }

  // Final sweep: re-verify all objects from all iterations
  std::cout << "Final sweep: verifying " << all_entries.size() << " objects...\n";
  for (const auto &d : all_entries) {
    if (!verify_entry(service, tenant_id, bucket_name, d)) {
      std::cerr << "FAIL: final sweep  seed=" << seed << "\n";
      print_error_stats(service.error_stats());
      print_batch_stats(service.batch_queue().stats(), total_sent, single_entry_iters);
      print_batch_sanity(service.batch_queue().stats(), total_sent, kNumIterations);
      return 1;
    }
  }

  const bool had_errors = print_error_stats(service.error_stats());
  const bool sanity_ok  = print_batch_sanity(service.batch_queue().stats(), total_sent, kNumIterations);
  if (g_verbose || had_errors || !sanity_ok) {
    print_batch_stats(service.batch_queue().stats(), total_sent, single_entry_iters);
  }
  if (had_errors) {
    std::cerr << kRed << "FAIL: ErrorStats recorded nonzero error count(s)  seed=" << seed
              << kReset << "\n";
    return 1;
  }
  if (!sanity_ok) {
    std::cerr << kRed << "FAIL: BatchStats sanity check(s) failed  seed=" << seed
              << kReset << "\n";
    return 1;
  }

  // Cleanup: delete all objects via delete_multi, then delete the bucket.
  // reg_entry->keys already mirrors all_entries (every successful PUT was
  // appended there as it completed), so reuse it directly.
  std::vector<KvRgwServiceImpl::DeleteMultiKeyOutcome> outcomes;
  {
    const auto ec = service.delete_multi(
        tenant_id, bucket_name,
        std::span<const std::string>(reg_entry->keys),
        std::span<const KvRgwServiceImpl::DeleteMultiObjectRef>{},
        &outcomes);
    if (ec != KVRGW_ERR_OK) {
      std::cerr << kRed << "FAIL: delete_multi ec=" << kvrgw_strerror(ec)
                << kReset << "\n";
      return 1;
    }
    bool dm_ok = true;
    for (const auto &o : outcomes) {
      if (o.status != KvRgwServiceImpl::DeleteMultiKeyOutcome::Status::Deleted) {
        std::cerr << kRed << "FAIL: delete_multi key=" << o.key
                  << " ec=" << o.error_code << kReset << "\n";
        dm_ok = false;
      } else {
        // Deleted — drop it from the tracked set so a later failure in this
        // same phase doesn't make the cleanup guard attempt to re-delete it.
        auto &ks = reg_entry->keys;
        ks.erase(std::remove(ks.begin(), ks.end(), o.key), ks.end());
      }
    }
    if (!dm_ok) {
      return 1;
    }
  }
  {
    const auto ec = service.delete_bucket(tenant_id, bucket_name);
    if (ec != KVRGW_ERR_OK) {
      std::cerr << kRed << "FAIL: delete_bucket ec=" << kvrgw_strerror(ec)
                << kReset << "\n";
      return 1;
    }
  }

  // Bucket + all its objects are fully deleted — drop it from the registry
  // so BucketCleanupGuard doesn't try to touch it again.
  registry.remove(bucket_name);

  std::cout << kGreen << "PASS: " << phase_label << kReset << "\n";
  return 0;
}

}  // namespace

// ---------------------------------------------------------------------------
// Public entry point (can be called from perf driver or standalone main)
// ---------------------------------------------------------------------------
int run_batch_mixed_test(KvRgwRuntime &runtime)
{
  TierConfig tc{};
  tc.max_inline       = kMaxInlineBytes;
  tc.max_kv_store     = kMaxKvStoreBytes;
  tc.batch_size       = kBatchSize;
  tc.batch_timeout_us = kBatchTimeoutUs;
  tc.batch_threads    = kBatchThreads;
  runtime.service().tier_config_state().try_stage(tc);
  runtime.service().tier_config_state().maybe_apply_pending();

  // Time-based seed — printed in each phase banner for reproducibility.
  const uint32_t seed = static_cast<uint32_t>(std::time(nullptr));

  // Resolve / create tenant once, shared by both phases.
  tenant_id_t tenant_id = 0;
  {
    bool exists = false;
    auto ec = runtime.service().resolve_tenant(kTenantName, &exists, &tenant_id);
    if (ec != KVRGW_ERR_OK || !exists) {
      ec = runtime.service().add_tenant(kTenantName, &tenant_id);
      if (ec != KVRGW_ERR_OK) {
        std::cerr << "FAIL: add_tenant ec=" << kvrgw_strerror(ec) << "\n";
        return 1;
      }
    }
  }

  // Tracks every bucket created below; BucketCleanupGuard force-deletes
  // anything still registered when this function returns (any failure path)
  // so no bucket/object is ever left behind in S3.
  BucketRegistry registry;
  BucketCleanupGuard cleanup_guard{runtime.service(), tenant_id, registry};

  // -----------------------------------------------------------------------
  // Phase 1: short keys
  // -----------------------------------------------------------------------
  const std::string bucket1 = make_random_bucket_name();
  {
    const auto ec = runtime.service().create_bucket(tenant_id, bucket1);
    if (ec != KVRGW_ERR_OK && ec != KVRGW_ERR_BUCKET_ALREADY_EXISTS) {
      std::cerr << "FAIL: create_bucket (phase 1) ec=" << kvrgw_strerror(ec) << "\n";
      return 1;
    }
    registry.add_bucket(bucket1);
  }

  int rc = run_phase(
      runtime.service(), tenant_id, bucket1, registry, seed,
      /*iter_offset=*/0,
      "Phase 1: short keys",
      [](int iter, int entry, std::mt19937 &) {
        return make_obj_name(iter, entry);
      });
  if (rc != 0) return rc;

  // Reset stats so phase 2 sanity checks are independent.
  runtime.service().batch_queue().stats().reset();

  // -----------------------------------------------------------------------
  // Phase 2: long keys [512-1024]
  // -----------------------------------------------------------------------
  const std::string bucket2 = make_random_bucket_name();
  {
    const auto ec = runtime.service().create_bucket(tenant_id, bucket2);
    if (ec != KVRGW_ERR_OK && ec != KVRGW_ERR_BUCKET_ALREADY_EXISTS) {
      std::cerr << "FAIL: create_bucket (phase 2) ec=" << kvrgw_strerror(ec) << "\n";
      return 1;
    }
    registry.add_bucket(bucket2);
  }

  rc = run_phase(
      runtime.service(), tenant_id, bucket2, registry, seed,
      /*iter_offset=*/kNumIterations,
      "Phase 2: long keys [512-1024]",
      [](int iter, int entry, std::mt19937 &rng) {
        return make_long_key_name(iter, entry, rng);
      });
  if (rc != 0) return rc;

  // Reset stats so phase 3 sanity checks are independent.
  runtime.service().batch_queue().stats().reset();

  // -----------------------------------------------------------------------
  // Phase 3: storage-only [376-1024 key, 4097-8192 data]
  // -----------------------------------------------------------------------
  const std::string bucket3 = make_random_bucket_name();
  {
    const auto ec = runtime.service().create_bucket(tenant_id, bucket3);
    if (ec != KVRGW_ERR_OK && ec != KVRGW_ERR_BUCKET_ALREADY_EXISTS) {
      std::cerr << "FAIL: create_bucket (phase 3) ec=" << kvrgw_strerror(ec) << "\n";
      return 1;
    }
    registry.add_bucket(bucket3);
  }

  rc = run_phase(
      runtime.service(), tenant_id, bucket3, registry, seed,
      /*iter_offset=*/2 * kNumIterations,
      "Phase 3: storage-only [376-1024 key, 4097-8192 data]",
      [](int iter, int entry, std::mt19937 &rng) {
        return make_medium_key_name(iter, entry, rng);
      },
      /*min_entries=*/5,
      /*min_data_sz=*/kPhase3DataMin,
      /*max_data_sz=*/kPhase3DataMax);
  return rc;
}

}  // namespace kvrgw

// ---------------------------------------------------------------------------
// Standalone main
// ---------------------------------------------------------------------------
int main(int argc, char **argv)
{
  for (int i = 1; i < argc; ++i) {
    if (std::strcmp(argv[i], "-v") == 0 || std::strcmp(argv[i], "--verbose") == 0) {
      kvrgw::g_verbose = true;
    }
  }

  // Unique admin socket per process to avoid conflicts with running services.
  char admin_sock[64];
  std::snprintf(admin_sock, sizeof(admin_sock),
                "/tmp/kvrgw-test-%d.sock", static_cast<int>(getpid()));
  setenv("KVRGW_ADMIN_SOCKET", admin_sock, 1);

  // Set tier config via env so the runtime picks them up at start time.
  // This ensures batch_threads and batch_size are correct from construction.
  setenv("KVRGW_MAX_INLINE",       "256",   1);
  setenv("KVRGW_MAX_KV_STORE",     "4096",  1);
  setenv("KVRGW_BATCH_SIZE",       "10",    1);
  setenv("KVRGW_BATCH_TIMEOUT_US", "10000", 1);
  setenv("KVRGW_BATCH_THREADS",    "1",     1);

  kvrgw::KvRgwStartOptions opts;
  opts.perf_mode = false;

  // Unique per-process data_root so repeated/parallel runs never collide or
  // accumulate orphaned files from a prior run in a shared fixed directory.
  char data_root_buf[64];
  std::snprintf(data_root_buf, sizeof(data_root_buf),
                "/tmp/kvrgw-test-data-%d", static_cast<int>(getpid()));
  const char *data_root_env = std::getenv("KVRGW_DATA_ROOT");
  opts.data_root = data_root_env ? data_root_env : data_root_buf;

  // Best-effort removal of the local data_root directory on every exit path
  // (success and failure alike).
  auto remove_data_root = [&opts]() {
    std::error_code fs_ec;
    std::filesystem::remove_all(opts.data_root, fs_ec);
    if (fs_ec) {
      std::cerr << "WARN: failed to remove data_root '" << opts.data_root
                << "': " << fs_ec.message() << "\n";
    }
  };

  kvrgw::KvRgwRuntime runtime;
  if (!runtime.start(opts)) {
    std::cerr << "FAIL: runtime start failed\n";
    remove_data_root();
    return 1;
  }

  const int rc = kvrgw::run_batch_mixed_test(runtime);
  runtime.stop();
  remove_data_root();
  return rc;
}
