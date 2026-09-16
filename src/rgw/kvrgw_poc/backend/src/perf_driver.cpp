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

#include "perf_driver.hpp"

#include "constants.hpp"
#include "data_store.hpp"
#include "error_codes.hpp"
#include "fdb_latency.hpp"
#include "id_tag.hpp"
#include "key_buf.hpp"
#include "keys.hpp"
#include "kvrgw_runtime.hpp"
#include "object_value.hpp"
#include "ops_stats.hpp"
#include "tier_config_state.hpp"
#include "typed_ids.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <openssl/evp.h>
#include <optional>
#include <random>
#include <semaphore>
#include <span>
#include <sstream>
#include <string_view>
#include <thread>
#include <vector>

namespace kvrgw {

namespace {
static unsigned constexpr AWS_MAX_BUCKET_NAME = 64;
static constexpr int GET_REQUESTS_QUEUE_SIZE = 1024;
static constexpr int GET_FUTURE_MAX = 1024;
static constexpr auto GET_IDLE_SLEEP = std::chrono::microseconds(10);
static constexpr int kKeyPrefixMaxLen = 768;
static constexpr int kNameMidLen = 21;
static constexpr int GET_OBJECT_NAME_MAX = kKeyPrefixMaxLen + kNameMidLen + 1;
alignas(64) uint8_t g_put_buffer[16384] = {};

int g_instance_id = 0;
char g_key_prefix[kKeyPrefixMaxLen + 1] = "perf";
char g_key_suffix[kKeyPrefixMaxLen + 1] = "";
const char *g_metadata_file = nullptr;
std::vector<uint8_t> g_put_tag_buf;
std::span<const uint8_t> g_put_tags{};

std::string make_object_name(int thread_id, uint64_t seq)
{
  char buf[GET_OBJECT_NAME_MAX];
  if (g_key_suffix[0] == '\0') {
    std::snprintf(buf, sizeof(buf), "%s/%04d/%02d_%012lu", g_key_prefix,
                  thread_id, g_instance_id, seq);
  }
  else {
    std::snprintf(buf, sizeof(buf), "%s/%04d/%02d_%012lu%s", g_key_prefix,
                  thread_id, g_instance_id, seq, g_key_suffix);
  }
  return buf;
}

ObjectValue build_object_value(const RefTag &ref_tag, const uint8_t *data,
                               uint64_t size)
{
  ObjectValue ov{};
  std::memcpy(ov.hdr.ref_tag, ref_tag.data(), kRefTagSize);

  unsigned char digest[16];
  unsigned int digest_len = 0;
  auto *ctx = EVP_MD_CTX_new();
  EVP_DigestInit_ex(ctx, EVP_md5(), nullptr);
  EVP_DigestUpdate(ctx, data, size);
  EVP_DigestFinal_ex(ctx, digest, &digest_len);
  EVP_MD_CTX_free(ctx);

  ov.set_etag_raw(digest);
  ov.hdr.size = size;

  const auto now = std::chrono::system_clock::now();
  const auto sec =
      std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch());
  const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      now.time_since_epoch()) -
                  std::chrono::duration_cast<std::chrono::nanoseconds>(sec);
  ov.hdr.last_modified_sec = static_cast<uint32_t>(sec.count());
  ov.hdr.last_modified_nsec = static_cast<uint32_t>(ns.count());
  ov.content_type = "application/octet-stream";

  return ov;
}

} // namespace

struct BenchResult {
  std::atomic<int64_t> ops{};
  std::atomic<int64_t> errors{};
  std::atomic<int64_t> retries{};
  std::atomic<int64_t> err_counts[::kvrgw::v1::KvrgwErrorCode_ARRAYSIZE]{};

  void record_error(KvrgwErrorCode code)
  {
    err_counts[static_cast<int>(code)].fetch_add(1, std::memory_order_relaxed);
  }
};

namespace {

void print_results(const char *label, const BenchResult &result,
                   double elapsed_sec, LatencyStats &stats)
{
  auto ops = result.ops.load();
  auto errs = result.errors.load();
  double iops = elapsed_sec > 0 ? static_cast<double>(ops) / elapsed_sec : 0;

  std::cout << "\n=== " << label << " ===\n";
  std::cout << "  ops=" << ops << " errors=" << errs
            << " retries=" << result.retries.load() << " IOPS=" << std::fixed
            << std::setprecision(0) << iops
            << " elapsed=" << std::setprecision(1) << elapsed_sec << "s\n";
  if (errs > 0 || result.retries.load() > 0) {
    std::cout << "  Error breakdown:";
    for (int i = 0; i < ::kvrgw::v1::KvrgwErrorCode_ARRAYSIZE; ++i) {
      auto c = result.err_counts[i].load(std::memory_order_relaxed);
      if (c == 0) {
        continue;
      }
      auto ec = static_cast<::kvrgw::v1::KvrgwErrorCode>(i);
      std::cout << " " << ::kvrgw::v1::KvrgwErrorCode_Name(ec) << "=" << c;
    }
    std::cout << "\n";
  }

  std::cout << "  Latency breakdown:\n";
  for (int i = 0; i < static_cast<int>(OpType::kCount); ++i) {
    auto &s = stats.ops[i];
    auto cnt = s.count.load(std::memory_order_relaxed);
    if (cnt == 0) {
      continue;
    }
    auto total = s.total_us.load(std::memory_order_relaxed);
    auto fdb = s.fdb_us.load(std::memory_order_relaxed);
    auto disk = s.disk_us.load(std::memory_order_relaxed);
    double fdb_pct = total > 0 ? 100.0 * fdb / total : 0.0;
    double disk_pct = total > 0 ? 100.0 * disk / total : 0.0;
    std::cout << "    " << op_type_name(static_cast<OpType>(i))
              << " count=" << cnt << " avg_total_us=" << total / cnt
              << " avg_fdb_us=" << fdb / cnt << " fdb_pct=" << std::fixed
              << std::setprecision(1) << fdb_pct << "%"
              << " avg_disk_us=" << disk / cnt << " disk_pct=" << disk_pct
              << "%"
              << " avg_get_us="
              << s.fdb_get_us.load(std::memory_order_relaxed) / cnt
              << " avg_commit_us="
              << s.fdb_commit_us.load(std::memory_order_relaxed) / cnt << "\n";
  }
}

void print_fdb_put_stats(KvStore &store)
{
  auto &s = store.fdb_put_stats();
  const uint64_t n = s.num_put.load(std::memory_order_relaxed);
  const uint64_t k = s.key_bytes.load(std::memory_order_relaxed);
  const uint64_t v = s.value_bytes.load(std::memory_order_relaxed);
  const uint64_t avg_k = (n > 0) ? (k / n) : 0;
  const uint64_t avg_v = (n > 0) ? (v / n) : 0;
  std::cout << "  num-put=" << n << " avg-key-size-put=" << avg_k
            << " avg-value-size-put=" << avg_v << "\n";
}

struct ParsedParams {
  int concurrency = 64;
  int duration = 30;
  int buckets = 1;
  int versions = 0;
  int burst = 1;
  int64_t count = 0;
  int64_t base_files = 0;
  int64_t overwrite_count = -1;
  int progress_sec = 10;
  int producers = 0;
  int consumers = 0;
  int max_futures = 0;
  std::string mode = "none";
  std::vector<uint64_t> tiers;
};

ParsedParams parse_params(const std::string &line)
{
  ParsedParams p;
  std::istringstream iss(line);
  std::string token;
  while (iss >> token) {
    if (token.rfind("c=", 0) == 0) {
      p.concurrency = std::atoi(token.c_str() + 2);
    }
    else if (token.rfind("duration=", 0) == 0) {
      p.duration = std::atoi(token.c_str() + 9);
      p.count = 0;
    }
    else if (token.rfind("buckets=", 0) == 0) {
      p.buckets = std::atoi(token.c_str() + 8);
    }
    else if (token.rfind("mode=", 0) == 0) {
      p.mode = token.substr(5);
    }
    else if (token.rfind("tiers=", 0) == 0) {
      std::string tstr = token.substr(6);
      std::istringstream ts(tstr);
      std::string t;
      while (std::getline(ts, t, ',')) {
        p.tiers.push_back(std::stoull(t));
      }
    }
    else if (token.rfind("count=", 0) == 0) {
      p.count = std::atoll(token.c_str() + 6);
      p.duration = 0;
    }
    else if (token.rfind("versions=", 0) == 0) {
      p.versions = std::atoi(token.c_str() + 9);
    }
    else if (token.rfind("burst=", 0) == 0) {
      p.burst = std::atoi(token.c_str() + 6);
      if (p.burst < 1) {
        p.burst = 1;
      }
    }
    else if (token.rfind("base_files=", 0) == 0) {
      p.base_files = std::atoll(token.c_str() + 11);
    }
    else if (token.rfind("overwrite_count=", 0) == 0) {
      p.overwrite_count = std::atoll(token.c_str() + 16);
    }
    else if (token.rfind("--progress-sec=", 0) == 0) {
      p.progress_sec = std::atoi(token.c_str() + 15);
    }
    else if (token.rfind("producers=", 0) == 0) {
      p.producers = std::atoi(token.c_str() + 10);
    }
    else if (token.rfind("consumers=", 0) == 0) {
      p.consumers = std::atoi(token.c_str() + 10);
    }
    else if (token.rfind("max_futures=", 0) == 0) {
      p.max_futures = std::atoi(token.c_str() + 12);
    }
  }
  return p;
}

//--------------------------------------------------------------------------------
static void fill_idtag_buffer_random(std::span<char> buffer, size_t num_bytes)
{
  assert(num_bytes <= buffer.size() && "Requested bytes exceed buffer size!");

  // AWS:S3 allowed character set for idtag
  static constexpr std::string_view charset(
      " 0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_:./"
      "=+@-");
  constexpr size_t max_index = charset.size() - 1;

  thread_local static std::random_device rd;
  thread_local static std::mt19937 gen(rd());
  std::uniform_int_distribution<size_t> distrib(0, max_index);

  // Fill N elements starting from the beginning of the reference array
  std::generate_n(buffer.begin(), num_bytes,
                  [&distrib]() { return charset[distrib(gen)]; });
}

//--------------------------------------------------------------------------------
static bool init_put_tags()
{
  const char *count_env = std::getenv("KVRGW_TAG_COUNT");
  int count = 0;
  if (count_env) {
    count = std::atoi(count_env);
  }
  if (count == 0) {
    g_put_tags = {};
    g_put_tag_buf.clear();
    return true;
  }
  if (count < 1 || count > static_cast<int>(kMaxTags)) {
    std::cerr << "ERROR: KVRGW_TAG_COUNT must be 0.." << kMaxTags << "\n";
    return false;
  }
  const char *base = std::getenv("KVRGW_TAG_NAME_BASE");
  const char *size_env = std::getenv("KVRGW_TAG_DATA_SIZE");
  if (!base || !base[0]) {
    std::cerr << "ERROR: KVRGW_TAG_NAME_BASE required when tag_count > 0\n";
    return false;
  }
  if (!size_env) {
    std::cerr << "ERROR: KVRGW_TAG_DATA_SIZE required when tag_count > 0\n";
    return false;
  }
  const int data_size = std::atoi(size_env);
  if (data_size < 1 || data_size > static_cast<int>(kMaxTagValueLen)) {
    std::cerr << "ERROR: KVRGW_TAG_DATA_SIZE must be 1.." << kMaxTagValueLen
              << "\n";
    return false;
  }

  std::array<std::array<char, kMaxTagKeyLen + 16>, kMaxTags> keybufs{};
  std::array<std::array<char, kMaxTagValueLen>, kMaxTags> valbufs{};
  // std::array<char, kMaxTagValueLen> val{};
  std::array<TagPair, kMaxTags> pairs;
  for (int i = 0; i < count; ++i) {
    const int n =
        std::snprintf(keybufs[i].data(), keybufs[i].size(), "%s%d", base, i);
    if (n < 1 || static_cast<size_t>(n) > kMaxTagKeyLen) {
      std::cerr << "ERROR: tag key too long\n";
      return false;
    }
    fill_idtag_buffer_random(valbufs[i], static_cast<size_t>(data_size));
    pairs[static_cast<size_t>(i)] = {
        std::string_view(keybufs[i].data(), static_cast<size_t>(n)),
        std::string_view(valbufs[i].data(), static_cast<size_t>(data_size))};
  }
  if (!encode(
          std::span<const TagPair>(pairs.data(), static_cast<size_t>(count)),
          g_put_tag_buf)) {
    std::cerr << "ERROR: tag encode failed\n";
    return false;
  }
  g_put_tags = std::span<const uint8_t>(g_put_tag_buf);
  return true;
}

static void progress_tick(int thread_id, int progress_sec, OpType op,
                          LatencyStats &stats,
                          std::chrono::steady_clock::time_point t0,
                          std::chrono::steady_clock::time_point &last_t,
                          int64_t &last_count, int64_t &last_total_us)
{
  if (thread_id != 0 || progress_sec <= 0) {
    return;
  }
  const auto now = std::chrono::steady_clock::now();
  const double dt = std::chrono::duration<double>(now - last_t).count();
  if (dt < static_cast<double>(progress_sec)) {
    return;
  }

  const auto &s = stats.ops[static_cast<int>(op)];
  const int64_t cnt = s.count.load(std::memory_order_relaxed);
  const int64_t tot = s.total_us.load(std::memory_order_relaxed);
  const double elapsed = std::chrono::duration<double>(now - t0).count();
  const int64_t dcnt = cnt - last_count;
  const int64_t dtot = tot - last_total_us;
  const int64_t iops =
      elapsed > 0 ? static_cast<int64_t>(static_cast<double>(cnt) / elapsed)
                  : 0;
  const int64_t iops_last =
      dt > 0 ? static_cast<int64_t>(static_cast<double>(dcnt) / dt) : 0;
  const int64_t lat = cnt > 0 ? tot / cnt : 0;
  const int64_t lat_last = dcnt > 0 ? dtot / dcnt : 0;

  char line[256];
  std::snprintf(line, sizeof(line),
                "progress: %6.1f elapsed; total: (%6.1fM objects %6lld iops, "
                "%6lldus latency) | "
                "interval: (%6.1fM objects %6lld iops, %6lldus latency)\n",
                elapsed, static_cast<double>(cnt) / 1000000.0,
                static_cast<long long>(iops), static_cast<long long>(lat),
                static_cast<double>(dcnt) / 1000000.0,
                static_cast<long long>(iops_last),
                static_cast<long long>(lat_last));
  std::cout << line << std::flush;

  last_t = now;
  last_count = cnt;
  last_total_us = tot;
}

std::string tier_label(uint64_t size)
{
  if (size < 1024) {
    return std::to_string(size) + "B";
  }
  return std::to_string(size / 1024) + "KB";
}

} // namespace

// --- Workers ---

static constexpr int kMaxBuckets = 128;

struct PutWorkerResult {
  int64_t ops{};
  int64_t errors{};
  int64_t retries{};
  uint64_t bucket_seq[kMaxBuckets]{};
};

void put_worker(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                const std::vector<std::string> &bucket_names, uint64_t obj_size,
                int thread_id, int burst_size, int progress_sec,
                std::chrono::steady_clock::time_point t0,
                std::atomic<bool> &stop, PutWorkerResult &wr,
                BenchResult &result)
{
  int num_buckets = static_cast<int>(bucket_names.size());
  int bucket_idx = 0;
  int burst_count = 0;
  auto last_t = t0;
  int64_t last_count = 0;
  int64_t last_total_us = 0;

  while (!stop.load(std::memory_order_relaxed)) {
    const auto &bucket_name = bucket_names[bucket_idx];
    uint64_t seq = wr.bucket_seq[bucket_idx]++;
    const auto name = make_object_name(thread_id, seq);
    const auto ref_tag = service.ref_tags().next();
    auto ov = build_object_value(ref_tag, g_put_buffer, obj_size);

    ScopedRequestLatency _lat(service.latency_stats(), OpType::kPutObject);

    KvRgwServiceImpl::PutObjectRequest req;
    req.tenant_id = tenant_id;
    req.bucket_name = bucket_name;
    req.object_name = name;
    req.ref_tag = ref_tag;
    req.value = std::move(ov);
    req.estimated_size = obj_size;
    req.cond = nullptr;
    req.tags = g_put_tags;

    auto res = service.put_object_route(req, g_put_buffer, obj_size);
    if (res.error_code == KVRGW_ERR_OK) {
      ++wr.ops;
    }
    else {
      result.record_error(res.error_code);
      if (kvrgw::is_retriable(res.error_code)) {
        ++wr.retries;
      }
      else {
        ++wr.errors;
        std::cerr << "PUT_ERR t=" << thread_id << " key=" << name
                  << " code=" << res.error_code
                  << " msg=" << kvrgw_strerror(res.error_code) << "\n";
      }
    }

    progress_tick(thread_id, progress_sec, OpType::kPutObject,
                  service.latency_stats(), t0, last_t, last_count,
                  last_total_us);

    if (++burst_count >= burst_size) {
      burst_count = 0;
      bucket_idx = (bucket_idx + 1) % num_buckets;
    }
  }
}

void put_pad_worker(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                    const std::vector<std::string> &bucket_names,
                    uint64_t obj_size, int thread_id, uint64_t target_seq,
                    PutWorkerResult &wr)
{
  int num_buckets = static_cast<int>(bucket_names.size());
  for (int b = 0; b < num_buckets; ++b) {
    while (wr.bucket_seq[b] < target_seq) {
      uint64_t seq = wr.bucket_seq[b]++;
      const auto &bucket_name = bucket_names[b];
      const auto name = make_object_name(thread_id, seq);
      const auto ref_tag = service.ref_tags().next();
      auto ov = build_object_value(ref_tag, g_put_buffer, obj_size);

      KvRgwServiceImpl::PutObjectRequest req;
      req.tenant_id = tenant_id;
      req.bucket_name = bucket_name;
      req.object_name = name;
      req.ref_tag = ref_tag;
      req.value = std::move(ov);
      req.estimated_size = obj_size;
      req.cond = nullptr;
      req.tags = g_put_tags;

      service.put_object_route(req, g_put_buffer, obj_size);
    }
  }
}

void get_worker(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                const std::string &bucket_name,
                const std::vector<std::string> &object_names, int thread_id,
                std::atomic<int64_t> &index, BenchResult &result)
{
  int total = static_cast<int>(object_names.size());
  while (true) {
    int64_t idx = index.fetch_add(1, std::memory_order_relaxed);
    if (idx >= total) {
      break;
    }
    KvRgwServiceImpl::GetObjectResult out;
    const auto ec = service.get_object(
        tenant_id, bucket_name, object_names[idx], std::nullopt, nullptr, &out);
    if (ec == KVRGW_ERR_OK) {
      result.ops.fetch_add(1, std::memory_order_relaxed);
    }
    else {
      result.errors.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

void delete_worker(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                   const std::string &bucket_name,
                   const std::vector<std::string> &object_names, int thread_id,
                   std::atomic<int64_t> &index, BenchResult &result)
{
  int total = static_cast<int>(object_names.size());
  while (true) {
    int64_t idx = index.fetch_add(1, std::memory_order_relaxed);
    if (idx >= total) {
      break;
    }
    KvRgwServiceImpl::DeleteResult dr;
    const auto ec = service.delete_object(tenant_id, bucket_name,
                                          object_names[idx], nullptr, &dr);
    if (ec == KVRGW_ERR_OK) {
      result.ops.fetch_add(1, std::memory_order_relaxed);
    }
    else {
      result.record_error(ec);
      result.errors.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

void delete_direct_worker(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                          const std::string &bucket_name, int thread_id,
                          int64_t seq_start, int64_t seq_end,
                          BenchResult &result)
{
  for (int64_t seq = seq_start; seq < seq_end; ++seq) {
    auto name = make_object_name(thread_id, seq);
    KvRgwServiceImpl::DeleteResult dr;
    const auto ec =
        service.delete_object(tenant_id, bucket_name, name, nullptr, &dr);
    if (ec == KVRGW_ERR_OK) {
      result.ops.fetch_add(1, std::memory_order_relaxed);
    }
    else {
      result.record_error(ec);
      result.errors.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

void delete_multi_direct_worker(KvRgwServiceImpl &service,
                                tenant_id_t tenant_id,
                                const std::string &bucket_name, int thread_id,
                                int64_t seq_start, int64_t seq_end,
                                BenchResult &result)
{
  constexpr int batch = 10;
  for (int64_t seq = seq_start; seq < seq_end; seq += batch) {
    int64_t end = std::min(seq + batch, seq_end);
    std::vector<std::string> keys;
    keys.reserve(static_cast<size_t>(end - seq));
    for (int64_t s = seq; s < end; ++s) {
      keys.push_back(make_object_name(thread_id, s));
    }
    std::vector<KvRgwServiceImpl::DeleteMultiKeyOutcome> outcomes;
    const auto ec =
        service.delete_multi(tenant_id, bucket_name, keys, {}, &outcomes);
    result.ops.fetch_add(end - seq, std::memory_order_relaxed);
    int err_n = 0;
    if (ec != KVRGW_ERR_OK) {
      err_n = static_cast<int>(end - seq);
    }
    else {
      for (const auto &o : outcomes) {
        if (o.status ==
            KvRgwServiceImpl::DeleteMultiKeyOutcome::Status::Error) {
          ++err_n;
        }
      }
    }
    result.errors.fetch_add(err_n, std::memory_order_relaxed);
  }
}

// --- Command handlers ---
//--------------------------------------------------------------------------------
static std::vector<std::string> list_all_buckets(KvRgwServiceImpl &service,
                                                 tenant_id_t tenant_id)
{
  std::vector<std::string> buckets;
  std::string token;
  while (true) {
    KvRgwServiceImpl::ListBucketsResult out;
    const auto ec = service.list_buckets(tenant_id, {}, token, 1000, &out);
    for (const auto &b : out.buckets) {
      buckets.push_back(b.name);
    }
    if (ec != KVRGW_ERR_OK || out.continuation_token.empty()) {
      break;
    }
    token = out.continuation_token;
  }
  return buckets;
}

//--------------------------------------------------------------------------------
static std::vector<std::string> list_all_objects(KvRgwServiceImpl &service,
                                                 tenant_id_t tenant_id,
                                                 const std::string &bucket_name)
{
  std::vector<std::string> objects;
  std::string token;
  while (true) {
    KvRgwServiceImpl::ListObjectsResult out;
    const auto ec = service.list_objects(tenant_id, bucket_name, {}, {}, 1000,
                                         token, {}, &out);
    for (const auto &obj : out.objects) {
      objects.push_back(obj.key);
    }
    if (ec != KVRGW_ERR_OK || !out.is_truncated) {
      break;
    }
    token = out.next_continuation_token;
  }
  return objects;
}

//--------------------------------------------------------------------------------
static void cmd_create_buckets(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                               const ParsedParams &p)
{
  std::cout << "create-buckets: count=" << p.buckets << " mode=" << p.mode
            << "\n";
  int created = 0;
  for (int i = 0; i < p.buckets; ++i) {
    std::string bname = "perf-bucket-" + std::to_string(i);
    const auto ec = service.create_bucket(tenant_id, bname);
    if (ec != KVRGW_ERR_OK) {
      std::cerr << "  failed to create " << bname << ": " << kvrgw_strerror(ec)
                << "\n";
      continue;
    }
    if (p.mode == "versioned" || p.mode == "suspended") {
      const auto vs =
          (p.mode == "versioned") ? VERSIONING_ENABLED : VERSIONING_SUSPENDED;
      const auto vec = service.put_bucket_versioning(tenant_id, bname, vs);
      if (vec != KVRGW_ERR_OK) {
        std::cerr << "  failed to set versioning on " << bname << ": "
                  << kvrgw_strerror(vec) << "\n";
      }
    }
    ++created;
  }
  std::cout << "  created " << created << " buckets\n";
}

//--------------------------------------------------------------------------------
static void cmd_put(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                    const std::string &tenant_name, const ParsedParams &p)
{
  if (p.duration == 0) {
    std::cerr << "ERROR: specify duration=\n";
    return;
  }
  auto all_buckets = list_all_buckets(service, tenant_id);
  if (all_buckets.empty()) {
    std::cerr << "ERROR: no buckets found. Run create-buckets first.\n";
    return;
  }
  if (static_cast<int>(all_buckets.size()) > kMaxBuckets) {
    std::cerr << "ERROR: too many buckets (" << all_buckets.size() << " > "
              << kMaxBuckets << ")\n";
    return;
  }

  std::cout << "put: c=" << p.concurrency << " duration=" << p.duration
            << "s buckets=" << all_buckets.size() << " burst=" << p.burst
            << " tiers=[";
  for (size_t i = 0; i < p.tiers.size(); ++i) {
    if (i > 0) {
      std::cout << ",";
    }
    std::cout << tier_label(p.tiers[i]);
  }
  std::cout << "]\n";

  for (auto obj_size : p.tiers) {
    service.latency_stats().reset();
    service.store().fdb_put_stats().reset();
    std::atomic<bool> stop{false};
    BenchResult result;
    std::vector<std::thread> threads;
    std::vector<PutWorkerResult> worker_results(p.concurrency);

    auto t0 = std::chrono::steady_clock::now();
    for (int t = 0; t < p.concurrency; ++t) {
      threads.emplace_back(put_worker, std::ref(service), tenant_id,
                           std::cref(all_buckets), obj_size, t, p.burst,
                           p.progress_sec, t0, std::ref(stop),
                           std::ref(worker_results[t]), std::ref(result));
    }

    std::this_thread::sleep_for(std::chrono::seconds(p.duration));
    stop.store(true, std::memory_order_relaxed);
    for (auto &th : threads) {
      th.join();
    }

    // Aggregate per-thread local counters into BenchResult
    int64_t total_ops = 0;
    for (int t = 0; t < p.concurrency; ++t) {
      total_ops += worker_results[t].ops;
      result.ops.fetch_add(worker_results[t].ops, std::memory_order_relaxed);
      result.retries.fetch_add(worker_results[t].retries,
                               std::memory_order_relaxed);
      result.errors.fetch_add(worker_results[t].errors,
                              std::memory_order_relaxed);
    }

    auto t1 = std::chrono::steady_clock::now();
    double elapsed = static_cast<double>(p.duration);

    std::string label = "PUT " + tier_label(obj_size);
    print_results(label.c_str(), result, elapsed, service.latency_stats());
    int num_buckets = static_cast<int>(all_buckets.size());
    uint64_t global_max = 0;
    for (int t = 0; t < p.concurrency; ++t) {
      for (int b = 0; b < num_buckets; ++b) {
        if (worker_results[t].bucket_seq[b] > global_max) {
          global_max = worker_results[t].bucket_seq[b];
        }
      }
    }

    int64_t pad_total = 0;
    for (int t = 0; t < p.concurrency; ++t) {
      for (int b = 0; b < num_buckets; ++b) {
        pad_total +=
            static_cast<int64_t>(global_max - worker_results[t].bucket_seq[b]);
      }
    }

    if (pad_total > 0) {
      std::cout << "  padding: global_max_seq=" << global_max
                << " pad_objects=" << pad_total << "\n";
      std::vector<std::thread> pad_threads;
      for (int t = 0; t < p.concurrency; ++t) {
        pad_threads.emplace_back(put_pad_worker, std::ref(service), tenant_id,
                                 std::cref(all_buckets), obj_size, t,
                                 global_max, std::ref(worker_results[t]));
      }
      for (auto &th : pad_threads) {
        th.join();
      }
      std::cout << "  padding complete\n";
    }

    int64_t total_objects =
        static_cast<int64_t>(p.concurrency) * global_max * num_buckets;
    std::cout << "  total_objects=" << total_objects
              << " (threads=" << p.concurrency << " × seq=" << global_max
              << " × buckets=" << num_buckets << ")\n";
    print_fdb_put_stats(service.store());

    if (g_metadata_file) {
      std::ofstream mf(g_metadata_file);
      if (mf) {
        mf << "global_max_seq=" << global_max << "\n";
      }
    }
  }
}

static void put_multi_worker(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                             const std::vector<std::string> &bucket_names,
                             uint64_t obj_size, int thread_id,
                             std::atomic<bool> &stop, BenchResult &result)
{
  const int batch_size = 10;
  int num_buckets = static_cast<int>(bucket_names.size());
  uint64_t seq = 0;

  while (!stop.load(std::memory_order_relaxed)) {
    const auto &bucket_name = bucket_names[(seq / batch_size) % num_buckets];

    auto cached_bid = service.resolve_bucket_id(tenant_id, bucket_name);
    if (cached_bid == 0) {
      result.errors.fetch_add(batch_size, std::memory_order_relaxed);
      continue;
    }

    auto tr_result = service.store().begin_transaction();
    if (!tr_result) {
      result.errors.fetch_add(batch_size, std::memory_order_relaxed);
      continue;
    }
    auto &tr = *tr_result;

    bool ok = true;
    for (int i = 0; i < batch_size; ++i) {
      auto name = make_object_name(thread_id, seq++);
      auto ref_tag = service.ref_tags().next();
      auto ov = build_object_value(ref_tag, g_put_buffer, obj_size);

      if (obj_size <= service.tier_config_state().active_copy().max_inline) {
        ov.hdr.chunk.type = CHUNK_INLINE;
        ov.inline_data.assign(reinterpret_cast<const char *>(g_put_buffer),
                              reinterpret_cast<const char *>(g_put_buffer) +
                                  obj_size);
      }
      else {
        ov.hdr.chunk.type = CHUNK_CHILD_D;
      }

      std::string data(reinterpret_cast<const char *>(g_put_buffer), obj_size);
      KvRgwServiceImpl::PutInTxnParams params{
          tenant_id, bucket_name, cached_bid, name,    ref_tag, ov,
          &data,     nullptr,     g_put_tags, (i > 0), false};
      auto st = service.put_object_in_txn(*tr, params, nullptr);
      if (st != KVRGW_ERR_OK) {
        ok = false;
        break;
      }
    }

    if (ok) {
      auto rc = tr->commit();
      if (rc) {
        result.ops.fetch_add(batch_size, std::memory_order_relaxed);
      }
      else {
        result.errors.fetch_add(batch_size, std::memory_order_relaxed);
      }
    }
    else {
      result.errors.fetch_add(batch_size, std::memory_order_relaxed);
    }
  }
}

static void cmd_put_multi(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                          const std::string &tenant_name, const ParsedParams &p)
{
  auto all_buckets = list_all_buckets(service, tenant_id);
  if (all_buckets.empty()) {
    std::cerr << "ERROR: no buckets found. Run create-buckets first.\n";
    return;
  }
  std::cout << "put-multi: c=" << p.concurrency << " duration=" << p.duration
            << "s buckets=" << all_buckets.size() << " tiers=[";
  for (size_t i = 0; i < p.tiers.size(); ++i) {
    if (i > 0) {
      std::cout << ",";
    }
    std::cout << tier_label(p.tiers[i]);
  }
  std::cout << "] batch=10\n";

  for (auto obj_size : p.tiers) {
    service.latency_stats().reset();
    service.store().fdb_put_stats().reset();
    std::atomic<bool> stop{false};
    BenchResult result;
    std::vector<std::thread> threads;

    for (int t = 0; t < p.concurrency; ++t) {
      threads.emplace_back(put_multi_worker, std::ref(service), tenant_id,
                           std::cref(all_buckets), obj_size, t, std::ref(stop),
                           std::ref(result));
    }

    std::this_thread::sleep_for(std::chrono::seconds(p.duration));
    stop.store(true, std::memory_order_relaxed);
    for (auto &th : threads) {
      th.join();
    }

    std::string label = "PUT-MULTI " + tier_label(obj_size);
    print_results(label.c_str(), result, static_cast<double>(p.duration),
                  service.latency_stats());
    print_fdb_put_stats(service.store());
  }
}

static void cmd_get(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                    const std::string &tenant_name, const ParsedParams &p)
{
  auto all_buckets = list_all_buckets(service, tenant_id);
  if (all_buckets.empty()) {
    std::cerr << "ERROR: no buckets found.\n";
    return;
  }
  std::cout << "get: c=" << p.concurrency << " buckets=" << all_buckets.size()
            << "\n";

  for (const auto &bname : all_buckets) {
    auto objects = list_all_objects(service, tenant_id, bname);
    if (objects.empty()) {
      continue;
    }

    bucket_id_t bucket_id = service.resolve_bucket_id(tenant_id, bname);
    if (bucket_id == 0) {
      continue;
    }

    service.latency_stats().reset();
    BenchResult result;
    std::atomic<int64_t> index{0};
    std::vector<std::thread> threads;

    auto t0 = std::chrono::steady_clock::now();
    for (int t = 0; t < p.concurrency; ++t) {
      threads.emplace_back(get_worker, std::ref(service), tenant_id,
                           std::cref(bname), std::cref(objects), t,
                           std::ref(index), std::ref(result));
    }
    for (auto &th : threads) {
      th.join();
    }
    auto t1 = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();

    std::string label =
        "GET " + bname + " (" + std::to_string(objects.size()) + " objs)";
    print_results(label.c_str(), result, elapsed, service.latency_stats());
  }
}

struct GetRequest {
  int bidx = 0;
  char name[GET_OBJECT_NAME_MAX]{};
  uint32_t version_raw = 0;
  bool has_version = false;
  bool eof = false;
  std::chrono::steady_clock::time_point creation{};
  std::chrono::steady_clock::time_point serviced{};
};

struct GetTask {
  GetRequest req{};
  std::unique_ptr<KvTransaction> txn;
  FdbFuture future;
  bool occupied = false;
};

struct GetQueue {
  GetRequest items[GET_REQUESTS_QUEUE_SIZE]{};
  int head = 0;
  int tail = 0;
  std::mutex mu;
  std::counting_semaphore<GET_REQUESTS_QUEUE_SIZE> empty{
      GET_REQUESTS_QUEUE_SIZE};
  std::counting_semaphore<GET_REQUESTS_QUEUE_SIZE> filled{0};

  void push(const GetRequest &r, std::atomic<int64_t> *blocked_ns)
  {
    const auto w0 = std::chrono::steady_clock::now();
    empty.acquire();
    if (blocked_ns) {
      const auto w1 = std::chrono::steady_clock::now();
      blocked_ns->fetch_add(
          std::chrono::duration_cast<std::chrono::nanoseconds>(w1 - w0).count(),
          std::memory_order_relaxed);
    }
    {
      std::lock_guard<std::mutex> g(mu);
      items[tail] = r;
      tail = (tail + 1) % GET_REQUESTS_QUEUE_SIZE;
    }
    filled.release();
  }

  bool try_pop(GetRequest *out)
  {
    if (!out) {
      return false;
    }
    if (!filled.try_acquire()) {
      return false;
    }
    {
      std::lock_guard<std::mutex> g(mu);
      *out = items[head];
      head = (head + 1) % GET_REQUESTS_QUEUE_SIZE;
    }
    empty.release();
    out->serviced = std::chrono::steady_clock::now();
    return true;
  }

  GetRequest pop()
  {
    filled.acquire();
    GetRequest r;
    {
      std::lock_guard<std::mutex> g(mu);
      r = items[head];
      head = (head + 1) % GET_REQUESTS_QUEUE_SIZE;
    }
    empty.release();
    r.serviced = std::chrono::steady_clock::now();
    return r;
  }
};

static void get_producer_thread_range(int producer_id, int nprod, int nthreads,
                                      int *t0, int *t1)
{
  if (!t0 || !t1) {
    return;
  }
  if (producer_id < 0 || nprod <= 0 || nthreads < 0 || producer_id >= nprod) {
    *t0 = 0;
    *t1 = 0;
    return;
  }
  *t0 =
      static_cast<int>((static_cast<int64_t>(producer_id) * nthreads) / nprod);
  *t1 = static_cast<int>((static_cast<int64_t>(producer_id + 1) * nthreads) /
                         nprod);
}

static bool get_enqueue_one(GetQueue &q, GetRequest r, std::atomic<bool> *stop,
                            std::atomic<int64_t> *remaining,
                            std::atomic<int64_t> *blocked_ns)
{
  if (stop && stop->load(std::memory_order_relaxed)) {
    return false;
  }
  if (remaining) {
    const int64_t ticket = remaining->fetch_sub(1, std::memory_order_relaxed);
    if (ticket <= 0) {
      return false;
    }
  }
  r.creation = std::chrono::steady_clock::now();
  r.eof = false;
  q.push(r, blocked_ns);
  return true;
}

static bool get_enqueue_key(GetQueue &q, int bidx, int thread_id, uint64_t seq,
                            bool all_versions, uint32_t latest_vid,
                            int64_t nver, std::atomic<bool> *stop,
                            std::atomic<int64_t> *remaining,
                            std::atomic<int64_t> *blocked_ns)
{
  GetRequest r{};
  r.bidx = bidx;
  const auto name = make_object_name(thread_id, seq);
  if (name.size() >= sizeof(r.name)) {
    std::cerr << "GET_ERR object name too long\n";
    return false;
  }
  std::snprintf(r.name, sizeof(r.name), "%s", name.c_str());
  r.has_version = false;
  if (!get_enqueue_one(q, r, stop, remaining, blocked_ns)) {
    return false;
  }
  if (!all_versions || nver <= 1) {
    return true;
  }
  for (int64_t ver_i = 1; ver_i < nver; ++ver_i) {
    r.has_version = true;
    r.version_raw = latest_vid + static_cast<uint32_t>(ver_i);
    if (!get_enqueue_one(q, r, stop, remaining, blocked_ns)) {
      return false;
    }
  }
  return true;
}

static void get_complete_task(GetTask &task, KvRgwServiceImpl &service,
                              BenchResult &result,
                              std::atomic<int64_t> &missing)
{
  const auto now = std::chrono::steady_clock::now();
  const int64_t queue_us =
      std::chrono::duration_cast<std::chrono::microseconds>(task.req.serviced -
                                                            task.req.creation)
          .count();
  const int64_t fdb_us = std::chrono::duration_cast<std::chrono::microseconds>(
                             now - task.req.serviced)
                             .count();
  int64_t total_us = queue_us + fdb_us;
  if (total_us < 0) {
    total_us = 0;
  }

  auto raw = task.txn->kv_wait_get(task.future);
  RequestLatency rl{};
  rl.fdb_get_us = fdb_us > 0 ? fdb_us : 0;
  rl.fdb_get_count = 1;
  service.latency_stats().record(OpType::kGetObject, total_us, rl);
  service.ops_stats().inc(OpType::kGetObject);

  if (!raw) {
    const auto ec = fdb_to_error(raw.error());
    result.record_error(ec);
    result.errors.fetch_add(1, std::memory_order_relaxed);
  }
  else if (!*raw) {
    missing.fetch_add(1, std::memory_order_relaxed);
  }
  else {
    auto obj = parse_object_value(**raw);
    if (!obj) {
      result.record_error(KVRGW_ERR_CORRUPT_VALUE);
      result.errors.fetch_add(1, std::memory_order_relaxed);
    }
    else if (obj->is_delete_marker()) {
      missing.fetch_add(1, std::memory_order_relaxed);
    }
    else {
      result.ops.fetch_add(1, std::memory_order_relaxed);
    }
  }
  task.future = FdbFuture();
  task.txn.reset();
  task.occupied = false;
}

static bool get_issue_task(GetTask &task, const GetRequest &req,
                           KvRgwServiceImpl &service,
                           const std::vector<bucket_id_t> &bucket_ids,
                           BenchResult &result)
{
  if (req.bidx < 0 || req.bidx >= static_cast<int>(bucket_ids.size())) {
    std::cerr << "GET_ERR bidx out of range\n";
    result.errors.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  const bucket_id_t bucket_id = bucket_ids[static_cast<size_t>(req.bidx)];
  auto tr_res = service.store().begin_transaction();
  if (!tr_res) {
    result.record_error(fdb_to_error(tr_res.error()));
    result.errors.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  task.req = req;
  task.txn = std::move(*tr_res);
  KeyBuf key = req.has_version ? make_v_key(bucket_id, req.name,
                                            version_id_t{req.version_raw})
                               : make_object_key(bucket_id, req.name);
  task.future = task.txn->kv_async_get(key.view());
  task.occupied = true;
  return true;
}

static int get_find_free_slot(GetTask *array, unsigned max_futures)
{
  if (!array) {
    return -1;
  }
  for (unsigned i = 0; i < max_futures; ++i) {
    if (!array[i].occupied) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

static void get_test_producer(
    GetQueue &q, int producer_id, int nprod, int nconsumers, int nthreads,
    int num_buckets, uint64_t seq_end, bool overwrite, int files_per_thread,
    bool all_versions, uint32_t latest_vid, int64_t nver,
    std::atomic<bool> *stop, std::atomic<int64_t> *remaining,
    std::atomic<int> &producers_left, std::atomic<int64_t> &blocked_ns,
    std::atomic<int64_t> &elapsed_ns)
{
  const auto t0 = std::chrono::steady_clock::now();
  int t0_id = 0;
  int t1_id = 0;
  get_producer_thread_range(producer_id, nprod, nthreads, &t0_id, &t1_id);

  auto one_pass = [&]() -> bool {
    if (overwrite) {
      for (int thread_id = t0_id; thread_id < t1_id; ++thread_id) {
        for (int j = 0; j < files_per_thread; ++j) {
          const int file_id = thread_id * files_per_thread + j;
          const int bidx = file_id % num_buckets;
          if (!get_enqueue_key(q, bidx, thread_id, static_cast<uint64_t>(j),
                               all_versions, latest_vid, nver, stop, remaining,
                               &blocked_ns)) {
            return false;
          }
        }
      }
    }
    else {
      for (int thread_id = t0_id; thread_id < t1_id; ++thread_id) {
        for (int b = 0; b < num_buckets; ++b) {
          for (uint64_t seq = 0; seq < seq_end; ++seq) {
            if (!get_enqueue_key(q, b, thread_id, seq, all_versions, latest_vid,
                                 nver, stop, remaining, &blocked_ns)) {
              return false;
            }
          }
        }
      }
    }
    return true;
  };

  if (stop) {
    while (!stop->load(std::memory_order_relaxed)) {
      if (!one_pass()) {
        break;
      }
    }
  }
  else {
    while (one_pass()) {
      if (!remaining) {
        break;
      }
    }
  }

  if (producers_left.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    GetRequest eof{};
    eof.eof = true;
    eof.creation = std::chrono::steady_clock::now();
    for (int i = 0; i < nconsumers; ++i) {
      q.push(eof, &blocked_ns);
    }
  }

  const auto t1 = std::chrono::steady_clock::now();
  elapsed_ns.fetch_add(
      std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count(),
      std::memory_order_relaxed);
}

static void get_test_consumer(KvRgwServiceImpl &service, int consumer_id,
                              unsigned max_futures, GetQueue &q,
                              const std::vector<bucket_id_t> &bucket_ids,
                              int progress_sec,
                              std::chrono::steady_clock::time_point t0,
                              BenchResult &result,
                              std::atomic<int64_t> &missing)
{
  GetTask array[max_futures];
  unsigned free_count = max_futures;
  bool eof = false;
  auto last_t = t0;
  int64_t last_count = 0;
  int64_t last_total_us = 0;

  while (true) {
    for (unsigned i = 0; i < max_futures; ++i) {
      if (!array[i].occupied) {
        continue;
      }
      if (!array[i].future.is_ready()) {
        continue;
      }
      get_complete_task(array[i], service, result, missing);
      ++free_count;
    }
    progress_tick(consumer_id, progress_sec, OpType::kGetObject,
                  service.latency_stats(), t0, last_t, last_count,
                  last_total_us);

    if (eof) {
      if (free_count == max_futures) {
        return;
      }
      std::this_thread::sleep_for(GET_IDLE_SLEEP);
      continue;
    }

    while (free_count > 0) {
      GetRequest r;
      if (!q.try_pop(&r)) {
        break;
      }
      if (r.eof) {
        eof = true;
        break;
      }
      const int slot = get_find_free_slot(array, max_futures);
      if (slot < 0) {
        std::cerr << "GET_ERR no free slot with free_count=" << free_count
                  << "\n";
        break;
      }
      if (get_issue_task(array[slot], r, service, bucket_ids, result)) {
        --free_count;
      }
    }

    if (eof) {
      continue;
    }
    if (free_count == 0) {
      std::this_thread::sleep_for(GET_IDLE_SLEEP);
      continue;
    }
    if (free_count < max_futures) {
      std::this_thread::sleep_for(GET_IDLE_SLEEP);
      continue;
    }

    GetRequest r = q.pop();
    if (r.eof) {
      eof = true;
      continue;
    }
    const int slot = get_find_free_slot(array, max_futures);
    if (slot < 0) {
      std::cerr << "GET_ERR no free slot after blocking pop\n";
      continue;
    }
    if (get_issue_task(array[slot], r, service, bucket_ids, result)) {
      --free_count;
    }
  }
}

//------------------------------------------------------------------------------
static void cmd_get_test(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                         const ParsedParams &p, const std::string &opts)
{
  if (!g_metadata_file) {
    std::cerr << "GET_ERR KVRGW_METADATA_FILE not set\n";
    return;
  }
  if (p.producers <= 0) {
    std::cerr << "GET_ERR producers= must be > 0\n";
    return;
  }
  if (p.consumers <= 0) {
    std::cerr << "GET_ERR consumers= must be > 0\n";
    return;
  }
  if (p.max_futures <= 0 || p.max_futures > GET_FUTURE_MAX) {
    std::cerr << "GET_ERR max_futures= must be in 1.." << GET_FUTURE_MAX
              << "\n";
    return;
  }

  int threads = 0, instances = 0, meta_buckets = 0;
  uint64_t max_seq_arr[128]{};
  std::string src_workload;
  int64_t base_files = 0;
  int64_t overwrite_count = 0;
  {
    std::ifstream mf(g_metadata_file);
    if (!mf) {
      std::cerr << "GET_ERR cannot open " << g_metadata_file << "\n";
      return;
    }
    std::string line;
    while (std::getline(mf, line)) {
      if (line.rfind("threads=", 0) == 0) {
        threads = std::atoi(line.c_str() + 8);
      }
      else if (line.rfind("instances=", 0) == 0) {
        instances = std::atoi(line.c_str() + 10);
      }
      else if (line.rfind("buckets=", 0) == 0) {
        meta_buckets = std::atoi(line.c_str() + 8);
      }
      else if (line.rfind("workload=", 0) == 0) {
        src_workload = line.substr(9);
      }
      else if (line.rfind("base_files=", 0) == 0) {
        base_files = std::atoll(line.c_str() + 11);
      }
      else if (line.rfind("overwrite_count=", 0) == 0) {
        overwrite_count = std::atoll(line.c_str() + 16);
      }
      else if (line.rfind("max_seq_", 0) == 0) {
        auto eq = line.find('=');
        if (eq != std::string::npos) {
          int idx = std::atoi(line.c_str() + 8);
          if (idx >= 0 && idx < 128) {
            max_seq_arr[idx] = std::stoull(line.substr(eq + 1));
          }
        }
      }
    }
  }
  if (threads <= 0 || instances <= 0 || meta_buckets <= 0) {
    std::cerr << "GET_ERR invalid metadata (threads=" << threads
              << " instances=" << instances << " buckets=" << meta_buckets
              << ")\n";
    return;
  }
  if (g_instance_id < 0 || g_instance_id >= instances || g_instance_id >= 128) {
    std::cerr << "GET_ERR instance_id=" << g_instance_id
              << " out of range (instances=" << instances << ")\n";
    return;
  }

  bool all_versions = false;
  {
    std::istringstream iss(opts);
    std::string token;
    while (iss >> token) {
      if (token == "--all-versions") {
        all_versions = true;
      }
    }
  }

  const bool overwrite = (src_workload == "put-overwrite");
  int files_per_thread = 0;
  uint64_t seq_end = max_seq_arr[g_instance_id];
  if (overwrite) {
    if (base_files <= 0) {
      std::cerr << "GET_ERR put-overwrite source requires base_files\n";
      return;
    }
    if (base_files % threads != 0 || base_files % meta_buckets != 0) {
      std::cerr << "GET_ERR base_files not divisible by threads/buckets\n";
      return;
    }
    files_per_thread = static_cast<int>(base_files / threads);
  }
  else if (seq_end == 0) {
    std::cerr << "GET_ERR max_seq_" << g_instance_id
              << " missing or zero in metadata\n";
    return;
  }

  int64_t nver = 1;
  uint32_t latest_vid = kFirstVersionId.raw();
  if (all_versions && overwrite) {
    if (overwrite_count < 0) {
      std::cerr
          << "GET_ERR --all-versions requires overwrite_count in metadata\n";
      return;
    }
    if (overwrite_count > static_cast<int64_t>(kFirstVersionId.raw())) {
      std::cerr << "GET_ERR overwrite_count too large\n";
      return;
    }
    nver = overwrite_count + 1;
    latest_vid = kFirstVersionId.raw() - static_cast<uint32_t>(overwrite_count);
  }

  const int64_t expected = overwrite
                               ? static_cast<int64_t>(files_per_thread) * nver
                               : static_cast<int64_t>(meta_buckets) *
                                     static_cast<int64_t>(seq_end) * nver;
  const int64_t expected_inst = expected * threads;

  std::vector<bucket_id_t> bucket_ids;
  bucket_ids.reserve(static_cast<size_t>(meta_buckets));
  for (int b = 0; b < meta_buckets; ++b) {
    char bname[AWS_MAX_BUCKET_NAME];
    std::snprintf(bname, sizeof(bname), "perf-bucket-%d", b);
    bool exists = false;
    bucket_id_t bid = 0;
    const auto ec =
        service.bucket_exists_cached(tenant_id, bname, &exists, &bid);
    if (ec != KVRGW_ERR_OK || !exists) {
      std::cerr << "GET_ERR bucket " << bname << " missing\n";
      return;
    }
    bucket_ids.push_back(bid);
  }

  std::cout << "get-test: prefix=" << g_key_prefix
            << " instance=" << g_instance_id << " producers=" << p.producers
            << " consumers=" << p.consumers << " max_futures=" << p.max_futures
            << " put_threads=" << threads << " buckets=" << meta_buckets;
  if (overwrite) {
    std::cout << " workload=put-overwrite files/thread=" << files_per_thread;
  }
  else {
    std::cout << " max_seq=" << seq_end;
  }
  if (all_versions) {
    std::cout << " --all-versions nver=" << nver;
  }
  if (p.duration > 0) {
    std::cout << " duration=" << p.duration;
  }
  if (p.count > 0) {
    std::cout << " count=" << p.count;
  }
  std::cout << "\n";

  service.latency_stats().reset();
  BenchResult result;
  std::atomic<int64_t> missing{0};
  std::atomic<bool> stop_flag{false};
  std::atomic<int64_t> remaining{p.count};
  std::atomic<bool> *stop = (p.duration > 0) ? &stop_flag : nullptr;
  std::atomic<int64_t> *rem = (p.count > 0) ? &remaining : nullptr;
  std::atomic<int> producers_left{p.producers};
  std::atomic<int64_t> blocked_ns{0};
  std::atomic<int64_t> producer_elapsed_ns{0};
  GetQueue queue;

  std::vector<std::thread> threads_v;
  auto t0 = std::chrono::steady_clock::now();
  for (int t = 0; t < p.producers; ++t) {
    threads_v.emplace_back(
        get_test_producer, std::ref(queue), t, p.producers, p.consumers,
        threads, meta_buckets, seq_end, overwrite, files_per_thread,
        all_versions, latest_vid, nver, stop, rem, std::ref(producers_left),
        std::ref(blocked_ns), std::ref(producer_elapsed_ns));
  }
  for (int t = 0; t < p.consumers; ++t) {
    threads_v.emplace_back(get_test_consumer, std::ref(service), t,
                           static_cast<unsigned>(p.max_futures),
                           std::ref(queue), std::cref(bucket_ids),
                           p.progress_sec, t0, std::ref(result),
                           std::ref(missing));
  }
  if (p.duration > 0) {
    std::this_thread::sleep_for(std::chrono::seconds(p.duration));
    stop_flag.store(true, std::memory_order_relaxed);
  }
  for (auto &th : threads_v) {
    th.join();
  }
  auto t1 = std::chrono::steady_clock::now();
  double elapsed = std::chrono::duration<double>(t1 - t0).count();

  print_results("GET-TEST", result, elapsed, service.latency_stats());
  const int64_t miss = missing.load();
  const int64_t hits = result.ops.load();
  const int64_t errs = result.errors.load();
  const int64_t bns = blocked_ns.load();
  const int64_t ens = producer_elapsed_ns.load();
  const double blocked_pct =
      (ens > 0) ? (100.0 * static_cast<double>(bns) / static_cast<double>(ens))
                : 0.0;
  std::cout << "  producers blocked_full_pct=" << std::fixed
            << std::setprecision(1) << blocked_pct << "\n";
  std::cout << "get-test complete expected=" << expected_inst
            << " hits=" << hits << " missing=" << miss << " errors=" << errs
            << "\n";
  if (miss > 0) {
    std::cerr << "GET_ERR missing=" << miss << " expected=" << expected_inst
              << " hits=" << hits << "\n";
  }
  if (p.duration == 0 && p.count == 0 && hits != expected_inst) {
    std::cerr << "GET_ERR hits=" << hits << " != expected=" << expected_inst
              << "\n";
  }
}

static void cmd_delete(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                       const std::string &tenant_name, const ParsedParams &p)
{
  if (p.count > 0) {
    std::string bname = "perf-bucket-0";
    std::cout << "delete: c=" << p.concurrency << " count=" << p.count
              << " bucket=" << bname << " (direct)\n";

    service.latency_stats().reset();
    BenchResult result;
    std::vector<std::thread> threads;

    int64_t per_thread = p.count / p.concurrency;
    int64_t remainder = p.count % p.concurrency;

    auto t0 = std::chrono::steady_clock::now();
    for (int t = 0; t < p.concurrency; ++t) {
      int64_t count_for_thread = per_thread + (t < remainder ? 1 : 0);
      int64_t seq_start = 0;
      int64_t seq_end = count_for_thread;
      threads.emplace_back(delete_direct_worker, std::ref(service), tenant_id,
                           std::cref(bname), t, seq_start, seq_end,
                           std::ref(result));
    }
    for (auto &th : threads) {
      th.join();
    }
    auto t1 = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();

    std::string label =
        "DELETE " + bname + " (" + std::to_string(p.count) + " objs, direct)";
    print_results(label.c_str(), result, elapsed, service.latency_stats());
    return;
  }

  auto all_buckets = list_all_buckets(service, tenant_id);
  if (all_buckets.empty()) {
    std::cerr << "ERROR: no buckets found.\n";
    return;
  }
  std::cout << "delete: c=" << p.concurrency
            << " buckets=" << all_buckets.size() << "\n";

  for (const auto &bname : all_buckets) {
    auto objects = list_all_objects(service, tenant_id, bname);
    if (objects.empty()) {
      continue;
    }

    service.latency_stats().reset();
    BenchResult result;
    std::atomic<int64_t> index{0};
    std::vector<std::thread> threads;

    auto t0 = std::chrono::steady_clock::now();
    for (int t = 0; t < p.concurrency; ++t) {
      threads.emplace_back(delete_worker, std::ref(service), tenant_id,
                           std::cref(bname), std::cref(objects), t,
                           std::ref(index), std::ref(result));
    }
    for (auto &th : threads) {
      th.join();
    }
    auto t1 = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();

    std::string label =
        "DELETE " + bname + " (" + std::to_string(objects.size()) + " objs)";
    print_results(label.c_str(), result, elapsed, service.latency_stats());
  }
}

static void delete_multi_worker(KvRgwServiceImpl &service,
                                tenant_id_t tenant_id,
                                const std::string &bucket_name,
                                const std::vector<std::string> &objects,
                                std::atomic<int64_t> &index,
                                BenchResult &result)
{
  const int batch_size = 10;
  int total = static_cast<int>(objects.size());
  while (true) {
    int64_t start = index.fetch_add(batch_size, std::memory_order_relaxed);
    if (start >= total) {
      break;
    }
    int64_t end = std::min(start + batch_size, static_cast<int64_t>(total));

    std::vector<std::string> keys(objects.begin() + start,
                                  objects.begin() + end);
    std::vector<KvRgwServiceImpl::DeleteMultiKeyOutcome> outcomes;
    const auto ec =
        service.delete_multi(tenant_id, bucket_name, keys, {}, &outcomes);
    result.ops.fetch_add(end - start, std::memory_order_relaxed);
    int err_n = 0;
    if (ec != KVRGW_ERR_OK) {
      err_n = static_cast<int>(end - start);
    }
    else {
      for (const auto &o : outcomes) {
        if (o.status ==
            KvRgwServiceImpl::DeleteMultiKeyOutcome::Status::Error) {
          ++err_n;
        }
      }
    }
    result.errors.fetch_add(err_n, std::memory_order_relaxed);
  }
}

static void cmd_delete_multi(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                             const std::string &tenant_name,
                             const ParsedParams &p)
{
  if (p.count > 0) {
    std::string bname = "perf-bucket-0";
    std::cout << "delete-multi: c=" << p.concurrency << " count=" << p.count
              << " bucket=" << bname << " (direct)\n";

    service.latency_stats().reset();
    BenchResult result;
    std::vector<std::thread> threads;

    int64_t per_thread = p.count / p.concurrency;
    int64_t remainder = p.count % p.concurrency;

    auto t0 = std::chrono::steady_clock::now();
    for (int t = 0; t < p.concurrency; ++t) {
      int64_t count_for_thread = per_thread + (t < remainder ? 1 : 0);
      int64_t seq_start = 0;
      int64_t seq_end = count_for_thread;
      threads.emplace_back(delete_multi_direct_worker, std::ref(service),
                           tenant_id, std::cref(bname), t, seq_start, seq_end,
                           std::ref(result));
    }
    for (auto &th : threads) {
      th.join();
    }
    auto t1 = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();

    std::string label = "DELETE-MULTI " + bname + " (" +
                        std::to_string(p.count) + " objs, direct)";
    print_results(label.c_str(), result, elapsed, service.latency_stats());
    return;
  }

  auto all_buckets = list_all_buckets(service, tenant_id);
  if (all_buckets.empty()) {
    std::cerr << "ERROR: no buckets found.\n";
    return;
  }
  std::cout << "delete-multi: c=" << p.concurrency
            << " buckets=" << all_buckets.size() << "\n";

  for (const auto &bname : all_buckets) {
    auto objects = list_all_objects(service, tenant_id, bname);
    if (objects.empty()) {
      continue;
    }

    service.latency_stats().reset();
    BenchResult result;
    std::atomic<int64_t> index{0};
    std::vector<std::thread> threads;

    auto t0 = std::chrono::steady_clock::now();
    for (int t = 0; t < p.concurrency; ++t) {
      threads.emplace_back(delete_multi_worker, std::ref(service), tenant_id,
                           std::cref(bname), std::cref(objects),
                           std::ref(index), std::ref(result));
    }
    for (auto &th : threads) {
      th.join();
    }
    auto t1 = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();

    std::string label = "DELETE-MULTI " + bname + " (" +
                        std::to_string(objects.size()) + " objs)";
    print_results(label.c_str(), result, elapsed, service.latency_stats());
  }
}

static void cmd_delete_buckets(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                               const std::string &tenant_name,
                               const ParsedParams &p)
{
  auto all_buckets = list_all_buckets(service, tenant_id);
  std::cout << "delete-buckets: " << all_buckets.size() << " buckets\n";
  int deleted = 0;
  for (const auto &bname : all_buckets) {
    const auto ec = service.delete_bucket(tenant_id, bname);
    if (ec != KVRGW_ERR_OK) {
      std::cerr << "  failed to delete " << bname << ": " << kvrgw_strerror(ec)
                << "\n";
      continue;
    }
    ++deleted;
  }
  std::cout << "  deleted " << deleted << "/" << all_buckets.size() << "\n";
}

static void cmd_list_buckets(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                             const ParsedParams &p)
{
  service.latency_stats().reset();
  auto t0 = std::chrono::steady_clock::now();
  auto buckets = list_all_buckets(service, tenant_id);
  auto t1 = std::chrono::steady_clock::now();
  double elapsed = std::chrono::duration<double>(t1 - t0).count();
  std::cout << "list-buckets: " << buckets.size() << " buckets in "
            << std::fixed << std::setprecision(3) << elapsed << "s\n";
}

static void cmd_list_objects(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                             const ParsedParams &p)
{
  auto all_buckets = list_all_buckets(service, tenant_id);
  if (all_buckets.empty()) {
    std::cerr << "ERROR: no buckets found.\n";
    return;
  }
  std::cout << "list-objects: buckets=" << all_buckets.size() << "\n";

  for (const auto &bname : all_buckets) {
    auto t0 = std::chrono::steady_clock::now();
    auto objects = list_all_objects(service, tenant_id, bname);
    auto t1 = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();
    std::cout << "  " << bname << ": " << objects.size() << " objects in "
              << std::fixed << std::setprecision(3) << elapsed << "s\n";
  }
}

// --- Prepare helper ---

struct ThreadRange {
  std::string bucket_name;
  bucket_id_t bucket_id{};
  std::vector<std::string> keys;
};

static std::vector<ThreadRange>
prepare_thread_ranges(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                      const std::string &tenant_name, int concurrency)
{
  auto all_buckets = list_all_buckets(service, tenant_id);
  if (all_buckets.empty()) {
    std::cerr << "ERROR: no buckets found.\n";
    return {};
  }
  const auto &bname = all_buckets[0];
  auto objects = list_all_objects(service, tenant_id, bname);
  if (objects.empty()) {
    std::cerr << "ERROR: no objects in " << bname << ".\n";
    return {};
  }
  std::sort(objects.begin(), objects.end());

  bucket_id_t bucket_id = service.resolve_bucket_id(tenant_id, bname);
  if (bucket_id == 0) {
    std::cerr << "ERROR: cannot resolve bucket_id for " << bname << ".\n";
    return {};
  }

  std::vector<ThreadRange> ranges(concurrency);
  for (size_t i = 0; i < objects.size(); ++i) {
    int tid = static_cast<int>(i % concurrency);
    ranges[tid].keys.push_back(objects[i]);
  }
  for (auto &r : ranges) {
    r.bucket_name = bname;
    r.bucket_id = bucket_id;
  }

  std::cout << "  prepared " << objects.size() << " objects across "
            << concurrency << " threads\n";
  return ranges;
}

// --- New workers ---

void copy_worker(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                 const ThreadRange &range, BenchResult &result)
{
  for (const auto &key : range.keys) {
    ScopedRequestLatency _lat(service.latency_stats_, OpType::kCopyObject);
    std::string dst_key = "COPY_" + key.substr(4);
    KvRgwServiceImpl::CopyObjectRequest req;
    req.tenant_id = tenant_id;
    req.src_bucket_name = range.bucket_name;
    req.src_key = key;
    req.dst_bucket_name = range.bucket_name;
    req.dst_key = dst_key;
    KvRgwServiceImpl::CopyObjectResult out;
    const auto ec = service.copy_object(req, &out);
    if (ec == KVRGW_ERR_OK) {
      result.ops.fetch_add(1, std::memory_order_relaxed);
    }
    else {
      result.errors.fetch_add(1, std::memory_order_relaxed);
      result.record_error(ec);
    }
  }
}

void put_overwrite_standalone_worker(KvRgwServiceImpl &service,
                                     tenant_id_t tenant_id, uint64_t obj_size,
                                     int thread_id, int files_per_thread,
                                     int64_t overwrite_count, int num_buckets,
                                     int progress_sec,
                                     std::chrono::steady_clock::time_point t0,
                                     BenchResult &result)
{
  char bname[AWS_MAX_BUCKET_NAME];
  auto last_t = t0;
  int64_t last_count = 0;
  int64_t last_total_us = 0;
  for (int64_t i = 0; i <= overwrite_count; ++i) {
    for (int j = 0; j < files_per_thread; ++j) {
      const int file_id = thread_id * files_per_thread + j;
      const int bidx = file_id % num_buckets;
      std::snprintf(bname, sizeof(bname), "perf-bucket-%d", bidx);
      const auto name = make_object_name(thread_id, static_cast<uint64_t>(j));
      const auto ref_tag = service.ref_tags().next();
      auto ov = build_object_value(ref_tag, g_put_buffer, obj_size);

      ScopedRequestLatency _lat(service.latency_stats(), OpType::kPutObject);

      KvRgwServiceImpl::PutObjectRequest req;
      req.tenant_id = tenant_id;
      req.bucket_name = bname;
      req.object_name = name;
      req.ref_tag = ref_tag;
      req.value = std::move(ov);
      req.estimated_size = obj_size;
      req.cond = nullptr;
      req.tags = g_put_tags;

      auto res = service.put_object_route(req, g_put_buffer, obj_size);
      if (res.error_code == KVRGW_ERR_OK) {
        result.ops.fetch_add(1, std::memory_order_relaxed);
      }
      else {
        result.errors.fetch_add(1, std::memory_order_relaxed);
        result.record_error(res.error_code);
        std::cerr << "PUT_ERR t=" << thread_id << " key=" << name
                  << " bucket=" << bname << " code=" << res.error_code
                  << " msg=" << kvrgw_strerror(res.error_code) << "\n";
      }
      progress_tick(thread_id, progress_sec, OpType::kPutObject,
                    service.latency_stats(), t0, last_t, last_count,
                    last_total_us);
    }
  }
}

void put_overwrite_worker(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                          const ThreadRange &range, uint64_t obj_size,
                          BenchResult &result)
{
  for (const auto &key : range.keys) {
    ScopedRequestLatency _lat(service.latency_stats_, OpType::kPutObject);
    const auto ref_tag = service.ref_tags_.next();
    auto ov = build_object_value(ref_tag, g_put_buffer, obj_size);

    KvRgwServiceImpl::PutObjectRequest req;
    req.tenant_id = tenant_id;
    req.bucket_name = range.bucket_name;
    req.object_name = key;
    req.ref_tag = ref_tag;
    req.value = std::move(ov);
    req.estimated_size = obj_size;
    req.cond = nullptr;
    req.tags = g_put_tags;

    auto res = service.put_object_route(req, g_put_buffer, obj_size);
    if (res.error_code == KVRGW_ERR_OK) {
      result.ops.fetch_add(1, std::memory_order_relaxed);
    }
    else {
      result.errors.fetch_add(1, std::memory_order_relaxed);
      result.record_error(res.error_code);
    }
  }
}

void put_overwrite_versioned_worker(KvRgwServiceImpl &service,
                                    tenant_id_t tenant_id,
                                    const ThreadRange &range, uint64_t obj_size,
                                    int versions, BenchResult &result)
{
  for (int v = 0; v < versions; ++v) {
    for (const auto &key : range.keys) {
      ScopedRequestLatency _lat(service.latency_stats_, OpType::kPutObject);
      const auto ref_tag = service.ref_tags_.next();
      auto ov = build_object_value(ref_tag, g_put_buffer, obj_size);

      KvRgwServiceImpl::PutObjectRequest req;
      req.tenant_id = tenant_id;
      req.bucket_name = range.bucket_name;
      req.object_name = key;
      req.ref_tag = ref_tag;
      req.value = std::move(ov);
      req.estimated_size = obj_size;
      req.cond = nullptr;
      req.tags = g_put_tags;

      auto res = service.put_object_route(req, g_put_buffer, obj_size);
      if (res.error_code == KVRGW_ERR_OK) {
        result.ops.fetch_add(1, std::memory_order_relaxed);
      }
      else {
        result.errors.fetch_add(1, std::memory_order_relaxed);
        result.record_error(res.error_code);
      }
    }
  }
}

void delete_version_worker(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                           const ThreadRange &range, int versions,
                           int thread_id, BenchResult &result)
{
  std::mt19937 rng(static_cast<uint32_t>(thread_id));
  std::uniform_int_distribution<int> dist(0, versions - 1);
  for (const auto &key : range.keys) {
    ScopedRequestLatency _lat(service.latency_stats_,
                              OpType::kDeleteObjectVersion);
    int r = dist(rng);
    version_id_t vid{0xFFFFFFFE - static_cast<uint32_t>(r)};
    const auto ec = service.delete_object_version(tenant_id, range.bucket_name,
                                                  key, vid, nullptr);
    if (ec == KVRGW_ERR_OK) {
      result.ops.fetch_add(1, std::memory_order_relaxed);
    }
    else {
      result.errors.fetch_add(1, std::memory_order_relaxed);
      result.record_error(ec);
    }
  }
}

// --- New command handlers ---

static void cmd_copy(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                     const std::string &tenant_name, const ParsedParams &p)
{
  std::cout << "copy: c=" << p.concurrency << "\n";
  auto ranges =
      prepare_thread_ranges(service, tenant_id, tenant_name, p.concurrency);
  if (ranges.empty()) {
    return;
  }

  service.latency_stats().reset();
  BenchResult result;
  std::vector<std::thread> threads;

  auto t0 = std::chrono::steady_clock::now();
  for (int t = 0; t < p.concurrency; ++t) {
    threads.emplace_back(copy_worker, std::ref(service), tenant_id,
                         std::cref(ranges[t]), std::ref(result));
  }
  for (auto &th : threads) {
    th.join();
  }
  auto t1 = std::chrono::steady_clock::now();
  double elapsed = std::chrono::duration<double>(t1 - t0).count();

  int64_t total = 0;
  for (const auto &r : ranges) {
    total += r.keys.size();
  }
  std::string label = "COPY (" + std::to_string(total) + " objs)";
  print_results(label.c_str(), result, elapsed, service.latency_stats());
}

static int parse_perf_bucket_index(std::string_view name)
{
  static constexpr char kPrefix[] = "perf-bucket-";
  static constexpr size_t kPrefixLen = sizeof(kPrefix) - 1;
  if (name.size() <= kPrefixLen || name.compare(0, kPrefixLen, kPrefix) != 0) {
    return -1;
  }
  return std::atoi(name.data() + kPrefixLen);
}

static void cmd_put_overwrite_standalone(KvRgwServiceImpl &service,
                                         tenant_id_t tenant_id,
                                         const ParsedParams &p)
{
  if (p.concurrency <= 0) {
    std::cerr << "PUT_ERR put-overwrite requires c=N (N > 0)\n";
    return;
  }
  if (p.base_files <= 0) {
    std::cerr
        << "PUT_ERR put-overwrite standalone requires base_files=N (N > 0)\n";
    return;
  }
  if (p.overwrite_count < 0) {
    std::cerr
        << "PUT_ERR put-overwrite standalone requires overwrite_count=N\n";
    return;
  }
  if (p.tiers.size() != 1) {
    std::cerr << "PUT_ERR standalone put-overwrite requires exactly one tiers= "
                 "size\n";
    return;
  }
  if (p.base_files % p.concurrency != 0) {
    std::cerr << "PUT_ERR base_files (" << p.base_files
              << ") must be divisible by c (" << p.concurrency << ")\n";
    return;
  }
  auto all_buckets = list_all_buckets(service, tenant_id);
  if (all_buckets.empty()) {
    std::cerr << "PUT_ERR no buckets found. Run create-buckets first.\n";
    return;
  }
  int num_buckets = 0;
  for (const auto &b : all_buckets) {
    int idx = parse_perf_bucket_index(b);
    if (idx < 0) {
      std::cerr << "PUT_ERR unexpected bucket name " << b << "\n";
      return;
    }
    if (idx + 1 > num_buckets) {
      num_buckets = idx + 1;
    }
  }
  if (static_cast<int>(all_buckets.size()) != num_buckets) {
    std::cerr << "PUT_ERR bucket names are not contiguous perf-bucket-0.."
              << (num_buckets - 1) << "\n";
    return;
  }
  if (p.base_files % num_buckets != 0) {
    std::cerr << "PUT_ERR base_files (" << p.base_files
              << ") must be divisible by buckets (" << num_buckets << ")\n";
    return;
  }
  const int files_per_thread = static_cast<int>(p.base_files / p.concurrency);
  const uint64_t obj_size = p.tiers[0];
  const int64_t writes_per_thread =
      static_cast<int64_t>(files_per_thread) * (p.overwrite_count + 1);

  std::cout << "put-overwrite: standalone c=" << p.concurrency
            << " base_files=" << p.base_files
            << " overwrite_count=" << p.overwrite_count
            << " files/thread=" << files_per_thread
            << " buckets=" << num_buckets << " tiers=[" << tier_label(obj_size)
            << "]\n";

  service.latency_stats().reset();
  service.store().fdb_put_stats().reset();
  BenchResult result;
  std::vector<std::thread> threads;

  auto t0 = std::chrono::steady_clock::now();
  for (int t = 0; t < p.concurrency; ++t) {
    threads.emplace_back(put_overwrite_standalone_worker, std::ref(service),
                         tenant_id, obj_size, t, files_per_thread,
                         p.overwrite_count, num_buckets, p.progress_sec, t0,
                         std::ref(result));
  }
  for (auto &th : threads) {
    th.join();
  }
  auto t1 = std::chrono::steady_clock::now();
  double elapsed = std::chrono::duration<double>(t1 - t0).count();

  const int64_t total = writes_per_thread * p.concurrency;
  std::string label = "PUT-OVERWRITE " + tier_label(obj_size) + " (" +
                      std::to_string(total) + " writes)";
  print_results(label.c_str(), result, elapsed, service.latency_stats());
  print_fdb_put_stats(service.store());
  std::cout << "put-overwrite complete\n";
}

static void cmd_put_overwrite(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                              const std::string &tenant_name,
                              const ParsedParams &p)
{
  if (p.tiers.empty()) {
    std::cerr << "ERROR: put-overwrite requires tiers= parameter\n";
    return;
  }
  if (p.base_files > 0 && p.overwrite_count < 0) {
    std::cerr << "ERROR: base_files= requires overwrite_count=\n";
    return;
  }
  if (p.base_files <= 0 && p.overwrite_count >= 0) {
    std::cerr << "ERROR: overwrite_count= requires base_files=\n";
    return;
  }
  if (p.base_files > 0) {
    cmd_put_overwrite_standalone(service, tenant_id, p);
    return;
  }
  std::cout << "put-overwrite: c=" << p.concurrency << " tiers=[";
  for (size_t i = 0; i < p.tiers.size(); ++i) {
    if (i > 0) {
      std::cout << ",";
    }
    std::cout << tier_label(p.tiers[i]);
  }
  std::cout << "]\n";

  auto ranges =
      prepare_thread_ranges(service, tenant_id, tenant_name, p.concurrency);
  if (ranges.empty()) {
    return;
  }

  for (auto obj_size : p.tiers) {
    service.latency_stats().reset();
    service.store().fdb_put_stats().reset();
    BenchResult result;
    std::vector<std::thread> threads;

    auto t0 = std::chrono::steady_clock::now();
    for (int t = 0; t < p.concurrency; ++t) {
      threads.emplace_back(put_overwrite_worker, std::ref(service), tenant_id,
                           std::cref(ranges[t]), obj_size, std::ref(result));
    }
    for (auto &th : threads) {
      th.join();
    }
    auto t1 = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();

    int64_t total = 0;
    for (const auto &r : ranges) {
      total += r.keys.size();
    }
    std::string label = "PUT-OVERWRITE " + tier_label(obj_size) + " (" +
                        std::to_string(total) + " objs)";
    print_results(label.c_str(), result, elapsed, service.latency_stats());
    print_fdb_put_stats(service.store());
  }
}

static void cmd_put_overwrite_versioned(KvRgwServiceImpl &service,
                                        tenant_id_t tenant_id,
                                        const std::string &tenant_name,
                                        const ParsedParams &p)
{
  if (p.tiers.empty()) {
    std::cerr << "ERROR: put-overwrite-versioned requires tiers= parameter\n";
    return;
  }
  if (p.versions <= 0) {
    std::cerr << "ERROR: put-overwrite-versioned requires versions=N (N > 0)\n";
    return;
  }
  std::cout << "put-overwrite-versioned: c=" << p.concurrency << " tiers=[";
  for (size_t i = 0; i < p.tiers.size(); ++i) {
    if (i > 0) {
      std::cout << ",";
    }
    std::cout << tier_label(p.tiers[i]);
  }
  std::cout << "] versions=" << p.versions << "\n";

  auto ranges =
      prepare_thread_ranges(service, tenant_id, tenant_name, p.concurrency);
  if (ranges.empty()) {
    return;
  }

  for (auto obj_size : p.tiers) {
    service.latency_stats().reset();
    service.store().fdb_put_stats().reset();
    BenchResult result;
    std::vector<std::thread> threads;

    auto t0 = std::chrono::steady_clock::now();
    for (int t = 0; t < p.concurrency; ++t) {
      threads.emplace_back(put_overwrite_versioned_worker, std::ref(service),
                           tenant_id, std::cref(ranges[t]), obj_size,
                           p.versions, std::ref(result));
    }
    for (auto &th : threads) {
      th.join();
    }
    auto t1 = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();

    int64_t total = 0;
    for (const auto &r : ranges) {
      total += static_cast<int64_t>(r.keys.size()) * p.versions;
    }
    std::string label = "PUT-OVERWRITE-VERSIONED " + tier_label(obj_size) +
                        " (" + std::to_string(total) + " writes)";
    print_results(label.c_str(), result, elapsed, service.latency_stats());
    print_fdb_put_stats(service.store());
  }
}

static void cmd_delete_version(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                               const std::string &tenant_name,
                               const ParsedParams &p)
{
  if (p.versions <= 0) {
    std::cerr << "ERROR: delete-version requires versions=N (must match prior "
                 "put-overwrite-versioned)\n";
    return;
  }
  std::cout << "delete-version: c=" << p.concurrency
            << " versions=" << p.versions << "\n";

  auto ranges =
      prepare_thread_ranges(service, tenant_id, tenant_name, p.concurrency);
  if (ranges.empty()) {
    return;
  }

  service.latency_stats().reset();
  BenchResult result;
  std::vector<std::thread> threads;

  auto t0 = std::chrono::steady_clock::now();
  for (int t = 0; t < p.concurrency; ++t) {
    threads.emplace_back(delete_version_worker, std::ref(service), tenant_id,
                         std::cref(ranges[t]), p.versions, t, std::ref(result));
  }
  for (auto &th : threads) {
    th.join();
  }
  auto t1 = std::chrono::steady_clock::now();
  double elapsed = std::chrono::duration<double>(t1 - t0).count();

  int64_t total = 0;
  for (const auto &r : ranges) {
    total += r.keys.size();
  }
  std::string label = "DELETE-VERSION (" + std::to_string(total) + " objs)";
  print_results(label.c_str(), result, elapsed, service.latency_stats());
}

// --- KeyIterator ---

class KeyIterator {
public:
  KeyIterator(std::string_view base_prefix, std::string_view base_suffix,
              int thread_count, int instance_count,
              const uint64_t *max_seq_per_instance, int bucket_idx = -1,
              int num_buckets = 1, uint64_t files_per_thread = 0)
      : thread_count_(thread_count), instance_count_(instance_count),
        max_seq_(max_seq_per_instance), thread_id_(0), instance_id_(0), seq_(0),
        done_(false), bucket_idx_(bucket_idx), num_buckets_(num_buckets),
        files_per_thread_(files_per_thread)
  {
    const int plen = static_cast<int>(base_prefix.size());
    const int slen = static_cast<int>(base_suffix.size());
    if (plen < 0 || slen < 0 ||
        plen + kNameMidLen + slen >= GET_OBJECT_NAME_MAX) {
      std::cerr << "ERROR: object name (prefix+mid+suffix) exceeds "
                << GET_OBJECT_NAME_MAX << "\n";
      done_ = true;
      len_ = 0;
      buf_[0] = '\0';
      return;
    }
    std::memcpy(buf_, base_prefix.data(), static_cast<size_t>(plen));
    buf_[plen] = '/';
    thread_pos_ = plen + 1;
    buf_[thread_pos_ + 4] = '/';
    instance_pos_ = thread_pos_ + 5;
    buf_[instance_pos_ + 2] = '_';
    seq_pos_ = instance_pos_ + 3;
    const int after_seq = seq_pos_ + 12;
    if (slen > 0) {
      std::memcpy(buf_ + after_seq, base_suffix.data(),
                  static_cast<size_t>(slen));
    }
    len_ = after_seq + slen;
    buf_[len_] = '\0';
    write_thread();
    write_instance();
    write_seq();
    skip_to_match();
  }

  bool next()
  {
    if (!advance_raw()) {
      return false;
    }
    skip_to_match();
    return !done_;
  }

  std::string_view view() const { return {buf_, static_cast<size_t>(len_)}; }

  void reset()
  {
    thread_id_ = 0;
    instance_id_ = 0;
    seq_ = 0;
    done_ = false;
    write_thread();
    write_instance();
    write_seq();
    skip_to_match();
  }

  uint64_t total_keys() const
  {
    if (bucket_idx_ < 0) {
      uint64_t total = 0;
      for (int i = 0; i < instance_count_; ++i) {
        total += max_seq_[i];
      }
      return static_cast<uint64_t>(thread_count_) * total;
    }
    uint64_t total = 0;
    for (int t = 0; t < thread_count_; ++t) {
      for (int i = 0; i < instance_count_; ++i) {
        for (uint64_t s = 0; s < max_seq_[i]; ++s) {
          const uint64_t file_id =
              static_cast<uint64_t>(t) * files_per_thread_ + s;
          if (static_cast<int>(file_id % static_cast<uint64_t>(num_buckets_)) ==
              bucket_idx_) {
            ++total;
          }
        }
      }
    }
    return total;
  }

  bool is_done() const { return done_; }

private:
  char buf_[GET_OBJECT_NAME_MAX];
  int len_;
  int thread_pos_, instance_pos_, seq_pos_;
  int thread_id_, instance_id_;
  uint64_t seq_;
  int thread_count_, instance_count_;
  const uint64_t *max_seq_;
  bool done_;
  int bucket_idx_;
  int num_buckets_;
  uint64_t files_per_thread_;

  bool in_bucket() const
  {
    if (bucket_idx_ < 0) {
      return true;
    }
    const uint64_t file_id =
        static_cast<uint64_t>(thread_id_) * files_per_thread_ + seq_;
    return static_cast<int>(file_id % static_cast<uint64_t>(num_buckets_)) ==
           bucket_idx_;
  }

  bool advance_raw()
  {
    if (done_) {
      return false;
    }
    ++seq_;
    if (seq_ >= max_seq_[instance_id_]) {
      seq_ = 0;
      ++instance_id_;
      if (instance_id_ >= instance_count_) {
        instance_id_ = 0;
        ++thread_id_;
        if (thread_id_ >= thread_count_) {
          done_ = true;
          return false;
        }
        write_thread();
      }
      write_instance();
    }
    write_seq();
    return true;
  }

  void skip_to_match()
  {
    while (!done_ && !in_bucket()) {
      if (!advance_raw()) {
        break;
      }
    }
  }

  void write_thread()
  {
    buf_[thread_pos_] = '0' + (thread_id_ / 1000) % 10;
    buf_[thread_pos_ + 1] = '0' + (thread_id_ / 100) % 10;
    buf_[thread_pos_ + 2] = '0' + (thread_id_ / 10) % 10;
    buf_[thread_pos_ + 3] = '0' + thread_id_ % 10;
  }
  void write_instance()
  {
    buf_[instance_pos_] = '0' + (instance_id_ / 10) % 10;
    buf_[instance_pos_ + 1] = '0' + instance_id_ % 10;
  }
  void write_seq()
  {
    uint64_t s = seq_;
    for (int i = 11; i >= 0; --i) {
      buf_[seq_pos_ + i] = '0' + static_cast<char>(s % 10);
      s /= 10;
    }
  }
};

// --- list-test command ---
//--------------------------------------------------------------------------------
static void cmd_list_test(KvRgwServiceImpl &service, tenant_id_t tenant_id,
                          const std::string &opts)
{
  bool blind = false;
  bool quiet = false;
  bool all_versions = false;
  bool ryw_cache_enabled = false;
  int max_pages = 0;
  int progress_interval = 100;
  {
    std::istringstream iss(opts);
    std::string token;
    while (iss >> token) {
      if (token == "--blind") {
        blind = true;
      }
      else if (token == "--quiet") {
        quiet = true;
      }
      else if (token == "--all-versions") {
        all_versions = true;
      }
      else if (token == "--ryw-cache=enabled") {
        ryw_cache_enabled = true;
      }
      else if (token == "--ryw-cache=disabled") {
        ryw_cache_enabled = false;
      }
      else if (token.rfind("--max-pages=", 0) == 0) {
        max_pages = std::atoi(token.c_str() + 12);
      }
      else if (token.rfind("--progress=", 0) == 0) {
        progress_interval = std::atoi(token.c_str() + 11);
      }
    }
  }

  if (quiet && progress_interval > 0) {
    std::cerr << "ERROR: --quiet and --progress are mutually exclusive\n";
    return;
  }

  auto all_buckets = list_all_buckets(service, tenant_id);
  if (all_buckets.empty()) {
    std::cerr << "ERROR: no buckets found.\n";
    return;
  }

  if (!g_metadata_file) {
    std::cerr << "ERROR: KVRGW_METADATA_FILE not set. Run via "
                 "run_perf_from_def.sh.\n";
    return;
  }

  // Derive merged metadata path from per-instance file's directory
  std::string meta_path(g_metadata_file);
  auto last_slash = meta_path.rfind('/');
  std::string merged_meta =
      (last_slash != std::string::npos)
          ? meta_path.substr(0, last_slash + 1) + "test_metadata.txt"
          : "test_metadata.txt";

  int threads = 0, instances = 0, meta_buckets = 0;
  uint64_t max_seq_arr[128]{};
  std::string workload;
  std::string version_state;
  int64_t base_files = 0;
  int64_t overwrite_count = 0;

  {
    std::ifstream mf(merged_meta);
    if (!mf) {
      std::cerr << "ERROR: cannot open " << merged_meta << "\n";
      return;
    }
    std::string line;
    while (std::getline(mf, line)) {
      if (line.rfind("threads=", 0) == 0) {
        threads = std::atoi(line.c_str() + 8);
      }
      else if (line.rfind("instances=", 0) == 0) {
        instances = std::atoi(line.c_str() + 10);
      }
      else if (line.rfind("buckets=", 0) == 0) {
        meta_buckets = std::atoi(line.c_str() + 8);
      }
      else if (line.rfind("workload=", 0) == 0) {
        workload = line.substr(9);
      }
      else if (line.rfind("version_state=", 0) == 0) {
        version_state = line.substr(14);
      }
      else if (line.rfind("base_files=", 0) == 0) {
        base_files = std::atoll(line.c_str() + 11);
      }
      else if (line.rfind("overwrite_count=", 0) == 0) {
        overwrite_count = std::atoll(line.c_str() + 16);
      }
      else if (line.rfind("max_seq_", 0) == 0) {
        auto eq = line.find('=');
        if (eq != std::string::npos) {
          int idx = std::atoi(line.c_str() + 8);
          if (idx >= 0 && idx < 128) {
            max_seq_arr[idx] = std::stoull(line.substr(eq + 1));
          }
        }
      }
    }
  }

  if (threads <= 0 || instances <= 0) {
    std::cerr << "ERROR: invalid metadata (threads=" << threads
              << " instances=" << instances << ")\n";
    return;
  }

  if (all_versions && version_state != "versioned") {
    std::cerr << "ERROR: --all-versions requires version_state=versioned (got '"
              << version_state << "')\n";
    return;
  }

  const bool overwrite = (workload == "put-overwrite");
  uint64_t files_per_thread = 0;
  if (overwrite) {
    if (base_files <= 0 || meta_buckets <= 0) {
      std::cerr << "ERROR: put-overwrite list-test requires base_files and "
                   "buckets in metadata\n";
      return;
    }
    if (base_files % threads != 0) {
      std::cerr << "ERROR: base_files (" << base_files
                << ") not divisible by threads (" << threads << ")\n";
      return;
    }
    if (base_files % meta_buckets != 0) {
      std::cerr << "ERROR: base_files (" << base_files
                << ") not divisible by buckets (" << meta_buckets << ")\n";
      return;
    }
    files_per_thread = static_cast<uint64_t>(base_files / threads);
    for (int i = 0; i < instances && i < 128; ++i) {
      max_seq_arr[i] = files_per_thread;
    }
  }

  int64_t nver = 1;
  uint32_t latest_vid = kFirstVersionId.raw();
  if (all_versions && overwrite) {
    if (overwrite_count > static_cast<int64_t>(kFirstVersionId.raw())) {
      std::cerr << "ERROR: overwrite_count too large for version_id space\n";
      return;
    }
    nver = overwrite_count + 1;
    latest_vid = kFirstVersionId.raw() - static_cast<uint32_t>(overwrite_count);
  }

  std::string prefix(g_key_prefix);
  std::string suffix(g_key_suffix);
  std::string prefix_slash = prefix + "/";

  std::cout << "list-test: prefix=" << prefix << " suffix=" << suffix
            << " threads=" << threads << " instances=" << instances
            << " buckets=" << all_buckets.size();
  if (overwrite) {
    std::cout << " workload=put-overwrite base_files=" << base_files;
  }
  if (blind) {
    std::cout << " --blind";
  }
  if (quiet) {
    std::cout << " --quiet";
  }
  if (all_versions) {
    std::cout << " --all-versions nver=" << nver;
  }
  std::cout << " --ryw-cache=" << (ryw_cache_enabled ? "enabled" : "disabled");
  if (max_pages > 0) {
    std::cout << " --max-pages=" << max_pages;
  }
  std::cout << " --progress=" << progress_interval;
  std::cout << "\n";

  auto fmt_M = [](uint64_t n, char *buf, size_t buflen) -> const char * {
    snprintf(buf, buflen, "%6.1fM", n / 1000000.0);
    return buf;
  };
  auto fmt_K = [](uint64_t n, char *buf, size_t buflen) -> const char * {
    snprintf(buf, buflen, "%6.1fK", n / 1000.0);
    return buf;
  };

  int total_pages = 0;
  uint64_t total_keys = 0;
  int buckets_done = 0;
  std::vector<int64_t> all_latencies;
  bool failed = false;
  bool global_limit_hit = false;
  uint64_t expected_per_bucket = 0;
  uint64_t prev_keys = 0;
  double prev_elapsed = 0.0;

  if (ryw_cache_enabled) {
    service.set_listing_disable_ryw(false);
  }

  service.latency_stats().reset_list_scan_stats();

  auto t0_total = std::chrono::steady_clock::now();

  for (const auto &bname : all_buckets) {
    if (global_limit_hit) {
      break;
    }

    int bucket_idx = -1;
    int num_b = 1;
    uint64_t fpt = 0;
    if (overwrite) {
      bucket_idx = parse_perf_bucket_index(bname);
      if (bucket_idx < 0 || bucket_idx >= meta_buckets) {
        std::cerr << "  FAIL: unexpected bucket name " << bname << "\n";
        failed = true;
        break;
      }
      num_b = meta_buckets;
      fpt = files_per_thread;
    }

    KeyIterator iter(prefix, suffix, threads, instances, max_seq_arr,
                     bucket_idx, num_b, fpt);
    if (iter.view().empty()) {
      failed = true;
      break;
    }
    if (expected_per_bucket == 0) {
      expected_per_bucket = iter.total_keys();
      if (all_versions) {
        expected_per_bucket *= static_cast<uint64_t>(nver);
      }
    }

    std::string token;
    std::string key_marker;
    version_id_t vid_marker{};
    int64_t ver_i = 0;

    while (true) {
      if (max_pages > 0 && total_pages >= max_pages) {
        global_limit_hit = true;
        break;
      }

      auto t0 = std::chrono::steady_clock::now();
      KvrgwErrorCode ec;
      bool truncated = false;
      size_t nentries = 0;

      if (all_versions) {
        KvRgwServiceImpl::ListObjectVersionsResult out;
        ec = service.list_object_versions(tenant_id, bname, prefix_slash, 1000,
                                          key_marker, vid_marker, &out);
        auto t1 = std::chrono::steady_clock::now();
        all_latencies.push_back(
            std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
                .count());
        if (ec != KVRGW_ERR_OK) {
          std::cerr << "  FAIL: bucket=" << bname
                    << " list_object_versions error=" << kvrgw_strerror(ec)
                    << "\n";
          failed = true;
          break;
        }
        nentries = out.versions.size();
        if (!blind) {
          for (const auto &obj : out.versions) {
            if (iter.is_done()) {
              std::cerr << "  FAIL: bucket=" << bname << " page=" << total_pages
                        << " extra key=" << obj.key << "\n";
              failed = true;
              break;
            }
            if (obj.key != iter.view()) {
              std::cerr << "  FAIL: bucket=" << bname << " page=" << total_pages
                        << " got=" << obj.key << " expected=" << iter.view()
                        << "\n";
              failed = true;
              break;
            }
            if (obj.is_delete_marker) {
              std::cerr << "  FAIL: bucket=" << bname
                        << " unexpected delete marker key=" << obj.key << "\n";
              failed = true;
              break;
            }
            const uint32_t exp_vid = latest_vid + static_cast<uint32_t>(ver_i);
            const bool exp_latest = (ver_i == 0);
            if (obj.version_id.raw() != exp_vid) {
              std::cerr << "  FAIL: bucket=" << bname << " key=" << obj.key
                        << " vid=0x" << std::hex << obj.version_id.raw()
                        << " expected=0x" << exp_vid << std::dec << "\n";
              failed = true;
              break;
            }
            if (obj.is_latest != exp_latest) {
              std::cerr << "  FAIL: bucket=" << bname << " key=" << obj.key
                        << " is_latest=" << obj.is_latest
                        << " expected=" << exp_latest << "\n";
              failed = true;
              break;
            }
            ++ver_i;
            if (ver_i >= nver) {
              ver_i = 0;
              iter.next();
            }
            ++total_keys;
          }
          if (failed) {
            break;
          }
        }
        else {
          total_keys += nentries;
        }
        truncated = out.is_truncated;
        key_marker = out.next_key_marker;
        vid_marker = out.next_version_id_marker;
      }
      else {
        KvRgwServiceImpl::ListObjectsResult out;
        ec = service.list_objects(tenant_id, bname, prefix_slash, {}, 1000,
                                  token, {}, &out);
        auto t1 = std::chrono::steady_clock::now();
        all_latencies.push_back(
            std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
                .count());
        if (ec != KVRGW_ERR_OK) {
          std::cerr << "  FAIL: bucket=" << bname
                    << " list_objects error=" << kvrgw_strerror(ec) << "\n";
          failed = true;
          break;
        }
        nentries = out.objects.size();
        if (!blind) {
          for (const auto &obj : out.objects) {
            if (iter.is_done()) {
              std::cerr << "  FAIL: bucket=" << bname << " page=" << total_pages
                        << " extra key=" << obj.key << "\n";
              failed = true;
              break;
            }
            if (obj.key != iter.view()) {
              std::cerr << "  FAIL: bucket=" << bname << " page=" << total_pages
                        << " got=" << obj.key << " expected=" << iter.view()
                        << "\n";
              failed = true;
              break;
            }
            iter.next();
            ++total_keys;
          }
          if (failed) {
            break;
          }
        }
        else {
          total_keys += nentries;
        }
        truncated = out.is_truncated;
        token = out.next_continuation_token;
      }

      ++total_pages;
      if (!quiet && progress_interval > 0 &&
          total_pages % progress_interval == 0) {
        double elapsed_so_far = std::chrono::duration<double>(
                                    std::chrono::steady_clock::now() - t0_total)
                                    .count();
        double cum_kps = elapsed_so_far > 0 ? total_keys / elapsed_so_far : 0;
        double cum_pps = elapsed_so_far > 0 ? total_pages / elapsed_so_far : 0;
        double dt = elapsed_so_far - prev_elapsed;
        double int_kps = dt > 0 ? (total_keys - prev_keys) / dt : 0;
        char pages_buf[32], keys_buf[32];
        char elapsed_buf[32];
        char list_stats_buf[64];
        snprintf(elapsed_buf, sizeof(elapsed_buf), "%6.1f", elapsed_so_far);
        snprintf(
            list_stats_buf, sizeof(list_stats_buf),
            "%6lld avg_range_scan_us, %2lld avg iter per call",
            static_cast<long long>(service.latency_stats().avg_range_scan_us()),
            static_cast<long long>(
                service.latency_stats().avg_list_iter_per_call()));
        std::cerr << "  progress: "
                  << fmt_K(total_pages, pages_buf, sizeof(pages_buf))
                  << " pages " << fmt_M(total_keys, keys_buf, sizeof(keys_buf))
                  << " keys " << elapsed_buf << "s elapsed"
                  << " (" << static_cast<int64_t>(cum_kps) << " keys/s, "
                  << static_cast<int64_t>(cum_pps) << " pages/s)"
                  << " interval: " << static_cast<int64_t>(int_kps) << " keys/s"
                  << " " << list_stats_buf << "\n";
        prev_keys = total_keys;
        prev_elapsed = elapsed_so_far;
      }
      if (!truncated) {
        break;
      }
    }

    if (failed) {
      break;
    }
    if (!blind && !global_limit_hit && (!iter.is_done() || ver_i != 0)) {
      std::cerr << "  FAIL: bucket=" << bname
                << " iterator not exhausted (missing keys)\n";
      failed = true;
      break;
    }
    ++buckets_done;
  }

  if (ryw_cache_enabled) {
    service.set_listing_disable_ryw(true);
  }

  auto t1_total = std::chrono::steady_clock::now();
  double elapsed = std::chrono::duration<double>(t1_total - t0_total).count();

  uint64_t expected_total = expected_per_bucket * all_buckets.size();

  std::cerr << "\n\n\n=== LIST-TEST Summary ===\n";
  double sum_kps = elapsed > 0 ? total_keys / elapsed : 0;
  double sum_pps = elapsed > 0 ? total_pages / elapsed : 0;
  std::cout << "  buckets=" << buckets_done << "/" << all_buckets.size()
            << " pages=" << total_pages << " keys=" << total_keys
            << " elapsed=" << std::fixed << std::setprecision(1) << elapsed
            << "s"
            << " (" << static_cast<int64_t>(sum_kps) << " keys/s, "
            << static_cast<int64_t>(sum_pps) << " pages/s)\n";

  if (!all_latencies.empty()) {
    std::sort(all_latencies.begin(), all_latencies.end());
    int n = static_cast<int>(all_latencies.size());
    int64_t mn = all_latencies.front();
    int64_t mx = all_latencies.back();
    int64_t sum = 0;
    for (auto v : all_latencies) {
      sum += v;
    }
    double avg = static_cast<double>(sum) / n;
    auto pctl = [&](double p) -> int64_t {
      double k = (p / 100.0) * (n - 1);
      int f = static_cast<int>(k);
      int c = std::min(f + 1, n - 1);
      return all_latencies[f] +
             static_cast<int64_t>((all_latencies[c] - all_latencies[f]) *
                                  (k - f));
    };
    int64_t med = pctl(50), p95 = pctl(95), p99 = pctl(99);
    std::cout << "  Page latency: min=" << std::setprecision(1) << mn / 1000.0
              << "ms max=" << mx / 1000.0 << "ms avg=" << avg / 1000.0
              << "ms median=" << med / 1000.0 << "ms p95=" << p95 / 1000.0
              << "ms p99=" << p99 / 1000.0 << "ms\n";
  }

  if (!blind) {
    if (failed) {
      std::cout << "  Verification: FAIL\n";
    }
    else if (global_limit_hit) {
      std::cout << "  Verification: PASS (partial, " << total_keys
                << " keys checked)\n";
    }
    else {
      std::cout << "  Verification: PASS (" << total_keys << "/"
                << expected_total << " keys)\n";
    }
  }
}

// --- Main command loop ---

int run_perf_driver(KvRgwRuntime &runtime)
{
  PerfConfig pcfg;
  pcfg.perf_data_store = runtime.perf_data_store();
  return run_perf_driver(runtime.service(), runtime.store(), pcfg);
}

int run_perf_driver(KvRgwServiceImpl &service, KvStore &store,
                    const PerfConfig &config)
{
  if (const char *iid = std::getenv("KVRGW_INSTANCE_ID")) {
    g_instance_id = std::atoi(iid);
  }
  if (const char *kp = std::getenv("KVRGW_KEY_PREFIX")) {
    if (std::strlen(kp) > kKeyPrefixMaxLen) {
      std::cerr << "ERROR: KVRGW_KEY_PREFIX too long (" << std::strlen(kp)
                << " > " << kKeyPrefixMaxLen << ")\n";
      return 1;
    }
    std::strncpy(g_key_prefix, kp, kKeyPrefixMaxLen);
    g_key_prefix[kKeyPrefixMaxLen] = '\0';
  }
  if (const char *ks = std::getenv("KVRGW_KEY_SUFFIX")) {
    if (std::strlen(ks) > kKeyPrefixMaxLen) {
      std::cerr << "ERROR: KVRGW_KEY_SUFFIX too long (" << std::strlen(ks)
                << " > " << kKeyPrefixMaxLen << ")\n";
      return 1;
    }
    std::strncpy(g_key_suffix, ks, kKeyPrefixMaxLen);
    g_key_suffix[kKeyPrefixMaxLen] = '\0';
  }
  if (std::strlen(g_key_prefix) + std::strlen(g_key_suffix) >
      kKeyPrefixMaxLen) {
    std::cerr << "ERROR: prefix+suffix length exceeds " << kKeyPrefixMaxLen
              << "\n";
    return 1;
  }
  if (!init_put_tags()) {
    return 1;
  }
  g_metadata_file = std::getenv("KVRGW_METADATA_FILE");
  std::cout << "=== KV-RGW Perf Driver (interactive) ===\n";
  std::cout << "tenant=" << config.tenant_name
            << " instance_id=" << g_instance_id << " prefix=" << g_key_prefix
            << " suffix=" << g_key_suffix << "\n";
  std::cout << "Commands: create-buckets, put, put-multi, get, delete, "
               "delete-multi,\n"
            << "          copy, put-overwrite, put-overwrite-versioned, "
               "delete-version,\n"
            << "          delete-buckets, list-buckets, list-objects, "
               "list-test, get-test, set-batch, quit\n";
  std::cout << "Example: put c=128 tiers=128,4096,8192 duration=300 "
               "[--progress-sec=N]\n";
  std::cout
      << "         list-test [--blind] [--quiet] [--all-versions] "
         "[--ryw-cache=enabled|disabled] [--max-pages=N] [--progress=N]\n";
  std::cout << "         get-test producers=N consumers=M max_futures=K "
               "[duration=N|count=N] [--all-versions] [--progress-sec=N]\n\n";

  tenant_id_t tenant_id = 0;
  {
    auto tid = service.resolve_tenant_id(config.tenant_name);
    if (!tid || !*tid) {
      tenant_id_t created = 0;
      service.add_tenant(config.tenant_name, &created);
      tid = service.resolve_tenant_id(config.tenant_name);
    }
    if (!tid || !*tid) {
      std::cerr << "tenant resolution failed for: " << config.tenant_name
                << "\n";
      return 1;
    }
    tenant_id = **tid;
  }

  std::string line;
  std::cout << "perf> " << std::flush;
  while (std::getline(std::cin, line)) {
    if (line.empty()) {
      std::cout << "perf> " << std::flush;
      continue;
    }

    std::string cmd;
    std::string rest;
    auto sp = line.find(' ');
    if (sp == std::string::npos) {
      cmd = line;
    }
    else {
      cmd = line.substr(0, sp);
      rest = line.substr(sp + 1);
    }

    if (cmd == "quit" || cmd == "exit") {
      std::cout << "bye\n";
      return 0;
    }

    auto params = parse_params(rest);

    if (cmd == "create-buckets") {
      cmd_create_buckets(service, tenant_id, params);
    }
    else if (cmd == "put") {
      if (params.tiers.empty()) {
        std::cerr << "ERROR: put requires tiers= parameter\n";
      }
      else {
        cmd_put(service, tenant_id, config.tenant_name, params);
      }
    }
    else if (cmd == "put-multi") {
      if (params.tiers.empty()) {
        std::cerr << "ERROR: put-multi requires tiers= parameter\n";
      }
      else {
        cmd_put_multi(service, tenant_id, config.tenant_name, params);
      }
    }
    else if (cmd == "get") {
      cmd_get(service, tenant_id, config.tenant_name, params);
    }
    else if (cmd == "get-test") {
      if (rest.find("duration=") == std::string::npos) {
        params.duration = 0;
      }
      cmd_get_test(service, tenant_id, params, rest);
    }
    else if (cmd == "delete") {
      cmd_delete(service, tenant_id, config.tenant_name, params);
    }
    else if (cmd == "delete-multi") {
      cmd_delete_multi(service, tenant_id, config.tenant_name, params);
    }
    else if (cmd == "copy") {
      cmd_copy(service, tenant_id, config.tenant_name, params);
    }
    else if (cmd == "put-overwrite") {
      cmd_put_overwrite(service, tenant_id, config.tenant_name, params);
    }
    else if (cmd == "put-overwrite-versioned") {
      cmd_put_overwrite_versioned(service, tenant_id, config.tenant_name,
                                  params);
    }
    else if (cmd == "delete-version") {
      cmd_delete_version(service, tenant_id, config.tenant_name, params);
    }
    else if (cmd == "delete-buckets") {
      cmd_delete_buckets(service, tenant_id, config.tenant_name, params);
    }
    else if (cmd == "list-buckets") {
      cmd_list_buckets(service, tenant_id, params);
    }
    else if (cmd == "list-objects") {
      cmd_list_objects(service, tenant_id, params);
    }
    else if (cmd == "list-test") {
      cmd_list_test(service, tenant_id, rest);
    }
    else if (cmd == "set-batch") {
      auto tc = service.tier_config_state().active_copy();
      std::istringstream iss(rest);
      std::string token;
      while (iss >> token) {
        if (token.rfind("size=", 0) == 0) {
          tc.batch_size = std::atoi(token.c_str() + 5);
        }
        else if (token.rfind("timeout=", 0) == 0) {
          tc.batch_timeout_us = std::atoi(token.c_str() + 8);
        }
        else if (token.rfind("threads=", 0) == 0) {
          tc.batch_threads = std::atoi(token.c_str() + 8);
        }
      }
      service.tier_config_state().try_stage(tc);
      service.tier_config_state().maybe_apply_pending();
      service.batch_queue().stop();
      if (tc.batch_size > 1) {
        service.batch_queue().start(tc.batch_threads);
      }
      std::cout << "batch: size=" << tc.batch_size
                << " timeout=" << tc.batch_timeout_us << "us"
                << " threads=" << tc.batch_threads << "\n";
    }
    else if (cmd == "batch-stats") {
      auto &s = service.batch_queue().stats();
      auto &ls = service.latency_stats();
      int64_t bc = s.batch_commits.load();
      int64_t te = s.total_entries_batched.load();
      double avg_bs = bc > 0 ? static_cast<double>(te) / bc : 0;
      int64_t avg_wait = bc > 0 ? s.total_wait_us.load() / bc : 0;
      double avg_qsz =
          bc > 0
              ? static_cast<double>(s.total_queue_size_at_extract.load()) / bc
              : 0;
      std::cout << "batch_commits=" << bc << " entries_batched=" << te
                << " conflict_pushbacks=" << s.conflict_pushbacks.load()
                << " avg_batch_size=" << std::fixed << std::setprecision(1)
                << avg_bs << " min_batch_size=" << s.min_batch_size.load()
                << " max_batch_size=" << s.max_batch_size.load()
                << " avg_queue_size=" << std::setprecision(1) << avg_qsz
                << " avg_wait_us=" << avg_wait
                << " min_wait_us=" << s.min_wait_us.load()
                << " max_wait_us=" << s.max_wait_us.load()
                << " txn_retries=" << ls.txn_retries.load()
                << " txn_hard_failures=" << ls.txn_hard_failures.load()
                << " txn_max_retries_exceeded="
                << ls.txn_max_retries_exceeded.load() << "\n";
      s.reset();
    }
    else if (cmd == "set-sim-disk-write-us") {
      if (!config.perf_data_store) {
        std::cerr << "ERROR: not in perf data store mode\n";
      }
      else {
        int64_t us = rest.empty() ? 0 : std::atoll(rest.c_str());
        config.perf_data_store->set_sim_write_us(us);
        std::cout << "sim_disk_write_us=" << us << "\n";
      }
    }
    else if (cmd == "set-sim-disk-read-us") {
      if (!config.perf_data_store) {
        std::cerr << "ERROR: not in perf data store mode\n";
      }
      else {
        int64_t us = rest.empty() ? 0 : std::atoll(rest.c_str());
        config.perf_data_store->set_sim_read_us(us);
        std::cout << "sim_disk_read_us=" << us << "\n";
      }
    }
    else if (cmd == "get-sim-disk") {
      if (!config.perf_data_store) {
        std::cerr << "ERROR: not in perf data store mode\n";
      }
      else {
        std::cout << "sim_disk_write_us="
                  << config.perf_data_store->sim_write_us()
                  << " sim_disk_read_us="
                  << config.perf_data_store->sim_read_us() << "\n";
      }
    }
    else {
      std::cerr << "unknown command: " << cmd << "\n";
    }

    std::cout << "perf> " << std::flush;
  }

  return 0;
}

} // namespace kvrgw
