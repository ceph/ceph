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

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string_view>

namespace kvrgw {

enum class OpType : uint8_t {
  kPutObject,
  kGetObject,
  kHeadObject,
  kDeleteObject,
  kDeleteMulti,
  kDeleteBucket,
  kCreateBucket,
  kListBuckets,
  kListObjects,
  kListObjectVersions,
  kDeleteObjectVersion,
  kCopyObject,
  kPutObjectTagging,
  kGetObjectTagging,
  kDeleteObjectTagging,
  kBucketExists,
  kPutBucketPolicy,
  kGetBucketPolicy,
  kDeleteBucketPolicy,
  kPutBucketVersioning,
  kGetBucketVersioning,
  kOther,
  kCount
};

inline const char* op_type_name(OpType op) {
  switch (op) {
    case OpType::kPutObject:           return "PutObject";
    case OpType::kGetObject:           return "GetObject";
    case OpType::kHeadObject:          return "HeadObject";
    case OpType::kDeleteObject:        return "DeleteObject";
    case OpType::kDeleteMulti:         return "DeleteMulti";
    case OpType::kDeleteBucket:        return "DeleteBucket";
    case OpType::kCreateBucket:        return "CreateBucket";
    case OpType::kListBuckets:         return "ListBuckets";
    case OpType::kListObjects:         return "ListObjects";
    case OpType::kListObjectVersions:  return "ListObjVersions";
    case OpType::kDeleteObjectVersion: return "DeleteObjVersion";
    case OpType::kCopyObject:          return "CopyObject";
    case OpType::kPutObjectTagging:    return "PutObjTagging";
    case OpType::kGetObjectTagging:    return "GetObjTagging";
    case OpType::kDeleteObjectTagging: return "DelObjTagging";
    case OpType::kBucketExists:        return "BucketExists";
    case OpType::kPutBucketPolicy:     return "PutBucketPolicy";
    case OpType::kGetBucketPolicy:     return "GetBucketPolicy";
    case OpType::kDeleteBucketPolicy:  return "DelBucketPolicy";
    case OpType::kPutBucketVersioning: return "PutBucketVer";
    case OpType::kGetBucketVersioning: return "GetBucketVer";
    default:                           return "Other";
  }
}

struct RequestLatency {
  int64_t fdb_get_us{};
  int64_t fdb_put_us{};
  int64_t fdb_del_us{};
  int64_t fdb_commit_us{};
  int64_t fdb_scan_us{};
  int64_t disk_us{};
  int fdb_get_count{};
  int fdb_put_count{};
  int fdb_del_count{};
  int fdb_commit_count{};
  int fdb_scan_count{};
  int disk_count{};

  int64_t fdb_total_us() const {
    return fdb_get_us + fdb_put_us + fdb_del_us + fdb_commit_us + fdb_scan_us;
  }

  int fdb_total_ops() const {
    return fdb_get_count + fdb_put_count + fdb_del_count + fdb_commit_count + fdb_scan_count;
  }
};

inline thread_local RequestLatency* tl_request_latency = nullptr;

inline void fdb_record_get(int64_t us) {
  if (auto* r = tl_request_latency) { r->fdb_get_us += us; ++r->fdb_get_count; }
}
inline void fdb_record_put(int64_t us) {
  if (auto* r = tl_request_latency) { r->fdb_put_us += us; ++r->fdb_put_count; }
}
inline void fdb_record_del(int64_t us) {
  if (auto* r = tl_request_latency) { r->fdb_del_us += us; ++r->fdb_del_count; }
}
inline void fdb_record_commit(int64_t us) {
  if (auto* r = tl_request_latency) { r->fdb_commit_us += us; ++r->fdb_commit_count; }
}
inline void fdb_record_scan(int64_t us) {
  if (auto* r = tl_request_latency) { r->fdb_scan_us += us; ++r->fdb_scan_count; }
}
inline void fdb_record_disk(int64_t us) {
  if (auto* r = tl_request_latency) { r->disk_us += us; ++r->disk_count; }
}

struct OpStats {
  std::atomic<int64_t> count{};
  std::atomic<int64_t> total_us{};
  std::atomic<int64_t> fdb_us{};
  std::atomic<int64_t> fdb_get_us{};
  std::atomic<int64_t> fdb_put_us{};
  std::atomic<int64_t> fdb_del_us{};
  std::atomic<int64_t> fdb_commit_us{};
  std::atomic<int64_t> fdb_scan_us{};
  std::atomic<int64_t> fdb_ops{};
  std::atomic<int64_t> disk_us{};
};

struct LatencyStats {
  OpStats ops[static_cast<int>(OpType::kCount)];

  std::atomic<int64_t> txn_retries{0};
  std::atomic<int64_t> txn_hard_failures{0};
  std::atomic<int64_t> txn_max_retries_exceeded{0};

  std::atomic<int64_t> list_scan_us{0};
  std::atomic<int64_t> list_scan_n{0};
  std::atomic<int64_t> list_iter_sum{0};
  std::atomic<int64_t> list_rpc_n{0};
  std::atomic<int64_t> list_drain_calls{0};

  void record_list_scan(int64_t us) {
    list_scan_us.fetch_add(us, std::memory_order_relaxed);
    list_scan_n.fetch_add(1, std::memory_order_relaxed);
  }

  void record_list_call(int64_t iters) {
    list_iter_sum.fetch_add(iters, std::memory_order_relaxed);
    list_rpc_n.fetch_add(1, std::memory_order_relaxed);
  }

  int64_t avg_range_scan_us() const {
    const auto n = list_scan_n.load(std::memory_order_relaxed);
    if (n <= 0) return 0;
    return list_scan_us.load(std::memory_order_relaxed) / n;
  }

  int64_t avg_list_iter_per_call() const {
    const auto n = list_rpc_n.load(std::memory_order_relaxed);
    if (n <= 0) return 0;
    return list_iter_sum.load(std::memory_order_relaxed) / n;
  }

  void reset_list_scan_stats() {
    list_scan_us.store(0, std::memory_order_relaxed);
    list_scan_n.store(0, std::memory_order_relaxed);
    list_iter_sum.store(0, std::memory_order_relaxed);
    list_rpc_n.store(0, std::memory_order_relaxed);
    list_drain_calls.store(0, std::memory_order_relaxed);
  }

  void record(OpType op, int64_t total_us, const RequestLatency& fdb) {
    auto& s = ops[static_cast<int>(op)];
    s.count.fetch_add(1, std::memory_order_relaxed);
    s.total_us.fetch_add(total_us, std::memory_order_relaxed);
    s.fdb_us.fetch_add(fdb.fdb_total_us(), std::memory_order_relaxed);
    s.fdb_get_us.fetch_add(fdb.fdb_get_us, std::memory_order_relaxed);
    s.fdb_put_us.fetch_add(fdb.fdb_put_us, std::memory_order_relaxed);
    s.fdb_del_us.fetch_add(fdb.fdb_del_us, std::memory_order_relaxed);
    s.fdb_commit_us.fetch_add(fdb.fdb_commit_us, std::memory_order_relaxed);
    s.fdb_scan_us.fetch_add(fdb.fdb_scan_us, std::memory_order_relaxed);
    s.fdb_ops.fetch_add(fdb.fdb_total_ops(), std::memory_order_relaxed);
    s.disk_us.fetch_add(fdb.disk_us, std::memory_order_relaxed);
  }

  void dump(FILE* out = stderr) const {
    std::fprintf(out, "\n=== FDB Latency Breakdown ===\n");
    std::fprintf(out, "%-18s %8s %10s %10s %6s  %10s %10s %10s %10s %10s\n",
                 "Operation", "Count", "Total(us)", "FDB(us)", "FDB%",
                 "Get(us)", "Put(us)", "Del(us)", "Commit(us)", "Scan(us)");
    for (int i = 0; i < static_cast<int>(OpType::kCount); ++i) {
      auto& s = ops[i];
      auto cnt = s.count.load(std::memory_order_relaxed);
      if (cnt == 0) continue;
      auto total = s.total_us.load(std::memory_order_relaxed);
      auto fdb = s.fdb_us.load(std::memory_order_relaxed);
      double pct = total > 0 ? 100.0 * fdb / total : 0.0;
      std::fprintf(out, "%-18s %8lld %10lld %10lld %5.1f%%  %10lld %10lld %10lld %10lld %10lld\n",
                   op_type_name(static_cast<OpType>(i)),
                   static_cast<long long>(cnt),
                   static_cast<long long>(total / cnt),
                   static_cast<long long>(fdb / cnt),
                   pct,
                   static_cast<long long>(s.fdb_get_us.load(std::memory_order_relaxed) / cnt),
                   static_cast<long long>(s.fdb_put_us.load(std::memory_order_relaxed) / cnt),
                   static_cast<long long>(s.fdb_del_us.load(std::memory_order_relaxed) / cnt),
                   static_cast<long long>(s.fdb_commit_us.load(std::memory_order_relaxed) / cnt),
                   static_cast<long long>(s.fdb_scan_us.load(std::memory_order_relaxed) / cnt));
    }
    std::fprintf(out, "\n");
  }

  void reset() {
    for (auto& s : ops) {
      s.count.store(0, std::memory_order_relaxed);
      s.total_us.store(0, std::memory_order_relaxed);
      s.fdb_us.store(0, std::memory_order_relaxed);
      s.fdb_get_us.store(0, std::memory_order_relaxed);
      s.fdb_put_us.store(0, std::memory_order_relaxed);
      s.fdb_del_us.store(0, std::memory_order_relaxed);
      s.fdb_commit_us.store(0, std::memory_order_relaxed);
      s.fdb_scan_us.store(0, std::memory_order_relaxed);
      s.fdb_ops.store(0, std::memory_order_relaxed);
      s.disk_us.store(0, std::memory_order_relaxed);
    }
    txn_retries.store(0, std::memory_order_relaxed);
    txn_hard_failures.store(0, std::memory_order_relaxed);
    txn_max_retries_exceeded.store(0, std::memory_order_relaxed);
    reset_list_scan_stats();
  }
};

class ScopedRequestLatency {
 public:
  ScopedRequestLatency(LatencyStats& stats, OpType op)
      : stats_(stats), op_(op), start_(std::chrono::steady_clock::now()) {
    tl_request_latency = &req_;
  }

  ~ScopedRequestLatency() {
    tl_request_latency = nullptr;
    auto end = std::chrono::steady_clock::now();
    auto total_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start_).count();
    stats_.record(op_, total_us, req_);
  }

  ScopedRequestLatency(const ScopedRequestLatency&) = delete;
  ScopedRequestLatency& operator=(const ScopedRequestLatency&) = delete;

 private:
  LatencyStats& stats_;
  OpType op_;
  RequestLatency req_{};
  std::chrono::steady_clock::time_point start_;
};

}  // namespace kvrgw
