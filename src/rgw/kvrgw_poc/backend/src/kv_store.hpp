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

#include "error_codes.hpp"
#include "fdb.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace kvrgw {

class FdbFuture {
 public:
  FdbFuture() noexcept : f_(nullptr) {}
  explicit FdbFuture(FDBFuture* f) noexcept : f_(f) {}
  ~FdbFuture() { if (f_) fdb_future_destroy(f_); }

  FdbFuture(FdbFuture&& o) noexcept : f_(std::exchange(o.f_, nullptr)) {}
  FdbFuture& operator=(FdbFuture&& o) noexcept {
    if (this != &o) {
      if (f_) fdb_future_destroy(f_);
      f_ = std::exchange(o.f_, nullptr);
    }
    return *this;
  }

  FdbFuture(const FdbFuture&) = delete;
  FdbFuture& operator=(const FdbFuture&) = delete;

  FDBFuture* raw() const { return f_; }
  explicit operator bool() const { return f_ != nullptr; }
  bool is_ready() const { return f_ && fdb_future_is_ready(f_); }

 private:
  FDBFuture* f_;
};

struct RangeScanResult {
  std::string key;
  std::string value;
};

struct KVEntry {
  std::string_view key;
  std::string_view value;
};

// Zero-copy, non-movable, non-copyable holder for an FDB single-key GET future.
// Owns the future; exposes the value directly as string_view into the FDB buffer.
// Must call wait() before accessing value().
class FdbGetHolder {
public:
  explicit FdbGetHolder(FdbFuture&& f) noexcept : f_(std::move(f)) {}
  ~FdbGetHolder() = default;

  FdbGetHolder(const FdbGetHolder&) = delete;
  FdbGetHolder& operator=(const FdbGetHolder&) = delete;
  FdbGetHolder(FdbGetHolder&&) = delete;
  FdbGetHolder& operator=(FdbGetHolder&&) = delete;

  // Block until ready and populate internal buffer pointer.
  // Returns 0 on success, non-zero FDB error code on failure.
  fdb_error_t wait() noexcept;

  bool is_ready()  const noexcept { return f_.is_ready(); }
  bool present()   const noexcept { assert(ready_); return present_; }

  // Valid only if present() — string_view into FDB-owned buffer.
  // Lifetime tied to this holder.
  std::string_view value() const noexcept {
    assert(ready_ && present_);
    return {reinterpret_cast<const char*>(data_),
            static_cast<size_t>(len_)};
  }

private:
  FdbFuture       f_;
  const uint8_t*  data_{nullptr};
  int             len_{0};
  bool            present_{false};
  bool            ready_{false};
};

// Zero-copy, non-movable, non-copyable holder for an FDB range future.
// Owns the future; exposes the FDB buffer directly via range-for.
// Must call wait() before iterating.
class FdbRangeHolder {
public:
  explicit FdbRangeHolder(FdbFuture&& f) noexcept : f_(std::move(f)) {}
  ~FdbRangeHolder() = default;

  FdbRangeHolder(const FdbRangeHolder&) = delete;
  FdbRangeHolder& operator=(const FdbRangeHolder&) = delete;
  FdbRangeHolder(FdbRangeHolder&&) = delete;
  FdbRangeHolder& operator=(FdbRangeHolder&&) = delete;

  // Block until ready and populate internal buffer pointers.
  // Returns 0 on success, non-zero FDB error code on failure.
  fdb_error_t wait() noexcept;

  bool is_ready() const noexcept { return f_.is_ready(); }
  bool more()     const noexcept { return more_; }
  int  count()    const noexcept { return count_; }

  // Returns the last entry — valid after wait() with count() > 0.
  KVEntry back() const noexcept {
    assert(ready_ && count_ > 0);
    return {
      std::string_view(reinterpret_cast<const char*>(kv_[count_-1].key),
                       static_cast<size_t>(kv_[count_-1].key_length)),
      std::string_view(reinterpret_cast<const char*>(kv_[count_-1].value),
                       static_cast<size_t>(kv_[count_-1].value_length))
    };
  }

  class iterator {
  public:
    KVEntry operator*() const noexcept {
      return {
        std::string_view(reinterpret_cast<const char*>(kv_[i_].key),
                         static_cast<size_t>(kv_[i_].key_length)),
        std::string_view(reinterpret_cast<const char*>(kv_[i_].value),
                         static_cast<size_t>(kv_[i_].value_length))
      };
    }
    iterator& operator++() noexcept { ++i_; return *this; }
    bool operator!=(const iterator& o) const noexcept { return i_ != o.i_; }

  private:
    friend class FdbRangeHolder;
    iterator(const FDBKeyValue* kv, int i) noexcept : kv_(kv), i_(i) {}
    const FDBKeyValue* kv_;
    int i_;
  };

  iterator begin() const noexcept {
    assert(ready_ && "FdbRangeHolder::wait() must be called before iterating");
    return {kv_, 0};
  }
  iterator end() const noexcept { return {kv_, count_}; }

private:
  FdbFuture          f_;
  const FDBKeyValue* kv_{nullptr};
  int                count_{0};
  bool               more_{false};
  bool               ready_{false};
};

struct TxnStats {
  std::atomic<uint64_t> commits{0};
  std::atomic<uint64_t> conflicts{0};
  std::atomic<uint64_t> internals{0};
};

struct FdbPutStats {
  std::atomic<uint64_t> num_put{0};
  std::atomic<uint64_t> key_bytes{0};
  std::atomic<uint64_t> value_bytes{0};

  void record(size_t key_len, size_t value_len) {
    num_put.fetch_add(1, std::memory_order_relaxed);
    key_bytes.fetch_add(key_len, std::memory_order_relaxed);
    value_bytes.fetch_add(value_len, std::memory_order_relaxed);
  }

  void reset() {
    num_put.store(0, std::memory_order_relaxed);
    key_bytes.store(0, std::memory_order_relaxed);
    value_bytes.store(0, std::memory_order_relaxed);
  }
};

enum class TxnRetryPolicy { kRetry, kNoRetry };

class KvTransaction;

class KvStore {
 public:
  static std::expected<KvStore, fdb_error_t> create();
  ~KvStore();

  KvStore(KvStore&& other) noexcept;
  KvStore& operator=(KvStore&& other) noexcept;
  KvStore(const KvStore&) = delete;
  KvStore& operator=(const KvStore&) = delete;

  std::expected<std::optional<std::string>, fdb_error_t> get(std::string_view key);
  std::expected<void, fdb_error_t> set(std::string_view key, std::string_view value);
  // disable_ryw: when true, sets FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE on the
  // transaction before the GetRange call. This disables the client-side
  // Read-Your-Writes (RYW) cache — an in-process cache that stores all read
  // results within a transaction so subsequent reads of the same key return the
  // cached copy (including uncommitted writes).
  //
  // Listing operations (ListObjects, ListBuckets, ListObjectVersions) are
  // read-only, forward-only sequential scans that never revisit a key. Every
  // entry cached by the RYW layer is dead weight — allocated, populated, and
  // discarded without ever being consulted. Setting disable_ryw=true skips the
  // cache entirely, eliminating per-page allocation/population/deallocation
  // overhead on large scans.
  //
  // Must NOT be used on transactions that mix reads and writes (PUT, DELETE,
  // etc.) where reading back uncommitted writes is required for correctness.
  std::expected<std::vector<RangeScanResult>, fdb_error_t> range_scan(
      std::string_view start,
      std::string_view end,
      int limit,
      bool exclusive_scan = false,
      bool disable_ryw = false,
      FDBStreamingMode mode = FDB_STREAMING_MODE_EXACT);

  std::expected<std::unique_ptr<KvTransaction>, fdb_error_t> begin_transaction();

  std::expected<uint32_t, KvrgwErrorCode> allocate_rgw_id();

  template <typename Fn>
  auto run_transaction(Fn&& fn, TxnRetryPolicy policy = TxnRetryPolicy::kRetry)
      -> decltype(fn(std::declval<KvTransaction&>()));

  TxnStats& txn_stats() { return txn_stats_; }
  FdbPutStats& fdb_put_stats() { return fdb_put_stats_; }

  FDBDatabase* database() const;

 private:
  KvStore() = default;
  struct Impl;
  std::unique_ptr<Impl> impl_;
  TxnStats txn_stats_;
  FdbPutStats fdb_put_stats_;
};

class KvTransaction {
 public:
  ~KvTransaction();

  KvTransaction(const KvTransaction&) = delete;
  KvTransaction& operator=(const KvTransaction&) = delete;

  FdbFuture kv_async_get(std::string_view key);
  std::expected<std::optional<std::string>, fdb_error_t> kv_wait_get(FdbFuture& f);
  std::expected<std::optional<std::string>, fdb_error_t> kv_get(std::string_view key);

  std::expected<void, fdb_error_t> disable_ryw();
  FdbFuture kv_async_get_range(std::string_view start, bool exclusive_begin,
                               std::string_view end, int limit,
                               FDBStreamingMode mode = FDB_STREAMING_MODE_EXACT);
  std::expected<std::vector<RangeScanResult>, fdb_error_t> kv_wait_range(
      FdbFuture& f, bool* more = nullptr);

  // Zero-copy variant: returns an FdbGetHolder that must have wait() called
  // before accessing value(). The holder owns the future and the FDB buffer.
  FdbGetHolder kv_async_get_holder(std::string_view key);

  // Zero-copy variant: returns an FdbRangeHolder that must have wait() called
  // before iterating. The holder owns the future and the FDB buffer.
  FdbRangeHolder kv_async_get_range_holder(
      std::string_view start, bool exclusive_begin,
      std::string_view end, int limit,
      FDBStreamingMode mode = FDB_STREAMING_MODE_EXACT);

  void kv_put(std::string_view key, std::string_view value);
  void kv_del(std::string_view key);
  void kv_range_clear(std::string_view begin, std::string_view end);
  std::expected<std::vector<RangeScanResult>, fdb_error_t> kv_range_scan(
      std::string_view start,
      std::string_view end,
      int limit);

  std::expected<void, fdb_error_t> commit();
  FdbFuture commit_async();
  static std::expected<void, fdb_error_t> resolve_commit(FdbFuture& f);

 private:
  friend class KvStore;
  KvTransaction(KvStore& store, FDBTransaction* tr);
  KvStore& store_;
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

template <typename Fn>
auto KvStore::run_transaction(Fn&& fn, TxnRetryPolicy policy)
    -> decltype(fn(std::declval<KvTransaction&>())) {
  constexpr int kMaxRetries = 10;
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    auto tr_result = begin_transaction();
    if (!tr_result) {
      if (fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED, tr_result.error())) {
        if (policy == TxnRetryPolicy::kNoRetry) {
          txn_stats_.conflicts.fetch_add(1, std::memory_order_relaxed);
          return std::unexpected(tr_result.error());
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10 * (attempt + 1)));
        continue;
      }
      txn_stats_.internals.fetch_add(1, std::memory_order_relaxed);
      return std::unexpected(tr_result.error());
    }
    auto& tr = *tr_result;
    auto result = fn(*tr);
    if (!result) return result;
    auto rc = tr->commit();
    if (rc) {
      txn_stats_.commits.fetch_add(1, std::memory_order_relaxed);
      return result;
    }
    if (fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED, rc.error())) {
      txn_stats_.conflicts.fetch_add(1, std::memory_order_relaxed);
      if (policy == TxnRetryPolicy::kNoRetry) {
        return std::unexpected(rc.error());
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10 * (attempt + 1)));
      continue;
    }
    txn_stats_.internals.fetch_add(1, std::memory_order_relaxed);
    std::cerr << "run_transaction: non-retriable error " << rc.error() << "\n";
    return std::unexpected(rc.error());
  }
  txn_stats_.conflicts.fetch_add(1, std::memory_order_relaxed);
  return std::unexpected(static_cast<fdb_error_t>(1020));
}

}  // namespace kvrgw
