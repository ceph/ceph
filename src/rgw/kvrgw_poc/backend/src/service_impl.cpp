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

#include "service_impl.hpp"

#include "admin_server.hpp"
#include "bucket_policy.hpp"
#include "byte_range.hpp"
#include "constants.hpp"
#include "error_codes.hpp"
#include "gc_value.hpp"
#include "keys.hpp"
#include "kv_store.hpp"
#include "l_keys.hpp"
#include "object_value.hpp"
#include "ref_count.hpp"
#include "tenant_value.hpp"
#include "tier_config_state.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <endian.h>
#include <iomanip>
#include <iostream>
#include <limits>
#include <openssl/evp.h>
#include <span>
#include <sstream>
#include <string_view>
#include <thread>
#include <vector>

using namespace std::chrono;
namespace kvrgw {
constexpr FDBStreamingMode streamingMode = FDB_STREAMING_MODE_EXACT;

namespace {

// base64: every 3 raw bytes encode to 4 characters (with '=' padding as needed).
constexpr int kBase64RawBlockSize     = 3;
constexpr int kBase64EncodedBlockSize = 4;

//--------------------------------------------------------------------------------
std::string base64_encode(std::string_view data)
{
  const int encoded_len = kBase64EncodedBlockSize *
      ((static_cast<int>(data.size()) + kBase64RawBlockSize - 1) / kBase64RawBlockSize);

  std::string out;
  out.resize(static_cast<size_t>(encoded_len));
  const int len = EVP_EncodeBlock(reinterpret_cast<unsigned char *>(out.data()),
                                  reinterpret_cast<const unsigned char *>(data.data()),
                                  static_cast<int>(data.size()));

  out.resize(static_cast<size_t>(len));
  return out;
}

//--------------------------------------------------------------------------------
std::optional<std::string> base64_decode(std::string_view data)
{
  std::string out;
  out.resize((data.size() * kBase64RawBlockSize) / kBase64EncodedBlockSize +
             kBase64EncodedBlockSize);
  const int len = EVP_DecodeBlock(reinterpret_cast<unsigned char *>(out.data()),
                                  reinterpret_cast<const unsigned char *>(data.data()),
                                  static_cast<int>(data.size()));
  if (len < 0) {
    return std::nullopt;
  }
  // EVP_DecodeBlock always returns (data.size()/4)*3 bytes and does not
  // account for '=' padding — strip the phantom trailing byte(s) so the
  // returned length matches the true decoded payload size.
  int pad = 0;
  if (!data.empty() && data.back() == '=') {
    ++pad;
  }

  if (data.size() > 1 && data[data.size() - 2] == '=') {
    ++pad;
  }

  out.resize(static_cast<size_t>(len - pad));
  return out;
}

//--------------------------------------------------------------------------------
bool is_valid_bucket_name(std::string_view name)
{
  if (name.size() < AWS_MinBucketNameLen || name.size() > AWS_MaxBucketNameLen) {
    return false;
  }
  for (size_t i = 0; i < name.size(); ++i) {
    const char c = name[i];
    if (c >= 'a' && c <= 'z') {
      continue;
    }
    if (c >= '0' && c <= '9') {
      continue;
    }
    if (c == '-') {
      continue;
    }
    if (c == '.') {
      continue;
    }
    return false;
  }
  if (name.front() == '-' || name.front() == '.') {
    return false;
  }
  if (name.back() == '-' || name.back() == '.') {
    return false;
  }
  if (name.find("..") != std::string::npos) {
    return false;
  }
  if (name.find(".-") != std::string::npos) {
    return false;
  }
  if (name.find("-.") != std::string::npos) {
    return false;
  }
  // Reject IP-address-like names (4 groups of digits separated by dots)
  int dots = 0, groups = 0;
  bool all_digits_between_dots = true;
  for (char c : name) {
    if (c == '.') {
      ++dots;
      ++groups;
    }
    else if (c < '0' || c > '9') {
      all_digits_between_dots = false;
    }
  }
  if (dots == 3 && all_digits_between_dots && groups == 3) {
    return false;
  }
  return true;
}

} // namespace

// --- BatchCommitQueue ---

BatchCommitQueue::BatchCommitQueue(KvRgwServiceImpl &service)
    : service_(service)
{
  if (const char *v = std::getenv("KVRGW_BATCH_MAX_INFLIGHT")) {
    max_inflight_ = std::max(1, std::min(4, std::atoi(v)));
  }
}

BatchCommitQueue::~BatchCommitQueue() { stop(); }

void BatchCommitQueue::start(int num_threads)
{
  if (!workers_.empty()) {
    return;
  }
  stop_.store(false, std::memory_order_relaxed);
  int n = std::max(1, num_threads);
  for (int i = 0; i < n; ++i) {
    workers_.emplace_back(&BatchCommitQueue::run, this);
  }
}

void BatchCommitQueue::stop()
{
  stop_.store(true, std::memory_order_relaxed);
  cv_.notify_all();
  for (auto &w : workers_) {
    if (w.joinable()) {
      w.join();
    }
  }
  workers_.clear();
  std::lock_guard lock(mu_);
  for (auto &entry : pending_) {
    entry.promise.set_value(KVRGW_ERR_INTERNAL);
  }
  pending_.clear();
}

std::future<BatchPutResult> BatchCommitQueue::enqueue(BatchCommitEntry entry)
{
  auto fut = entry.promise.get_future();
  entry.enqueued_at = std::chrono::steady_clock::now();
  bool should_notify = false;
  {
    std::lock_guard lock(mu_);
    pending_.push_back(std::move(entry));
    auto tc = service_.tier_config_state_.active_copy();
    should_notify = static_cast<int>(pending_.size()) >= tc.batch_size;
  }
  if (should_notify) {
    cv_.notify_one();
  }
  return fut;
}

void BatchCommitQueue::run()
{
  WorkerState state{};

  while (!stop_.load(std::memory_order_relaxed)) {
    bool any_progress = false;

    for (int i = 0; i < max_inflight_; ++i) {
      auto &ib = state.slots[i];
      if (ib.phase == InFlightBatch::PHASE_EMPTY) {
        continue;
      }

      if (ib.phase == InFlightBatch::PHASE_1 &&
          ib.step == InFlightBatch::STEP_COMMIT_ISSUED) {
        if (!ib.commit_future.is_ready()) {
          continue;
        }
        any_progress = true;
        fdb_error_t err = KvTransaction::resolve_commit(ib.commit_future);
        if (err) {
          auto ec = fdb_to_error(err);
          service_.error_stats_->record(ec);
          for (int j = 0; j < ib.entry_count; ++j) {
            ib.entries[j].promise.set_value(ec);
          }
          ib.reset();
          continue;
        }
        bool disk_ok = true;
        for (int j = 0; j < ib.entry_count; ++j) {
          if (ib.entries[j].chunk_type != CHUNK_STORAGE) {
            continue;
          }
          auto write_ec = service_.data_store_.write(
              ib.entries[j].value.hdr.ref_tag.filename(), ib.entries[j].data);
          if (write_ec) {
            service_.error_stats_->record(KVRGW_ERR_INTERNAL);
            for (int k = 0; k < ib.entry_count; ++k) {
              ib.entries[k].promise.set_value(KVRGW_ERR_INTERNAL);
            }
            disk_ok = false;
            break;
          }
        }
        if (!disk_ok) {
          ib.reset();
          continue;
        }
        if (service_.err_insertion_.is_error_active(
                FaultType::kAbortAfterBatchPhase2)) {
          for (int j = 0; j < ib.entry_count; ++j) {
            ib.entries[j].promise.set_value(KVRGW_ERR_INTERNAL);
          }
          ib.reset();
          continue;
        }
        ib.phase = InFlightBatch::PHASE_3;
        ib.step = InFlightBatch::STEP_WORKING;
        ib.txn.reset();
        ib.commit_future = FdbFuture();
      }

      if (ib.phase == InFlightBatch::PHASE_FALLBACK) {
        any_progress = true;
        constexpr int kMaxRetries = 3;
        if (ib.start_attempt > 0 && ib.start_attempt <= kMaxRetries) {
          // Retriable error incremented start_attempt — retry start_batch.
          ib.phase = InFlightBatch::PHASE_EMPTY;
          start_batch(ib);
        } else {
          fallback_to_single(ib);
        }
        continue;
      }

      if (ib.phase == InFlightBatch::PHASE_3 &&
          ib.step == InFlightBatch::STEP_WORKING) {
        any_progress = true;
        if (!do_phase3_work(ib)) {
          ib.reset();
          continue;
        }
      }

      if (ib.phase == InFlightBatch::PHASE_3 &&
          ib.step == InFlightBatch::STEP_COMMIT_ISSUED) {
        if (!ib.commit_future.is_ready()) {
          continue;
        }
        any_progress = true;
        fdb_error_t err = KvTransaction::resolve_commit(ib.commit_future);
        if (!err) {
          // Record committed stats.
          int64_t bs = static_cast<int64_t>(ib.entry_count);
          stats_.committed_batch_commits.fetch_add(1, std::memory_order_relaxed);
          stats_.committed_total_entries.fetch_add(bs, std::memory_order_relaxed);
          if (bs == 1) {
            stats_.committed_single_entry_batches.fetch_add(1, std::memory_order_relaxed);
          }
          if (ib.phase1_key_count == 0) {
            stats_.committed_inline_only_batch_commits.fetch_add(1, std::memory_order_relaxed);
            stats_.committed_inline_only_batch_entries.fetch_add(bs, std::memory_order_relaxed);
          }
          for (int j = 0; j < ib.entry_count; ++j) {
            ib.entries[j].promise.set_value(BatchPutResult{
                KVRGW_ERR_OK, ib.entries[j].value.hdr.version_id});
          }
          ib.reset();
        }
        else {
          auto ec = fdb_to_error(err);
          if (is_retriable(ec) && ib.p3_attempt < 10) {
            service_.latency_stats_.txn_retries.fetch_add(
                1, std::memory_order_relaxed);
            ib.p3_attempt++;
            ib.txn.reset();
            ib.commit_future = FdbFuture();
            ib.step = InFlightBatch::STEP_WORKING;
          }
          else {
            service_.error_stats_->record(ec);
            if (!is_retriable(ec)) {
              service_.latency_stats_.txn_hard_failures.fetch_add(
                  1, std::memory_order_relaxed);
            }
            else {
              service_.latency_stats_.txn_max_retries_exceeded.fetch_add(
                  1, std::memory_order_relaxed);
            }
            for (int j = 0; j < ib.entry_count; ++j) {
              ib.entries[j].promise.set_value(ec);
            }
            ib.reset();
          }
        }
      }
    }

    int active_count = 0;
    for (int i = 0; i < max_inflight_; ++i) {
      if (state.slots[i].phase != InFlightBatch::PHASE_EMPTY) {
        active_count++;
      }
    }

    if (active_count < max_inflight_) {
      std::vector<BatchCommitEntry> local_batch;
      {
        std::unique_lock lock(mu_);
        auto tc = service_.tier_config_state_.active_copy();
        int batch_size = tc.batch_size;
        auto timeout = std::chrono::microseconds(tc.batch_timeout_us);

        if (active_count > 0) {
          if (static_cast<int>(pending_.size()) < batch_size) {
            if (pending_.empty()) {
              goto no_new_work;
            }
            auto age = std::chrono::steady_clock::now() - pending_.front().enqueued_at;
            if (age < timeout) {
              goto no_new_work;
            }
          }
        }
        else {
          while (true) {
            if (stop_.load(std::memory_order_relaxed)) {
              return;
            }
            if (static_cast<int>(pending_.size()) >= batch_size) {
              break;
            }
            if (!pending_.empty()) {
              auto age = std::chrono::steady_clock::now() - pending_.front().enqueued_at;
              if (age >= timeout) {
                break;
              }
              auto remaining = duration_cast<microseconds>(timeout - age);
              cv_.wait_for(lock, remaining);
            }
            else {
              cv_.wait_for(lock, timeout);
            }
          }
        }

        if (pending_.empty()) {
          goto no_new_work;
        }

        stats_.total_queue_size_at_extract.fetch_add(
            static_cast<int64_t>(pending_.size()), std::memory_order_relaxed);

        std::unordered_set<std::string> conflict_set;
        int batch_size_actual =
            service_.tier_config_state_.active_copy().batch_size;
        size_t max_scan = pending_.size();
        size_t scanned = 0;
        while (static_cast<int>(local_batch.size()) < batch_size_actual &&
               scanned < max_scan) {
          auto entry = std::move(pending_.front());
          pending_.pop_front();
          scanned++;
          std::string key = entry.bucket_name + "/" + entry.object_name;
          if (conflict_set.count(key)) {
            stats_.conflict_pushbacks.fetch_add(1, std::memory_order_relaxed);
            pending_.push_back(std::move(entry));
            continue;
          }
          conflict_set.insert(key);
          local_batch.push_back(std::move(entry));
        }
      }

      if (!local_batch.empty()) {
        for (int i = 0; i < max_inflight_; ++i) {
          if (state.slots[i].phase != InFlightBatch::PHASE_EMPTY) {
            continue;
          }
          auto &ib = state.slots[i];
          ib.entry_count = static_cast<uint8_t>(local_batch.size());
          for (int j = 0; j < ib.entry_count; ++j) {
            ib.entries[j] = std::move(local_batch[j]);
          }
          start_batch(ib);
          break;
        }
      }
    }
  no_new_work:

    if (!any_progress && active_count == 0) {
      sleep_for_usec(100);
    }
    else if (!any_progress && active_count > 0) {
      sleep_for_usec(50);
    }
  }

  for (int i = 0; i < max_inflight_; ++i) {
    auto &ib = state.slots[i];
    if (ib.phase != InFlightBatch::PHASE_EMPTY && ib.commit_future) {
      [[maybe_unused]] auto _ =
          fdb_future_block_until_ready(ib.commit_future.raw());
    }
    if (ib.phase != InFlightBatch::PHASE_EMPTY) {
      for (int j = 0; j < ib.entry_count; ++j) {
        ib.entries[j].promise.set_value(KVRGW_ERR_INTERNAL);
      }
      ib.reset();
    }
  }
}

//--------------------------------------------------------------------------------
void BatchCommitQueue::fallback_to_single(InFlightBatch &ib)
{
  stats_.fallback_to_single_count.fetch_add(1, std::memory_order_relaxed);
  stats_.fallback_to_single_entries.fetch_add(
      static_cast<int64_t>(ib.entry_count), std::memory_order_relaxed);
  for (int i = 0; i < ib.entry_count; ++i) {
    auto &e = ib.entries[i];
    const PutCondition *cond_ptr = e.has_cond ? &e.cond : nullptr;
    auto ec = service_.put_object_single_txn(e.tenant_id, e.bucket_name,
                                             e.object_name, e.value, e.data, nullptr,
                                             cond_ptr, std::span<const uint8_t>(e.tag_encoded),
                                             std::span<const uint8_t>(e.metadata_encoded.data(),
                                                                      e.metadata_encoded_size));
    e.promise.set_value(BatchPutResult{ec, e.value.hdr.version_id});
  }
  ib.reset();
}

//--------------------------------------------------------------------------------
void BatchCommitQueue::record_batch_stats(InFlightBatch &ib)
{
  auto now = std::chrono::steady_clock::now();
  int64_t bs = static_cast<int64_t>(ib.entry_count);
  stats_.attempted_batch_commits.fetch_add(1, std::memory_order_relaxed);
  stats_.attempted_total_entries_batch.fetch_add(bs, std::memory_order_relaxed);
  if (bs == 1) {
    stats_.attempted_single_entry_batches.fetch_add(1, std::memory_order_relaxed);
  }
  auto cur_min = stats_.attempted_min_batch_size.load(std::memory_order_relaxed);
  while ((cur_min == 0 || bs < cur_min) &&
         !stats_.attempted_min_batch_size.compare_exchange_weak(cur_min, bs)) {
  }
  auto cur_max = stats_.attempted_max_batch_size.load(std::memory_order_relaxed);
  while (bs > cur_max &&
         !stats_.attempted_max_batch_size.compare_exchange_weak(cur_max, bs)) {
  }
  auto oldest = ib.entries[0].enqueued_at;
  for (int i = 1; i < ib.entry_count; ++i) {
    if (ib.entries[i].enqueued_at < oldest) {
      oldest = ib.entries[i].enqueued_at;
    }
  }
  int64_t wait_us = duration_cast<microseconds>(now - oldest).count();
  stats_.attempted_total_wait_us.fetch_add(wait_us, std::memory_order_relaxed);
  auto wmin = stats_.attempted_min_wait_us.load(std::memory_order_relaxed);
  while ((wmin == 0 || wait_us < wmin) &&
         !stats_.attempted_min_wait_us.compare_exchange_weak(wmin, wait_us)) {
  }
  auto wmax = stats_.attempted_max_wait_us.load(std::memory_order_relaxed);
  while (wait_us > wmax &&
         !stats_.attempted_max_wait_us.compare_exchange_weak(wmax, wait_us)) {
  }
}

//--------------------------------------------------------------------------------
void BatchCommitQueue::start_batch(InFlightBatch &ib)
{
  constexpr int kMaxRetries = 3;

  // On any error: mark PHASE_FALLBACK. If retriable and under retry limit,
  // increment start_attempt so the run() loop retries start_batch.
  // Otherwise start_attempt stays at its limit and run() calls fallback_to_single.
  auto handle_error = [&](KvrgwErrorCode ec) {
    service_.error_stats_->record(ec);
    if (is_retriable(ec) && ib.start_attempt < kMaxRetries) {
      ib.start_attempt++;
    }
    ib.phase = InFlightBatch::PHASE_FALLBACK;
  };

  ib.phase1_key_count = 0;
  bool has_storage_entry = false;

  // ---------------------------------------------------------------
  // Step 1: resolve bucket_id for every entry.
  // ---------------------------------------------------------------
  for (int i = 0; i < ib.entry_count; ++i) {
    auto &e = ib.entries[i];
    if (e.chunk_type == CHUNK_STORAGE) {
      has_storage_entry = true;
    }
    auto cached_bid = service_.get_bucket_id_cached(e.tenant_id, e.bucket_name);
    if (!cached_bid || !*cached_bid) {
      auto ec = cached_bid ? KVRGW_ERR_NO_SUCH_BUCKET
                           : fdb_to_error(cached_bid.error());
      handle_error(ec);
      return;
    }
    e.bucket_id = (**cached_bid).bucket_id;
  }

  // Only storage-tier PUTs require Phase 1 P:O entries.
  if (!has_storage_entry) {
    stats_.attempted_inline_only_batch_commits.fetch_add(1, std::memory_order_relaxed);
    stats_.attempted_inline_only_batch_entries.fetch_add(
        static_cast<int64_t>(ib.entry_count), std::memory_order_relaxed);
    ib.phase = InFlightBatch::PHASE_3;
    ib.step  = InFlightBatch::STEP_WORKING;
    return;
  }

  // ---------------------------------------------------------------
  // Step 2: accumulate storage-tier entries into Group P:O or single
  // P:O keys, flushing when the accumulator is full or the variable
  // section would overflow 2 × AWS_MaxObjectNameLen.
  // ---------------------------------------------------------------
  static constexpr size_t kMaxNameBytes = 2 * AWS_MaxObjectNameLen;

  GroupPoValue accumulator{};
  uint16_t name_bytes{0};

  auto flush_as_group_po = [&](std::unique_ptr<KvTransaction>& tr) {
    const auto ref_view = accumulator.entries[0].gc.chunk.ref_tag_view();
    KeyBuf& key = ib.phase1_keys[ib.phase1_key_count];
    make_group_po_key(ref_view, key);
    GroupPoValueBuf val_buf;
    make_group_po_value(val_buf, accumulator);
    tr->kv_put(key.view(), val_buf.view());
    ib.phase1_key_count++;
    accumulator = GroupPoValue{};
    name_bytes = 0;
  };

  auto flush_as_single_po = [&](std::unique_ptr<KvTransaction>& tr) {
    const auto& gc = accumulator.entries[0].gc;
    const std::string_view object_name = accumulator.entries[0].object_name;
    KeyBuf& key = ib.phase1_keys[ib.phase1_key_count];
    make_po_key(gc.chunk.get_bucket_id(), gc.chunk.ref_tag_view(), key);
    PoValueBuf val_buf;
    make_po_value(val_buf, gc, object_name);
    tr->kv_put(key.view(), val_buf.view());
    ib.phase1_key_count++;
    accumulator = GroupPoValue{};
    name_bytes = 0;
  };

  auto tr_result = service_.store_.begin_transaction();
  if (!tr_result) {
    handle_error(fdb_to_error(tr_result.error()));
    return;
  }
  std::unique_ptr<KvTransaction> phase1_txn = std::move(*tr_result);

  for (int i = 0; i < ib.entry_count; ++i) {
    auto &e = ib.entries[i];
    if (e.chunk_type != CHUNK_STORAGE) continue;

    // Set created_at_unix once on the first storage entry.
    if (accumulator.count == 0 && name_bytes == 0) {
      accumulator.created_at_unix = static_cast<uint32_t>(now_unix());
    }

    const size_t this_name_len = e.object_name.size();

    // Flush accumulator before adding if it would overflow.
    if (accumulator.count == kMaxGroupPoSize ||
        (accumulator.count >= 2 &&
         static_cast<size_t>(name_bytes) + this_name_len > kMaxNameBytes)) {
      flush_as_group_po(phase1_txn);
    }

    auto& slot = accumulator.entries[accumulator.count];
    slot.gc.chunk = ChunkDescriptor(CHUNK_STORAGE, e.value.hdr.ref_tag, e.bucket_id);
    slot.gc.object_size = e.value.hdr.size;
    slot.gc.mtime = accumulator.created_at_unix;
    slot.object_name = e.object_name;
    accumulator.count++;
    name_bytes = static_cast<uint16_t>(name_bytes + this_name_len);
  }

  // Flush remainder.
  if (accumulator.count >= 2) {
    flush_as_group_po(phase1_txn);
  } else if (accumulator.count == 1) {
    flush_as_single_po(phase1_txn);
  }

  record_batch_stats(ib);

  // Commit Phase 1 transaction (all P:O / Group P:O keys).
  // ---------------------------------------------------------------
  ib.txn = std::move(phase1_txn);
  ib.commit_future = ib.txn->commit_async();
  ib.phase = InFlightBatch::PHASE_1;
  ib.step = InFlightBatch::STEP_COMMIT_ISSUED;
}

//--------------------------------------------------------------------------------
bool BatchCommitQueue::do_phase3_work(InFlightBatch &ib)
{
  constexpr int kMaxRetries = 3;
  auto tr_result = service_.store_.begin_transaction();
  if (!tr_result) {
    auto ec = fdb_to_error(tr_result.error());
    service_.error_stats_->record(ec);
    if (is_retriable(ec) && ib.p3_attempt < kMaxRetries) {
      ib.p3_attempt++;
      return true;
    }
    for (int j = 0; j < ib.entry_count; ++j) {
      ib.entries[j].promise.set_value(ec);
    }
    return false;
  }
  auto &tr = *tr_result;

  KvRgwServiceImpl::VerifiedBucket verified[kMaxBatchSize];
  int verified_count = 0;
  KvRgwServiceImpl::PutContext ctxs[kMaxBatchSize];

  // Step 1: Issue one async kv_get per Phase 1 key (pipelined).
  FdbFuture phase1_futures[InFlightBatch::kMaxPhase1Keys];
  for (int k = 0; k < ib.phase1_key_count; ++k) {
    phase1_futures[k] = tr->kv_async_get(ib.phase1_keys[k].view());
  }

  // Step 2: Issue async bucket + object-key reads via put_prepare.
  // Pass is_storage_tier=false so put_prepare does NOT issue a per-entry
  // single P:O read (ctx.f_po stays empty). Set ctx.is_storage_tier=true
  // afterward so put_finalize still takes the storage path.
  struct IssuedBkt {
    tenant_id_t tid;
    const std::string *name;
  };
  IssuedBkt issued[kMaxBatchSize];
  int issued_count = 0;

  bool any_failed = false;
  KvrgwErrorCode fail_ec = KVRGW_ERR_OK;

  for (int i = 0; i < ib.entry_count; ++i) {
    auto &entry = ib.entries[i];

    bool need_bucket = true;
    for (int j = 0; j < issued_count; ++j) {
      if (issued[j].tid == entry.tenant_id &&
          *issued[j].name == entry.bucket_name) {
        need_bucket = false;
        break;
      }
    }
    if (need_bucket) {
      issued[issued_count++] = {entry.tenant_id, &entry.bucket_name};
    }

    // is_storage_tier=false: suppresses per-entry P:O read in put_prepare.
    KvRgwServiceImpl::PutInTxnParams params{
        entry.tenant_id,
        entry.bucket_name,
        entry.bucket_id,
        entry.object_name,
        entry.value,
        &entry.data,
        nullptr,
        std::span<const uint8_t>(entry.tag_encoded),
        !need_bucket,
        false,   // is_storage_tier suppressed — no per-entry P:O read
        std::span<const uint8_t>(entry.metadata_encoded.data(),
                                 entry.metadata_encoded_size)};
    ctxs[i] = service_.put_prepare(*tr, params, need_bucket);
    // Restore correct type so put_finalize takes the right path.
    ctxs[i].is_storage_tier = entry.chunk_type == CHUNK_STORAGE;
  }

  // Step 3: put_finalize — resolves bucket + object reads, writes object values.
  // put_finalize verifies bucket_id freshness via resolve_bucket_verify.
  // ctx.f_po is empty for all entries, so no per-entry P:O existence check inside.
  if (!any_failed) {
    for (int i = 0; i < ib.entry_count; ++i) {
      auto &entry = ib.entries[i];
      const PutCondition *cond_ptr = entry.has_cond ? &entry.cond : nullptr;
      KvRgwServiceImpl::PutInTxnParams params{
          entry.tenant_id,
          entry.bucket_name,
          entry.bucket_id,
          entry.object_name,
          entry.value,
          &entry.data,
          cond_ptr,
          std::span<const uint8_t>(entry.tag_encoded),
          false,
          entry.chunk_type == CHUNK_STORAGE,
          std::span<const uint8_t>(entry.metadata_encoded.data(),
                                   entry.metadata_encoded_size)};
      auto ec = service_.put_finalize(*tr, ctxs[i], params, verified,
                                      verified_count, nullptr);
      if (ec != KVRGW_ERR_OK) {
        any_failed = true;
        fail_ec = ec;
        break;
      }
    }
  }

  // Step 4: Verify all Phase 1 keys are present (one wait per key).
  if (!any_failed) {
    for (int k = 0; k < ib.phase1_key_count; ++k) {
      FdbGetHolder holder(std::move(phase1_futures[k]));
      const fdb_error_t err = holder.wait();
      if (err) {
        any_failed = true;
        fail_ec = fdb_to_error(err);
        break;
      }
      if (!holder.present()) {
        any_failed = true;
        fail_ec = KVRGW_ERR_INTERNAL;
        break;
      }
    }
  }

  // Step 5: Delete all Phase 1 keys (Group P:O and single P:O).
  if (!any_failed) {
    for (int k = 0; k < ib.phase1_key_count; ++k) {
      tr->kv_del(ib.phase1_keys[k].view());
    }
  }

  if (any_failed) {
    service_.error_stats_->record(fail_ec);
    if (is_retriable(fail_ec) && ib.p3_attempt < kMaxRetries) {
      ib.p3_attempt++;
      return true;
    }
    for (int j = 0; j < ib.entry_count; ++j) {
      ib.entries[j].promise.set_value(fail_ec);
    }
    return false;
  }

  ib.txn = std::move(tr_result.value());
  ib.commit_future = ib.txn->commit_async();
  ib.step = InFlightBatch::STEP_COMMIT_ISSUED;
  return true;
}

// --- End BatchCommitQueue ---

KvRgwServiceImpl::KvRgwServiceImpl(KvStore &store, DataStore &data_store,
                                   RefTagGenerator &ref_tags,
                                   TierConfigState &tier_config,
                                   Sweeper &sweeper)
    : store_(store), data_store_(data_store), ref_tags_(ref_tags),
      tier_config_state_(tier_config), sweeper_(sweeper),
      error_stats_(std::make_unique<ErrorStats>()), batch_queue_(*this)
{
  auto tc = tier_config.active_copy();
  if (tc.batch_size > 1) {
    batch_queue_.start(tc.batch_threads);
  }
}

//--------------------------------------------------------------------------------
void KvRgwServiceImpl::put_bucket_cache(tenant_id_t tenant_id,
                                        std::string_view bucket_name,
                                        bucket_id_t bucket_id,
                                        uint8_t access_flags,
                                        VersioningState versioning_state)
{
  std::unique_lock lock(cache_mu_);
  auto timepoint = std::chrono::steady_clock::now();
  // The only allocation point in the bucket-cache API: building the owning
  // map key from the caller's view, done once here on insert/update.
  bucket_cache_[BucketCacheKey{tenant_id, std::string(bucket_name)}] =
      BucketCacheEntry{bucket_id, access_flags, versioning_state, timepoint};
}

//--------------------------------------------------------------------------------
void KvRgwServiceImpl::invalidate_bucket_cache(tenant_id_t tenant_id,
                                               std::string_view bucket_name)
{
  std::unique_lock lock(cache_mu_);
  // Heterogeneous find + erase(iterator) -- allocation-free (erase iterator)
  const auto it = bucket_cache_.find(BucketCacheKeyView{tenant_id, bucket_name});
  if (it != bucket_cache_.end()) {
    bucket_cache_.erase(it);
  }
}

//--------------------------------------------------------------------------------
std::expected<std::optional<KvRgwServiceImpl::BucketState>, fdb_error_t>
KvRgwServiceImpl::get_bucket_id_cached(tenant_id_t tenant_id,
                                       std::string_view bucket_name)
{
  {
    std::shared_lock lock(cache_mu_);
    // Heterogeneous find -- no allocation for the lookup.
    const auto it = bucket_cache_.find(BucketCacheKeyView{tenant_id, bucket_name});
    if (it != bucket_cache_.end()) {
      const auto age = std::chrono::steady_clock::now() - it->second.cached_at;
      if (age <= kBucketCacheTtl) {
        return std::optional<BucketState>(
            BucketState{it->second.bucket_id, it->second.access_flags,
                        it->second.versioning_state, 0});
      }
    }
  }
  // read_bucket_state() still takes an owning std::string (out of scope for
  // this refactor) -- only materialized here, on an actual cache miss, right
  // before the much more expensive KV round-trip it's about to perform.
  return read_bucket_state(tenant_id, bucket_name);
}

//--------------------------------------------------------------------------------
std::expected<std::optional<KvRgwServiceImpl::BucketState>, fdb_error_t>
KvRgwServiceImpl::read_bucket_state(tenant_id_t tenant_id,
                                    std::string_view bucket_name)
{
  if (!is_valid_bucket_name(bucket_name)) {
    return std::optional<BucketState>(std::nullopt);
  }
  KeyBuf key;
  make_bucket_key(tenant_id, bucket_name, key);
  auto value = store_.get(key.view());
  if (!value) {
    return std::unexpected(value.error());
  }
  if (!*value) {
    invalidate_bucket_cache(tenant_id, bucket_name);
    return std::optional<BucketState>(std::nullopt);
  }
  auto bv = parse_bucket_value(**value);
  if (!bv) {
    return std::optional<BucketState>(std::nullopt);
  }
  BucketState state{bv->bucket_id, bv->access_flags, bv->versioning_state,
                    bv->created_at_unix};
  put_bucket_cache(tenant_id, bucket_name, state.bucket_id, state.access_flags,
                   state.versioning_state);
  return std::optional<BucketState>(state);
}

//--------------------------------------------------------------------------------
namespace {

// Fixed-capacity, std::string-like bucket-name buffer -- no heap allocation.
// Capacity matches AWS_MaxBucketNameLen (63); a single length byte suffices.
class bucket_name_t {
  char data_[AWS_MaxBucketNameLen]{};
  uint8_t len_{};

 public:
  bucket_name_t() = default;

  // Returns false (leaves *this cleared) if name exceeds capacity.
  bool assign(std::string_view name) {
    if (name.size() > AWS_MaxBucketNameLen) [[unlikely]] {
      return false;
    }
    std::memcpy(data_, name.data(), name.size());
    len_ = static_cast<uint8_t>(name.size());
    return true;
  }

  std::string_view view() const { return {data_, len_}; }
  size_t size() const { return len_; }

  bool operator==(const bucket_name_t& o) const {
    return len_ == o.len_ && std::memcmp(data_, o.data_, len_) == 0;
  }
};

// Thread-local micro-cache in front of KvRgwServiceImpl::bucket_cache_,
// consulted (and maintained) only by bucket_cache_fast_refresh(). Touches
// only this thread's own memory -- no locks, no atomics, no cross-core
// traffic -- so it's cheap to check on every resolve_bucket_verify() call.
// Fixed-size, statically allocated: 10 entries x (64B name + 4B tenant_id +
// 8B bucket_id + 8B last-check timestamp).
constexpr int kLocalBucketCacheSize = 10;

struct LocalBucketCacheEntry {
  bucket_name_t bucket_name{};
  bucket_id_t bucket_id{};
  int64_t last_check_ns{};
  tenant_id_t tenant_id{};
};

thread_local LocalBucketCacheEntry g_local_bucket_cache[kLocalBucketCacheSize];
}  // namespace

//--------------------------------------------------------------------------------
void KvRgwServiceImpl::bucket_cache_fast_refresh(tenant_id_t tenant_id,
                                                 std::string_view bucket_name_in,
                                                 bucket_id_t bucket_id,
                                                 uint8_t access_flags,
                                                 VersioningState versioning_state)
{
  constexpr int64_t kHalfTtlNs = duration_cast<nanoseconds>(kBucketCacheTtl / 2).count();
  bucket_name_t bucket_name;
  if (!bucket_name.assign(bucket_name_in)) [[unlikely]] {
    // Defensive only -- AWS_MaxBucketNameLen (63) already guarantees this
    // never happens, but never let an oversized name touch the fixed-size
    // local table.
    put_bucket_cache(tenant_id, bucket_name_in, bucket_id, access_flags, versioning_state);
    return;
  }

  const int64_t now = steady_now_ns();
  int oldest_idx = 0;
  // use first entry as an LRU anchor
  int64_t oldest_time = g_local_bucket_cache[0].last_check_ns;

  for (int i = 0; i < kLocalBucketCacheSize; ++i) {
    auto &e = g_local_bucket_cache[i];
    if (e.tenant_id == tenant_id && e.bucket_name == bucket_name) {
      if (e.bucket_id == bucket_id && (now - e.last_check_ns) <= kHalfTtlNs) {
        return;  // fresh per this thread's own record already exists
      }

      // Locally stale -- refresh unconditionally, no reader-lock pre-check.
      put_bucket_cache(tenant_id, bucket_name_in, bucket_id, access_flags, versioning_state);
      e.bucket_id = bucket_id;
      e.last_check_ns = now;
      return;
    }

    if (e.last_check_ns < oldest_time) {
      oldest_time = e.last_check_ns;
      oldest_idx = i;
    }
  }

  // Not found locally -- refresh unconditionally and insert at the
  // least-recently-checked slot
  put_bucket_cache(tenant_id, bucket_name_in, bucket_id, access_flags, versioning_state);
  auto &slot = g_local_bucket_cache[oldest_idx];
  slot.bucket_name = bucket_name;
  slot.tenant_id = tenant_id;
  slot.bucket_id = bucket_id;
  slot.last_check_ns = now;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::check_access(tenant_id_t tenant_id,
                                              std::string_view bucket_name,
                                              const BucketState &cached,
                                              uint8_t deny_mask)
{
  if (!(cached.access_flags & deny_mask)) {
    return KVRGW_ERR_OK;
  }

  // Before denying access -> get a fresh copy of bucket state and double check
  auto fresh = read_bucket_state(tenant_id, bucket_name);
  if (!fresh) {
    return fdb_to_error(fresh.error());
  }
  if (!*fresh) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  if ((*fresh)->access_flags & deny_mask) {
    return KVRGW_ERR_ACCESS_DENIED;
  }
  return KVRGW_ERR_OK;
}

//--------------------------------------------------------------------------------
FdbGetHolder KvRgwServiceImpl::issue_bucket_get(KvTransaction &tr,
                                                tenant_id_t tenant_id,
                                                std::string_view bucket_name)
{
  KeyBuf bucket_key;
  make_bucket_key(tenant_id, bucket_name, bucket_key);
  return tr.kv_async_get_holder(bucket_key.view());
}

//--------------------------------------------------------------------------------
std::expected<KvRgwServiceImpl::BucketState, KvrgwErrorCode>
KvRgwServiceImpl::resolve_bucket_verify(tenant_id_t tenant_id,
                                        std::string_view bucket_name,
                                        FdbGetHolder &holder,
                                        bucket_id_t expected_bucket_id,
                                        uint8_t deny_mask)
{
  const fdb_error_t err = holder.wait();
  if (err) [[unlikely]] {
    return std::unexpected(fdb_to_error(err));
  }
  if (!holder.present()) [[unlikely]] {
    invalidate_bucket_cache(tenant_id, bucket_name);
    return std::unexpected(KVRGW_ERR_NO_SUCH_BUCKET);
  }
  const BucketValueHeader* bvh = bvh_ptr(holder.value());
  if (!bvh) [[unlikely]] {
    invalidate_bucket_cache(tenant_id, bucket_name);
    return std::unexpected(KVRGW_ERR_CORRUPT_VALUE);
  }
  const bucket_id_t bucket_id = bvh_bucket_id(bvh);
  const auto access_flags = bvh_access_flags(bvh);
  if (bucket_id != expected_bucket_id) [[unlikely]] {
    // bucket was deleted and re-created underneath us -- refresh the cache
    // with the fresh state we just read instead of merely invalidating it,
    // so the caller's retry hits the cache instead of re-fetching.
    put_bucket_cache(tenant_id, bucket_name, bucket_id, access_flags,
                      bvh_versioning_state(bvh));
    return std::unexpected(KVRGW_ERR_BUCKET_ID_MISMATCH);
  }
  // Opportunistic cache refresh: we already paid for this authoritative read,
  // so extend the cache's life for free (no extra KV round-trip) rather than
  // letting it hard-expire and force a future get_bucket_id_cached() miss to
  // re-fetch. Thread-local fast path -- no reader-lock check first.
  bucket_cache_fast_refresh(tenant_id, bucket_name, bucket_id, access_flags,
                            bvh_versioning_state(bvh));
  if (deny_mask != 0 && (access_flags & deny_mask)) [[unlikely]] {
    return std::unexpected(KVRGW_ERR_ACCESS_DENIED);
  }
  return BucketState{bucket_id, access_flags, bvh_versioning_state(bvh),
                     bvh_created_at_unix(bvh)};
}

//--------------------------------------------------------------------------------
void KvRgwServiceImpl::put_tenant_cache(std::string_view tenant_name,
                                        tenant_id_t tenant_id)
{
  std::unique_lock lock(cache_mu_);
  // The only allocation point: building the owning map key on insert/update.
  tenant_cache_.insert_or_assign(std::string(tenant_name), tenant_id);
}

//--------------------------------------------------------------------------------
void KvRgwServiceImpl::invalidate_tenant_cache(std::string_view tenant_name)
{
  std::unique_lock lock(cache_mu_);
  // Heterogeneous find + erase(iterator) -- allocation-free.
  const auto it = tenant_cache_.find(tenant_name);
  if (it != tenant_cache_.end()) {
    tenant_cache_.erase(it);
  }
}

//--------------------------------------------------------------------------------
std::expected<std::optional<tenant_id_t>, fdb_error_t>
KvRgwServiceImpl::resolve_tenant_id(std::string_view tenant_name)
{
  {
    std::shared_lock lock(cache_mu_);
    // Heterogeneous find -- no allocation for the lookup.
    const auto it = tenant_cache_.find(tenant_name);
    if (it != tenant_cache_.end()) {
      return std::optional<tenant_id_t>(it->second);
    }
  }
  const auto key = make_tenant_key(tenant_name);
  auto value = store_.get(key.view());
  if (!value) {
    return std::unexpected(value.error());
  }
  if (!*value) {
    return std::nullopt;
  }
  const auto tenant_value = parse_tenant_value(**value);
  if (!tenant_value) {
    return std::nullopt;
  }
  put_tenant_cache(tenant_name, tenant_value->tenant_id);
  return std::optional<tenant_id_t>(tenant_value->tenant_id);
}

//---------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::add_tenant(std::string_view tenant_name,
                                            tenant_id_t *out_id)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kOther);
  if (tenant_name.empty()) {
    return KVRGW_ERR_INVALID_ARGUMENT;
  }

  auto resolved = resolve_tenant_id(tenant_name);
  if (!resolved) {
    return fdb_to_error(resolved.error());
  }
  if (*resolved) {
    return KVRGW_ERR_TENANT_ALREADY_EXISTS;
  }

  uint64_t tenant_num = 0;
  auto ec = LKeyNumericCounter::allocate(store_, kLocalCounterTenantId, tenant_num);
  if (ec != KVRGW_ERR_OK) {
    return ec;
  }
  if (tenant_num > std::numeric_limits<uint32_t>::max()) {
    return KVRGW_ERR_INTERNAL;
  }

  const tenant_id_t tenant_id = static_cast<tenant_id_t>(tenant_num);
  const auto tenant_key = make_tenant_key(tenant_name);

  for (int attempt = 0; attempt < kMaxTxnRetries; ++attempt) {
    auto tr_res = store_.begin_transaction();
    if (!tr_res) {
      auto ec = fdb_to_error(tr_res.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_res;
    tr->kv_put(tenant_key.view(), make_tenant_value(tenant_id, now_unix()));
    fdb_error_t err = tr->commit();
    if (!err) {
      put_tenant_cache(tenant_name, tenant_id);
      if (out_id) {
        *out_id = tenant_id;
      }
      return KVRGW_ERR_OK;
    }
    auto commit_ec = fdb_to_error(err);
    if (!is_retriable(commit_ec)) {
      return commit_ec;
    }
    sleep_for_msec(10 * (attempt + 1));
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//---------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::resolve_tenant(std::string_view tenant_name,
                                                bool *out_exists,
                                                tenant_id_t *out_id)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kOther);
  auto resolved = resolve_tenant_id(tenant_name);
  if (!resolved) {
    return fdb_to_error(resolved.error());
  }
  if (*resolved) {
    *out_exists = true;
    *out_id = **resolved;
  }
  else {
    *out_exists = false;
  }
  return KVRGW_ERR_OK;
}

//---------------------------------------------------------------------------------
std::expected<std::optional<ObjectValue>, fdb_error_t>
KvRgwServiceImpl::load_object(bucket_id_t bucket_id,
                              std::string_view object_name)
{
  KeyBuf key;
  make_object_key(bucket_id, object_name, key);
  auto value = store_.get(key.view());
  if (!value) {
    return std::unexpected(value.error());
  }
  if (!*value) {
    return std::nullopt;
  }
  return parse_object_value(**value);
}

//---------------------------------------------------------------------------------
std::expected<std::optional<KvRgwServiceImpl::LoadResult>, fdb_error_t>
KvRgwServiceImpl::load_object_with_data(bucket_id_t bucket_id,
                                        std::string_view object_name)
{
  auto tr_result = store_.begin_transaction();
  if (!tr_result) {
    return std::unexpected(tr_result.error());
  }
  auto &tr = *tr_result;
  KeyBuf key;
  make_object_key(bucket_id, object_name, key);
  auto raw = tr->kv_get(key.view());
  if (!raw) {
    return std::unexpected(raw.error());
  }
  if (!*raw) {
    return std::nullopt;
  }
  auto obj = parse_object_value(**raw);
  if (!obj) {
    return std::nullopt;
  }

  LoadResult result;
  result.value = std::move(*obj);

  if (result.value.hdr.chunk_type == CHUNK_INLINE) {
    result.data.assign(result.value.inline_data.begin(),
                       result.value.inline_data.end());
  }
  else if (result.value.hdr.chunk_type == CHUNK_CHILD_D ||
           result.value.hdr.chunk_type == CHUNK_CHILD_D_REF) {
    const uint8_t st = d_size_tier_from_size(result.value.hdr.size);
    const uint32_t mtime = result.value.hdr.last_modified_sec;
    const bucket_id_t& d_bucket = (result.value.hdr.chunk_type == CHUNK_CHILD_D_REF)
      ? result.value.chunk.get_bucket_id() : bucket_id;
    const std::string_view ref_sv = result.value.data_ref_view();
    KeyBuf d_key;
    make_d_key(d_bucket, st, ref_sv, mtime, d_key);
    auto d_val = tr->kv_get(d_key.view());
    if (!d_val) {
      return std::unexpected(d_val.error());
    }
    if (!*d_val) {
      return std::nullopt;
    }
    const auto payload = child_value_payload(**d_val, result.value.hdr.size);
    if (payload.size() != result.value.hdr.size) {
      return std::nullopt;
    }
    result.data.assign(payload.begin(), payload.end());
  }

  return result;
}

KvRgwServiceImpl::NewVersionIds
KvRgwServiceImpl::compute_new_version(VersioningState versioning_state,
                                      const ObjectValue *old_o)
{
  return compute_new_version(versioning_state, old_o ? &old_o->hdr : nullptr);
}

KvRgwServiceImpl::NewVersionIds
KvRgwServiceImpl::compute_new_version(VersioningState versioning_state,
                                      const ObjectValueHeader *old_hdr)
{
  switch (versioning_state) {
  case VERSIONING_ENABLED:
    {
      version_id_t vid = old_hdr ? ovh_next_vid(old_hdr): kFirstVersionId;
      return {vid, vid.next_vid()};
    }
  case VERSIONING_SUSPENDED:
    if (old_hdr) {
      return {kNullVersion, ovh_next_vid(old_hdr)};
    }
    return {kNullVersion, kFirstVersionId};
  case VERSIONING_DISABLED:
  default:
    return {kNullVersion, kFirstVersionId};
  }
}

//--------------------------------------------------------------------------------
static bool must_defer_to_gc(ChunkType chunk_type,
                             uint8_t   flags,
                             bool      kv_store_coalescing,
                             uint16_t  annotations_count)
{
  // A plain, unshared CHUNK_MULTIPART still goes straight to a G:M{FREE_PARTS}
  // entry (see the `else` branch below) -- only once it's actually shared
  // (kFlagSharedData, i.e. at least one CHUNK_MULTIPART_REF copy exists) does
  // its deletion need to go through the ref-counted R:-key path instead.
  const bool shared = (flags & ObjectValue::kFlagSharedData) != 0;
  return (chunk_type == CHUNK_STORAGE       ||
          chunk_type == CHUNK_STORAGE_REF   ||
          chunk_type == CHUNK_MULTIPART_REF ||
          (chunk_type == CHUNK_MULTIPART && shared) ||
          kv_store_coalescing             ||
          (flags & ObjectValue::kFlagExternalAnnotations && annotations_count > 0));
}

//--------------------------------------------------------------------------------
// Zero-copy move_object_to_g: reads directly from wire-format raw_value.
bool KvRgwServiceImpl::move_object_to_g(KvTransaction &tr,
                                         std::string_view object_key,
                                         std::string_view raw_value)
{
  const auto parts = parse_object_key_parts_view(object_key);
  if (!parts) {
    std::cerr << "move_object_to_g: corrupt object key (len="
              << object_key.size() << "), fencing entry\n";
    tr.kv_del(object_key);
    return false;
  }

  const ObjectValueHeader *h = ovh_ptr(raw_value);
  if (!h) {
    tr.kv_del(object_key);
    return false;
  }

  const std::string_view ref_tag_sv = h->ref_tag.view();
  const auto tc = tier_config_state_.active_copy();
  const bool defer_to_gc = must_defer_to_gc(h->chunk_type, h->flags, tc.kv_store_coalescing,
                                            be16toh(h->annotations_count));
  const uint64_t obj_size = be64toh(h->size);
  const uint32_t mtime = be32toh(h->last_modified_sec);

  KeyBuf key_buf;
  if (defer_to_gc) {
    GcValueHeader gc_hdr{};
    gc_hdr.chunk_type = h->chunk_type;
    gc_hdr.flags = h->flags;
    GcDescriptor gc{};
    if (h->chunk_type == CHUNK_MULTIPART_REF) {
      // Unlike plain (shared) CHUNK_MULTIPART, this object's own ref_tag
      // (used below only to build a unique G:O key) is *not* the shared
      // part-file identity -- that lives in the wire-tail ref_tag, same
      // split as CHUNK_CHILD_D_REF's a few lines above.
      OvhChunkRef cref;
      if (ovh_chunk_ref(raw_value, h, cref)) {
        gc.chunk = ChunkDescriptor(h->chunk_type, cref.ref_tag.data(), kNullBucket);
      }
      else {
        tr.kv_del(object_key);
        return false;
      }
    }
    else {
      gc.chunk = ChunkDescriptor(h->chunk_type, ref_tag_sv.data(), parts->bucket_id);
    }
    gc.object_size = obj_size;
    gc.mtime = mtime;
    make_go_key(parts->shard_count, parts->shard_id, parts->bucket_id, ref_tag_sv,
                obj_size, key_buf);
    GcValueBuf gc_buf;
    make_gc_value(gc_buf, gc_hdr, gc);
    tr.kv_put(key_buf.view(), gc_buf.view());
    tr.kv_del(object_key);
  }
  else {
    const uint8_t st = d_size_tier_from_size(obj_size);

    if (h->chunk_type == CHUNK_CHILD_D) {
      make_d_key(parts->bucket_id, st, ref_tag_sv, mtime, key_buf);
      const bool shared = (h->flags & ObjectValue::kFlagSharedData) != 0;
      decrement_or_del_child_d(tr, key_buf.view(), obj_size, shared);
    }
    else if (h->chunk_type == CHUNK_CHILD_D_REF) {
      OvhChunkRef cref;
      if (ovh_chunk_ref(raw_value, h, cref)) {
        make_d_key(cref.bucket_id, st, cref.ref_tag, mtime, key_buf);
        const bool shared = (h->flags & ObjectValue::kFlagSharedData) != 0;
        decrement_or_del_child_d(tr, key_buf.view(), obj_size, shared);
      }
    }
    else if (h->chunk_type == CHUNK_MULTIPART) {
      // By now every S:U row for this ref_tag is long gone (Complete's own
      // G:M{MIXED} pass already cleared them) -- the manifest carried on
      // this :O: value is the only remaining record of which part files
      // are still on disk. Hand it to GcWorker verbatim (no decode/
      // re-encode -- same trick as P:M -> G:M{MIXED} above) as a
      // G:M{FREE_PARTS} entry.
      const std::string_view manifest_body = ovh_multipart_manifest_body(raw_value, h);
      if (!manifest_body.empty()) {
        make_gm_key(parts->bucket_id, ref_tag_sv, key_buf);
        GmValueBuf gm_buf;
        gm_buf.data[0] = static_cast<uint8_t>(GM_MODE_FREE_PARTS);
        std::memcpy(gm_buf.data + 1, manifest_body.data(), manifest_body.size());
        gm_buf.len = 1 + manifest_body.size();
        tr.kv_put(key_buf.view(), gm_buf.view());
      }
    }

    if (h->flags & ObjectValue::kFlagExternalTags) {
      make_ct_key(parts->bucket_id, ref_tag_sv, key_buf);
      tr.kv_del(key_buf.view());
    }
    if (h->flags & ObjectValue::kFlagExtendedAttrs) {
      make_c_prefix(parts->bucket_id, ref_tag_sv, key_buf);
      // TBD: dynamic allocation! should be removed
      const auto ce_end = std::string(key_buf.view()) + "\xFF";
      tr.kv_range_clear(key_buf.view(), ce_end);
    }

    tr.kv_del(object_key);
  }
  return true;
}

//--------------------------------------------------------------------------------
// Zero-copy displace_old_object: writes raw_value directly to :V: (versioned),
// or delegates to the raw move_object_to_g (unversioned/GC path).
void KvRgwServiceImpl::displace_old_object(KvTransaction &tr,
                                            VersioningState versioning_state,
                                            std::string_view object_key,
                                            std::string_view raw_value)
{
  const ObjectValueHeader *h = ovh_ptr(raw_value);
  if (!h) {
    // Corrupt value — treat as unversioned GC to clean up what we can.
    move_object_to_g(tr, object_key, raw_value);
    return;
  }

  KeyBuf v_key;
  if (versioning_state == VERSIONING_ENABLED) {
    const auto parts = parse_object_key_parts_view(object_key);
    if (!parts) {
      move_object_to_g(tr, object_key, raw_value);
      return;
    }
    make_v_key(parts->bucket_id, parts->object_name, ovh_version_id(h), v_key);
    tr.kv_put(v_key.view(), raw_value);
  }
  else if (versioning_state == VERSIONING_SUSPENDED) {
    const auto parts = parse_object_key_parts_view(object_key);
    if (!parts) {
      move_object_to_g(tr, object_key, raw_value);
      return;
    }
    const version_id_t vid = ovh_version_id(h);
    if (vid != kNullVersion) {
      make_v_key(parts->bucket_id, parts->object_name, vid, v_key);
      tr.kv_put(v_key.view(), raw_value);
    }
    else {
      move_object_to_g(tr, object_key, raw_value);
    }
    // Also clean up any existing null-version slot.
    make_v_key(parts->bucket_id, parts->object_name, kNullVersion, v_key);
    auto null_v_raw = tr.kv_get(v_key.view());
    if (null_v_raw && *null_v_raw) {
      // move_object_to_g calls kv_del(v_key) internally.
      move_object_to_g(tr, v_key.view(), **null_v_raw);
    }
  }
  else {
    move_object_to_g(tr, object_key, raw_value);
  }
}

//--------------------------------------------------------------------------------
// Shared helper: displace the old object (if any) per versioning_state, then
// compute the new object's (version_id, next_vid).
KvRgwServiceImpl::NewVersionIds KvRgwServiceImpl::displace_and_version(
    KvTransaction &tr, VersioningState vs, std::string_view object_key,
    const ObjectValueHeader *old_hdr, std::string_view old_raw)
{
  if (old_hdr) {
    displace_old_object(tr, vs, object_key, old_raw);
  }
  return compute_new_version(vs, old_hdr);
}

//--------------------------------------------------------------------------------
// Pure If-Match / If-None-Match evaluation. No writes.
KvrgwErrorCode
KvRgwServiceImpl::check_put_condition(const PutCondition *cond,
                                      const ObjectValueHeader *old_hdr)
{
  if (!cond) {
    return KVRGW_ERR_OK;
  }
  if (cond->flags.if_match()) {
    if (!old_hdr || ovh_is_delete_marker(old_hdr)) {
      return KVRGW_ERR_NO_SUCH_KEY;
    }
    if (!cond->flags.etag_is_star() && old_hdr->etag != cond->etag) {
      return KVRGW_ERR_PRECONDITION_FAILED;
    }
  }
  if (cond->flags.if_none_match()) {
    if (cond->flags.if_none_match_star() && old_hdr &&
        !ovh_is_delete_marker(old_hdr)) {
      return KVRGW_ERR_PRECONDITION_FAILED;
    }
    if (!cond->flags.if_none_match_star() && old_hdr &&
        !ovh_is_delete_marker(old_hdr) && old_hdr->etag == cond->etag) {
      return KVRGW_ERR_PRECONDITION_FAILED;
    }
  }
  return KVRGW_ERR_OK;
}

//--------------------------------------------------------------------------------
// check condition -> displace -> version -> stamp. All reads/validation happen
// before the first write on `tr`.
std::expected<KvRgwServiceImpl::NewVersionIds, KvrgwErrorCode>
KvRgwServiceImpl::install_object_value( KvTransaction &tr,
                                        VersioningState vs,
                                        std::string_view object_key,
                                        std::string_view old_raw,
                                        const PutCondition *cond,
                                        ObjectValue &new_value)
{
  const ObjectValueHeader *old_hdr = nullptr;
  if (!old_raw.empty()) {
    old_hdr = ovh_ptr(old_raw);
    if (!old_hdr) [[unlikely]] {
      return std::unexpected(KVRGW_ERR_CORRUPT_VALUE);
    }
  }

  if (auto ec = check_put_condition(cond, old_hdr); ec != KVRGW_ERR_OK) {
    return std::unexpected(ec);
  }

  auto ids = displace_and_version(tr, vs, object_key, old_hdr, old_raw);
  new_value.hdr.version_id = ids.version_id;
  new_value.hdr.next_vid = ids.next_vid;
  return ids;
}

//--------------------------------------------------------------------------------
// Picks the buffer size by chunk type, serializes and kv_put()s the :O: value.
bool KvRgwServiceImpl::write_o_value(KvTransaction &tr,
                                     std::string_view object_key,
                                     const ObjectValue &value,
                                     size_t *out_size)
{
  if (value.hdr.chunk_type == CHUNK_MULTIPART ||
      value.hdr.chunk_type == CHUNK_MULTIPART_REF) {
    OValueBufMultipart buf;
    if (!write_object_value(buf, value)) {
      return false;
    }
    tr.kv_put(object_key, buf.view());
    if (out_size) {
      *out_size = buf.view().size();
    }
    return true;
  }
  OValueBuf buf;
  if (!write_object_value(buf, value)) {
    return false;
  }
  tr.kv_put(object_key, buf.view());
  if (out_size) {
    *out_size = buf.view().size();
  }
  return true;
}

//--------------------------------------------------------------------------------
std::expected<KvRgwServiceImpl::DeleteBatchCtx, KvrgwErrorCode>
KvRgwServiceImpl::delete_prepare_batch(KvTransaction &tr, tenant_id_t tenant_id,
                                       const std::string &bucket_name,
                                       bucket_id_t bucket_id)
{
  DeleteBatchCtx batch;
  batch.tenant_id = tenant_id;
  batch.bucket_name = &bucket_name;  // pointer — no copy
  if (bucket_id == kNullBucket) {
    auto cached = get_bucket_id_cached(tenant_id, bucket_name);
    if (!cached) {
      return std::unexpected(fdb_to_error(cached.error()));
    }
    if (!*cached) {
      return std::unexpected(KVRGW_ERR_NO_SUCH_BUCKET);
    }
    bucket_id = (**cached).bucket_id;
  }
  batch.bucket_id = bucket_id;
  // TBD - should be done once for the full multi-delete not per-batch!
  KeyBuf bkt_key;
  make_bucket_key(tenant_id, bucket_name, bkt_key);
  batch.f_bkt = tr.kv_async_get(bkt_key.view());
  return batch;
}

//--------------------------------------------------------------------------------
void KvRgwServiceImpl::delete_prepare_key(KvTransaction &tr, bucket_id_t bucket_id,
                                          std::string_view object_name,
                                          DeleteContext &ctx)
{
  make_object_key(bucket_id, object_name, ctx.object_key);
  ctx.f_obj = tr.kv_async_get(ctx.object_key.view());
}

//--------------------------------------------------------------------------------
std::expected<KvRgwServiceImpl::BucketState, KvrgwErrorCode>
KvRgwServiceImpl::delete_verify_bucket(DeleteBatchCtx &batch)
{
  FdbGetHolder holder(std::move(batch.f_bkt));
  auto vb = resolve_bucket_verify(batch.tenant_id, *batch.bucket_name, holder,
                                  batch.bucket_id, kDenyWrite);
  if (!vb) {
    return std::unexpected(vb.error());
  }
  batch.bucket_id = vb->bucket_id;
  return *vb;
}

//--------------------------------------------------------------------------------
std::expected<KvRgwServiceImpl::DeleteResult, KvrgwErrorCode>
KvRgwServiceImpl::delete_apply(KvTransaction &tr, DeleteContext &ctx,
                               const BucketState &bucket_state,
                               const GetCondition *cond)
{
  std::string_view raw_val;
  std::optional<FdbGetHolder> holder_storage;
  if (ctx.resolved) {
    raw_val = *ctx.resolved;
  }
  else {
    holder_storage.emplace(std::move(ctx.f_obj));
    const fdb_error_t obj_err = holder_storage->wait();
    if (obj_err) {
      return std::unexpected(fdb_to_error(obj_err));
    }
    if (holder_storage->present()) {
      raw_val = holder_storage->value();
    }
  }

  const bool obj_present = !raw_val.empty();
  const ObjectValueHeader *old_hdr = obj_present ? ovh_ptr(raw_val) : nullptr;

  if (obj_present && !old_hdr) {
    return std::unexpected(KVRGW_ERR_CORRUPT_VALUE);
  }

  // Conditional delete — all checks use zero-copy ovh_* accessors; no parse needed.
  if (cond) {
    if (!old_hdr) {
      return std::unexpected(KVRGW_ERR_NO_SUCH_KEY);
    }
    if (ovh_is_delete_marker(old_hdr)) {
      return std::unexpected(KVRGW_ERR_PRECONDITION_FAILED);
    }
    if (cond->flags.if_match() && !cond->flags.etag_is_star()) {
      if (old_hdr->etag != cond->etag) {
        return std::unexpected(KVRGW_ERR_PRECONDITION_FAILED);
      }
    }
    if (cond->flags.has_mtime()) {
      if (ovh_last_modified_sec(old_hdr) != static_cast<int64_t>(cond->mtime)) {
        return std::unexpected(KVRGW_ERR_PRECONDITION_FAILED);
      }
    }
    if (cond->flags.has_size()) {
      if (ovh_size(old_hdr) != cond->size) {
        return std::unexpected(KVRGW_ERR_PRECONDITION_FAILED);
      }
    }
  }

  DeleteResult result;
  switch (bucket_state.versioning_state) {
  case VERSIONING_DISABLED:
    if (!old_hdr) {
      return result;
    }
    displace_old_object(tr, VERSIONING_DISABLED, ctx.object_key.view(), raw_val);
    return result;

  case VERSIONING_ENABLED:
  case VERSIONING_SUSPENDED: {
    if (old_hdr) {
      displace_old_object(tr, bucket_state.versioning_state, ctx.object_key.view(),
                          raw_val);
    }
    auto ids = compute_new_version(bucket_state.versioning_state, old_hdr);
    // create a delete marker
    ObjectValue dm;
    dm.hdr.flags = ObjectValue::kFlagFenced;
    dm.hdr.version_id = ids.version_id;
    dm.hdr.next_vid = ids.next_vid;
    dm.hdr.chunk_type = CHUNK_INLINE;
    //dm.hdr.chunk_type = CHUNK_EMPTY;
    OValueBuf dm_buf;
    if (!write_object_value(dm_buf, dm)) {
      return std::unexpected(KVRGW_ERR_VALUE_TOO_LARGE);
    }
    tr.kv_put(ctx.object_key.view(), dm_buf.view());
    result.created_dm = true;
    result.dm_version_id = ids.version_id;
    return result;
  }
  default:
    return result;
  }
}

//--------------------------------------------------------------------------------
std::expected<KvRgwServiceImpl::DeleteResult, KvrgwErrorCode>
KvRgwServiceImpl::delete_single(KvTransaction &tr, tenant_id_t tenant_id,
                                const std::string &bucket_name,
                                std::string_view object_name,
                                const GetCondition *cond)
{
  auto batch = delete_prepare_batch(tr, tenant_id, bucket_name, kNullBucket);
  if (!batch) {
    return std::unexpected(batch.error());
  }
  auto bs = delete_verify_bucket(*batch);
  if (!bs) {
    return std::unexpected(bs.error());
  }
  DeleteContext ctx;
  delete_prepare_key(tr, batch->bucket_id, object_name, ctx);
  return delete_apply(tr, ctx, *bs, cond);
}

//--------------------------------------------------------------------------------
bool KvRgwServiceImpl::pack_object_into_gc(std::string_view object_key,
                                            std::string_view raw_value,
                                            VersioningState versioning_state,
                                            GcDescriptor& out) const
{
  if (versioning_state != VERSIONING_DISABLED) {
    return false;
  }
  const auto parts = parse_object_key_parts_view(object_key);
  if (!parts) {
    return false;
  }
  const ObjectValueHeader *h = ovh_ptr(raw_value);
  if (!h) {
    return false;
  }
  const auto tc = tier_config_state_.active_copy();
  const bool defer_to_gc = must_defer_to_gc(h->chunk_type, h->flags, tc.kv_store_coalescing,
                                            be16toh(h->annotations_count));
  if (!defer_to_gc) {
    return false;
  }
  out.chunk = ChunkDescriptor(h->chunk_type, h->ref_tag.view().data(), parts->bucket_id);
  out.object_size = be64toh(h->size);
  out.mtime = be32toh(h->last_modified_sec);
  return true;
}

bool KvRgwServiceImpl::delete_multi_try_commit(
    tenant_id_t tenant_id, const std::string &bucket_name,
    bucket_id_t bucket_id, std::span<const std::string> keys,
    std::span<DeleteContext> ctxs,
    std::vector<DeleteMultiKeyOutcome> *out)
{
  if (keys.empty()) {
    return true;
  }
  auto tr_result = store_.begin_transaction();
  if (!tr_result) {
    return false;
  }
  auto &tr = *tr_result;

  // Phase 1: issue bucket get + all per-key object gets concurrently
  auto batch = delete_prepare_batch(*tr, tenant_id, bucket_name, bucket_id);
  if (!batch) {
    return false;
  }

  for (size_t i = 0; i < keys.size(); ++i) {
    delete_prepare_key(*tr, batch->bucket_id, keys[i], ctxs[i]);
  }

  // Phase 2: verify bucket once
  auto bs = delete_verify_bucket(*batch);
  if (!bs) {
    return false;
  }

  // Phase 2b: resolve all object futures and populate ctx.resolved for GC extraction
  for (size_t i = 0; i < keys.size(); ++i) {
    auto val = tr->kv_wait_get(ctxs[i].f_obj);
    if (val && *val) {
      ctxs[i].resolved = std::move(**val);
    }
  }

  // Phase 3: append directly into out; roll back on commit failure
  // Also accumulate GcDescriptors for deferred-GC objects into a single group G:G key.
  GcDescriptor gc_descs[kMaxBatchSize];
  uint8_t gc_count = 0;

  const size_t base = out->size();
  for (size_t i = 0; i < keys.size(); ++i) {
    GcDescriptor gc{};
    if (ctxs[i].resolved &&
        pack_object_into_gc(ctxs[i].object_key.view(), *ctxs[i].resolved,
                            bs->versioning_state, gc)) {
      gc_descs[gc_count++] = gc;
      // object_key delete is handled inside move_object_to_g via displace_old_object;
      // for the GC path we skip move_object_to_g and delete the key directly.
      tr->kv_del(ctxs[i].object_key.view());
      out->emplace_back(DeleteMultiKeyOutcome::Status::Deleted, keys[i], "", false,
                       version_id_t{});
    }
    else {
      auto result = delete_apply(*tr, ctxs[i], *bs, nullptr);
      if (result) {
        out->emplace_back(DeleteMultiKeyOutcome::Status::Deleted, keys[i], "",
                         result->created_dm, result->dm_version_id);
      }
      else {
        out->emplace_back(DeleteMultiKeyOutcome::Status::Error, keys[i],
                         "InternalError");
      }
    }
  }

  // Write single group G:G key for all accumulated GC entries
  if (gc_count > 0) {
    const auto go_key = make_group_go_key(gc_descs[0].chunk.ref_tag_view());
    GroupGcValueBuf go_buf;
    make_group_gc_value(go_buf, gc_descs, gc_count);
    tr->kv_put(go_key.view(), go_buf.view());
  }

  if (tr->commit() == 0) {
    return true;
  }
  out->resize(base);
  return false;
}

void KvRgwServiceImpl::delete_multi_one_key(
    tenant_id_t tenant_id, const std::string &bucket_name,
    bucket_id_t bucket_id, std::string_view key,
    std::vector<DeleteMultiKeyOutcome> &out)
{
  auto tr_result = store_.begin_transaction();
  if (!tr_result) {
    DeleteMultiKeyOutcome err;
    err.key = std::string(key);
    err.status = DeleteMultiKeyOutcome::Status::Error;
    err.error_code = "InternalError";
    err.error_message = "transaction begin failed";
    out.push_back(std::move(err));
    return;
  }
  auto &tr = *tr_result;

  auto batch = delete_prepare_batch(*tr, tenant_id, bucket_name, bucket_id);
  if (!batch) {
    DeleteMultiKeyOutcome err;
    err.key = std::string(key);
    err.status = DeleteMultiKeyOutcome::Status::Error;
    err.error_code = "InternalError";
    err.error_message = "prepare batch failed";
    out.push_back(std::move(err));
    return;
  }

  DeleteContext ctx;
  delete_prepare_key(*tr, batch->bucket_id, key, ctx);

  auto bs = delete_verify_bucket(*batch);
  DeleteMultiKeyOutcome outcome;
  outcome.key = std::string(key);
  if (bs) {
    auto result = delete_apply(*tr, ctx, *bs, nullptr);
    if (result && tr->commit() == 0) {
      outcome.status = DeleteMultiKeyOutcome::Status::Deleted;
      outcome.created_dm = result->created_dm;
      outcome.dm_version_id = result->dm_version_id;
    }
    else {
      outcome.status = DeleteMultiKeyOutcome::Status::Error;
      outcome.error_code = "InternalError";
      outcome.error_message = "transaction commit failed";
    }
  }
  else {
    outcome.status = DeleteMultiKeyOutcome::Status::Error;
    outcome.error_code = "InternalError";
    outcome.error_message = "bucket verify failed";
  }
  out.push_back(std::move(outcome));
}

void apply_tags_to_value(ObjectValue &obj, std::span<const uint8_t> encoded,
                         KvTransaction &tr, bucket_id_t bucket_id,
                         std::string_view ref_tag)
{
  const tag_count_t count = read_be_field<tag_count_t>(encoded.data());
  obj.hdr.tags_count = static_cast<uint8_t>(count);
  obj.hdr.flags |= ObjectValue::kFlagExternalTags;
  KeyBuf ct_key;
  make_ct_key(bucket_id, ref_tag, ct_key);
  ChildValueHeader ch{};
  tr.kv_put(ct_key.view(), make_child_value(
              ch, std::string_view(reinterpret_cast<const char *>(encoded.data()),
                                   encoded.size())));
}

void clear_object_tags(ObjectValue &obj, KvTransaction &tr,
                       bucket_id_t bucket_id, std::string_view ref_tag)
{
  if (obj.has_external_tags()) {
    KeyBuf ct_key;
    make_ct_key(bucket_id, ref_tag, ct_key);
    tr.kv_del(ct_key.view());
  }
  obj.hdr.tags_count = 0;
  obj.hdr.flags &= ~ObjectValue::kFlagExternalTags;
}

KvrgwErrorCode KvRgwServiceImpl::put_object_phase3(
    tenant_id_t tenant_id, const std::string &bucket_name,
    bucket_id_t bucket_id, const std::string &object_name,
    ObjectValue &new_value, VersioningState *out_versioning_state,
    const PutCondition *cond, std::span<const uint8_t> tags,
    std::span<const uint8_t> metadata)
{
  constexpr int kMaxRetries = 3;
  static const std::string empty_data;

  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    auto tr_result = store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;

    PutInTxnParams params{tenant_id, bucket_name, bucket_id, object_name,
                          new_value, &empty_data, cond,
                          tags,      false,       true,      metadata};
    auto ec = put_object_in_txn(*tr, params, out_versioning_state);
    if (ec != KVRGW_ERR_OK) {
      if (ec == KVRGW_ERR_INTERNAL) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      if (ec == KVRGW_ERR_PRECONDITION_FAILED || ec == KVRGW_ERR_NO_SUCH_KEY) {
        // put_finalize queued the blob's move to GC on this transaction.
        // Commit it; whatever the result, report the precondition error (if
        // the commit fails, the sweeper reaches the same outcome later).
        const fdb_error_t cerr = tr->commit();
        if (cerr && is_retriable(fdb_to_error(cerr)) &&
            attempt + 1 < kMaxRetries) {
          // A concurrent change to :O: -- re-evaluate the condition.
          sleep_for_msec(10 * (attempt + 1));
          continue;
        }
      }
      return ec;
    }

    fdb_error_t err = tr->commit();
    if (!err) {
      put_bucket_cache(tenant_id, bucket_name, bucket_id, 0,
                       out_versioning_state ? *out_versioning_state : VERSIONING_DISABLED);
      return KVRGW_ERR_OK;
    }
    auto commit_ec = fdb_to_error(err);
    if (!is_retriable(commit_ec)) {
      return commit_ec;
    }
    sleep_for_msec(10 * (attempt + 1));
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//---------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::create_bucket(tenant_id_t tenant_id,
                                               std::string_view bucket_name)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kCreateBucket);
  ops_stats_.inc(OpType::kCreateBucket);
  if (!is_valid_bucket_name(bucket_name)) {
    return KVRGW_ERR_INVALID_BUCKET_NAME;
  }

  auto state = read_bucket_state(tenant_id, bucket_name);
  if (!state) {
    return fdb_to_error(state.error());
  }
  if (state->has_value()) {
    return KVRGW_ERR_OK;
  }

  uint64_t bucket_num = 0;
  auto ec = LKeyNumericCounter::allocate(store_, kLocalCounterBucketId, bucket_num);
  if (ec != KVRGW_ERR_OK) {
    return ec;
  }

  const bucket_id_t bucket_id = bucket_id_t::from_counter(bucket_num);
  KeyBuf bucket_key;
  make_bucket_key(tenant_id, bucket_name, bucket_key);

  for (int attempt = 0; attempt < kMaxTxnRetries; ++attempt) {
    auto tr_res = store_.begin_transaction();
    if (!tr_res) {
      auto ec = fdb_to_error(tr_res.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_res;
    tr->kv_put(bucket_key.view(), make_bucket_value(bucket_id, now_unix()));
    fdb_error_t err = tr->commit();
    if (!err) {
      put_bucket_cache(tenant_id, bucket_name, bucket_id);
      return KVRGW_ERR_OK;
    }
    auto commit_ec = fdb_to_error(err);
    if (!is_retriable(commit_ec)) {
      return commit_ec;
    }
    sleep_for_msec(10 * (attempt + 1));
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//---------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::bucket_exists(tenant_id_t tenant_id,
                                               std::string_view bucket_name,
                                               bool *out_exists)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kBucketExists);
  ops_stats_.inc(OpType::kBucketExists);
  auto state = read_bucket_state(tenant_id, bucket_name);
  if (!state) {
    return fdb_to_error(state.error());
  }
  *out_exists = state->has_value();
  return KVRGW_ERR_OK;
}

//---------------------------------------------------------------------------------
KvrgwErrorCode
KvRgwServiceImpl::bucket_exists_cached(tenant_id_t tenant_id,
                                       std::string_view bucket_name,
                                       bool *out_exists)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kBucketExists);
  ops_stats_.inc(OpType::kBucketExists);
  auto state = get_bucket_id_cached(tenant_id, bucket_name);
  if (!state) {
    return fdb_to_error(state.error());
  }
  *out_exists = state->has_value();
  return KVRGW_ERR_OK;
}

//--------------------------------------------------------------------------------
std::expected<std::optional<bucket_id_t>, KvrgwErrorCode>
KvRgwServiceImpl::resolve_bucket_id(tenant_id_t tenant_id,
                                     std::string_view bucket_name)
{
  auto state = get_bucket_id_cached(tenant_id, bucket_name);
  if (!state) {
    return std::unexpected(fdb_to_error(state.error()));
  }
  if (!state->has_value()) {
    return std::nullopt;
  }
  return (**state).bucket_id;
}

//--------------------------------------------------------------------------------
KvRgwServiceImpl::PutContext
KvRgwServiceImpl::put_prepare(KvTransaction &tr, PutInTxnParams &params,
                              bool need_bucket)
{
  PutContext ctx;
  make_object_key(params.bucket_id, params.object_name, ctx.object_key);
  ctx.is_storage_tier = params.is_storage_tier;

  if (need_bucket) {
    KeyBuf bucket_key;
    make_bucket_key(params.tenant_id, params.bucket_name, bucket_key);
    ctx.f_bkt = tr.kv_async_get(bucket_key.view());
    ctx.has_bucket_future = true;
  }

  if (params.is_storage_tier) {
    make_po_key(params.bucket_id, params.value.hdr.ref_tag.view(), ctx.po_key);
    ctx.f_po = tr.kv_async_get(ctx.po_key.view());
  }

  ctx.f_obj = tr.kv_async_get(ctx.object_key.view());
  return ctx;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode
KvRgwServiceImpl::put_finalize(KvTransaction &tr, PutContext &ctx,
                               PutInTxnParams &params, VerifiedBucket *verified,
                               int &verified_count,
                               VersioningState *out_versioning_state)
{
  VersioningState vs = VERSIONING_DISABLED;
  if (ctx.has_bucket_future) {
    FdbGetHolder holder(std::move(ctx.f_bkt));
    ctx.has_bucket_future = false;
    auto vb = resolve_bucket_verify(params.tenant_id, params.bucket_name, holder,
                                    params.bucket_id, kDenyWrite);
    if (!vb) {
      return vb.error();
    }
    vs = vb->versioning_state;
    auto &slot = verified[verified_count++];
    slot.tenant_id = params.tenant_id;
    slot.bucket_name = &params.bucket_name;
    slot.state = *vb;
  }
  else {
    bool found = false;
    for (int j = 0; j < verified_count; ++j) {
      if (verified[j].tenant_id == params.tenant_id &&
          *verified[j].bucket_name == params.bucket_name) {
        vs = verified[j].state.versioning_state;
        found = true;
        break;
      }
    }
#if 0
    assert(found);
    return KVRGW_ERR_INTERNAL;
#else
    if (!found) {
      // TBD: why are we doing blocking get ???
      auto holder = issue_bucket_get(tr, params.tenant_id, params.bucket_name);
      auto vb = resolve_bucket_verify(params.tenant_id, params.bucket_name, holder,
                                      params.bucket_id, kDenyWrite);
      if (!vb) {
        return vb.error();
      }
      vs = vb->versioning_state;
      auto &slot = verified[verified_count++];
      slot.tenant_id = params.tenant_id;
      slot.bucket_name = &params.bucket_name;
      slot.state = *vb;
    }
#endif
  }
  if (out_versioning_state) {
    *out_versioning_state = vs;
  }

  if (ctx.is_storage_tier) {
    FdbGetHolder existing(std::move(ctx.f_obj));
    {
      const fdb_error_t err = existing.wait();
      if (err) {
        return fdb_to_error(err);
      }
    }

    if (ctx.f_po) {
      FdbGetHolder po_holder(std::move(ctx.f_po));
      const fdb_error_t po_err = po_holder.wait();
      if (po_err) {
        return fdb_to_error(po_err);
      }

      if (!po_holder.present()) {
        if (existing.present()) {
          const auto *h = ovh_ptr(existing.value());
          if (h && h->ref_tag == params.value.hdr.ref_tag) {
            return KVRGW_ERR_OK;
          }
        }
        return KVRGW_ERR_INTERNAL;
      }

      if (existing.present()) {
        const auto *h = ovh_ptr(existing.value());
        if (h && h->ref_tag == params.value.hdr.ref_tag) {
          tr.kv_del(ctx.po_key.view());
          return KVRGW_ERR_OK;
        }
      }
    }
    else {
      if (existing.present()) {
        const auto *h = ovh_ptr(existing.value());
        if (h && h->ref_tag == params.value.hdr.ref_tag) {
          return KVRGW_ERR_OK;
        }
      }
    }

    auto ids = install_object_value(
        tr, vs, ctx.object_key.view(),
        existing.value_or_empty(),
        params.cond, params.value);
    if (!ids) {
      const auto ec = ids.error();
      if ((ec == KVRGW_ERR_PRECONDITION_FAILED || ec == KVRGW_ERR_NO_SUCH_KEY) &&
          ctx.po_key.size() > 0) {
        // Single-request path: the blob and P:O were written in phase 2.
        // Move them to GC in this same transaction (the caller commits it
        // for these two codes). tr holds only reads at this point.
        GcDescriptor gc{};
        gc.chunk = ChunkDescriptor(CHUNK_STORAGE, params.value.hdr.ref_tag,
                                   params.bucket_id);
        gc.object_size = params.value.hdr.size;
        gc.mtime = static_cast<uint32_t>(now_unix());
        PoKeyParts po_parts;
        po_parts.bucket_id = params.bucket_id;
        po_parts.ref_tag = params.value.hdr.ref_tag;
        [[maybe_unused]] const fdb_error_t gc_err =
            move_po_to_go(tr, po_parts, gc, params.object_name);
      }
      return ec;
    }

    if (!params.metadata.empty()) {
      params.value.metadata_frame.assign(params.metadata.begin(),
                                         params.metadata.end());
    }

    if (!params.tags.empty()) {
      apply_tags_to_value(params.value, params.tags, tr, params.bucket_id,
                          params.value.hdr.ref_tag.view());
    }

    size_t written = 0;
    if (!write_o_value(tr, ctx.object_key.view(), params.value, &written)) {
      return KVRGW_ERR_VALUE_TOO_LARGE;
    }
    store_.fdb_put_stats().record_value(written);

    if (ctx.po_key.size() > 0) {
      tr.kv_del(ctx.po_key.view());
    }

    return KVRGW_ERR_OK;
  }

  // Inline / child-D path
  FdbGetHolder existing(std::move(ctx.f_obj));
  {
    const fdb_error_t err = existing.wait();
    if (err) return fdb_to_error(err);
  }

  auto ids = install_object_value(
      tr, vs, ctx.object_key.view(),
      existing.value_or_empty(),
      params.cond, params.value);
  if (!ids) {
    return ids.error();
  }
  if (out_versioning_state) {
    *out_versioning_state = vs;
  }

  if (!params.metadata.empty()) {
    params.value.metadata_frame.assign(params.metadata.begin(),
                                       params.metadata.end());
  }

  if (!params.tags.empty()) {
    apply_tags_to_value(params.value, params.tags, tr, params.bucket_id,
                        params.value.hdr.ref_tag.view());
  }

  size_t written = 0;
  if (!write_o_value(tr, ctx.object_key.view(), params.value, &written)) {
    return KVRGW_ERR_VALUE_TOO_LARGE;
  }
  store_.fdb_put_stats().record_value(written);

  if (params.value.hdr.chunk_type == CHUNK_CHILD_D && params.data) {
    const uint8_t st = d_size_tier_from_size(params.data->size());
    const uint32_t mtime =
        static_cast<uint32_t>(params.value.hdr.last_modified_sec);
    KeyBuf d_key;
    make_d_key(params.bucket_id, st, params.value.hdr.ref_tag.view(), mtime, d_key);
    ChildValueHeader ch{};
    tr.kv_put(d_key.view(), make_child_value(ch, *params.data));
  }

  return KVRGW_ERR_OK;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode
KvRgwServiceImpl::put_object_in_txn(KvTransaction &tr, PutInTxnParams &params,
                                    VersioningState *out_versioning_state)
{
  VerifiedBucket verified[1];
  int vc = 0;
  auto ctx = put_prepare(tr, params, !params.skip_bucket_verify);
  return put_finalize(tr, ctx, params, verified, vc, out_versioning_state);
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::put_object_single_txn(
    tenant_id_t tenant_id, const std::string &bucket_name,
    const std::string &object_name,
    ObjectValue &new_value, const std::string &data,
    VersioningState *out_versioning_state, const PutCondition *cond,
    std::span<const uint8_t> tags, std::span<const uint8_t> metadata)
{
  batch_queue_.stats().single_txn_puts.fetch_add(1, std::memory_order_relaxed);
  constexpr int kMaxRetries = 3;

  auto cached_bid = get_bucket_id_cached(tenant_id, bucket_name);
  if (!cached_bid) {
    return fdb_to_error(cached_bid.error());
  }
  if (!*cached_bid) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  const bucket_id_t bucket_id = (**cached_bid).bucket_id;

  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    auto tr_result = store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      error_stats_->record(ec);
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;

    PutInTxnParams params{tenant_id, bucket_name, bucket_id, object_name,
                          new_value, &data,        cond,
                          tags,      false,        false,    metadata};
    auto ec = put_object_in_txn(*tr, params, out_versioning_state);
    if (ec != KVRGW_ERR_OK) {
      error_stats_->record(ec);
      if (ec == KVRGW_ERR_INTERNAL) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }

    fdb_error_t err = tr->commit();
    if (!err) {
      put_bucket_cache(tenant_id, bucket_name, bucket_id, 0,
                       out_versioning_state ? *out_versioning_state : VERSIONING_DISABLED);
      return KVRGW_ERR_OK;
    }
    auto commit_ec = fdb_to_error(err);
    error_stats_->record(commit_ec);
    if (!is_retriable(commit_ec)) {
      latency_stats_.txn_hard_failures.fetch_add(1, std::memory_order_relaxed);
      return commit_ec;
    }
    latency_stats_.txn_retries.fetch_add(1, std::memory_order_relaxed);
    sleep_for_msec(10 * (attempt + 1));
  }
  latency_stats_.txn_max_retries_exceeded.fetch_add(1, std::memory_order_relaxed);
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::select_storage_tier(
    tenant_id_t tenant_id, const std::string &bucket_name,
    const std::string &object_name, ObjectValue &object_value,
    const std::string &data, uint64_t estimated_size,
    VersioningState *out_versioning_state, const PutCondition *cond,
    std::span<const uint8_t> tags, std::span<const uint8_t> metadata)
{
  const auto tc = tier_config_state_.active_copy();
  if (data.size() <= tc.max_inline) {
    object_value.hdr.chunk_type = CHUNK_INLINE;
    object_value.inline_data.assign(data.begin(), data.end());
    return put_object_single_txn(tenant_id, bucket_name, object_name, object_value,
                                 data, out_versioning_state, cond, tags, metadata);
  }
  if (data.size() <= tc.max_kv_store) {
    object_value.hdr.chunk_type = CHUNK_CHILD_D;
    return put_object_single_txn(tenant_id, bucket_name, object_name, object_value,
                                 data, out_versioning_state, cond, tags, metadata);
  }
  object_value.hdr.chunk_type = CHUNK_STORAGE;

  auto cached_bid = get_bucket_id_cached(tenant_id, bucket_name);
  if (!cached_bid) {
    return fdb_to_error(cached_bid.error());
  }
  if (!*cached_bid) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }

  const bucket_id_t bid = (**cached_bid).bucket_id;
  KeyBuf po_key;
  const auto& ref_tag = object_value.hdr.ref_tag;
  make_po_key(bid, ref_tag.view(), po_key);
  GcDescriptor po_gc{};
  po_gc.chunk = ChunkDescriptor(CHUNK_STORAGE, ref_tag, bid);
  po_gc.object_size = estimated_size;
  po_gc.mtime = static_cast<uint32_t>(now_unix());
  PoValueBuf po_val_buf;
  make_po_value(po_val_buf, po_gc, object_name);
  fdb_error_t err = store_.set(po_key.view(), po_val_buf.view());
  if (err) {
    return fdb_to_error(err);
  }

  const Stopwatch ds_t0;
  auto write_ec = data_store_.write(ref_tag.filename(), data);
  fdb_record_disk(ds_t0.elapsed_us());
  if (write_ec) {
    return KVRGW_ERR_INTERNAL;
  }

  // Fault injection: abort after Phase 2, leaving stale individual P:O + orphan
  // blob
  if (err_insertion_.is_error_active(FaultType::kAbortAfterSinglePhase2)) {
    return KVRGW_ERR_INTERNAL;
  }

  return put_object_phase3(tenant_id, bucket_name, bid, object_name,
                           object_value, out_versioning_state, cond, tags,
                           metadata);
}

//--------------------------------------------------------------------------------
KvRgwServiceImpl::PutObjectResult
KvRgwServiceImpl::put_object(tenant_id_t tenant_id,
                              std::string_view bucket_name,
                              std::string_view object_name,
                              const uint8_t *data, size_t data_len,
                              std::string_view content_type,
                              uint64_t estimated_size,
                              const AWSPutCondition *aws_cond,
                              std::span<const TagPair> tags,
                              std::span<const MetaPair> metadata)
{
  // Two independent, unrelated stats sharing one call site: the exact
  // end-client-supplied object-name and bucket-name lengths. Recorded here
  // (not in put_finalize()) so they have a 1:1 relationship with logical
  // put_object() calls, immune to internal transaction retries.
  store_.fdb_put_stats().record_key(object_name.size());
  store_.fdb_put_stats().record_bucket_name(bucket_name.size());

  if (content_type.empty()) {
    content_type = "application/octet-stream";
  }

  const auto ref_tag = ref_tags_.next();

  unsigned char digest[16];
  unsigned int digest_len = 0;
  EVP_MD_CTX *md5_ctx = EVP_MD_CTX_new();
  if (md5_ctx == nullptr) {
    return PutObjectResult{KVRGW_ERR_INTERNAL};
  }
  EVP_DigestInit_ex(md5_ctx, EVP_md5(), nullptr);
  if (data_len > 0) {
    EVP_DigestUpdate(md5_ctx, data, data_len);
  }
  EVP_DigestFinal_ex(md5_ctx, digest, &digest_len);
  EVP_MD_CTX_free(md5_ctx);

  ObjectValue object_value;
  object_value.hdr.ref_tag = ref_tag;
  object_value.set_etag_raw(digest);
  object_value.hdr.size = data_len;
  stamp_mtime(object_value.hdr);
  object_value.content_type = std::string(content_type);

  std::vector<uint8_t> tag_buf;
  if (!tags.empty()) {
    if (!encode_tags(tags, tag_buf)) {
      return PutObjectResult{KVRGW_ERR_INVALID_TAG};
    }
  }

  std::array<uint8_t, MAX_META_FRAME_BYTES> meta_buf{};
  size_t meta_len = 0;
  if (!metadata.empty()) {
    if (!encode_metadata(metadata, meta_buf, meta_len)) {
      return PutObjectResult{KVRGW_ERR_INVALID_ARGUMENT};
    }
    object_value.hdr.metadata_count = static_cast<uint16_t>(metadata.size());
  }

  PutCondition bin_cond{};
  const PutCondition *cond_ptr = nullptr;
  if (aws_cond) {
    auto ec = bin_cond.encode(aws_cond->if_match, aws_cond->if_none_match);
    if (ec != KVRGW_ERR_OK) {
      return PutObjectResult{ec};
    }
    cond_ptr = &bin_cond;
  }

  PutObjectRequest req;
  req.tenant_id = tenant_id;
  req.bucket_name = std::string(bucket_name);
  req.object_name = std::string(object_name);
  req.value = std::move(object_value);
  req.estimated_size = estimated_size > 0 ? estimated_size : data_len;
  req.cond = cond_ptr;
  if (!tag_buf.empty()) {
    req.tags = std::span<const uint8_t>(tag_buf.data(), tag_buf.size());
  }
  if (meta_len > 0) {
    req.metadata = std::span<const uint8_t>(meta_buf.data(), meta_len);
  }

  return put_object_route(req, data, data_len);
}

//--------------------------------------------------------------------------------
KvRgwServiceImpl::PutObjectResult
KvRgwServiceImpl::put_object_route(PutObjectRequest &req, const uint8_t *data,
                                   size_t data_len)
{
  PutObjectResult result;
  ops_stats_.inc(OpType::kPutObject);

  const auto tc = tier_config_state_.active_copy();
  std::string data_str(reinterpret_cast<const char *>(data), data_len);

  const bool use_batch = tc.batch_size > 1;

  if (use_batch) {
    if (data_len == 0 || data_len <= tc.max_inline) {
      req.value.hdr.chunk_type = CHUNK_INLINE;
      req.value.inline_data.assign(data, data + data_len);
    }
    else if (data_len <= tc.max_kv_store) {
      req.value.hdr.chunk_type = CHUNK_CHILD_D;
    }
    else {
      req.value.hdr.chunk_type = CHUNK_STORAGE;
    }

    BatchCommitEntry entry;
    entry.tenant_id = req.tenant_id;
    entry.bucket_name = req.bucket_name;
    entry.object_name = req.object_name;
    entry.value = req.value;
    entry.data = std::move(data_str);
    entry.chunk_type = req.value.hdr.chunk_type;
    if (req.cond) {
      entry.cond = *req.cond;
      entry.has_cond = true;
    }
    if (!req.tags.empty()) {
      entry.tag_encoded.assign(req.tags.begin(), req.tags.end());
    }
    if (!req.metadata.empty()) {
      if (req.metadata.size() > entry.metadata_encoded.size()) {
        result.error_code = KVRGW_ERR_INTERNAL;
        return result;
      }
      std::copy(req.metadata.begin(), req.metadata.end(),
                entry.metadata_encoded.begin());
      entry.metadata_encoded_size = req.metadata.size();
    }
    auto fut = batch_queue_.enqueue(std::move(entry));
    auto batch_result = fut.get();
    result.error_code = batch_result.error_code;
    if (result.error_code == KVRGW_ERR_OK) {
      result.etag = req.value.etag_display();
      if (batch_result.version_id.is_valid() &&
          !batch_result.version_id.is_null()) {
        result.version_id = batch_result.version_id;
      }
    }
    return result;
  }

  VersioningState vs = VERSIONING_DISABLED;
  if (data_len == 0) {
    req.value.hdr.chunk_type = CHUNK_INLINE;
    result.error_code = put_object_single_txn(req.tenant_id, req.bucket_name,
                                              req.object_name, req.value, data_str,
                                              &vs, req.cond, req.tags, req.metadata);
  }
  else {
    result.error_code = select_storage_tier(req.tenant_id, req.bucket_name,
                                            req.object_name, req.value, data_str,
                                            req.estimated_size, &vs, req.cond, req.tags, req.metadata);
  }

  if (result.error_code == KVRGW_ERR_OK) {
    result.etag = req.value.etag_display();
    if (vs == VERSIONING_ENABLED) {
      result.version_id = req.value.hdr.version_id;
    }
  }
  return result;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode
KvRgwServiceImpl::list_buckets(tenant_id_t tenant_id,
                               std::string_view prefix,
                               std::string_view continuation_token,
                               uint32_t max_buckets,
                               ListBucketsResult *out)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kListBuckets);
  ops_stats_.inc(OpType::kListBuckets);
  *out = {};
  if (max_buckets == 0) {
    return KVRGW_ERR_INVALID_ARGUMENT;
  }
  max_buckets = std::min(max_buckets, AWS_MaxBuckets);

  const auto bprefix = make_bucket_prefix(tenant_id);
  const std::string scan_end = prefix_range_end(bprefix.view());

  constexpr int kMaxRetries = 3;
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    *out = {};

    KeyBuf bucket_key;
    if (!continuation_token.empty()) {
      make_bucket_key(tenant_id, continuation_token, bucket_key);
    }
    else {
      make_bucket_key(tenant_id, prefix, bucket_key);
    }
    std::string scan_begin(bucket_key.view());
    bool exclusive_scan = !continuation_token.empty();

    KvrgwErrorCode iter_ec = KVRGW_ERR_OK;
    while (true) {
      auto tr_result = store_.begin_transaction();
      if (!tr_result) {
        iter_ec = fdb_to_error(tr_result.error());
        break;
      }
      auto &tr = *tr_result;

      if (listing_disable_ryw_) {
        // best effort, don't fail on this
        tr->disable_ryw();
      }

      FdbRangeHolder holder(tr->kv_async_get_range_holder(scan_begin, exclusive_scan,
                                                          scan_end, kListBucketsFdbPage, streamingMode));
      exclusive_scan = true;

      const fdb_error_t wait_err = holder.wait();
      if (wait_err) {
        iter_ec = fdb_to_error(wait_err);
        break;
      }
      if (holder.count() == 0) {
        break;
      }

      size_t rsv = std::min(out->buckets.size() + static_cast<size_t>(holder.count()),
                            static_cast<size_t>(max_buckets));
      out->buckets.reserve(rsv);

      for (const auto& [key, val] : holder) {
        const auto bucket_name = parse_bucket_key_view(key);
        if (!bucket_name) [[unlikely]] {
          continue;
        }
        const BucketValueHeader* bvh = bvh_ptr(val);
        if (!bvh) [[unlikely]] {
          continue;
        }
        if (!prefix.empty() && !starts_with(*bucket_name, prefix)) {
          // prefix was depleted -> stop the scan
          return KVRGW_ERR_OK;
        }
        if (out->buckets.size() >= max_buckets) {
          out->continuation_token = out->buckets.back().name;
          return KVRGW_ERR_OK;
        }
        out->buckets.emplace_back(BucketListEntry{
            std::string(*bucket_name), bvh_created_at_unix(bvh)});
      }

      if (!holder.more()) {
        break;
      }
      scan_begin.assign(holder.back().key);
    }

    if (iter_ec == KVRGW_ERR_OK) {
      return KVRGW_ERR_OK;
    }
    if (is_retriable(iter_ec)) {
      sleep_for_msec(10 * (attempt + 1));
      continue;
    }
    *out = {};
    return iter_ec;
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::list_objects(tenant_id_t tenant_id,
                                              std::string_view bucket_name,
                                              std::string_view prefix,
                                              std::string_view delimiter,
                                              uint32_t max_keys,
                                              std::string_view continuation_token,
                                              std::string_view marker,
                                              ListObjectsResult *out)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kListObjects);
  ops_stats_.inc(OpType::kListObjects);
  *out = {};
  const std::string_view list_prefix = prefix;
  const std::string_view delim = delimiter;
  const bool use_delimiter = !delim.empty();
  max_keys = std::min((max_keys > 0 ? max_keys : AWS_MaxKeys), AWS_MaxKeys);

  std::string start_after;
  if (!marker.empty()) {
    start_after = std::string(marker);
  }
  else if (!continuation_token.empty()) {
    start_after = std::string(continuation_token);
  }

  constexpr int kMaxRetries = 3;
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    *out = {};

    auto cached = get_bucket_id_cached(tenant_id, bucket_name);
    if (!cached) {
      auto ec = fdb_to_error(cached.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    if (!*cached) {
      return KVRGW_ERR_NO_SUCH_BUCKET;
    }
    if ((*cached)->access_flags & kDenyList) {
      return KVRGW_ERR_ACCESS_DENIED;
    }
    const auto bucket_id = (*cached)->bucket_id;

    std::string scan_begin;
    if (!start_after.empty()) {
      scan_begin = make_object_key(bucket_id, start_after).view();
    }
    else if (!list_prefix.empty()) {
      scan_begin = make_object_key(bucket_id, list_prefix).view();
    }
    else {
      scan_begin = make_object_prefix(bucket_id).view();
    }

    const std::string scan_end = list_prefix.empty() ?
      prefix_range_end(make_object_prefix(bucket_id).view()) :
      prefix_range_end(make_object_key(bucket_id, list_prefix).view());

    const int batch_limit = max_keys + 1;

    int budget = 0;
    int list_iters = 0;
    std::string last_common_prefix;
    std::string last_scanned;
    std::string last_returned;
    bool truncated = false;
    bool truncated_on_common_prefix = false;
    bool need_prefix_skip = false;
    std::string prefix_skip_target;
    bool exclusive_scan = !start_after.empty();
    KvrgwErrorCode iter_ec = KVRGW_ERR_OK;
    bool restart_attempt = false;

    while (!truncated) {
      ++list_iters;

      auto tr_result = store_.begin_transaction();
      if (!tr_result) [[unlikely]] {
        iter_ec = fdb_to_error(tr_result.error());
        break;
      }
      auto &tr = *tr_result;

      if (listing_disable_ryw_) {
        // best effort, don't fail on this
        tr->disable_ryw();
      }

      auto f_bkt = issue_bucket_get(*tr, tenant_id, bucket_name);
      const Stopwatch scan_t0;
      FdbRangeHolder holder(tr->kv_async_get_range_holder(scan_begin, exclusive_scan,
                                                          scan_end, batch_limit, streamingMode));
      exclusive_scan = true;
      const fdb_error_t wait_err = holder.wait();
      latency_stats_.record_list_scan(scan_t0.elapsed_us());

      if (wait_err) [[unlikely]] {
        iter_ec = fdb_to_error(wait_err);
        break;
      }
      if (holder.count() == 0) {
        // we could check here if bucket-id is legal and retry on change
        break;
      }

      need_prefix_skip = false;
      size_t rsv = std::min(out->objects.size() + static_cast<size_t>(holder.count()),
                            static_cast<size_t>(max_keys));
      out->objects.reserve(rsv);
      for (const auto& [key, val] : holder) {
        const auto object_name = parse_object_key_view(key);
        if (!object_name) {
          continue;
        }
        last_scanned = *object_name;

        if (!list_prefix.empty() && !starts_with(*object_name, list_prefix)) {
          continue;
        }

        const ObjectValueHeader* hdr = ovh_ptr(val);
        if (!hdr) {
          continue;
        }

        if (ovh_is_delete_marker(hdr)) {
          continue;
        }

        if (use_delimiter) {
          const std::string_view relative =
            list_prefix.empty() ? *object_name
            : object_name->substr(list_prefix.size());
          const auto delim_pos = relative.find(delim);
          if (delim_pos != std::string::npos) {
            std::string common;
            common.reserve(list_prefix.size() + delim_pos + delim.size());
            common.append(list_prefix);
            common.append(relative.substr(0, delim_pos + delim.size()));
            if (common != last_common_prefix) {
              if (budget >= max_keys) {
                truncated = true;
                truncated_on_common_prefix = true;
                break;
              }
              out->common_prefixes.push_back(common);
              last_common_prefix = common;
              ++budget;
            }
            prefix_skip_target = std::string(
              make_object_key(bucket_id, prefix_range_end(common)).view());
            need_prefix_skip = true;
            break;
          }
        }

        if (budget >= max_keys) {
          truncated = true;
          break;
        }

        last_returned = *object_name;
        out->objects.emplace_back(ObjectListEntry{
            std::string(*object_name), ovh_size(hdr),
            ovh_etag_display(hdr), ovh_last_modified_sec(hdr)});
        ++budget;
      }

      // check bucket future now (no latency added)
      auto vb = resolve_bucket_verify(tenant_id, bucket_name, f_bkt, bucket_id,
                                      kDenyList);
      if (!vb) [[unlikely]] {
        iter_ec = vb.error();
        if (iter_ec == KVRGW_ERR_BUCKET_ID_MISMATCH) {
          // bucket was overwritten -> restart scan clean
          restart_attempt = true;
          break;
        }
        // otherwise fall through to common error handler
        break;
      }

      if (need_prefix_skip) {
        scan_begin = prefix_skip_target;
        exclusive_scan = false;
        continue;
      }

      if (out->objects.size() == static_cast<size_t>(max_keys)) {
        truncated = true;
      }

      if (truncated || !holder.more()) {
        break;
      }

      // copy only the last key for the next page — one alloc per page
      scan_begin.assign(holder.back().key);
    }

    latency_stats_.record_list_call(list_iters);

    if (restart_attempt) [[unlikely]] {
      *out = {};
      continue;
    }

    if (iter_ec != KVRGW_ERR_OK) [[unlikely]] {
      if (is_retriable(iter_ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      // clear out buffer before reporting an error
      *out = {};
      return iter_ec;
    }

    out->is_truncated = truncated;
    if (truncated) {
      std::string continuation;
      if (truncated_on_common_prefix && !last_scanned.empty()) {
        continuation = last_scanned;
      }
      else if (!last_returned.empty()) {
        continuation = last_returned;
      }
      else if (!last_scanned.empty()) {
        continuation = last_scanned;
      }
      if (!continuation.empty()) {
        out->next_continuation_token = continuation;
      }
    }

    return KVRGW_ERR_OK;
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::delete_object_version(
    tenant_id_t tenant_id, std::string_view bucket_name, std::string_view key,
    version_id_t version_id, const AWSDeleteCondition *aws_cond)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kDeleteObjectVersion);
  ops_stats_.inc(OpType::kDeleteObjectVersion);

  GetCondition bin_cond{};
  if (aws_cond) {
    auto ec = bin_cond.encode(aws_cond->if_match,
                              static_cast<uint32_t>(aws_cond->if_match_last_modified_time),
                              static_cast<uint64_t>(aws_cond->if_match_size),
                              aws_cond->has_if_match_size);
    if (ec != KVRGW_ERR_OK) {
      return ec;
    }
  }
  auto bucket_id_res = get_bucket_id_cached(tenant_id, bucket_name);
  if (!bucket_id_res) {
    return fdb_to_error(bucket_id_res.error());
  }
  if (!*bucket_id_res) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  const auto bucket_id = (**bucket_id_res).bucket_id;
  const version_id_t target_vid = version_id;

  KeyBuf object_key;
  make_object_key(bucket_id, key, object_key);
  KeyBuf v_key;
  make_v_key(bucket_id, key, target_vid, v_key);

  constexpr int kMaxRetries = 3;
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    auto tr_result = store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;

    auto f_current = tr->kv_async_get(object_key.view());
    auto f_v = tr->kv_async_get(v_key.view());

    auto current_raw = tr->kv_wait_get(f_current);
    if (!current_raw) {
      auto ec = fdb_to_error(current_raw.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }

    std::optional<ObjectValue> current;
    if (*current_raw) {
      current = parse_object_value(**current_raw);
    }

    const bool has_cond = aws_cond && bin_cond.flags.has_any();

    if (current && current->hdr.version_id == target_vid) {
      if (has_cond) {
        if (current->is_delete_marker()) {
          return KVRGW_ERR_PRECONDITION_FAILED;
        }
        if (bin_cond.flags.if_match() && !bin_cond.flags.etag_is_star() &&
            current->hdr.etag != bin_cond.etag) {
          return KVRGW_ERR_PRECONDITION_FAILED;
        }
        if (bin_cond.flags.has_mtime() &&
            static_cast<int64_t>(current->hdr.last_modified_sec) !=
                static_cast<int64_t>(bin_cond.mtime)) {
          return KVRGW_ERR_PRECONDITION_FAILED;
        }
        if (bin_cond.flags.has_size() && current->hdr.size != bin_cond.size) {
          return KVRGW_ERR_PRECONDITION_FAILED;
        }
      }
      // Case 2: removing the current version — promote latest V:
      const auto v_prefix = make_v_prefix(bucket_id, key);
      const auto v_end = prefix_range_end(v_prefix.view());
      auto v_scan = tr->kv_range_scan(v_prefix.view(), v_end, 1);
      if (!v_scan) {
        auto ec = fdb_to_error(v_scan.error());
        if (is_retriable(ec)) {
          sleep_for_msec(10 * (attempt + 1));
          continue;
        }
        return ec;
      }

      if (!v_scan->empty()) {
        auto promoted = parse_object_value(v_scan->front().value);
        if (promoted) {
          promoted->hdr.next_vid = current->hdr.next_vid;
          OValueBuf pbuf;
          if (write_object_value(pbuf, *promoted)) {
            tr->kv_put(object_key.view(), pbuf.view());
          }
          tr->kv_del(std::string_view(v_scan->front().key));
        }
      }
      else {
        tr->kv_del(object_key.view());
      }

      if (current->has_data()) {
        const auto parts = parse_object_key(object_key.view());
        if (parts) {
          const std::string_view ref_sv = current->hdr.ref_tag.view();
          if (current->hdr.chunk_type == CHUNK_CHILD_D) {
            const uint8_t st = d_size_tier_from_size(current->hdr.size);
            const uint32_t mtime =
                static_cast<uint32_t>(current->hdr.last_modified_sec);
            KeyBuf d_key;
            make_d_key(parts->bucket_id, st, ref_sv, mtime, d_key);
            tr->kv_del(d_key.view());
          }
          else if (current->hdr.chunk_type == CHUNK_STORAGE) {
            KeyBuf go_key;
            make_go_key(parts->shard_count, parts->shard_id,
                        parts->bucket_id, ref_sv, current->hdr.size, go_key);
            GcValueHeader gc_hdr{};
            gc_hdr.chunk_type = current->hdr.chunk_type;
            GcDescriptor gc{};
            gc.chunk = ChunkDescriptor(current->hdr.chunk_type, ref_sv.data(), parts->bucket_id);
            gc.object_size = current->hdr.size;
            gc.mtime = current->hdr.last_modified_sec;
            GcValueBuf gc_buf_1;
            make_gc_value(gc_buf_1, gc_hdr, gc);
            tr->kv_put(go_key.view(), gc_buf_1.view());
          }
        }
      }
    }
    else {
      // Case 1: removing a non-current version from V: (already issued in
      // pipeline)
      auto v_raw = tr->kv_wait_get(f_v);
      if (!v_raw) {
        auto ec = fdb_to_error(v_raw.error());
        if (is_retriable(ec)) {
          sleep_for_msec(10 * (attempt + 1));
          continue;
        }
        return ec;
      }
      if (!*v_raw) {
        return KVRGW_ERR_OK;
      }

      auto v_entry = parse_object_value(**v_raw);
      if (has_cond && v_entry) {
        if (v_entry->is_delete_marker()) {
          return KVRGW_ERR_PRECONDITION_FAILED;
        }
        if (bin_cond.flags.if_match() && !bin_cond.flags.etag_is_star() &&
            v_entry->hdr.etag != bin_cond.etag) {
          return KVRGW_ERR_PRECONDITION_FAILED;
        }
        if (bin_cond.flags.has_mtime() &&
            static_cast<int64_t>(v_entry->hdr.last_modified_sec) !=
                static_cast<int64_t>(bin_cond.mtime)) {
          return KVRGW_ERR_PRECONDITION_FAILED;
        }
        if (bin_cond.flags.has_size() && v_entry->hdr.size != bin_cond.size) {
          return KVRGW_ERR_PRECONDITION_FAILED;
        }
      }
      if (v_entry && v_entry->has_data()) {
        const auto parts = parse_object_key(object_key.view());
        if (parts) {
          const std::string_view ref_sv = v_entry->hdr.ref_tag.view();
          KeyBuf go_key;
          make_go_key(parts->shard_count, parts->shard_id,
                      parts->bucket_id, ref_sv, v_entry->hdr.size, go_key);
          GcValueHeader gc_hdr{};
          gc_hdr.chunk_type = v_entry->hdr.chunk_type;
          GcDescriptor gc{};
          gc.chunk = ChunkDescriptor(v_entry->hdr.chunk_type, ref_sv.data(), parts->bucket_id);
          gc.object_size = v_entry->hdr.size;
          gc.mtime = v_entry->hdr.last_modified_sec;
          GcValueBuf gc_buf_2;
          make_gc_value(gc_buf_2, gc_hdr, gc);
          tr->kv_put(go_key.view(), gc_buf_2.view());
        }
      }
      tr->kv_del(v_key.view());
    }

    fdb_error_t err = tr->commit();
    if (!err) {
      return KVRGW_ERR_OK;
    }
    auto commit_ec = fdb_to_error(err);
    if (!is_retriable(commit_ec)) {
      return commit_ec;
    }
    sleep_for_msec(10 * (attempt + 1));
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode
KvRgwServiceImpl::delete_multi(tenant_id_t tenant_id,
                               std::string_view bucket_name,
                               std::span<const std::string> keys,
                               std::span<const DeleteMultiObjectRef> objects,
                               std::vector<DeleteMultiKeyOutcome> *out)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kDeleteMulti);
  ops_stats_.inc(OpType::kDeleteMulti);
  constexpr int kTxnFailThreshold = 3;
  out->clear();

  const std::string bname(bucket_name);
  auto bucket_id_res = get_bucket_id_cached(tenant_id, bname);
  if (!bucket_id_res) {
    return fdb_to_error(bucket_id_res.error());
  }
  if (!*bucket_id_res) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  auto ac = check_access(tenant_id, bname, **bucket_id_res, kDenyWrite);
  if (ac != KVRGW_ERR_OK) {
    return ac;
  }
  const auto bucket_id = (**bucket_id_res).bucket_id;

  int txn_failures = 0;
  bool single_delete_mode = false;

  // TBD: version-deletion must be refactored to work in bulk
  if (!objects.empty()) {
    out->reserve(out->size() + objects.size());
    for (const auto &obj : objects) {
      if (obj.version_id) {
        const auto ec = delete_object_version(tenant_id, bname, obj.key,
                                              *obj.version_id, nullptr);
        if (ec == KVRGW_ERR_OK) {
          out->emplace_back(DeleteMultiKeyOutcome::Status::Deleted,
                            std::string(obj.key), "");
          out->back().version_id = *obj.version_id;
        }
        else {
          out->emplace_back(DeleteMultiKeyOutcome::Status::Error,
                            std::string(obj.key), "InternalError");
          out->back().error_message = kvrgw_strerror(ec);
          out->back().version_id = *obj.version_id;
        }
      }
      else {
        delete_multi_one_key(tenant_id, bname, bucket_id, obj.key, *out);
      }
    }
    return KVRGW_ERR_OK;
  }

  const int key_count = static_cast<int>(keys.size());
  out->reserve(out->size() + static_cast<size_t>(key_count));
  // Allocate the per-key ctx array once — reused across all chunk iterations.
  std::unique_ptr<DeleteContext[]> ctx_buf(new DeleteContext[kMaxBatchSize]);
  for (int i = 0; i < key_count;) {
    if (single_delete_mode) {
      delete_multi_one_key(tenant_id, bname, bucket_id,
                           keys[static_cast<size_t>(i)], *out);
      ++i;
      continue;
    }

    const int chunk_end = std::min(i + kMaxBatchSize, key_count);
    const auto chunk = keys.subspan(static_cast<size_t>(i),
                                    static_cast<size_t>(chunk_end - i));
    const std::span<DeleteContext> ctxs(ctx_buf.get(), chunk.size());

    if (!delete_multi_try_commit(tenant_id, bname, bucket_id, chunk, ctxs, out)) {
      ++txn_failures;
      for (const auto &key : chunk) {
        delete_multi_one_key(tenant_id, bname, bucket_id, key, *out);
      }
      if (txn_failures > kTxnFailThreshold) {
        single_delete_mode = true;
      }
    }
    i = chunk_end;
  }
  return KVRGW_ERR_OK;
}

//---------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::delete_bucket(tenant_id_t tenant_id,
                                               std::string_view bucket_name)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kDeleteBucket);
  ops_stats_.inc(OpType::kDeleteBucket);
  constexpr int kMaxRetries = 3;
  // generate key to the bucket entry
  KeyBuf bucket_key;
  make_bucket_key(tenant_id, bucket_name, bucket_key);

  auto cached = get_bucket_id_cached(tenant_id, bucket_name);
  if (!cached) {
    // failure
    return fdb_to_error(cached.error());
  }
  if (!*cached) {
    // bucket-already gone -> abort
    return KVRGW_ERR_OK;
  }

  auto ac = check_access(tenant_id, bucket_name, **cached, kDenyDeleteBucket);
  if (ac != KVRGW_ERR_OK) {
    return ac;
  }
  const auto bucket_id = (**cached).bucket_id;

  // start txn
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    auto tr_result = store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      if (is_retriable(ec)) {
        // A retriable FDB error poisons that txn -> restart a fresh txn
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;

    // block on bucket-entry KV
    auto bkt = tr->kv_get(bucket_key.view());
    if (!bkt) {
      // failure
      auto ec = fdb_to_error(bkt.error());
      if (is_retriable(ec)) {
        // A retriable FDB error poisons that txn -> restart a fresh txn
        continue;
      }
      return ec;
    }

    if (!*bkt) {
      // bucket-already gone -> abort
      invalidate_bucket_cache(tenant_id, bucket_name);
      return KVRGW_ERR_OK;
    }

    auto bv = parse_bucket_value(**bkt);
    if (!bv) {
      return KVRGW_ERR_CORRUPT_VALUE;
    }
    if (bv->access_flags & kDenyDeleteBucket) {
      return KVRGW_ERR_ACCESS_DENIED;
    }

    // scan objects - must be empty
    const auto object_prefix = make_object_prefix(bucket_id);
    const auto object_end = prefix_range_end(object_prefix.view());
    auto obj_check = tr->kv_range_scan(object_prefix.view(), object_end, 1);
    if (!obj_check) {
      // failure
      auto ec = fdb_to_error(obj_check.error());
      if (is_retriable(ec)) {
        // A retriable FDB error poisons that txn -> restart a fresh txn
        continue;
      }
      return ec;
    }
    if (!obj_check->empty()) {
      return KVRGW_ERR_BUCKET_NOT_EMPTY;
    }

    // scan versions - must be empty
    const KeyBuf v_prefix_buf = make_version_prefix(bucket_id);
    const auto v_end = prefix_range_end(v_prefix_buf.view());
    auto v_check = tr->kv_range_scan(v_prefix_buf.view(), v_end, 1);
    if (!v_check) {
      // failure
      auto ec = fdb_to_error(v_check.error());
      if (is_retriable(ec)) {
        // A retriable FDB error poisons that txn -> restart a fresh txn
        continue;
      }
      return ec;
    }
    if (!v_check->empty()) {
      return KVRGW_ERR_BUCKET_NOT_EMPTY;
    }

    tr->kv_del(bucket_key.view());
    fdb_error_t err = tr->commit();
    if (!err) {
      invalidate_bucket_cache(tenant_id, bucket_name);
      sweeper_.process_bucket(bucket_id, true);
      return KVRGW_ERR_OK;
    }
    auto ec = fdb_to_error(err);
    if (is_retriable(ec)) {
      // A retriable FDB error poisons that txn -> restart a fresh txn
      continue;
    }
    return ec;
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//---------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::put_bucket_policy(tenant_id_t tenant_id,
                                                   std::string_view bucket_name,
                                                   std::string_view policy_json)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kPutBucketPolicy);
  ops_stats_.inc(OpType::kPutBucketPolicy);
  KeyBuf bucket_key;
  make_bucket_key(tenant_id, bucket_name, bucket_key);

  auto existing = store_.get(bucket_key.view());
  if (!existing) {
    return fdb_to_error(existing.error());
  }
  if (!*existing) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  auto bv = parse_bucket_value(**existing);
  if (!bv) {
    return KVRGW_ERR_INTERNAL;
  }

  const uint8_t flags = parse_policy_flags(policy_json);
  const auto new_val = make_bucket_value(bv->bucket_id, bv->created_at_unix, flags,
                                         bv->versioning_state, policy_json);

  auto tr = store_.begin_transaction();
  if (!tr) {
    return fdb_to_error(tr.error());
  }
  (*tr)->kv_put(bucket_key.view(), new_val);
  fdb_error_t err = (*tr)->commit();
  if (err) {
    return fdb_to_error(err);
  }
  invalidate_bucket_cache(tenant_id, bucket_name);
  return KVRGW_ERR_OK;
}

//---------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::get_bucket_policy(tenant_id_t tenant_id,
                                                   std::string_view bucket_name,
                                                   std::string *out_policy_json)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kGetBucketPolicy);
  ops_stats_.inc(OpType::kGetBucketPolicy);
  KeyBuf bucket_key;
  make_bucket_key(tenant_id, bucket_name, bucket_key);
  auto val = store_.get(bucket_key.view());
  if (!val) {
    return fdb_to_error(val.error());
  }
  if (!*val) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  auto bv = parse_bucket_value(**val);
  if (!bv) {
    return KVRGW_ERR_INTERNAL;
  }
  *out_policy_json = std::move(bv->policy_json);
  return KVRGW_ERR_OK;
}

//---------------------------------------------------------------------------------
KvrgwErrorCode
KvRgwServiceImpl::delete_bucket_policy(tenant_id_t tenant_id,
                                       std::string_view bucket_name)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kDeleteBucketPolicy);
  ops_stats_.inc(OpType::kDeleteBucketPolicy);
  KeyBuf bucket_key;
  make_bucket_key(tenant_id, bucket_name, bucket_key);

  auto existing = store_.get(bucket_key.view());
  if (!existing) {
    return fdb_to_error(existing.error());
  }
  if (!*existing) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  auto bv = parse_bucket_value(**existing);
  if (!bv) {
    return KVRGW_ERR_INTERNAL;
  }

  const auto new_val = make_bucket_value(bv->bucket_id, bv->created_at_unix, 0,
                                         bv->versioning_state, "");

  auto tr = store_.begin_transaction();
  if (!tr) {
    return fdb_to_error(tr.error());
  }
  (*tr)->kv_put(bucket_key.view(), new_val);
  if (fdb_error_t err = (*tr)->commit()) {
    return fdb_to_error(err);
  }
  invalidate_bucket_cache(tenant_id, bucket_name);
  return KVRGW_ERR_OK;
}

//---------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::put_bucket_versioning(tenant_id_t tenant_id,
                                                       std::string_view bucket_name,
                                                       VersioningState state)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kPutBucketVersioning);
  ops_stats_.inc(OpType::kPutBucketVersioning);
  constexpr int kMaxRetries = 3;
  KeyBuf bucket_key;
  make_bucket_key(tenant_id, bucket_name, bucket_key);

  // start txn
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    auto tr_result = store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      if (is_retriable(ec)) {
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;

    // block on bucket-entry KV
    auto existing = tr->kv_get(bucket_key.view());
    if (!existing) {
      // failure
      auto ec = fdb_to_error(existing.error());
      if (is_retriable(ec)) {
        // A retriable FDB error poisons that txn -> restart a fresh txn
        continue;
      }
      return ec;
    }
    if (!*existing) {
      return KVRGW_ERR_NO_SUCH_BUCKET;
    }

    auto bv = parse_bucket_value(**existing);
    if (!bv) {
      return KVRGW_ERR_INTERNAL;
    }

    const auto new_val =
        make_bucket_value(bv->bucket_id, bv->created_at_unix, bv->access_flags,
                          state, bv->policy_json);
    tr->kv_put(bucket_key.view(), new_val);

    fdb_error_t err = tr->commit();
    if (!err) {
      invalidate_bucket_cache(tenant_id, bucket_name);
      return KVRGW_ERR_OK;
    }
    auto commit_ec = fdb_to_error(err);
    if (is_retriable(commit_ec)) {
      // A retriable FDB error poisons that txn -> restart a fresh txn
      sleep_for_msec(10 * (attempt + 1));
      continue;
    }
    return commit_ec;
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//---------------------------------------------------------------------------------
KvrgwErrorCode
KvRgwServiceImpl::get_bucket_versioning(tenant_id_t tenant_id,
                                        std::string_view bucket_name,
                                        VersioningState *out_state)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kGetBucketVersioning);
  ops_stats_.inc(OpType::kGetBucketVersioning);
  auto state = read_bucket_state(tenant_id, bucket_name);
  if (!state) {
    return fdb_to_error(state.error());
  }
  if (!*state) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  *out_state = (*state)->versioning_state;
  return KVRGW_ERR_OK;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::copy_object(const CopyObjectRequest &req,
                                             CopyObjectResult *out)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kCopyObject);
  ops_stats_.inc(OpType::kCopyObject);
  const tenant_id_t tenant_id = req.tenant_id;
  const std::string_view src_bucket = req.src_bucket_name;
  const std::string_view dst_bucket = req.dst_bucket_name;
  const std::string_view src_key = req.src_key;
  const std::string_view dst_key = req.dst_key;

  auto src_cached = get_bucket_id_cached(tenant_id, src_bucket);
  if (!src_cached) {
    return fdb_to_error(src_cached.error());
  }
  if (!*src_cached) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  const bucket_id_t src_bucket_id = (**src_cached).bucket_id;

  bucket_id_t dst_bucket_id;
  if (dst_bucket == src_bucket) {
    dst_bucket_id = src_bucket_id;
  }
  else {
    auto dst_cached = get_bucket_id_cached(tenant_id, dst_bucket);
    if (!dst_cached) {
      return fdb_to_error(dst_cached.error());
    }
    if (!*dst_cached) {
      return KVRGW_ERR_NO_SUCH_BUCKET;
    }
    dst_bucket_id = (**dst_cached).bucket_id;
  }

  KeyBuf src_o_key;
  make_object_key(src_bucket_id, src_key, src_o_key);
  KeyBuf dst_o_key;
  make_object_key(dst_bucket_id, dst_key, dst_o_key);

  constexpr int kMaxRetries = 3;
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    auto tr_result = store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;

    // --- Group 1: pipeline dst B: + src S:O + dst S:O ---
    auto f_dst_bkt = issue_bucket_get(*tr, tenant_id, dst_bucket);
    FdbGetHolder src_o_h(tr->kv_async_get(src_o_key.view()));
    FdbGetHolder dst_o_h(tr->kv_async_get(dst_o_key.view()));

    {
      auto src_o_err = src_o_h.wait();
      if (src_o_err) {
        auto ec = fdb_to_error(src_o_err);
        if (is_retriable(ec)) {
          sleep_for_msec(10 * (attempt + 1));
          continue;
        }
        return ec;
      }
    }

    // --- Read source ---
    ObjectValue src;
    std::string src_entry_key;

    if (req.src_version_id) {
      const version_id_t src_vid = *req.src_version_id;
      bool found_in_o = false;
      if (src_o_h.present()) {
        auto parsed = parse_object_value(src_o_h.value());
        if (parsed && parsed->hdr.version_id == src_vid) {
          src = std::move(*parsed);
          src_entry_key.assign(src_o_key.view());
          found_in_o = true;
        }
      }
      if (!found_in_o) {
        KeyBuf v_key;
        make_v_key(src_bucket_id, src_key, src_vid, v_key);
        auto v_raw = tr->kv_get(v_key.view());
        if (!v_raw) {
          auto ec = fdb_to_error(v_raw.error());
          if (is_retriable(ec)) {
            sleep_for_msec(10 * (attempt + 1));
            continue;
          }
          return ec;
        }
        if (!*v_raw) {
          return KVRGW_ERR_NO_SUCH_VERSION;
        }
        auto parsed = parse_object_value(**v_raw);
        if (!parsed) {
          return KVRGW_ERR_INTERNAL;
        }
        src = std::move(*parsed);
        src_entry_key.assign(v_key.view());
      }
    }
    else {
      if (!src_o_h.present()) {
        return KVRGW_ERR_NO_SUCH_KEY;
      }
      auto parsed = parse_object_value(src_o_h.value());
      if (!parsed) {
        return KVRGW_ERR_INTERNAL;
      }
      src = std::move(*parsed);
      src_entry_key.assign(src_o_key.view());
    }

    if (src.is_delete_marker()) {
      return KVRGW_ERR_NO_SUCH_KEY;
    }

    // --- Source preconditions ---
    if (!req.if_match.empty()) {
      if (req.if_match != "*" && src.etag_display() != req.if_match) {
        return KVRGW_ERR_PRECONDITION_FAILED;
      }
    }
    if (!req.if_none_match.empty()) {
      if (req.if_none_match == "*") {
        return KVRGW_ERR_PRECONDITION_FAILED;
      }
      if (src.etag_display() == req.if_none_match) {
        return KVRGW_ERR_PRECONDITION_FAILED;
      }
    }

    // --- Resolve destination O: (already issued in Group 1) ---
    {
      auto dst_o_err = dst_o_h.wait();
      if (dst_o_err) {
        auto ec = fdb_to_error(dst_o_err);
        if (is_retriable(ec)) {
          sleep_for_msec(10 * (attempt + 1));
          continue;
        }
        return ec;
      }
    }
    const ObjectValueHeader *dst_hdr = nullptr;
    std::string_view dst_raw;
    if (dst_o_h.present()) {
      dst_raw = dst_o_h.value();
      dst_hdr = ovh_ptr(dst_raw);
    }

    // --- Destination preconditions ---
    if (!req.dst_if_match.empty()) {
      if (!dst_hdr || ovh_is_delete_marker(dst_hdr)) {
        return KVRGW_ERR_NO_SUCH_KEY;
      }
      if (req.dst_if_match != "*" &&
          ovh_etag_display(dst_hdr) != req.dst_if_match) {
        return KVRGW_ERR_PRECONDITION_FAILED;
      }
    }
    if (!req.dst_if_none_match.empty()) {
      if (req.dst_if_none_match == "*" && dst_hdr &&
          !ovh_is_delete_marker(dst_hdr)) {
        return KVRGW_ERR_PRECONDITION_FAILED;
      }
    }

    // --- Metadata-only self-copy optimization ---
    const bool same_object = (src_bucket_id == dst_bucket_id &&
                              src_key == dst_key && !req.src_version_id);

    if (same_object && !req.replace_metadata) {
      return KVRGW_ERR_INVALID_REQUEST;
    }

    // Deferred B: resolve — consumed by self-copy or normal path (whichever
    // runs first)
    std::optional<BucketState> vb_resolved;
    auto resolve_bkt_once =
        [&]() -> std::expected<BucketState, KvrgwErrorCode> {
      if (!vb_resolved) {
        auto vb = resolve_bucket_verify(tenant_id, dst_bucket, f_dst_bkt,
                                        dst_bucket_id);
        if (!vb) {
          return vb;
        }
        vb_resolved = *vb;
      }
      return *vb_resolved;
    };

    if (same_object && (req.replace_metadata || req.replace_tags)) {
      auto vb = resolve_bkt_once();
      if (!vb) {
        return vb.error();
      }
      const bool in_place = (vb->versioning_state == VERSIONING_DISABLED) ||
                            (vb->versioning_state == VERSIONING_SUSPENDED &&
                             src.hdr.version_id == kNullVersion);
      if (in_place) {
        if (req.replace_metadata) {
          src.content_type = req.content_type;
          src.hdr.last_modified_sec = static_cast<uint32_t>(now_unix());
          if (req.metadata.empty()) {
            src.hdr.metadata_count = 0;
            src.metadata_frame.clear();
          }
          else {
            src.hdr.metadata_count =
                read_be_field<tag_count_t>(req.metadata.data());
            src.metadata_frame.assign(req.metadata.begin(), req.metadata.end());
          }
        }
        if (req.replace_tags) {
          if (req.tags.empty()) {
            return KVRGW_ERR_INVALID_TAG;
          }
          apply_tags_to_value(src, req.tags, *tr, dst_bucket_id,
                              src.hdr.ref_tag.view());
        }
        OValueBuf buf;
        if (!write_object_value(buf, src)) {
          return KVRGW_ERR_INTERNAL;
        }
        tr->kv_put(dst_o_key.view(), buf.view());
        fdb_error_t err = tr->commit();
        if (!err) {
          out->etag = src.etag_display();
          out->last_modified_unix = src.hdr.last_modified_sec;
          out->copy_source_version_id = src.hdr.version_id;
          return KVRGW_ERR_OK;
        }
        {
          auto ec = fdb_to_error(err);
          if (!is_retriable(ec)) {
            return ec;
          }
        }
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
    }

    // --- Build new value ---
    ObjectValue new_value;
    const auto new_ref_tag = ref_tags_.next();
    new_value.hdr.ref_tag = new_ref_tag;
    new_value.set_etag(src.get_etag());
    new_value.hdr.size = src.hdr.size;
    new_value.hdr.last_modified_sec = static_cast<uint32_t>(now_unix());
    new_value.hdr.last_modified_nsec = 0;
    if (req.replace_metadata) {
      new_value.content_type = std::string(req.content_type);
    }
    else {
      new_value.content_type = src.content_type;
    }

    if (req.replace_metadata) {
      if (!req.metadata.empty()) {
        new_value.hdr.metadata_count =
            read_be_field<tag_count_t>(req.metadata.data());
        new_value.metadata_frame.assign(req.metadata.begin(),
                                        req.metadata.end());
      }
    }
    else {
      new_value.hdr.metadata_count = src.hdr.metadata_count;
      new_value.metadata_frame = src.metadata_frame;
    }

    // --- Data sharing by tier ---
    switch (src.hdr.chunk_type) {
    case CHUNK_INLINE: {
      new_value.hdr.chunk_type = CHUNK_INLINE;
      new_value.inline_data = src.inline_data;
      break;
    }
    case CHUNK_CHILD_D:
    case CHUNK_CHILD_D_REF: {
      const bucket_id_t& d_bucket = (src.hdr.chunk_type == CHUNK_CHILD_D_REF)
        ? src.chunk.get_bucket_id() : src_bucket_id;
      const std::string_view d_ref_sv = src.data_ref_view();
      const uint8_t st = d_size_tier_from_size(src.hdr.size);
      KeyBuf d_key;
      make_d_key(d_bucket, st, d_ref_sv, src.hdr.last_modified_sec, d_key);

      auto d_raw = tr->kv_get(d_key.view());
      if (!d_raw) {
        auto ec = fdb_to_error(d_raw.error());
        if (is_retriable(ec)) {
          sleep_for_msec(10 * (attempt + 1));
          continue;
        }
        return ec;
      }
      if (!*d_raw) {
        return KVRGW_ERR_INTERNAL;
      }

      const auto dref = read_d_ref_count(**d_raw);
      if (!dref.shared) {
        tr->kv_put(d_key.view(),
                   write_d_with_ref(d_data_portion(**d_raw, src.hdr.size), 2));
      }
      else {
        tr->kv_put(d_key.view(),
                   write_d_with_ref(d_data_portion(**d_raw, src.hdr.size),
                                    dref.ref_count + 1));
      }

      if (!src.has_shared_data()) {
        src.hdr.flags |= ObjectValue::kFlagSharedData;
        OValueBuf src_buf;
        if (write_object_value(src_buf, src)) {
          tr->kv_put(src_entry_key, src_buf.view());
        }
      }

      new_value.hdr.chunk_type = CHUNK_CHILD_D_REF;
      new_value.chunk = ChunkDescriptor(CHUNK_CHILD_D_REF, d_ref_sv.data(), d_bucket);
      new_value.hdr.flags |= ObjectValue::kFlagSharedData;
      break;
    }
    case CHUNK_STORAGE:
    case CHUNK_STORAGE_REF: {
      const std::string_view src_data_ref = src.data_ref_view();
      const auto r_key = make_r_key(src_data_ref);

      if (src.has_shared_data()) {
        auto r_raw = tr->kv_get(r_key.view());
        if (!r_raw) {
          auto ec = fdb_to_error(r_raw.error());
          if (is_retriable(ec)) {
            sleep_for_msec(10 * (attempt + 1));
            continue;
          }
          return ec;
        }
        if (!*r_raw) {
          return KVRGW_ERR_INTERNAL;
        }
        auto rv = parse_r_value(**r_raw);
        if (!rv) {
          return KVRGW_ERR_CORRUPT_VALUE;
        }
        tr->kv_put(r_key.view(),
                   write_r_value(rv->ref_count + 1, rv->chunk_descriptor));
      }
      else {
        GcValueHeader gc_hdr{};
        gc_hdr.chunk_type = src.hdr.chunk_type;
        GcDescriptor gc{};
        gc.chunk = ChunkDescriptor(src.hdr.chunk_type, src_data_ref.data(), src_bucket_id);
        gc.object_size = src.hdr.size;
        gc.mtime = src.hdr.last_modified_sec;
        GcValueBuf gc_buf_3;
        make_gc_value(gc_buf_3, gc_hdr, gc);
        tr->kv_put(r_key.view(), write_r_value(2, gc_buf_3.view()));

        src.hdr.flags |= ObjectValue::kFlagSharedData;
        OValueBuf src_buf;
        if (write_object_value(src_buf, src)) {
          tr->kv_put(src_entry_key, src_buf.view());
        }
      }

      new_value.hdr.chunk_type = CHUNK_STORAGE_REF;
      new_value.chunk = ChunkDescriptor(CHUNK_STORAGE_REF, src_data_ref.data(), kNullBucket);
      new_value.hdr.flags |= ObjectValue::kFlagSharedData;
      break;
    }
    case CHUNK_MULTIPART:
    case CHUNK_MULTIPART_REF: {
      // Zero-copy sharing for a completed multipart object -- same
      // ref-counted pattern as CHUNK_STORAGE/CHUNK_STORAGE_REF above, but
      // the R: value's chunk_descriptor payload slot carries the
      // manifest body (verbatim) instead of a GcValueBuf, since GcWorker
      // needs the manifest itself (not just a ref_tag+size+mtime) once
      // the last reference is dropped -- see gc_once's CHUNK_MULTIPART/
      // _REF handling.
      const std::string_view src_data_ref = src.data_ref_view();
      const auto r_key = make_r_key(src_data_ref);

      if (src.has_shared_data()) {
        auto r_raw = tr->kv_get(r_key.view());
        if (!r_raw) {
          auto ec = fdb_to_error(r_raw.error());
          if (is_retriable(ec)) {
            sleep_for_msec(10 * (attempt + 1));
            continue;
          }
          return ec;
        }
        if (!*r_raw) {
          return KVRGW_ERR_INTERNAL;
        }
        auto rv = parse_r_value(**r_raw);
        if (!rv) {
          return KVRGW_ERR_CORRUPT_VALUE;
        }
        tr->kv_put(r_key.view(),
                   write_r_value(rv->ref_count + 1, rv->chunk_descriptor));
      }
      else {
        tr->kv_put(r_key.view(), write_r_value(2, src.manifest_body));

        src.hdr.flags |= ObjectValue::kFlagSharedData;
        OValueBufMultipart src_buf;
        if (write_object_value(src_buf, src)) {
          tr->kv_put(src_entry_key, src_buf.view());
        }
      }

      new_value.hdr.chunk_type = CHUNK_MULTIPART_REF;
      new_value.hdr.part_count = src.hdr.part_count;
      new_value.manifest_body = src.manifest_body;
      new_value.chunk = ChunkDescriptor(CHUNK_MULTIPART_REF, src_data_ref.data(), kNullBucket);
      new_value.hdr.flags |= ObjectValue::kFlagSharedData;
      break;
    }
    default:
      return KVRGW_ERR_INTERNAL;
    }

    if (req.replace_tags) {
      if (req.tags.empty()) {
        return KVRGW_ERR_INVALID_TAG;
      }
      apply_tags_to_value(new_value, req.tags, *tr, dst_bucket_id,
                          new_value.hdr.ref_tag.view());
    }
    else if (src.hdr.tags_count > 0) {
      KeyBuf src_ct;
      make_ct_key(src_bucket_id, src.hdr.ref_tag.view(), src_ct);
      auto ct_val = tr->kv_get(src_ct.view());
      if (!ct_val || !*ct_val) {
        return KVRGW_ERR_CORRUPT_VALUE;
      }
      KeyBuf dst_ct;
      make_ct_key(dst_bucket_id, new_value.hdr.ref_tag.view(), dst_ct);
      tr->kv_put(dst_ct.view(), **ct_val);
      new_value.hdr.tags_count = src.hdr.tags_count;
      new_value.hdr.flags |= ObjectValue::kFlagExternalTags;
    }

    // --- Resolve B: (deferred from Group 1 — overlaps with Group 2 reads
    // above) ---
    auto vb = resolve_bkt_once();
    if (!vb) {
      return vb.error();
    }

    // --- Displace destination + versioning ---
    auto ids = displace_and_version(*tr, vb->versioning_state, dst_o_key.view(),
                                    dst_hdr, dst_raw);
    new_value.hdr.version_id = ids.version_id;
    new_value.hdr.next_vid = ids.next_vid;

    // --- Write destination O: ---
    if (!write_o_value(*tr, dst_o_key.view(), new_value)) {
      return KVRGW_ERR_INTERNAL;
    }

    fdb_error_t err = tr->commit();
    if (!err) {
      out->etag = new_value.etag_display();
      out->last_modified_unix = new_value.hdr.last_modified_sec;
      if (vb->versioning_state == VERSIONING_ENABLED) {
        out->version_id = new_value.hdr.version_id;
      }
      out->copy_source_version_id = src.hdr.version_id;
      return KVRGW_ERR_OK;
    }
    {
      auto ec = fdb_to_error(err);
      if (!is_retriable(ec)) {
        return ec;
      }
    }
    sleep_for_msec(10 * (attempt + 1));
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//namespace {
//--------------------------------------------------------------------------------
std::string delete_marker_detail(const ObjectValue &v)
{
  return "DeleteMarker:" + v.hdr.version_id.to_hex() + ":" +
         std::to_string(v.hdr.last_modified_sec);
}

//} // namespace

//--------------------------------------------------------------------------------
// on failure read a fresh bucket-state copy, replacing error code if needed
// should use it for all failures using cached bucket-state
KvrgwErrorCode KvRgwServiceImpl::resolve_bucket_error(tenant_id_t tenant_id,
                                                      std::string_view bucket_name,
                                                      KvrgwErrorCode tentative_err_code)
{
  auto state = read_bucket_state(tenant_id, bucket_name);
  if (!state) {
    return fdb_to_error(state.error());
  }
  if (*state) {
    return tentative_err_code;
  }
  else {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::load_object_for_read(tenant_id_t tenant_id,
                                                      std::string_view bucket_name,
                                                      std::string_view object_name,
                                                      std::optional<version_id_t> version_id,
                                                      bool load_kv_data,
                                                      ObjectValue *value,
                                                      std::string *kv_data,
                                                      std::string *error_detail)
{
  auto bucket_id_res = get_bucket_id_cached(tenant_id, bucket_name);
  if (!bucket_id_res) {
    return fdb_to_error(bucket_id_res.error());
  }
  if (!*bucket_id_res) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  auto ec = check_access(tenant_id, bucket_name, **bucket_id_res, kDenyRead);
  if (ec != KVRGW_ERR_OK) {
    return ec;
  }
  const bucket_id_t bucket_id = (**bucket_id_res).bucket_id;

  auto accept = [&](ObjectValue obj, std::string data) -> KvrgwErrorCode {
    if (obj.is_delete_marker()) {
      if (error_detail) {
        *error_detail = delete_marker_detail(obj);
      }
      return resolve_bucket_error(tenant_id, bucket_name, KVRGW_ERR_NO_SUCH_KEY);
    }
    *value = std::move(obj);
    if (kv_data) {
      *kv_data = std::move(data);
    }
    return KVRGW_ERR_OK;
  };

  if (version_id) {
    const version_id_t target_vid = *version_id;
    if (load_kv_data) {
      auto current_res = load_object_with_data(bucket_id, object_name);
      if (!current_res) {
        return fdb_to_error(current_res.error());
      }
      if (*current_res && (*current_res)->value.hdr.version_id == target_vid) {
        return accept(std::move((*current_res)->value),
                      std::move((*current_res)->data));
      }
    }
    else {
      auto current = load_object(bucket_id, object_name);
      if (!current) {
        return fdb_to_error(current.error());
      }
      if (*current && (*current)->hdr.version_id == target_vid) {
        return accept(std::move(**current), {});
      }
    }

    auto tr_result = store_.begin_transaction();
    if (!tr_result) {
      return fdb_to_error(tr_result.error());
    }
    auto &tr = *tr_result;
    KeyBuf v_key;
    make_v_key(bucket_id, object_name, target_vid, v_key);
    auto v_raw = tr->kv_get(v_key.view());
    if (!v_raw) {
      return fdb_to_error(v_raw.error());
    }
    if (!*v_raw) {
      return KVRGW_ERR_NO_SUCH_VERSION;
    }
    auto v_obj = parse_object_value(**v_raw);
    if (!v_obj) {
      return KVRGW_ERR_CORRUPT_VALUE;
    }

    std::string data;
    if (load_kv_data) {
      if (v_obj->hdr.chunk_type == CHUNK_INLINE) {
        data.assign(v_obj->inline_data.begin(), v_obj->inline_data.end());
      }
      else if (v_obj->hdr.chunk_type == CHUNK_CHILD_D ||
               v_obj->hdr.chunk_type == CHUNK_CHILD_D_REF) {
        const uint8_t st = d_size_tier_from_size(v_obj->hdr.size);
        const uint32_t mtime = v_obj->hdr.last_modified_sec;
        const bucket_id_t& d_bucket =
          (v_obj->hdr.chunk_type == CHUNK_CHILD_D_REF)
          ? v_obj->chunk.get_bucket_id()
          : bucket_id;
        const std::string_view ref_sv = v_obj->data_ref_view();
        KeyBuf d_key;
        make_d_key(d_bucket, st, ref_sv, mtime, d_key);
        auto d_val = tr->kv_get(d_key.view());
        if (!d_val) {
          return fdb_to_error(d_val.error());
        }
        if (!*d_val) {
          return resolve_bucket_error(tenant_id, bucket_name, KVRGW_ERR_NO_SUCH_KEY);
        }
        const auto payload = child_value_payload(**d_val, v_obj->hdr.size);
        if (payload.size() != v_obj->hdr.size) {
          return KVRGW_ERR_CORRUPT_VALUE;
        }
        data.assign(payload.begin(), payload.end());
      }
    }
    return accept(std::move(*v_obj), std::move(data));
  }

  if (load_kv_data) {
    auto result_res = load_object_with_data(bucket_id, object_name);
    if (!result_res) {
      return fdb_to_error(result_res.error());
    }
    if (!*result_res) {
      return resolve_bucket_error(tenant_id, bucket_name, KVRGW_ERR_NO_SUCH_KEY);
    }
    return accept(std::move((*result_res)->value),
                  std::move((*result_res)->data));
  }

  auto object_value = load_object(bucket_id, object_name);
  if (!object_value) {
    return fdb_to_error(object_value.error());
  }
  if (!*object_value) {
    return resolve_bucket_error(tenant_id, bucket_name, KVRGW_ERR_NO_SUCH_KEY);
  }
  return accept(std::move(**object_value), {});
}

namespace {

// Resolves a validated (1-based) PartNumber to its (generation, size)
// within an already-decoded manifest. included_parts is sorted ascending
// (decode_manifest_body's contract), so a binary search finds the exact
// match in O(log n). Returns false if part_number isn't actually present
// in the manifest (shouldn't happen once the caller's bounds check
// against hdr.part_count has passed, but checked defensively).
// CHUNK_MULTIPART_REF's part files live under the *shared* ref_tag
// (chunk.get_ref_tag()), not this object's own identity (hdr.ref_tag) --
// same split as CHUNK_STORAGE_REF's data_ref_view(). Plain CHUNK_MULTIPART
// has no such split; hdr.ref_tag is both its identity and the data's.
const RefTag &multipart_data_ref_tag(const ObjectValue &v)
{
  if (v.hdr.chunk_type == CHUNK_MULTIPART_REF) {
    return v.chunk.get_ref_tag();
  }
  return v.hdr.ref_tag;
}

bool resolve_manifest_part(const DecodedManifestBody &decoded,
                           uint16_t part_number, uint32_t *out_generation,
                           uint64_t *out_size)
{
  const auto it = std::lower_bound(decoded.included_parts.begin(),
                                   decoded.included_parts.end(), part_number);
  if (it == decoded.included_parts.end() || *it != part_number) {
    return false;
  }
  const size_t idx = static_cast<size_t>(it - decoded.included_parts.begin());
  *out_generation = decoded.generations[idx];
  *out_size = decoded.sizes[idx];
  return true;
}

}  // namespace

//--------------------------------------------------------------------------------
KvrgwErrorCode
KvRgwServiceImpl::get_object(tenant_id_t tenant_id,
                             std::string_view bucket_name,
                             std::string_view object_name,
                             std::optional<version_id_t> version_id,
                             const ByteRange *range,
                             uint16_t part_number,
                             GetObjectResult *out)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kGetObject);
  ops_stats_.inc(OpType::kGetObject);
  *out = {};

  ObjectValue object_value;
  std::string kv_data;
  const auto ec = load_object_for_read(tenant_id, bucket_name, object_name, version_id, true,
                                       &object_value, &kv_data, &out->error_detail);
  if (ec != KVRGW_ERR_OK) {
    return ec;
  }

  // PartNumber is validated uniformly across every chunk type: valid range
  // is [1, part_count] -- 1 for non-multipart objects (so PartNumber=1
  // reads the whole object, below), included_parts.size() for
  // CHUNK_MULTIPART (see object_value.hpp's ObjectValueHeader::part_count).
  if (part_number != 0 &&
      (part_number < 1 || part_number > object_value.hdr.part_count)) {
    return KVRGW_ERR_INVALID_PART;
  }

  const int64_t object_size = static_cast<int64_t>(object_value.hdr.size);
  std::string body;
  if (object_value.hdr.chunk_type == CHUNK_INLINE ||
      object_value.hdr.chunk_type == CHUNK_CHILD_D ||
      object_value.hdr.chunk_type == CHUNK_CHILD_D_REF) {
    body = std::move(kv_data);
    if (range) {
      bool invalid = false;
      const auto slice = resolve_byte_range(*range, object_size, &invalid);
      if (invalid) {
        return KVRGW_ERR_INVALID_RANGE;
      }
      if (slice) {
        body = body.substr(static_cast<size_t>(slice->start),
                           static_cast<size_t>(slice->length));
      }
    }
  }
  else if (object_value.hdr.chunk_type == CHUNK_MULTIPART ||
           object_value.hdr.chunk_type == CHUNK_MULTIPART_REF) {
    auto decoded = decode_manifest_body(object_value.manifest_body);
    if (!decoded || decoded->included_parts.empty()) [[unlikely]] {
      return KVRGW_ERR_CORRUPT_VALUE;
    }
    const RefTag &data_ref_tag = multipart_data_ref_tag(object_value);

    if (part_number != 0) {
      // PartNumber addressing: whole-part read, no range slicing. ETag
      // stays the whole object's composite ETag (set by load_object_for_read
      // above); only the reported size is overridden to this one part's.
      uint32_t generation = 0;
      uint64_t part_size = 0;
      if (!resolve_manifest_part(*decoded, part_number, &generation, &part_size)) [[unlikely]] {
        return KVRGW_ERR_CORRUPT_VALUE;
      }
      const std::string fname =
          part_filename(data_ref_tag, part_number, generation);
      if (fname.empty()) [[unlikely]] {
        return KVRGW_ERR_INTERNAL;
      }
      const Stopwatch ds_t0;
      auto rd_ec = data_store_.read(fname, 0, part_size, &body);
      fdb_record_disk(ds_t0.elapsed_us());
      if (rd_ec) {
        return KVRGW_ERR_INTERNAL;
      }
      object_value.hdr.size = part_size;
      out->value = std::move(object_value);
      out->body = std::move(body);
      return KVRGW_ERR_OK;
    }

    const size_t n = decoded->included_parts.size();
    // prefix[i] = byte offset where part i starts; prefix[n] = total size.
    std::vector<uint64_t> prefix(n + 1, 0);
    for (size_t i = 0; i < n; ++i) {
      prefix[i + 1] = prefix[i] + decoded->sizes[i];
    }

    uint64_t range_start = 0;
    uint64_t range_len = static_cast<uint64_t>(object_size);
    if (range) {
      bool invalid = false;
      const auto slice = resolve_byte_range(*range, object_size, &invalid);
      if (invalid) {
        return KVRGW_ERR_INVALID_RANGE;
      }
      if (slice) {
        range_start = static_cast<uint64_t>(slice->start);
        range_len = static_cast<uint64_t>(slice->length);
      }
    }

    if (range_len > 0) {
      // Find the first part holding data at range_start -- part i occupies
      // [prefix[i], prefix[i+1]) -- then read serially, one part after
      // another, until range_len bytes are collected. Full (no-range)
      // reads are just this same loop starting at range_start=0.
      auto it = std::upper_bound(prefix.begin(), prefix.end(), range_start);
      size_t part_idx = static_cast<size_t>(it - prefix.begin());
      part_idx = part_idx > 0 ? part_idx - 1 : 0;

      body.reserve(static_cast<size_t>(range_len));
      uint64_t cur_pos = range_start;
      uint64_t remaining = range_len;
      for (; part_idx < n && remaining > 0; ++part_idx) {
        const uint64_t part_start = prefix[part_idx];
        const uint64_t part_size = decoded->sizes[part_idx];
        if (part_size == 0 || cur_pos >= part_start + part_size) {
          continue;
        }
        const uint64_t read_off = cur_pos - part_start;
        const uint64_t to_read = std::min(part_size - read_off, remaining);
        const std::string fname = part_filename(data_ref_tag,
                                                decoded->included_parts[part_idx],
                                                decoded->generations[part_idx]);
        if (fname.empty()) [[unlikely]] {
          return KVRGW_ERR_INTERNAL;
        }
        std::string part_body;
        const Stopwatch ds_t0;
        auto rd_ec = data_store_.read(fname, read_off, to_read, &part_body);
        fdb_record_disk(ds_t0.elapsed_us());
        if (rd_ec) {
          return KVRGW_ERR_INTERNAL;
        }
        body += part_body;
        cur_pos += to_read;
        remaining -= to_read;
      }
    }
  }
  else {
    const std::string ref_sv =
        (object_value.hdr.chunk_type == CHUNK_STORAGE_REF)
            ? object_value.chunk.ref_tag_filename()
            : object_value.hdr.ref_tag.filename();
    if (range) {
      bool invalid = false;
      const auto slice = resolve_byte_range(*range, object_size, &invalid);
      if (invalid) {
        return KVRGW_ERR_INVALID_RANGE;
      }
      if (!slice) {
        const Stopwatch ds_t0;
        auto rd_ec = data_store_.read(ref_sv, 0, object_value.hdr.size, &body);
        fdb_record_disk(ds_t0.elapsed_us());
        if (rd_ec) {
          return KVRGW_ERR_INTERNAL;
        }
      }
      else {
        const Stopwatch ds_t0;
        auto rd_ec = data_store_.read(ref_sv, static_cast<uint64_t>(slice->start),
                                      static_cast<uint64_t>(slice->length), &body);
        fdb_record_disk(ds_t0.elapsed_us());
        if (rd_ec) {
          return KVRGW_ERR_INTERNAL;
        }
      }
    }
    else {
      const Stopwatch ds_t0;
      auto rd_ec = data_store_.read(ref_sv, 0, object_value.hdr.size, &body);
      fdb_record_disk(ds_t0.elapsed_us());
      if (rd_ec) {
        return KVRGW_ERR_INTERNAL;
      }
    }
  }

  out->value = std::move(object_value);
  out->body = std::move(body);
  return KVRGW_ERR_OK;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::head_object(
    tenant_id_t tenant_id, std::string_view bucket_name, std::string_view object_name,
    std::optional<version_id_t> version_id, uint16_t part_number, ObjectValue *out,
    std::string *error_detail)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kHeadObject);
  ops_stats_.inc(OpType::kHeadObject);
  const auto ec = load_object_for_read(tenant_id, bucket_name, object_name, version_id, false, out,
                                       nullptr, error_detail);
  if (ec != KVRGW_ERR_OK) {
    return ec;
  }

  // Same uniform [1, part_count] bounds check as get_object's.
  if (part_number != 0 &&
      (part_number < 1 || part_number > out->hdr.part_count)) {
    return KVRGW_ERR_INVALID_PART;
  }
  if (part_number != 0 &&
      (out->hdr.chunk_type == CHUNK_MULTIPART ||
       out->hdr.chunk_type == CHUNK_MULTIPART_REF)) {
    auto decoded = decode_manifest_body(out->manifest_body);
    if (!decoded || decoded->included_parts.empty()) [[unlikely]] {
      return KVRGW_ERR_CORRUPT_VALUE;
    }
    uint32_t generation = 0;
    uint64_t part_size = 0;
    if (!resolve_manifest_part(*decoded, part_number, &generation, &part_size)) [[unlikely]] {
      return KVRGW_ERR_CORRUPT_VALUE;
    }
    out->hdr.size = part_size;
  }
  return KVRGW_ERR_OK;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::get_object_attributes(
    tenant_id_t tenant_id, std::string_view bucket_name, std::string_view key,
    std::optional<version_id_t> version_id, uint16_t max_parts,
    uint16_t part_number_marker, GetObjectAttributesResult *out)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kOther);
  ops_stats_.inc(OpType::kOther);
  const auto ec = load_object_for_read(tenant_id, bucket_name, key, version_id, false,
                                       &out->value, nullptr, &out->error_detail);
  if (ec != KVRGW_ERR_OK) {
    return ec;
  }

  if (out->value.hdr.chunk_type != CHUNK_MULTIPART &&
      out->value.hdr.chunk_type != CHUNK_MULTIPART_REF) {
    return KVRGW_ERR_OK;
  }

  auto decoded = decode_manifest_body(out->value.manifest_body);
  if (!decoded || decoded->included_parts.empty()) [[unlikely]] {
    return KVRGW_ERR_CORRUPT_VALUE;
  }

  out->has_parts = true;
  out->total_parts_count = static_cast<uint32_t>(decoded->included_parts.size());

  const auto it = std::upper_bound(decoded->included_parts.begin(),
                                   decoded->included_parts.end(), part_number_marker);
  size_t idx = static_cast<size_t>(it - decoded->included_parts.begin());
  const size_t n = decoded->included_parts.size();
  for (; idx < n && out->part_numbers.size() < max_parts; ++idx) {
    out->part_numbers.push_back(decoded->included_parts[idx]);
    out->part_sizes.push_back(decoded->sizes[idx]);
  }
  if (idx < n) {
    out->parts_truncated = true;
    out->next_part_number_marker = out->part_numbers.empty()
        ? part_number_marker : out->part_numbers.back();
  }
  return KVRGW_ERR_OK;
}

namespace {

constexpr std::string_view kLovTok{"lov1."};

void append_version_entry(KvRgwServiceImpl::ListObjectVersionsResult *out,
                          std::string_view name, version_id_t vid,
                          bool is_latest, const ObjectValueHeader *hdr)
{
  out->versions.emplace_back(KvRgwServiceImpl::ObjectVersionEntry{
      std::string(name), vid, is_latest, ovh_is_delete_marker(hdr),
      ovh_size(hdr), ovh_etag_display(hdr), ovh_last_modified_sec(hdr)});
}

std::string encode_lov_cursors(std::string_view last_o, std::string_view last_v)
{
  const uint32_t ol = htobe32(static_cast<uint32_t>(last_o.size()));
  const uint32_t vl = htobe32(static_cast<uint32_t>(last_v.size()));
  std::string raw(sizeof(ol) + sizeof(vl) + last_o.size() + last_v.size(), '\0');
  size_t off = 0;
  std::memcpy(raw.data() + off, &ol, sizeof(ol)); off += sizeof(ol);
  if (!last_o.empty()) {
    std::memcpy(raw.data() + off, last_o.data(), last_o.size());
  }
  off += last_o.size();
  std::memcpy(raw.data() + off, &vl, sizeof(vl)); off += sizeof(vl);
  if (!last_v.empty()) {
    std::memcpy(raw.data() + off, last_v.data(), last_v.size());
  }
  return std::string(kLovTok) + base64_encode(raw);
}

bool decode_lov_cursors(std::string_view marker, std::string *last_o,
                        std::string *last_v)
{
  if (marker.size() < kLovTok.size() ||
      marker.substr(0, kLovTok.size()) != kLovTok) {
    return false;
  }
  auto dec = base64_decode(marker.substr(kLovTok.size()));
  uint32_t ol_be{}, vl_be{};
  size_t off = 0;
  if (!dec || dec->size() < off + sizeof(ol_be)) {
    return false;
  }
  std::memcpy(&ol_be, dec->data() + off, sizeof(ol_be)); off += sizeof(ol_be);
  const uint32_t ol = be32toh(ol_be);
  if (dec->size() < off + ol) {
    return false;
  }
  last_o->assign(dec->data() + off, ol); off += ol;
  if (dec->size() < off + sizeof(vl_be)) {
    return false;
  }
  std::memcpy(&vl_be, dec->data() + off, sizeof(vl_be)); off += sizeof(vl_be);
  const uint32_t vl = be32toh(vl_be);
  if (dec->size() != off + vl) {
    return false;
  }
  last_v->assign(dec->data() + off, vl);
  return true;
}

//--------------------------------------------------------------------------------
void merge_version_streams(const FdbRangeHolder &o_holder,
                           const FdbRangeHolder &v_holder,
                           std::string_view list_prefix, int limit,
                           std::string *last_o, std::string *last_v,
                           KvRgwServiceImpl::ListObjectVersionsResult *out)
{
  size_t new_cnt = static_cast<size_t>(o_holder.count() + v_holder.count());
  size_t rsv = std::min(out->versions.size() + new_cnt, static_cast<size_t>(limit));
  out->versions.reserve(rsv);

  int remaining = limit;
  auto oit = o_holder.begin();
  auto vit = v_holder.begin();

  // Track last key seen per stream as string_view — no alloc per row.
  // Copied to *last_o / *last_v once at the end.
  std::string_view last_o_sv;
  std::string_view last_v_sv;

  // current decoded row for each stream; nullptr = not yet loaded / exhausted
  std::optional<std::string_view>  o_name;
  const ObjectValueHeader*         o_hdr{nullptr};
  std::optional<std::string_view>  v_name;
  const ObjectValueHeader*         v_hdr{nullptr};

  auto load_o = [&]() -> bool {
    while (oit != o_holder.end()) {
      const auto [key, val] = *oit;
      o_name = parse_object_key_view(key);
      if (o_name) {
        const ObjectValueHeader* h = ovh_ptr(val);
        if (h && (list_prefix.empty() || starts_with(*o_name, list_prefix))) {
          o_hdr = h;
          return true;
        }
      }
      ++oit;
    }
    return false;
  };
  auto load_v = [&]() -> bool {
    while (vit != v_holder.end()) {
      const auto [key, val] = *vit;
      v_name = parse_v_key_view(key);
      if (v_name) {
        const ObjectValueHeader* h = ovh_ptr(val);
        if (h && (list_prefix.empty() || starts_with(*v_name, list_prefix))) {
          v_hdr = h;
          return true;
        }
      }
      ++vit;
    }
    return false;
  };

  // Initial load of both streams before entering the merge loop.
  bool o_ok = load_o();
  bool v_ok = load_v();

  while (remaining > 0 && (o_ok || v_ok)) {
    const bool take_o = o_ok && (!v_ok || *o_name <= *v_name);
    if (take_o) {
      last_o_sv = (*oit).key;
      const version_id_t vid = ovh_version_id(o_hdr);
      append_version_entry(out, *o_name, vid, true, o_hdr);
      out->next_version_id_marker = vid;
      ++oit;
      o_name.reset();
      o_hdr = nullptr;
      o_ok = load_o();
    }
    else {
      last_v_sv = (*vit).key;
      const version_id_t vid = ovh_version_id(v_hdr);
      append_version_entry(out, *v_name, vid, false, v_hdr);
      out->next_version_id_marker = vid;
      ++vit;
      v_name.reset();
      v_hdr = nullptr;
      v_ok = load_v();
    }
    --remaining;
  }

  const bool o_full = o_holder.count() == limit;
  const bool v_full = v_holder.count() == limit;
  const bool o_unread = oit != o_holder.end();
  const bool v_unread = vit != v_holder.end();
  const bool more = o_unread || v_unread || o_full || v_full;
  out->is_truncated = more && (remaining == 0 || o_full || v_full);

  // Single copy per stream — only the last key seen, deferred from the loop.
  if (!last_o_sv.empty()) { last_o->assign(last_o_sv); }
  if (!last_v_sv.empty()) { last_v->assign(last_v_sv); }
}

} // namespace

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::list_object_versions(
    tenant_id_t tenant_id, std::string_view bucket_name,
    std::string_view prefix, uint32_t max_keys, std::string_view key_marker,
    version_id_t version_id_marker, ListObjectVersionsResult *out)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kListObjectVersions);
  ops_stats_.inc(OpType::kListObjectVersions);
  *out = {};
  const std::string_view list_prefix = prefix;
  max_keys = std::min((max_keys > 0 ? max_keys : AWS_MaxKeys), AWS_MaxKeys);

  constexpr int kMaxRetries = 3;
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    *out = {};
    auto cached = get_bucket_id_cached(tenant_id, bucket_name);
    if (!cached) [[unlikely]] {
      auto ec = fdb_to_error(cached.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    if (!*cached) {
      return KVRGW_ERR_NO_SUCH_BUCKET;
    }
    if ((*cached)->access_flags & kDenyList) {
      return KVRGW_ERR_ACCESS_DENIED;
    }
    const auto bucket_id = (*cached)->bucket_id;

    std::string last_o;
    std::string last_v;
    if (!key_marker.empty() && !decode_lov_cursors(key_marker, &last_o, &last_v)) {
      KeyBuf o_key, v_key;
      make_object_key(bucket_id, key_marker, o_key);
      last_o = std::string(o_key.view());
      make_v_key(bucket_id, key_marker, version_id_marker, v_key);
      last_v = std::string(v_key.view());
    }

    std::string o_begin;
    if (!last_o.empty()) {
      o_begin = last_o;
    }
    else if (!list_prefix.empty()) {
      o_begin = std::string(make_object_key(bucket_id, list_prefix).view());
    }
    else {
      o_begin = std::string(make_object_prefix(bucket_id).view());
    }
    const std::string o_end = list_prefix.empty() ?
      prefix_range_end(make_object_prefix(bucket_id).view()) :
      prefix_range_end(make_object_key(bucket_id, list_prefix).view());

    KeyBuf v_dom = make_version_prefix(bucket_id);
    if (!list_prefix.empty()) {
      v_dom.append(list_prefix.data(), list_prefix.size());
    }
    const std::string_view v_domain = v_dom.view();
    const std::string v_end = prefix_range_end(v_domain);
    const std::string_view v_begin = last_v.empty() ? v_domain : last_v;

    auto tr_result = store_.begin_transaction();
    if (!tr_result) [[unlikely]] {
      auto ec = fdb_to_error(tr_result.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;
    if (listing_disable_ryw_) {
      // best effort, don't fail on this
      tr->disable_ryw();
    }

    auto f_bkt = issue_bucket_get(*tr, tenant_id, bucket_name);
    const Stopwatch scan_t0;
    FdbRangeHolder o_holder(tr->kv_async_get_range_holder(
        o_begin, !last_o.empty(), o_end, max_keys, streamingMode));
    FdbRangeHolder v_holder(tr->kv_async_get_range_holder(
        v_begin, !last_v.empty(), v_end, max_keys, streamingMode));

    const fdb_error_t o_err = o_holder.wait();
    const auto o_us = scan_t0.elapsed_us();
    if (o_err) [[unlikely]] {
      auto ec = fdb_to_error(o_err);
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }

    const fdb_error_t v_err = v_holder.wait();
    const auto v_us = scan_t0.elapsed_us();
    if (v_err) [[unlikely]] {
      auto ec = fdb_to_error(v_err);
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }

    merge_version_streams(o_holder, v_holder, list_prefix, max_keys, &last_o,
                          &last_v, out);

    auto vb = resolve_bucket_verify(tenant_id, bucket_name, f_bkt, bucket_id,
                                    kDenyList);
    if (!vb) [[unlikely]] {
      const auto ec = vb.error();
      if (ec == KVRGW_ERR_BUCKET_ID_MISMATCH) {
        // bucket was overwritten -> restart scan clean
        *out = {};
        continue;
      }
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      *out = {};
      return ec;
    }

    if (out->is_truncated) {
      out->next_key_marker = encode_lov_cursors(last_o, last_v);
    }
    else {
      out->next_key_marker.clear();
      out->next_version_id_marker = version_id_t{};
    }
    latency_stats_.record_list_scan(o_us + v_us);
    // we issued 2 range scans (:O: + :V:)
    latency_stats_.record_list_call(1 + 1);
    return KVRGW_ERR_OK;
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::delete_object(tenant_id_t tenant_id,
                                               std::string_view bucket_name,
                                               std::string_view object_name,
                                               const AWSDeleteCondition *aws_cond,
                                               DeleteResult *out)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kDeleteObject);
  ops_stats_.inc(OpType::kDeleteObject);
  const std::string bname(bucket_name);

  GetCondition bin_cond{};
  const GetCondition *cond_ptr = nullptr;
  if (aws_cond) {
    auto ec = bin_cond.encode(aws_cond->if_match,
                              static_cast<uint32_t>(aws_cond->if_match_last_modified_time),
                              static_cast<uint64_t>(aws_cond->if_match_size),
                              aws_cond->has_if_match_size);
    if (ec != KVRGW_ERR_OK) {
      return ec;
    }
    cond_ptr = &bin_cond;
  }

  constexpr int kMaxRetries = 3;
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    auto tr_result = store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;

    auto result = delete_single(*tr, tenant_id, bname, object_name, cond_ptr);
    if (!result) {
      return result.error();
    }

    fdb_error_t err = tr->commit();
    if (!err) {
      if (out) {
        *out = *result;
      }
      return KVRGW_ERR_OK;
    }
    auto ec = fdb_to_error(err);
    if (!is_retriable(ec)) {
      return ec;
    }
    sleep_for_msec(10 * (attempt + 1));
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::put_object_tagging(tenant_id_t tenant_id,
                                                    std::string_view bucket_name,
                                                    std::string_view key,
                                                    std::span<const uint8_t> tags)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kPutObjectTagging);
  ops_stats_.inc(OpType::kPutObjectTagging);
  auto cached = get_bucket_id_cached(tenant_id, bucket_name);
  if (!cached) {
    return fdb_to_error(cached.error());
  }
  if (!*cached) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  if (auto ac = check_access(tenant_id, bucket_name, **cached, kDenyWrite);
      ac != KVRGW_ERR_OK) {
    return ac;
  }
  const auto bucket_id = (**cached).bucket_id;
  KeyBuf object_key;
  make_object_key(bucket_id, key, object_key);

  constexpr int kMaxRetries = 3;
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    auto tr_result = store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;

    auto f_bkt = issue_bucket_get(*tr, tenant_id, bucket_name);
    auto f_obj = tr->kv_async_get(object_key.view());

    auto vb = resolve_bucket_verify(tenant_id, bucket_name, f_bkt, bucket_id,
                                    kDenyWrite);
    if (!vb) {
      return vb.error();
    }

    auto existing = tr->kv_wait_get(f_obj);
    if (!existing) {
      auto ec = fdb_to_error(existing.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    if (!*existing) {
      return KVRGW_ERR_NO_SUCH_KEY;
    }
    auto value = parse_object_value(**existing);
    if (!value) {
      return KVRGW_ERR_INTERNAL;
    }
    if (value->is_delete_marker()) {
      return KVRGW_ERR_NO_SUCH_KEY;
    }

    if (tags.empty()) {
      clear_object_tags(*value, *tr, bucket_id, value->hdr.ref_tag.view());
    }
    else {
      apply_tags_to_value(*value, tags, *tr, bucket_id, value->hdr.ref_tag.view());
    }

    OValueBuf buf;
    if (!write_object_value(buf, *value)) {
      return KVRGW_ERR_INTERNAL;
    }
    tr->kv_put(object_key.view(), buf.view());

    fdb_error_t err = tr->commit();
    if (!err) {
      return KVRGW_ERR_OK;
    }
    {
      auto ec = fdb_to_error(err);
      if (!is_retriable(ec)) {
        return ec;
      }
    }
    sleep_for_msec(10 * (attempt + 1));
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::get_object_tagging(
    tenant_id_t tenant_id, std::string_view bucket_name, std::string_view key,
    std::vector<uint8_t> &live, std::array<TagPair, MAX_TAG_COUNT> &out_tags,
    size_t *out_count)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kGetObjectTagging);
  ops_stats_.inc(OpType::kGetObjectTagging);
  if (out_count == nullptr) {
    return KVRGW_ERR_INVALID_ARGUMENT;
  }
  *out_count = 0;
  auto cached = get_bucket_id_cached(tenant_id, bucket_name);
  if (!cached) {
    return fdb_to_error(cached.error());
  }
  if (!*cached) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  if (auto ac = check_access(tenant_id, bucket_name, **cached, kDenyRead);
      ac != KVRGW_ERR_OK) {
    return ac;
  }
  const auto bucket_id = (**cached).bucket_id;
  KeyBuf object_key;
  make_object_key(bucket_id, key, object_key);

  auto tr_result = store_.begin_transaction();
  if (!tr_result) {
    return fdb_to_error(tr_result.error());
  }
  auto &tr = *tr_result;

  auto existing = tr->kv_get(object_key.view());
  if (!existing) {
    return fdb_to_error(existing.error());
  }
  if (!*existing) {
    return KVRGW_ERR_NO_SUCH_KEY;
  }
  auto value = parse_object_value(**existing);
  if (!value) {
    return KVRGW_ERR_INTERNAL;
  }
  if (value->is_delete_marker()) {
    return KVRGW_ERR_NO_SUCH_KEY;
  }

  if (value->hdr.tags_count == 0) {
    return KVRGW_ERR_OK;
  }

  KeyBuf ct_key;
  make_ct_key(bucket_id, value->hdr.ref_tag.view(), ct_key);
  auto ct_val = tr->kv_get(ct_key.view());
  if (!ct_val) {
    return fdb_to_error(ct_val.error());
  }
  if (!*ct_val) {
    return KVRGW_ERR_CORRUPT_VALUE;
  }
  const auto payload = child_value_payload(**ct_val);
  size_t frame_size = 0;
  if (!encoded_tag_frame_size(
          std::span<const uint8_t>(
              reinterpret_cast<const uint8_t *>(payload.data()),
              payload.size()),
          frame_size)) {
    return KVRGW_ERR_CORRUPT_VALUE;
  }
  live.assign(reinterpret_cast<const uint8_t *>(payload.data()),
              reinterpret_cast<const uint8_t *>(payload.data()) + frame_size);
  if (!decode_tags(std::span<const uint8_t>(live.data(), live.size()), out_tags)) {
    return KVRGW_ERR_CORRUPT_VALUE;
  }
  *out_count = value->hdr.tags_count;
  return KVRGW_ERR_OK;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::delete_object_tagging(
    tenant_id_t tenant_id, std::string_view bucket_name, std::string_view key)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kDeleteObjectTagging);
  ops_stats_.inc(OpType::kDeleteObjectTagging);
  auto cached = get_bucket_id_cached(tenant_id, bucket_name);
  if (!cached) {
    return fdb_to_error(cached.error());
  }
  if (!*cached) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  if (auto ac = check_access(tenant_id, bucket_name, **cached, kDenyWrite);
      ac != KVRGW_ERR_OK) {
    return ac;
  }
  const auto bucket_id = (**cached).bucket_id;
  KeyBuf object_key;
  make_object_key(bucket_id, key, object_key);

  constexpr int kMaxRetries = 3;
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    auto tr_result = store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    auto &tr = *tr_result;

    auto f_bkt = issue_bucket_get(*tr, tenant_id, bucket_name);
    auto f_obj = tr->kv_async_get(object_key.view());

    auto vb = resolve_bucket_verify(tenant_id, bucket_name, f_bkt, bucket_id,
                                    kDenyWrite);
    if (!vb) {
      return vb.error();
    }

    auto existing = tr->kv_wait_get(f_obj);
    if (!existing) {
      auto ec = fdb_to_error(existing.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    if (!*existing) {
      return KVRGW_ERR_NO_SUCH_KEY;
    }
    auto value = parse_object_value(**existing);
    if (!value) {
      return KVRGW_ERR_INTERNAL;
    }
    if (value->is_delete_marker()) {
      return KVRGW_ERR_NO_SUCH_KEY;
    }

    clear_object_tags(*value, *tr, bucket_id, value->hdr.ref_tag.view());
    OValueBuf buf;
    if (!write_object_value(buf, *value)) {
      return KVRGW_ERR_INTERNAL;
    }
    tr->kv_put(object_key.view(), buf.view());

    fdb_error_t err = tr->commit();
    if (!err) {
      return KVRGW_ERR_OK;
    }
    {
      auto ec = fdb_to_error(err);
      if (!is_retriable(ec)) {
        return ec;
      }
    }
    sleep_for_msec(10 * (attempt + 1));
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

} // namespace kvrgw
