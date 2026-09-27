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

namespace kvrgw {
constexpr FDBStreamingMode streamingMode = FDB_STREAMING_MODE_EXACT;
namespace {

int64_t now_unix()
{
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::string prefix_range_end(std::string_view prefix)
{
  std::string end(prefix);
  while (!end.empty()) {
    const unsigned char last = static_cast<unsigned char>(end.back());
    if (last < 0xFF) {
      end.back() = static_cast<char>(last + 1);
      return end;
    }
    end.pop_back();
  }
  return std::string("\xFF", 1);
}

std::string base64_encode(std::string_view data)
{
  const int encoded_len = 4 * ((static_cast<int>(data.size()) + 2) / 3);
  std::string out;
  out.resize(static_cast<size_t>(encoded_len));
  const int len =
      EVP_EncodeBlock(reinterpret_cast<unsigned char *>(out.data()),
                      reinterpret_cast<const unsigned char *>(data.data()),
                      static_cast<int>(data.size()));
  out.resize(static_cast<size_t>(len));
  return out;
}

std::optional<std::string> base64_decode(std::string_view data)
{
  std::string out;
  out.resize((data.size() * 3) / 4 + 4);
  const int len =
      EVP_DecodeBlock(reinterpret_cast<unsigned char *>(out.data()),
                      reinterpret_cast<const unsigned char *>(data.data()),
                      static_cast<int>(data.size()));
  if (len < 0) {
    return std::nullopt;
  }
  out.resize(static_cast<size_t>(len));
  return out;
}

bool starts_with(std::string_view value, std::string_view prefix)
{
  return value.size() >= prefix.size() &&
         value.substr(0, prefix.size()) == prefix;
}

std::optional<uint64_t> read_counter_le(const char *data, size_t len)
{
  if (len != sizeof(uint64_t)) {
    return std::nullopt;
  }
  uint64_t val = 0;
  std::memcpy(&val, data, sizeof(val));
  return val;
}

void write_counter_le(uint64_t val, KvTransaction &tr, std::string_view key)
{
  std::string buf(sizeof(uint64_t), '\0');
  std::memcpy(buf.data(), &val, sizeof(val));
  tr.kv_put(key, buf);
}

bool is_valid_bucket_name(std::string_view name)
{
  if (name.size() < 3 || name.size() > 63) {
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

bool write_object_value(OValueBuf &buf, const ObjectValue &value)
{
  ObjectValueHeader wire = value.hdr;
  wire.content_type_len = static_cast<uint8_t>(value.content_type.size());
  hdr_to_be(wire);
  if (!buf.set_header(wire)) {
    return false;
  }
  if (!buf.append(value.content_type.data(), value.content_type.size())) {
    return false;
  }
  if (value.hdr.chunk.type == CHUNK_INLINE && !value.inline_data.empty()) {
    if (!buf.append(value.inline_data.data(), value.inline_data.size())) {
      return false;
    }
  }
  if (value.hdr.chunk.type == CHUNK_CHILD_D_REF) {
    uint8_t bid_be[sizeof(bucket_id_t)];
    value.chunk_data_bucket_id.serialize(bid_be);
    if (!buf.append(bid_be, sizeof(bid_be))) {
      return false;
    }
    if (!buf.append(value.chunk_data_ref_tag, kRefTagSize)) {
      return false;
    }
  }
  else if (value.hdr.chunk.type == CHUNK_STORAGE_REF) {
    if (!buf.append(value.chunk_data_ref_tag, kRefTagSize)) {
      return false;
    }
  }
  if (value.hdr.metadata_count > 0) {
    if (value.metadata_frame.empty()) {
      return false;
    }
    if (!buf.append(value.metadata_frame.data(), value.metadata_frame.size())) {
      return false;
    }
  }
  return true;
}

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
        auto rc = KvTransaction::resolve_commit(ib.commit_future);
        if (!rc) {
          auto ec = fdb_to_error(rc.error());
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
              ref_tag_view(ib.entries[j].ref_tag), ib.entries[j].data);
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
        auto rc = KvTransaction::resolve_commit(ib.commit_future);
        if (rc) {
          for (int j = 0; j < ib.entry_count; ++j) {
            ib.entries[j].promise.set_value(BatchPutResult{
                KVRGW_ERR_OK, ib.entries[j].value.hdr.version_id});
          }
          ib.reset();
        }
        else {
          auto ec = fdb_to_error(rc.error());
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
              auto remaining =
                  std::chrono::duration_cast<std::chrono::microseconds>(
                      timeout - age);
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

void BatchCommitQueue::start_batch(InFlightBatch &ib)
{
  ib.storage_entry_count = 0;
  ib.group_bucket_id = kNullBucket;

  for (int i = 0; i < ib.entry_count; ++i) {
    auto &e = ib.entries[i];
    if (e.chunk_type == CHUNK_STORAGE) {
      if (ib.group_bucket_id == kNullBucket) {
        auto cached_bid =
            service_.get_bucket_id_cached(e.tenant_id, e.bucket_name);
        if (!cached_bid || !*cached_bid) {
          auto ec = cached_bid ? KVRGW_ERR_NO_SUCH_BUCKET
                               : fdb_to_error(cached_bid.error());
          for (int j = 0; j < ib.entry_count; ++j) {
            ib.entries[j].promise.set_value(ec);
          }
          ib.reset();
          return;
        }
        service_.put_bucket_cache(e.tenant_id, e.bucket_name,
                                  (**cached_bid).bucket_id);
        ib.group_bucket_id = (**cached_bid).bucket_id;
      }
      ib.storage_entries[ib.storage_entry_count].ref_tag = e.ref_tag;
      ib.storage_entries[ib.storage_entry_count].object_size = e.value.hdr.size;
      ib.storage_entry_count++;
    }
  }

  auto now = std::chrono::steady_clock::now();
  int64_t bs = static_cast<int64_t>(ib.entry_count);
  stats_.batch_commits.fetch_add(1, std::memory_order_relaxed);
  stats_.total_entries_batched.fetch_add(bs, std::memory_order_relaxed);
  auto cur_min = stats_.min_batch_size.load(std::memory_order_relaxed);
  while ((cur_min == 0 || bs < cur_min) &&
         !stats_.min_batch_size.compare_exchange_weak(cur_min, bs)) {
  }
  auto cur_max = stats_.max_batch_size.load(std::memory_order_relaxed);
  while (bs > cur_max &&
         !stats_.max_batch_size.compare_exchange_weak(cur_max, bs)) {
  }
  auto oldest = ib.entries[0].enqueued_at;
  for (int i = 1; i < ib.entry_count; ++i) {
    if (ib.entries[i].enqueued_at < oldest) {
      oldest = ib.entries[i].enqueued_at;
    }
  }
  int64_t wait_us = std::chrono::duration_cast<std::chrono::microseconds>(now - oldest).count();
  stats_.total_wait_us.fetch_add(wait_us, std::memory_order_relaxed);
  auto wmin = stats_.min_wait_us.load(std::memory_order_relaxed);
  while ((wmin == 0 || wait_us < wmin) &&
         !stats_.min_wait_us.compare_exchange_weak(wmin, wait_us)) {
  }
  auto wmax = stats_.max_wait_us.load(std::memory_order_relaxed);
  while (wait_us > wmax &&
         !stats_.max_wait_us.compare_exchange_weak(wmax, wait_us)) {
  }

  if (ib.storage_entry_count > 0) {
    auto tr_result = service_.store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      service_.error_stats_->record(ec);
      for (int j = 0; j < ib.entry_count; ++j) {
        ib.entries[j].promise.set_value(ec);
      }
      ib.reset();
      return;
    }
    auto group_ref_view = ref_tag_view(ib.storage_entries[0].ref_tag);
    KeyBuf gpo_key;
    make_group_po_key(ib.group_bucket_id, group_ref_view, gpo_key);
    auto gpo_val =
        make_group_po_value(ib.storage_entries, ib.storage_entry_count,
                            static_cast<uint32_t>(now_unix()));
    (*tr_result)->kv_put(gpo_key.view(), gpo_val);
    ib.txn = std::move(*tr_result);
    ib.commit_future = ib.txn->commit_async();
    ib.phase = InFlightBatch::PHASE_1;
    ib.step = InFlightBatch::STEP_COMMIT_ISSUED;
  }
  else {
    ib.phase = InFlightBatch::PHASE_3;
    ib.step = InFlightBatch::STEP_WORKING;
  }
}

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
  bucket_id_t bucket_ids[kMaxBatchSize]{};

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
    auto cached_bid =
        service_.get_bucket_id_cached(entry.tenant_id, entry.bucket_name);
    if (!cached_bid) {
      any_failed = true;
      fail_ec = fdb_to_error(cached_bid.error());
      break;
    }
    if (!*cached_bid) {
      any_failed = true;
      fail_ec = KVRGW_ERR_NO_SUCH_BUCKET;
      break;
    }
    service_.put_bucket_cache(entry.tenant_id, entry.bucket_name,
                              (**cached_bid).bucket_id);
    bucket_ids[i] = (**cached_bid).bucket_id;

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

    KvRgwServiceImpl::PutInTxnParams params{
        entry.tenant_id,
        entry.bucket_name,
        bucket_ids[i],
        entry.object_name,
        entry.ref_tag,
        entry.value,
        &entry.data,
        nullptr,
        std::span<const uint8_t>(entry.tag_encoded),
        !need_bucket,
        false,
        std::span<const uint8_t>(entry.metadata_encoded.data(),
                                 entry.metadata_encoded_size)};
    ctxs[i] = service_.put_prepare(*tr, params, need_bucket);
    ctxs[i].is_storage_tier = entry.chunk_type == CHUNK_STORAGE;
  }

  FdbFuture f_group_po;
  if (ib.storage_entry_count > 0 && !any_failed) {
    auto group_ref_view = ref_tag_view(ib.storage_entries[0].ref_tag);
    KeyBuf gpo_key;
    make_group_po_key(ib.group_bucket_id, group_ref_view, gpo_key);
    f_group_po = tr->kv_async_get(gpo_key.view());
  }

  if (!any_failed) {
    for (int i = 0; i < ib.entry_count; ++i) {
      auto &entry = ib.entries[i];
      KvRgwServiceImpl::PutCondition cond_obj;
      const KvRgwServiceImpl::PutCondition *cond_ptr = nullptr;
      if (!entry.if_match.empty() || !entry.if_none_match.empty()) {
        cond_obj.if_match = entry.if_match;
        cond_obj.if_none_match = entry.if_none_match;
        cond_ptr = &cond_obj;
      }
      KvRgwServiceImpl::PutInTxnParams params{
          entry.tenant_id,
          entry.bucket_name,
          bucket_ids[i],
          entry.object_name,
          entry.ref_tag,
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

  if (ib.storage_entry_count > 0 && !any_failed) {
    auto gpo_val = tr->kv_wait_get(f_group_po);
    if (!gpo_val) {
      any_failed = true;
      fail_ec = fdb_to_error(gpo_val.error());
    }
    else if (!*gpo_val) {
      any_failed = true;
      fail_ec = KVRGW_ERR_INTERNAL;
    }
    else {
      auto group_ref_view = ref_tag_view(ib.storage_entries[0].ref_tag);
      KeyBuf gpo_key;
      make_group_po_key(ib.group_bucket_id, group_ref_view, gpo_key);
      tr->kv_del(gpo_key.view());
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

void BatchCommitQueue::commit_batch(std::vector<BatchCommitEntry> &batch)
{
  constexpr int kMaxRetries = 3;

  auto now = std::chrono::steady_clock::now();
  int64_t bs = static_cast<int64_t>(batch.size());
  stats_.batch_commits.fetch_add(1, std::memory_order_relaxed);
  stats_.total_entries_batched.fetch_add(bs, std::memory_order_relaxed);

  auto cur_min = stats_.min_batch_size.load(std::memory_order_relaxed);
  while ((cur_min == 0 || bs < cur_min) &&
         !stats_.min_batch_size.compare_exchange_weak(cur_min, bs)) {
  }
  auto cur_max = stats_.max_batch_size.load(std::memory_order_relaxed);
  while (bs > cur_max &&
         !stats_.max_batch_size.compare_exchange_weak(cur_max, bs)) {
  }

  auto oldest = batch[0].enqueued_at;
  for (const auto &e : batch) {
    if (e.enqueued_at < oldest) {
      oldest = e.enqueued_at;
    }
  }
  int64_t wait_us = std::chrono::duration_cast<std::chrono::microseconds>(now - oldest).count();
  stats_.total_wait_us.fetch_add(wait_us, std::memory_order_relaxed);
  auto wmin = stats_.min_wait_us.load(std::memory_order_relaxed);
  while ((wmin == 0 || wait_us < wmin) &&
         !stats_.min_wait_us.compare_exchange_weak(wmin, wait_us)) {
  }
  auto wmax = stats_.max_wait_us.load(std::memory_order_relaxed);
  while (wait_us > wmax &&
         !stats_.max_wait_us.compare_exchange_weak(wmax, wait_us)) {
  }

  // Phase 1 & 2: Handle Class B (storage tier) entries
  bool has_storage_tier = false;
  bucket_id_t group_bucket_id = kNullBucket;
  std::string_view group_ref_tag;
  GroupPoEntry storage_entries[kMaxBatchSize];
  int storage_entry_count = 0;

  for (auto &e : batch) {
    if (e.chunk_type == CHUNK_STORAGE) {
      has_storage_tier = true;
      if (group_bucket_id == kNullBucket) {
        auto cached_bid =
            service_.get_bucket_id_cached(e.tenant_id, e.bucket_name);
        if (!cached_bid) {
          auto ec = fdb_to_error(cached_bid.error());
          service_.error_stats_->record(ec);
          for (auto &be : batch) {
            be.promise.set_value(ec);
          }
          return;
        }
        if (!*cached_bid) {
          for (auto &be : batch) {
            be.promise.set_value(KVRGW_ERR_NO_SUCH_BUCKET);
          }
          return;
        }
        service_.put_bucket_cache(e.tenant_id, e.bucket_name,
                                  (**cached_bid).bucket_id);
        group_bucket_id = (**cached_bid).bucket_id;
        group_ref_tag = ref_tag_view(e.ref_tag);
      }
      storage_entries[storage_entry_count].ref_tag = e.ref_tag;
      storage_entries[storage_entry_count].object_size = e.value.hdr.size;
      storage_entry_count++;
    }
  }

  if (has_storage_tier) {
    KeyBuf gpo_key;
    make_group_po_key(group_bucket_id, group_ref_tag, gpo_key);
    auto gpo_val = make_group_po_value(storage_entries, storage_entry_count,
                                       static_cast<uint32_t>(now_unix()));
    auto set_rc = service_.store_.set(gpo_key.view(), gpo_val);
    if (!set_rc) {
      auto ec = fdb_to_error(set_rc.error());
      service_.error_stats_->record(ec);
      for (auto &e : batch) {
        e.promise.set_value(ec);
      }
      return;
    }

    for (auto &e : batch) {
      if (e.chunk_type != CHUNK_STORAGE) {
        continue;
      }
      auto write_ec =
          service_.data_store_.write(ref_tag_view(e.ref_tag), e.data);
      if (write_ec) {
        service_.error_stats_->record(KVRGW_ERR_INTERNAL);
        for (auto &be : batch) {
          be.promise.set_value(KVRGW_ERR_INTERNAL);
        }
        return;
      }
    }
  }

  // Fault injection: abort after Phase 2, leaving stale group P:O + orphan
  // blobs
  if (service_.err_insertion_.is_error_active(
          FaultType::kAbortAfterBatchPhase2)) {
    for (auto &e : batch) {
      e.promise.set_value(KVRGW_ERR_INTERNAL);
    }
    return;
  }

  // Phase 3: Metadata commit (all entries)
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    auto tr_result = service_.store_.begin_transaction();
    if (!tr_result) {
      auto ec = fdb_to_error(tr_result.error());
      service_.error_stats_->record(ec);
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      for (auto &e : batch) {
        e.promise.set_value(ec);
      }
      return;
    }
    auto &tr = *tr_result;

    KvRgwServiceImpl::VerifiedBucket verified[kMaxBatchSize];
    int verified_count = 0;
    KvRgwServiceImpl::PutContext ctxs[kMaxBatchSize];
    bucket_id_t bucket_ids[kMaxBatchSize]{};

    struct IssuedBkt {
      tenant_id_t tid;
      const std::string *name;
    };
    IssuedBkt issued[kMaxBatchSize];
    int issued_count = 0;

    bool any_failed = false;
    KvrgwErrorCode fail_ec = KVRGW_ERR_OK;

    for (size_t i = 0; i < batch.size(); ++i) {
      auto &entry = batch[i];
      auto cached_bid =
          service_.get_bucket_id_cached(entry.tenant_id, entry.bucket_name);
      if (!cached_bid) {
        any_failed = true;
        fail_ec = fdb_to_error(cached_bid.error());
        break;
      }
      if (!*cached_bid) {
        any_failed = true;
        fail_ec = KVRGW_ERR_NO_SUCH_BUCKET;
        break;
      }
      service_.put_bucket_cache(entry.tenant_id, entry.bucket_name,
                                (**cached_bid).bucket_id);
      bucket_ids[i] = (**cached_bid).bucket_id;

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

      KvRgwServiceImpl::PutInTxnParams params{
          entry.tenant_id,
          entry.bucket_name,
          bucket_ids[i],
          entry.object_name,
          entry.ref_tag,
          entry.value,
          &entry.data,
          nullptr,
          std::span<const uint8_t>(entry.tag_encoded),
          !need_bucket,
          false,
          std::span<const uint8_t>(entry.metadata_encoded.data(),
                                   entry.metadata_encoded_size)};
      ctxs[i] = service_.put_prepare(*tr, params, need_bucket);
      ctxs[i].is_storage_tier = entry.chunk_type == CHUNK_STORAGE;
    }

    FdbFuture f_group_po;
    if (has_storage_tier && !any_failed) {
      KeyBuf gpo_key;
      make_group_po_key(group_bucket_id, group_ref_tag, gpo_key);
      f_group_po = tr->kv_async_get(gpo_key.view());
    }

    if (!any_failed) {
      for (size_t i = 0; i < batch.size(); ++i) {
        auto &entry = batch[i];

        KvRgwServiceImpl::PutCondition cond_obj;
        const KvRgwServiceImpl::PutCondition *cond_ptr = nullptr;
        if (!entry.if_match.empty() || !entry.if_none_match.empty()) {
          cond_obj.if_match = entry.if_match;
          cond_obj.if_none_match = entry.if_none_match;
          cond_ptr = &cond_obj;
        }

        KvRgwServiceImpl::PutInTxnParams params{
            entry.tenant_id,
            entry.bucket_name,
            bucket_ids[i],
            entry.object_name,
            entry.ref_tag,
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

    if (has_storage_tier && !any_failed) {
      auto gpo_val = tr->kv_wait_get(f_group_po);
      if (!gpo_val) {
        any_failed = true;
        fail_ec = fdb_to_error(gpo_val.error());
      }
      else if (!*gpo_val) {
        any_failed = true;
        fail_ec = KVRGW_ERR_INTERNAL;
      }
      else {
        KeyBuf gpo_key;
        make_group_po_key(group_bucket_id, group_ref_tag, gpo_key);
        tr->kv_del(gpo_key.view());
      }
    }

    if (any_failed) {
      service_.error_stats_->record(fail_ec);
      if (attempt < kMaxRetries - 1 && fail_ec == KVRGW_ERR_INTERNAL) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      for (auto &e : batch) {
        e.promise.set_value(fail_ec);
      }
      return;
    }

    auto rc = tr->commit();
    if (rc) {
      for (auto &e : batch) {
        e.promise.set_value(
            BatchPutResult{KVRGW_ERR_OK, e.value.hdr.version_id});
      }
      return;
    }
    auto commit_ec = fdb_to_error(rc.error());
    service_.error_stats_->record(commit_ec);
    if (!is_retriable(commit_ec)) {
      service_.latency_stats_.txn_hard_failures.fetch_add(
          1, std::memory_order_relaxed);
      for (auto &e : batch) {
        e.promise.set_value(commit_ec);
      }
      return;
    }
    service_.latency_stats_.txn_retries.fetch_add(1, std::memory_order_relaxed);
    sleep_for_msec(10 * (attempt + 1));
  }

  service_.latency_stats_.txn_max_retries_exceeded.fetch_add(
      1, std::memory_order_relaxed);
  for (auto &e : batch) {
    e.promise.set_value(KVRGW_ERR_MAX_RETRIES_EXCEEDED);
  }
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
                                        const std::string &bucket_name,
                                        bucket_id_t bucket_id,
                                        uint8_t access_flags)
{
  std::lock_guard lock(cache_mu_);
  bucket_cache_[BucketCacheKey{tenant_id, bucket_name}] =
      BucketCacheEntry{bucket_id, access_flags, VERSIONING_DISABLED,
                       std::chrono::steady_clock::now()};
}

//--------------------------------------------------------------------------------
void KvRgwServiceImpl::invalidate_bucket_cache(tenant_id_t tenant_id,
                                               const std::string &bucket_name)
{
  std::lock_guard lock(cache_mu_);
  bucket_cache_.erase(BucketCacheKey{tenant_id, bucket_name});
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::check_access(tenant_id_t tenant_id,
                                              const std::string &bucket_name,
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
                                                const std::string &bucket_name)
{
  KeyBuf bucket_key;
  make_bucket_key(tenant_id, bucket_name, bucket_key);
  return tr.kv_async_get_holder(bucket_key.view());
}

//--------------------------------------------------------------------------------
std::expected<KvRgwServiceImpl::BucketState, KvrgwErrorCode>
KvRgwServiceImpl::resolve_bucket_verify(tenant_id_t tenant_id,
                                        const std::string &bucket_name,
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
  if (bucket_id != expected_bucket_id) [[unlikely]] {
    invalidate_bucket_cache(tenant_id, bucket_name);
    return std::unexpected(KVRGW_ERR_BUCKET_ID_MISMATCH);
  }
  const auto access_flags = bvh_access_flags(bvh);
  if (deny_mask != 0 && (access_flags & deny_mask)) [[unlikely]] {
    return std::unexpected(KVRGW_ERR_ACCESS_DENIED);
  }
  return BucketState{bucket_id, access_flags, bvh_versioning_state(bvh),
                     bvh_created_at_unix(bvh)};
}

//--------------------------------------------------------------------------------
std::expected<KvRgwServiceImpl::BucketState, KvrgwErrorCode>
KvRgwServiceImpl::verify_bucket_in_txn(KvTransaction &tr, tenant_id_t tenant_id,
                                       const std::string &bucket_name,
                                       bucket_id_t expected_bucket_id,
                                       uint8_t deny_mask)
{
  // TBD: consider removing this function!!!
  auto holder = issue_bucket_get(tr, tenant_id, bucket_name);
  return resolve_bucket_verify(tenant_id, bucket_name, holder,
                               expected_bucket_id, deny_mask);
}

//--------------------------------------------------------------------------------
std::expected<std::optional<KvRgwServiceImpl::BucketState>, fdb_error_t>
KvRgwServiceImpl::get_bucket_id_cached(tenant_id_t tenant_id,
                                       const std::string &bucket_name)
{
  {
    std::lock_guard lock(cache_mu_);
    const auto it = bucket_cache_.find(BucketCacheKey{tenant_id, bucket_name});
    if (it != bucket_cache_.end()) {
      const auto age = std::chrono::steady_clock::now() - it->second.cached_at;
      if (age <= kBucketCacheTtl) {
        return std::optional<BucketState>(
            BucketState{it->second.bucket_id, it->second.access_flags,
                        it->second.versioning_state, 0});
      }
    }
  }
  return read_bucket_state(tenant_id, bucket_name);
}

//--------------------------------------------------------------------------------
std::expected<std::optional<KvRgwServiceImpl::BucketState>, fdb_error_t>
KvRgwServiceImpl::read_bucket_state(tenant_id_t tenant_id,
                                    const std::string &bucket_name)
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
  put_bucket_cache(tenant_id, bucket_name, state.bucket_id, state.access_flags);
  return std::optional<BucketState>(state);
}

//--------------------------------------------------------------------------------
void KvRgwServiceImpl::put_tenant_cache(std::string_view tenant_name,
                                        tenant_id_t tenant_id)
{
  std::lock_guard lock(cache_mu_);
  tenant_cache_.insert_or_assign(std::string(tenant_name), tenant_id);
}

//--------------------------------------------------------------------------------
void KvRgwServiceImpl::invalidate_tenant_cache(const std::string &tenant_name)
{
  std::lock_guard lock(cache_mu_);
  tenant_cache_.erase(tenant_name);
}

//--------------------------------------------------------------------------------
std::expected<std::optional<tenant_id_t>, fdb_error_t>
KvRgwServiceImpl::resolve_tenant_id(std::string_view tenant_name)
{
  {
    std::lock_guard lock(cache_mu_);
    const auto it = tenant_cache_.find(std::string(tenant_name));
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

//--------------------------------------------------------------------------------
KvrgwErrorCode
KvRgwServiceImpl::tenant_id_for_name(const std::string &tenant_name,
                                     tenant_id_t *tenant_id)
{
  auto resolved = resolve_tenant_id(tenant_name);
  if (!resolved) {
    return fdb_to_error(resolved.error());
  }
  if (*resolved) {
    *tenant_id = **resolved;
    return KVRGW_ERR_OK;
  }

  tenant_id_t created = 0;
  const auto ec = add_tenant(tenant_name, &created);
  if (ec == KVRGW_ERR_OK) {
    *tenant_id = created;
    return KVRGW_ERR_OK;
  }
  if (ec == KVRGW_ERR_BUCKET_ALREADY_EXISTS) {
    resolved = resolve_tenant_id(tenant_name);
    if (!resolved) {
      return fdb_to_error(resolved.error());
    }
    if (*resolved) {
      *tenant_id = **resolved;
      return KVRGW_ERR_OK;
    }
  }
  return ec;
}

//---------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::add_tenant(std::string_view tenant_name,
                                            tenant_id_t *out_id)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kOther);
  constexpr int kMaxRetries = 3;
  if (tenant_name.empty()) {
    return KVRGW_ERR_INVALID_ARGUMENT;
  }

  // generate key to the tenant entry
  const auto tenant_key = make_tenant_key(tenant_name);

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

    // block on tenant-entry KV
    auto existing = tr->kv_get(tenant_key.view());
    if (!existing) {
      // failure
      auto ec = fdb_to_error(existing.error());
      if (is_retriable(ec)) {
        // A retriable FDB error poisons that txn -> restart a fresh txn
        continue;
      }
      return ec;
    }

    if (*existing) {
      // tenant-already exist -> abort
      return KVRGW_ERR_BUCKET_ALREADY_EXISTS;
    }

    // generate key to the tenant-assigned-counter
    const auto counter_key =
        make_l_key(kLocalTypeNumeric, kLocalCounterTenantId);
    auto counter_val = tr->kv_get(counter_key.view());
    if (!counter_val) {
      // failure
      auto ec = fdb_to_error(counter_val.error());
      if (is_retriable(ec)) {
        // A retriable FDB error poisons that txn -> restart a fresh txn
        continue;
      }
      return ec;
    }

    uint64_t tenant_num = 1;
    if (*counter_val) {
      auto cur =
          read_counter_le((*counter_val)->data(), (*counter_val)->size());
      if (!cur) {
        return KVRGW_ERR_CORRUPT_VALUE;
      }
      tenant_num = *cur + 1;
    }
    if (tenant_num > std::numeric_limits<uint32_t>::max()) {
      return KVRGW_ERR_INTERNAL;
    }
    write_counter_le(tenant_num, *tr, counter_key.view());
    const tenant_id_t tenant_id = static_cast<tenant_id_t>(tenant_num);
    tr->kv_put(tenant_key.view(), make_tenant_value(tenant_id, now_unix()));

    auto rc = tr->commit();
    if (rc) {
      put_tenant_cache(tenant_name, tenant_id);
      *out_id = tenant_id;
      return KVRGW_ERR_OK;
    }
    auto commit_ec = fdb_to_error(rc.error());
    if (is_retriable(commit_ec)) {
      // A retriable FDB error poisons that txn -> restart a fresh txn
      continue;
    }
    return commit_ec;
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

std::expected<std::optional<ObjectValue>, fdb_error_t>
KvRgwServiceImpl::load_object(bucket_id_t bucket_id,
                              const std::string &object_name)
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

std::expected<std::optional<KvRgwServiceImpl::LoadResult>, fdb_error_t>
KvRgwServiceImpl::load_object_with_data(bucket_id_t bucket_id,
                                        const std::string &object_name)
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

  if (result.value.hdr.chunk.type == CHUNK_INLINE) {
    result.data.assign(result.value.inline_data.begin(),
                       result.value.inline_data.end());
  }
  else if (result.value.hdr.chunk.type == CHUNK_CHILD_D ||
           result.value.hdr.chunk.type == CHUNK_CHILD_D_REF) {
    const uint8_t st = d_size_tier_from_size(result.value.hdr.size);
    const uint32_t mtime = result.value.hdr.last_modified_sec;
    const bucket_id_t d_bucket =
        (result.value.hdr.chunk.type == CHUNK_CHILD_D_REF)
            ? result.value.chunk_data_bucket_id
            : bucket_id;
    const std::string_view ref_sv =
        (result.value.hdr.chunk.type == CHUNK_CHILD_D_REF)
            ? std::string_view(reinterpret_cast<const char *>(
                                   result.value.chunk_data_ref_tag),
                               12)
            : std::string_view(
                  reinterpret_cast<const char *>(result.value.hdr.ref_tag), 12);
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
// Zero-copy move_object_to_g: reads directly from wire-format raw_value.
bool KvRgwServiceImpl::move_object_to_g(KvTransaction &tr,
                                         std::string_view object_key,
                                         std::string_view raw_value)
{
  const auto parts = parse_object_key_parts_view(object_key);
  if (!parts) {
    std::cerr << "move_object_to_g: corrupt object key (len="
              << object_key.size() << "), fencing entry\n";
    return false;
  }

  const ObjectValueHeader *h = ovh_ptr(raw_value);
  if (!h) {
    tr.kv_del(object_key);
    return false;
  }

  const std::string_view ref_tag_sv = ovh_ref_tag(h);
  const auto tc = tier_config_state_.active_copy();

  const bool must_defer_to_gc = h->chunk.type == CHUNK_STORAGE ||
                                h->chunk.type == CHUNK_STORAGE_REF ||
                                (h->flags & ObjectValue::kFlagExternalAnnotations &&
                                 be16toh(h->annotations_count) > 0) ||
                                tc.kv_store_coalescing;

  const uint64_t obj_size = be64toh(h->size);
  const uint32_t mtime = be32toh(h->last_modified_sec);

  KeyBuf key_buf;
  if (must_defer_to_gc) {
    GcValueHeader gc_hdr{};
    gc_hdr.chunk = h->chunk;
    gc_hdr.flags = h->flags;
    gc_hdr.object_size = obj_size;
    gc_hdr.mtime = mtime;
    make_go_key(parts->shard_count, parts->shard_id,
                parts->bucket_id, ref_tag_sv, obj_size, key_buf);
    tr.kv_put(key_buf.view(), make_gc_value(gc_hdr));
    tr.kv_del(object_key);
  }
  else {
    const uint8_t st = d_size_tier_from_size(obj_size);

    if (h->chunk.type == CHUNK_CHILD_D) {
      make_d_key(parts->bucket_id, st, ref_tag_sv, mtime, key_buf);
      const bool shared = (h->flags & ObjectValue::kFlagSharedData) != 0;
      decrement_or_del_child_d(tr, key_buf.view(), obj_size, shared);
    }
    else if (h->chunk.type == CHUNK_CHILD_D_REF) {
      OvhChunkRef cref;
      if (ovh_chunk_ref(raw_value, h, cref)) {
        make_d_key(cref.bucket_id, st, cref.ref_tag, mtime, key_buf);
        const bool shared = (h->flags & ObjectValue::kFlagSharedData) != 0;
        decrement_or_del_child_d(tr, key_buf.view(), obj_size, shared);
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
                                          const std::string &object_name,
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
                               const DeleteCondition *cond)
{
  // Construct FdbGetHolder on-the-fly from the stored future for zero-copy access.
  FdbGetHolder obj_holder(std::move(ctx.f_obj));
  const fdb_error_t obj_err = obj_holder.wait();
  if (obj_err) {
    return std::unexpected(fdb_to_error(obj_err));
  }

  const bool obj_present = obj_holder.present();
  const std::string_view raw_val = obj_present ? obj_holder.value() : std::string_view{};
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
    if (!cond->if_match.empty() && cond->if_match != "*") {
      if (ovh_etag_display(old_hdr) != cond->if_match) {
        return std::unexpected(KVRGW_ERR_PRECONDITION_FAILED);
      }
    }
    if (cond->if_match_last_modified_time != 0) {
      if (ovh_last_modified_sec(old_hdr) != cond->if_match_last_modified_time) {
        return std::unexpected(KVRGW_ERR_PRECONDITION_FAILED);
      }
    }
    if (cond->has_if_match_size) {
      if (static_cast<int64_t>(ovh_size(old_hdr)) != cond->if_match_size) {
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
    dm.hdr.chunk.type = CHUNK_INLINE;
    //dm.hdr.chunk.type = CHUNK_EMPTY;
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

std::expected<KvRgwServiceImpl::DeleteResult, KvrgwErrorCode>
KvRgwServiceImpl::delete_single(KvTransaction &tr, tenant_id_t tenant_id,
                                const std::string &bucket_name,
                                const std::string &object_name,
                                const DeleteCondition *cond)
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

  // Phase 3: append directly into out; roll back on commit failure
  const size_t base = out->size();
  for (size_t i = 0; i < keys.size(); ++i) {
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

  if (tr->commit()) {
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
  delete_prepare_key(*tr, batch->bucket_id, std::string(key), ctx);

  auto bs = delete_verify_bucket(*batch);
  DeleteMultiKeyOutcome outcome;
  outcome.key = std::string(key);
  if (bs) {
    auto result = delete_apply(*tr, ctx, *bs, nullptr);
    if (result && tr->commit()) {
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
  tr.kv_put(
      ct_key.view(),
      make_child_value(
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
    const RefTag &ref_tag, ObjectValue &new_value,
    VersioningState *out_versioning_state, std::span<const uint8_t> tags,
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

    PutInTxnParams params{tenant_id, bucket_name, bucket_id,   object_name,
                          ref_tag,   new_value,   &empty_data, nullptr,
                          tags,      false,       true,        metadata};
    auto ec = put_object_in_txn(*tr, params, out_versioning_state);
    if (ec != KVRGW_ERR_OK) {
      if (ec == KVRGW_ERR_INTERNAL) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }

    auto rc = tr->commit();
    if (rc) {
      put_bucket_cache(tenant_id, bucket_name, bucket_id);
      return KVRGW_ERR_OK;
    }
    auto commit_ec = fdb_to_error(rc.error());
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
  constexpr int kMaxRetries = 3;
  if (!is_valid_bucket_name(bucket_name)) {
    return KVRGW_ERR_INVALID_BUCKET_NAME;
  }
  // generate key to the bucket entry
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

    // generate key to the bucket-assigned-counter
    const auto counter_key =
        make_l_key(kLocalTypeNumeric, kLocalCounterBucketId);
    // async get() in parallel of both KV
    auto f_existing = tr->kv_async_get(bucket_key.view());
    auto f_counter = tr->kv_async_get(counter_key.view());

    // block on bucket-entry KV
    auto existing = tr->kv_wait_get(f_existing);
    if (!existing) {
      // failure
      auto ec = fdb_to_error(existing.error());
      if (is_retriable(ec)) {
        // A retriable FDB error poisons that txn -> restart a fresh txn
        continue;
      }
      return ec;
    }

    if (*existing) {
      // bucket-already exist -> abort
      return KVRGW_ERR_OK;
    }

    // block on bucket-assigned-counter
    auto counter_val = tr->kv_wait_get(f_counter);
    if (!counter_val) {
      // failure
      auto ec = fdb_to_error(counter_val.error());
      if (is_retriable(ec)) {
        // A retriable FDB error poisons that txn -> restart a fresh txn
        continue;
      }
      return ec;
    }

    uint64_t bucket_num = 1;
    if (*counter_val) {
      auto cur =
          read_counter_le((*counter_val)->data(), (*counter_val)->size());
      if (!cur) {
        return KVRGW_ERR_CORRUPT_VALUE;
      }
      bucket_num = *cur + 1;
    }
    write_counter_le(bucket_num, *tr, counter_key.view());
    const bucket_id_t bucket_id = static_cast<bucket_id_t>(bucket_num);
    tr->kv_put(bucket_key.view(), make_bucket_value(bucket_id, now_unix()));

    auto rc = tr->commit();
    if (rc) {
      put_bucket_cache(tenant_id, std::string(bucket_name), bucket_id);
      return KVRGW_ERR_OK;
    }
    auto ec = fdb_to_error(rc.error());
    if (is_retriable(ec)) {
      // A retriable FDB error poisons that txn -> restart a fresh txn
      continue;
    }
    return ec;
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//---------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::bucket_exists(tenant_id_t tenant_id,
                                               std::string_view bucket_name,
                                               bool *out_exists,
                                               bucket_id_t *out_id)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kBucketExists);
  ops_stats_.inc(OpType::kBucketExists);
  auto state = read_bucket_state(tenant_id, std::string(bucket_name));
  if (!state) {
    return fdb_to_error(state.error());
  }
  if (*state) {
    *out_exists = true;
    *out_id = (*state)->bucket_id;
  }
  else {
    *out_exists = false;
  }
  return KVRGW_ERR_OK;
}

//---------------------------------------------------------------------------------
KvrgwErrorCode
KvRgwServiceImpl::bucket_exists_cached(tenant_id_t tenant_id,
                                       std::string_view bucket_name,
                                       bool *out_exists, bucket_id_t *out_id)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kBucketExists);
  ops_stats_.inc(OpType::kBucketExists);
  auto state = get_bucket_id_cached(tenant_id, std::string(bucket_name));
  if (!state) {
    return fdb_to_error(state.error());
  }
  if (*state) {
    *out_exists = true;
    *out_id = (*state)->bucket_id;
  }
  else {
    *out_exists = false;
  }
  return KVRGW_ERR_OK;
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
    make_po_key(params.bucket_id, params.object_name,
                ref_tag_view(params.ref_tag), ctx.po_key);
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
    if (!found) {
      // TBD: why are we doing blocking get ???
      auto vb = verify_bucket_in_txn(tr, params.tenant_id, params.bucket_name,
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
  }
  if (out_versioning_state) {
    *out_versioning_state = vs;
  }

  if (ctx.is_storage_tier) {
    auto existing = tr.kv_wait_get(ctx.f_obj);
    if (!existing) {
      return fdb_to_error(existing.error());
    }

    if (ctx.f_po) {
      auto po_val = tr.kv_wait_get(ctx.f_po);
      if (!po_val) {
        return fdb_to_error(po_val.error());
      }

      if (!*po_val) {
        if (*existing) {
          const auto *h = ovh_ptr(**existing);
          if (h && RefTagGenerator::equal(ovh_ref_tag(h),
                                          ref_tag_view(params.ref_tag))) {
            return KVRGW_ERR_OK;
          }
        }
        return KVRGW_ERR_INTERNAL;
      }

      if (*existing) {
        const auto *h = ovh_ptr(**existing);
        if (h && RefTagGenerator::equal(ovh_ref_tag(h),
                                        ref_tag_view(params.ref_tag))) {
          tr.kv_del(ctx.po_key.view());
          return KVRGW_ERR_OK;
        }
        if (h) {
          displace_old_object(tr, vs, ctx.object_key.view(), **existing);
        }
      }
    }
    else {
      if (*existing) {
        const auto *h = ovh_ptr(**existing);
        if (h && RefTagGenerator::equal(ovh_ref_tag(h),
                                        ref_tag_view(params.ref_tag))) {
          return KVRGW_ERR_OK;
        }
        if (h) {
          displace_old_object(tr, vs, ctx.object_key.view(), **existing);
        }
      }
    }

    const ObjectValueHeader *old_hdr = nullptr;
    if (*existing) {
      old_hdr = ovh_ptr(**existing);
    }

    auto ids = compute_new_version(vs, old_hdr);
    params.value.hdr.version_id = ids.version_id;
    params.value.hdr.next_vid = ids.next_vid;

    if (!params.metadata.empty()) {
      params.value.metadata_frame.assign(params.metadata.begin(),
                                         params.metadata.end());
    }

    if (!params.tags.empty()) {
      apply_tags_to_value(params.value, params.tags, tr, params.bucket_id,
                          ref_tag_view(params.ref_tag));
    }

    OValueBuf vbuf;
    if (!write_object_value(vbuf, params.value)) {
      return KVRGW_ERR_VALUE_TOO_LARGE;
    }

    tr.kv_put(ctx.object_key.view(), vbuf.view());

    if (ctx.f_po) {
      tr.kv_del(ctx.po_key.view());
    }

    return KVRGW_ERR_OK;
  }

  // Inline / child-D path
  auto existing = tr.kv_wait_get(ctx.f_obj);
  if (!existing) {
    return fdb_to_error(existing.error());
  }

  const ObjectValueHeader *old_hdr = nullptr;
  std::string_view old_raw;
  if (*existing) {
    old_raw = **existing;
    old_hdr = ovh_ptr(old_raw);
    if (!old_hdr) {
      return KVRGW_ERR_CORRUPT_VALUE;
    }
  }

  if (params.cond) {
    if (!params.cond->if_match.empty()) {
      if (!old_hdr || ovh_is_delete_marker(old_hdr)) {
        return KVRGW_ERR_NO_SUCH_KEY;
      }
      if (params.cond->if_match != "*" &&
          ovh_etag_display(old_hdr) != params.cond->if_match) {
        return KVRGW_ERR_PRECONDITION_FAILED;
      }
    }
    if (!params.cond->if_none_match.empty()) {
      if (params.cond->if_none_match == "*" && old_hdr &&
          !ovh_is_delete_marker(old_hdr)) {
        return KVRGW_ERR_PRECONDITION_FAILED;
      }
      if (params.cond->if_none_match != "*" && old_hdr &&
          !ovh_is_delete_marker(old_hdr) &&
          ovh_etag_display(old_hdr) == params.cond->if_none_match) {
        return KVRGW_ERR_PRECONDITION_FAILED;
      }
    }
  }

  if (old_hdr) {
    displace_old_object(tr, vs, ctx.object_key.view(), old_raw);
  }

  auto ids = compute_new_version(vs, old_hdr);
  params.value.hdr.version_id = ids.version_id;
  params.value.hdr.next_vid = ids.next_vid;
  if (out_versioning_state) {
    *out_versioning_state = vs;
  }

  if (!params.metadata.empty()) {
    params.value.metadata_frame.assign(params.metadata.begin(),
                                       params.metadata.end());
  }

  if (!params.tags.empty()) {
    apply_tags_to_value(params.value, params.tags, tr, params.bucket_id,
                        ref_tag_view(params.ref_tag));
  }

  OValueBuf vbuf;
  if (!write_object_value(vbuf, params.value)) {
    return KVRGW_ERR_VALUE_TOO_LARGE;
  }
  tr.kv_put(ctx.object_key.view(), vbuf.view());

  if (params.value.hdr.chunk.type == CHUNK_CHILD_D && params.data) {
    const uint8_t st = d_size_tier_from_size(params.data->size());
    const uint32_t mtime =
        static_cast<uint32_t>(params.value.hdr.last_modified_sec);
    KeyBuf d_key;
    make_d_key(params.bucket_id, st, ref_tag_view(params.ref_tag), mtime, d_key);
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
    const std::string &object_name, const RefTag &ref_tag,
    ObjectValue &new_value, const std::string &data,
    VersioningState *out_versioning_state, const PutCondition *cond,
    std::span<const uint8_t> tags, std::span<const uint8_t> metadata)
{
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
                          ref_tag,   new_value,   &data,     cond,
                          tags,      false,       false,     metadata};
    auto ec = put_object_in_txn(*tr, params, out_versioning_state);
    if (ec != KVRGW_ERR_OK) {
      error_stats_->record(ec);
      if (ec == KVRGW_ERR_INTERNAL) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }

    auto rc = tr->commit();
    if (rc) {
      put_bucket_cache(tenant_id, bucket_name, bucket_id);
      return KVRGW_ERR_OK;
    }
    auto commit_ec = fdb_to_error(rc.error());
    error_stats_->record(commit_ec);
    if (!is_retriable(commit_ec)) {
      latency_stats_.txn_hard_failures.fetch_add(1, std::memory_order_relaxed);
      return commit_ec;
    }
    latency_stats_.txn_retries.fetch_add(1, std::memory_order_relaxed);
    sleep_for_msec(10 * (attempt + 1));
  }
  latency_stats_.txn_max_retries_exceeded.fetch_add(1,
                                                    std::memory_order_relaxed);
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::select_storage_tier(
    tenant_id_t tenant_id, const std::string &bucket_name,
    const std::string &object_name, const RefTag &ref_tag,
    ObjectValue &object_value, const std::string &data, uint64_t estimated_size,
    VersioningState *out_versioning_state, const PutCondition *cond,
    std::span<const uint8_t> tags, std::span<const uint8_t> metadata)
{
  const auto tc = tier_config_state_.active_copy();
  if (data.size() <= tc.max_inline) {
    object_value.hdr.chunk.type = CHUNK_INLINE;
    object_value.inline_data.assign(data.begin(), data.end());
    return put_object_single_txn(tenant_id, bucket_name, object_name, ref_tag,
                                 object_value, data, out_versioning_state, cond,
                                 tags, metadata);
  }
  if (data.size() <= tc.max_kv_store) {
    object_value.hdr.chunk.type = CHUNK_CHILD_D;
    return put_object_single_txn(tenant_id, bucket_name, object_name, ref_tag,
                                 object_value, data, out_versioning_state, cond,
                                 tags, metadata);
  }
  object_value.hdr.chunk.type = CHUNK_STORAGE;

  auto cached_bid = get_bucket_id_cached(tenant_id, bucket_name);
  if (!cached_bid) {
    return fdb_to_error(cached_bid.error());
  }
  if (!*cached_bid) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }

  const bucket_id_t bid = (**cached_bid).bucket_id;
  KeyBuf po_key;
  make_po_key(bid, object_name, ref_tag_view(ref_tag), po_key);
  auto po_rc = store_.set(
      po_key.view(),
      make_po_value(estimated_size, static_cast<uint32_t>(now_unix())));
  if (!po_rc) {
    return fdb_to_error(po_rc.error());
  }

  const Stopwatch ds_t0;
  auto write_ec = data_store_.write(ref_tag_view(ref_tag), data);
  fdb_record_disk(ds_t0.elapsed_us());
  if (write_ec) {
    return KVRGW_ERR_INTERNAL;
  }

  // Fault injection: abort after Phase 2, leaving stale individual P:O + orphan
  // blob
  if (err_insertion_.is_error_active(FaultType::kAbortAfterSinglePhase2)) {
    return KVRGW_ERR_INTERNAL;
  }

  return put_object_phase3(tenant_id, bucket_name, bid, object_name, ref_tag,
                           object_value, out_versioning_state, tags, metadata);
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
      req.value.hdr.chunk.type = CHUNK_INLINE;
      req.value.inline_data.assign(data, data + data_len);
    }
    else if (data_len <= tc.max_kv_store) {
      req.value.hdr.chunk.type = CHUNK_CHILD_D;
    }
    else {
      req.value.hdr.chunk.type = CHUNK_STORAGE;
    }

    BatchCommitEntry entry;
    entry.tenant_id = req.tenant_id;
    entry.bucket_name = req.bucket_name;
    entry.object_name = req.object_name;
    entry.ref_tag = req.ref_tag;
    entry.value = req.value;
    entry.data = std::move(data_str);
    entry.chunk_type = req.value.hdr.chunk.type;
    if (req.cond) {
      entry.if_match = req.cond->if_match;
      entry.if_none_match = req.cond->if_none_match;
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
    req.value.hdr.chunk.type = CHUNK_INLINE;
    result.error_code = put_object_single_txn(
        req.tenant_id, req.bucket_name, req.object_name, req.ref_tag, req.value,
        data_str, &vs, req.cond, req.tags, req.metadata);
  }
  else {
    result.error_code = select_storage_tier(
        req.tenant_id, req.bucket_name, req.object_name, req.ref_tag, req.value,
        data_str, req.estimated_size, &vs, req.cond, req.tags, req.metadata);
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
  const std::string bname(bucket_name);
  const std::string list_prefix(prefix);
  const std::string delim(delimiter);
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

    auto cached = get_bucket_id_cached(tenant_id, bname);
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

      auto f_bkt = issue_bucket_get(*tr, tenant_id, bname);
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
            const std::string common =
              list_prefix + std::string(relative.substr(0, delim_pos + delim.size()));
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
      auto vb = resolve_bucket_verify(tenant_id, bname, f_bkt, bucket_id,
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
    version_id_t version_id, const DeleteCondition *cond)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kDeleteObjectVersion);
  ops_stats_.inc(OpType::kDeleteObjectVersion);
  const std::string bname(bucket_name);
  const DeleteCondition empty_cond{};
  const DeleteCondition &c = cond ? *cond : empty_cond;
  auto bucket_id_res = get_bucket_id_cached(tenant_id, bname);
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

    const bool has_cond = !c.if_match.empty() || c.has_if_match_size ||
                          c.if_match_last_modified_time != 0;

    if (current && current->hdr.version_id == target_vid) {
      if (has_cond) {
        if (current->is_delete_marker()) {
          return KVRGW_ERR_PRECONDITION_FAILED;
        }
        const std::string &im = c.if_match;
        if (!im.empty() && im != "*" && current->etag_display() != im) {
          return KVRGW_ERR_PRECONDITION_FAILED;
        }
        if (c.if_match_last_modified_time != 0 &&
            static_cast<int64_t>(current->hdr.last_modified_sec) !=
                c.if_match_last_modified_time) {
          return KVRGW_ERR_PRECONDITION_FAILED;
        }
        if (c.has_if_match_size &&
            static_cast<int64_t>(current->hdr.size) != c.if_match_size) {
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
          const std::string_view ref_sv(
              reinterpret_cast<const char *>(current->hdr.ref_tag), 12);
          if (current->hdr.chunk.type == CHUNK_CHILD_D) {
            const uint8_t st = d_size_tier_from_size(current->hdr.size);
            const uint32_t mtime =
                static_cast<uint32_t>(current->hdr.last_modified_sec);
            KeyBuf d_key;
            make_d_key(parts->bucket_id, st, ref_sv, mtime, d_key);
            tr->kv_del(d_key.view());
          }
          else if (current->hdr.chunk.type == CHUNK_STORAGE) {
            KeyBuf go_key;
            make_go_key(parts->shard_count, parts->shard_id,
                        parts->bucket_id, ref_sv, current->hdr.size, go_key);
            GcValueHeader gc_hdr{};
            gc_hdr.chunk = current->hdr.chunk;
            gc_hdr.object_size = current->hdr.size;
            gc_hdr.mtime = current->hdr.last_modified_sec;
            tr->kv_put(go_key.view(), make_gc_value(gc_hdr));
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
        const std::string &im = c.if_match;
        if (!im.empty() && im != "*" && v_entry->etag_display() != im) {
          return KVRGW_ERR_PRECONDITION_FAILED;
        }
        if (c.if_match_last_modified_time != 0 &&
            static_cast<int64_t>(v_entry->hdr.last_modified_sec) !=
                c.if_match_last_modified_time) {
          return KVRGW_ERR_PRECONDITION_FAILED;
        }
        if (c.has_if_match_size &&
            static_cast<int64_t>(v_entry->hdr.size) != c.if_match_size) {
          return KVRGW_ERR_PRECONDITION_FAILED;
        }
      }
      if (v_entry && v_entry->has_data()) {
        const auto parts = parse_object_key(object_key.view());
        if (parts) {
          const std::string_view ref_sv(
              reinterpret_cast<const char *>(v_entry->hdr.ref_tag), 12);
          KeyBuf go_key;
          make_go_key(parts->shard_count, parts->shard_id,
                      parts->bucket_id, ref_sv, v_entry->hdr.size, go_key);
          GcValueHeader gc_hdr{};
          gc_hdr.chunk = v_entry->hdr.chunk;
          gc_hdr.object_size = v_entry->hdr.size;
          gc_hdr.mtime = v_entry->hdr.last_modified_sec;
          tr->kv_put(go_key.view(), make_gc_value(gc_hdr));
        }
      }
      tr->kv_del(v_key.view());
    }

    auto rc = tr->commit();
    if (rc) {
      return KVRGW_ERR_OK;
    }
    auto commit_ec = fdb_to_error(rc.error());
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
  constexpr int kChunkSize = 20;
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
      const std::string key(obj.key);
      if (obj.version_id) {
        const auto ec = delete_object_version(tenant_id, bname, key,
                                              *obj.version_id, nullptr);
        if (ec == KVRGW_ERR_OK) {
          out->emplace_back(DeleteMultiKeyOutcome::Status::Deleted, key, "");
          out->back().version_id = *obj.version_id;
        }
        else {
          out->emplace_back(DeleteMultiKeyOutcome::Status::Error, key,
                            "InternalError");
          out->back().error_message = kvrgw_strerror(ec);
          out->back().version_id = *obj.version_id;
        }
      }
      else {
        delete_multi_one_key(tenant_id, bname, bucket_id, key, *out);
      }
    }
    return KVRGW_ERR_OK;
  }

  const int key_count = static_cast<int>(keys.size());
  out->reserve(out->size() + static_cast<size_t>(key_count));
  // Allocate the per-key ctx array once — reused across all chunk iterations.
  std::unique_ptr<DeleteContext[]> ctx_buf(new DeleteContext[kChunkSize]);
  for (int i = 0; i < key_count;) {
    if (single_delete_mode) {
      delete_multi_one_key(tenant_id, bname, bucket_id,
                           keys[static_cast<size_t>(i)], *out);
      ++i;
      continue;
    }

    const int chunk_end = std::min(i + kChunkSize, key_count);
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
  const std::string name(bucket_name);

  auto cached = get_bucket_id_cached(tenant_id, name);
  if (!cached) {
    // failure
    return fdb_to_error(cached.error());
  }
  if (!*cached) {
    // bucket-already gone -> abort
    return KVRGW_ERR_OK;
  }

  auto ac = check_access(tenant_id, name, **cached, kDenyDeleteBucket);
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
      invalidate_bucket_cache(tenant_id, name);
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
    KeyBuf v_prefix_buf;
    uint8_t bid_be[sizeof(bucket_id_t)];
    bucket_id.serialize(bid_be);
    KeyHeaderS hdr('S', kShardCount, kShardId, bid_be, kCategoryVersion);

    v_prefix_buf.set_header(hdr);
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
    auto rc = tr->commit();
    if (rc) {
      invalidate_bucket_cache(tenant_id, name);
      sweeper_.process_bucket(bucket_id, true);
      return KVRGW_ERR_OK;
    }
    auto ec = fdb_to_error(rc.error());
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
  const std::string name(bucket_name);

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
  auto rc = (*tr)->commit();
  if (!rc) {
    return fdb_to_error(rc.error());
  }
  invalidate_bucket_cache(tenant_id, name);
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
  const std::string name(bucket_name);

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
  auto rc = (*tr)->commit();
  if (!rc) {
    return fdb_to_error(rc.error());
  }
  invalidate_bucket_cache(tenant_id, name);
  return KVRGW_ERR_OK;
}

//---------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::put_bucket_versioning(
    tenant_id_t tenant_id, std::string_view bucket_name, VersioningState state)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kPutBucketVersioning);
  ops_stats_.inc(OpType::kPutBucketVersioning);
  constexpr int kMaxRetries = 3;
  KeyBuf bucket_key;
  make_bucket_key(tenant_id, bucket_name, bucket_key);
  const std::string name(bucket_name);

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

    auto rc = tr->commit();
    if (rc) {
      invalidate_bucket_cache(tenant_id, name);
      return KVRGW_ERR_OK;
    }
    auto commit_ec = fdb_to_error(rc.error());
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
  auto state = read_bucket_state(tenant_id, std::string(bucket_name));
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
  const std::string src_bucket(req.src_bucket_name);
  const std::string dst_bucket(req.dst_bucket_name);
  const std::string src_key(req.src_key);
  const std::string dst_key(req.dst_key);

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
    auto f_src_o = tr->kv_async_get(src_o_key.view());
    auto f_dst_o = tr->kv_async_get(dst_o_key.view());

    auto src_o_raw = tr->kv_wait_get(f_src_o);
    if (!src_o_raw) {
      auto ec = fdb_to_error(src_o_raw.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }

    // --- Read source ---
    ObjectValue src;
    std::string src_entry_key;

    if (req.src_version_id) {
      const version_id_t src_vid = *req.src_version_id;
      bool found_in_o = false;
      if (*src_o_raw) {
        auto parsed = parse_object_value(**src_o_raw);
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
      if (!*src_o_raw) {
        return KVRGW_ERR_NO_SUCH_KEY;
      }
      auto parsed = parse_object_value(**src_o_raw);
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
    auto dst_o_raw = tr->kv_wait_get(f_dst_o);
    if (!dst_o_raw) {
      auto ec = fdb_to_error(dst_o_raw.error());
      if (is_retriable(ec)) {
        sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }
    const ObjectValueHeader *dst_hdr = nullptr;
    std::string_view dst_raw;
    if (*dst_o_raw) {
      dst_raw = **dst_o_raw;
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
          const std::string_view src_ref_sv(
              reinterpret_cast<const char *>(src.hdr.ref_tag), kRefTagSize);
          apply_tags_to_value(src, req.tags, *tr, dst_bucket_id, src_ref_sv);
        }
        OValueBuf buf;
        if (!write_object_value(buf, src)) {
          return KVRGW_ERR_INTERNAL;
        }
        tr->kv_put(dst_o_key.view(), buf.view());
        auto rc = tr->commit();
        if (rc) {
          out->etag = src.etag_display();
          out->last_modified_unix = src.hdr.last_modified_sec;
          out->copy_source_version_id = src.hdr.version_id;
          return KVRGW_ERR_OK;
        }
        {
          auto ec = fdb_to_error(rc.error());
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
    std::memcpy(new_value.hdr.ref_tag, new_ref_tag.data(), kRefTagSize);
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
    switch (src.hdr.chunk.type) {
    case CHUNK_INLINE: {
      new_value.hdr.chunk.type = CHUNK_INLINE;
      new_value.inline_data = src.inline_data;
      break;
    }
    case CHUNK_CHILD_D:
    case CHUNK_CHILD_D_REF: {
      const bucket_id_t d_bucket = (src.hdr.chunk.type == CHUNK_CHILD_D_REF)
                                       ? src.chunk_data_bucket_id
                                       : src_bucket_id;
      const std::string_view d_ref_sv =
          (src.hdr.chunk.type == CHUNK_CHILD_D_REF)
              ? std::string_view(
                    reinterpret_cast<const char *>(src.chunk_data_ref_tag),
                    kRefTagSize)
              : std::string_view(
                    reinterpret_cast<const char *>(src.hdr.ref_tag),
                    kRefTagSize);
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

      new_value.hdr.chunk.type = CHUNK_CHILD_D_REF;
      new_value.chunk_data_bucket_id = d_bucket;
      std::memcpy(new_value.chunk_data_ref_tag, d_ref_sv.data(), kRefTagSize);
      new_value.hdr.flags |= ObjectValue::kFlagSharedData;
      break;
    }
    case CHUNK_STORAGE:
    case CHUNK_STORAGE_REF: {
      const std::string_view src_data_ref =
          (src.hdr.chunk.type == CHUNK_STORAGE_REF)
              ? std::string_view(
                    reinterpret_cast<const char *>(src.chunk_data_ref_tag),
                    kRefTagSize)
              : std::string_view(
                    reinterpret_cast<const char *>(src.hdr.ref_tag),
                    kRefTagSize);
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
        gc_hdr.chunk = src.hdr.chunk;
        gc_hdr.object_size = src.hdr.size;
        gc_hdr.mtime = src.hdr.last_modified_sec;
        const auto chunk_desc = make_gc_value(gc_hdr);
        tr->kv_put(r_key.view(), write_r_value(2, chunk_desc));

        src.hdr.flags |= ObjectValue::kFlagSharedData;
        OValueBuf src_buf;
        if (write_object_value(src_buf, src)) {
          tr->kv_put(src_entry_key, src_buf.view());
        }
      }

      new_value.hdr.chunk.type = CHUNK_STORAGE_REF;
      std::memcpy(new_value.chunk_data_ref_tag, src_data_ref.data(),
                  kRefTagSize);
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
      const std::string_view new_ref_sv(
          reinterpret_cast<const char *>(new_value.hdr.ref_tag), kRefTagSize);
      apply_tags_to_value(new_value, req.tags, *tr, dst_bucket_id, new_ref_sv);
    }
    else if (src.hdr.tags_count > 0) {
      const std::string_view src_ref_sv(
          reinterpret_cast<const char *>(src.hdr.ref_tag), kRefTagSize);
      KeyBuf src_ct;
      make_ct_key(src_bucket_id, src_ref_sv, src_ct);
      auto ct_val = tr->kv_get(src_ct.view());
      if (!ct_val || !*ct_val) {
        return KVRGW_ERR_CORRUPT_VALUE;
      }
      const std::string_view new_ref_sv(
          reinterpret_cast<const char *>(new_value.hdr.ref_tag), kRefTagSize);
      KeyBuf dst_ct;
      make_ct_key(dst_bucket_id, new_ref_sv, dst_ct);
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
    if (dst_hdr) {
      displace_old_object(*tr, vb->versioning_state, dst_o_key.view(), dst_raw);
    }
    auto ids = compute_new_version(vb->versioning_state, dst_hdr);
    new_value.hdr.version_id = ids.version_id;
    new_value.hdr.next_vid = ids.next_vid;

    // --- Write destination O: ---
    OValueBuf dst_buf;
    if (!write_object_value(dst_buf, new_value)) {
      return KVRGW_ERR_INTERNAL;
    }
    tr->kv_put(dst_o_key.view(), dst_buf.view());

    auto rc = tr->commit();
    if (rc) {
      out->etag = new_value.etag_display();
      out->last_modified_unix = new_value.hdr.last_modified_sec;
      if (vb->versioning_state == VERSIONING_ENABLED) {
        out->version_id = new_value.hdr.version_id;
      }
      out->copy_source_version_id = src.hdr.version_id;
      return KVRGW_ERR_OK;
    }
    {
      auto ec = fdb_to_error(rc.error());
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
                                                      const std::string &bucket_name,
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
                                                      const std::string &bucket_name,
                                                      std::string_view key,
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
  const std::string object_name(key);

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
      if (v_obj->hdr.chunk.type == CHUNK_INLINE) {
        data.assign(v_obj->inline_data.begin(), v_obj->inline_data.end());
      }
      else if (v_obj->hdr.chunk.type == CHUNK_CHILD_D ||
               v_obj->hdr.chunk.type == CHUNK_CHILD_D_REF) {
        const uint8_t st = d_size_tier_from_size(v_obj->hdr.size);
        const uint32_t mtime = v_obj->hdr.last_modified_sec;
        const bucket_id_t d_bucket =
          (v_obj->hdr.chunk.type == CHUNK_CHILD_D_REF)
          ? v_obj->chunk_data_bucket_id
          : bucket_id;
        const std::string_view ref_sv =
          (v_obj->hdr.chunk.type == CHUNK_CHILD_D_REF)
          ? std::string_view(
            reinterpret_cast<const char *>(v_obj->chunk_data_ref_tag),
            12)
          : std::string_view(
            reinterpret_cast<const char *>(v_obj->hdr.ref_tag), 12);
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

//--------------------------------------------------------------------------------
KvrgwErrorCode
KvRgwServiceImpl::get_object(tenant_id_t tenant_id,
                             std::string_view bucket_name, std::string_view key,
                             std::optional<version_id_t> version_id,
                             const ByteRange *range, GetObjectResult *out)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kGetObject);
  ops_stats_.inc(OpType::kGetObject);
  *out = {};
  const std::string bname(bucket_name);
  ObjectValue object_value;
  std::string kv_data;
  const auto ec =
      load_object_for_read(tenant_id, bname, key, version_id, true,
                           &object_value, &kv_data, &out->error_detail);
  if (ec != KVRGW_ERR_OK) {
    return ec;
  }

  const int64_t object_size = static_cast<int64_t>(object_value.hdr.size);
  std::string body;
  if (object_value.hdr.chunk.type == CHUNK_INLINE ||
      object_value.hdr.chunk.type == CHUNK_CHILD_D ||
      object_value.hdr.chunk.type == CHUNK_CHILD_D_REF) {
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
  else {
    const std::string_view ref_sv =
        (object_value.hdr.chunk.type == CHUNK_STORAGE_REF)
            ? std::string_view(reinterpret_cast<const char *>(
                                   object_value.chunk_data_ref_tag),
                               12)
            : std::string_view(
                  reinterpret_cast<const char *>(object_value.hdr.ref_tag), 12);
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
    tenant_id_t tenant_id, std::string_view bucket_name, std::string_view key,
    std::optional<version_id_t> version_id, ObjectValue *out,
    std::string *error_detail)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kHeadObject);
  ops_stats_.inc(OpType::kHeadObject);
  const std::string bname(bucket_name);
  return load_object_for_read(tenant_id, bname, key, version_id, false, out,
                              nullptr, error_detail);
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
  std::string raw(8 + last_o.size() + last_v.size(), '\0');
  const uint32_t ol = htobe32(static_cast<uint32_t>(last_o.size()));
  const uint32_t vl = htobe32(static_cast<uint32_t>(last_v.size()));
  std::memcpy(raw.data(), &ol, 4);
  if (!last_o.empty()) {
    std::memcpy(raw.data() + 4, last_o.data(), last_o.size());
  }
  std::memcpy(raw.data() + 4 + last_o.size(), &vl, 4);
  if (!last_v.empty()) {
    std::memcpy(raw.data() + 8 + last_o.size(), last_v.data(), last_v.size());
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
  if (!dec || dec->size() < 8) {
    return false;
  }
  uint32_t ol_be{};
  std::memcpy(&ol_be, dec->data(), 4);
  const uint32_t ol = be32toh(ol_be);
  if (dec->size() < 8 + ol) {
    return false;
  }
  uint32_t vl_be{};
  std::memcpy(&vl_be, dec->data() + 4 + ol, 4);
  const uint32_t vl = be32toh(vl_be);
  if (dec->size() < 8 + ol + vl) {
    return false;
  }
  last_o->assign(dec->data() + 4, ol);
  last_v->assign(dec->data() + 8 + ol, vl);
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
  const std::string bname(bucket_name);
  const std::string list_prefix(prefix);
  max_keys = std::min((max_keys > 0 ? max_keys : AWS_MaxKeys), AWS_MaxKeys);

  constexpr int kMaxRetries = 3;
  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    *out = {};
    auto cached = get_bucket_id_cached(tenant_id, bname);
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
    const std::string v_domain(v_dom.view());
    const std::string v_end = prefix_range_end(v_domain);
    const std::string v_begin = last_v.empty() ? v_domain : last_v;

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

    auto f_bkt = issue_bucket_get(*tr, tenant_id, bname);
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

    auto vb = resolve_bucket_verify(tenant_id, bname, f_bkt, bucket_id,
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
                                               std::string_view key,
                                               const DeleteCondition *cond,
                                               DeleteResult *out)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kDeleteObject);
  ops_stats_.inc(OpType::kDeleteObject);
  const std::string bname(bucket_name);
  const std::string object_name(key);
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

    auto result = delete_single(*tr, tenant_id, bname, object_name, cond);
    if (!result) {
      return result.error();
    }

    auto rc = tr->commit();
    if (rc) {
      if (out) {
        *out = *result;
      }
      return KVRGW_ERR_OK;
    }
    auto ec = fdb_to_error(rc.error());
    if (!is_retriable(ec)) {
      return ec;
    }
    sleep_for_msec(10 * (attempt + 1));
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

//--------------------------------------------------------------------------------
KvrgwErrorCode KvRgwServiceImpl::put_object_tagging(
    tenant_id_t tenant_id, std::string_view bucket_name, std::string_view key,
    std::span<const uint8_t> tags)
{
  ScopedRequestLatency _lat(latency_stats_, OpType::kPutObjectTagging);
  ops_stats_.inc(OpType::kPutObjectTagging);
  const std::string bname(bucket_name);
  auto cached = get_bucket_id_cached(tenant_id, bname);
  if (!cached) {
    return fdb_to_error(cached.error());
  }
  if (!*cached) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  if (auto ac = check_access(tenant_id, bname, **cached, kDenyWrite);
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

    auto f_bkt = issue_bucket_get(*tr, tenant_id, bname);
    auto f_obj = tr->kv_async_get(object_key.view());

    auto vb = resolve_bucket_verify(tenant_id, bname, f_bkt, bucket_id,
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

    const std::string_view ref_tag_sv(
        reinterpret_cast<const char *>(value->hdr.ref_tag), kRefTagSize);

    if (tags.empty()) {
      clear_object_tags(*value, *tr, bucket_id, ref_tag_sv);
    }
    else {
      apply_tags_to_value(*value, tags, *tr, bucket_id, ref_tag_sv);
    }

    OValueBuf buf;
    if (!write_object_value(buf, *value)) {
      return KVRGW_ERR_INTERNAL;
    }
    tr->kv_put(object_key.view(), buf.view());

    auto rc = tr->commit();
    if (rc) {
      return KVRGW_ERR_OK;
    }
    {
      auto ec = fdb_to_error(rc.error());
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
  const std::string bname(bucket_name);
  auto cached = get_bucket_id_cached(tenant_id, bname);
  if (!cached) {
    return fdb_to_error(cached.error());
  }
  if (!*cached) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  if (auto ac = check_access(tenant_id, bname, **cached, kDenyRead);
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

  const std::string_view ref_tag_sv(
      reinterpret_cast<const char *>(value->hdr.ref_tag), kRefTagSize);
  KeyBuf ct_key;
  make_ct_key(bucket_id, ref_tag_sv, ct_key);
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
  if (!decode(std::span<const uint8_t>(live.data(), live.size()), out_tags)) {
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
  const std::string bname(bucket_name);
  auto cached = get_bucket_id_cached(tenant_id, bname);
  if (!cached) {
    return fdb_to_error(cached.error());
  }
  if (!*cached) {
    return KVRGW_ERR_NO_SUCH_BUCKET;
  }
  if (auto ac = check_access(tenant_id, bname, **cached, kDenyWrite);
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

    auto f_bkt = issue_bucket_get(*tr, tenant_id, bname);
    auto f_obj = tr->kv_async_get(object_key.view());

    auto vb = resolve_bucket_verify(tenant_id, bname, f_bkt, bucket_id,
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

    const std::string_view ref_tag_sv(
        reinterpret_cast<const char *>(value->hdr.ref_tag), kRefTagSize);
    clear_object_tags(*value, *tr, bucket_id, ref_tag_sv);
    OValueBuf buf;
    if (!write_object_value(buf, *value)) {
      return KVRGW_ERR_INTERNAL;
    }
    tr->kv_put(object_key.view(), buf.view());

    auto rc = tr->commit();
    if (rc) {
      return KVRGW_ERR_OK;
    }
    {
      auto ec = fdb_to_error(rc.error());
      if (!is_retriable(ec)) {
        return ec;
      }
    }
    sleep_for_msec(10 * (attempt + 1));
  }
  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

} // namespace kvrgw
