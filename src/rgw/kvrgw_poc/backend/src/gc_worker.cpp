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

#include "gc_worker.hpp"

#include "constants.hpp"
#include "gc_value.hpp"
#include "keys.hpp"
#include "ref_count.hpp"
#include "ref_tag.hpp"

#include <arpa/inet.h>
#include <cstring>
#include <filesystem>
#include <thread>

namespace kvrgw {

namespace {

uint64_t blob_bytes_for(const DataStore &data_store, std::string_view filename)
{
  const auto path = data_store.path_for(filename);
  std::error_code ec;
  if (!std::filesystem::exists(path, ec) || ec) {
    return 0;
  }
  const auto size = std::filesystem::file_size(path, ec);
  if (ec) {
    return 0;
  }
  return size;
}

} // namespace

GcWorker::GcWorker(KvStore &store, DataStore &data_store, GcConfigState &config,
                   std::atomic<bool> &stop_flag)
    : store_(store), data_store_(data_store), config_(config),
      stop_flag_(stop_flag)
{
}

void GcWorker::sleep_ms(int ms)
{
  for (int i = 0; i < ms && !stop_flag_; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

bool GcWorker::rate_allow(RateWindow *rate, const GcPolicy &policy,
                          uint64_t blob_bytes)
{
  const auto now = std::chrono::steady_clock::now();
  if (rate->window_start.time_since_epoch().count() == 0 ||
      now - rate->window_start >= std::chrono::seconds(1)) {
    rate->window_start = now;
    rate->objects = 0;
    rate->bytes = 0;
  }

  if (policy.max_objects_per_sec > 0 &&
      rate->objects >= policy.max_objects_per_sec) {
    return false;
  }
  const uint64_t max_bytes =
      policy.max_mb_per_sec > 0
          ? static_cast<uint64_t>(policy.max_mb_per_sec) * 1024ULL * 1024ULL
          : 0;
  if (max_bytes > 0 && rate->bytes + blob_bytes > max_bytes) {
    return false;
  }

  rate->objects += 1;
  rate->bytes += blob_bytes;
  return true;
}

void GcWorker::gc_once(const GcPolicy &policy, RateWindow *rate)
{
  const auto prefix = make_g_prefix();
  const auto end = [&prefix]() {
    std::string out(prefix.view());
    while (!out.empty()) {
      const unsigned char last = static_cast<unsigned char>(out.back());
      if (last < 0xFF) {
        out.back() = static_cast<char>(last + 1);
        return out;
      }
      out.pop_back();
    }
    return std::string("\xFF", 1);
  }();

  auto scan_tr = store_.begin_transaction();
  if (!scan_tr) {
    return;
  }
  // FdbRangeHolder owns the underlying FdbFuture (not scan_tr), so the
  // key/value views below remain valid for the lifetime of `holder`,
  // independent of scan_tr's state — safe to use across this entire loop
  // even though each iteration opens its own separate transaction for the
  // actual mutation/commit.
  FdbRangeHolder holder((*scan_tr)->kv_async_get_range_holder(
      prefix.view(), /*exclusive_begin=*/false, end, kGcMaxKeys));
  if (holder.wait()) {
    return;
  }

  GcPolicy entry_policy = config_.active_policy_copy();
  for (const auto &[key, value] : holder) {
    if (stop_flag_) {
      return;
    }

    if (config_.maybe_apply_pending()) {
      entry_policy = config_.active_policy_copy();
      if (entry_policy.suspended) {
        return;
      }
    }

    const auto parts = parse_go_key(key);
    if (!parts) {
      const auto group_parts = parse_group_go_key(key);
      if (!group_parts) {
        continue;
      }

      GroupGcValue group_val;
      if (!parse_group_gc_value(value, group_val)) {
        continue;
      }

      for (auto i = 0; i < group_val.count; ++i) {
        const auto &ge = group_val.entries[i];
        if (!rate_allow(rate, entry_policy, ge.object_size)) {
          return;
        }
        (void)data_store_.remove(ge.chunk.ref_tag_filename());
      }
      auto tr = store_.begin_transaction();
      if (!tr) {
        continue;
      }
      (*tr)->kv_del(key);
      (*tr)->commit();
      continue;
    }

    GcValue gc_val;
    if (!parse_gc_value(value, gc_val)) {
      continue;
    }
    const ChunkType ct         = gc_val.gc.chunk.get_type();
    const bool shared          = gc_val.hdr.flags & ObjectValue::kFlagSharedData;
    const bool has_external_children = gc_val.hdr.flags & kFlagExternalTags;
    const auto rt_view         = gc_val.gc.chunk.ref_tag_view();
    const auto rt_filename     = gc_val.gc.chunk.ref_tag_filename();
    const auto& bucket_id      = gc_val.gc.chunk.get_bucket_id();

    auto clean_children = [&](KvTransaction &txn) {
      if (!has_external_children) {
        return;
      }
      KeyBuf c_prefix;
      make_c_prefix(bucket_id, rt_view, c_prefix);
      auto c_end = c_prefix;
      c_end.append_byte(0xFF);
      txn.kv_range_clear(c_prefix.view(), c_end.view());
    };

    if (ct == CHUNK_STORAGE || ct == CHUNK_STORAGE_REF) {
      if (shared) {
        const auto r_key = make_r_key(rt_view);
        auto tr = store_.begin_transaction();
        if (!tr) {
          continue;
        }
        FdbGetHolder r_holder((*tr)->kv_async_get_holder(r_key.view()));
        if (!r_holder.wait() && r_holder.present()) {
          auto rv = parse_r_value(r_holder.value());
          if (!rv) {
            continue;
          }
          if (rv->ref_count > 1) {
            (*tr)->kv_put(r_key.view(),
                          write_r_value(rv->ref_count - 1, rv->chunk_descriptor));
            (*tr)->kv_del(key);
            (*tr)->commit();
            continue;
          }
          (*tr)->kv_del(r_key.view());
        }
        (void)data_store_.remove(rt_filename);
        (*tr)->kv_del(key);
        clean_children(**tr);
        (*tr)->commit();
      }
      else {
        const uint64_t blob_bytes = blob_bytes_for(data_store_, rt_filename);
        if (!rate_allow(rate, entry_policy, blob_bytes)) {
          return;
        }
        if (data_store_.remove(rt_filename)) {
          continue;
        }
        auto tr = store_.begin_transaction();
        if (!tr) {
          continue;
        }
        (*tr)->kv_del(key);
        clean_children(**tr);
        (*tr)->commit();
      }
    }
    else if (ct == CHUNK_CHILD_D || ct == CHUNK_CHILD_D_REF) {
      if (!rate_allow(rate, entry_policy, 0)) {
        return;
      }
      const uint8_t st = d_size_tier_from_size(gc_val.gc.object_size);
      KeyBuf d_key;
      make_d_key(bucket_id, st, rt_view, gc_val.gc.mtime, d_key);
      auto tr = store_.begin_transaction();
      if (!tr) {
        continue;
      }
      decrement_or_del_child_d(**tr, d_key.view(), gc_val.gc.object_size,
                               shared);
      (*tr)->kv_del(key);
      clean_children(**tr);
      (*tr)->commit();
    }
    else {
      if (!rate_allow(rate, entry_policy, 0)) {
        return;
      }
      auto tr = store_.begin_transaction();
      if (!tr) {
        continue;
      }
      (*tr)->kv_del(key);
      clean_children(**tr);
      (*tr)->commit();
    }
  }
}

void GcWorker::run()
{
  RateWindow rate;
  while (!stop_flag_) {
    config_.maybe_apply_pending();
    GcPolicy policy = config_.active_policy_copy();
    if (!policy.suspended) {
      gc_once(policy, &rate);
    }

    const int interval_ms =
        policy.suspended ? 100 : std::max(policy.interval_sec, 0) * 1000;
    for (int slept = 0; slept < interval_ms && !stop_flag_; slept += 100) {
      config_.maybe_apply_pending();
      const GcPolicy updated = config_.active_policy_copy();
      if (updated.suspended != policy.suspended ||
          updated.interval_sec != policy.interval_sec ||
          updated.max_objects_per_sec != policy.max_objects_per_sec ||
          updated.max_mb_per_sec != policy.max_mb_per_sec) {
        break;
      }
      sleep_ms(100);
    }
  }
}

} // namespace kvrgw
