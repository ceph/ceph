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

uint64_t blob_bytes_for(const DataStore &data_store, std::string_view ref_tag)
{
  const auto path = data_store.path_for(ref_tag);
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

  auto rows = store_.range_scan(prefix.view(), end, kGcMaxKeys);
  if (!rows) {
    return;
  }

  for (const auto &row : *rows) {
    if (stop_flag_) {
      return;
    }

    config_.maybe_apply_pending();
    const GcPolicy entry_policy = config_.active_policy_copy();
    if (entry_policy.suspended) {
      return;
    }

    const auto parts = parse_go_key(row.key);
    if (!parts) {
      const auto group_parts = parse_group_go_key(row.key);
      if (!group_parts) {
        continue;
      }

      const auto group_val = parse_group_gc_value(row.value);
      if (!group_val) {
        continue;
      }

      for (uint8_t i = 0; i < group_val->count; ++i) {
        const auto &ge = group_val->entries[i];
        if (!rate_allow(rate, entry_policy, ge.object_size)) {
          return;
        }
        (void)data_store_.remove(ref_tag_view(ge.ref_tag));
      }
      auto tr = store_.begin_transaction();
      if (!tr) {
        continue;
      }
      (*tr)->kv_del(row.key);
      (*tr)->commit();
      continue;
    }

    const auto gc_val = parse_gc_value(row.value);
    const ChunkType ct = gc_val ? gc_val->hdr.chunk.type : CHUNK_STORAGE;

    const bool shared =
        gc_val && (gc_val->hdr.flags & ObjectValue::kFlagSharedData);

    const bool has_external_children =
        gc_val && (gc_val->hdr.flags & kFlagExternalTags);
    const auto rt_view = ref_tag_view(parts->ref_tag);
    auto clean_children = [&](KvTransaction &txn) {
      if (!has_external_children) {
        return;
      }
      auto c_prefix = make_c_prefix(parts->bucket_id, rt_view);
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
        auto r_val = (*tr)->kv_get(r_key.view());
        if (r_val && *r_val) {
          auto rv = parse_r_value(**r_val);
          if (!rv) {
            continue;
          }
          if (rv->ref_count > 1) {
            (*tr)->kv_put(r_key.view(),
                          write_r_value(rv->ref_count - 1, rv->chunk_descriptor));
            (*tr)->kv_del(row.key);
            (*tr)->commit();
            continue;
          }
          (*tr)->kv_del(r_key.view());
        }
        (void)data_store_.remove(rt_view);
        (*tr)->kv_del(row.key);
        clean_children(**tr);
        (*tr)->commit();
      }
      else {
        const uint64_t blob_bytes = blob_bytes_for(data_store_, rt_view);
        if (!rate_allow(rate, entry_policy, blob_bytes)) {
          return;
        }
        if (data_store_.remove(rt_view)) {
          continue;
        }
        auto tr = store_.begin_transaction();
        if (!tr) {
          continue;
        }
        (*tr)->kv_del(row.key);
        clean_children(**tr);
        (*tr)->commit();
      }
    }
    else if (ct == CHUNK_CHILD_D || ct == CHUNK_CHILD_D_REF) {
      if (!rate_allow(rate, entry_policy, 0)) {
        return;
      }
      const uint8_t st = d_size_tier_from_size(gc_val->hdr.object_size);
      const auto d_key =
          make_d_key(parts->bucket_id, st, rt_view, gc_val->hdr.mtime);
      auto tr = store_.begin_transaction();
      if (!tr) {
        continue;
      }
      decrement_or_del_child_d(**tr, d_key.view(), gc_val->hdr.object_size,
                               shared);
      (*tr)->kv_del(row.key);
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
      (*tr)->kv_del(row.key);
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
