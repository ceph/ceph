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
#include "multipart_value.hpp"
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

bool GcWorker::process_gu_entry(const GuKeyParts &parts, std::string_view value,
                                RateWindow *rate, const GcPolicy &policy)
{
  auto gu = parse_gu_value(value);
  if (!gu) {
    return true;  // corrupt value -- skip, mirrors G:O's malformed-value handling
  }

  for (uint8_t i = 0; i < gu->count; ++i) {
    const auto &e = gu->entries[i];
    const std::string fname =
        part_filename(parts.ref_tag, e.part_number, e.generation);
    if (fname.empty()) {
      continue;
    }
    const uint64_t blob_bytes = blob_bytes_for(data_store_, fname);
    if (!rate_allow(rate, policy, blob_bytes)) {
      return false;  // rate-limited -- key left in place, retried next cycle
    }
    (void)data_store_.remove(fname);
  }

  KeyBuf gu_key;
  make_gu_key(parts.bucket_id, parts.ref_tag.view(), parts.part_number,
             parts.generation, gu_key);
  auto tr = store_.begin_transaction();
  if (!tr) {
    return false;
  }
  (*tr)->kv_del(gu_key.view());
  (*tr)->commit();
  return true;
}

bool GcWorker::process_gm_entry(const GmKeyParts &parts, std::string_view key,
                                std::string_view value, RateWindow *rate,
                                const GcPolicy &policy)
{
  auto gm = parse_gm_value(value);
  if (!gm) {
    return true;  // corrupt value -- skip, mirrors G:O's malformed-value handling
  }

  // FREE_PARTS -- the completed CHUNK_MULTIPART :O: was deleted/overwritten
  // after Complete's own G:M{MIXED} pass already finished (no S:U rows for
  // this ref_tag remain -- the manifest carried on this entry is the only
  // remaining record of which part files are still on disk). Decode it and
  // free every (part_number, generation) file directly; no S:U scan, no
  // clearRange -- just delete this G:M key once the whole list is freed.
  if (gm->mode == GM_MODE_FREE_PARTS) {
    auto decoded = decode_manifest_body(gm->body);
    if (!decoded) {
      return true;  // corrupt body -- skip, same as a top-level parse failure
    }
    for (size_t i = 0; i < decoded->included_parts.size(); ++i) {
      const std::string fname =
          part_filename(parts.ref_tag, decoded->included_parts[i], decoded->generations[i]);
      if (fname.empty()) {
        continue;
      }
      const uint64_t blob_bytes = blob_bytes_for(data_store_, fname);
      if (!rate_allow(rate, policy, blob_bytes)) {
        return false;  // rate-limited -- key left in place, retried next cycle
                        // (idempotent: already-removed files are simply
                        // skipped by blob_bytes_for/remove)
      }
      (void)data_store_.remove(fname);
    }
    auto tr = store_.begin_transaction();
    if (!tr) {
      return false;
    }
    (*tr)->kv_del(key);
    (*tr)->commit();
    return true;
  }

  const std::string_view rt_view = parts.ref_tag.view();
  KeyBuf u_prefix;
  make_u_prefix(parts.bucket_id, rt_view, u_prefix);
  const std::string scan_end = prefix_range_end(u_prefix.view());

  // Incremental decode -- walks Knob 1 (membership) + Knob 2 (deviating
  // generation) in lockstep with the S:U scan below, which already visits
  // every row in ascending part_number order. Never materializes a full
  // keep-list.
  ManifestKeepCursor keep_cursor(gm->mode == GM_MODE_MIXED ? gm->body : std::string_view{});
  if (gm->mode == GM_MODE_MIXED && !keep_cursor.valid()) {
    return true;  // corrupt body -- skip this cycle, same as a top-level parse failure
  }

  // DEEP (empty keep_list): free every S:U row's data individually.
  // MIXED: free every row NOT in the keep-list -- the ones chosen for the
  // final object stay on disk, referenced going forward only by the
  // :O: entry's ChunkDescriptor, never again by this G:M key.
  std::string scan_pos(u_prefix.view());
  bool exclusive = false;
  for (;;) {
    if (stop_flag_) {
      return false;
    }
    auto scan_tr = store_.begin_transaction();
    if (!scan_tr) {
      return false;
    }
    FdbRangeHolder holder((*scan_tr)->kv_async_get_range_holder(
        scan_pos, exclusive, scan_end, kGcMaxKeys));
    if (holder.wait()) {
      return false;
    }
    if (holder.count() == 0) {
      break;
    }
    exclusive = true;
    const bool more = holder.more();
    std::string next_pos(holder.back().key);

    for (const auto &[row_key, row_value] : holder) {
      auto u_parts = parse_u_key(row_key);
      if (!u_parts) {
        continue;
      }
      const bool kept = gm->mode == GM_MODE_MIXED &&
          keep_cursor.is_kept(u_parts->part_number, u_parts->generation);
      if (kept) {
        continue;
      }
      const std::string fname = part_filename(
          parts.ref_tag, u_parts->part_number, u_parts->generation);
      if (fname.empty()) {
        continue;
      }
      const uint64_t blob_bytes = blob_bytes_for(data_store_, fname);
      if (!rate_allow(rate, policy, blob_bytes)) {
        return false;  // rate-limited -- key left in place, rescans from the
                        // top next cycle (idempotent: already-removed files
                        // are simply skipped by blob_bytes_for/remove)
      }
      (void)data_store_.remove(fname);
    }

    if (!more) {
      break;
    }
    scan_pos.assign(next_pos);
  }

  // All out-of-keep-list data is freed -- one clearRange covers every
  // S:U row (kept or not; kept rows' KV entries are no longer needed once
  // the final object's own :O: entry exists) plus the G:M key itself.
  auto tr = store_.begin_transaction();
  if (!tr) {
    return false;
  }
  (*tr)->kv_range_clear(u_prefix.view(), scan_end);
  (*tr)->kv_del(key);
  (*tr)->commit();
  return true;
}

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

    if (const auto gu_parts = parse_gu_key(key)) {
      if (!process_gu_entry(*gu_parts, value, rate, entry_policy)) {
        return;
      }
      continue;
    }
    if (const auto gm_parts = parse_gm_key(key)) {
      if (!process_gm_entry(*gm_parts, key, value, rate, entry_policy)) {
        return;
      }
      continue;
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

    if (ct == CHUNK_STORAGE || ct == CHUNK_STORAGE_REF ||
        ct == CHUNK_MULTIPART || ct == CHUNK_MULTIPART_REF) {
      const bool is_multipart = (ct == CHUNK_MULTIPART || ct == CHUNK_MULTIPART_REF);
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
          // Truly last reference.
          if (is_multipart) {
            // rv->chunk_descriptor holds the manifest body verbatim (see
            // copy_object's CHUNK_MULTIPART/_REF case) -- hand off to the
            // existing rate-limited GM_MODE_FREE_PARTS free loop
            // (process_gm_entry) instead of duplicating a per-part free
            // loop here.
            KeyBuf gm_key;
            make_gm_key(bucket_id, rt_view, gm_key);
            GmValueBuf gm_buf;
            gm_buf.data[0] = static_cast<uint8_t>(GM_MODE_FREE_PARTS);
            std::memcpy(gm_buf.data + 1, rv->chunk_descriptor.data(),
                       rv->chunk_descriptor.size());
            gm_buf.len = 1 + rv->chunk_descriptor.size();
            (*tr)->kv_put(gm_key.view(), gm_buf.view());
            (*tr)->kv_del(r_key.view());
            (*tr)->kv_del(key);
            (*tr)->commit();
            continue;
          }
          (*tr)->kv_del(r_key.view());
        }
        if (!is_multipart) {
          (void)data_store_.remove(rt_filename);
        }
        (*tr)->kv_del(key);
        clean_children(**tr);
        (*tr)->commit();
      }
      else if (is_multipart) {
        // Shouldn't occur -- an unshared CHUNK_MULTIPART is freed directly
        // by move_object_to_g's own G:M{FREE_PARTS} hand-off and never
        // creates a G:O entry in the first place. Defensive cleanup only.
        auto tr = store_.begin_transaction();
        if (!tr) {
          continue;
        }
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
