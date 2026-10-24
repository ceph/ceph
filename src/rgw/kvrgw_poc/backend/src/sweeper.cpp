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

#include "sweeper.hpp"

#include "constants.hpp"
#include "fdb_latency.hpp"
#include "gc_value.hpp"
#include "keys.hpp"
#include "object_value.hpp"
#include "ref_tag.hpp"

#include <iostream>
#include <thread>

namespace kvrgw {

Sweeper::Sweeper(KvStore &store, std::atomic<bool> &stop_flag, int interval_sec,
                 int min_age_sec)
    : store_(store), stop_flag_(stop_flag), interval_sec_(interval_sec),
      min_age_sec_(min_age_sec)
{
}

void Sweeper::run()
{
  while (!stop_flag_) {
    sweep_once();
    for (int i = 0; i < interval_sec_ * 10 && !stop_flag_; ++i) {
      sleep_for_msec(100);
    }
  }
}

// Computes the exclusive upper bound of a prefix scan: the smallest key
// strictly greater than every key that starts with `prefix`.
std::string Sweeper::compute_exclusive_end(std::string_view prefix)
{
  std::string out(prefix);
  while (!out.empty()) {
    const unsigned char last = static_cast<unsigned char>(out.back());
    if (last < 0xFF) {
      out.back() = static_cast<char>(last + 1);
      return out;
    }
    out.pop_back();
  }
  return std::string("\xFF", 1);
}

void Sweeper::sweep_once()
{
  const auto prefix = make_p_prefix();
  sweep_range(prefix.view(), compute_exclusive_end(prefix.view()), /*force=*/false);
}

void Sweeper::process_bucket(bucket_id_t bucket_id, bool force)
{
  const auto prefix = make_p_bucket_prefix(bucket_id);
  sweep_range(prefix.view(), compute_exclusive_end(prefix.view()), force);
}

// Scans [prefix, end) of the P: namespace and promotes aged-out entries to
// the G: (GC) namespace. Handles both single P:O keys and Group P:O keys
// (the latter is structurally unreachable when `prefix` is a per-bucket
// KeyHeaderPO prefix, since Group P:O keys carry no bucket_id — see
// process_bucket()/make_p_bucket_prefix()).
void Sweeper::sweep_range(std::string_view prefix, std::string_view end, bool force)
{
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
      prefix, /*exclusive_begin=*/false, end, kSweeperMaxKeys));
  if (holder.wait()) {
    return;
  }

  const int64_t now = now_unix();

  for (const auto &[key, value] : holder) {
    // Single P:O key: [ns='P'][bucket_id 8B][cat='O'][ref_tag 12B] = 22B
    const auto parts = parse_po_key(key);
    if (parts) {
      const auto po_entry = parse_po_value_view(value);
      if (!po_entry) {
        continue;
      }
      if (!force && now - po_entry->gc.mtime < min_age_sec_) {
        continue;
      }
      // TBD - move up-to kMaxBatchSize entries into a single Group GC
      auto tr = store_.begin_transaction();
      if (!tr) {
        continue;
      }
      if (move_po_to_go(**tr, *parts, po_entry->gc, po_entry->object_name) != 0) {
        continue;
      }
      auto commit_rc = (*tr)->commit();
      if (!commit_rc) {
        std::cerr << "sweeper: commit failed: "
                  << fdb_get_error(commit_rc.error()) << std::endl;
      }
      continue;
    }

    process_group_po_entry(key, value, now);
  }
}

// Handles one Group P:O row: checks liveness of each grouped entry via
// async S:O/R: reads, then promotes survivors to an individual G:O key or
// a grouped G:G key, or deletes the Group P:O key outright if nothing
// survived. No-op if `key` doesn't match the Group P:O pattern.
void Sweeper::process_group_po_entry(std::string_view key, std::string_view value,
                                      int64_t now)
{
  // Group P:O key: [ns='P'][cat='G'][ref_tag_0 12B] = 14B
  if (!parse_group_po_key(key)) {
    return;
  }
  const auto group_val = parse_group_po_value_view(value);
  if (!group_val || group_val->count == 0) {
    return;
  }
  // Age check: mtime stored in first entry's GcDescriptor
  if (now - group_val->entries[0].gc.mtime < min_age_sec_) {
    return;
  }

  auto tr = store_.begin_transaction();
  if (!tr) {
    return;
  }

  // Issue async reads for each entry: S:O key and R: key
  struct EntryFutures {
    FdbFuture f_so;
    FdbFuture f_r;
  };
  EntryFutures futures[kMaxGroupPoSize];
  for (uint8_t i = 0; i < group_val->count; ++i) {
    const auto& entry = group_val->entries[i];
    KeyBuf so_key;
    make_object_key(entry.gc.chunk.get_bucket_id(), entry.object_name, so_key);
    futures[i].f_so = (*tr)->kv_async_get(so_key.view());
    KeyBuf r_key = make_r_key(entry.gc.chunk.ref_tag_view());
    futures[i].f_r = (*tr)->kv_async_get(r_key.view());
  }

  // Collect entries that need GC
  GcDescriptor gc_entries[kMaxGroupPoSize];
  uint8_t gc_count = 0;

  for (uint8_t i = 0; i < group_val->count; ++i) {
    const auto& entry = group_val->entries[i];

    FdbGetHolder so_holder(std::move(futures[i].f_so));
    if (so_holder.wait()) {
      continue; // FDB error — skip entry
    }
    if (so_holder.present()) {
      // S:O exists — check if ref_tag matches
      const auto* ovh = ovh_ptr(so_holder.value());
      if (ovh && ovh->ref_tag == entry.gc.chunk.get_ref_tag()) {
        continue; // still alive — skip
      }
    }

    FdbGetHolder r_holder(std::move(futures[i].f_r));
    if (r_holder.wait()) {
      continue; // FDB error — skip entry
    }
    if (r_holder.present()) {
      continue; // R: entry exists — shared object, skip
    }

    gc_entries[gc_count++] = entry.gc;
  }

  if (gc_count == 0) {
    // Nothing to GC — just delete the Group P:O key
    (*tr)->kv_del(key);
  } else if (gc_count == 1) {
    // Single entry: write individual G:O
    KeyBuf go_key;
    make_go_key(kShardCount, kShardId,
                gc_entries[0].chunk.get_bucket_id(),
                gc_entries[0].chunk.ref_tag_view(),
                gc_entries[0].object_size,
                go_key);
    GcValueHeader gc_hdr{};
    gc_hdr.chunk_type = CHUNK_STORAGE;
    GcValueBuf gc_buf;
    make_gc_value(gc_buf, gc_hdr, gc_entries[0]);
    (*tr)->kv_put(go_key.view(), gc_buf.view());
    (*tr)->kv_del(key);
  } else {
    // Multiple entries: write Group G:G
    const auto go_key = make_group_go_key(gc_entries[0].chunk.ref_tag_view());
    GroupGcValueBuf go_buf;
    make_group_gc_value(go_buf, gc_entries, gc_count);
    (*tr)->kv_put(go_key.view(), go_buf.view());
    (*tr)->kv_del(key);
  }

  auto commit_rc = (*tr)->commit();
  if (!commit_rc) {
    std::cerr << "sweeper: group commit failed: "
              << fdb_get_error(commit_rc.error()) << std::endl;
  }
}

} // namespace kvrgw
