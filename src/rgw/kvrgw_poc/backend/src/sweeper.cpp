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
#include "gc_value.hpp"
#include "keys.hpp"
#include "ref_tag.hpp"

#include <chrono>
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
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
}

void Sweeper::sweep_once()
{
  const auto prefix = make_p_prefix(kShardCount, kShardId);
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

  auto rows = store_.range_scan(prefix.view(), end, kSweeperMaxKeys);
  if (!rows) {
    return;
  }

  const int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

  for (const auto &row : *rows) {
    const auto parts = parse_po_key(row.key);
    if (parts) {
      const auto po_value = parse_po_value(row.value);
      if (!po_value) {
        continue;
      }
      if (now - po_value->hdr.created_at_unix < min_age_sec_) {
        continue;
      }

      auto tr = store_.begin_transaction();
      if (!tr) {
        continue;
      }
      auto rc = move_po_to_go(**tr, *parts, *po_value);
      if (!rc) {
        continue;
      }
      auto commit_rc = (*tr)->commit();
      if (!commit_rc) {
        std::cerr << "sweeper: commit failed: "
                  << fdb_get_error(commit_rc.error()) << std::endl;
      }
      continue;
    }

    const auto group_parts = parse_group_po_key(row.key);
    if (group_parts) {
      const auto group_val = parse_group_po_value(row.value);
      if (!group_val) {
        continue;
      }
      if (now - group_val->created_at_unix < min_age_sec_) {
        continue;
      }

      auto tr = store_.begin_transaction();
      if (!tr) {
        continue;
      }

      auto po_exists = (*tr)->kv_get(row.key);
      if (!po_exists || !*po_exists) {
        continue;
      }

      // Build group G:O value from group P:O entries.
      // flags=0: Phase 3 never committed → S:O never written → no shared data,
      // no external tags (tagged PUTs bypass batch), no annotations (per AWS
      // spec, PutObjectAnnotation targets committed objects only).
      GroupGcEntry gc_entries[kMaxBatchSize];
      for (uint8_t i = 0; i < group_val->count; ++i) {
        gc_entries[i].ref_tag = group_val->entries[i].ref_tag;
        gc_entries[i].chunk = CHUNK_STORAGE;
        gc_entries[i].flags = 0;
        gc_entries[i].object_size = group_val->entries[i].object_size;
      }

      const auto go_key = make_group_go_key(
          group_parts->bucket_id, ref_tag_view(group_parts->group_ref_tag));
      (*tr)->kv_put(go_key.view(),
                    make_group_gc_value(gc_entries, group_val->count));
      (*tr)->kv_del(row.key);

      auto commit_rc = (*tr)->commit();
      if (!commit_rc) {
        std::cerr << "sweeper: group commit failed: "
                  << fdb_get_error(commit_rc.error()) << std::endl;
      }
    }
  }
}

void Sweeper::process_bucket(bucket_id_t bucket_id, bool force)
{
  const auto prefix = make_p_bucket_prefix(bucket_id);
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

  auto rows = store_.range_scan(prefix.view(), end, kSweeperMaxKeys);
  if (!rows) {
    return;
  }

  const int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();

  for (const auto &row : *rows) {
    const auto parts = parse_po_key(row.key);
    if (!parts) {
      continue;
    }
    const auto po_value = parse_po_value(row.value);
    if (!po_value) {
      continue;
    }
    if (!force && now - po_value->hdr.created_at_unix < min_age_sec_) {
      continue;
    }

    auto tr = store_.begin_transaction();
    if (!tr) {
      continue;
    }
    auto rc = move_po_to_go(**tr, *parts, *po_value);
    if (!rc) {
      continue;
    }
    (*tr)->commit();
  }
}

} // namespace kvrgw
