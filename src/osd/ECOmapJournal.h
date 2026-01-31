// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

/*
 * ECOmapJournal
 *
 * An in-memory journal to maintain OMAP consistency for Erasure Coded pools.
 *
 * Background:
 * Unlike Replicated pools which use a "roll-forward" recovery model, EC pools rely on a
 * "roll-back" mechanism for interrupted updates. To support efficient rollback without
 * incurring expensive read-before-write operations to generate rollback info, EC OMAP
 * updates are initially recorded only in the PG Log. They are applied to the underlying
 * ObjectStore only after the transaction is fully committed on all shards.
 *
 * The Problem:
 * This deferred application creates a latency window where the authoritative state exists
 * in the PG Log but the ObjectStore contains stale data. Reads served during this window
 * would return incorrect results.
 *
 * The Solution:
 * An ECOmapJournal tracks these "log-only" updates in memory. When an OMAP read occurs,
 * the backend fetches the base state from the ObjectStore and supplements it with the
 * updates stored in this journal. This ensures clients always receive the most up-to-date
 * result, merging the persistent state with the in-flight log state.
 */

#pragma once

#include <map>
#include <optional>
#include <utility>
#include <vector>

#include "include/buffer.h"

#include "osd_types.h"

struct eversion_t;

class ECOmapJournalEntry {
 public:
  eversion_t version;
  bool clear_omap;
  std::optional<ceph::buffer::list> omap_header;
  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> omap_updates;

  ECOmapJournalEntry(
    eversion_t version, bool clear_omap, std::optional<ceph::buffer::list> omap_header,
    std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> omap_updates);
  bool operator==(const ECOmapJournalEntry& other) const;
};

class ECOmapValue {
 public:
  eversion_t version;
  std::optional<ceph::buffer::list> value;

  ECOmapValue(
    const eversion_t version,
    const std::optional<ceph::buffer::list> &value)
    : version(version), value(value) {}

  void update_value(eversion_t new_version, std::optional<ceph::buffer::list> new_value);
};

class ECOmapRemovedRanges {
 public:
  eversion_t version;
  std::list<std::pair<std::string, std::optional<std::string>>> ranges;

  explicit ECOmapRemovedRanges(const eversion_t version) : version(version) {}
  ECOmapRemovedRanges(
    const eversion_t version, std::list<std::pair<std::string,
    std::optional<std::string>>> ranges)
    : version(version), ranges(std::move(ranges)) {}

  void add_range(const std::string& start, const std::optional<std::string>& end);
  void clear_omap();
};

class ECOmapHeader {
 public:
  eversion_t version = eversion_t();
  std::optional<ceph::buffer::list> header = std::nullopt;

  ECOmapHeader(const eversion_t version, std::optional<ceph::buffer::list> header)
    : version(version), header(std::move(header)) {}
  ECOmapHeader() = default;

  void update_header(eversion_t new_version, std::optional<ceph::buffer::list> new_header);
};

class ECOmapJournal {
  using UpdateMapType = std::map<std::string, ECOmapValue>;
  using RangeMapType = std::map<std::string, std::optional<std::string>>;
  using const_iterator = std::list<ECOmapJournalEntry>::const_iterator;
 private:
  // Unprocessed journal entries 
  std::map<hobject_t, std::list<ECOmapJournalEntry>> entries;

  // Processed journal entries
  std::map<hobject_t, std::map<std::string, ECOmapValue>> key_map;
  std::map<hobject_t, std::list<ECOmapRemovedRanges>> removed_ranges_map;
  std::map<hobject_t, ECOmapHeader> header_map;

  // Function to get specific object's unprocessed entries
  std::list<ECOmapJournalEntry>& get_entries(const hobject_t &hoid);
  std::list<ECOmapJournalEntry> snapshot_entries(const hobject_t &hoid) const;

  using iterator = std::list<ECOmapJournalEntry>::iterator;
  iterator begin_entries_mutable(const hobject_t &hoid);
  iterator end_entries_mutable(const hobject_t &hoid);
  void process_entries(const hobject_t &hoid);
  bool remove_processed_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry);
  bool remove_processed_entry_by_version(const hobject_t &hoid, const eversion_t version);
  UpdateMapType get_key_map(const hobject_t &hoid);
  RangeMapType get_removed_ranges(const hobject_t &hoid);

 public:
  void add_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry);
  bool remove_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry);
  bool remove_entry_by_version(const hobject_t &hoid, const eversion_t version);
  void clear(const hobject_t &hoid);
  void clear_all();
  [[nodiscard]] std::size_t entries_size(const hobject_t &hoid) const;
  std::tuple<UpdateMapType, RangeMapType> get_value_updates(const hobject_t &hoid);
  std::optional<ceph::buffer::list> get_updated_header(const hobject_t &hoid);

  const_iterator begin_entries(const hobject_t &hoid);
  const_iterator end_entries(const hobject_t &hoid);
};
