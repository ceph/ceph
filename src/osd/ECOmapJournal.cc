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

#include "ECOmapJournal.h"

#include <utility>

ECOmapJournalEntry::ECOmapJournalEntry(const eversion_t version, const bool clear_omap, std::optional<ceph::buffer::list> omap_header,
  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> omap_updates) : 
  version(version), clear_omap(clear_omap), omap_header(std::move(omap_header)), omap_updates(std::move(omap_updates)) {}

bool ECOmapJournalEntry::operator==(const ECOmapJournalEntry& other) const {
  return this->version == other.version;
}

void ECOmapValue::update_value(const eversion_t new_version, std::optional<ceph::buffer::list> new_value) {
  this->version = new_version;
  this->value = std::move(new_value);
}

void RemovedRanges::add_range(const std::string& start, const std::optional<std::string>& end) {
  std::string new_start = start;
  std::optional<std::string> new_end = end;
  auto it = ranges.begin();
  bool inserted = false;
  while (it != ranges.end()) {
    // Current range is to the left of new range
    if (it->second && *it->second < new_start) {
      ++it;
      continue;
    }
    // Current range is to the right of new range
    if (!new_end || *new_end < it->first) {
      ranges.insert(it, {new_start, new_end});
      inserted = true;
      break;
    }
    // Ranges overlap, merge them
    if (it->first < new_start) {
      new_start = it->first;
    }
    if (!it->second) {
      new_end = std::nullopt;
    } else if (new_end && *it->second > *new_end) {
      new_end = it->second;
    }
    it = ranges.erase(it);
  }
  if (!inserted) {
    ranges.emplace_back(new_start, new_end);
  }
}

void RemovedRanges::clear_omap() {
  ranges.clear();
  ranges.emplace_back("", std::nullopt);
}

void ECOmapHeader::update_header(const eversion_t new_version, std::optional<ceph::buffer::list> new_header) {
  this->version = new_version;
  this->header = std::move(new_header);
}


void ECOmapJournal::add_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry) {
  entries[hoid].push_back(entry);
}

bool ECOmapJournal::remove_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry) {
  // Attempt to remove entry from unprocessed entries
  if (const auto it_map = entries.find(hoid);
    it_map != entries.end()) {
    auto &entry_list = it_map->second;
    for (const auto& an_entry : entry_list) {
      if (an_entry.version == entry.version) {
        entry_list.remove(an_entry);
        if (const auto header_it = header_map.find(hoid);
          header_it != header_map.end() &&
            header_it->second.version == entry.version) {
          header_map.erase(header_it);
        }
        return true;
      }
    }
  }

  // Attempt to remove entry from processed entries
  return remove_processed_entry(hoid, entry);
}

bool ECOmapJournal::remove_entry_by_version(const hobject_t &hoid, const eversion_t version) {
  // Attempt to remove entry from unprocessed entries
  if (const auto it_map = entries.find(hoid);
    it_map != entries.end()) {
    auto &entry_list = it_map->second;
    for (const auto& entry : entry_list) {
      if (entry.version == version) {
        entry_list.remove(entry);
        if (const auto header_it = header_map.find(hoid);
          header_it != header_map.end() &&
            header_it->second.version == entry.version) {
          header_map.erase(header_it);
            }
        return true;
      }
    }
  }

  // Attempt to remove entry from processed entries
  return remove_processed_entry_by_version(hoid, version);
}

void ECOmapJournal::clear(const hobject_t &hoid) {
  entries.erase(hoid);
  key_map.erase(hoid);
  removed_ranges_map.erase(hoid);
  header_map.erase(hoid);
}

void ECOmapJournal::clear_all() {
  entries.clear();
  key_map.clear();
  removed_ranges_map.clear();
  header_map.clear();
}

// Only works if each processed object creates a RemovedRanges entry
std::size_t ECOmapJournal::entries_size(const hobject_t &hoid) const {
  if (const auto entries_it = entries.find(hoid);
    entries_it != entries.end()) {
    return entries_it->second.size();
  }
  return 0u;
}

// Function to get specific object's entries, if not present, creates an empty list
std::list<ECOmapJournalEntry>& ECOmapJournal::get_entries(const hobject_t &hoid) {
  return entries[hoid];
}

std::list<ECOmapJournalEntry> ECOmapJournal::snapshot_entries(const hobject_t &hoid) const {
  if (const auto it = entries.find(hoid);
    it != entries.end()) {
    return it->second;
  }
  return {};
}

ECOmapJournal::const_iterator ECOmapJournal::begin_entries(const hobject_t &hoid) {
  return entries[hoid].begin();
}

ECOmapJournal::const_iterator ECOmapJournal::end_entries(const hobject_t &hoid) {
  return entries[hoid].end();
}

ECOmapJournal::iterator ECOmapJournal::begin_entries_mutable(const hobject_t &hoid)
{
  return entries[hoid].begin();
}

ECOmapJournal::iterator ECOmapJournal::end_entries_mutable(const hobject_t& hoid)
{
  return entries[hoid].end();
}

std::optional<ceph::buffer::list> ECOmapJournal::get_updated_header(const hobject_t &hoid) {
  process_headers(hoid);
  if (!header_map.contains(hoid)) {
    return std::nullopt;
  }
  return header_map[hoid].header;
}

std::tuple<ECOmapJournal::UpdateMapType, ECOmapJournal::RangeMapType> ECOmapJournal::get_value_updates(const hobject_t &hoid) {
  process_entries(hoid);
  return {get_key_map(hoid), get_removed_ranges(hoid)};
}

void ECOmapJournal::process_headers(const hobject_t& hoid) {
  for (auto entry_iter = begin_entries_mutable(hoid);
        entry_iter != end_entries_mutable(hoid); ++entry_iter) {
    // Update header if present
    if (entry_iter->omap_header) {
      if (!header_map.contains(hoid)) {
        header_map[hoid] = ECOmapHeader(entry_iter->version, entry_iter->omap_header);
      } else {
        header_map[hoid].update_header(entry_iter->version, entry_iter->omap_header);
      }
      entry_iter->omap_header = std::nullopt;
    }
  }
}

void ECOmapJournal::process_entries(const hobject_t &hoid) {
  for (auto entry_iter = begin_entries(hoid);
        entry_iter != end_entries(hoid); ++entry_iter) {
    RemovedRanges removed_ranges(entry_iter->version);

    // Clear omap if specified
    if (entry_iter->clear_omap) {
      // Mark all keys as removed
      for (auto [_, value] : key_map[hoid]) {
        value.update_value(entry_iter->version, std::nullopt);
      }
      // Mark entire range as removed
      removed_ranges.clear_omap();
    }

    // Update header if present
    if (entry_iter->omap_header) {
      if (!header_map.contains(hoid)) {
        header_map[hoid] = ECOmapHeader(entry_iter->version, entry_iter->omap_header);
      } else {
        header_map[hoid].update_header(entry_iter->version, entry_iter->omap_header);
      }
    }

    // Process key updates
    auto &obj_map = key_map[hoid];
    for (const auto & [type, update] : entry_iter->omap_updates) {
      auto iter = update.cbegin();
      switch (type) {
        case OmapUpdateType::Insert: {
          std::map<std::string, ceph::buffer::list> vals;
          decode(vals, iter);
          // Insert key value pairs into update_map
          for (auto it = vals.begin(); it != vals.end(); ++it) {
            const auto &key = it->first;
            const auto &val = it->second;
            
            // Check if key already exists in key map
            auto entry_it = obj_map.find(key);
            if (entry_it != obj_map.end()) {
              // Update existing value
              entry_it->second.update_value(entry_iter->version, val);
            } else {
              // Insert new value
              obj_map.emplace(key, ECOmapValue(entry_iter->version, val));
            }
          }
          break;
        }
        case OmapUpdateType::Remove: {
          std::set<std::string> keys;
          decode(keys, iter);
          // Mark keys in key_map as removed
          for (const auto &key : keys) {
            // Check if key already exists in key map
            if (auto entry_it = obj_map.find(key);
              entry_it != obj_map.end()) {
              // Update existing value to null
              entry_it->second.update_value(entry_iter->version, std::nullopt);
            } else {
              // Insert new null value
              obj_map.emplace(key, ECOmapValue(entry_iter->version, std::nullopt));
            }
          }
          break;
        }
        case OmapUpdateType::RemoveRange: {
          std::string key_begin, key_end;
          decode(key_begin, iter);
          decode(key_end, iter);
          
          // Add removed range
          std::string start = key_begin;
          std::optional<std::string> end = key_end;
          removed_ranges.add_range(start, end);

          // Mark keys in key_map as removed that fall within the removed range
          auto map_it = obj_map.lower_bound(key_begin);
          while (map_it != obj_map.end()) {
            if (map_it->first >= key_end) {
              break;
            }
            map_it->second.update_value(entry_iter->version, std::nullopt);
            ++map_it;
          }
          break;
        }
        default:
          ceph_abort_msg("Unknown OmapUpdateType");
      }
    }
    if (!removed_ranges.ranges.empty()) {
      removed_ranges_map[hoid].emplace_back(removed_ranges);
    }
  }
  entries.erase(hoid);
}

bool ECOmapJournal::remove_processed_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry) {
  // Remove the header if version matches
  if (const auto header_it = header_map.find(hoid);
    header_it != header_map.end() && header_it->second.version == entry.version) {
      header_map.erase(header_it);
  }

  // Remove key updates if version matches
  auto &obj_map = key_map[hoid];
  for (const auto & [type, update] : entry.omap_updates) {
    auto iter = update.cbegin();
    switch (type) {
      case OmapUpdateType::Insert: {
        std::map<std::string, ceph::buffer::list> vals;
        decode(vals, iter);
        for (auto val_it = vals.begin(); val_it != vals.end(); ++val_it) {
          const auto &key = val_it->first;
          auto key_it = obj_map.find(key);
          if (key_it != obj_map.end() && 
            key_it->second.version == entry.version) {
            obj_map.erase(key);
          }
        }
        break;
      }
      case OmapUpdateType::Remove: {
        std::set<std::string> keys;
        decode(keys, iter);
        for (const auto &key : keys) {
          if (auto it = obj_map.find(key);
            it != obj_map.end() &&
              it->second.version == entry.version) {
            obj_map.erase(key);
          }
        }
        break;
      }
      case OmapUpdateType::RemoveRange: {
        std::string key_begin, key_end;
        decode(key_begin, iter);
        decode(key_end, iter);
        auto map_it = obj_map.lower_bound(key_begin);
        while (map_it != obj_map.end()) {
          if (map_it->first >= key_end) {
            break;
          }
          if (map_it->second.version == entry.version) {
            map_it = obj_map.erase(map_it);
          } else {
            ++map_it;
          }
        }
        break;
      }
      default: {
        return false;
      }
    }
  }

  // Remove removed ranges if version matches
  if (const auto removed_ranges_it = removed_ranges_map.find(hoid);
    removed_ranges_it != removed_ranges_map.end()) {
    auto &removed_ranges_list = removed_ranges_it->second;
    for (auto rr_it = removed_ranges_list.begin(); rr_it != removed_ranges_list.end(); ++rr_it) {
      if (rr_it->version == entry.version) {
        removed_ranges_list.erase(rr_it);
        break;
      }
    }
  } else {
    return false;
  }

  return true;
}

bool ECOmapJournal::remove_processed_entry_by_version(const hobject_t &hoid, const eversion_t version) {
  // Remove the header if version matches
  if (const auto header_it = header_map.find(hoid);
    header_it != header_map.end() && header_it->second.version == version) {
    header_map.erase(header_it);
  }

  // Remove key updates if version matches
  auto key_map_it = key_map.find(hoid);
  if (key_map_it != key_map.end()) {
   for (auto it = key_map_it->second.begin(); it != key_map_it->second.end(); ) {
      if (it->second.version == version) {
        it = key_map_it->second.erase(it);
      } else {
        ++it;
      }
    }
  }

  // Remove removed ranges if version matches
  auto removed_ranges_it = removed_ranges_map.find(hoid);
  if (removed_ranges_it != removed_ranges_map.end()) {
    auto &removed_ranges_list = removed_ranges_it->second;
    for (auto rr_it = removed_ranges_list.begin(); rr_it != removed_ranges_list.end(); ++rr_it) {
      if (rr_it->version == version) {
        removed_ranges_list.erase(rr_it);
        break;
      }
    }
  } else {
    return false;
  }

  return true;
}

ECOmapJournal::UpdateMapType ECOmapJournal::get_key_map(const hobject_t &hoid) {
  return key_map[hoid];
}

ECOmapJournal::RangeMapType ECOmapJournal::get_removed_ranges(const hobject_t &hoid) {
  // Merge all removed ranges for the object
  RangeMapType merged_ranges;
  if (const auto it = removed_ranges_map.find(hoid);
    it != removed_ranges_map.end()) {
    for (const auto &rr : it->second) {
      for (const auto & [range_first, range_second] : rr.ranges) {
        // Add range to merged_ranges, merging overlapping ranges
        std::string start = range_first;
        std::optional<std::string> end = range_second;

        // Find the range that starts after the current start
        auto map_it = merged_ranges.upper_bound(start);
        if (map_it != merged_ranges.begin()) {
          // Merge range to the left, if they overlap
          if (const auto prev = std::prev(map_it);
            !prev->second || *prev->second >= start) {
            start = prev->first;
            if (!end) {

            } else if (!prev->second) {
              end = std::nullopt;
            } else if (*prev->second > *end) {
              end = *prev->second;
            }
            merged_ranges.erase(prev);
          }
        }
        // Merge ranges to the right, if they overlap
        while (map_it != merged_ranges.end() &&
               (!end || map_it->first <= *end)) {
          if (!end) {

          } else if (!map_it->second) {
            end = std::nullopt;
          } else if (*map_it->second > *end) {
            end = map_it->second;
          }
          map_it = merged_ranges.erase(map_it);
        }
        merged_ranges.emplace_hint(map_it, start, end);
      }
    }
  }
  return merged_ranges;
}
