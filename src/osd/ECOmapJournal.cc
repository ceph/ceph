// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

#include "ECOmapJournal.h"

ECOmapJournalEntry::ECOmapJournalEntry(eversion_t version, bool clear_omap, std::optional<ceph::buffer::list> omap_header,
  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> omap_updates) : 
  version(version), clear_omap(clear_omap), omap_header(omap_header), omap_updates(std::move(omap_updates)) {}

bool ECOmapJournalEntry::operator==(const ECOmapJournalEntry& other) const {
  return this->version == other.version;
}

void ECOmapValue::update_value(eversion_t version, std::optional<ceph::buffer::list> value) {
  this->version = version;
  this->value = value;
}

void RemovedRanges::add_range(const std::optional<std::string>& start, const std::optional<std::string>& end) {
  std::optional<std::string> new_start = start;
  std::optional<std::string> new_end = end;
  auto it = ranges.begin();
  bool inserted = false;
  while (it != ranges.end()) {
    // Current range is to the left of new range
    if (it->second && *it->second < *new_start) {
      it++;
      continue;
    }
    // Current range is to the right of new range
    if (it->first && (!new_end || *new_end < *it->first)) {
      ranges.insert(it, {new_start, new_end});
      inserted = true;
      break;
    }
    // Ranges overlap, merge them
    if (!it->first || (new_start && *it->first < *new_start)) {
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
  ranges.emplace_back(std::nullopt, std::nullopt);
}

void ECOmapHeader::update_header(eversion_t version, std::optional<ceph::buffer::list> header) {
  this->version = version;
  this->header = header;
}


void ECOmapJournal::add_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry) {
  entries[hoid].push_back(entry);
}

bool ECOmapJournal::remove_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry) {
  // Attempt to remove entry from unprocessed entries
  auto it_map = entries.find(hoid);
  if (it_map != entries.end()) {
    auto &entry_list = it_map->second;
    for (auto an_entry : entry_list) {
      if (an_entry.version == entry.version) {
        entry_list.remove(an_entry);
        return true;
      }
    }
  }

  // Attempt to remove entry from processed entries
  return remove_processed_entry(hoid, entry);
}

bool ECOmapJournal::remove_entry_by_version(const hobject_t &hoid, const eversion_t version) {
  // Attempt to remove entry from unprocessed entries
  auto it_map = entries.find(hoid);
  if (it_map != entries.end()) {
    auto &entry_list = it_map->second;
    for (auto entry : entry_list) {
      if (entry.version == version) {
        entry_list.remove(entry);
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
int ECOmapJournal::size(const hobject_t &hoid) const {
  auto entries_it = entries.find(hoid);
  auto removed_ranges_it = removed_ranges_map.find(hoid);
  int sum = 0;
  if (entries_it != entries.end()) {
    sum += entries_it->second.size();
  }
  if (removed_ranges_it != removed_ranges_map.end()) {
    sum += removed_ranges_it->second.size();
  }
  return sum;
}

// Function to get specific object's entries, if not present, creates an empty list
std::list<ECOmapJournalEntry>& ECOmapJournal::get_entries(const hobject_t &hoid) {
  return entries[hoid];
}

std::list<ECOmapJournalEntry> ECOmapJournal::snapshot_entries(const hobject_t &hoid) const {
  auto it = entries.find(hoid);
  if (it != entries.end()) {
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

std::optional<ceph::buffer::list> ECOmapJournal::get_updated_header(const hobject_t &hoid) {
  process_entries(hoid);
  if (header_map.find(hoid) == header_map.end()) {
    return std::nullopt;
  }
  return header_map[hoid].header;
}

std::tuple<ECOmapJournal::UpdateMapType, ECOmapJournal::RangeListType> ECOmapJournal::get_value_updates(const hobject_t &hoid) {
  process_entries(hoid);
  return {get_key_map(hoid), get_removed_ranges(hoid)};
}

void ECOmapJournal::process_entries(const hobject_t &hoid) {
  for (auto entry_iter = begin_entries(hoid);
        entry_iter != end_entries(hoid); ++entry_iter) {
    RemovedRanges removed_ranges(entry_iter->version);

    // Clear omap if specified
    if (entry_iter->clear_omap) {
      // Mark all keys as removed
      for (auto &&key_value : key_map[hoid]) {
        key_value.second.update_value(entry_iter->version, std::nullopt);
      }
      // Mark entire range as removed
      removed_ranges.clear_omap();
    }

    // Update header if present
    if (entry_iter->omap_header) {
      if (header_map.find(hoid) == header_map.end()) {
        header_map[hoid] = ECOmapHeader(entry_iter->version, entry_iter->omap_header);
      } else {
        header_map[hoid].update_header(entry_iter->version, entry_iter->omap_header);
      }
    }

    // Process key updates
    auto &obj_map = key_map[hoid];
    for (auto &&update : entry_iter->omap_updates) {
      OmapUpdateType type = update.first;
      auto iter = update.second.cbegin();
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
            auto entry_it = obj_map.find(key);
            if (entry_it != obj_map.end()) {
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
          std::optional<std::string> start = key_begin;
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
      removed_ranges_map[hoid].emplace_back(removed_ranges);
    }
  }
  entries.erase(hoid);
}

bool ECOmapJournal::remove_processed_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry) {
  // Remove the header if version matches
  auto header_it = header_map.find(hoid);
  if (header_it != header_map.end() && header_it->second.version == entry.version) {
    header_map.erase(header_it);
  }

  // Remove key updates if version matches
  auto &obj_map = key_map[hoid];
  for (auto &&update : entry.omap_updates) {
    OmapUpdateType type = update.first;
    auto iter = update.second.cbegin();
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
          auto it = obj_map.find(key);
          if (it != obj_map.end() && 
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
  auto removed_ranges_it = removed_ranges_map.find(hoid);
  if (removed_ranges_it != removed_ranges_map.end()) {
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
  auto header_it = header_map.find(hoid);
  if (header_it != header_map.end() && header_it->second.version == version) {
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

ECOmapJournal::RangeListType ECOmapJournal::get_removed_ranges(const hobject_t &hoid) {
  // Merge all removed ranges for the object
  RangeListType merged_ranges;
  auto it = removed_ranges_map.find(hoid);
  if (it != removed_ranges_map.end()) {
    for (const auto &rr : it->second) {
      for (const auto &range : rr.ranges) {
        // Add range to merged_ranges, merging overlapping ranges
        std::optional<std::string> start = range.first;
        std::optional<std::string> end = range.second;
        auto mr_it = merged_ranges.begin();
        bool inserted = false;
        while (mr_it != merged_ranges.end()) {
          // Current range is to the left of new range
          if (mr_it->second && *mr_it->second < *start) {
            ++mr_it;
            continue;
          }
          // Current range is to the right of new range
          if (mr_it->first && (!end || *end < *mr_it->first)) {
            merged_ranges.insert(mr_it, {start, end});
            inserted = true;
            break;
          }
          // Ranges overlap, merge them
          if (!mr_it->first || (start && *mr_it->first < *start)) {
            start = mr_it->first;
          }
          if (!mr_it->second) {
            end = std::nullopt;
          } else if (end && *mr_it->second > *end) {
            end = mr_it->second;
          }
          mr_it = merged_ranges.erase(mr_it);
        }
        if (!inserted) {
          merged_ranges.emplace_back(start, end);
        }
      }
    }
  }
  return merged_ranges;
}
