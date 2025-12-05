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
  auto it_map = entries.find(hoid);
  if (it_map == entries.end()) {
    return false;
  }
  auto &entry_list = it_map->second;
  for (auto an_entry : entry_list) {
    if (an_entry == entry) {
      entry_list.remove(entry);
      return true;
    }
  }
  return false;
}

bool ECOmapJournal::remove_entry_by_version(const hobject_t &hoid, const eversion_t version) {
  auto it_map = entries.find(hoid);
  if (it_map == entries.end()) {
    return false;
  }
  auto &entry_list = it_map->second;
  for (auto entry : entry_list) {
    if (entry.version == version) {
      entry_list.remove(entry);
      return true;
    }
  }
  return false;
}

void ECOmapJournal::clear(const hobject_t &hoid) {
  entries.erase(hoid);
}

void ECOmapJournal::clear_all() {
  entries.clear();
}

int ECOmapJournal::size(const hobject_t &hoid) const {
  auto it = entries.find(hoid);
  return it != entries.end() ? it->second.size() : 0;
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

ECOmapJournal::const_iterator ECOmapJournal::begin(const hobject_t &hoid) {
  return entries[hoid].begin();
}

ECOmapJournal::const_iterator ECOmapJournal::end(const hobject_t &hoid) {
  return entries[hoid].end();
}

std::optional<ceph::buffer::list> ECOmapJournal::get_updated_header(const hobject_t &hoid) {
  std::optional<ceph::buffer::list> updated_header = std::nullopt;
  for (auto journal_it = begin(hoid);
      journal_it != end(hoid); ++journal_it) {
    if (journal_it->omap_header.has_value()) {
      updated_header = journal_it->omap_header;
    }
  }
  return updated_header;
}

std::tuple<ECOmapJournal::UpdateMapType, ECOmapJournal::RangeListType> ECOmapJournal::get_value_updates(const hobject_t &hoid) {
  ECOmapJournal::UpdateMapType update_map;
  ECOmapJournal::RangeListType remove_ranges;
  for (auto entry_iter = begin(hoid);
        entry_iter != end(hoid); ++entry_iter) {
    if (entry_iter->clear_omap) {
      // Clear all previous updates
      update_map.clear();
      // Mark entire range as removed
      remove_ranges.clear();
      remove_ranges.push_back({std::nullopt, std::nullopt});
    }

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
            update_map[key] = val;
          }
          break;
        }
        case OmapUpdateType::Remove: {
          std::set<std::string> keys;
          decode(keys, iter);
          // Mark keys in update_map as removed
          for (const auto &key : keys) {
            update_map[key] = std::nullopt;
          }
          break;
        }
        case OmapUpdateType::RemoveRange: {
          std::string key_begin, key_end;
          decode(key_begin, iter);
          decode(key_end, iter);
          
          // Add range to remove_ranges, merging overlapping ranges
          std::optional<std::string> start = key_begin;
          std::optional<std::string> end = key_end;
          auto it = remove_ranges.begin();
          bool inserted = false;
          while (it != remove_ranges.end()) {
            // Current range is to the left of new range
            if (it->second && *it->second < *start) {
              it++;
              continue;
            }
            // Current range is to the right of new range
            if (it->first && (!end || *end < *it->first)) {
              remove_ranges.insert(it, {start, end});
              inserted = true;
              break;
            }
            // Ranges overlap, merge them
            if (!it->first || (start && *it->first < *start)) {
              start = it->first;
            }
            if (!it->second) {
              end = std::nullopt;
            } else if (end && *it->second > *end) {
              end = it->second;
            }
            it = remove_ranges.erase(it);
          }
          if (!inserted) {
            remove_ranges.emplace_back(start, end);
          }

          // Erase keys in update_map that fall within the removed range
          auto map_it = update_map.lower_bound(key_begin);
          while (map_it != update_map.end()) {
            if (map_it->first >= key_end) {
              break;
            }
            map_it = update_map.erase(map_it);
          }
          break;
        }
        default:
          ceph_abort_msg("Unknown OmapUpdateType");
      }
    }
  }
  return {update_map, remove_ranges};
}
