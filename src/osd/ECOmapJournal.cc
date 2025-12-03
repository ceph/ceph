// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

#include "ECOmapJournal.h"

ECOmapJournalEntry::ECOmapJournalEntry(eversion_t version, bool clear_omap, std::optional<ceph::buffer::list> omap_header,
  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> omap_updates) : 
  version(version), clear_omap(clear_omap), omap_header(omap_header), omap_updates(std::move(omap_updates)) {}

bool ECOmapJournalEntry::operator==(const ECOmapJournalEntry& other) const {
  return this->version == other.version;
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

