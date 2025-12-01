// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

#include "ECOmapJournal.h"

uint64_t ECOmapJournalEntry::global_id_counter = 0;

ECOmapJournalEntry::ECOmapJournalEntry(bool clear_omap, std::optional<ceph::buffer::list> omap_header,
  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> omap_updates) : 
  clear_omap(clear_omap), omap_header(omap_header), omap_updates(std::move(omap_updates)) {
    id = get_new_id();
}

ECOmapJournalEntry::ECOmapJournalEntry(uint64_t id, bool clear_omap, std::optional<ceph::buffer::list> omap_header,
  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> omap_updates) : 
  id(id), clear_omap(clear_omap), omap_header(omap_header), omap_updates(std::move(omap_updates)) {}

int ECOmapJournalEntry::get_new_id() {
  return ++global_id_counter;
}

bool ECOmapJournalEntry::operator==(const ECOmapJournalEntry& other) const {
  return this->id == other.id;
}


void ECOmapJournal::add_entry(const ECOmapJournalEntry &entry) {
  entries.push_back(entry);
}

bool ECOmapJournal::remove_entry(const ECOmapJournalEntry &entry) {
  return entries.remove(entry);
}

bool ECOmapJournal::remove_entry_by_id(const uint64_t id) {
  for (const auto &journal_entry : entries) {
    if (journal_entry.id == id) {
      return entries.remove(journal_entry);
    }
  }
  return false;
}

ECOmapJournal::const_iterator ECOmapJournal::begin() const {
  return entries.begin();
}

ECOmapJournal::const_iterator ECOmapJournal::end() const {
  return entries.end();
}

void ECOmapJournal::clear() {
  entries.clear();
}

int ECOmapJournal::size() const {
  return entries.size();
}