// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

#include <cstdint>
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

  ECOmapJournalEntry(eversion_t version, bool clear_omap, std::optional<ceph::buffer::list> omap_header,
    std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> omap_updates);
  bool operator==(const ECOmapJournalEntry& other) const;
};

class ECOmapJournal {
 private:
  std::list<ECOmapJournalEntry> entries;
 public:
  using const_iterator = std::list<ECOmapJournalEntry>::const_iterator;
  void add_entry(const ECOmapJournalEntry &entry);
  bool remove_entry(const ECOmapJournalEntry &entry);
  bool remove_entry_by_version(const eversion_t version);
  void clear();
  int size() const;
  const_iterator begin() const;
  const_iterator end() const;
};
