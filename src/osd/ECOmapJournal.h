// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>
#include <map>

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

class ECOmapValue {
 public:
  eversion_t version;
  std::optional<ceph::buffer::list> value;

  ECOmapValue(eversion_t version, std::optional<ceph::buffer::list> value) : version(version), value(value) {}

  void update_value(eversion_t version, std::optional<ceph::buffer::list> value);
};

class RemovedRanges {
 public:
  std::list<std::pair<std::optional<std::string>, std::optional<std::string>>> ranges;

  RemovedRanges() = default;

  void add_range(const std::optional<std::string>& start, const std::optional<std::string>& end);
  void clear_omap();
};

class ECOmapHeader {
 public:
  eversion_t version = eversion_t();
  std::optional<ceph::buffer::list> header = std::nullopt;

  ECOmapHeader(eversion_t version, std::optional<ceph::buffer::list> header) 
    : version(version), header(header) {}
  ECOmapHeader() = default;

  void update_header(eversion_t version, std::optional<ceph::buffer::list> header);
};

class ECOmapJournal {
 private:
  // Intact journal entries 
  std::map<hobject_t, std::list<ECOmapJournalEntry>> entries;

  // Split up journal entries
  std::map<hobject_t, std::map<std::string, ECOmapValue>> key_map;
  std::map<hobject_t, std::list<std::map<eversion_t, RemovedRanges>>> removed_ranges_map;
  std::map<hobject_t, ECOmapHeader> header_map;

  // Function to get specific object's entries
  std::list<ECOmapJournalEntry>& get_entries(const hobject_t &hoid);
  std::list<ECOmapJournalEntry> snapshot_entries(const hobject_t &hoid) const;

 public:
  using const_iterator = std::list<ECOmapJournalEntry>::const_iterator;

  // Specific object operations
  void add_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry);
  bool remove_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry);
  bool remove_entry_by_version(const hobject_t &hoid, const eversion_t version);
  // Clear entries for a specific object
  void clear(const hobject_t &hoid);
  // Clear all entries
  void clear_all();
  // Entries for a specific object
  int size(const hobject_t &hoid) const;
  
  const_iterator begin(const hobject_t &hoid);
  const_iterator end(const hobject_t &hoid);

  using UpdateMapType = std::map<std::string, std::optional<ceph::buffer::list>>;
  using RangeListType = std::list<std::pair<std::optional<std::string>, std::optional<std::string>>>;
  std::tuple<UpdateMapType, RangeListType> get_value_updates(const hobject_t &hoid);;
  std::optional<ceph::buffer::list> get_updated_header(const hobject_t &hoid);
};
