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
  eversion_t version;
  std::list<std::pair<std::optional<std::string>, std::optional<std::string>>> ranges;

  RemovedRanges(eversion_t version) : version(version) {}
  RemovedRanges(eversion_t version, std::list<std::pair<std::optional<std::string>, std::optional<std::string>>> ranges) 
    : version(version), ranges(std::move(ranges)) {}

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
  using UpdateMapType = std::map<std::string, ECOmapValue>;
  using RangeListType = std::list<std::pair<std::optional<std::string>, std::optional<std::string>>>;
  using const_iterator = std::list<ECOmapJournalEntry>::const_iterator;
 private:
  // Unprocessed journal entries 
  std::map<hobject_t, std::list<ECOmapJournalEntry>> entries;

  // Processed journal entries
  std::map<hobject_t, std::map<std::string, ECOmapValue>> key_map;
  std::map<hobject_t, std::list<RemovedRanges>> removed_ranges_map;
  std::map<hobject_t, ECOmapHeader> header_map;

  // Function to get specific object's unprocessed entries
  std::list<ECOmapJournalEntry>& get_entries(const hobject_t &hoid);
  std::list<ECOmapJournalEntry> snapshot_entries(const hobject_t &hoid) const;

  void process_entries(const hobject_t &hoid);
  bool remove_processed_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry);
  bool remove_processed_entry_by_version(const hobject_t &hoid, const eversion_t version);
  UpdateMapType get_key_map(const hobject_t &hoid);
  RangeListType get_removed_ranges(const hobject_t &hoid);

 public:
  void add_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry);
  bool remove_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry);
  bool remove_entry_by_version(const hobject_t &hoid, const eversion_t version);
  void clear(const hobject_t &hoid);
  void clear_all();
  int entries_size(const hobject_t &hoid) const;
  std::tuple<UpdateMapType, RangeListType> get_value_updates(const hobject_t &hoid);
  std::optional<ceph::buffer::list> get_updated_header(const hobject_t &hoid);

  const_iterator begin_entries(const hobject_t &hoid);
  const_iterator end_entries(const hobject_t &hoid);
};
