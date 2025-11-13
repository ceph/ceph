// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

enum class OmapUpdateType : uint8_t {Remove, Insert, RemoveRange};

class ECOmapJournal {
 private:
  static uint64_t global_id_counter;
 public:
  uint64_t id;
  bool clear_omap;
  std::optional<ceph::buffer::list> omap_header;
  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> &omap_updates;

  ECOmapJournal(bool clear_omap, std::optional<ceph::buffer::list> omap_header,
    std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> &omap_updates);

  ECOmapJournal(uint64_t id, bool clear_omap, std::optional<ceph::buffer::list> omap_header,
    std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> &omap_updates);

  static int get_new_id();

  bool operator==(const ECOmapJournal& other);
};
