// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

#include "ECOmapJournal.h"

ECOmapJournal::ECOmapJournal(bool clear_omap, std::optional<ceph::buffer::list> omap_header,
  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> &omap_updates) : 
  clear_omap(clear_omap), omap_header(omap_header), omap_updates(omap_updates) {
    id = get_new_id();
}

ECOmapJournal::ECOmapJournal(uint64_t id, bool clear_omap, std::optional<ceph::buffer::list> omap_header,
  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> &omap_updates) : 
  id(id), clear_omap(clear_omap), omap_header(omap_header), omap_updates(omap_updates) {}

static int ECOmapJournal::get_new_id() {
  return global_id_counter++;
}

bool ECOmapJournal::operator==(const ECOmapJournal& other) const {
  return this->id == other.id;
}