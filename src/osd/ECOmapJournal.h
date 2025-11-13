// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

#include "osd_types.h"

class ECOmapJournal {
 public:
  bool clear_omap;
  std::optional<ceph::buffer::list> omap_header;
  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> &omap_updates;

  ECOmapJournal(bool clear_omap, std::optional<ceph::buffer::list> omap_header,
  std::vector<std::pair<OmapUpdateType, ceph::buffer::list>> &omap_updates) : 
    clear_omap(clear_omap), omap_header(omap_header), omap_updates(omap_updates) {}
};
