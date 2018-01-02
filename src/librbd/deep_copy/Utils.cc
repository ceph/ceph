// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Utils.h"

namespace librbd {
namespace deep_copy {
namespace util {

void compute_snap_map(librados::snap_t snap_id_start,
                      librados::snap_t snap_id_end,
                      const SnapSeqs &snap_seqs,
                      SnapMap *snap_map) {
  SnapIds snap_ids;
  for (auto &it : snap_seqs) {
    snap_ids.insert(snap_ids.begin(), it.second);
    if (it.first < snap_id_start) {
      continue;
    } else if (snap_id_end != CEPH_NOSNAP && it.first > snap_id_end) {
      break;
    }

    (*snap_map)[it.first] = snap_ids;
  }

  if (snap_id_end == CEPH_NOSNAP) {
    snap_ids.insert(snap_ids.begin(), CEPH_NOSNAP);
    (*snap_map)[CEPH_NOSNAP] = snap_ids;
  }
}

} // util
} // namespace deep_copy
} // namespace librbd
