// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "Utils.h"
#include <set>

namespace librbd {
namespace deep_copy {
namespace util {

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::deep_copy::util::" << __func__ << ": "

void compute_snap_map(CephContext* cct,
                      librados::snap_t src_snap_id_start,
                      librados::snap_t src_snap_id_end,
                      const SnapIds& dst_snap_ids,
                      const SnapSeqs &snap_seqs,
                      SnapMap *snap_map) {
  std::set<librados::snap_t> ordered_dst_snap_ids{
    dst_snap_ids.begin(), dst_snap_ids.end()};
  auto dst_snap_id_it = ordered_dst_snap_ids.begin();

  SnapIds snap_ids;
  for (auto &it : snap_seqs) {
    // ensure all dst snap ids are included in the mapping table since
    // deep copy will skip non-user snapshots
    while (dst_snap_id_it != ordered_dst_snap_ids.end()) {
      if (*dst_snap_id_it < it.second) {
        snap_ids.insert(snap_ids.begin(), *dst_snap_id_it);
      } else if (*dst_snap_id_it > it.second) {
        break;
      }
      ++dst_snap_id_it;
    }

    // we should only have the HEAD revision in the last snap seq
    ceph_assert(snap_ids.empty() || snap_ids[0] != CEPH_NOSNAP);
    snap_ids.insert(snap_ids.begin(), it.second);

    if (it.first < src_snap_id_start) {
      continue;
    } else if (it.first > src_snap_id_end) {
      break;
    }

    (*snap_map)[it.first] = snap_ids;
  }

  ldout(cct, 10) << "src_snap_id_start=" << src_snap_id_start << ", "
                 << "src_snap_id_end=" << src_snap_id_end << ", "
                 << "dst_snap_ids=" << dst_snap_ids << ", "
                 << "snap_seqs=" << snap_seqs << ", "
                 << "snap_map=" << *snap_map << dendl;
}

} // namespace util
} // namespace deep_copy
} // namespace librbd
