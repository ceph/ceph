// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Utils.h"
#include "common/debug.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_types.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_replayer::snapshot::util::" \
                           << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace snapshot {
namespace util {

uint64_t compute_remote_snap_id(
    const ceph::shared_mutex& local_image_lock,
    const std::map<librados::snap_t, librbd::SnapInfo>& local_snap_infos,
    uint64_t local_snap_id, const std::string& remote_mirror_uuid) {
  ceph_assert(ceph_mutex_is_locked(local_image_lock));

  // Search our local non-primary snapshots for a mapping to the remote
  // snapshot. The non-primary mirror snapshot with the mappings will always
  // come at or after the snapshot we are searching against
  for (auto snap_it = local_snap_infos.lower_bound(local_snap_id);
       snap_it != local_snap_infos.end(); ++snap_it) {
    auto mirror_ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
      &snap_it->second.snap_namespace);
    if (mirror_ns == nullptr || !mirror_ns->is_non_primary()) {
      continue;
    }

    if (mirror_ns->primary_mirror_uuid != remote_mirror_uuid) {
      dout(20) << "local snapshot " << snap_it->first << " not tied to remote"
               << dendl;
      continue;
    } else if (local_snap_id == snap_it->first) {
      dout(15) << "local snapshot " << local_snap_id << " maps to "
               << "remote snapshot " << mirror_ns->primary_snap_id << dendl;
      return mirror_ns->primary_snap_id;
    }

    const auto& snap_seqs = mirror_ns->snap_seqs;
    for (auto [remote_snap_id_seq, local_snap_id_seq] : snap_seqs) {
      if (local_snap_id_seq == local_snap_id) {
        dout(15) << "local snapshot " << local_snap_id << " maps to "
                 << "remote snapshot " << remote_snap_id_seq << dendl;
        return remote_snap_id_seq;
      }
    }
  }

  return CEPH_NOSNAP;
}

} // namespace util
} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd
