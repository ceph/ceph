// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/ceph_assert.h"
#include "common/Cond.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/api/Utils.h"
#include "librbd/api/Group.h"
#include "librbd/group/ListSnapshotsRequest.h"
#include "librbd/group/UnlinkPeerGroupRequest.h"
#include "librbd/mirror/snapshot/UnlinkPeerRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::group::UnlinkPeerGroupRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace group {

using util::create_rados_callback;
using util::create_context_callback;

template <typename I>
void UnlinkPeerGroupRequest<I>::send() {
  CephContext *cct = (CephContext *)m_group_io_ctx.cct();
  ldout(cct, 10) << dendl;
  unlink_peer();
}

template <typename I>
void UnlinkPeerGroupRequest<I>::unlink_peer() {
  CephContext *cct = (CephContext *)m_group_io_ctx.cct();
  ldout(cct, 10) << dendl;

  std::vector<cls::rbd::GroupSnapshot> snaps;
  C_SaferCond cond;
  auto req = group::ListSnapshotsRequest<>::create(m_group_io_ctx, m_group_id,
                                                   true, true, &snaps, &cond);
  req->send();
  cond.wait();
  uint64_t count = 0;
  auto unlink_snap = snaps.end();
  auto unlink_unsynced_snap = snaps.end();
  bool unlink_unsynced = false;
  for (auto it = snaps.begin(); it != snaps.end(); it++) {
    auto ns = std::get_if<cls::rbd::MirrorGroupSnapshotNamespace>(
        &it->snapshot_namespace);
    if (ns != nullptr) {
      // FIXME: after relocate, on new primary the previous primary demoted
      // snap is not getting deleted, until the next demotion.
      if (ns->state != cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY &&
          ns->state != cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY) {
        continue;
      }
      count++;
      if (count == 3) {
        unlink_unsynced_snap = it;
      }
      ceph_assert(count <= 5);

      if (ns->mirror_peer_uuids.empty()) {
        auto next_snap = std::next(it);
        if (next_snap != snaps.end()) {
          unlink_snap = it;
          break;
        }
      }
    }
    // TODO: fix the hardcoded max_snaps value
    if (count == 5) {
      unlink_unsynced = true;
    }
  }

  if (unlink_snap != snaps.end()) {
    remove_group_snapshot(*unlink_snap);
  } else if (unlink_unsynced && unlink_unsynced_snap != snaps.end()) {
    remove_group_snapshot(*unlink_unsynced_snap);
  } else {
    finish(0);
  }
}

template <typename I>
void UnlinkPeerGroupRequest<I>::remove_group_snapshot(
                              cls::rbd::GroupSnapshot group_snap) {
  CephContext *cct = (CephContext *)m_group_io_ctx.cct();
  ldout(cct, 10) << dendl;

  for (auto &snap : group_snap.snaps) {
    if (snap.snap_id == CEPH_NOSNAP) {
      continue;
    }
    ImageCtx *ictx = nullptr;
    for (size_t i = 0; i < m_image_ctxs->size(); ++i) {
      ictx = (*m_image_ctxs)[i];
      if (ictx->id  != snap.image_id) {
        ictx = nullptr;
      } else {
        break;
      }
    }
    if (!ictx) {
      continue;
    }
    ldout(cct, 10) << "removing individual snapshot: "
                   << snap.snap_id << ", from image id:" << snap.image_id << dendl;
    remove_image_snapshot(ictx, snap.snap_id);
  }

  int r = cls_client::group_snap_remove(&m_group_io_ctx,
      librbd::util::group_header_name(m_group_id), group_snap.id);
  if (r < 0) {
    lderr(cct) << "failed to remove group snapshot metadata: "
               << cpp_strerror(r) << dendl;
  }

  unlink_peer();
}

template <typename I>
void UnlinkPeerGroupRequest<I>::remove_image_snapshot(
    ImageCtx *image_ctx, uint64_t snap_id) {
  CephContext *cct = (CephContext *)m_group_io_ctx.cct();
  ldout(cct, 10) << snap_id << dendl;

  image_ctx->image_lock.lock_shared();
  int r = -ENOENT;
  cls::rbd::SnapshotNamespace snap_namespace;
  std::string snap_name;
  for (auto snap_it = image_ctx->snap_info.find(snap_id);
       snap_it != image_ctx->snap_info.end(); ++snap_it) {
    if (snap_it->first == snap_id) {
      r = 0;
      snap_namespace = snap_it->second.snap_namespace;
      snap_name = snap_it->second.name;
    }
  }

  if (r == -ENOENT) {
    ldout(cct, 10) << "missing snapshot: snap_id=" << snap_id << dendl;
    image_ctx->image_lock.unlock_shared();
    return;
  }

  auto mirror_ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
    &snap_namespace);
  if (mirror_ns == nullptr) {
    lderr(cct) << "not mirror snapshot (snap_id=" << snap_id << ")" << dendl;
    image_ctx->image_lock.unlock_shared();
    return;
  }
  image_ctx->image_lock.unlock_shared();
  image_ctx->operations->snap_remove(snap_namespace, snap_name.c_str());
}

template <typename I>
void UnlinkPeerGroupRequest<I>::finish(int r) {
  CephContext *cct = (CephContext *)m_group_io_ctx.cct();
  ldout(cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
}

} // namespace group
} // namespace librbd

template class librbd::group::UnlinkPeerGroupRequest<librbd::ImageCtx>;
