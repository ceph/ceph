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
//#include "librbd/mirror/snapshot/UnlinkPeerRequest.h"
#include "librbd/mirror/snapshot/GroupUnlinkPeerRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::group::GroupUnlinkPeerRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using util::create_rados_callback;
using util::create_context_callback;

template <typename I>
void GroupUnlinkPeerRequest<I>::send() {
  ldout(m_cct, 10) << dendl;
  m_max_snaps = m_cct->_conf.get_val<uint64_t>("rbd_mirroring_max_mirroring_snapshots");
  list_group_snaps();
}

template <typename I>
void GroupUnlinkPeerRequest<I>::unlink_peer() {
  ldout(m_cct, 10) << "rbd_mirroring_max_mirroring_snapshots = " << m_max_snaps << dendl;

  uint64_t count = 0;
  auto unlink_snap = m_group_snaps.end();
  bool unlink = false;

  for (auto it = m_group_snaps.begin(); it != m_group_snaps.end(); it++) {
    auto ns = std::get_if<cls::rbd::GroupSnapshotNamespaceMirror>(
        &it->snapshot_namespace);
    if (ns == nullptr) {
      continue;
    }

    // FIXME: after relocate, on new primary the previous primary demoted
    // snap is not getting deleted, until the next demotion.
    if (ns->state != cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY) {
      // Reset the count if the group was demoted.
      count = 0;
      continue;
    }
    count++;

    if (ns->mirror_peer_uuids.empty() || 
        it->state == cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE) {
      auto next_snap = std::next(it);
      if (next_snap != m_group_snaps.end() &&
        next_snap->state == cls::rbd::GROUP_SNAPSHOT_STATE_COMPLETE) {
	unlink_snap = it;
	unlink = true;
        break;
      }
    }
    if (count == m_max_snaps) {
      unlink_snap = it;
    }
    if (count > m_max_snaps) {
      unlink = true;
      break;
    }
  }

  if (unlink == true && (unlink_snap != m_group_snaps.end())) {
    remove_group_snapshot(*unlink_snap);
  } else {
    ldout(m_cct, 10)<< "No more snaps" << dendl;
    finish(0);
  }
}


template <typename I>
void GroupUnlinkPeerRequest<I>::list_group_snaps() {
  ldout(m_cct, 10) << dendl;

  auto ctx = util::create_context_callback<
    GroupUnlinkPeerRequest<I>,
    &GroupUnlinkPeerRequest<I>::handle_list_group_snaps>(
      this);

  m_group_snaps.clear();
  auto req = group::ListSnapshotsRequest<I>::create(
    m_group_io_ctx, m_group_id, true, true, &m_group_snaps, ctx);

  req->send();
}



template <typename I>
void GroupUnlinkPeerRequest<I>::handle_list_group_snaps(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to list group snapshots of group ID '"
                 << m_group_id << "': " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  unlink_peer();
}

template <typename I>
void GroupUnlinkPeerRequest<I>::remove_group_snapshot(
                              cls::rbd::GroupSnapshot group_snap) {
  ldout(m_cct, 10) << "group snap id: " << group_snap.id << dendl;

  if (!m_image_ctxs->empty() && m_image_ctx_map.empty()) {
    for (size_t i = 0; i < m_image_ctxs->size(); ++i) {
      ImageCtx *ictx = (*m_image_ctxs)[i];
      m_image_ctx_map[ictx->id] = ictx;
    }
  }

  auto ctx = create_context_callback<
      GroupUnlinkPeerRequest,
      &GroupUnlinkPeerRequest<I>::handle_remove_group_snapshot>(this);

  m_group_snap_id = group_snap.id;

  C_Gather *gather_ctx = new C_Gather(g_ceph_context, ctx);
  for (auto &snap : group_snap.snaps) {
    if (snap.snap_id == CEPH_NOSNAP) {
      continue;
    }
    ImageCtx *ictx = nullptr;
    if (m_image_ctx_map.find(snap.image_id) != m_image_ctx_map.end()) {
      ictx = m_image_ctx_map[snap.image_id];
    }

    if (!ictx) {
      ldout(m_cct, 10) << "Failed to remove individual snapshot: " << dendl;
      continue;
    }
    ldout(m_cct, 10) << "removing individual snapshot: "
                   << snap.snap_id << ", from image id:" << snap.image_id << dendl;
    remove_image_snapshot(ictx, snap.snap_id, gather_ctx);
  }

  gather_ctx->activate();

}

template <typename I>
void GroupUnlinkPeerRequest<I>::handle_remove_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove image snapshot metadata: "
               << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

// Todo: refresh images ?
  // Make this async!
  remove_snap();
}

template <typename I>
void GroupUnlinkPeerRequest<I>::remove_snap() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::group_snap_remove(&op,m_group_snap_id);

  auto aio_comp = create_rados_callback<
    GroupUnlinkPeerRequest<I>,
    &GroupUnlinkPeerRequest<I>::handle_remove_snap>(this);
  int r = m_group_io_ctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                     aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void GroupUnlinkPeerRequest<I>::handle_remove_snap(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove group snapshot metadata: "
               << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }
  list_group_snaps();
}

template <typename I>
void GroupUnlinkPeerRequest<I>::remove_image_snapshot(ImageCtx *image_ctx,
                                                      uint64_t snap_id,
                                                      C_Gather *gather_ctx) {
  ldout(m_cct, 10) << snap_id << dendl;

  int r = -ENOENT;
  cls::rbd::SnapshotNamespace snap_namespace;
  std::string snap_name;

  image_ctx->image_lock.lock_shared();
  auto snap_it = image_ctx->snap_info.find(snap_id);
  if (snap_it != image_ctx->snap_info.end()) {
    r = 0;
    snap_namespace = snap_it->second.snap_namespace;
    snap_name = snap_it->second.name;
  }

  image_ctx->image_lock.unlock_shared();
  if (r == -ENOENT) {
    ldout(m_cct, 10) << "missing snapshot: snap_id=" << snap_id << dendl;
    return;
  }
  auto mirror_ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
    &snap_namespace);
  if (mirror_ns == nullptr) {
    lderr(m_cct) << "not mirror snapshot (snap_id=" << snap_id << ")" << dendl;
    return;
  }
  image_ctx->operations->snap_remove(snap_namespace, snap_name.c_str(),
                                     gather_ctx->new_sub());
}

template <typename I>
void GroupUnlinkPeerRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::GroupUnlinkPeerRequest<librbd::ImageCtx>;
