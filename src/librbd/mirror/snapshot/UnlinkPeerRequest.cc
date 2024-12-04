// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/UnlinkPeerRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::UnlinkPeerRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void UnlinkPeerRequest<I>::send() {
  if (!m_image_ctx->state->is_refresh_required()) {
    unlink_peer();
    return;
  }

  refresh_image();
}

template <typename I>
void UnlinkPeerRequest<I>::refresh_image() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  auto ctx = create_context_callback<
    UnlinkPeerRequest<I>, &UnlinkPeerRequest<I>::handle_refresh_image>(this);
  m_image_ctx->state->refresh(ctx);
}

template <typename I>
void UnlinkPeerRequest<I>::handle_refresh_image(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to refresh image: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  unlink_peer();
}

template <typename I>
void UnlinkPeerRequest<I>::unlink_peer() {
  CephContext *cct = m_image_ctx->cct;

  m_image_ctx->image_lock.lock_shared();
  int r = -ENOENT;
  cls::rbd::SnapshotNamespace snap_namespace;
  std::string snap_name;
  bool have_newer_mirror_snapshot = false;
  for (auto snap_it = m_image_ctx->snap_info.find(m_snap_id);
       snap_it != m_image_ctx->snap_info.end(); ++snap_it) {
    if (snap_it->first == m_snap_id) {
      r = 0;
      snap_namespace = snap_it->second.snap_namespace;
      snap_name = snap_it->second.name;
    } else if (std::holds_alternative<cls::rbd::MirrorSnapshotNamespace>(
                 snap_it->second.snap_namespace)) {
      ldout(cct, 15) << "located newer mirror snapshot" << dendl;
      have_newer_mirror_snapshot = true;
      break;
    }
  }

  if (r == -ENOENT) {
    ldout(cct, 15) << "missing snapshot: snap_id=" << m_snap_id << dendl;
    m_image_ctx->image_lock.unlock_shared();
    finish(r);
    return;
  }

  auto mirror_ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
    &snap_namespace);
  if (mirror_ns == nullptr) {
    lderr(cct) << "not mirror snapshot (snap_id=" << m_snap_id << ")" << dendl;
    m_image_ctx->image_lock.unlock_shared();
    finish(-EINVAL);
    return;
  }

  // if there is or will be no more peers in the mirror snapshot and we have
  // a more recent mirror snapshot, remove the older one
  if ((mirror_ns->mirror_peer_uuids.empty() ||
       (mirror_ns->mirror_peer_uuids.size() == 1 &&
        mirror_ns->mirror_peer_uuids.count(m_mirror_peer_uuid) != 0)) &&
      have_newer_mirror_snapshot) {
    if (m_allow_remove) {
      m_image_ctx->image_lock.unlock_shared();
      remove_snapshot(snap_namespace, snap_name);
      return;
    } else {
      ldout(cct, 15) << "skipping removal of snapshot: snap_id=" << m_snap_id
                     << ", mirror_peer_uuid=" << m_mirror_peer_uuid
                     << ", mirror_peer_uuids=" << mirror_ns->mirror_peer_uuids
                     << dendl;
    }
  }

  if (mirror_ns->mirror_peer_uuids.count(m_mirror_peer_uuid) == 0) {
    ldout(cct, 15) << "no peer to unlink: snap_id=" << m_snap_id
                   << ", mirror_peer_uuid=" << m_mirror_peer_uuid
                   << ", mirror_peer_uuids=" << mirror_ns->mirror_peer_uuids
                   << dendl;
    m_image_ctx->image_lock.unlock_shared();
    finish(0);
    return;
  }

  m_image_ctx->image_lock.unlock_shared();

  ldout(cct, 15) << "snap_id=" << m_snap_id << ", "
                 << "mirror_peer_uuid=" << m_mirror_peer_uuid << dendl;
  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_snapshot_unlink_peer(&op, m_snap_id,
                                                        m_mirror_peer_uuid);
  auto aio_comp = create_rados_callback<
    UnlinkPeerRequest<I>, &UnlinkPeerRequest<I>::handle_unlink_peer>(this);
  r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void UnlinkPeerRequest<I>::handle_unlink_peer(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r == -ERESTART || r == -ENOENT) {
    if (r == -ERESTART) {
      ldout(cct, 15) << "unlinking last peer not supported" << dendl;
      m_allow_remove = true;
    }
    refresh_image();
    return;
  }

  if (r < 0) {
    lderr(cct) << "failed to unlink peer: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  notify_update();
}

template <typename I>
void UnlinkPeerRequest<I>::notify_update() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  auto ctx = create_context_callback<
    UnlinkPeerRequest<I>, &UnlinkPeerRequest<I>::handle_notify_update>(this);
  m_image_ctx->notify_update(ctx);
}

template <typename I>
void UnlinkPeerRequest<I>::handle_notify_update(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r == -ENOENT || r == -ETIMEDOUT) {
    // non-fatel errors
    lderr(cct) << "failed to notify update: " << cpp_strerror(r) << dendl;
  } else if (r < 0) {
    lderr(cct) << "failed to notify update: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  refresh_image();
}

template <typename I>
void UnlinkPeerRequest<I>::remove_snapshot(
    const cls::rbd::SnapshotNamespace& snap_namespace,
    const std::string& snap_name) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  auto ctx = create_context_callback<
    UnlinkPeerRequest<I>, &UnlinkPeerRequest<I>::handle_remove_snapshot>(this);
  m_image_ctx->operations->snap_remove(snap_namespace, snap_name, ctx);
}

template <typename I>
void UnlinkPeerRequest<I>::handle_remove_snapshot(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to remove snapshot: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void UnlinkPeerRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  auto on_finish = m_on_finish;
  delete this;
  on_finish->complete(r);
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::UnlinkPeerRequest<librbd::ImageCtx>;
