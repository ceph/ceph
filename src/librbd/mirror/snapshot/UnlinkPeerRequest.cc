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
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    UnlinkPeerRequest<I>, &UnlinkPeerRequest<I>::handle_refresh_image>(this);
  m_image_ctx->state->refresh(ctx);
}

template <typename I>
void UnlinkPeerRequest<I>::handle_refresh_image(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

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

  auto snap_info = m_image_ctx->get_snap_info(m_snap_id);
  if (!snap_info) {
    m_image_ctx->image_lock.unlock_shared();
    finish(-ENOENT);
    return;
  }

  auto info = boost::get<cls::rbd::MirrorPrimarySnapshotNamespace>(
    &snap_info->snap_namespace);
  if (info == nullptr) {
    lderr(cct) << "not mirror primary snapshot (snap_id=" << m_snap_id << ")"
               << dendl;
    m_image_ctx->image_lock.unlock_shared();
    finish(-EINVAL);
    return;
  }

  if (info->mirror_peer_uuids.count(m_mirror_peer_uuid) == 0 ||
      info->mirror_peer_uuids.size() == 1U) {
    m_image_ctx->image_lock.unlock_shared();
    remove_snapshot();
    return;
  }

  m_image_ctx->image_lock.unlock_shared();

  ldout(cct, 20) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_snapshot_unlink_peer(&op, m_snap_id,
                                                        m_mirror_peer_uuid);
  auto aio_comp = create_rados_callback<
    UnlinkPeerRequest<I>, &UnlinkPeerRequest<I>::handle_unlink_peer>(this);
  int r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, aio_comp,
                                          &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void UnlinkPeerRequest<I>::handle_unlink_peer(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r == -ERESTART || r == -ENOENT) {
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
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    UnlinkPeerRequest<I>, &UnlinkPeerRequest<I>::handle_notify_update>(this);
  m_image_ctx->notify_update(ctx);
}

template <typename I>
void UnlinkPeerRequest<I>::handle_notify_update(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to notify update: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  refresh_image();
}

template <typename I>
void UnlinkPeerRequest<I>::remove_snapshot() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  cls::rbd::SnapshotNamespace snap_namespace;
  std::string snap_name;
  int r = 0;
  {
    std::shared_lock image_locker{m_image_ctx->image_lock};

    auto snap_info = m_image_ctx->get_snap_info(m_snap_id);
    if (!snap_info) {
      r = -ENOENT;
    } else {
      snap_namespace = snap_info->snap_namespace;
      snap_name = snap_info->name;
    }
  }

  if (r == ENOENT) {
    finish(0);
    return;
  }

  auto info = boost::get<cls::rbd::MirrorPrimarySnapshotNamespace>(
    &snap_namespace);
  ceph_assert(info);

  if (info->mirror_peer_uuids.size() > 1 ||
      info->mirror_peer_uuids.count(m_mirror_peer_uuid) == 0) {
    finish(0);
    return;
  }

  auto ctx = create_context_callback<
    UnlinkPeerRequest<I>, &UnlinkPeerRequest<I>::handle_remove_snapshot>(this);
  m_image_ctx->operations->snap_remove(snap_namespace, snap_name, ctx);
}

template <typename I>
void UnlinkPeerRequest<I>::handle_remove_snapshot(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

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
  ldout(cct, 20) << "r=" << r << dendl;

  auto on_finish = m_on_finish;
  delete this;
  on_finish->complete(r);
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::UnlinkPeerRequest<librbd::ImageCtx>;
