// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/mirror/snapshot/RemoveStandaloneSnapshotsRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include <shared_mutex>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::" \
  << "RemoveStandaloneSnapshotsRequest: " << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

template <typename I>
void RemoveStandaloneSnapshotsRequest<I>::send() {
  ldout(m_image_ctx->cct, 10) << "image_id=" << m_image_ctx->id << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_image_get_start(&op, m_image_ctx->id);
  auto comp = util::create_rados_callback<RemoveStandaloneSnapshotsRequest<I>,
    &RemoveStandaloneSnapshotsRequest<I>::handle_get_mirror_image>(this);
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void RemoveStandaloneSnapshotsRequest<I>::handle_get_mirror_image(int r) {
  ldout(m_image_ctx->cct, 10) << "image_id=" << m_image_ctx->id
                             << ", r=" << r << dendl;

  cls::rbd::MirrorImage mirror_image;
  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = cls_client::mirror_image_get_finish(&it, &mirror_image);
  }
  if (r < 0) {
    finish(r);
    return;
  }
  // Never discard the snapshots of an independently mirrored image.
  if (mirror_image.type != cls::rbd::MIRROR_IMAGE_TYPE_GROUP) {
    lderr(m_image_ctx->cct) << "image " << m_image_ctx->id
                           << " is not group mirrored" << dendl;
    finish(-EINVAL);
    return;
  }
  auto ctx = util::create_context_callback<RemoveStandaloneSnapshotsRequest<I>,
    &RemoveStandaloneSnapshotsRequest<I>::handle_refresh>(this);
  m_image_ctx->state->refresh(ctx);
}

template <typename I>
void RemoveStandaloneSnapshotsRequest<I>::handle_refresh(int r) {
  ldout(m_image_ctx->cct, 10) << "image_id=" << m_image_ctx->id
                             << ", r=" << r << dendl;

  if (r < 0) {
    finish(r);
    return;
  }
  {
    std::shared_lock locker{m_image_ctx->image_lock};
    if (m_image_ctx->group_spec != m_group_spec) {
      lderr(m_image_ctx->cct) << "image " << m_image_ctx->id
                             << " group membership changed" << dendl;
      locker.unlock();
      finish(-ESTALE);
      return;
    }
    for (const auto& [snap_id, snap_info] : m_image_ctx->snap_info) {
      auto ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
        &snap_info.snap_namespace);
      if (ns != nullptr && !ns->group_spec.is_valid() &&
          ns->group_snap_id.empty()) {
        m_snapshots.emplace_back(snap_info.snap_namespace, snap_info.name);
      }
    }
  }
  remove_snapshot();
}

template <typename I>
void RemoveStandaloneSnapshotsRequest<I>::remove_snapshot() {
  ldout(m_image_ctx->cct, 10) << "image_id=" << m_image_ctx->id
                             << ", remaining=" << m_snapshots.size() << dendl;

  if (m_snapshots.empty()) {
    finish(0);
    return;
  }
  auto cct = m_image_ctx->cct;
  auto& [snap_namespace, snap_name] = m_snapshots.front();
  ldout(cct, 10) << "image_id=" << m_image_ctx->id
                 << ", snap_name=" << snap_name << dendl;
  auto ctx = util::create_context_callback<RemoveStandaloneSnapshotsRequest<I>,
    &RemoveStandaloneSnapshotsRequest<I>::handle_remove_snapshot>(this);
  m_image_ctx->operations->snap_remove(snap_namespace, snap_name, ctx);
}

template <typename I>
void RemoveStandaloneSnapshotsRequest<I>::handle_remove_snapshot(int r) {
  ldout(m_image_ctx->cct, 10) << "image_id=" << m_image_ctx->id
                             << ", snap_name=" << m_snapshots.front().second
                             << ", r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    auto cct = m_image_ctx->cct;
    lderr(cct) << "failed to remove standalone snapshot: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }
  m_snapshots.pop_front();
  remove_snapshot();
}

template <typename I>
void RemoveStandaloneSnapshotsRequest<I>::finish(int r) {
  ldout(m_image_ctx->cct, 10) << "image_id=" << m_image_ctx->id
                             << ", r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::RemoveStandaloneSnapshotsRequest<
  librbd::ImageCtx>;
