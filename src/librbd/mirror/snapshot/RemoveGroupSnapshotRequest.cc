// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/ceph_assert.h"
#include "common/Cond.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/api/Utils.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::RemoveGroupSnapshotRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using util::create_rados_callback;
using util::create_context_callback;

template <typename I>
void RemoveGroupSnapshotRequest<I>::send() {
  ldout(m_cct, 10) << dendl;

  remove_group_image_snapshots();
}

template <typename I>
void RemoveGroupSnapshotRequest<I>::remove_group_image_snapshots() {
  ldout(m_cct, 10) << "group snap id: " << m_group_snap->id << dendl;

  if (m_image_ctxs->empty()) {
    ldout(m_cct, 10) << "no images" << dendl;
    remove_group_snapshot();
    return;
  }

  auto ctx = create_context_callback<
      RemoveGroupSnapshotRequest,
      &RemoveGroupSnapshotRequest<I>::handle_remove_group_image_snapshots>(this);

  C_Gather *gather_ctx = new C_Gather(m_cct, ctx);
  for (size_t i = 0; i < m_group_snap->snaps.size(); ++i) {
    auto &snap = m_group_snap->snaps[i];
    if (snap.snap_id == CEPH_NOSNAP) {
      continue;
    }

    ImageCtx *ictx = (*m_image_ctxs)[i];
    if (!ictx) {
      ldout(m_cct, 10) << "failed to remove individual snapshot" << dendl;
      continue;
    }

    ldout(m_cct, 10) << "removing individual snapshot: "
                     << snap.snap_id << ", from image id:"
                     << snap.image_id << dendl;
    remove_image_snapshot(ictx, snap.snap_id, gather_ctx);
  }

  gather_ctx->activate();

}

template <typename I>
void RemoveGroupSnapshotRequest<I>::handle_remove_group_image_snapshots(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to remove individual group image snapshots: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  remove_group_snapshot();
}

template <typename I>
void RemoveGroupSnapshotRequest<I>::remove_group_snapshot() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::group_snap_remove(&op, m_group_snap->id);

  auto aio_comp = create_rados_callback<
    RemoveGroupSnapshotRequest<I>,
    &RemoveGroupSnapshotRequest<I>::handle_remove_group_snapshot>(this);
  int r = m_group_io_ctx.aio_operate(librbd::util::group_header_name(m_group_id),
                                     aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RemoveGroupSnapshotRequest<I>::handle_remove_group_snapshot(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove group snapshot metadata: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void RemoveGroupSnapshotRequest<I>::remove_image_snapshot(ImageCtx *image_ctx,
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
  image_ctx->operations->snap_remove(snap_namespace, snap_name.c_str(),
                                     gather_ctx->new_sub());
}

template <typename I>
void RemoveGroupSnapshotRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::RemoveGroupSnapshotRequest<librbd::ImageCtx>;
