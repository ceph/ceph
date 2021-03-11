// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/SetImageStateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/image/GetMetadataRequest.h"
#include "librbd/mirror/snapshot/WriteImageStateRequest.h"

#include <boost/algorithm/string/predicate.hpp>

#define dout_subsys ceph_subsys_rbd

#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror_snapshot::SetImageStateRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void SetImageStateRequest<I>::send() {
  get_name();
}

template <typename I>
void SetImageStateRequest<I>::get_name() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  librados::ObjectReadOperation op;
  cls_client::dir_get_name_start(&op, m_image_ctx->id);

  librados::AioCompletion *comp = create_rados_callback<
    SetImageStateRequest<I>,
    &SetImageStateRequest<I>::handle_get_name>(this);
  m_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(RBD_DIRECTORY, comp, &op, &m_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void SetImageStateRequest<I>::handle_get_name(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r == 0) {
    auto it = m_bl.cbegin();
    r = cls_client::dir_get_name_finish(&it, &m_image_state.name);
  }

  if (r < 0) {
    lderr(cct) << "failed to retrieve image name: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  ldout(cct, 15) << "name=" << m_image_state.name << dendl;

  get_snap_limit();
}

template <typename I>
void SetImageStateRequest<I>::get_snap_limit() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  librados::ObjectReadOperation op;
  cls_client::snapshot_get_limit_start(&op);

  librados::AioCompletion *comp = create_rados_callback<
    SetImageStateRequest<I>,
    &SetImageStateRequest<I>::handle_get_snap_limit>(this);
  m_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                          &m_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void SetImageStateRequest<I>::handle_get_snap_limit(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r == 0) {
    auto it = m_bl.cbegin();
    r = cls_client::snapshot_get_limit_finish(&it, &m_image_state.snap_limit);
  }

  if (r < 0) {
    lderr(cct) << "failed to retrieve snapshot limit: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  ldout(cct, 20) << "snap_limit=" << m_image_state.snap_limit << dendl;

  get_metadata();
}

template <typename I>
void SetImageStateRequest<I>::get_metadata() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
     SetImageStateRequest<I>,
     &SetImageStateRequest<I>::handle_get_metadata>(this);
  auto req = image::GetMetadataRequest<I>::create(
    m_image_ctx->md_ctx, m_image_ctx->header_oid, true, "", "", 0,
    &m_image_state.metadata, ctx);
  req->send();
}

template <typename I>
void SetImageStateRequest<I>::handle_get_metadata(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to retrieve metadata: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  {
    std::shared_lock image_locker{m_image_ctx->image_lock};

    m_image_state.features =
      m_image_ctx->features & ~RBD_FEATURES_IMPLICIT_ENABLE;

    for (auto &[snap_id, snap_info] : m_image_ctx->snap_info) {
      auto type = cls::rbd::get_snap_namespace_type(snap_info.snap_namespace);
      if (type != cls::rbd::SNAPSHOT_NAMESPACE_TYPE_USER) {
        // only replicate user snapshots -- trash snapshots will be
        // replicated by an implicit delete if required
        continue;
      }
      m_image_state.snapshots[snap_id] = {snap_info.snap_namespace,
                                          snap_info.name,
                                          snap_info.protection_status};
    }
  }

  write_image_state();
}

template <typename I>
void SetImageStateRequest<I>::write_image_state() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    SetImageStateRequest<I>,
    &SetImageStateRequest<I>::handle_write_image_state>(this);

  auto req = WriteImageStateRequest<I>::create(m_image_ctx, m_snap_id,
                                               m_image_state, ctx);
  req->send();
}

template <typename I>
void SetImageStateRequest<I>::handle_write_image_state(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to write image state: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  update_primary_snapshot();
}

template <typename I>
void SetImageStateRequest<I>::update_primary_snapshot() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_snapshot_set_copy_progress(
    &op, m_snap_id, true, 0);

  auto aio_comp = create_rados_callback<
    SetImageStateRequest<I>,
    &SetImageStateRequest<I>::handle_update_primary_snapshot>(this);
  int r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, aio_comp,
                                          &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void SetImageStateRequest<I>::handle_update_primary_snapshot(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to update primary snapshot: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void SetImageStateRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::SetImageStateRequest<librbd::ImageCtx>;
