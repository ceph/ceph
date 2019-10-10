// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/CreateNonPrimaryRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd

#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::CreateNonPrimaryRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void CreateNonPrimaryRequest<I>::send() {
  refresh_image();
}

template <typename I>
void CreateNonPrimaryRequest<I>::refresh_image() {
  if (!m_image_ctx->state->is_refresh_required()) {
    get_mirror_image();
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    CreateNonPrimaryRequest<I>,
    &CreateNonPrimaryRequest<I>::handle_refresh_image>(this);
  m_image_ctx->state->refresh(ctx);
}

template <typename I>
void CreateNonPrimaryRequest<I>::handle_refresh_image(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to refresh image: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_mirror_image();
}

template <typename I>
void CreateNonPrimaryRequest<I>::get_mirror_image() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_image_get_start(&op, m_image_ctx->id);

  librados::AioCompletion *comp = create_rados_callback<
    CreateNonPrimaryRequest<I>,
    &CreateNonPrimaryRequest<I>::handle_get_mirror_image>(this);
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void CreateNonPrimaryRequest<I>::handle_get_mirror_image(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  cls::rbd::MirrorImage mirror_image;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::mirror_image_get_finish(&iter, &mirror_image);
  }

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to retrieve mirroring state: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  if (mirror_image.mode != cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT) {
    lderr(cct) << "snapshot based mirroring is not enabled" << dendl;
    finish(-EINVAL);
    return;
  }

  if (!validate_snapshot()) {
    finish(-EINVAL);
    return;
  }

  uuid_d uuid_gen;
  uuid_gen.generate_random();
  m_snap_name = ".mirror.non_primary." + mirror_image.global_image_id + "." +
    uuid_gen.to_string();

  create_snapshot();
}

template <typename I>
void CreateNonPrimaryRequest<I>::create_snapshot() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  cls::rbd::MirrorNonPrimarySnapshotNamespace ns{m_primary_mirror_uuid,
                                                 m_primary_snap_id};
  auto ctx = create_context_callback<
    CreateNonPrimaryRequest<I>,
    &CreateNonPrimaryRequest<I>::handle_create_snapshot>(this);
  m_image_ctx->operations->snap_create(ns, m_snap_name, ctx);
}

template <typename I>
void CreateNonPrimaryRequest<I>::handle_create_snapshot(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to create mirror snapshot: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  if (m_snap_id != nullptr) {
    std::shared_lock image_locker{m_image_ctx->image_lock};
    cls::rbd::MirrorNonPrimarySnapshotNamespace ns{m_primary_mirror_uuid,
                                                   m_primary_snap_id};
    *m_snap_id = m_image_ctx->get_snap_id(ns, m_snap_name);
  }

  finish(0);
}

template <typename I>
void CreateNonPrimaryRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

template <typename I>
bool CreateNonPrimaryRequest<I>::validate_snapshot() {
  CephContext *cct = m_image_ctx->cct;

  std::shared_lock image_locker{m_image_ctx->image_lock};

  for (auto it = m_image_ctx->snap_info.rbegin();
       it != m_image_ctx->snap_info.rend(); it++) {
    auto primary = boost::get<cls::rbd::MirrorPrimarySnapshotNamespace>(
      &it->second.snap_namespace);
    if (primary != nullptr) {
      ldout(cct, 20) << "previous mirror snapshot snap_id=" << it->first << " "
                     << *primary << dendl;
      if (!primary->demoted) {
        lderr(cct) << "trying to create non-primary snapshot "
                   << "when previous primary snapshot is not in demoted state"
                   << dendl;
        return false;
      }
      return true;
    }
    auto non_primary = boost::get<cls::rbd::MirrorNonPrimarySnapshotNamespace>(
      &it->second.snap_namespace);
    if (non_primary == nullptr) {
      continue;
    }
    ldout(cct, 20) << "previous snapshot snap_id=" << it->first << " "
                   << *non_primary << dendl;
    if (!non_primary->copied) {
      lderr(cct) << "trying to create non-primary snapshot "
                 << "when previous non-primary snapshot is not copied yet"
                 << dendl;
      return false;
    }
    return true;
  }

  ldout(cct, 20) << "no previous mirror snapshots found" << dendl;
  return true;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::CreateNonPrimaryRequest<librbd::ImageCtx>;
