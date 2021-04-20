// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/snapshot/DemoteRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/mirror/snapshot/CreatePrimaryRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::snapshot::DemoteRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {
namespace snapshot {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void DemoteRequest<I>::send() {
  enable_non_primary_feature();
}

template <typename I>
void DemoteRequest<I>::enable_non_primary_feature() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  // ensure image is flagged with non-primary feature so that
  // standard RBD clients cannot write to it.
  librados::ObjectWriteOperation op;
  cls_client::set_features(&op, RBD_FEATURE_NON_PRIMARY,
                           RBD_FEATURE_NON_PRIMARY);

  auto aio_comp = create_rados_callback<
    DemoteRequest<I>,
    &DemoteRequest<I>::handle_enable_non_primary_feature>(this);
  int r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, aio_comp,
                                          &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void DemoteRequest<I>::handle_enable_non_primary_feature(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to enable non-primary feature: "
               << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  create_snapshot();
}

template <typename I>
void DemoteRequest<I>::create_snapshot() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << dendl;

  auto ctx = create_context_callback<
    DemoteRequest<I>, &DemoteRequest<I>::handle_create_snapshot>(this);

  auto req = CreatePrimaryRequest<I>::create(
    m_image_ctx, m_global_image_id, CEPH_NOSNAP,
    (snapshot::CREATE_PRIMARY_FLAG_IGNORE_EMPTY_PEERS |
     snapshot::CREATE_PRIMARY_FLAG_DEMOTED), nullptr, ctx);
  req->send();
}

template <typename I>
void DemoteRequest<I>::handle_create_snapshot(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to create mirror snapshot: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void DemoteRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 15) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

template class librbd::mirror::snapshot::DemoteRequest<librbd::ImageCtx>;
