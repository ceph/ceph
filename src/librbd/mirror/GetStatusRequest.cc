// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GetStatusRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/mirror/GetInfoRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GetStatusRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void GetStatusRequest<I>::send() {
  *m_mirror_image_status = cls::rbd::MirrorImageStatus(
    {{cls::rbd::MirrorImageSiteStatus::LOCAL_MIRROR_UUID,
      cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN, "status not found"}});

  get_info();
}

template <typename I>
void GetStatusRequest<I>::get_info() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    GetStatusRequest<I>, &GetStatusRequest<I>::handle_get_info>(this);
  auto req = GetInfoRequest<I>::create(m_image_ctx, m_mirror_image,
                                       m_promotion_state,
                                       &m_primary_mirror_uuid, ctx);
  req->send();
}

template <typename I>
void GetStatusRequest<I>::handle_get_info(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    if (r != -ENOENT) {
      lderr(cct) << "failed to retrieve mirroring state: " << cpp_strerror(r)
                 << dendl;
    }
    finish(r);
    return;
  } else if (m_mirror_image->state != cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
    finish(0);
    return;
  }

  get_status();
}

template <typename I>
void GetStatusRequest<I>::get_status() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_image_status_get_start(
    &op, m_mirror_image->global_image_id);

  librados::AioCompletion *comp = create_rados_callback<
    GetStatusRequest<I>, &GetStatusRequest<I>::handle_get_status>(this);
  int r = m_image_ctx.md_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GetStatusRequest<I>::handle_get_status(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::mirror_image_status_get_finish(&iter,
                                                   m_mirror_image_status);
  }

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to retrieve mirror image status: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void GetStatusRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GetStatusRequest<librbd::ImageCtx>;
