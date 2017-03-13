// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/PromoteRequest.h"
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
#define dout_prefix *_dout << "librbd::mirror::PromoteRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using librbd::util::create_context_callback;

template <typename I>
void PromoteRequest<I>::send() {
  get_info();
}

template <typename I>
void PromoteRequest<I>::get_info() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    PromoteRequest<I>, &PromoteRequest<I>::handle_get_info>(this);
  auto req = GetInfoRequest<I>::create(m_image_ctx, &m_mirror_image,
                                       &m_promotion_state, ctx);
  req->send();
}

template <typename I>
void PromoteRequest<I>::handle_get_info(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to retrieve mirroring state: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  } else if (m_mirror_image.state != cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
    lderr(cct) << "mirroring is not currently enabled" << dendl;
    finish(-EINVAL);
    return;
  } else if (m_promotion_state == PROMOTION_STATE_PRIMARY) {
    lderr(cct) << "image is already primary" << dendl;
    finish(-EINVAL);
    return;
  } else if (m_promotion_state == PROMOTION_STATE_NON_PRIMARY && !m_force) {
    lderr(cct) << "image is still primary within a remote cluster" << dendl;
    finish(-EBUSY);
    return;
  }

  promote();
}

template <typename I>
void PromoteRequest<I>::promote() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    PromoteRequest<I>, &PromoteRequest<I>::handle_promote>(this);
  Journal<I>::promote(&m_image_ctx, ctx);
}

template <typename I>
void PromoteRequest<I>::handle_promote(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to promote image: " << cpp_strerror(r)
               << dendl;
  }

  finish(r);
}

template <typename I>
void PromoteRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::PromoteRequest<librbd::ImageCtx>;
