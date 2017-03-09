// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GetInfoRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GetInfoRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void GetInfoRequest<I>::send() {
  refresh_image();
}

template <typename I>
void GetInfoRequest<I>::refresh_image() {
  if (!m_image_ctx.state->is_refresh_required()) {
    get_mirror_image();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    GetInfoRequest<I>, &GetInfoRequest<I>::handle_refresh_image>(this);
  m_image_ctx.state->refresh(ctx);
}

template <typename I>
void GetInfoRequest<I>::handle_refresh_image(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to refresh image: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  get_mirror_image();
}

template <typename I>
void GetInfoRequest<I>::get_mirror_image() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_image_get_start(&op, m_image_ctx.id);

  librados::AioCompletion *comp = create_rados_callback<
    GetInfoRequest<I>, &GetInfoRequest<I>::handle_get_mirror_image>(this);
  int r = m_image_ctx.md_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
void GetInfoRequest<I>::handle_get_mirror_image(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "r=" << r << dendl;

  m_mirror_image->state = cls::rbd::MIRROR_IMAGE_STATE_DISABLED;
  *m_promotion_state = PROMOTION_STATE_NON_PRIMARY;
  if (r == 0) {
    bufferlist::iterator iter = m_out_bl.begin();
    r = cls_client::mirror_image_get_finish(&iter, m_mirror_image);
  }

  if (r == -ENOENT ||
      m_mirror_image->state != cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
    ldout(cct, 20) << "mirroring is disabled" << dendl;
    finish(0);
    return;
  } else if (r < 0) {
    lderr(cct) << "failed to retrieve mirroring state: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  get_tag_owner();
}

template <typename I>
void GetInfoRequest<I>::get_tag_owner() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  auto ctx = create_context_callback<
    GetInfoRequest<I>, &GetInfoRequest<I>::handle_get_tag_owner>(this);
  Journal<I>::get_tag_owner(m_image_ctx.md_ctx, m_image_ctx.id,
                            &m_mirror_uuid, m_image_ctx.op_work_queue, ctx);
}

template <typename I>
void GetInfoRequest<I>::handle_get_tag_owner(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to determine tag ownership: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  if (m_mirror_uuid == Journal<>::LOCAL_MIRROR_UUID) {
    *m_promotion_state = PROMOTION_STATE_PRIMARY;
  } else if (m_mirror_uuid == Journal<>::ORPHAN_MIRROR_UUID) {
    *m_promotion_state = PROMOTION_STATE_ORPHAN;
  }

  finish(0);
}

template <typename I>
void GetInfoRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GetInfoRequest<librbd::ImageCtx>;
