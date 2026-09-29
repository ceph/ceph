// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GroupRemoveImageRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "librbd/mirror/ImageRemoveRequest.h"
#include "librbd/mirror/ImageStateUpdateRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GroupRemoveImageRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using librbd::util::create_context_callback;

template <typename I>
GroupRemoveImageRequest<I>::GroupRemoveImageRequest(I* image_ctx,
                                                    Context* on_finish)
  : m_image_ctx(image_ctx), m_on_finish(on_finish),
    m_cct(reinterpret_cast<CephContext*>(image_ctx->md_ctx.cct())) {
  ldout(m_cct, 10) << "removing image " << m_image_ctx->id << dendl;
}

template <typename I>
void GroupRemoveImageRequest<I>::send() {
  ldout(m_cct, 10) << dendl;

  get_mirror_info();
}

template <typename I>
void GroupRemoveImageRequest<I>::get_mirror_info() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupRemoveImageRequest<I>,
      &GroupRemoveImageRequest<I>::handle_get_mirror_info>(this);

  auto req = GetInfoRequest<I>::create(*m_image_ctx, &m_mirror_image,
                                       &m_promotion_state, &m_mirror_uuid, ctx);

  req->send();
}

template <typename I>
void GroupRemoveImageRequest<I>::handle_get_mirror_info(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    ldout(m_cct, 10) << "mirror info not found" << dendl;
    finish(0);
    return;
  }

  if (r < 0) {
    lderr(m_cct) << "failed to get mirror info: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  // The group membership was already restored by GroupPromoteRequest. Disable
  // mirroring before the caller removes this image.
  set_mirror_image_disabling();
}

template <typename I>
void GroupRemoveImageRequest<I>::set_mirror_image_disabling() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupRemoveImageRequest<I>,
    &GroupRemoveImageRequest<I>::handle_set_mirror_image_disabling>(this);

  auto req = ImageStateUpdateRequest<I>::create(m_image_ctx->md_ctx,
    m_image_ctx->id, cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
    m_mirror_image, ctx);

  req->send();
}

template <typename I>
void GroupRemoveImageRequest<I>::handle_set_mirror_image_disabling(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed setting DISABLING: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  remove_global_mirror_image_entry();
}

template <typename I>
void GroupRemoveImageRequest<I>::remove_global_mirror_image_entry() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupRemoveImageRequest<I>,
    &GroupRemoveImageRequest<I>::handle_remove_global_mirror_image_entry>(this);

  auto req = ImageRemoveRequest<I>::create(m_image_ctx->md_ctx,
    m_mirror_image.global_image_id, m_image_ctx->id, ctx);

  req->send();
}

template <typename I>
void GroupRemoveImageRequest<I>::handle_remove_global_mirror_image_entry(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void GroupRemoveImageRequest<I>::finish(int r) {
  ldout(m_cct, 10) << r << dendl;

  Context* on_finish = m_on_finish;
  delete this;
  on_finish->complete(r);
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GroupRemoveImageRequest<librbd::ImageCtx>;
