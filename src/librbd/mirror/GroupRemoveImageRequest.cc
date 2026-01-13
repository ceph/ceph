// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GroupRemoveImageRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "librbd/mirror/ImageRemoveRequest.h"
#include "librbd/mirror/ImageStateUpdateRequest.h"
#include "librbd/mirror/snapshot/PromoteRequest.h"
#include "librbd/mirror/snapshot/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GroupRemoveImageRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
GroupRemoveImageRequest<I>::GroupRemoveImageRequest(I* image_ctx,
                                                    const std::string& group_id,
                                                    librados::IoCtx& group_io_ctx,
                                                    Context* on_finish)
  : m_image_ctx(image_ctx), m_group_id(group_id),
    m_group_io_ctx(group_io_ctx), m_on_finish(on_finish),
    m_cct(reinterpret_cast<CephContext*>(image_ctx->md_ctx.cct())) {
  ldout(m_cct, 10) << "removing image " << m_image_ctx->id
                   << " from group " << m_group_id << dendl;
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
    remove_group_ref_from_image();
    return;
  }

  if (r < 0) {
    lderr(m_cct) << "failed to get mirror info: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  promote_image();
}

template <typename I>
void GroupRemoveImageRequest<I>::promote_image() {
  ldout(m_cct, 10) << dendl;

  bool is_primary = (m_promotion_state == PROMOTION_STATE_PRIMARY ||
                     m_promotion_state == PROMOTION_STATE_UNKNOWN);
  ldout(m_cct, 10) << "image primary=" << is_primary << dendl;

  if (is_primary) {
    set_mirror_image_disabling();
    return;
  }

  ldout(m_cct, 10) << "promoting non-primary image " << m_image_ctx->id
                   << dendl;
  auto ctx = create_context_callback<GroupRemoveImageRequest<I>,
    &GroupRemoveImageRequest<I>::handle_promote_image>(this);

  auto req = snapshot::PromoteRequest<I>::create(m_image_ctx,
    m_mirror_image.global_image_id, ctx);

  req->send();
}

template <typename I>
void GroupRemoveImageRequest<I>::handle_promote_image(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "promotion failed: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

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

  remove_group_ref_from_image();
}

template <typename I>
void GroupRemoveImageRequest<I>::remove_group_ref_from_image() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::image_group_remove(&op, {m_group_id, m_group_io_ctx.get_id()});

  auto comp = create_rados_callback<GroupRemoveImageRequest<I>,
    &GroupRemoveImageRequest<I>::handle_remove_group_ref_from_image>(this);

  int r = m_image_ctx->md_ctx.aio_operate(util::header_name(m_image_ctx->id),
                                          comp, &op);
  ceph_assert(r == 0);

  comp->release();
}

template <typename I>
void GroupRemoveImageRequest<I>::handle_remove_group_ref_from_image(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed removing group reference from image: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  remove_image_from_group();
}

template <typename I>
void GroupRemoveImageRequest<I>::remove_image_from_group() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::group_image_remove(
      &op, {m_image_ctx->id, m_image_ctx->md_ctx.get_id()});

  auto comp = create_rados_callback<GroupRemoveImageRequest<I>,
    &GroupRemoveImageRequest<I>::handle_remove_image_from_group>(this);

  int r = m_group_io_ctx.aio_operate(
      util::group_header_name(m_group_id), comp, &op);
  ceph_assert(r == 0);

  comp->release();
}

template <typename I>
void GroupRemoveImageRequest<I>::handle_remove_image_from_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to remove group image entry: "
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

  close_image();
}

template <typename I>
void GroupRemoveImageRequest<I>::close_image() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<GroupRemoveImageRequest<I>,
    &GroupRemoveImageRequest<I>::handle_close>(this);

  m_image_ctx->state->close(ctx);
}

template <typename I>
void GroupRemoveImageRequest<I>::handle_close(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;
  finish(r);
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
