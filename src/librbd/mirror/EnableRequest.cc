// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/EnableRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::EnableRequest: "

namespace librbd {
namespace mirror {

using util::create_context_callback;
using util::create_rados_ack_callback;

template <typename I>
EnableRequest<I>::EnableRequest(I *image_ctx, Context *on_finish)
  : m_io_ctx(&image_ctx->md_ctx), m_image_ctx(image_ctx),
    m_on_finish(on_finish) {
}

template <typename I>
void EnableRequest<I>::send() {
  send_get_tag_owner();
}

template <typename I>
void EnableRequest<I>::send_get_tag_owner() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = EnableRequest<I>;
  Context *ctx = create_context_callback<
      klass, &klass::handle_get_tag_owner>(this);

  librbd::Journal<>::is_tag_owner(m_image_ctx, &m_is_primary, ctx);
}

template <typename I>
Context *EnableRequest<I>::handle_get_tag_owner(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to check tag ownership: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  if (!m_is_primary) {
    lderr(cct) << "last journal tag not owned by local cluster" << dendl;
    *result = -EINVAL;
    return m_on_finish;
  }

  send_get_mirror_image();
  return nullptr;
}

template <typename I>
void EnableRequest<I>::send_get_mirror_image() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_image_get_start(&op, m_image_ctx->id);

  using klass = EnableRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_get_mirror_image>(this);
  m_out_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *EnableRequest<I>::handle_get_mirror_image(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result == 0) {
    bufferlist::iterator iter = m_out_bl.begin();
    *result = cls_client::mirror_image_get_finish(&iter, &m_mirror_image);
  }

  if (*result == 0) {
    if (m_mirror_image.state == cls::rbd::MIRROR_IMAGE_STATE_ENABLED) {
      ldout(cct, 10) << this << " " << __func__
                     << ": mirroring is already enabled" << dendl;
    } else {
      lderr(cct) << "currently disabling" << dendl;
      *result = -EINVAL;
    }
    return m_on_finish;
  }

  if (*result != -ENOENT) {
    lderr(cct) << "failed to retreive mirror image: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  *result = 0;
  m_mirror_image.state = cls::rbd::MIRROR_IMAGE_STATE_ENABLED;
  uuid_d uuid_gen;
  uuid_gen.generate_random();
  m_mirror_image.global_image_id = uuid_gen.to_string();

  send_set_mirror_image();
  return nullptr;
}

template <typename I>
void EnableRequest<I>::send_set_mirror_image() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::ObjectWriteOperation op;
  cls_client::mirror_image_set(&op, m_image_ctx->id, m_mirror_image);

  using klass = EnableRequest<I>;
  librados::AioCompletion *comp =
    create_rados_ack_callback<klass, &klass::handle_set_mirror_image>(this);
  m_out_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(RBD_MIRRORING, comp, &op);
  assert(r == 0);
  comp->release();
}

template <typename I>
Context *EnableRequest<I>::handle_set_mirror_image(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to enable mirroring: " << cpp_strerror(*result)
               << dendl;
    return m_on_finish;
  }

  send_notify_mirroring_watcher();
  return nullptr;
}

template <typename I>
void EnableRequest<I>::send_notify_mirroring_watcher() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = EnableRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_notify_mirroring_watcher>(this);

  MirroringWatcher<>::notify_image_updated(m_image_ctx->md_ctx,
                                           cls::rbd::MIRROR_IMAGE_STATE_ENABLED,
                                           m_image_ctx->id,
                                           m_mirror_image.global_image_id, ctx);
}

template <typename I>
Context *EnableRequest<I>::handle_notify_mirroring_watcher(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to send update notification: "
               << cpp_strerror(*result) << dendl;
    *result = 0;
  }

  return m_on_finish;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::EnableRequest<librbd::ImageCtx>;
