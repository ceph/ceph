// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/ImageStateUpdateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::ImageStateUpdateRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using util::create_rados_callback;

template <typename I>
ImageStateUpdateRequest<I>::ImageStateUpdateRequest(
    librados::IoCtx& io_ctx,
    const std::string& image_id,
    cls::rbd::MirrorImageState mirror_image_state,
    const cls::rbd::MirrorImage& mirror_image,
    Context* on_finish)
  : m_io_ctx(io_ctx), m_image_id(image_id),
    m_mirror_image_state(mirror_image_state), m_mirror_image(mirror_image),
    m_on_finish(on_finish), m_cct(static_cast<CephContext*>(m_io_ctx.cct())) {
  ceph_assert(m_mirror_image_state != cls::rbd::MIRROR_IMAGE_STATE_DISABLED);
}

template <typename I>
void ImageStateUpdateRequest<I>::send() {
  get_mirror_image();
}

template <typename I>
void ImageStateUpdateRequest<I>::get_mirror_image() {
  if (!m_mirror_image.global_image_id.empty()) {
    set_mirror_image();
    return;
  }

  ldout(m_cct, 10) << dendl;
  librados::ObjectReadOperation op;
  cls_client::mirror_image_get_start(&op, m_image_id);

  auto comp = create_rados_callback<
    ImageStateUpdateRequest<I>,
    &ImageStateUpdateRequest<I>::handle_get_mirror_image>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ImageStateUpdateRequest<I>::handle_get_mirror_image(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::mirror_image_get_finish(&iter, &m_mirror_image);
  }

  if (r == -ENOENT) {
    ldout(m_cct, 20) << "mirroring is disabled" << dendl;
    finish(0);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to retrieve mirroring state: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  set_mirror_image();
}

template <typename I>
void ImageStateUpdateRequest<I>::set_mirror_image() {
  if (m_mirror_image.state == m_mirror_image_state) {
    finish(0);
    return;
  }

  ldout(m_cct, 10) << dendl;
  m_mirror_image.state = m_mirror_image_state;

  librados::ObjectWriteOperation op;
  cls_client::mirror_image_set(&op, m_image_id, m_mirror_image);

  auto comp = create_rados_callback<
    ImageStateUpdateRequest<I>,
    &ImageStateUpdateRequest<I>::handle_set_mirror_image>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ImageStateUpdateRequest<I>::handle_set_mirror_image(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to disable mirroring image: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  notify_mirroring_watcher();
}

template <typename I>
void ImageStateUpdateRequest<I>::notify_mirroring_watcher() {
  ldout(m_cct, 10) << dendl;

  auto ctx = util::create_context_callback<
    ImageStateUpdateRequest<I>,
    &ImageStateUpdateRequest<I>::handle_notify_mirroring_watcher>(this);
  MirroringWatcher<I>::notify_image_updated(
    m_io_ctx, m_mirror_image_state, m_image_id, m_mirror_image.global_image_id,
    ctx);
}

template <typename I>
void ImageStateUpdateRequest<I>::handle_notify_mirroring_watcher(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to notify mirror image update: " << cpp_strerror(r)
                 << dendl;
  }

  finish(0);
}

template <typename I>
void ImageStateUpdateRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::ImageStateUpdateRequest<librbd::ImageCtx>;
