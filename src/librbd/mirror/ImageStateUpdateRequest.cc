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
    if (m_mirror_image.state == m_mirror_image_state) {
      finish(0);
      return;
    }

    get_group();
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

  if (m_mirror_image.state == m_mirror_image_state) {
    finish(0);
    return;
  }

  get_group();
}

template <typename I>
void ImageStateUpdateRequest<I>::get_group() {
  ldout(m_cct, 10) << dendl;
  librados::ObjectReadOperation op;
  cls_client::image_group_get_start(&op);

  auto comp = create_rados_callback<
    ImageStateUpdateRequest<I>,
    &ImageStateUpdateRequest<I>::handle_get_group>(this);
  m_out_bl.clear();
  int r = m_io_ctx.aio_operate(util::header_name(m_image_id), comp, &op,
                               &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ImageStateUpdateRequest<I>::handle_get_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::image_group_get_finish(&iter, &m_group_spec);
  }

  if (r < 0) {
    lderr(m_cct) << "failed to retrieve image group: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  get_mirror_group();
}

template <typename I>
void ImageStateUpdateRequest<I>::get_mirror_group() {
  if (!m_group_spec.is_valid()) {
    m_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_DISABLED;
    set_mirror_image();
    return;
  } else if (m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLED ||
             m_mirror_group.state == cls::rbd::MIRROR_GROUP_STATE_ENABLING) {
    ceph_assert(m_group_spec.is_valid());
    m_mirror_image.group_spec = m_group_spec;
  }

  ldout(m_cct, 10) << dendl;

  int r = util::create_ioctx(m_io_ctx, "group", m_group_spec.pool_id, {},
                             &m_group_io_ctx);
  if (r < 0) {
    finish(r);
    return;
  }

  librados::ObjectReadOperation op;
  cls_client::mirror_group_get_start(&op, m_group_spec.group_id);

  auto comp = create_rados_callback<
    ImageStateUpdateRequest<I>,
    &ImageStateUpdateRequest<I>::handle_get_mirror_group>(this);
  m_out_bl.clear();
  r = m_group_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ImageStateUpdateRequest<I>::handle_get_mirror_group(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::mirror_group_get_finish(&iter, &m_mirror_group);
  }

  if (r == -ENOENT) {
    m_mirror_group.state = cls::rbd::MIRROR_GROUP_STATE_DISABLED;
  } else if (r < 0) {
    lderr(m_cct) << "failed to retrieve group mirroring state: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  set_mirror_image();
}

template <typename I>
void ImageStateUpdateRequest<I>::set_mirror_image() {
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
    lderr(m_cct) << "failed to set mirror image: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  notify_mirroring_watcher();
}

template <typename I>
void ImageStateUpdateRequest<I>::notify_mirroring_watcher() {
  // skip image notification if mirroring for the image group is disabled
  if (m_mirror_group.state != cls::rbd::MIRROR_GROUP_STATE_DISABLED) {
    finish(0);
    return;
  }

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
