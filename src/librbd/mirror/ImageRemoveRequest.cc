// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/ImageRemoveRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/MirroringWatcher.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::ImageRemoveRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using util::create_rados_callback;

template <typename I>
ImageRemoveRequest<I>::ImageRemoveRequest(
    librados::IoCtx& io_ctx, const std::string& global_image_id,
    const std::string& image_id, Context* on_finish)
  : m_io_ctx(io_ctx), m_global_image_id(global_image_id), m_image_id(image_id),
    m_on_finish(on_finish), m_cct(static_cast<CephContext*>(m_io_ctx.cct())) {
}

template <typename I>
void ImageRemoveRequest<I>::send() {
  remove_mirror_image();
}

template <typename I>
void ImageRemoveRequest<I>::remove_mirror_image() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::mirror_image_remove(&op, m_image_id);

  auto comp = create_rados_callback<
    ImageRemoveRequest<I>,
    &ImageRemoveRequest<I>::handle_remove_mirror_image>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ImageRemoveRequest<I>::handle_remove_mirror_image(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to remove mirroring image: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  notify_mirroring_watcher();
}

template <typename I>
void ImageRemoveRequest<I>::notify_mirroring_watcher() {
  ldout(m_cct, 10) << dendl;

  auto ctx = util::create_context_callback<
    ImageRemoveRequest<I>,
    &ImageRemoveRequest<I>::handle_notify_mirroring_watcher>(this);
  MirroringWatcher<I>::notify_image_updated(
    m_io_ctx, cls::rbd::MIRROR_IMAGE_STATE_DISABLED,
    m_image_id, m_global_image_id, ctx);
}

template <typename I>
void ImageRemoveRequest<I>::handle_notify_mirroring_watcher(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to notify mirror image update: " << cpp_strerror(r)
                 << dendl;
  }

  finish(0);
}

template <typename I>
void ImageRemoveRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::ImageRemoveRequest<librbd::ImageCtx>;
