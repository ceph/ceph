// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/image_deleter/ImageRemoveRequest.h"
#include "include/rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/mirror/ImageRemoveRequest.h"
#include "librbd/mirror/GetInfoRequest.h"
#include "tools/rbd_mirror/image_deleter/Types.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_deleter::ImageRemoveRequest: " \
                           << this << " " << __func__ << ": "
namespace rbd {
namespace mirror {
namespace image_deleter {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
void ImageRemoveRequest<I>::send() {
  get_mirror_info();
}

template <typename I>
void ImageRemoveRequest<I>::get_mirror_info() {
  dout(10) << dendl;

  auto ctx = create_context_callback<
    ImageRemoveRequest<I>,
    &ImageRemoveRequest<I>::handle_get_mirror_info>(this);
  auto req = librbd::mirror::GetInfoRequest<I>::create(
    m_io_ctx, m_op_work_queue, m_image_id, &m_mirror_image, &m_promotion_state,
    &m_primary_mirror_uuid, ctx);
  req->send();
}

template <typename I>
void ImageRemoveRequest<I>::handle_get_mirror_info(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    dout(5) << "image " << m_global_image_id << " is not mirrored" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    derr << "error retrieving image primary info for image "
         << m_global_image_id << ": " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  disable_mirror_image();
}

template <typename I>
void ImageRemoveRequest<I>::disable_mirror_image() {
  dout(10) << dendl;

  m_mirror_image.state = cls::rbd::MIRROR_IMAGE_STATE_DISABLING;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_set(&op, m_image_id, m_mirror_image);

  auto aio_comp = create_rados_callback<
    ImageRemoveRequest<I>,
    &ImageRemoveRequest<I>::handle_disable_mirror_image>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void ImageRemoveRequest<I>::handle_disable_mirror_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    dout(10) << "local image is not mirrored, aborting deletion." << dendl;
    finish(r);
    return;
  } else if (r == -EEXIST || r == -EINVAL) {
    derr << "cannot disable mirroring for image " << m_global_image_id
         << ": global_image_id has changed/reused: "
         << cpp_strerror(r) << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    derr << "cannot disable mirroring for image " << m_global_image_id
         << ": " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  remove_mirror_image();
}

template <typename I>
void ImageRemoveRequest<I>::remove_mirror_image() {
  dout(10) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::mirror_image_remove(&op, m_image_id);

  auto aio_comp = create_rados_callback<
    ImageRemoveRequest<I>,
    &ImageRemoveRequest<I>::handle_remove_mirror_image>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void ImageRemoveRequest<I>::handle_remove_mirror_image(int r) {
  dout(10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    dout(10) << "local image is not mirrored" << dendl;
  } else if (r < 0) {
    derr << "failed to remove mirror image state for " << m_global_image_id
         << ": " << cpp_strerror(r) << dendl;
  }

  finish(r);
}
template <typename I>
void ImageRemoveRequest<I>::finish(int r) {
  dout(10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_deleter
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_deleter::ImageRemoveRequest<librbd::ImageCtx>;
