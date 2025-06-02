// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirror/GetMirrorImageRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"

#include <shared_mutex> // for std::shared_lock

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::mirror::GetMirrorImageRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace mirror {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
GetMirrorImageRequest<I>::GetMirrorImageRequest(librados::IoCtx& io_ctx,
                                  const std::string &image_id,
                                  cls::rbd::MirrorImage *mirror_image,
                                  Context *on_finish)
  : m_io_ctx(io_ctx), m_image_id(image_id),
    m_mirror_image(mirror_image), m_on_finish(on_finish),
    m_cct(reinterpret_cast<CephContext *>(io_ctx.cct())) {
}

template <typename I>
void GetMirrorImageRequest<I>::send() {
  get_mirror_image();
}

template <typename I>
void GetMirrorImageRequest<I>::get_mirror_image() {
  ldout(m_cct, 20) << "image_id: " << m_image_id << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_image_get_start(&op, m_image_id);

  librados::AioCompletion *comp = create_rados_callback<
    GetMirrorImageRequest<I>, &GetMirrorImageRequest<I>::handle_get_mirror_image>(this);
  int r = m_io_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void GetMirrorImageRequest<I>::handle_get_mirror_image(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  m_mirror_image->state = cls::rbd::MIRROR_IMAGE_STATE_DISABLED;
  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = cls_client::mirror_image_get_finish(&iter, m_mirror_image);
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

  finish(0);
}

template <typename I>
void GetMirrorImageRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace mirror
} // namespace librbd

template class librbd::mirror::GetMirrorImageRequest<librbd::ImageCtx>;
