// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/DetachParentRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::DetachParentRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace image {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
void DetachParentRequest<I>::send() {
  detach_parent();
}

template <typename I>
void DetachParentRequest<I>::detach_parent() {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << dendl;

  librados::ObjectWriteOperation op;
  if (!m_legacy_parent) {
    librbd::cls_client::parent_detach(&op);
  } else {
    librbd::cls_client::remove_parent(&op);
  }

  auto aio_comp = create_rados_callback<
    DetachParentRequest<I>,
    &DetachParentRequest<I>::handle_detach_parent>(this);
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void DetachParentRequest<I>::handle_detach_parent(int r) {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << dendl;

   if (!m_legacy_parent && r == -EOPNOTSUPP) {
    ldout(cct, 10) << "retrying using legacy parent method" << dendl;
    m_legacy_parent = true;
    detach_parent();
    return;
  }

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "detach parent encountered an error: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void DetachParentRequest<I>::finish(int r) {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image
} // namespace librbd

template class librbd::image::DetachParentRequest<librbd::ImageCtx>;
