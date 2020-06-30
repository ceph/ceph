// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/AttachParentRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::AttachParentRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace image {

using util::create_rados_callback;

template <typename I>
void AttachParentRequest<I>::send() {
  attach_parent();
}

template <typename I>
void AttachParentRequest<I>::attach_parent() {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << "parent_image_spec=" << m_parent_image_spec << dendl;

  librados::ObjectWriteOperation op;
  if (!m_legacy_parent) {
    librbd::cls_client::parent_attach(&op, m_parent_image_spec,
                                      m_parent_overlap, m_reattach);
  } else {
    librbd::cls_client::set_parent(&op, m_parent_image_spec, m_parent_overlap);
  }

  auto aio_comp = create_rados_callback<
    AttachParentRequest<I>,
    &AttachParentRequest<I>::handle_attach_parent>(this);
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void AttachParentRequest<I>::handle_attach_parent(int r) {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << dendl;

  if (!m_legacy_parent && r == -EOPNOTSUPP && !m_reattach) {
    if (m_parent_image_spec.pool_namespace ==
          m_image_ctx.md_ctx.get_namespace()) {
      m_parent_image_spec.pool_namespace = "";
    }
    if (m_parent_image_spec.pool_namespace.empty()) {
      ldout(cct, 10) << "retrying using legacy parent method" << dendl;
      m_legacy_parent = true;
      attach_parent();
      return;
    }

    // namespaces require newer OSDs
    r = -EXDEV;
  }

  if (r < 0) {
    lderr(cct) << "attach parent encountered an error: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void AttachParentRequest<I>::finish(int r) {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image
} // namespace librbd

template class librbd::image::AttachParentRequest<librbd::ImageCtx>;
