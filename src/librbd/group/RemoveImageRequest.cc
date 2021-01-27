// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/group/RemoveImageRequest.h"
#include "include/ceph_assert.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::group::RemoveImageRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace group {

using librbd::util::create_rados_callback;

template <typename I>
void RemoveImageRequest<I>::send() {
  pre_unlink();
}

template <typename I>
void RemoveImageRequest<I>::pre_unlink() {
  CephContext *cct = (CephContext *)m_image_io_ctx.cct();
  ldout(cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::group_image_set(
      &op, {m_image_id, m_image_io_ctx.get_id(),
            cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE});
  auto comp = create_rados_callback<
      RemoveImageRequest<I>, &RemoveImageRequest<I>::handle_pre_unlink>(this);

  int r = m_group_io_ctx.aio_operate(util::group_header_name(m_group_id), comp,
                                     &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void RemoveImageRequest<I>::handle_pre_unlink(int r) {
  CephContext *cct = (CephContext *)m_image_io_ctx.cct();
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to pre unlink group image: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  remove_group();
}

template <typename I>
void RemoveImageRequest<I>::remove_group() {
  CephContext *cct = (CephContext *)m_image_io_ctx.cct();
  ldout(cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::image_group_remove(&op, {m_group_id, m_group_io_ctx.get_id()});
  auto comp = create_rados_callback<
      RemoveImageRequest<I>, &RemoveImageRequest<I>::handle_remove_group>(this);

  int r = m_image_io_ctx.aio_operate(util::header_name(m_image_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void RemoveImageRequest<I>::handle_remove_group(int r) {
  CephContext *cct = (CephContext *)m_image_io_ctx.cct();
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to remove image group: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  post_unlink();
}

template <typename I>
void RemoveImageRequest<I>::post_unlink() {
  CephContext *cct = (CephContext *)m_image_io_ctx.cct();
  ldout(cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::group_image_remove(&op, {m_image_id, m_image_io_ctx.get_id()});
  auto comp = create_rados_callback<
      RemoveImageRequest<I>, &RemoveImageRequest<I>::handle_post_unlink>(this);

  int r = m_group_io_ctx.aio_operate(util::group_header_name(m_group_id), comp,
                                     &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void RemoveImageRequest<I>::handle_post_unlink(int r) {
  CephContext *cct = (CephContext *)m_image_io_ctx.cct();
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to post unlink group image: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void RemoveImageRequest<I>::finish(int r) {
  CephContext *cct = (CephContext *)m_image_io_ctx.cct();
  ldout(cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
}

} // namespace group
} // namespace librbd

template class librbd::group::RemoveImageRequest<librbd::ImageCtx>;
