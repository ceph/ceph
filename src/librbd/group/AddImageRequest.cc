// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/group/AddImageRequest.h"
#include "include/ceph_assert.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/ceph_context.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::group::AddImageRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace group {

using util::create_rados_callback;

template <typename I>
void AddImageRequest<I>::send() {
  pre_link();
}

template <typename I>
void AddImageRequest<I>::pre_link() {
  CephContext *cct = (CephContext *)m_image_io_ctx.cct();
  ldout(cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::group_image_set(
      &op, {m_image_id, m_image_io_ctx.get_id(),
            cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE});
  auto comp = create_rados_callback<
      AddImageRequest<I>, &AddImageRequest<I>::handle_pre_link>(this);

  int r = m_group_io_ctx.aio_operate(util::group_header_name(m_group_id), comp,
                                     &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void AddImageRequest<I>::handle_pre_link(int r) {
  CephContext *cct = (CephContext *)m_image_io_ctx.cct();
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to pre link group image: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  add_group();
}

template <typename I>
void AddImageRequest<I>::add_group() {
  CephContext *cct = (CephContext *)m_image_io_ctx.cct();
  ldout(cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::image_group_add(&op, {m_group_id, m_group_io_ctx.get_id()});
  auto comp = create_rados_callback<
      AddImageRequest<I>, &AddImageRequest<I>::handle_add_group>(this);

  int r = m_image_io_ctx.aio_operate(util::header_name(m_image_id), comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void AddImageRequest<I>::handle_add_group(int r) {
  CephContext *cct = (CephContext *)m_image_io_ctx.cct();
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to add image group: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
  }

  post_link();
}

template <typename I>
void AddImageRequest<I>::post_link() {
  CephContext *cct = (CephContext *)m_image_io_ctx.cct();
  ldout(cct, 10) << dendl;

  librados::ObjectWriteOperation op;

  if (m_ret_val < 0) {
    cls_client::group_image_remove(&op, {m_image_id, m_image_io_ctx.get_id()});
  } else {
    cls_client::group_image_set(
        &op, {m_image_id, m_image_io_ctx.get_id(),
              cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED});
  }
  auto comp = create_rados_callback<
      AddImageRequest<I>, &AddImageRequest<I>::handle_post_link>(this);

  int r = m_group_io_ctx.aio_operate(util::group_header_name(m_group_id), comp,
                                     &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void AddImageRequest<I>::handle_post_link(int r) {
  CephContext *cct = (CephContext *)m_image_io_ctx.cct();
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to post link group image: " << cpp_strerror(r)
               << dendl;
    if (m_ret_val == 0) {
      m_ret_val = r;
    }
  }

  finish(m_ret_val);
}

template <typename I>
void AddImageRequest<I>::finish(int r) {
  CephContext *cct = (CephContext *)m_image_io_ctx.cct();
  ldout(cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
}

} // namespace group
} // namespace librbd

template class librbd::group::AddImageRequest<librbd::ImageCtx>;
