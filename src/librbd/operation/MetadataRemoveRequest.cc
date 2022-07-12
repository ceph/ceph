// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/MetadataRemoveRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::MetadataRemoveRequest: "

namespace librbd {
namespace operation {

template <typename I>
MetadataRemoveRequest<I>::MetadataRemoveRequest(I &image_ctx,
                                                Context *on_finish,
                                                const std::string &key)
  : Request<I>(image_ctx, on_finish), m_key(key) {
}

template <typename I>
void MetadataRemoveRequest<I>::send_op() {
  send_metadata_remove();
}

template <typename I>
bool MetadataRemoveRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << " r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }
  return true;
}

template <typename I>
void MetadataRemoveRequest<I>::send_metadata_remove() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));

  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  librados::ObjectWriteOperation op;
  cls_client::metadata_remove(&op, m_key);

  librados::AioCompletion *comp = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

} // namespace operation
} // namespace librbd

template class librbd::operation::MetadataRemoveRequest<librbd::ImageCtx>;
