// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/SetFlagsRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::SetFlagsRequest: "

namespace librbd {
namespace image {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
SetFlagsRequest<I>::SetFlagsRequest(I *image_ctx, uint64_t flags,
				    uint64_t mask, Context *on_finish)
  : m_image_ctx(image_ctx), m_flags(flags), m_mask(mask),
    m_on_finish(on_finish) {
}

template <typename I>
void SetFlagsRequest<I>::send() {
  send_set_flags();
}

template <typename I>
void SetFlagsRequest<I>::send_set_flags() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  RWLock::WLocker snap_locker(m_image_ctx->snap_lock);
  std::vector<uint64_t> snap_ids;
  snap_ids.push_back(CEPH_NOSNAP);
  for (auto it : m_image_ctx->snap_info) {
    snap_ids.push_back(it.first);
  }

  Context *ctx = create_context_callback<
    SetFlagsRequest<I>, &SetFlagsRequest<I>::handle_set_flags>(this);
  C_Gather *gather_ctx = new C_Gather(cct, ctx);

  for (auto snap_id : snap_ids) {
    librados::ObjectWriteOperation op;
    cls_client::set_flags(&op, snap_id, m_flags, m_mask);

    librados::AioCompletion *comp =
      create_rados_callback(gather_ctx->new_sub());
    int r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op);
    assert(r == 0);
    comp->release();
  }
  gather_ctx->activate();
}

template <typename I>
Context *SetFlagsRequest<I>::handle_set_flags(int *result) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "set_flags failed: " << cpp_strerror(*result)
	       << dendl;
  }
  return m_on_finish;
}

} // namespace image
} // namespace librbd

template class librbd::image::SetFlagsRequest<librbd::ImageCtx>;
